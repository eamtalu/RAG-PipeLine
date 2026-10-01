"""Rolling-origin backtest: which candidate fits a series, and how wrong it tends to be per step.

Each fold trains on a prefix and predicts the `horizon` days that follow it, so the errors are the
errors a real forecast would have made. The residuals those folds leave, per step ahead, are what
size the interval: a model that is usually within 80 on day one and within 200 on day seven gets an
interval that says so, instead of a constant band drawn to look tidy.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from app.services.analytics_forecast import models as m

Z80 = 1.28  # the 10th and 90th percentiles of a normal spread
BLEND = "blend_top2"


@dataclass(frozen=True)
class Metrics:
    n: int
    mae: float
    #: Sum of absolute errors over sum of actuals. None when the actuals sum to zero.
    wape: float | None
    #: Mean of |error| / actual over the days with a non-zero actual, and how many those were.
    mape: float | None
    mape_n: int
    #: Mean signed error, predicted minus actual: positive is over-forecasting.
    bias: float


@dataclass(frozen=True)
class Fold:
    actual: np.ndarray
    predicted: np.ndarray


@dataclass
class Score:
    name: str
    wape: float | None
    mae: float
    #: residual (predicted - actual) per step ahead, 1-based, pooled across folds
    residuals: dict[int, list[float]] = field(default_factory=dict)
    folds: int = 0
    blend_of: tuple[str, ...] = ()


def metrics(actual: np.ndarray, predicted: np.ndarray) -> Metrics:
    a = np.asarray(actual, dtype=float)
    p = np.asarray(predicted, dtype=float)
    err = p - a
    total = float(np.abs(a).sum())
    nz = a != 0
    return Metrics(
        n=int(a.size), mae=float(np.abs(err).mean()) if a.size else 0.0,
        wape=float(np.abs(err).sum() / total) if total > 0 else None,
        mape=float((np.abs(err[nz]) / np.abs(a[nz])).mean()) if nz.any() else None,
        mape_n=int(nz.sum()), bias=float(err.mean()) if a.size else 0.0)


def rolling_origin(y: np.ndarray, predictor: m.Predictor, *, folds: int, horizon: int,
                   min_train: int) -> list[Fold]:
    """Up to `folds` folds, the newest first. Fold f trains on `y[:n-horizon-f]` and is tested on the
    `horizon` days after it. Folds that would leave fewer than `min_train` training days are dropped."""
    y = np.asarray(y, dtype=float)
    n = y.size
    out = []
    for f in range(folds):
        cut = n - horizon - f
        if cut < min_train:
            break
        out.append(Fold(actual=y[cut:cut + horizon], predicted=predictor(y[:cut], horizon)))
    return out


def _score(name: str, y: np.ndarray, predictor: m.Predictor, *, folds: int, horizon: int,
           min_train: int) -> Score:
    runs = rolling_origin(y, predictor, folds=folds, horizon=horizon, min_train=min_train)
    if not runs:
        return Score(name=name, wape=None, mae=float("inf"))
    met = metrics(np.concatenate([r.actual for r in runs]), np.concatenate([r.predicted for r in runs]))
    residuals: dict[int, list[float]] = {}
    for r in runs:
        for step, (a, p) in enumerate(zip(r.actual, r.predicted), start=1):
            residuals.setdefault(step, []).append(float(p - a))
    return Score(name=name, wape=met.wape, mae=met.mae, residuals=residuals, folds=len(runs))


def _rank(score: Score) -> tuple:
    """Lower is better: WAPE when it exists, else MAE; a simpler model wins a tie."""
    simplicity = m.SIMPLICITY.index(score.name) if score.name in m.SIMPLICITY else len(m.SIMPLICITY)
    return (score.wape if score.wape is not None else float("inf"), score.mae, simplicity)


def blend(predictors: list[m.Predictor]) -> m.Predictor:
    def _p(y: np.ndarray, h: int) -> np.ndarray:
        return np.mean([p(y, h) for p in predictors], axis=0)
    return _p


def score_candidates(y: np.ndarray, names: list[str], *, folds: int, horizon: int,
                     min_train: int) -> dict[str, Score]:
    """Score every named candidate, then the mean of the two best as a candidate of its own. Two
    models that err in different directions average to something better than either; when they do
    not, the blend simply loses the race."""
    y = np.asarray(y, dtype=float)
    scores = {n: _score(n, y, m.CANDIDATES[n], folds=folds, horizon=horizon, min_train=min_train)
              for n in names}
    ranked = sorted(scores.values(), key=_rank)
    if len(ranked) >= 2 and ranked[1].wape is not None:
        top = (ranked[0].name, ranked[1].name)
        blended = _score(BLEND, y, blend([m.CANDIDATES[t] for t in top]), folds=folds, horizon=horizon,
                         min_train=min_train)
        blended.blend_of = top
        scores[BLEND] = blended
    return scores


def select(scores: dict[str, Score]) -> str:
    if not scores:
        raise ValueError("no candidate was scored")
    return min(scores.values(), key=_rank).name


#: The narrowest a band may be, as a share of the point either side: with one fold the residual
#: spread is zero, and a zero-width band would claim a certainty nothing has earned.
MIN_HALF_WIDTH = 0.2


def interval(residuals: dict[int, list[float]], *, step: int, point: float) -> tuple[float, float]:
    """The 10th and 90th percentile of the residuals at `step`, around `point`, clipped at zero.

    With fewer than five residuals at the step the empirical quantiles are two of the values
    themselves, so the width comes from the spread of the residuals pooled over every step instead.
    Beyond the last backtested step the widest known step is used; uncertainty does not shrink with
    distance. A fallback band is never narrower than `MIN_HALF_WIDTH` of the point either side, and
    with no residuals at all it is zero to twice the point: honest about knowing nothing."""
    if not residuals:
        return 0.0, 2.0 * point
    known = max(residuals)
    res = np.asarray(residuals[step] if step in residuals else residuals[known], dtype=float)
    if step > known:
        res = max((np.asarray(v, dtype=float) for v in residuals.values()), key=lambda r: float(np.std(r)))
    if res.size >= 5:
        lo, hi = point - float(np.quantile(res, 0.9)), point - float(np.quantile(res, 0.1))
    else:
        pooled = np.asarray([r for v in residuals.values() for r in v], dtype=float)
        spread = float(np.std(pooled)) if pooled.size >= 3 else float(np.std(res)) if res.size else 0.0
        # the floor applies only here: five or more residuals at the step are an earned band
        half = max(Z80 * spread, MIN_HALF_WIDTH * point)
        lo, hi = point - half, point + half
    lo, hi = min(lo, hi), max(lo, hi)
    if hi - lo <= 0:
        # identical residuals at every fold: still not certainty
        lo, hi = point - MIN_HALF_WIDTH * point, point + MIN_HALF_WIDTH * point
    return max(0.0, lo), max(0.0, hi, point)
