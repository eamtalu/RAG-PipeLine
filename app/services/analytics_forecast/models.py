"""The candidate models. Each is `fit_predict(y, h) -> h values`, non-negative and finite.

Deliberately simple and few. On eighteen steady days a model with many parameters fits the noise,
and a forecast nobody can explain is not one an operations manager will trust. The backtest in
`backtest.py` decides which candidate a series gets; nothing here is chosen by hand.

`y` is a dense daily series ending on the day before the first forecast step, so step `k` lands on
index `len(y) + k - 1` and shares a weekday with every index seven apart from it.
"""

from __future__ import annotations

import warnings
from typing import Callable

import numpy as np

Predictor = Callable[[np.ndarray, int], np.ndarray]

SEASON = 7


def _clean(pred: np.ndarray, h: int) -> np.ndarray:
    out = np.asarray(pred, dtype=float).reshape(-1)[:h]
    if out.shape[0] < h:
        out = np.concatenate([out, np.repeat(out[-1] if out.size else 0.0, h - out.shape[0])])
    out = np.where(np.isfinite(out), out, 0.0)
    return np.maximum(out, 0.0)


def seasonal_naive(y: np.ndarray, h: int) -> np.ndarray:
    """Step k is the value one season before it: next Monday is last Monday."""
    y = np.asarray(y, dtype=float)
    if y.size == 0:
        return np.zeros(h)
    season = min(SEASON, y.size)
    last = y[-season:]
    return _clean(np.array([last[k % season] for k in range(h)]), h)


def moving_average(y: np.ndarray, h: int, window: int = SEASON) -> np.ndarray:
    """Flat at the mean of the last `window` days."""
    y = np.asarray(y, dtype=float)
    level = float(y[-window:].mean()) if y.size else 0.0
    return _clean(np.repeat(level, h), h)


def weekday_mean(y: np.ndarray, h: int, weeks: int = 4) -> np.ndarray:
    """Step k is the mean of the last `weeks` values that share its weekday. The weekly pattern
    with no smoothing to tune, which is why it is the fallback when a richer fit fails."""
    y = np.asarray(y, dtype=float)
    n = y.size
    out = []
    for k in range(1, h + 1):
        idx = [n + k - 1 - SEASON * j for j in range(1, weeks + 1)]
        same = [y[i] for i in idx if 0 <= i < n]
        out.append(float(np.mean(same)) if same else (float(y[-SEASON:].mean()) if n else 0.0))
    return _clean(np.array(out), h)


def _ets_fit(y: np.ndarray, h: int) -> np.ndarray:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fit = ExponentialSmoothing(y, trend=None, seasonal="add", seasonal_periods=SEASON,
                                   initialization_method="estimated").fit(optimized=True)
    return np.asarray(fit.forecast(h), dtype=float)


def ets_add_weekly(y: np.ndarray, h: int) -> np.ndarray:
    """Holt-Winters with an additive weekly season and no trend. Falls back to `weekday_mean` when
    the optimiser fails or returns nonsense, so a bad fit never becomes a bad forecast."""
    y = np.asarray(y, dtype=float)
    try:
        pred = _ets_fit(y, h)
        if not np.all(np.isfinite(pred)):
            raise ValueError("non-finite forecast")
    except Exception:
        return weekday_mean(y, h)
    return _clean(pred, h)


def croston_tsb(y: np.ndarray, h: int, alpha: float = 0.1, beta: float = 0.1) -> np.ndarray:
    """Teunter-Syntetos-Babai: smooth the probability of a demand day and the size of a demand
    separately; the forecast is their product, flat over the horizon. Built for series that are
    mostly zero, where an average over all days under-forecasts the days that matter."""
    y = np.asarray(y, dtype=float)
    hits = y[y > 0]
    if hits.size == 0:
        return np.zeros(h)
    first = int(np.argmax(y > 0))
    prob, size = 1.0 / (first + 1), float(hits[0])
    for v in y[first + 1:]:
        if v > 0:
            prob += beta * (1 - prob)
            size += alpha * (v - size)
        else:
            prob += beta * (0 - prob)
    return _clean(np.repeat(prob * size, h), h)


CANDIDATES: dict[str, Predictor] = {
    "seasonal_naive": seasonal_naive,
    "moving_average": moving_average,
    "weekday_mean": weekday_mean,
    "ets_add_weekly": ets_add_weekly,
    "croston_tsb": croston_tsb,
}

#: Fewer parameters first: this order breaks ties in the backtest.
SIMPLICITY = tuple(CANDIDATES)

SPARSE = ("intermittent", "lumpy")


def eligible(name: str, *, n: int, classification: str, min_days_ets: int) -> bool:
    """Whether a candidate may enter the race for a series of `n` days and this demand shape."""
    if n < SEASON:
        return False
    if name == "ets_add_weekly":
        return n >= min_days_ets and classification not in SPARSE
    if name == "croston_tsb":
        return classification in SPARSE
    return True
