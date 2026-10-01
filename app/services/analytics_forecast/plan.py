"""One run's arithmetic, start to finish, with no database in sight.

`run_all` takes what `history_store` read and returns every prediction and series row to write.
It runs inside `asyncio.to_thread`, so nothing here may touch a session. The split is the point:
this module can be tested on planted numbers in milliseconds, and the store modules can be tested
on a handful of rows, and neither test needs the other to be right.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, tzinfo
from decimal import Decimal

import numpy as np

from app.services.analytics_forecast import backtest as bt
from app.services.analytics_forecast import history_store as hs
from app.services.analytics_forecast import hourly, models, series, staffing
from app.services.analytics_forecast.prediction_store import PredictionRow, SeriesRow

TOTAL = "total"
RECENT_DAYS = 28


class InsufficientHistory(Exception):
    """Fewer steady days than the configuration allows a forecast to be built on."""


@dataclass(frozen=True)
class ForecastConfig:
    settlement: str = "pick_release"
    #: The settled value that counts units (the `picked` of a pick release).
    units_value: str = "picked"
    history_days: int = 120
    min_history_days: int = 14
    min_days_ets: int = 21
    #: An item with fewer active days in the last 28 gets week and month targets only.
    item_daily_min_active_days: int = 10
    max_items: int = 2000
    backtest_folds: int = 6
    backtest_horizon: int = 7
    min_train_days: int = 7
    score_lag_hours: int = 2
    accuracy_window_days: int = 28
    staffing_buffer_pct: float = 0.10
    horizon_days: int = 14
    horizon_weeks: int = 5
    horizon_months: int = 4
    heatmap_days: int = 8
    #: Operator override of the ramp trim: history is read from this day, whatever the data says.
    history_start: date | None = None


@dataclass
class Spec:
    """One thing to forecast and the daily history behind it."""

    metric: str
    subject_kind: str
    subject: str
    daily: series.DailySeries
    grains: tuple[str, ...]


@dataclass
class RunOutput:
    predictions: list[PredictionRow] = field(default_factory=list)
    series: list[SeriesRow] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    history: dict = field(default_factory=dict)
    counts: dict = field(default_factory=dict)


def _dec(x: float) -> Decimal:
    return Decimal(str(round(float(x), 6)))


# ============================================================== building the series

def _dense(rows, start: date, end: date, pick) -> series.DailySeries:
    return series.dense_daily(((r.day, pick(r)) for r in rows), start=start, end=end)


def build_specs(bundle: hs.HistoryBundle, *, as_of: date, cfg: ForecastConfig) -> list[Spec]:
    """Headline series for lines and units: the total, each warehouse, each transaction name. Then
    units per item; an item's grains depend on how often it moves."""
    start, end = bundle.start, as_of
    all_grains = tuple(series.GRAINS)
    specs: list[Spec] = []
    for metric, pick in (("lines", lambda r: r.lines), ("units", lambda r: r.units)):
        specs.append(Spec(metric, TOTAL, TOTAL, _dense(bundle.daily, start, end, pick), all_grains))
        for kind, attr in (("warehouse", "warehouse"), ("transaction_name", "transaction_name")):
            for subject in sorted({getattr(r, attr) for r in bundle.daily if getattr(r, attr)}):
                rows = [r for r in bundle.daily if getattr(r, attr) == subject]
                specs.append(Spec(metric, kind, subject, _dense(rows, start, end, pick), all_grains))
    by_item: dict[str, list[hs.ItemDayRow]] = {}
    for r in bundle.items:
        if r.item_number:
            by_item.setdefault(r.item_number, []).append(r)
    for item in sorted(by_item)[:cfg.max_items]:
        daily = _dense(by_item[item], start, end, lambda r: r.units)
        active = sum(1 for v in daily.values[-RECENT_DAYS:] if v > 0)
        grains = all_grains if active >= cfg.item_daily_min_active_days else ("week", "month")
        specs.append(Spec("units", "item_number", item, daily, grains))
    return specs


# ============================================================== one series

@dataclass
class Fitted:
    model: str
    blend_of: tuple[str, ...]
    path: np.ndarray
    residuals: dict[int, list[float]]
    scores: dict[str, bt.Score]


def _predictor(name: str, scores: dict[str, bt.Score]) -> models.Predictor:
    if name == bt.BLEND:
        return bt.blend([models.CANDIDATES[t] for t in scores[name].blend_of])
    return models.CANDIDATES[name]


def fit(y: np.ndarray, *, classification: str, steps: int, cfg: ForecastConfig) -> Fitted | None:
    """Race the eligible candidates, keep the winner, and walk it `steps` days ahead."""
    names = [n for n in models.CANDIDATES
             if models.eligible(n, n=y.size, classification=classification, min_days_ets=cfg.min_days_ets)]
    if not names:
        return None
    scores = bt.score_candidates(y, names, folds=cfg.backtest_folds, horizon=cfg.backtest_horizon,
                                 min_train=cfg.min_train_days)
    winner = bt.select(scores)
    path = _predictor(winner, scores)(y, steps)
    return Fitted(model=winner, blend_of=scores[winner].blend_of, path=path,
                  residuals=scores[winner].residuals, scores=scores)


def _series_detail(fitted: Fitted, classification: str, steady: date, as_of: date, n: int) -> dict:
    best = fitted.scores[fitted.model]
    return {
        "model": fitted.model, "blend_of": list(fitted.blend_of), "classification": classification,
        "backtest": {"wape": best.wape, "mae": best.mae, "folds": best.folds,
                     "candidates": {k: v.wape for k, v in fitted.scores.items()}},
        "history": {"start": steady.isoformat(), "end": as_of.isoformat(), "n": n}}


def _point(spec: Spec, target: series.Target, grain: str, as_of: date, fitted: Fitted, base: dict,
           *, model_version: str, run_id, predicted_at: datetime) -> PredictionRow:
    """A daily target is one step of the path with its own interval. A week or month is the actual
    so far plus the path over the days still to come, with the daily intervals summed: wider than
    the truth, which is the right side to be wrong on when the data is this young."""
    actual_to_date = 0.0
    p10 = p50 = p90 = 0.0
    day = target.start
    while day <= target.end:
        if day <= as_of:
            i = (day - spec.daily.start).days
            actual_to_date += spec.daily.values[i] if 0 <= i < len(spec.daily) else 0.0
        else:
            step = (day - as_of).days
            value = float(fitted.path[step - 1]) if step - 1 < fitted.path.size else float(fitted.path[-1])
            lo, hi = bt.interval(fitted.residuals, step=step, point=value)
            p50 += value
            p10 += lo
            p90 += hi
        day += timedelta(days=1)
    partial = target.start <= as_of
    # detail is JSONB: plain floats, never Decimal
    detail = {**base, "partial": partial, "actual_to_date": round(actual_to_date, 6) if partial else None}
    if grain == "day":
        detail["step"] = (target.start - as_of).days
    return PredictionRow(
        metric=spec.metric, grain=grain, subject_kind=spec.subject_kind, subject=spec.subject, horizon=target.label,
        model_version=model_version, target_at=target.target_at, predicted_at=predicted_at,
        value=_dec(actual_to_date + p50), p10=_dec(actual_to_date + p10), p90=_dec(actual_to_date + p90),
        detail=detail, run_id=run_id)


def _series_row(spec: Spec, *, classification: str, fitted: Fitted | None, n: int, run_id) -> SeriesRow:
    recent = spec.daily.values[-RECENT_DAYS:]
    best = fitted.scores[fitted.model] if fitted else None
    return SeriesRow(
        metric=spec.metric, grain="day", subject_kind=spec.subject_kind, subject=spec.subject,
        classification=classification, model=fitted.model if fitted else None,
        backtest_wape=_dec(best.wape) if best and best.wape is not None else None, history_days=n,
        active_days_28d=sum(1 for v in recent if v > 0), volume_28d=_dec(sum(recent)), last_run_id=run_id)


# ============================================================== the run

def _targets(as_of: date, tz: tzinfo, cfg: ForecastConfig) -> dict[str, list[series.Target]]:
    return {"day": series.targets(as_of=as_of, grain="day", n=cfg.horizon_days, tz=tz),
            "week": series.targets(as_of=as_of, grain="week", n=cfg.horizon_weeks, tz=tz),
            "month": series.targets(as_of=as_of, grain="month", n=cfg.horizon_months, tz=tz)}


def _hourly_rows(total: list[PredictionRow], bundle: hs.HistoryBundle, *, as_of: date, tz: tzinfo, steady: date,
                 cfg: ForecastConfig, model_version: str, run_id, predicted_at: datetime,
                 base: dict) -> tuple[list[PredictionRow], dict]:
    """The heatmap: the headline daily lines cut into hours by the measured profile, and the pickers
    each hour needs at the measured throughput."""
    rows_in = hourly.since(bundle.hourly, steady)
    share = hourly.profile(rows_in)
    tp = hourly.throughput(rows_in)
    days = [hourly.DayPoint(day=r.target_at.astimezone(tz).date(), p10=float(r.p10), p50=float(r.value),
                            p90=float(r.p90)) for r in total if r.grain == "day"][:cfg.heatmap_days]
    out = []
    for hp in hourly.spread(days, share):
        at = datetime(hp.day.year, hp.day.month, hp.day.day, hp.hour, tzinfo=tz).astimezone(series.timezone.utc)
        label = series.horizon_label("day", (hp.day - as_of).days)
        rate = tp.at(dow=hp.day.weekday(), hour=hp.hour)
        detail = {**base, "hour": hp.hour, "lines_per_picker_hour": rate}
        out.append(PredictionRow("lines", "hour", TOTAL, TOTAL, label, model_version, at, predicted_at,
                                 _dec(hp.p50), _dec(hp.p10), _dec(hp.p90), detail, run_id))
        need = staffing.pickers_needed(hp.p50, rate, buffer_pct=cfg.staffing_buffer_pct)
        high = staffing.pickers_needed(hp.p90, rate, buffer_pct=cfg.staffing_buffer_pct)
        low = staffing.pickers_needed(hp.p10, rate, buffer_pct=cfg.staffing_buffer_pct)
        out.append(PredictionRow("pickers", "hour", TOTAL, TOTAL, label, model_version, at, predicted_at,
                                 Decimal(need if need is not None else 0),
                                 Decimal(low if low is not None else 0), Decimal(high if high is not None else 0),
                                 {**detail, "throughput_known": rate is not None}, run_id))
    return out, {"lines_per_picker_hour": tp.overall, "slots_measured": len(tp.by_slot)}


def run_all(bundle: hs.HistoryBundle, *, as_of: date, tz: tzinfo, cfg: ForecastConfig, model_version: str,
            run_id, predicted_at: datetime) -> RunOutput:
    out = RunOutput()
    specs = build_specs(bundle, as_of=as_of, cfg=cfg)
    total = next(s for s in specs if s.metric == "lines" and s.subject_kind == TOTAL)
    # The read window may start long before the first settled row; the ramp is measured from the
    # first day that has any data, not from an empty window.
    data_start = next((d for d, v in zip(total.daily.dates(), total.daily.values) if v > 0), bundle.start)
    steady = cfg.history_start or series.steady_start(total.daily.since(data_start))
    steady = max(steady, data_start)
    n_total = len(total.daily.since(steady))
    out.history = {"start": data_start.isoformat(), "end": as_of.isoformat(), "steady_from": steady.isoformat(),
                   "ramp_trimmed_days": (steady - data_start).days, "days": n_total}
    if n_total < cfg.min_history_days:
        raise InsufficientHistory(f"insufficient_history: {n_total} steady days, {cfg.min_history_days} needed")
    if n_total < cfg.min_days_ets:
        out.warnings.append(f"ets ineligible: {n_total} steady days < {cfg.min_days_ets}; "
                            "weekly-pattern candidates only")
    if bundle.items_truncated:
        out.warnings.append(f"items truncated at the {cfg.max_items} busiest; raise analytics_forecast_max_items")

    targets = _targets(as_of, tz, cfg)
    steps = (targets["month"][-1].end - as_of).days
    headline_total: list[PredictionRow] = []
    counts = {"headline": 0, "items": 0, "items_daily": 0, "unforecast": 0}
    for spec in specs:
        y = np.asarray(spec.daily.since(steady).values, dtype=float)
        classification = series.classify(y)
        fitted = None if classification == "insufficient" else fit(y, classification=classification, steps=steps, cfg=cfg)
        out.series.append(_series_row(spec, classification=classification, fitted=fitted, n=y.size, run_id=run_id))
        is_item = spec.subject_kind == "item_number"
        counts["items" if is_item else "headline"] += 1
        if fitted is None:
            counts["unforecast"] += 1
            continue
        if is_item and "day" in spec.grains:
            counts["items_daily"] += 1
        base = _series_detail(fitted, classification, steady, as_of, y.size)
        for grain in spec.grains:
            for target in targets[grain]:
                row = _point(spec, target, grain, as_of, fitted, base, model_version=model_version, run_id=run_id,
                             predicted_at=predicted_at)
                out.predictions.append(row)
                if spec is total:
                    headline_total.append(row)
    if headline_total:
        base = {k: v for k, v in headline_total[0].detail.items() if k in ("model", "classification", "history")}
        rows, staffing_detail = _hourly_rows(headline_total, bundle, as_of=as_of, tz=tz, steady=steady, cfg=cfg,
                                             model_version=model_version, run_id=run_id, predicted_at=predicted_at,
                                             base=base)
        out.predictions.extend(rows)
        out.history["staffing"] = staffing_detail
        if staffing_detail["lines_per_picker_hour"] is None:
            out.warnings.append("no picker throughput measured; pickers needed is 0 everywhere")
    out.counts = counts
    return out


# ============================================================== actuals, for scoring

class Actuals:
    """What really happened, from the same bundle the run learned from, keyed the way a prediction is."""

    def __init__(self, bundle: hs.HistoryBundle, *, as_of: date, cfg: ForecastConfig):
        self._daily = {(s.metric, s.subject_kind, s.subject): s.daily for s in build_specs(bundle, as_of=as_of, cfg=cfg)}
        self._hours = {r.start: r for r in bundle.hourly}
        self._hours_from = min((r.start for r in bundle.hourly), default=None)
        self.start, self.end = bundle.start, as_of

    def value(self, *, metric: str, grain: str, subject_kind: str, subject: str, target_at: datetime,
              tz: tzinfo) -> Decimal | None:
        """The actual for one prediction, or None when the history read did not cover its bucket."""
        local = target_at.astimezone(tz)
        if grain == "hour":
            key = local.replace(tzinfo=None, minute=0, second=0, microsecond=0)
            if self._hours_from is None or key < self._hours_from or local.date() > self.end:
                return None
            row = self._hours.get(key)
            if metric == "pickers":
                return Decimal(row.pickers if row else 0)
            return _dec(row.lines if row else 0.0)
        daily = self._daily.get((metric, subject_kind, subject))
        if daily is None:
            return None
        start, end = series.bucket_bounds(local.date(), grain)
        if start < self.start or end > self.end:
            return None
        i, j = (start - daily.start).days, (end - daily.start).days + 1
        return _dec(sum(daily.values[i:j]))
