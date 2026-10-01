"""Chunk 136: the backtest that picks a model and sizes its uncertainty.

The pins: the error metrics match a hand calculation (WAPE over everything, MAPE only where the
actual is non-zero and says how many that was, bias signed), a fold never trains on a day it is
tested on, the winner is the model that fits the series rather than a favourite, and the interval
is ordered, non-negative, and falls back to a width from the spread when too few residuals exist.
"""

import numpy as np
import pytest

from app.services.analytics_forecast import backtest as bt
from app.services.analytics_forecast import models as m


def test_metrics_match_a_hand_calculation():
    actual = np.array([100.0, 0.0, 50.0, 200.0])
    predicted = np.array([110.0, 5.0, 40.0, 180.0])
    out = bt.metrics(actual, predicted)
    assert out.n == 4
    assert out.mae == pytest.approx((10 + 5 + 10 + 20) / 4)
    assert out.wape == pytest.approx(45 / 350)
    assert out.mape == pytest.approx((0.10 + 0.20 + 0.10) / 3)
    assert out.mape_n == 3
    assert out.bias == pytest.approx((10 + 5 - 10 - 20) / 4)


def test_metrics_on_an_all_zero_actual_have_no_wape_or_mape():
    out = bt.metrics(np.zeros(3), np.array([1.0, 2.0, 3.0]))
    assert out.wape is None and out.mape is None and out.mape_n == 0
    assert out.mae == 2.0


def test_rolling_origin_never_trains_on_a_tested_day():
    y = np.arange(30.0)
    seen = []

    def spy(train, h):
        seen.append(len(train))
        return np.repeat(train[-1], h)

    folds = bt.rolling_origin(y, spy, folds=4, horizon=7, min_train=7)
    assert len(folds) == 4
    for fold, train_len in zip(folds, seen):
        assert list(fold.actual) == list(y[train_len:train_len + 7])
    assert sorted(seen) == [20, 21, 22, 23]  # 30 - 7 - f for f in 0..3


def test_rolling_origin_shrinks_the_fold_count_to_what_the_history_allows():
    y = np.arange(18.0)
    folds = bt.rolling_origin(y, m.moving_average, folds=6, horizon=7, min_train=7)
    assert len(folds) == 5  # 18 - 7 - f >= 7  ->  f <= 4


def test_select_picks_the_model_that_fits_the_series():
    periodic = np.array([1000.0] * 5 + [500.0] * 2) * np.ones((5, 1))
    periodic = periodic.ravel()  # 35 days, perfectly weekly, Monday start
    scores = bt.score_candidates(periodic, ["seasonal_naive", "moving_average"], folds=3, horizon=7, min_train=7)
    assert bt.select(scores) == "seasonal_naive"

    rng = np.random.default_rng(7)
    flat = 100 + rng.normal(0, 3, 35)
    scores = bt.score_candidates(flat, ["seasonal_naive", "moving_average"], folds=3, horizon=7, min_train=7)
    assert bt.select(scores) == "moving_average"


def test_blend_of_the_top_two_is_scored_like_any_other_candidate():
    y = np.array([1000.0] * 5 + [500.0] * 2) * np.ones((5, 1))
    scores = bt.score_candidates(y.ravel(), ["seasonal_naive", "moving_average", "weekday_mean"],
                                 folds=3, horizon=7, min_train=7)
    assert "blend_top2" in scores
    assert scores["blend_top2"].blend_of == ("seasonal_naive", "weekday_mean")


def test_interval_is_ordered_non_negative_and_falls_back_when_residuals_are_few():
    residuals = {1: [-10.0, -5.0, 0.0, 5.0, 10.0, 20.0], 2: [-3.0, 3.0]}
    lo, hi = bt.interval(residuals, step=1, point=100.0)
    assert lo <= 100.0 <= hi and lo >= 0
    # a residual is predicted minus actual, so an over-forecast (positive) pulls the actual DOWN
    assert lo == pytest.approx(100.0 - np.quantile(residuals[1], 0.9))
    assert hi == pytest.approx(100.0 - np.quantile(residuals[1], 0.1))
    # two residuals at step 2: the width comes from the residuals pooled over every step, and never
    # less than a fifth of the point either side
    lo2, hi2 = bt.interval(residuals, step=2, point=100.0)
    pooled = np.std(residuals[1] + residuals[2])
    assert hi2 - lo2 == pytest.approx(2 * max(1.28 * pooled, 0.2 * 100.0))
    lo3, hi3 = bt.interval(residuals, step=9, point=2.0)  # beyond the last step uses the widest known
    assert lo3 == 0.0 and hi3 > 2.0
    assert bt.interval({}, step=1, point=50.0) == (0.0, 100.0)


def test_an_interval_never_collapses_to_the_point():
    """One fold leaves one residual per step: its spread is zero, and a band of zero width would
    tell the reader the model is certain. It is not; the width falls back to the pooled residuals,
    then to a floor of a fifth of the point."""
    one_fold = {s: [5.0 * s] for s in range(1, 8)}
    lo, hi = bt.interval(one_fold, step=1, point=458.0)
    assert hi - lo > 0 and lo < 458.0 < hi
    lo2, hi2 = bt.interval({1: [0.0]}, step=1, point=458.0)
    assert hi2 - lo2 >= 0.4 * 458.0 - 1e-9
    lo3, hi3 = bt.interval({1: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}, step=1, point=100.0)
    assert hi3 - lo3 >= 40.0 - 1e-9
