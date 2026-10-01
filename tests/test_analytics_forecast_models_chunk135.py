"""Chunk 135: the candidate models every series is tried against.

Every candidate has the same shape, `fit_predict(y, h) -> h values`, so the backtest can treat them
alike. The pins: a candidate never returns a negative or non-finite number, the simple ones recover
the pattern they are named after, Croston/TSB behaves on the sparse series it exists for, and the
eligibility gates keep a model with too little history or the wrong demand shape out of the race.
"""

import numpy as np
import pytest

from app.services.analytics_forecast import models as m

WEEKDAY, WEEKEND = 1000.0, 500.0


def _pattern(days: int, start_dow: int = 0) -> np.ndarray:
    """Weekday/weekend demand starting on `start_dow` (0 = Monday)."""
    return np.array([WEEKEND if (start_dow + i) % 7 >= 5 else WEEKDAY for i in range(days)])


@pytest.mark.parametrize("name", list(m.CANDIDATES))
def test_every_candidate_returns_h_finite_non_negative_values(name):
    y = np.array([3.0, 0.0, 0.0, 8.0, 0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 5.0, 0.0] * 2)
    out = m.CANDIDATES[name](y, 10)
    assert out.shape == (10,)
    assert np.all(np.isfinite(out)) and np.all(out >= 0)


def test_seasonal_naive_repeats_the_last_week():
    y = np.arange(1.0, 22.0)  # 21 days
    out = m.seasonal_naive(y, 10)
    assert list(out[:7]) == list(y[-7:])
    assert list(out[7:]) == list(y[-7:-4])


def test_moving_average_is_flat_at_the_mean_of_the_last_seven_days():
    y = np.array([0.0] * 7 + [7.0] * 7)
    assert list(m.moving_average(y, 3)) == [7.0, 7.0, 7.0]


def test_weekday_mean_recovers_a_weekday_weekend_pattern():
    y = _pattern(28)  # ends on a Sunday, so step 1 is a Monday
    out = m.weekday_mean(y, 7)
    assert list(out) == [WEEKDAY] * 5 + [WEEKEND] * 2


def test_croston_tsb_is_zero_on_nothing_and_a_rate_between_zero_and_the_biggest_hit_otherwise():
    assert list(m.croston_tsb(np.zeros(20), 3)) == [0.0, 0.0, 0.0]
    y = np.array([0, 0, 0, 4, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0], dtype=float)
    out = m.croston_tsb(y, 5)
    assert np.allclose(out, out[0])  # a flat demand rate
    assert 0 < out[0] < 6


def test_ets_fits_a_weekly_pattern_and_falls_back_when_the_fit_fails(monkeypatch):
    out = m.ets_add_weekly(_pattern(28), 7)
    assert np.allclose(out, [WEEKDAY] * 5 + [WEEKEND] * 2, rtol=0.10)

    def boom(*a, **k):
        raise ValueError("no convergence")
    monkeypatch.setattr(m, "_ets_fit", boom)
    assert list(m.ets_add_weekly(_pattern(28), 7)) == list(m.weekday_mean(_pattern(28), 7))


def test_eligibility_gates():
    assert m.eligible("ets_add_weekly", n=18, classification="smooth", min_days_ets=21) is False
    assert m.eligible("ets_add_weekly", n=28, classification="smooth", min_days_ets=21) is True
    assert m.eligible("ets_add_weekly", n=60, classification="intermittent", min_days_ets=21) is False
    assert m.eligible("croston_tsb", n=28, classification="smooth", min_days_ets=21) is False
    assert m.eligible("croston_tsb", n=28, classification="lumpy", min_days_ets=21) is True
    assert m.eligible("seasonal_naive", n=7, classification="smooth", min_days_ets=21) is True
    assert m.eligible("seasonal_naive", n=6, classification="smooth", min_days_ets=21) is False
    assert m.eligible("weekday_mean", n=28, classification="erratic", min_days_ets=21) is True
