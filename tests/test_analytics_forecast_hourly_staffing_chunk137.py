"""Chunk 137: the hour-of-day profile, picker throughput, and the staffing arithmetic.

The pins: a day's shares sum to one and the 08:00-14:00 lull stays near zero on a shift-shaped day,
the throughput is a median of lines per picker-hour with a sensible fallback, an hourly spread adds
back up to the daily figure it was cut from, and pickers needed rounds up with the buffer.
"""

from datetime import date, datetime, timedelta

import pytest

from app.services.analytics_forecast import hourly as h
from app.services.analytics_forecast import staffing as st

MON = date(2026, 9, 14)


def _shift_day(day: date, scale: float = 1.0) -> list[h.HourRow]:
    """A 14:00 -> 08:00 operation: 40 lines an hour with 2 pickers, nothing in the lull."""
    rows = []
    for hour in range(24):
        busy = hour >= 14 or hour < 8
        rows.append(h.HourRow(start=datetime(day.year, day.month, day.day, hour),
                              lines=40.0 * scale if busy else 0.0, pickers=2 if busy else 0))
    return rows


def _weeks(n: int) -> list[h.HourRow]:
    rows = []
    for i in range(7 * n):
        rows.extend(_shift_day(MON + timedelta(days=i), scale=0.5 if (MON + timedelta(days=i)).weekday() >= 5 else 1.0))
    return rows


def test_profile_shares_sum_to_one_and_the_lull_is_empty():
    share = h.profile(_weeks(3))
    for dow in range(7):
        assert sum(share[dow]) == pytest.approx(1.0)
        assert all(share[dow][hr] == 0 for hr in range(8, 14))
        assert share[dow][15] == pytest.approx(1 / 18)


def test_profile_of_a_day_type_with_no_data_borrows_the_overall_shape():
    rows = [r for r in _weeks(2) if r.start.weekday() != 2]  # no Wednesdays at all
    share = h.profile(rows)
    assert sum(share[2]) == pytest.approx(1.0)
    assert share[2][15] == pytest.approx(1 / 18)


def test_throughput_is_the_median_lines_per_picker_hour_with_a_global_fallback():
    rows = _weeks(2)
    tp = h.throughput(rows)
    assert tp.overall == pytest.approx(20.0)
    assert tp.at(dow=0, hour=15) == pytest.approx(20.0)
    assert tp.at(dow=0, hour=10) == pytest.approx(20.0)  # nothing ever happens at 10:00, so the fallback
    assert h.throughput([]).overall is None


def test_spread_adds_back_up_to_the_daily_figure():
    share = h.profile(_weeks(2))
    daily = [h.DayPoint(day=MON + timedelta(days=21), p10=900.0, p50=1000.0, p90=1200.0)]
    out = h.spread(daily, share)
    assert len(out) == 24
    assert sum(p.p50 for p in out) == pytest.approx(1000.0)
    assert sum(p.p10 for p in out) == pytest.approx(900.0)
    assert sum(p.p90 for p in out) == pytest.approx(1200.0)
    assert all(p.p50 == 0 for p in out if 8 <= p.hour < 14)
    assert out[0].day == MON + timedelta(days=21) and out[0].hour == 0


def test_pickers_needed_rounds_up_with_the_buffer():
    assert st.pickers_needed(100.0, 40.0, buffer_pct=0.10) == 3
    assert st.pickers_needed(80.0, 40.0, buffer_pct=0.0) == 2
    assert st.pickers_needed(0.0, 40.0, buffer_pct=0.10) == 0
    assert st.pickers_needed(100.0, None, buffer_pct=0.10) is None
    assert st.pickers_needed(100.0, 0.0, buffer_pct=0.10) is None
