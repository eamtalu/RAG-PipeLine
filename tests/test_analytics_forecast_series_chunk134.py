"""Chunk 134: the pure series helpers under the demand forecast.

Everything here is arithmetic on dates and numbers, no database and no clock. The pins are the
ones a wrong forecast would trace back to: a missing day must count as zero rather than vanish, a
Sunday belongs to the ISO week that started on the Monday before it, a rollout ramp is trimmed from
the front and only from the front, and a target instant is the tenant's local midnight even across
the clock change.
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app.services.analytics_forecast import series as s

LONDON = ZoneInfo("Europe/London")


def _flat(start: date, days: int, weekday: float = 1000.0, weekend: float = 500.0) -> s.DailySeries:
    values = [weekend if (start + timedelta(days=i)).weekday() >= 5 else weekday for i in range(days)]
    return s.DailySeries(start=start, values=tuple(values))


# ==================================================== 1. dense daily

def test_dense_daily_fills_missing_days_with_zero_and_keeps_order():
    rows = [(date(2026, 9, 16), 3.0), (date(2026, 9, 14), 1.0), (date(2026, 9, 14), 2.0)]
    out = s.dense_daily(rows, start=date(2026, 9, 14), end=date(2026, 9, 17))
    assert out.start == date(2026, 9, 14)
    assert out.values == (3.0, 0.0, 3.0, 0.0)
    assert out.dates() == [date(2026, 9, 14) + timedelta(days=i) for i in range(4)]


def test_dense_daily_ignores_rows_outside_the_window():
    out = s.dense_daily([(date(2026, 9, 1), 9.0), (date(2026, 9, 30), 9.0)],
                        start=date(2026, 9, 14), end=date(2026, 9, 15))
    assert out.values == (0.0, 0.0)


# ==================================================== 2. weeks and months

def test_sunday_belongs_to_the_iso_week_that_began_the_monday_before():
    ser = _flat(date(2026, 9, 28), 7)  # Mon 28 Sep .. Sun 4 Oct
    weeks = s.aggregate_weeks(ser, as_of=date(2026, 10, 4))
    assert len(weeks) == 1
    assert weeks[0].start == date(2026, 9, 28) and weeks[0].end == date(2026, 10, 4)
    assert weeks[0].total == 5 * 1000 + 2 * 500
    assert weeks[0].complete is True


def test_the_week_containing_as_of_is_not_complete():
    ser = _flat(date(2026, 9, 28), 10)  # through Wed 7 Oct
    weeks = s.aggregate_weeks(ser, as_of=date(2026, 10, 7))
    assert [w.complete for w in weeks] == [True, False]
    assert weeks[1].start == date(2026, 10, 5) and weeks[1].total == 3 * 1000


def test_months_aggregate_on_calendar_boundaries():
    ser = _flat(date(2026, 9, 29), 4)  # 29, 30 Sep, 1, 2 Oct
    months = s.aggregate_months(ser, as_of=date(2026, 10, 2))
    assert [(m.start, m.end, m.complete) for m in months] == [
        (date(2026, 9, 1), date(2026, 9, 30), True), (date(2026, 10, 1), date(2026, 10, 31), False)]
    assert months[0].total == 2000.0 and months[1].total == 2000.0


# ==================================================== 3. classification

def test_daily_demand_is_smooth_and_sparse_demand_is_intermittent():
    assert s.classify((10, 12, 11, 10, 13, 12, 11, 10, 12, 11, 10, 12, 11, 10)) == "smooth"
    assert s.classify((0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0)) == "intermittent"


def test_fewer_than_two_hits_is_insufficient_and_wild_sizes_are_lumpy():
    assert s.classify((0, 0, 0, 5, 0, 0)) == "insufficient"
    assert s.classify((0, 0, 100, 0, 0, 0, 1, 0, 0, 0, 0, 50, 0, 0, 0, 0, 0, 1)) == "lumpy"


# ==================================================== 4. ramp trim

def test_steady_start_skips_a_rollout_ramp_at_the_front():
    start = date(2026, 9, 3)
    ramp = [155, 0, 13, 124, 161, 117, 498, 400, 115, 36]  # 3 Sep .. 12 Sep, as tmp-live saw it
    steady = [708, 895, 740, 1211, 961, 1369, 449, 458, 1234, 864, 1042, 865, 1355, 520, 571, 1036]
    ser = s.DailySeries(start=start, values=tuple(float(v) for v in ramp + steady))
    assert s.steady_start(ser) == date(2026, 9, 13)


def test_steady_start_of_a_flat_series_is_its_first_day_and_a_later_dip_is_not_a_ramp():
    ser = _flat(date(2026, 9, 14), 28)
    assert s.steady_start(ser) == date(2026, 9, 14)
    values = list(ser.values)
    values[14] = 10.0  # one bank holiday in the middle
    assert s.steady_start(s.DailySeries(start=ser.start, values=tuple(values))) == date(2026, 9, 14)


def test_steady_start_of_an_all_zero_series_is_its_first_day():
    ser = s.DailySeries(start=date(2026, 9, 14), values=(0.0,) * 10)
    assert s.steady_start(ser) == date(2026, 9, 14)


# ==================================================== 5. horizons and targets

def test_horizon_labels():
    assert s.horizon_label("day", 5) == "5d"
    assert s.horizon_label("week", 0) == "0w"
    assert s.horizon_label("month", 2) == "2m"
    with pytest.raises(ValueError):
        s.horizon_label("fortnight", 1)


def test_daily_targets_are_local_midnights_across_the_clock_change():
    # BST ends 2026-10-25 01:00 UTC. Midnight 25 Oct is 23:00Z on the 24th; midnight 26 Oct is 00:00Z.
    out = s.targets(as_of=date(2026, 10, 23), grain="day", n=3, tz=LONDON)
    assert [t.label for t in out] == ["1d", "2d", "3d"]
    assert [t.start for t in out] == [date(2026, 10, 24), date(2026, 10, 25), date(2026, 10, 26)]
    assert all(t.start == t.end for t in out)
    assert out[1].target_at == datetime(2026, 10, 24, 23, 0, tzinfo=timezone.utc)
    assert out[2].target_at == datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc)


def test_week_zero_is_the_week_containing_the_day_after_as_of():
    # as_of Sunday 4 Oct: the job ran on Monday morning, so week 0 is the week that just began.
    out = s.targets(as_of=date(2026, 10, 4), grain="week", n=2, tz=LONDON)
    assert [(t.label, t.start, t.end) for t in out] == [
        ("0w", date(2026, 10, 5), date(2026, 10, 11)), ("1w", date(2026, 10, 12), date(2026, 10, 18))]


def test_month_targets_cover_whole_calendar_months():
    out = s.targets(as_of=date(2026, 10, 1), grain="month", n=3, tz=LONDON)
    assert [(t.label, t.start, t.end) for t in out] == [
        ("0m", date(2026, 10, 1), date(2026, 10, 31)), ("1m", date(2026, 11, 1), date(2026, 11, 30)),
        ("2m", date(2026, 12, 1), date(2026, 12, 31))]
    assert out[0].target_at == datetime(2026, 9, 30, 23, 0, tzinfo=timezone.utc)
