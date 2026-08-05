"""UTC instant windows - the partition-key predicate that hot queries have to carry.

`log_entries`, `log_transactions` and `log_entry_assignment` are range-partitioned by UTC day.
PostgreSQL prunes partitions from a predicate on the partition-key COLUMN and from nothing else: it
does not reason through `date_trunc`, it cannot derive `started_at` from `log_transactions.date`
(which is a customer-LOCAL day), and it cannot derive `log_entries.timestamp` from a join on
`entry_id`. So a query that filters on any of those still opens all 60 partitions.

This module owns ONLY the arithmetic of turning what a caller already knows into such a predicate.
It runs no queries and touches no models beyond the column it is handed, which is what keeps the
"is this window wide enough?" question - the one that can silently drop rows - answerable in one
place and testable without a database.

Two things here are load-bearing and easy to get wrong:

*Half-open, never inclusive.* `[start, end)` means adjacent windows tile without overlapping, so a
row can never be counted twice by two neighbouring days. The constructors therefore push `end` past
the last instant they were given rather than using it directly.

*NULL is not "outside the window".* A range predicate is FALSE for NULL, so an entry whose timestamp
could not be parsed - which lives in the DEFAULT partition - is dropped by a naive bound with no
error raised anywhere. `covers(..., include_null=True)` adds the branch back; callers that can prove
their rows are never NULL pass False and keep the DEFAULT partition out of the plan.
"""

from dataclasses import dataclass
from datetime import date as date_type, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import ColumnElement, and_, or_, true

# The end bound is exclusive, so it has to sit strictly after the newest instant a caller cares
# about. Timestamps are stored with microsecond resolution, so one microsecond is the smallest step
# that is guaranteed to clear it.
_TICK = timedelta(microseconds=1)

# Default pad for a window derived from a customer-LOCAL date.
#
# `log_transactions.date` was computed by converting `started_at` through whatever display timezone
# the customer had AT THE TIME the row was written. If that timezone is later changed, the stored
# local date and a window derived from the NEW zone stop lining up, and a zero-pad window can sit
# entirely beside the rows it is meant to select - the day view would just go blank. Real UTC offsets
# span UTC-12 to UTC+14, so no timezone change can move an instant by more than 26 hours; 27 on
# each side clears that with a margin. The cost is about five partitions instead of one, against
# sixty today.
_LOCAL_DATE_PAD = timedelta(hours=27)


@dataclass(frozen=True)
class UtcWindow:
    """A half-open `[start, end)` range of UTC instants. Either side may be None (unbounded).

    An open side is deliberate rather than defensive: the date-range delete accepts an open end, and
    inventing a bound for the missing side would change which rows are deleted. An open side simply
    prunes less.
    """

    start: datetime | None
    end: datetime | None

    def _range_predicates(self, column):
        """One predicate per side that is actually bounded - an open side simply contributes none.

        Yielded rather than returned so `covers` stays a single expression: the empty case (a window
        open at both ends) then folds into the `true()` seed instead of needing its own branch.
        """
        if self.start is not None:
            yield column >= self.start
        if self.end is not None:
            yield column < self.end

    def covers(self, column, *, include_null: bool) -> ColumnElement[bool]:
        """The predicate placing `column` inside this window.

        `include_null` decides whether rows with a NULL partition key match. Pass True wherever such
        rows are legitimate results - an entry with an unparsable timestamp still belongs to its
        transaction and still has to render.
        """
        inside = and_(true(), *self._range_predicates(column))
        return or_(inside, column.is_(None)) if include_null else inside


def _as_utc(dt: datetime) -> datetime:
    """A naive value out of asyncpg is a UTC instant. Reading it as local time would shift the window
    by the host's offset and cut real rows out of the result."""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _zone(tz_name: str) -> ZoneInfo:
    """A malformed timezone on a customer row must not 500 the feed; UTC is the safe fallback, and
    the generous pad below keeps the window correct anyway."""
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError, KeyError):
        return ZoneInfo("UTC")


def _local_midnight_utc(day: date_type | None, zone: ZoneInfo, *,
                        plus_days: int, shift: timedelta) -> datetime | None:
    """Local midnight `plus_days` after `day`, as a UTC instant, moved by `shift`. None passes through.

    Going through `ZoneInfo` rather than adding a fixed 24 hours is what makes a DST-shortened day 23
    hours and a lengthened one 25; a hard-coded day length is wrong for every date after a transition.
    """
    if day is None:
        return None
    midnight = datetime.combine(day + timedelta(days=plus_days), datetime.min.time(), tzinfo=zone)
    return midnight.astimezone(timezone.utc) + shift


def from_instants(instants, *, pad: timedelta = timedelta(0)) -> UtcWindow | None:
    """The window spanned by `instants`, ignoring Nones. None when there is nothing to bound.

    Returning None rather than an empty window is what lets a caller fall back to an unbounded query:
    a transaction with no `started_at` at all must still render its entries, and a window of zero
    width would filter every one of them out instead.
    """
    known = [_as_utc(i) for i in instants if i is not None]
    if not known:
        return None
    return UtcWindow(start=min(known) - pad, end=max(known) + pad + _TICK)


def from_local_dates(date_from: date_type | None, date_to: date_type | None, tz_name: str, *,
                     pad: timedelta = _LOCAL_DATE_PAD) -> UtcWindow | None:
    """The UTC instants covered by a customer-LOCAL calendar date range. None when both ends are open.

    Built by converting local midnight on each boundary through `ZoneInfo`, so a DST-shortened day is
    23 hours and a lengthened one is 25 - the arithmetic that hard-coding 24 hours gets wrong for
    every day after a transition.
    """
    if date_from is None and date_to is None:
        return None
    zone = _zone(tz_name)
    return UtcWindow(
        start=_local_midnight_utc(date_from, zone, plus_days=0, shift=-pad),
        # local midnight at the START of the following day - the exclusive end of `date_to`.
        end=_local_midnight_utc(date_to, zone, plus_days=1, shift=pad),
    )
