"""Incremental reading of `log_transactions` — where a rule stopped, and what is safe to read next.

The rule engine used to re-read the last hour on every 10 s tick with no memory of the previous run
(`engine.py`, before this change). The same rows were loaded and evaluated ~8,600 times a day, and
dedupe discarded the results only afterwards. Anything older than the lookback, or past the row cap,
was never seen at all.

A cursor is a bookmark: one timestamp per rule saying "I have read up to here".

Three ideas make it correct, and they are not interchangeable:

**Read on `updated_at`, never `started_at`.** `started_at` is when the log line happened;
`updated_at` is when the row was last written. A week-old file backfilled today has an old
`started_at` but a new `updated_at`, so a cursor on `started_at` silently skips it - the same bug as
the old 1-hour lookback.

**It was `created_at` until S1, and the change is load-bearing.** Sealing used to happen only as a
side effect of re-insertion, so a row that changed always got a fresh `created_at` and re-entered this
feed by itself. S1 made sealing an explicit UPDATE, which does not refresh `created_at` - so a sealed
row would have fallen permanently behind the cursor and `stability.py`'s `incomplete AND sealed`
alert, the one its docstring calls genuinely useful, could never have fired. Reading `updated_at`
instead is what keeps "a row that changed is a row this feed sees" true. S3 removes
delete-and-reinsert entirely, at which point this is the ONLY column that still moves.

**Lag behind the present.** `updated_at` is stamped when Python BUILDS or UPDATEs the row, not when
Postgres COMMITS it. A long Stage 2 transaction can therefore commit a row whose timestamp already
sits behind a cursor that has moved past it - and that row is never read again. Dedupe cannot help: it
prevents duplicates, it cannot recover something never seen. Staying `lag` behind the present
guarantees everything that could write into a range has committed.

**Left-inclusive windows.** `[lo, hi)` with `lo` inclusive, so a row landing exactly on a boundary is
read twice rather than zero times. The invariant this module exists to hold is:

    NO ROW IS EVER SKIPPED. A row MAY be read more than once.

Dedupe absorbs a repeat. Nothing absorbs a skip. Every trade-off below resolves in that direction.

This module owns only the arithmetic and the query. Deciding whether a transaction deserves an alert
belongs to the evaluators; sending belongs to the dispatcher.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.notification import NotificationRule
from app.settings import settings

logger = logging.getLogger(__name__)

#: Smallest step that guarantees forward progress past a timestamp, matching the storage resolution.
_TICK = timedelta(microseconds=1)


@dataclass(frozen=True)
class Window:
    """A half-open `[lo, hi)` range of WRITE times that is safe to read now."""

    lo: datetime
    hi: datetime

    #: Stated rather than implied: `lo` is INCLUSIVE, which is what makes a boundary row get read
    #: twice instead of zero times. Callers assert on this so the choice cannot be silently inverted.
    includes_lower_bound: bool = True


def read_window(cursor: datetime | None, *, now: datetime, lag: timedelta,
                lookback: timedelta) -> Window | None:
    """What this rule may safely read, or None when nothing is readable yet.

    A NULL cursor means "never run", not "read all history" — it starts at `now - lookback`, which is
    exactly the window the engine used before cursors existed. Activating a rule therefore behaves as
    it always has, rather than replaying everything ever ingested.

    None (rather than an empty window) when the lag has not elapsed, so a freshly created rule does
    not issue a pointless query on every tick.
    """
    lo = cursor if cursor is not None else now - lookback
    hi = now - lag
    return Window(lo=lo, hi=hi) if hi > lo else None


def read_window_for_group(cursors, *, now: datetime, lag: timedelta,
                          lookback: timedelta) -> Window | None:
    """One window covering several rules — the OLDEST cursor among them.

    Rules belonging to one customer are read with a single query rather than one query each. That is
    a measured decision, not a stylistic one: planning a query against this partitioned table costs
    roughly 50 ms because the planner considers every partition before pruning, so issuing one per
    rule would multiply that by the rule count. Execution is sub-millisecond either way.

    Starting from the oldest cursor is what keeps it lossless — a rule that is behind would otherwise
    never see the rows between its own position and its neighbours'. Rules ahead of that point filter
    the surplus out in memory via `is_after`, which costs nothing.
    """
    resolved = [c if c is not None else now - lookback for c in cursors]
    return read_window(min(resolved), now=now, lag=lag, lookback=lookback) if resolved else None


def is_after(updated_at: datetime, cursor: datetime | None) -> bool:
    """Whether a row is new to a rule sitting at `cursor`.

    The in-memory half of sharing one query: rows fetched on behalf of a rule that is behind must not
    re-alert a rule that has already passed them. Inclusive of the cursor itself, matching the
    window's inclusive lower bound — a boundary row is re-evaluated and dropped by dedupe.
    """
    return cursor is None or updated_at >= cursor


def advance(window: Window, *, rows_read: int, limit: int,
            newest_seen: datetime | None) -> datetime:
    """Where the cursor moves after reading `window`.

    Two cases, and conflating them loses data:

    *Window fully consumed* (fewer rows than the limit) — jump to `hi`. This is also what lets an idle
    rule make progress; without it the window would grow without bound and every tick would get
    slower.

    *Window truncated* (the limit was hit) — advance only to the newest row actually read. Jumping to
    `hi` here would skip everything between the last row read and the end of the window, permanently.

    The pathological case is `limit` rows sharing one timestamp, where the truncated branch cannot
    move at all and the rule would re-read the same page forever, silently alerting on nothing new.
    Nudging past it can skip a tied row, so it is logged CRITICAL rather than done quietly — a stalled
    rule and a skipped row are both bad, and only one of them is visible without being told.
    """
    if rows_read < limit or newest_seen is None:
        return window.hi
    if newest_seen > window.lo:
        return newest_seen
    logger.critical(
        "Notification cursor could not advance: %d rows (the full batch limit) all share the "
        "timestamp %s. Stepping past it to avoid stalling, which may skip rows at that exact "
        "instant. Raise the batch limit if this recurs.", rows_read, window.lo.isoformat())
    return window.lo + _TICK


def window_stmt(customer_code: str, window: Window, *, limit: int, extra=None):
    """The SELECT for one customer's window.

    `extra` is an optional list of additional criteria the CALLER supplies. This module stays
    deliberately ignorant of what they mean: it is generic incremental-read machinery that ML feature
    extraction and analytics are expected to reuse, so one consumer's semantics — notifications only
    wanting settled transactions, say — must not be baked in here.

    Ordered by `updated_at` ASCENDING, which `advance` depends on: it moves the cursor to the newest
    row read, and that is only sound if the batch is a contiguous prefix of the window. Newest-first
    would make a truncated read jump the cursor past everything it did not fetch.

    Exposed as a statement so a test can assert the ordering and EXPLAIN the plan.
    """
    return (
        select(LogTransaction)
        .where(
            LogTransaction.customer_code == customer_code,
            LogTransaction.updated_at >= window.lo,   # inclusive - see the module docstring
            LogTransaction.updated_at < window.hi,
            *(extra or []),
        )
        .order_by(LogTransaction.updated_at.asc())
        .limit(limit)
    )


# ============================================================== rule-level helpers
def _lag() -> timedelta:
    return timedelta(seconds=settings.notification_cursor_lag_seconds)


def _lookback() -> timedelta:
    return timedelta(seconds=settings.notification_lookback_seconds)


def last_window(rule: NotificationRule, *, now: datetime | None = None) -> Window | None:
    """The window this rule would read right now. Shared by the fetch and the advance so the two can
    never disagree about which range was covered."""
    return read_window(rule.cursor_at, now=now or datetime.now(timezone.utc),
                       lag=_lag(), lookback=_lookback())


async def fetch_for_rule(db: AsyncSession, rule: NotificationRule, *,
                         now: datetime | None = None,
                         limit: int | None = None,
                         extra=None) -> list[LogTransaction]:
    """Transactions this rule has not read yet. Empty when there is nothing safely readable.

    Convenience for one rule; the engine reads a whole customer at once via `window_stmt` and filters
    with `is_after`, for the planning-cost reason in `read_window_for_group`.
    """
    window = last_window(rule, now=now)
    if window is None:
        return []
    n = limit if limit is not None else settings.notification_candidate_limit
    return list((await db.execute(
        window_stmt(rule.customer_code, window, limit=n, extra=extra))).scalars().all())


def _newest(rows: list[LogTransaction]) -> datetime | None:
    """Newest write time in a batch, ignoring rows that somehow carry none.

    Reads `updated_at`, the column `window_stmt` ORDERS BY. Reading a different one would let the
    cursor advance past rows it never fetched, which is the single thing this module must never do.
    """
    return max((r.updated_at for r in rows if r.updated_at is not None), default=None)


def _forward_only(current: datetime | None, proposed: datetime) -> datetime:
    """A cursor only ever moves forward.

    A truncated batch advances to the newest row READ, which can sit behind a rule that was already
    further ahead — several rules share one query starting at the OLDEST cursor among them, so this is
    normal rather than exceptional. Moving such a rule back would re-evaluate rows it has already
    handled; dedupe would catch the alerts, but only by accident.
    """
    return proposed if current is None else max(current, proposed)


def advance_rule(db: AsyncSession, rule: NotificationRule, *, window: Window | None,
                 rows: list[LogTransaction], limit: int | None = None) -> None:
    """Move the rule's bookmark past the window it just read. No-op when there was no window.

    Deliberately SYNCHRONOUS: it only mutates the ORM object and stages it. Making it a coroutine
    would add nothing and would let a missing `await` silently skip every cursor advance — which is
    exactly the bug this shape prevents.

    Does not commit — the caller owns the transaction boundary, so the cursor advances in the same
    transaction as the events published from those rows. Advancing separately would risk a cursor
    that has moved past events which were never persisted.
    """
    if window is None:
        return
    n = limit if limit is not None else settings.notification_candidate_limit
    moved = advance(window, rows_read=len(rows), limit=n, newest_seen=_newest(rows))
    rule.cursor_at = _forward_only(rule.cursor_at, moved)
    db.add(rule)
