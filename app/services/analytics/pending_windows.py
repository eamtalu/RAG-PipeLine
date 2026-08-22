"""N1, the ticket publisher: "a bounded event-time range of this tenant's transactions changed".

Phase 2 of docs/analytics-ml-architecture/final_architecture.md.

The whole analytics platform rests on invariant 2: **no transaction is deleted by any path without a
committed ticket whose range contains its `started_at`.** The range diff can only correct a window it is
told to look at, so a path that removes rows without a ticket leaves their contribution in every total
permanently. And once the raw entries are dropped at 60 days there is nothing left to recount against,
so the error is not merely undetected, it becomes unprovable.

**This module never commits.** Invariant 3: the ticket and the change commit in the SAME transaction.
That is what makes the pair atomic in both directions -- a rolled-back rebuild takes its ticket with it,
and a committed change cannot outlive its ticket. Committing here would break both halves, so `publish`
takes the caller's session and leaves the boundary alone.

**Insert-only, and provably constraint-free** (A3). The row is written inside the ingestion or rebuild
transaction, so a failed insert here fails INGESTION. No foreign key, no unique constraint a retry could
violate, no trigger. The table shape enforces that and a Phase 1 test asserts it.

Two things about bounds are load-bearing.

*Bounds come from the rows actually REMOVED, never from the rows arriving* (F1). `regroup_incremental`
deletes `WHERE sealed IS FALSE` with no time predicate, so a bound inferred from incoming entries would
miss an older unsealed row caught in the same sweep, and that row's contribution would drift for good.

*They are padded by the same amount Stage 2 uses to call a window lossless.* A rebuild can produce a
transaction whose `started_at` sits slightly outside the span of what was freed, because it absorbed an
earlier entry. A ticket narrower than the rebuild it describes is invariant 2 violated by a few seconds,
which is the hardest kind to notice.
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow

logger = logging.getLogger(__name__)

#: A span wider than this is split into one ticket per day.
#:
#: `regroup_all` frees a tenant's entire history, and one ticket spanning 60 days would have the worker
#: read 60 days of transactions inside a single transaction and a single advisory lock. The plan's own
#: alternative is "one ticket per day of the span", which keeps each unit of work bounded and lets a
#: poison day fail in isolation. Cheap either way: the ticket table is built for many small rows, and
#: `log_regroup_pending` already carries thousands.
_MAX_TICKET_SPAN = timedelta(days=1)


def _pad() -> timedelta:
    """The pad Stage 2 uses to decide a window rebuild is lossless.

    Imported lazily to keep the import graph one-directional: Stage 2 publishes tickets, so a
    module-level import here would make the two mutually dependent.
    """
    from app.services.mnp_log_ingestion.pipeline.derive_transactions import _regroup_pad
    return _regroup_pad()


async def publish(db: AsyncSession, customer_code: str, *, lo: datetime, hi: datetime,
                  job_id: uuid.UUID | None = None) -> int:
    """Record that `[lo, hi]` changed for this tenant. Returns how many tickets were written.

    Pads the range and splits it into at most one ticket per day. Does NOT commit: the caller's
    transaction boundary is what makes the ticket atomic with the change it describes.
    """
    pad = _pad()
    start, end = lo - pad, hi + pad

    written = 0
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + _MAX_TICKET_SPAN, end)
        db.add(AnalyticsPendingWindow(customer_code=customer_code, job_id=job_id,
                                      range_start=cursor, range_end=chunk_end))
        written += 1
        if chunk_end >= end:
            break
        cursor = chunk_end
    return written


async def publish_for_transactions(db: AsyncSession, customer_code: str, *, started_ats,
                                  job_id: uuid.UUID | None = None) -> int:
    """Publish for a set of transactions being removed or rebuilt, given their `started_at` values.

    `started_ats` is whatever the caller selected BEFORE deleting -- ids alone are not enough, which is
    why the callers that only had ids now select the instant too.

    Nothing to describe means nothing published: a ticket for an empty freed set is one the worker must
    claim, lock, diff and consume only to discover that nothing moved.

    A NULL `started_at` is not skipped. A transaction all of whose entries lack a parsable timestamp has
    none, and it still has to be diffed. It cannot be placed in a range, so it is covered by publishing
    a degenerate ticket at the current instant and relying on N3 reading every range with
    `include_null=True` (A7) -- which is what reaches the DEFAULT partition where such a fact lives.
    Silently dropping it would be the same class of bug as the range predicate that put 294,747 rows in
    a DEFAULT partition nobody was looking at.
    """
    instants = list(started_ats)
    if not instants:
        return 0

    real = [i for i in instants if i is not None]
    if not real:
        now = datetime.now(timezone.utc)
        logger.info("Analytics: %d freed transaction(s) for %s have no start instant; publishing a "
                    "degenerate ticket so the NULL bucket is still diffed", len(instants),
                    customer_code)
        return await publish(db, customer_code, lo=now, hi=now, job_id=job_id)

    return await publish(db, customer_code, lo=min(real), hi=max(real), job_id=job_id)
