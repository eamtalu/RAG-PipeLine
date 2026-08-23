"""S1. Sealing as an explicit operation, instead of a side effect of re-insertion.

Why this exists
---------------
`sealed` was written in exactly one place: `_write_transaction` (`derive_transactions.py:611`) from
`_is_sealed` (`:542-553`). No statement anywhere UPDATEd it. So a transaction became sealed only
because some later rebuild happened to reconstruct it and decide so.

`regroup_window` frees transactions anchored in `[lo - pad, hi]`, so once `lo_p` advances past a row's
`started_at` (about 960 s) nothing re-derives it and it can never seal. Measured on the deployed
database: 2,516 rows permanently unsealed, oldest 2026-08-06. Two silent consequences - Stage 2's own
"never recompute a sealed row" optimisation was reasoning from a flag that had got stuck, and
`stability.py`'s `incomplete AND sealed` alert, which its docstring calls the genuinely useful one,
could never fire for those rows.

This module makes it a first-class UPDATE, so a row seals because it is time, not because something
happened to touch it.

The rule is NOT reimplemented here
----------------------------------
`_is_sealed` stays the authority for the rebuild path. This module expresses the SAME rule in SQL, and
`tests/test_stage2_sealer_chunk53.py` runs a matrix through both and asserts they agree - because a
row whose sealed-ness depended on which path touched it last would be worse than one that never seals.

The one subtlety is NULL status. `_is_sealed` sends anything that is not `incomplete` down the
terminal branch, including a NULL. Plain `status != 'incomplete'` would evaluate to NULL and quietly
match nothing, so `is_distinct_from` is used to reproduce the Python exactly. The column is NOT NULL
today; this costs nothing and means the two cannot drift apart if that ever changes.

Two clocks, deliberately
------------------------
The seal and abandon cutoffs come from `_cutoffs`, measured against the tenant's NEWEST ENTRY - the
log's notion of "now" - so back-dated and batch ingestion seal correctly, and one tenant's stale logs
are not dragged forward by another's live stream.

The HORIZON is measured on the DATABASE clock. What it guards against is retention dropping the
partition, and retention uses `db_today`. A tenant whose logs stopped 90 days ago has a `max_entry_ts`
90 days back, so a log-clock horizon would sit 150 days back and the sealer would reach into
partitions that no longer exist.

Why the tenant list is not the stitch queue
-------------------------------------------
The plan proposed hanging this off `log_stitch_worker`, which iterates `customers_with_due_work()` -
tenants with an open `log_regroup_pending` row. That would not have worked: the 2,516 rows are stuck
*because* nothing tickets them any more, so enumerating by ticket would leave the sealer unable to
reach the rows it exists to fix. It enumerates by unsealed rows instead, which is what
`ix_log_transactions_unsealed` is for.
"""

import logging
from datetime import datetime, timedelta

from sqlalchemy import and_, distinct, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.mnp_log_ingestion.pipeline.derive_transactions import _cutoffs
from app.settings import settings

logger = logging.getLogger(__name__)


async def horizon_floor(db: AsyncSession) -> datetime:
    """The oldest `ended_at` the sealer will touch, on the DATABASE clock.

    Read from the database rather than computed in Python for the same reason `db_today` is: partition
    bounds are cut on the database's notion of time, and the two machines' clocks in this project have
    already drifted far enough to make tests flap.
    """
    return await db.scalar(
        select(func.clock_timestamp() - timedelta(days=settings.log_seal_horizon_days)))


def _due(seal_cutoff: datetime, abandon_cutoff: datetime, horizon: datetime):
    """The SQL twin of `_is_sealed`, plus the horizon bound.

    Ordered so the cheap, highly selective terms come first: `NOT sealed` matches 2.1% of rows and is
    the index predicate, so it prunes before anything else is evaluated.
    """
    return (
        LogTransaction.sealed.is_(False),
        LogTransaction.ended_at.isnot(None),
        LogTransaction.ended_at >= horizon,
        or_(
            # terminal (or, defensively, NULL) status: the short window
            and_(LogTransaction.status.is_distinct_from(LogTransactionStatus.incomplete),
                 LogTransaction.ended_at < seal_cutoff),
            # incomplete: the long one. Never split a slow request.
            and_(LogTransaction.status == LogTransactionStatus.incomplete,
                 LogTransaction.ended_at < abandon_cutoff),
        ),
    )


async def customers_needing_seal(db: AsyncSession, limit: int | None = None) -> list[str]:
    """Tenants that plausibly have something to seal.

    A CANDIDATE filter, not the authority: the real per-tenant cutoffs live on the log clock and cannot
    be computed for every tenant in one statement without joining `max(log_entries.timestamp)` per
    tenant, which is the expensive query this whole design avoids. `seal_customer` applies the exact
    rule, so a tenant that slips through here simply seals nothing.

    The prefilter uses the seal window on the DATABASE clock. It can therefore be one tick late for a
    tenant whose log clock runs ahead of the database's, which is a delay rather than a miss: the row
    becomes eligible as the database clock advances. It cannot be early, which is the direction that
    would matter.
    """
    cap = limit if limit is not None else settings.log_stitch_max_customers_per_tick
    horizon = await horizon_floor(db)
    ceiling = await db.scalar(
        select(func.clock_timestamp() - timedelta(seconds=settings.log_seal_window_seconds)))
    return list((await db.execute(
        select(distinct(LogTransaction.customer_code)).where(
            LogTransaction.sealed.is_(False),
            LogTransaction.ended_at.isnot(None),
            LogTransaction.ended_at >= horizon,
            LogTransaction.ended_at < ceiling,
        ).limit(cap)
    )).scalars().all())


async def seal_customer(db: AsyncSession, customer_code: str) -> int:
    """Seal everything due for one tenant. Returns how many rows changed. Does NOT commit.

    Idempotent by construction: `NOT sealed` is part of the predicate, so a second run over the same
    range matches nothing. That matters more than it sounds - this runs on a tick, and a version that
    rewrote the tail every time would have made S1 a write amplifier rather than a fix.

    `updated_at` is bumped and `created_at` is deliberately NOT touched. Refreshing `created_at` would
    also have made the alert fire, by putting the row back in the cursor's old feed, but it would
    reassert the "last rebuilt" meaning that S3 exists to remove, and the analytics frontier reads it -
    so a 60-day-old row sealing today would jump the frontier to now.
    """
    seal_cutoff, abandon_cutoff = await _cutoffs(db, customer_code)
    if seal_cutoff is None:
        return 0        # the tenant has no entries at all, so it has no notion of "now" yet
    horizon = await horizon_floor(db)
    result = await db.execute(
        update(LogTransaction)
        .where(LogTransaction.customer_code == customer_code,
               *_due(seal_cutoff, abandon_cutoff, horizon))
        .values(sealed=True, updated_at=func.clock_timestamp())
        .execution_options(synchronize_session=False))
    return result.rowcount or 0


async def seal_due(limit: int | None = None) -> dict:
    """One sealer pass across every tenant with work. Per-tenant failures are isolated.

    Each tenant gets its own short-lived session and its own transaction, holding the same
    `pg_advisory_xact_lock(hashtext(customer_code))` that `finalize_pending` takes. Without that lock a
    rebuild could be deciding `sealed` for a row from `_is_sealed` while this statement decides it from
    SQL, and the loser's answer would win by arriving second.

    The lock is per tenant, so one tenant's sealing never blocks another's - and because it is an
    `xact` lock it is released by the commit rather than needing to be tracked.
    """
    stats = {"customers": 0, "sealed": 0, "failed": 0}
    async with async_session() as db:
        codes = await customers_needing_seal(db, limit)
    for cc in codes:
        try:
            async with async_session() as db:
                await db.execute(select(func.pg_advisory_xact_lock(func.hashtext(cc))))
                sealed = await seal_customer(db, cc)
                await db.commit()
            stats["customers"] += 1
            stats["sealed"] += sealed
        except Exception:
            stats["failed"] += 1
            logger.exception(
                "Sealer: failed for %s - its rows stay unsealed and are retried next tick; "
                "other tenants are unaffected", cc)
    return stats


async def overdue_count() -> int:
    """Rows that should be sealed and are not, across all tenants - the standing alarm.

    Sealing failure was previously unmonitorable: nothing wrote the flag, so nothing could report that
    it had stopped. This is the number that should trend to zero from 2,516 and stay there. A value
    that climbs means the sealer is not running, or is failing for a tenant every tick.

    Uses the database clock for both bounds, so it is an approximation of the per-tenant rule in the
    same direction as `customers_needing_seal`.
    """
    async with async_session() as db:
        horizon = await horizon_floor(db)
        ceiling = await db.scalar(
            select(func.clock_timestamp()
                   - timedelta(seconds=settings.log_abandon_window_seconds)))
        return await db.scalar(
            select(func.count()).select_from(LogTransaction).where(
                LogTransaction.sealed.is_(False),
                LogTransaction.ended_at.isnot(None),
                LogTransaction.ended_at >= horizon,
                LogTransaction.ended_at < ceiling)) or 0
