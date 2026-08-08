"""Parse worker — drains the log_source_objects queue and runs Stage 1 on each downloaded range.

Why this exists: the SSH fetcher used to await parse+insert inside its byte-read loop, so the SSH
connection and the per-host advisory lock were held while the database worked, and a crash mid-parse
left the file orphaned with nothing recording that it still needed work. The fetcher now saves the
bytes, inserts a queue row and advances the checkpoint in ONE transaction, and walks away. This
worker picks the row up.

Retry semantics, and how they differ from Stage 2 (log_regroup_pending):

- Stage 2 retries a failing window on EVERY finalize tick with no delay, which hammers a degraded
  disk. Here each attempt backs off exponentially with jitter (`_backoff_seconds`).
- Stage 2 spends the full budget on every failure, including hopeless ones. Here failures are
  classified: a TRANSIENT failure (bad sector, statement timeout, dropped connection) consumes the
  budget, while a PERMANENT one (corrupt bytes, missing storage key) is abandoned on the first
  attempt, because retrying cannot help and three tries would just triple the log noise.
- Neither stage has lease recovery. Here a crashed worker's row returns to `pending` once its lease
  expires, and that counts as a consumed attempt so a crash-loop cannot spin forever.

Claiming uses `FOR UPDATE SKIP LOCKED`, so one poison row can never block the rest of the queue —
which is exactly what the old source-level circuit breaker did, disabling a whole server because of
one unparseable file.
"""

import asyncio
import logging
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, text as sa_text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.models.log_source_object import LogSourceObject, SourceObjectStatus
from app.persistence.storage.base import ObjectStorage
from app.persistence.storage.local import LocalStorage
from app.services.mnp_log_ingestion.io_errors import is_disk_io_error, disk_io_detail
from app.services.mnp_log_ingestion.LogIngestion import DOCUMENT_TYPE
from app.services.mnp_log_ingestion.pipeline.parse_insert import run_log_parse_insert
from app.services.queueing import retry_policy

logger = logging.getLogger(__name__)

# Backoff and failure classification live in app/services/queueing/retry_policy.py so BOTH durable
# queues (this one and the Stage 2 stitch queue) share exactly one implementation. Two copies would
# drift, and then the same failure would behave differently depending on which queue it landed in.
_is_transient = retry_policy.is_transient


def _worker_id() -> str:
    """Stable-ish identity for lease ownership, useful when reading the table by hand."""
    return f"{socket.gethostname()}:{os.getpid()}"


def _backoff_seconds(attempts: int) -> float:
    return retry_policy.backoff_seconds(
        attempts,
        base=settings.log_parse_backoff_base_seconds,
        cap=settings.log_parse_backoff_max_seconds,
    )


# ============================================================ claiming
async def claim_one(db: AsyncSession, worker_id: str) -> LogSourceObject | None:
    """Lease the oldest due pending row, or return None.

    `FOR UPDATE SKIP LOCKED` is what makes concurrent workers safe and keeps a stuck row from
    blocking the queue. `available_at <= now()` is what makes the backoff real, and the
    `status = 'pending'` predicate is what makes an abandoned row a genuine dead letter (the same
    exclusion Stage 2 does at derive_transactions.py:775).
    """
    lease_until = datetime.now(timezone.utc) + timedelta(seconds=settings.log_parse_lease_seconds)
    stmt = sa_text("""
        WITH candidate AS (
            SELECT id FROM log_source_objects
            WHERE status = 'pending' AND available_at <= clock_timestamp()
            ORDER BY available_at, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE log_source_objects q
        SET status = 'leased', lease_owner = :owner, lease_expires_at = :until
        FROM candidate
        WHERE q.id = candidate.id
        RETURNING q.id
    """)
    row_id = await db.scalar(stmt, {"owner": worker_id, "until": lease_until})
    if row_id is None:
        await db.rollback()
        return None
    obj = (await db.execute(
        select(LogSourceObject).where(LogSourceObject.id == row_id))).scalars().one()
    await db.commit()
    return obj


async def reclaim_expired_leases(db: AsyncSession) -> int:
    """Return rows whose worker died back to `pending`.

    The expiry counts as a consumed attempt, so a worker that reliably crashes on one row cannot
    spin on it forever; it walks the same budget down to `abandoned` as any other failure.
    """
    now = datetime.now(timezone.utc)
    rows = (await db.execute(
        select(LogSourceObject).where(
            LogSourceObject.status == SourceObjectStatus.leased,
            LogSourceObject.lease_expires_at.isnot(None),
            LogSourceObject.lease_expires_at < now,
        ).with_for_update(skip_locked=True)
    )).scalars().all()
    n = 0
    for obj in rows:
        obj.attempts = (obj.attempts or 0) + 1
        obj.lease_owner = None
        obj.lease_expires_at = None
        obj.last_error = "lease expired (worker died or stalled)"
        if obj.attempts >= (obj.max_attempts or settings.log_parse_max_attempts):
            obj.status = SourceObjectStatus.abandoned
            logger.critical(
                "Ingest queue: object %s (%s %s bytes %d-%d) ABANDONED after %d attempts — repeated "
                "lease expiry. Bytes remain at %s; re-arm with POST /logs/ingest-queue/reset-abandoned.",
                obj.id, obj.source_name, obj.remote_path, obj.start_offset, obj.end_offset,
                obj.attempts, obj.storage_key)
        else:
            obj.status = SourceObjectStatus.pending
            obj.available_at = (func.clock_timestamp()
                                + func.make_interval(0, 0, 0, 0, 0, 0,
                                                     _backoff_seconds(obj.attempts)))
        n += 1
    await db.commit()
    if n:
        logger.warning("Ingest queue: reclaimed %d expired lease(s)", n)
    return n


# ============================================================ outcomes
async def record_failure(db: AsyncSession, row_id: uuid.UUID, exc: BaseException) -> str:
    """Apply the retry policy to one failed row. Returns the resulting status."""
    obj = (await db.execute(
        select(LogSourceObject).where(LogSourceObject.id == row_id))).scalars().one()
    obj.attempts = (obj.attempts or 0) + 1
    # Store the FULL message: last_error is what an operator reads to diagnose, and the extracted
    # disk_io_detail summary throws away the file/block context they need. The short form is used
    # for the log line only.
    obj.last_error = (str(exc) or repr(exc))[:2000]
    short = disk_io_detail(exc) if is_disk_io_error(exc) else obj.last_error[:200]
    obj.lease_owner = None
    obj.lease_expires_at = None

    transient = _is_transient(exc)
    cap = obj.max_attempts or settings.log_parse_max_attempts

    if not transient:
        obj.status = SourceObjectStatus.abandoned
        logger.critical(
            "Ingest queue: object %s (%s %s bytes %d-%d) ABANDONED on attempt %d — PERMANENT failure "
            "(%s). Retrying cannot help. Bytes remain at %s; investigate, then re-arm with "
            "POST /logs/ingest-queue/reset-abandoned.",
            obj.id, obj.source_name, obj.remote_path, obj.start_offset, obj.end_offset,
            obj.attempts, short, obj.storage_key)
    elif obj.attempts >= cap:
        obj.status = SourceObjectStatus.abandoned
        logger.critical(
            "Ingest queue: object %s (%s %s bytes %d-%d) ABANDONED after %d failed attempts (%s). "
            "Bytes remain at %s; re-arm with POST /logs/ingest-queue/reset-abandoned.",
            obj.id, obj.source_name, obj.remote_path, obj.start_offset, obj.end_offset,
            obj.attempts, short, obj.storage_key)
    else:
        delay = _backoff_seconds(obj.attempts)
        obj.status = SourceObjectStatus.pending
        # Database clock, not the app host's — claim_one compares against clock_timestamp().
        obj.available_at = func.clock_timestamp() + func.make_interval(0, 0, 0, 0, 0, 0, delay)
        logger.warning(
            "Ingest queue: object %s failed (attempt %d/%d, %s) — retrying in %.0fs",
            obj.id, obj.attempts, cap, short, delay)
    status = obj.status
    await db.commit()
    return status


async def record_success(db: AsyncSession, row_id: uuid.UUID, job_id: uuid.UUID,
                         entries_inserted: int) -> None:
    obj = (await db.execute(
        select(LogSourceObject).where(LogSourceObject.id == row_id))).scalars().one()
    obj.status = SourceObjectStatus.ingested
    obj.ingested_at = datetime.now(timezone.utc)
    obj.job_id = job_id
    obj.entries_inserted = int(entries_inserted or 0)
    obj.last_error = None
    obj.lease_owner = None
    obj.lease_expires_at = None
    await db.commit()


async def reset_abandoned_objects(db: AsyncSession, customer_code: str) -> int:
    """Re-arm every dead-lettered row for a tenant.

    Mirrors reset_abandoned_windows (derive_transactions.py:869) deliberately, so operators learn
    one pattern rather than two. Re-processing reads the LOCAL stored file — no network re-fetch.
    """
    res = await db.execute(
        update(LogSourceObject)
        .where(LogSourceObject.customer_code == customer_code,
               LogSourceObject.status == SourceObjectStatus.abandoned)
        .values(status=SourceObjectStatus.pending, attempts=0, last_error=None,
                lease_owner=None, lease_expires_at=None,
                # clock_timestamp(), evaluated by the DATABASE — the same clock `claim_one` compares
                # against (`available_at <= clock_timestamp()`). Using the app host's clock here made
                # a re-armed row briefly unclaimable whenever that host ran ahead of the database:
                # measured drift on this deployment swings roughly +3 ms to -55 ms, and the row stays
                # invisible for the whole positive excursion. Same reason as the Stage 2 backoff in
                # derive_transactions.py:1035.
                available_at=func.clock_timestamp())
    )
    await db.commit()
    n = res.rowcount or 0
    if n:
        logger.info("Ingest queue: re-armed %d abandoned object(s) for %s", n, customer_code)
    return n


# ============================================================ processing
async def process_claimed(obj: LogSourceObject, storage: ObjectStorage) -> str:
    """Run Stage 1 for one leased row. Never raises — the outcome is recorded on the row.

    The bytes are already on disk at obj.storage_key, so this does NOT go through
    LogIngestion.ingest (which would save a second copy). It creates the Job for the existing key
    and calls the unchanged run_log_parse_insert, so Stage 1 behaviour is byte-for-byte the same as
    the inline path.
    """
    filename = f"{obj.source_name}/{obj.remote_path.rstrip('/').rsplit('/', 1)[-1]}"
    try:
        async with async_session() as s:
            job = Job(customer_code=obj.customer_code, filename=filename,
                      storage_key=obj.storage_key, document_type=DOCUMENT_TYPE)
            s.add(job)
            await s.commit()
            job_id = job.id

        async with async_session() as s:
            inserted = await run_log_parse_insert(job_id, s, storage)

        async with async_session() as s:
            await record_success(s, obj.id, job_id, inserted)
        return SourceObjectStatus.ingested
    except Exception as exc:  # noqa: BLE001 — the row records every outcome; the loop must go on
        async with async_session() as s:
            return await record_failure(s, obj.id, exc)


async def _delete_ingested_file(obj: LogSourceObject, storage: ObjectStorage) -> None:
    """Reclaim the stored copy once its row proves the bytes are safely in Postgres.

    Nothing has ever been able to do this before: LocalStorage.delete existed but had no caller in
    the log path, because nothing tracked whether a file had been successfully ingested.
    """
    if not settings.log_parse_delete_ingested_files:
        return
    try:
        await storage.delete(obj.storage_key)
        async with async_session() as s:
            await s.execute(
                update(LogSourceObject).where(LogSourceObject.id == obj.id)
                .values(file_deleted_at=datetime.now(timezone.utc)))
            await s.commit()
    except Exception:  # housekeeping must never fail the drain
        logger.warning("Ingest queue: could not delete %s", obj.storage_key, exc_info=True)


async def drain_once(storage: ObjectStorage | None = None) -> dict:
    """Reclaim expired leases, then process up to log_parse_batch_size rows.

    Returns counts only; every per-row outcome is durable on the row itself.
    """
    stats = {"claimed": 0, "ingested": 0, "failed": 0, "abandoned": 0}
    worker_id = _worker_id()

    async with async_session() as s:
        await reclaim_expired_leases(s)

    for _ in range(max(1, int(settings.log_parse_batch_size))):
        async with async_session() as s:
            obj = await claim_one(s, worker_id)
        if obj is None:
            break
        stats["claimed"] += 1
        if storage is None:
            storage = LocalStorage(settings.upload_dir)

        status = await process_claimed(obj, storage)
        if status == SourceObjectStatus.ingested:
            stats["ingested"] += 1
            await _delete_ingested_file(obj, storage)
        elif status == SourceObjectStatus.abandoned:
            stats["abandoned"] += 1
        else:
            stats["failed"] += 1

    # NOTE: this worker does NOT stitch. Stage 1 already wrote a log_regroup_pending ticket inside
    # the same transaction as the entries (parse_insert.py:178), and the stitch worker owns draining
    # that queue. Calling Stage 2 from here would put us back where we started, with every producer
    # having to remember to trigger it.
    return stats


async def run_log_parse_worker() -> None:
    """Forever loop. Survives errors; only CancelledError (shutdown) stops it."""
    logger.info("Log parse worker started (poll=%.1fs, batch=%d, max_attempts=%d)",
                settings.log_parse_poll_seconds, settings.log_parse_batch_size,
                settings.log_parse_max_attempts)
    storage = LocalStorage(settings.upload_dir)
    while True:
        try:
            stats = await drain_once(storage)
            if stats["claimed"]:
                logger.info("Ingest queue drain: %s", stats)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Log parse worker error — retrying next tick")
        await asyncio.sleep(settings.log_parse_poll_seconds)


async def unfinished_ingest_objects() -> int:
    """Rows still owed parsing, across all tenants.

    Used at startup to decide whether the worker must run even with the feature flag off: once a row
    exists its checkpoint has already advanced past those bytes, so leaving it undrained loses them.
    """
    async with async_session() as s:
        return await s.scalar(
            select(func.count()).select_from(LogSourceObject).where(
                LogSourceObject.status.in_(
                    (SourceObjectStatus.pending, SourceObjectStatus.leased)))
        ) or 0


async def pending_count(customer_code: str) -> int:
    """Backlog depth for one tenant — drives the fetcher's queue-depth guard."""
    async with async_session() as s:
        return await s.scalar(
            select(func.count()).select_from(LogSourceObject).where(
                LogSourceObject.customer_code == customer_code,
                LogSourceObject.status.in_(
                    (SourceObjectStatus.pending, SourceObjectStatus.leased)),
            )
        ) or 0
