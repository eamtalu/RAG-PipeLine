# parse_insert.py — Stage 1 of the log pipeline (parse → insert raw entries)
#
#   run_log_parse_insert(job_id, db, storage)
#
#   1. Load the raw log file bytes for the job
#   2. Parse into LogRecords (one per timestamped entry) via the configured parser
#   3. Bulk-insert them as log_entries rows — the lossless source of truth
#
#   Grouping into transactions (Stage 2) is intentionally NOT done here: it runs separately over
#   the whole log_entries table ordered by timestamp, which is what lets a transaction span files.

"""Stage 1 — parse a log file and insert its raw entries (content-deduped)."""

import hashlib
import logging
import uuid
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import text as sa_text, update  # aliased: a local var `text` holds the file contents
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.persistence.models.job import Job, JobStatus
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.repositories.customer_repository import get_customer_timezone_raw
from app.persistence.storage.base import ObjectStorage
from app.services.mnp_log_ingestion.io_errors import is_disk_io_error, disk_io_detail
from app.services.mnp_log_ingestion.parsers.LogParserFactory import get_log_parser

logger = logging.getLogger(__name__)

# Small batches: each INSERT maintains ~11 indexes on the 40 GB log_entries table, and on the
# failing/saturated production disk a large batch can crawl past statement_timeout. Smaller
# statements finish quicker and keep progress granular.
_INSERT_BATCH = 200


def _entry_hash(raw_body: str) -> str:
    """Content dedup key — sha256 of the full raw entry text (incl. ms timestamp + message)."""
    return hashlib.sha256((raw_body or "").encode("utf-8")).hexdigest()


async def _insert_dedup(db: AsyncSession, rows: list[dict]) -> list:
    """Bulk INSERT ... ON CONFLICT (entry_hash) DO NOTHING RETURNING timestamp.

    Returns the timestamps of the rows ACTUALLY inserted (duplicates are skipped, so they are not
    returned). The caller derives the ingest's dirty time-range from these — NOT from a separate
    `SELECT min/max WHERE job_id=...` scan, which on the failing production disk reads old blocks
    (including a dead sector) and errors. RETURNING only reports the just-written rows, so it never
    touches old data. See app/services/mnp_log_ingestion/io_errors.py and the disk-io-resilience doc.
    """
    if not rows:
        return []
    # Dedup is per customer: the same line for two customers is two distinct rows.
    #
    # `timestamp` joined this key when log_entries became partitioned by UTC day — PostgreSQL requires
    # a unique constraint on a partitioned table to contain every partition column, and ON CONFLICT
    # must name the constraint EXACTLY or it fails outright with "no unique or exclusion constraint
    # matching the ON CONFLICT specification".
    #
    # It stays a correct dedup key because `entry_hash` is a sha256 over the raw line INCLUDING its
    # millisecond timestamp text, so a replay of the same line parses to the same instant and lands on
    # the same row. The one way the two can disagree is a customer's display timezone being changed
    # between ingests, which re-parses the same text to a different UTC instant — a narrow but real
    # hole tracked as step 5 of the partitioning plan.
    stmt = pg_insert(LogEntry).values(rows).on_conflict_do_nothing(
        index_elements=["customer_code", "entry_hash", "timestamp"]
    ).returning(LogEntry.timestamp)
    return list((await db.execute(stmt)).scalars().all())


async def run_log_parse_insert(job_id: UUID, db: AsyncSession, storage: ObjectStorage) -> int:
    """Parse the job's log file and insert raw log_entries (skipping content duplicates).

    Returns the number of NEW entries inserted (duplicates already in the DB are skipped).
    """
    job = await db.get(Job, job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    # Capture now: after a rollback in the except handler the ORM `job` is expired, and touching its
    # attributes there would trigger sync IO in the async context (MissingGreenlet).
    filename = job.filename
    customer_code = job.customer_code

    try:
        await _set_status(db, job_id, JobStatus.parsing)

        data = await storage.load(job.storage_key)
        text = data.decode("utf-8", errors="replace")

        parser = get_log_parser(settings.log_format)
        records = parser.parse(text)

        # The parser yields the log's NAIVE local wall-clock. Attach THIS customer's configured
        # timezone so it becomes a true UTC instant on insert — independent of the ingest host's
        # timezone (asyncpg would otherwise assume the host's). DST is handled per-date by ZoneInfo.
        # This is the single choke point for EVERY ingestion path (frontend upload/scan/remote-fetch,
        # backend SSH auto-pull, watcher), so the safeguard warning below covers them all.
        raw_tz = await get_customer_timezone_raw(db, job.customer_code)
        if raw_tz is None:
            logger.warning(
                "Customer %r ingested logs with NO timezone configured — falling back to %s. If this "
                "customer's log server is not in that zone, its stored timestamps will be wrong. Set it "
                "via PATCH /api/v1/customers/%s {\"timezone\": \"...\"} and re-ingest.",
                job.customer_code, settings.display_timezone, job.customer_code,
            )
        cust_zone = ZoneInfo(raw_tz or settings.display_timezone)

        # Ingestion runs on the dedicated worker over a degraded, slow disk: an insert maintaining
        # ~11 indexes on a 40 GB table can legitimately exceed the (web-tier) statement_timeout and
        # be cancelled mid-batch (QueryCanceledError). Relax the timeout FOR THIS TRANSACTION ONLY to a
        # generous but FINITE cap so a slow insert can complete, while a pathological bad-sector stall is
        # still bounded (not an indefinite hang). SET LOCAL reverts on commit, so pooled connections and
        # the web tier keep their own guardrail. (The value must be a literal — SET can't be parameterised
        # — but it is an int from settings, not user input.)
        await db.execute(sa_text(f"SET LOCAL statement_timeout = {int(settings.log_worker_statement_timeout_ms)}"))

        # Map LogRecords → row dicts, dedup-by-content via entry_hash, insert in batches.
        # Within-file duplicates (same raw_body twice) are collapsed here so one INSERT batch
        # never carries two rows with the same entry_hash (which ON CONFLICT can't resolve).
        batch: list[dict] = []
        seen_in_batch: set[str] = set()
        inserted = 0
        parsed = 0
        # Dirty time-range of the rows we insert, accumulated from the INSERT ... RETURNING
        # timestamps (never from a separate scan — see _insert_dedup).
        lo = None
        hi = None

        async def flush(b: list[dict]) -> int:
            nonlocal lo, hi
            ts_list = await _insert_dedup(db, b)
            for ts in ts_list:
                if ts is None:
                    continue
                if lo is None or ts < lo:
                    lo = ts
                if hi is None or ts > hi:
                    hi = ts
            return len(ts_list)

        for rec in records:
            parsed += 1
            h = _entry_hash(rec.raw_body)
            if h in seen_in_batch:
                continue
            seen_in_batch.add(h)
            ts = rec.timestamp
            if ts is not None and ts.tzinfo is None:
                ts = ts.replace(tzinfo=cust_zone)  # naive local wall-clock → aware (customer zone)
            batch.append({
                "id": uuid.uuid4(),
                "job_id": job_id,
                "customer_code": job.customer_code,
                "entry_hash": h,
                "source_file": job.filename,
                "line_number": rec.line_number,
                "timestamp": ts,
                "level": rec.level,
                "thread": rec.thread,
                "user_ctx": rec.user,
                "logger": rec.logger,
                "method": rec.method,
                "entry_type": LogEntryType(rec.entry_type),
                "mi_program": rec.mi_program,
                "mi_transaction": rec.mi_transaction,
                "result_status": rec.result_status,
                "record_count": rec.record_count,
                "message": rec.message,
                "raw_body": rec.raw_body,
                "fields": rec.fields or {},
            })
            if len(batch) >= _INSERT_BATCH:
                inserted += await flush(batch)
                batch = []
                seen_in_batch.clear()
        inserted += await flush(batch)

        # Mark the time range this ingest touched as needing (re)grouping, so a later scoped regroup
        # (console finalize / watcher drain) stitches exactly this window — not the whole table. The
        # range is the min/max timestamp of the rows we just inserted, taken from INSERT ... RETURNING
        # (lo/hi above) rather than a `SELECT min/max WHERE job_id` scan — that scan reads old blocks
        # and, on the failing disk, hits a dead sector. Skip when nothing new was inserted or every
        # new entry is timestamp-less (a windowed regroup can't place those).
        if inserted and lo is not None and hi is not None:
            db.add(LogRegroupPending(customer_code=customer_code, job_id=job_id,
                                     range_start=lo, range_end=hi))

        # Stage 1 done at line level. Grouping (Stage 2) runs separately.
        await db.execute(
            update(Job).where(Job.id == job_id).values(
                status=JobStatus.completed,
                chunk_count=inserted,  # NEW entries from this file (duplicates skipped)
            )
        )
        await db.commit()

        logger.info("Job %s: parsed %d entries, inserted %d new (%d duplicates skipped)",
                    job_id, parsed, inserted, parsed - inserted)
        return inserted

    except Exception as exc:
        await db.rollback()
        if is_disk_io_error(exc):
            # A dead sector was hit while ingesting this file. We cannot repair the disk, so mark the
            # file failed with a clear label and let the caller move on to the next file. Loud so it
            # is visible: the disk has failing sectors.
            detail = disk_io_detail(exc)
            logger.critical(
                "DISK FAULT ingesting job %s (file=%s, customer=%s): %s — skipping this file. "
                "The disk is failing/too slow; this file will be retried next poll. Ensure backups.",
                job_id, filename, customer_code, detail,
            )
            await _set_status(db, job_id, JobStatus.failed, error=f"disk fault: {detail}")
        else:
            logger.exception("Log parse/insert failed for job %s", job_id)
            await _set_status(db, job_id, JobStatus.failed, error=str(exc))
        raise


async def _set_status(
    db: AsyncSession, job_id: UUID, status: JobStatus, error: str | None = None
) -> None:
    values: dict = {"status": status}
    if error:
        values["error"] = error
    await db.execute(update(Job).where(Job.id == job_id).values(**values))
    await db.commit()
