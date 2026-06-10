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

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.persistence.models.job import Job, JobStatus
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.storage.base import ObjectStorage
from app.services.mnp_log_ingestion.parsers.LogParserFactory import get_log_parser

logger = logging.getLogger(__name__)

_INSERT_BATCH = 1000


def _entry_hash(raw_body: str) -> str:
    """Content dedup key — sha256 of the full raw entry text (incl. ms timestamp + message)."""
    return hashlib.sha256((raw_body or "").encode("utf-8")).hexdigest()


async def _insert_dedup(db: AsyncSession, rows: list[dict]) -> int:
    """Bulk INSERT ... ON CONFLICT (entry_hash) DO NOTHING. Returns rows actually inserted."""
    if not rows:
        return 0
    stmt = pg_insert(LogEntry).values(rows).on_conflict_do_nothing(index_elements=["entry_hash"])
    result = await db.execute(stmt)
    return result.rowcount or 0


async def run_log_parse_insert(job_id: UUID, db: AsyncSession, storage: ObjectStorage) -> int:
    """Parse the job's log file and insert raw log_entries (skipping content duplicates).

    Returns the number of NEW entries inserted (duplicates already in the DB are skipped).
    """
    job = await db.get(Job, job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")

    try:
        await _set_status(db, job_id, JobStatus.parsing)

        data = await storage.load(job.storage_key)
        text = data.decode("utf-8", errors="replace")

        parser = get_log_parser(settings.log_format)
        records = parser.parse(text)

        # Map LogRecords → row dicts, dedup-by-content via entry_hash, insert in batches.
        # Within-file duplicates (same raw_body twice) are collapsed here so one INSERT batch
        # never carries two rows with the same entry_hash (which ON CONFLICT can't resolve).
        batch: list[dict] = []
        seen_in_batch: set[str] = set()
        inserted = 0
        parsed = 0

        async def flush(b: list[dict]) -> int:
            return await _insert_dedup(db, b)

        for rec in records:
            parsed += 1
            h = _entry_hash(rec.raw_body)
            if h in seen_in_batch:
                continue
            seen_in_batch.add(h)
            batch.append({
                "id": uuid.uuid4(),
                "job_id": job_id,
                "entry_hash": h,
                "source_file": job.filename,
                "line_number": rec.line_number,
                "timestamp": rec.timestamp,
                "level": rec.level,
                "thread": rec.thread,
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
        logger.exception("Log parse/insert failed for job %s", job_id)
        await db.rollback()
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
