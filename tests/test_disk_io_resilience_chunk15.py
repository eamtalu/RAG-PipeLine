"""Chunk 15: resilience to disk I/O (bad-sector) errors on the failing production HDD.

Context (see the disk-io-resilience plan): /dev/sda has unrecoverable bad sectors. The per-file
ingest previously computed its dirty range with `SELECT min/max WHERE job_id=...`, which scans old
rows and hits a dead block, failing every ingest. The fix derives the range from
`INSERT ... RETURNING timestamp` (only the just-written rows), and classifies genuine disk I/O
errors so each stage can skip + report + continue.

Covered:
- is_disk_io_error / disk_io_detail classify the real Postgres "could not read block" message
  (direct and chained) and don't false-positive on ordinary errors;
- _insert_dedup returns the timestamps of the rows ACTUALLY inserted (RETURNING), and nothing for
  duplicates — which is exactly what the pending-range min/max now relies on (no job_id scan).
"""

import hashlib
import uuid
from datetime import datetime, timezone

from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.services.mnp_log_ingestion.io_errors import is_disk_io_error, disk_io_detail
from app.services.mnp_log_ingestion.pipeline.parse_insert import _insert_dedup

_REAL_MSG = ('(sqlalchemy.dialects.postgresql.asyncpg.Error) '
             '<class \'asyncpg.exceptions.PostgresIOError\'>: could not read block 45991 in file '
             '"base/16388/16634": Input/output error')


def test_is_disk_io_error_classifies_real_message_and_extracts_block():
    err = RuntimeError(_REAL_MSG)
    assert is_disk_io_error(err) is True
    assert disk_io_detail(err) == 'block 45991 in file base/16388/16634'


def test_is_disk_io_error_follows_the_cause_chain():
    inner = RuntimeError('could not read block 7 in file "base/1/2": Input/output error')
    outer = RuntimeError("Stage 1 failed")
    outer.__cause__ = inner
    assert is_disk_io_error(outer) is True


def test_is_disk_io_error_ignores_ordinary_errors():
    assert is_disk_io_error(ValueError("bad input")) is False
    assert is_disk_io_error(RuntimeError("connection refused")) is False


async def test_insert_dedup_returns_inserted_timestamps_via_returning(db):
    """The pending range is now min/max of these returned timestamps — never a job_id scan."""
    cc = "TEST_CHUNK15"
    job = Job(customer_code=cc, filename="t.log", storage_key="k")
    db.add(job)
    await db.flush()

    ts = [datetime(2026, 7, 24, 10, 0, i, tzinfo=timezone.utc) for i in range(3)]

    def _row(i: int) -> dict:
        raw = f"entry-{i}"
        return {
            "id": uuid.uuid4(), "job_id": job.id, "customer_code": cc,
            "entry_hash": hashlib.sha256(raw.encode()).hexdigest(),
            "source_file": "t.log", "timestamp": ts[i], "entry_type": LogEntryType.info,
        }

    rows = [_row(0), _row(1), _row(2)]
    returned = await _insert_dedup(db, rows)
    assert sorted(returned) == sorted(ts)          # RETURNING gives back exactly the inserted rows
    assert min(returned) == ts[0] and max(returned) == ts[2]  # drives the pending range

    # Re-inserting the identical rows returns nothing (ON CONFLICT DO NOTHING) -> no phantom range.
    assert await _insert_dedup(db, rows) == []
