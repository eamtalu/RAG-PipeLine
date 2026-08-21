"""Chunk 27 (step 6 of docs/plan/2026-08-05_20-32_daily-partitioning.md): surface partition health on
`GET /logs/regroup/status`.

Three numbers matter operationally, and none of them has any other symptom until something breaks:

- `days_ahead` — when it reaches 0, ingestion STOPS. An insert into a day with no partition fails
  outright with "no partition of relation found for row". This is the one that pages someone.
- `default_partition_rows` — the DEFAULT partition holds entries whose timestamp would not parse.
  Steady growth means the parser is silently failing on a log format, and nothing else reports it.
- `oldest_day` — whether retention is actually running, or the drop half has been quietly failing
  while the create half keeps working.

It rides on this endpoint rather than a new one because the AUTO-POLL card already polls it via
`RegroupContext`, so the frontend needs no second request. The block is a `pg_class` catalogue read,
not a data scan, so it is safe at the card's existing cadence — asserted below rather than assumed.

Partitions are GLOBAL while this endpoint is tenant-scoped, so every customer sees the same block.
That is deliberate and documented; it is infrastructure health, not tenant data.

`healthy` is computed server-side so the frontend does not encode the thresholds — a threshold in a
React component is one nobody can change without a deploy, and it drifts from the worker's own alarm.
"""

import uuid
from datetime import date as date_type, datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, text

from app.persistence import partitioning as pt
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.settings import settings

CC = "test_chunk27"


async def _cleanup(db):
    await db.execute(delete(LogEntry).where(LogEntry.customer_code == CC))
    await db.execute(delete(Job).where(Job.customer_code == CC))
    await db.flush()


async def _status(db):
    from app.api.v1.logs import regroup_status
    return await regroup_status(customer=CC, db=db)


# ==================================================== health rules (pure)
def test_healthy_needs_both_runway_and_an_empty_default_partition():
    from app.api.v1.logs import _partitions_healthy
    assert _partitions_healthy(days_ahead=30, default_rows=0) is True
    assert _partitions_healthy(days_ahead=30, default_rows=1) is False
    assert _partitions_healthy(days_ahead=1, default_rows=0) is False


def test_the_runway_threshold_matches_the_worker_alarm():
    """If the card and the worker disagreed about what 'short' means, one would be reporting green
    while the other paged — and the operator would trust the green one."""
    from app.api.v1.logs import _partitions_healthy
    floor = settings.log_partition_min_runway_days
    assert _partitions_healthy(days_ahead=floor, default_rows=0) is True
    assert _partitions_healthy(days_ahead=floor - 1, default_rows=0) is False


def test_a_missing_runway_is_unhealthy_not_a_crash():
    """`days_of_runway` returns -1 when a table has no future partition at all — the worst case, and
    the one most likely to be mishandled as 'no data'."""
    from app.api.v1.logs import _partitions_healthy
    assert _partitions_healthy(days_ahead=-1, default_rows=0) is False


# ==================================================== the block itself
async def test_the_status_response_carries_a_partitions_block(db):
    body = await _status(db)
    assert "partitions" in body
    p = body["partitions"]
    assert set(p) >= {"days_ahead", "oldest_day", "newest_day", "retention_days",
                      "default_partition_rows", "healthy"}


async def test_the_block_reports_the_real_runway(db):
    """It has to read the live catalogue, not echo the configured pre-create value: the whole point is
    to reveal that the worker has STOPPED extending the runway.

    The two are deliberately forced APART first. In a healthy database the live runway equals the
    configured one, so a version that simply returned the setting would look correct — the assertion
    only has teeth once they differ.
    """
    from app.services.workers.log_partition_worker import db_today, days_of_runway
    today = await db_today(db)
    far = today + timedelta(days=settings.log_partition_precreate_days + 9)
    await pt.ensure_coverage(db, days=[far])
    try:
        live = await days_of_runway(db, today)
        assert live != settings.log_partition_precreate_days, "the two must differ for this to test"
        assert (await _status(db))["partitions"]["days_ahead"] == live
    finally:
        for t in pt.PARTITIONED:
            await db.execute(text(f"DROP TABLE IF EXISTS {pt.partition_name(t.table, far)}"))


async def test_the_reported_days_are_real_partition_boundaries(db):
    """Derived from partition BOUNDS, so a partition whose name and range disagreed would be reported
    honestly rather than plausibly."""
    body = await _status(db)
    p = body["partitions"]
    covered = await pt.covered_days(db, "log_entries")
    assert p["oldest_day"] == min(covered).isoformat()
    assert p["newest_day"] == max(covered).isoformat()


async def test_retention_is_echoed_so_the_operator_can_see_the_policy(db):
    body = await _status(db)
    assert body["partitions"]["retention_days"] == settings.log_partition_retention_days


async def test_a_timestampless_entry_shows_up_in_the_default_partition_count(db):
    """The only signal that the parser is dropping timestamps. Nothing else surfaces it."""
    await _cleanup(db)
    before = (await _status(db))["partitions"]["default_partition_rows"]
    j = Job(customer_code=CC, filename="c27.log", storage_key=f"{CC}/{uuid.uuid4().hex}/c27.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()
    db.add(LogEntry(customer_code=CC, job_id=j.id, timestamp=None, source_file="c27.log",
                    line_number=1, level="INFO", raw_body="x", entry_hash=uuid.uuid4().hex))
    await db.flush()
    after = (await _status(db))["partitions"]
    assert after["default_partition_rows"] == before + 1
    assert after["healthy"] is False, "a NULL-timestamp entry must show as unhealthy"
    await _cleanup(db)


async def test_a_timestamped_entry_does_not_touch_the_default_count(db):
    """The guard against the count being something cheap-but-wrong, like all entries.

    The partition for the day being written has to exist first, or PostgreSQL routes a perfectly good
    timestamp into DEFAULT and this fails for a reason that has nothing to do with the count. In
    production the partition worker guarantees that; here it is stated explicitly, so a lapsed runway
    in a development database (the app not having run for a couple of days) cannot make a correct
    metric look broken. Runway health has its own tests in test_partition_worker_chunk25.
    """
    await _cleanup(db)
    ts = datetime.now(timezone.utc)
    await pt.ensure_coverage(db, days=[ts.date()])
    before = (await _status(db))["partitions"]["default_partition_rows"]
    j = Job(customer_code=CC, filename="c27.log", storage_key=f"{CC}/{uuid.uuid4().hex}/c27.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()
    db.add(LogEntry(customer_code=CC, job_id=j.id, timestamp=ts, source_file="c27.log",
                    line_number=1, level="INFO", raw_body="x", entry_hash=uuid.uuid4().hex))
    await db.flush()
    assert (await _status(db))["partitions"]["default_partition_rows"] == before
    await _cleanup(db)


# ==================================================== cost
async def test_the_runway_read_is_a_catalogue_read_not_a_data_scan(db):
    """The card polls this on a timer. Touching log_entries itself would turn a status widget into a
    recurring full scan across every partition."""
    plan = "\n".join(r[0] for r in (await db.execute(text("""
        EXPLAIN SELECT (regexp_match(pg_get_expr(c.relpartbound, c.oid),
                                     'FROM \\(''([0-9-]{10})'))[1]::date
        FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'log_entries'::regclass
    """))).all())
    assert "log_entries_2026" not in plan, f"must not scan the partitions themselves:\n{plan}"


async def test_the_default_partition_count_touches_only_the_default_partition(db):
    """Counting NULL-timestamp rows must read the DEFAULT partition alone — counting through the
    parent would scan all ~130 of them on every poll."""
    from app.api.v1.logs import _default_partition_count_stmt
    plan = "\n".join(r[0] for r in (await db.execute(
        text("EXPLAIN " + str(_default_partition_count_stmt().compile(
            db.bind, compile_kwargs={"literal_binds": True}))))).all())
    scanned = [ln for ln in plan.splitlines() if "log_entries_2026" in ln]
    assert not scanned, f"must not touch dated partitions:\n{plan}"


# ==================================================== TWO causes, not one
# The DEFAULT partition catches rows for TWO unrelated reasons, and the single
# `default_partition_rows` count conflates them:
#
#   1. the partition key is NULL — the parser could not read the timestamp;
#   2. the key is perfectly valid but NO PARTITION EXISTS for its day.
#
# Only (1) is a parser fault. (2) happens because the runway is built FORWARD
# only (`coverage_days(today, ahead=...)`), so nothing ever provisions a past day
# and a backfill of older log files lands in DEFAULT permanently.
#
# Measured on the live server 2026-08-21: 294,748 rows in the DEFAULT entry
# partition, of which exactly ONE had a NULL timestamp. The other 294,747 were
# valid instants between 2026-06-29 and 2026-08-05, all predating the oldest
# partition. The card reported that as a parser failure, which sent the operator
# looking in the wrong place. These tests pin the split so it cannot recur.


async def test_the_split_separates_a_null_key_from_a_missing_partition(db):
    await _cleanup(db)
    j = Job(customer_code=CC, filename="c27.log", storage_key=f"{CC}/{uuid.uuid4().hex}/c27.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()

    # A day with no partition. Far enough back that the runway can never reach it.
    orphan_day = datetime(2019, 3, 4, 9, 30, tzinfo=timezone.utc)
    assert not await pt.partition_exists(db, "log_entries", orphan_day.date()), \
        "the premise of this test is that no partition exists for that day"

    before = (await _status(db))["partitions"]
    db.add(LogEntry(customer_code=CC, job_id=j.id, timestamp=None, source_file="c27.log",
                    line_number=1, level="INFO", raw_body="a", entry_hash=uuid.uuid4().hex))
    db.add(LogEntry(customer_code=CC, job_id=j.id, timestamp=orphan_day, source_file="c27.log",
                    line_number=2, level="INFO", raw_body="b", entry_hash=uuid.uuid4().hex))
    await db.flush()
    after = (await _status(db))["partitions"]

    assert after["unparsable_timestamp_rows"] == before["unparsable_timestamp_rows"] + 1, \
        "only the NULL-timestamp row is a parser fault"
    assert after["unpartitioned_rows"] == before["unpartitioned_rows"] + 1, \
        "the valid-timestamp row is in DEFAULT because its day has no partition"
    assert after["default_partition_rows"] == before["default_partition_rows"] + 2
    await _cleanup(db)


async def test_the_two_causes_always_sum_to_the_total(db):
    """They partition the DEFAULT partition exactly: a row's key is either NULL or it is not."""
    p = (await _status(db))["partitions"]
    assert p["unparsable_timestamp_rows"] + p["unpartitioned_rows"] == p["default_partition_rows"]


async def test_both_counts_come_from_one_scan_of_only_the_default_partition(db):
    """The card polls this on a timer, so the split must not cost a second pass over 300k rows."""
    from app.api.v1.logs import _default_partition_count_stmt
    sql = str(_default_partition_count_stmt().compile(
        db.bind, compile_kwargs={"literal_binds": True}))
    assert sql.upper().count("FROM") == 1, f"the split must be one scan, not two:\n{sql}"
    plan = "\n".join(r[0] for r in (await db.execute(text("EXPLAIN " + sql))).all())
    assert not [ln for ln in plan.splitlines() if "log_entries_2026" in ln], \
        f"must not touch dated partitions:\n{plan}"


async def test_healthy_still_keys_on_the_total_so_the_contract_is_unchanged(db):
    """The split is for the MESSAGE, not for the verdict. Either cause needs attention, so both keep
    the card amber; what changes is that the operator is now told which one."""
    from app.api.v1.logs import _partitions_healthy
    assert _partitions_healthy(days_ahead=30, default_rows=0) is True
    assert _partitions_healthy(days_ahead=30, default_rows=1) is False


# ==================================================== no regression
async def test_the_existing_stitching_fields_are_untouched(db):
    """The card already renders these; an additive block must stay additive."""
    body = await _status(db)
    for k in ("customer_code", "pending", "pending_windows", "oldest_pending_at",
              "last_regroup_at", "abandoned_windows", "backing_off_windows"):
        assert k in body, f"{k} disappeared from /regroup/status"


async def test_a_partition_read_failure_does_not_break_the_status_endpoint(db, monkeypatch):
    """Partition health is a nice-to-have on a widget that primarily reports STITCHING. If the
    catalogue read fails, the card must still render the stitching status rather than 500."""
    import app.api.v1.logs as logs_api

    async def boom(*a, **k):
        raise RuntimeError("catalogue unavailable")
    monkeypatch.setattr(logs_api, "_partition_status", boom)
    body = await _status(db)
    assert body["pending_windows"] is not None
    assert body["partitions"] is None, "an unavailable block must be null, not absent or fabricated"
