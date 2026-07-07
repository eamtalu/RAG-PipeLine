"""Chunk 1 of the SSH log-fetch hardening: settings, model columns, and the migration.

Covers the schema/config groundwork the rest of the hardening builds on. Behavioural edge-case
suites (rotation, concurrency lock, circuit breaker, double-submit, cancel, windowed resume,
timeouts) land with their respective code chunks.
"""

import uuid

from alembic.config import Config
from alembic.script import ScriptDirectory

from app.settings import settings
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_file_checkpoint import LogSshFileCheckpoint


# --------------------------------------------------------------------------- settings
def test_new_ssh_settings_exist_with_defaults():
    assert settings.ssh_operation_timeout_seconds == 60.0
    assert settings.ssh_keepalive_interval_seconds == 15.0
    assert settings.ssh_keepalive_count_max == 3
    assert settings.ssh_fingerprint_bytes == 4096
    assert settings.ssh_checkpoint_retention_days == 30
    assert settings.ssh_fetch_lock_wait_seconds == 30.0
    assert settings.ssh_poll_max_concurrent == 8
    assert settings.ssh_poll_reconcile_seconds == 30.0
    assert settings.ssh_auto_disable_after_failures == 10


# --------------------------------------------------------------------------- migration graph
def test_single_alembic_head_and_chain():
    script = ScriptDirectory.from_config(Config("alembic.ini"))
    # exactly one head (no divergent branches), and this chunk's migration chains off the prior head
    assert len(script.get_heads()) == 1, "migrations must leave a single head"
    rev = script.get_revision("b3f9a1c05d27")
    assert rev.down_revision == "e5a2c9f10b34"


# --------------------------------------------------------------------------- model columns
def _make_source(**over):
    base = dict(customer_code="TEST_CHUNK1", name=f"src-{uuid.uuid4().hex[:8]}",
                host="host.example", username="svc", remote_log_dir="C:/logs/m3")
    base.update(over)
    return LogSshSource(**base)


async def test_source_new_columns_and_defaults(db):
    src = _make_source()
    db.add(src)
    await db.flush()
    await db.refresh(src)
    # circuit-breaker counter defaults to 0; the timestamp/marker columns default to NULL.
    assert src.consecutive_failures == 0
    assert src.auto_disabled_at is None
    assert src.last_attempt_at is None
    # last_ok_at (last success) remains distinct and starts NULL.
    assert src.last_ok_at is None


async def test_source_columns_are_writable(db):
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    src = _make_source(consecutive_failures=7, auto_disabled_at=now, last_attempt_at=now)
    db.add(src)
    await db.flush()
    await db.refresh(src)
    assert src.consecutive_failures == 7
    assert src.auto_disabled_at is not None
    assert src.last_attempt_at is not None


async def test_checkpoint_head_fingerprint_default_and_writable(db):
    src = _make_source()
    db.add(src)
    await db.flush()
    ck = LogSshFileCheckpoint(source_id=src.id, customer_code=src.customer_code,
                              remote_path="C:/logs/m3/app.log")
    db.add(ck)
    await db.flush()
    await db.refresh(ck)
    assert ck.head_fingerprint is None  # lazily backfilled
    ck.head_fingerprint = "a" * 64
    await db.flush()
    await db.refresh(ck)
    assert ck.head_fingerprint == "a" * 64
