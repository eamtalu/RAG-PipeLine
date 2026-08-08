"""Chunk 29 (step 3 of docs/plan/2026-08-08_notification-architecture.html): do not alert on a
transaction that is still changing.

Stage 2 rebuilds the unsealed tail on every cycle. A transaction with a REQUEST but no RESPONSE yet is
`incomplete`, and minutes later the RESPONSE arrives and it becomes `success`. If a rule alerted on the
`incomplete` version, that alert was wrong - and because `dedup_key` is stable per (rule, transaction),
**no correction is ever sent**. The channel keeps a permanent record of something that never happened.

Measured on production 7 Aug: 81 of 412 `incomplete` transactions were unsealed, i.e. still able to
change.

The fix gates on stability, not on age:

    incomplete + unsealed -> WAIT.   It is in flight; it will probably become success.
    incomplete + SEALED   -> ALERT.  Past the abandon window, the response is never coming.
                                     This is the genuinely interesting alert.
    error / soft / success -> ALERT immediately. Delaying these by the 15-minute seal window
                                     would defeat the point of alerting at all.

The whole thing rests on one invariant, pinned by a test below because everything else is built on it:

    A REBUILT TRANSACTION GETS A NEW `created_at`.

Stage 2 does DELETE + INSERT (`derive_transactions.py:600` constructs a fresh `LogTransaction`), so a
skipped row re-enters the cursor's feed the moment it seals. Without that, gating would mean "never
alert" rather than "alert later", and the fix would silently be a data-loss bug.
"""

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, func, select, text

from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.persistence.models.notification import (
    NotificationDelivery, NotificationEvent, NotificationRule, RuleStatus,
)
from app.services.notifications import cursor as cur
from app.services.notifications.rules import stability
from app.settings import settings

CC = "test_chunk29"


def _aged():
    """A write time far enough back that the cursor lag has elapsed."""
    return datetime.now(timezone.utc) - timedelta(
        seconds=settings.notification_cursor_lag_seconds + 30)


# =============================================================== the rule (pure)
def test_an_in_flight_incomplete_transaction_is_not_alertable():
    """The false alarm this whole chunk exists to prevent: it will most likely become `success`."""
    assert stability.is_alertable(LogTransactionStatus.incomplete, sealed=False) is False


def test_a_sealed_incomplete_transaction_is_alertable():
    """Sealed incomplete means past the abandon window - the response is never coming. That is a real
    condition worth alerting on, and gating must not swallow it."""
    assert stability.is_alertable(LogTransactionStatus.incomplete, sealed=True) is True


def test_an_error_alerts_immediately_without_waiting_to_seal():
    """Gating everything would delay a genuine error by up to the 15-minute seal window, which defeats
    the purpose of alerting. An error entry never un-errors."""
    assert stability.is_alertable(LogTransactionStatus.error, sealed=False) is True


def test_success_and_soft_alert_immediately_by_default():
    """Both require a RESPONSE, so they are already terminal in the ordinary case."""
    assert stability.is_alertable(LogTransactionStatus.success, sealed=False) is True
    assert stability.is_alertable(LogTransactionStatus.soft, sealed=False) is True


def test_the_strict_mode_waits_for_everything(monkeypatch):
    """The residual risk with the default: a late error entry can still join an already-responded
    transaction inside the seal window and flip `success` to `error`. Rare, but real - so an operator
    who prefers accuracy over latency can require sealing for every status."""
    monkeypatch.setattr(settings, "notification_alert_only_sealed", True)
    assert stability.is_alertable(LogTransactionStatus.error, sealed=False) is False
    assert stability.is_alertable(LogTransactionStatus.success, sealed=False) is False
    assert stability.is_alertable(LogTransactionStatus.error, sealed=True) is True


def test_a_transaction_with_no_status_is_not_alertable():
    """Defensive: a row mid-write or with a NULL status has nothing meaningful to match on."""
    assert stability.is_alertable(None, sealed=False) is False


def test_the_default_is_the_fast_one():
    """Waiting for seal on everything would be a silent latency regression on error alerting."""
    assert settings.notification_alert_only_sealed is False


# =============================================================== the SQL predicate
def _where(stmt, bind) -> str:
    """Just the WHERE clause. `sealed` is a column on the model so it always appears in the SELECT
    list; only its presence as a FILTER means anything here."""
    compiled = str(stmt.compile(bind, compile_kwargs={"literal_binds": True}))
    return compiled.split("WHERE", 1)[1] if "WHERE" in compiled else ""


async def test_the_gate_is_applied_in_sql_not_after_fetching(db):
    """Stage 2 rewrites the unsealed tail constantly, and every rewrite refreshes `created_at`, so an
    in-flight transaction re-enters the cursor's feed on every rebuild. Filtering after fetching would
    mean paying for that churn on every tick."""
    stmt = cur.window_stmt(
        CC, cur.Window(lo=_aged() - timedelta(hours=1), hi=_aged()), limit=10,
        extra=[stability.alertable_predicate()])
    assert "sealed" in _where(stmt, db.bind)


async def test_strict_mode_narrows_the_sql_gate_to_sealed_only(db):
    """The in-memory rule and the SQL predicate must agree. If only one honoured strict mode, the
    engine would fetch rows it then silently discarded - or worse, alert on rows the operator
    explicitly asked it not to."""
    from app.persistence.models.log_transaction import LogTransaction as LT
    import app.services.notifications.rules.stability as st
    original = settings.notification_alert_only_sealed
    try:
        settings.notification_alert_only_sealed = True
        stmt = cur.window_stmt(CC, cur.Window(lo=_aged() - timedelta(hours=1), hi=_aged()),
                               limit=10, extra=[st.alertable_predicate()])
        where = _where(stmt, db.bind)
        assert "sealed" in where
        assert "status" not in where, "strict mode gates on sealed alone, not on the status list"
    finally:
        settings.notification_alert_only_sealed = original


async def test_the_cursor_stays_generic_without_the_gate(db):
    """`cursor.py` is shared with future ML and analytics readers. Stability is a NOTIFICATION concern,
    so it must be passed in, never baked in."""
    stmt = cur.window_stmt(CC, cur.Window(lo=_aged() - timedelta(hours=1), hi=_aged()), limit=10)
    assert "sealed" not in _where(stmt, db.bind), "the gate must not appear unless it is passed in"
    import inspect
    assert "sealed" not in inspect.getsource(cur), "cursor.py must not know about seal semantics"


# =============================================================== the DB
async def _cleanup(db):
    await db.execute(delete(NotificationDelivery).where(
        NotificationDelivery.event_id.in_(
            select(NotificationEvent.id).where(NotificationEvent.customer_code == CC))))
    await db.execute(delete(NotificationEvent).where(NotificationEvent.customer_code == CC))
    await db.execute(delete(NotificationRule).where(NotificationRule.customer_code == CC))
    await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == CC))
    await db.execute(delete(LogEntry).where(LogEntry.customer_code == CC))
    await db.execute(delete(Job).where(Job.customer_code == CC))
    await db.flush()


async def _job(db):
    j = Job(customer_code=CC, filename="c29.log", storage_key=f"{CC}/{uuid.uuid4().hex}/c29.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()
    return j


async def _txn(db, job, *, status, sealed, created_at=None):
    created_at = created_at or _aged()
    t = LogTransaction(id=uuid.uuid4(), customer_code=CC, job_id=job.id, sealed=sealed,
                       status=status, started_at=created_at, ended_at=created_at,
                       date=created_at.date(), error_text="boom")
    db.add(t)
    await db.flush()
    await db.execute(text("UPDATE log_transactions SET created_at = :c WHERE id = :i"),
                     {"c": created_at, "i": t.id})
    await db.flush()
    return t


async def _fetch(db, *, cursor_at):
    window = cur.read_window(cursor_at, now=datetime.now(timezone.utc),
                             lag=timedelta(seconds=settings.notification_cursor_lag_seconds),
                             lookback=timedelta(seconds=settings.notification_lookback_seconds))
    return list((await db.execute(
        cur.window_stmt(CC, window, limit=100, extra=[stability.alertable_predicate()])
    )).scalars().all())


async def test_an_unsealed_incomplete_transaction_is_not_fetched(db):
    await _cleanup(db)
    job = await _job(db)
    await _txn(db, job, status=LogTransactionStatus.incomplete, sealed=False)
    assert await _fetch(db, cursor_at=_aged() - timedelta(minutes=5)) == []


async def test_a_sealed_incomplete_transaction_is_fetched(db):
    await _cleanup(db)
    job = await _job(db)
    await _txn(db, job, status=LogTransactionStatus.incomplete, sealed=True)
    assert len(await _fetch(db, cursor_at=_aged() - timedelta(minutes=5))) == 1


async def test_an_unsealed_error_is_still_fetched(db):
    """No latency regression on the alerts that matter most."""
    await _cleanup(db)
    job = await _job(db)
    await _txn(db, job, status=LogTransactionStatus.error, sealed=False)
    assert len(await _fetch(db, cursor_at=_aged() - timedelta(minutes=5))) == 1


# =============================================================== the load-bearing invariant
async def test_a_rebuilt_transaction_gets_a_new_created_at(db):
    """THE assumption the whole gate rests on.

    Skipping an unsealed row is only safe because it comes BACK: Stage 2 rebuilds it (DELETE +
    INSERT), which stamps a fresh `created_at`, so it re-enters the cursor's feed ahead of the
    bookmark. If a rebuild ever preserved the original `created_at`, gating would silently mean
    'never alert' instead of 'alert later' - the exact data-loss shape this file is trying to avoid.
    """
    await _cleanup(db)
    job = await _job(db)
    from app.services.mnp_log_ingestion.pipeline.derive_transactions import _write_transaction

    base = _aged()
    values = dict(customer_code=CC, job_id=job.id, status=LogTransactionStatus.incomplete,
                  started_at=base, ended_at=base, date=base.date())
    tid = uuid.uuid4()
    first = await _write_transaction(db, tid=tid, values=values, is_sealed=False,
                                     entries=[], customer_code=CC)
    created_first = first.created_at

    # what Stage 2 does on the next cycle: delete the unsealed row and rebuild it, now sealed
    await db.execute(delete(LogTransaction).where(LogTransaction.id == tid))
    await db.flush()
    second = await _write_transaction(db, tid=tid, values=values, is_sealed=True,
                                      entries=[], customer_code=CC)

    assert second.created_at > created_first, (
        "a rebuild must refresh created_at, or a gated transaction never re-enters the feed")
    assert second.id == first.id, "the deterministic id must survive the rebuild"


# =============================================================== the engine, end to end
async def _run_engine_once():
    from app.services.notifications import dispatcher as nd
    from app.services.notifications.bus import bus
    from app.services.notifications.rules.engine import run_rules_once
    bus.clear()
    nd.register()
    await run_rules_once()


async def _committed(status, sealed, *, created_at=None):
    from app.config.database import async_session
    created_at = created_at or _aged()
    async with async_session() as s:
        j = Job(customer_code=CC, filename="c29.log", storage_key=f"{CC}/{uuid.uuid4().hex}/c29.log",
                document_type="transaction_log", status="completed")
        s.add(j)
        await s.flush()
        t = LogTransaction(id=uuid.uuid4(), customer_code=CC, job_id=j.id, sealed=sealed,
                           status=status, started_at=created_at, ended_at=created_at,
                           date=created_at.date(), error_text="boom")
        s.add(t)
        await s.flush()
        await s.execute(text("UPDATE log_transactions SET created_at = :c WHERE id = :i"),
                        {"c": created_at, "i": t.id})
        await s.commit()
        return t.id


async def _committed_rule(statuses):
    from app.config.database import async_session
    async with async_session() as s:
        r = NotificationRule(customer_code=CC, name=f"r-{uuid.uuid4().hex[:6]}",
                             rule_type="status_match", match={"statuses": statuses},
                             severity="error", status=RuleStatus.active.value)
        s.add(r)
        await s.commit()
        return r.id


async def _hard_cleanup():
    from app.config.database import async_session
    async with async_session() as s:
        await s.execute(delete(NotificationDelivery).where(
            NotificationDelivery.event_id.in_(
                select(NotificationEvent.id).where(NotificationEvent.customer_code == CC))))
        for m in (NotificationEvent, NotificationRule, LogTransaction, LogEntry, Job):
            await s.execute(delete(m).where(m.customer_code == CC))
        await s.commit()


async def _event_count():
    from app.config.database import async_session
    async with async_session() as s:
        return await s.scalar(select(func.count()).select_from(NotificationEvent)
                              .where(NotificationEvent.customer_code == CC))


async def test_engine_does_not_alert_on_an_in_flight_incomplete(db):
    """End to end: a rule explicitly watching `incomplete` must stay quiet while the transaction can
    still become `success`."""
    await _hard_cleanup()
    try:
        await _committed(LogTransactionStatus.incomplete, sealed=False)
        await _committed_rule(["incomplete"])
        await _run_engine_once()
        assert await _event_count() == 0
    finally:
        await _hard_cleanup()


async def test_engine_alerts_once_the_incomplete_transaction_seals(db):
    """...and does alert once it is permanently incomplete, which is the genuinely useful case."""
    await _hard_cleanup()
    try:
        await _committed(LogTransactionStatus.incomplete, sealed=True)
        await _committed_rule(["incomplete"])
        await _run_engine_once()
        assert await _event_count() == 1
    finally:
        await _hard_cleanup()


async def test_engine_still_alerts_immediately_on_an_unsealed_error(db):
    """The no-regression guard. Gating must not have slowed down the alerts people actually rely on."""
    await _hard_cleanup()
    try:
        await _committed(LogTransactionStatus.error, sealed=False)
        await _committed_rule(["error"])
        await _run_engine_once()
        assert await _event_count() == 1
    finally:
        await _hard_cleanup()
