"""Chunk 65 (section 18q of docs/analytics-ml-architecture/final_architecture.md): the bounded
backward window extension - the cross-pad fix that replaced the "cross-pad _persist" design.

The case: a transaction STARTED before a rebuild window's padded floor (`lo_p`), so it is not freed,
while a NEW entry inside the window would genuinely continue it (within the 300 s stream gap of its
end). Today that entry becomes an orphan fragment, and S3's fingerprint skip makes the fragmentation
permanent - nothing ever revisits it. Structurally this needs the old transaction to span more than
pad - gap = 600 s, which live traffic does not currently produce (max measured span 363.7 s), so this
is INSURANCE for long-span transactions and backfill healing, not a fix for a daily bug.

The design deliberately adds NO new persistence path. When a joinable candidate exists, the window's
padded floor is moved back to the candidate's start (bounded), and the EXISTING cold rebuild - the one
algorithm that has always been authoritative - sees the whole conversation and joins it through the
already-tested grouping, fingerprinting, update-in-place and ticketing machinery. The rejected
alternative (attaching to an exact transaction id inside `_persist`) could not satisfy its own span
guard and re-opened the phantom-cascade divergence fixed on 2026-08-25 (section 18p).

Three load-bearing amendments from the adversarial design review, each pinned below:

- The extension floor is `pad + gap` (1200 s), NOT `pad`: a legal transaction ending within `gap` of
  `lo_p` can start up to `pad + gap` back, and a floor at `pad` would dead-letter the healthy
  600-900 s spans this feature exists to serve.
- The ownerless-entry precondition is MANDATORY, not an optimisation: something ends within `gap` of
  `lo_p` on almost every live tick, and extending for candidates nothing can join would re-free the
  freshly-sealed band every tick (a `row_only` rewrite + `updated_at` bump + notification-cursor
  re-entry, fleet-wide). Ownerless entries sit at/after the ticket's `lo` in steady state - a full
  pad above the candidate band - so the precondition makes steady-state cost exactly zero.
- A candidate that is joinable but starts BELOW the floor has a span over the pad: rebuilding it
  partially would violate the losslessness invariant silently, so the window RAISES and the ticket
  retries and dead-letters loudly (the chosen refusal semantics).

Everything here commits (regroup_window opens real transactions over committed rows), chunk-45/60
style: per-file tenant, frozen T0, wipe before and after."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_open_stream import LogOpenStream, LogPendingRequest
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.settings import settings

CC = "test_chunk65"
CC2 = "test_chunk65_other"
T0 = datetime(2026, 8, 14, 10, 0, tzinfo=timezone.utc)

PAD = timedelta(seconds=900)
GAP = timedelta(seconds=300)


async def _wipe():
    async with async_session() as db:
        for cc in (CC, CC2):
            for model in (LogOpenStream, LogPendingRequest, AnalyticsPendingWindow,
                          LogRegroupPending, LogEntryAssignment, LogTransaction, LogEntry):
                await db.execute(delete(model).where(model.customer_code == cc))
            await db.execute(delete(Job).where(Job.customer_code == cc))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


@pytest.fixture
def cross_pad_on(monkeypatch):
    monkeypatch.setattr(settings, "stage2_cross_pad", "on", raising=False)


async def _job(db, cc=CC) -> Job:
    job = Job(customer_code=cc, filename="t.log", document_type="transaction_log",
              storage_key=f"{cc}/{uuid.uuid4().hex}/t.log", status="completed")
    db.add(job)
    await db.flush()
    return job


def _entry(job, kind, at, line, *, cc=CC, thread="T1", user="amin") -> LogEntry:
    return LogEntry(id=uuid.uuid4(), customer_code=cc, job_id=job.id, timestamp=at,
                    source_file="a.log", line_number=line, level="INFO", raw_body="x",
                    message="x", entry_hash=uuid.uuid4().hex, entry_type=kind,
                    thread=thread, user_ctx=user, fields={})


async def _plant_entries(specs, *, cc=CC) -> None:
    """Commit raw entries the way Stage 1 leaves them: unassigned, ready to stitch."""
    async with async_session() as db:
        job = await _job(db, cc)
        for kind, at, line in specs:
            db.add(_entry(job, kind, at, line, cc=cc))
        await db.commit()


async def _stitch(lo, hi, *, cc=CC) -> dict:
    async with async_session() as db:
        return await dt.regroup_window(db, cc, lo, hi)


async def _txns(cc=CC) -> list[LogTransaction]:
    async with async_session() as db:
        return list((await db.execute(select(LogTransaction).where(
            LogTransaction.customer_code == cc)
            .order_by(LogTransaction.started_at.nullslast()))).scalars().all())


#: The candidate conversation, POST-shaped (request + body first, in stream order) because a GET's
#: late-bound request is appended at the builder's END, which makes `last_ts` read the OLDEST
#: timestamp and evict the builder early on synthetic spacings. Real GET conversations bind within
#: milliseconds so production never sees that; the POST shape keeps the fixture honest.
_CANDIDATE = (
    ("request", timedelta(0), 1),
    ("request_body", timedelta(seconds=1), 2),
    ("info", timedelta(seconds=250), 3),
    ("info", timedelta(seconds=500), 4),
    ("info", timedelta(seconds=700), 5),
)


async def _candidate_via_window_one(*, cc=CC) -> LogTransaction:
    """A 700 s-span incomplete conversation, stitched by its own (earlier) window - the shape a
    prior tick leaves behind. Span > 600 s is the structural threshold below which a cross-pad
    join is arithmetically impossible, so anything smaller would test nothing."""
    await _plant_entries([(k, T0 + d, n) for k, d, n in _CANDIDATE], cc=cc)
    await _stitch(T0, T0 + timedelta(seconds=700), cc=cc)
    txns = await _txns(cc)
    assert len(txns) == 1 and txns[0].status.value == "incomplete", "window-one setup broke"
    return txns[0]


# ==================================================== 1. the flagship: failure mode 1

async def test_cross_pad_late_entry_joins_its_transaction(cross_pad_on):
    """The single most important test in the plan: a RESPONSE arriving in a later window than its
    REQUEST becomes ONE transaction, not two. The response lands at T0+950; the later ticket's padded
    floor is T0+50, so the T0-anchored conversation is invisible to a plain rebuild and the response
    would fragment - permanently, because of S3. With the extension the floor moves back to T0 and
    the ordinary cold rebuild joins it."""
    before = await _candidate_via_window_one()

    await _plant_entries([("response", T0 + timedelta(seconds=950), 6)])
    await _stitch(T0 + timedelta(seconds=950), T0 + timedelta(seconds=960))

    txns = await _txns()
    assert len(txns) == 1, f"expected one healed transaction, got {len(txns)}"
    healed = txns[0]
    assert healed.id == before.id, "the conversation must keep its identity"
    assert healed.status.value != "incomplete", "the response must have closed it"
    assert healed.ended_at == T0 + timedelta(seconds=950)
    assert healed.entry_count == 6


async def test_without_the_extension_the_late_entry_fragments():
    """The counterfactual the flagship is measured against, and the documentation of today's
    behaviour: with the feature off, the same late response becomes its own fragment transaction.
    If this test ever fails, the fragmentation happened differently and the flagship's premise
    needs re-deriving."""
    await _candidate_via_window_one()

    await _plant_entries([("response", T0 + timedelta(seconds=950), 6)])
    await _stitch(T0 + timedelta(seconds=950), T0 + timedelta(seconds=960))

    txns = await _txns()
    assert len(txns) == 2, "off must mean today's behaviour: the fragment exists"


# ==================================================== 2. the mandatory precondition

async def test_extension_never_fires_in_steady_state(cross_pad_on):
    """Something ends within the gap of `lo_p` on nearly every live tick, so without the ownerless
    precondition the detector would extend ~every window and re-free the freshly-sealed band - one
    pointless rewrite and one notification re-entry per transaction, fleet-wide. A candidate with
    nothing new to give it must leave the window bounds and the candidate row byte-alone."""
    await _plant_entries([(k, T0 + d, n) for k, d, n in _CANDIDATE]
                         + [("response", T0 + timedelta(seconds=950), 6)])
    await _stitch(T0, T0 + timedelta(seconds=960))
    healed = (await _txns())[0]
    untouched = healed.updated_at

    stats = await _stitch(T0 + timedelta(seconds=950), T0 + timedelta(seconds=960))

    assert stats.get("cross_pad", {}).get("extended_seconds", 0) == 0
    assert (await _txns())[0].updated_at == untouched, (
        "a no-op window must not rewrite the candidate")


# ==================================================== 3. the floor is pad + gap, not pad

async def test_a_legal_span_candidate_does_not_dead_letter(cross_pad_on):
    """The review's amendment B1. A 700 s conversation ending exactly `gap` before `lo_p` starts
    `pad + 100 s` before it - inside the extension band only because the floor is `pad + gap`.
    A floor at `pad` would raise here, dead-lettering precisely the healthy long-span population
    this feature exists to serve."""
    await _candidate_via_window_one()
    await _plant_entries([("response", T0 + timedelta(seconds=1000), 6)])

    stats = await _stitch(T0 + timedelta(seconds=1900), T0 + timedelta(seconds=1910))

    txns = await _txns()
    assert len(txns) == 1, "the boundary candidate must merge, not raise"
    assert stats["cross_pad"]["extended_seconds"] > 0


async def test_a_span_over_pad_candidate_raises_and_is_left_for_the_dead_letter(cross_pad_on):
    """A joinable candidate below the floor has a span over the pad; rebuilding it PARTIALLY would
    split it silently - the exact invariant violation the docstring at regroup_window promises not
    to commit. So the window refuses loudly: the raise propagates to finalize_pending, which bumps
    attempts and eventually abandons the ticket with the reason (the chosen refusal semantics)."""
    await _plant_entries([
        ("request", T0, 1),
        ("request_body", T0 + timedelta(seconds=1), 2),
        ("info", T0 + timedelta(seconds=290), 3),
        ("info", T0 + timedelta(seconds=580), 4),
        ("info", T0 + timedelta(seconds=870), 5),
        ("info", T0 + timedelta(seconds=1160), 6),
        ("info", T0 + timedelta(seconds=1450), 7),
        ("info", T0 + timedelta(seconds=1500), 8),
    ])
    await _stitch(T0, T0 + timedelta(seconds=1500))
    assert len(await _txns()) == 1

    # joinable (100 s after the candidate's end) and inside the probe range, but the candidate
    # starts 1300 s before this window's padded floor - beyond pad + gap.
    await _plant_entries([("info", T0 + timedelta(seconds=1600), 9)])

    with pytest.raises(dt.CrossPadSpanExceeded):
        await _stitch(T0 + timedelta(seconds=2200), T0 + timedelta(seconds=2210))


# ==================================================== 4. identity and idempotency

async def test_extension_preserves_an_inherited_id(cross_pad_on):
    """The review's amendment B3, the silent-id-churn trap: the candidate's stored id may be
    INHERITED (continuity), not equal to the id its anchor would mint. The extension must thread the
    moved floor through the owner-map load, or the rebuild mints a fresh id, the stored one falls
    into the vanished DELETE, and every citation to it breaks - silently and permanently."""
    inherited = uuid.uuid4()
    async with async_session() as db:
        job = await _job(db)
        entries = [
            _entry(job, "request", T0, 1),
            _entry(job, "request_body", T0 + timedelta(seconds=1), 2),
            _entry(job, "info", T0 + timedelta(seconds=250), 3),
            _entry(job, "info", T0 + timedelta(seconds=500), 4),
            _entry(job, "info", T0 + timedelta(seconds=700), 5),
        ]
        for e in entries:
            db.add(e)
        await db.flush()
        db.add(LogTransaction(
            id=inherited, customer_code=CC, job_id=job.id, sealed=False,
            started_at=T0, ended_at=T0 + timedelta(seconds=700), date=T0.date(),
            duration_ms=700000, method="Pick", transaction_name="Pick",
            transaction_type="002001", status="incomplete", attributes={}))
        await db.flush()
        for seq, e in enumerate(entries):
            db.add(LogEntryAssignment(entry_id=e.id, entry_ts=e.timestamp,
                                      transaction_id=inherited, seq=seq, customer_code=CC))
        await db.commit()

    await _plant_entries([("response", T0 + timedelta(seconds=950), 6)])
    stats = await _stitch(T0 + timedelta(seconds=950), T0 + timedelta(seconds=960))

    txns = await _txns()
    assert len(txns) == 1
    assert txns[0].id == inherited, "the inherited identity must survive the extension"
    assert stats.get("transactions_deleted", 0) == 0, (
        "an id falling into the vanished branch is the churn this test exists to prevent")


async def test_rerun_of_an_extended_window_writes_nothing(cross_pad_on):
    """Idempotency, inherited from the machinery rather than proven anew: the healed transaction now
    owns the late entry, so the re-run's ownerless probe finds nothing, the floor stays put, and the
    fingerprint skip reports the world unchanged."""
    await _candidate_via_window_one()
    await _plant_entries([("response", T0 + timedelta(seconds=950), 6)])
    await _stitch(T0 + timedelta(seconds=950), T0 + timedelta(seconds=960))
    healed = (await _txns())[0]

    stats = await _stitch(T0 + timedelta(seconds=950), T0 + timedelta(seconds=960))

    assert stats.get("transactions_created", 0) == 0
    assert stats.get("transactions_rewritten", 0) == 0
    assert stats.get("transactions_deleted", 0) == 0
    again = (await _txns())[0]
    assert again.id == healed.id and again.updated_at == healed.updated_at


# ==================================================== 5. sealed candidates and the ticket

async def test_a_sealed_cross_pad_candidate_is_freed_and_healed(cross_pad_on):
    """Backfill consistency. Inside a window, regroup_window frees sealed rows deliberately - that is
    what makes back-filling a sealed region lossless. The realistic cross-pad candidate is ALREADY
    sealer-sealed by the time a backfilled late entry arrives, so excluding sealed rows from the
    detector would silently miss the main real-world case."""
    before = await _candidate_via_window_one()
    async with async_session() as db:
        from sqlalchemy import update
        await db.execute(update(LogTransaction).where(LogTransaction.id == before.id)
                         .values(sealed=True))
        await db.commit()

    await _plant_entries([("response", T0 + timedelta(seconds=950), 6)])
    await _stitch(T0 + timedelta(seconds=950), T0 + timedelta(seconds=960))

    txns = await _txns()
    assert len(txns) == 1 and txns[0].id == before.id
    assert txns[0].entry_count == 6


async def test_the_ticket_covers_the_extended_floor(cross_pad_on):
    """Invariant 2 of the analytics ticket contract: no path changes a transaction without a
    committed ticket whose range contains its `started_at`. The extension moves work back to T0, so
    the ticket published by this window must reach at least that far - it does, structurally, because
    the publish reads the same moved floor the rebuild uses. The geometry is the far-band one (the
    ticket sits pad+gap above T0) because a nearer window's ticket covers T0 through the publisher's
    own second pad even WITHOUT the extension, which would pass vacuously."""
    await _candidate_via_window_one()
    async with async_session() as db:
        await db.execute(delete(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC))
        await db.commit()

    await _plant_entries([("response", T0 + timedelta(seconds=1000), 6)])
    await _stitch(T0 + timedelta(seconds=1900), T0 + timedelta(seconds=1910))

    async with async_session() as db:
        covered = (await db.execute(select(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC,
            AnalyticsPendingWindow.range_start <= T0,
            AnalyticsPendingWindow.range_end >= T0))).scalars().all()
    assert covered, "the healed transaction's started_at must be inside a published ticket"


# ==================================================== 6. shadow mode and isolation

async def test_shadow_mode_only_reports(monkeypatch):
    """Shadow is the deploy default: it measures the population without changing a single row, so
    the counts can be reviewed before anything is enabled. The fragment still happens (today's
    behaviour), and the stats say what WOULD have been done."""
    monkeypatch.setattr(settings, "stage2_cross_pad", "shadow", raising=False)
    await _candidate_via_window_one()

    await _plant_entries([("response", T0 + timedelta(seconds=950), 6)])
    stats = await _stitch(T0 + timedelta(seconds=950), T0 + timedelta(seconds=960))

    assert len(await _txns()) == 2, "shadow must not heal - it only measures"
    report = stats.get("cross_pad", {})
    assert report.get("would_extend_seconds", 0) > 0
    assert report.get("candidates", 0) >= 1


async def test_extension_respects_tenant_isolation(cross_pad_on):
    """The detector must carry customer_code: another tenant's boundary conversation is not a reason
    to widen this tenant's window."""
    await _candidate_via_window_one(cc=CC2)

    await _plant_entries([("response", T0 + timedelta(seconds=950), 6)])
    stats = await _stitch(T0 + timedelta(seconds=950), T0 + timedelta(seconds=960))

    assert stats.get("cross_pad", {}).get("extended_seconds", 0) == 0
    assert len(await _txns(CC2)) == 1, "the other tenant's conversation must be untouched"
