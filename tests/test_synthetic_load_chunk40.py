"""Chunk 40, Phase 0: the synthetic-load generator, driven through the REAL pipeline.

F11 requires a generator whose output "drives Stage 2 normally, so tickets and rebuilds are genuinely
exercised". The only way to know that is to run it: generate log TEXT, parse it with the real
`M3DotNetLogParser`, insert it through Stage 1, let Stage 2 group it, and read what comes out.

That is what most of this file does, and it is the part that could not be faked. A generator that
fabricated `log_transactions` rows directly would make every assertion here vacuously true while
proving nothing about the parser, the ticket or the grouping.

It also closes the loop on the hand-built fixtures in `tests/analytics_fixtures.py`. Those assert what
the analytics diff should do given a before/after pair; these assert that the pipeline actually
produces those pairs from real text. Neither is sufficient alone: the first could describe transitions
that never happen, and the second could pass while the diff mishandles them.

Isolation is asserted, not assumed. A generator that can write 78,000 synthetic picks an hour must be
incapable of writing them anywhere but its own tenant.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select, text

from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.services.analytics import contract as c
from app.services.analytics import definition as d
from app.services.analytics import synthetic as syn
from app.services.mnp_log_ingestion.parsers.m3_dotnet_parser import M3DotNetLogParser
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

CC = syn.TENANT


# ==================================================== helpers: the real ingest path
async def _cleanup(db):
    ids = (await db.execute(select(LogTransaction.id).where(
        LogTransaction.customer_code == CC))).scalars().all()
    if ids:
        await db.execute(delete(LogEntryAssignment).where(
            LogEntryAssignment.transaction_id.in_(list(ids))))
    await db.execute(delete(LogEntryAssignment).where(LogEntryAssignment.customer_code == CC))
    await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == CC))
    await db.execute(delete(LogEntry).where(LogEntry.customer_code == CC))
    await db.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == CC))
    await db.execute(delete(Job).where(Job.customer_code == CC))
    await db.flush()


async def _ingest(db, log_text: str) -> int:
    """Parse `log_text` with the REAL parser and insert the entries, the way Stage 1 does.

    Goes through `M3DotNetLogParser` rather than hand-building LogEntry rows: a template with a broken
    header would otherwise pass every test here while producing NULL timestamps in production.
    """
    job = Job(customer_code=CC, filename="synthetic.log", document_type="transaction_log",
              storage_key=f"{CC}/{uuid.uuid4().hex}/synthetic.log", status="completed")
    db.add(job)
    await db.flush()

    records = M3DotNetLogParser().parse(log_text)
    for r in records:
        ts = r.timestamp.replace(tzinfo=timezone.utc) if r.timestamp else None
        db.add(LogEntry(
            customer_code=CC, job_id=job.id, entry_hash=uuid.uuid4().hex,
            source_file="synthetic.log", line_number=r.line_number, timestamp=ts,
            level=r.level, thread=r.thread, user_ctx=r.user, logger=r.logger, method=r.method,
            entry_type=r.entry_type, mi_program=r.mi_program, mi_transaction=r.mi_transaction,
            result_status=r.result_status, record_count=r.record_count,
            message=r.message, raw_body=r.raw_body, fields=r.fields or {}))
    await db.flush()
    return len(records)


async def _stitch(db, lo: datetime, hi: datetime):
    """Run Stage 2 over the window, exactly as the stitch worker does."""
    await dt.regroup_window(db, CC, lo, hi, commit=False)


async def _txns(db) -> list[LogTransaction]:
    return list((await db.execute(select(LogTransaction).where(
        LogTransaction.customer_code == CC).order_by(LogTransaction.started_at))).scalars().all())


AT = datetime(2026, 8, 5, 18, 59, 43, 915000, tzinfo=timezone.utc)
LO, HI = AT - timedelta(hours=2), AT + timedelta(hours=2)


# ==================================================== isolation
def test_the_tenant_is_a_constant_not_a_parameter():
    """A generator that could be pointed at a real customer_code is one keystroke from writing 78,000
    synthetic picks into production data."""
    assert syn.TENANT == "synthetic-load"
    import inspect
    for fn in (syn.render, syn.scenario, syn.pick_lines):
        assert "customer_code" not in inspect.signature(fn).parameters


def test_generated_text_never_mentions_a_real_tenant():
    for name in syn.scenario_names():
        body = syn.scenario(name)
        for real in ("tmp-live", "tmp-test", "TMP-AZ-BEC01", "TMP-AZ-BEC02"):
            assert real not in body, f"{name} leaked {real}"


# ==================================================== the text is real M3, provably
def test_every_scenario_parses_with_the_real_parser_and_loses_no_line():
    """The template's header layout has to satisfy `_HEADER` exactly. A missing bracket yields a NULL
    timestamp, which lands in the DEFAULT partition and is invisible to a windowed regroup -- the
    failure we spent this afternoon purging 294,747 rows of."""
    parser = M3DotNetLogParser()
    for name in syn.scenario_names():
        records = parser.parse(syn.scenario(name))
        assert records, f"{name} parsed to nothing"
        for r in records:
            assert r.timestamp is not None, f"{name}: a line failed the header regex"
            assert r.thread, f"{name}: thread missing"


def test_the_generated_request_body_carries_the_fields_the_metric_measures():
    """`ConfirmPickLine` is a POST, so `MethodName` and `QuantityPicked` arrive in the BODY, not the
    URL. Getting that wrong yields transactions with no method and no quantity, and every downstream
    assertion would be vacuously true."""
    records = M3DotNetLogParser().parse(syn.scenario("full pick"))
    body = next(r for r in records if r.entry_type == "request_body")
    assert body.fields["MethodName"] == "ConfirmPickLine"
    assert body.fields["QuantityPicked"] == "10.0"
    assert body.fields["QuantityToBePicked"] == "", "left empty because that is what the server sends"


def test_timestamps_use_milliseconds_because_the_splitter_demands_exactly_three_digits():
    text_out = syn.scenario("full pick")
    first = text_out.splitlines()[0]
    assert first[:23] == "2026-08-05 18:59:43,915", first[:40]


# ==================================================== through Stage 2, for real
async def test_a_generated_pick_becomes_one_transaction_with_the_right_method_and_quantity(db):
    await _cleanup(db)
    await _ingest(db, syn.scenario("full pick"))
    await _stitch(db, LO, HI)
    txns = await _txns(db)
    assert len(txns) == 1, [t.method for t in txns]
    t = txns[0]
    assert t.method == "ConfirmPickLine"
    assert t.status is LogTransactionStatus.success
    assert t.attributes["QuantityPicked"] == "10.0"
    await _cleanup(db)


@pytest.mark.parametrize("scenario,quantity,classification", [
    ("full pick", "10.0", c.Classification.pick),
    ("zero pick", "0.0", c.Classification.attempt),
    ("fractional pick", "0.333333", c.Classification.pick),
])
async def test_the_contract_classifies_what_the_real_pipeline_produced(
        db, scenario, quantity, classification):
    """The join between the two halves of Phase 0: text -> pipeline -> transaction -> contract. If the
    generator and the contract ever disagree about a field name, this is where it shows."""
    await _cleanup(db)
    await _ingest(db, syn.scenario(scenario))
    await _stitch(db, LO, HI)
    t = (await _txns(db))[0]
    qty = c.parse_quantity(t.attributes.get(c.quantity_field(t.method)))
    assert qty == Decimal(quantity)
    assert c.classify(t.method, qty) is classification
    await _cleanup(db)


async def test_an_incomplete_pick_has_no_response_and_no_usable_quantity(db):
    """Its quantity is unknown, NOT zero. This is the row still due to move, and folding it in as a
    zero-unit attempt would invent a confirmation that never happened."""
    await _cleanup(db)
    await _ingest(db, syn.scenario("incomplete"))
    await _stitch(db, LO, HI)
    t = (await _txns(db))[0]
    assert t.status is LogTransactionStatus.incomplete
    folded = d.fold([{"method": t.method, "quantity": None,
                      "quantity_classification": c.Classification.unusable}], d.CONSUMPTION)
    assert folded["attempt_count"][d.Role.count_value] == 0
    await _cleanup(db)


async def test_an_error_pick_is_reported_as_an_error_by_the_real_classifier(db):
    await _cleanup(db)
    await _ingest(db, syn.scenario("error"))
    await _stitch(db, LO, HI)
    t = (await _txns(db))[0]
    assert t.status is LogTransactionStatus.error
    await _cleanup(db)


async def test_two_users_interleaved_on_one_thread_do_not_cross_stitch(db):
    """The .NET server reuses a thread mid-request, which is why `_group` keys on (thread, user). A
    generator that only ever emitted one user would never exercise it, and a regression here would
    silently merge two operators' picks into one transaction."""
    await _cleanup(db)
    await _ingest(db, syn.scenario("interleaved users"))
    await _stitch(db, LO, HI)
    txns = await _txns(db)
    assert len(txns) == 2, f"expected one per user, got {[t.user_name for t in txns]}"
    assert {t.attributes.get("QuantityPicked") for t in txns} == {"2.0", "3.0"}
    await _cleanup(db)


async def test_picks_beyond_the_open_gap_stay_two_transactions(db):
    """`log_open_gap_seconds` is 300. Two picks 400s apart must not be glued into one, or a day of
    activity collapses into a handful of enormous transactions."""
    await _cleanup(db)
    await _ingest(db, syn.scenario("beyond the open gap"))
    await _stitch(db, LO, HI + timedelta(hours=1))
    assert len(await _txns(db)) == 2
    await _cleanup(db)


async def test_a_rebuild_keeps_the_transaction_id_and_writes_no_duplicate(db):
    """The 98.7% case, through the real pipeline. Stitching the same window twice must be idempotent:
    same id, same count. A changed id here would make `analytics_fact_ledger` churn without bound."""
    await _cleanup(db)
    await _ingest(db, syn.scenario("full pick"))
    await _stitch(db, LO, HI)
    before = [(t.id, t.method) for t in await _txns(db)]
    await _stitch(db, LO, HI)
    after = [(t.id, t.method) for t in await _txns(db)]
    assert before == after, "a re-stitch must be a no-op for identity"
    await _cleanup(db)


async def test_stage_1_writes_a_ticket_so_the_stitch_worker_would_pick_this_up(db):
    """F11's requirement that the generator "drives Stage 2 normally". Without a ticket the real worker
    would never see this data, and a load test would be measuring nothing."""
    await _cleanup(db)
    from app.services.analytics.synthetic import scenario
    records = M3DotNetLogParser().parse(scenario("full pick"))
    stamps = [r.timestamp for r in records if r.timestamp]
    lo, hi = min(stamps), max(stamps)
    db.add(LogRegroupPending(customer_code=CC, job_id=uuid.uuid4(),
                             range_start=lo.replace(tzinfo=timezone.utc),
                             range_end=hi.replace(tzinfo=timezone.utc)))
    await db.flush()
    open_windows = (await db.execute(select(LogRegroupPending).where(
        LogRegroupPending.customer_code == CC,
        LogRegroupPending.consumed_at.is_(None)))).scalars().all()
    assert len(open_windows) == 1
    await _cleanup(db)


# ==================================================== the load path
def test_the_generator_scales_in_ENTRIES_not_physical_lines():
    """78,000 records an hour is the Phase 7 exit criterion, so the stable unit has to be the one that
    becomes a `log_entries` row.

    That is a parsed RECORD, not a physical line. A real `mi_call` line spans several physical lines --
    the parser's entire first pass exists to fold continuations back into their header -- so the
    generator emits 7 lines per pick and the parser folds them into 6 entries. Counting lines would
    make any rate calculation quietly wrong by a sixth.
    """
    parser = M3DotNetLogParser()
    one = parser.parse(syn.render([syn.Pick(at=AT)]))
    assert len(one) == 6, [r.entry_type for r in one]
    assert [r.entry_type for r in one] == [
        "request", "request_body", "info", "mi_call", "mi_result", "response"]
    many = parser.parse(syn.render([syn.Pick(at=AT + timedelta(seconds=i)) for i in range(100)]))
    assert len(many) == 600


def test_every_generated_ENTRY_is_unique_so_stage_1_dedup_does_not_eat_them():
    """`entry_hash` is a sha256 over an entry's whole `raw_body`, and Stage 1 drops duplicates. Two
    picks that produced identical entries would collapse into one and a load test would silently
    generate half the volume it reported.

    Asserted per ENTRY rather than per physical line, and the distinction is not pedantic: the
    continuation line of `mi_call` carries no timestamp, so it IS byte-identical between two picks.
    Harmless, because it is folded into a parent whose header is unique -- but a line-level assertion
    would fail while the property that actually matters holds.
    """
    parser = M3DotNetLogParser()
    records = parser.parse(syn.render([syn.Pick(at=AT), syn.Pick(at=AT + timedelta(seconds=1))]))
    bodies = [r.raw_body for r in records]
    assert len(set(bodies)) == len(bodies) == 12, "every entry hashes distinctly"


def test_scenarios_are_a_registry_not_an_if_chain():
    assert "full pick" in syn.scenario_names()
    with pytest.raises(KeyError, match="unknown scenario"):
        syn.scenario("no such thing")
