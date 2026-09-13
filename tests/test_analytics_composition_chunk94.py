"""Chunk 94: what a fact is composed of - request and response by default, MI opt-in per transaction,
bare responses kept.

Decided 2026-09-13 against live data. A fact is the business event: the request the device sent,
the response it got, and the typed columns. The M3 calls in between are the transaction's business,
kept in full by Stage 2's timeline and, as rows, by the record grain. Their summary on the fact added
nothing `status` did not already say (0 of 49,087 facts in a week were success with a failed last MI
call, and no metric named an `mi.*` key), so it is no longer written unless a transaction's new `mi`
switch asks for it - and then per KIND of call, so a transaction with 39 calls of 5 kinds is 5 groups.

A response that is a bare value (`{"response": "3.333333"}`, every recent pick) used to be dropped
because it had no field name. It is kept as `resp.value`.

Pinned here
-----------
    resp.value      bare string or number -> resp.value; empty string -> nothing; an object -> per key
    mi off          extract yields no mi.* at all
    mi on           mi.<program>.<transaction>.{calls, errors, result, record_count} per kind, with the
                    three live shapes as fixtures; missing program or transaction -> "_"
    seeding         resp.value and per-kind mi names arrive approved; the four legacy names do not
    bookkeeping     __mi_records carries the record total regardless of the switch, so the record
                    grain's zero-record gate still works; legacy mi.record_count still honoured
    switch          analytics_transaction_registry.mi, default off; PATCH mi=true publishes tickets,
                    mi=false publishes none; list, detail and catalog carry it
    version         _NORMALISE_VERSION is 2, so stored facts are restated when their range is folded
    end to end      a folded pick carries resp.value and no mi.*; with mi on, per-kind keys; a
                    preview may measure attr:resp.value; the composition endpoint groups a sample
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, update

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import capture
from app.services.analytics import consume as n3
from app.services.analytics import payload as p
from app.services.analytics.contract import QUANTITY_FIELD as QF

CC = "test_chunk94"
T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsRecordFact, AnalyticsPendingWindow,
          AnalyticsTenantState, AnalyticsFieldRegistry, AnalyticsTransactionRegistry,
          LogEntryAssignment, LogEntry, LogTransaction)


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="composition probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


# =============================================================== 1. extract: the response half

def test_a_bare_string_response_is_kept_as_resp_value():
    assert p.extract([("response", {"response": "3.333333"})]) == {"resp.value": "3.333333"}


def test_a_bare_number_response_is_kept_as_resp_value():
    assert p.extract([("response", {"response": 0})]) == {"resp.value": 0}
    assert p.extract([("response", {"response": True})]) == {"resp.value": True}


def test_an_empty_string_response_is_still_nothing():
    assert p.extract([("response", {"response": ""})]) == {}
    assert p.extract([("response", {"response": "   "})]) == {}


def test_an_object_response_is_still_unwrapped_per_key_and_has_no_resp_value():
    got = p.extract([("response", {"response": {"StockZone": "A1"}})])
    assert got == {"resp.StockZone": "A1"}


def test_resp_value_is_seeded_and_not_credential_shaped():
    assert p.seeded("resp.value")


# =============================================================== 2. extract: MI off and on

PICK = [("mi_result", {"result": "OK", "program": "MMS060MI", "transaction": "LstBalID",
                       "records": [{"STQT": "624"}, {"STQT": "12"}]}),
        ("mi_result", {"result": "OK", "program": "MHS850MI", "transaction": "AddPickViaRepNo",
                       "records": [{"RPNO": "1"}]}),
        ("response", {"response": "3.333333"})]


def test_with_mi_off_no_mi_key_is_written():
    got = p.extract(PICK)
    assert got == {"resp.value": "3.333333"}
    assert not any(k.startswith("mi.") for k in got)


def test_with_mi_on_each_kind_of_call_is_one_group():
    got = p.extract(PICK, mi=True)
    assert got == {
        "resp.value": "3.333333",
        "mi.MMS060MI.LstBalID.calls": 1, "mi.MMS060MI.LstBalID.errors": 0,
        "mi.MMS060MI.LstBalID.result": "OK", "mi.MMS060MI.LstBalID.record_count": 2,
        "mi.MHS850MI.AddPickViaRepNo.calls": 1, "mi.MHS850MI.AddPickViaRepNo.errors": 0,
        "mi.MHS850MI.AddPickViaRepNo.result": "OK", "mi.MHS850MI.AddPickViaRepNo.record_count": 1,
    }


def test_repeated_calls_of_one_kind_collapse_into_counters():
    """ListShipmentPackagesToLoad on tmp-live: LstPack once, LstContents 38 times."""
    entries = [("mi_result", {"result": "OK", "program": "MWS423MI", "transaction": "LstPack",
                              "records": [{}] * 43})]
    entries += [("mi_result", {"result": "OK", "program": "MWS423MI", "transaction": "LstContents",
                               "records": [{}] * n}) for n in ([1] * 30 + [7, 5] + [3] * 6)]
    got = p.extract(entries, mi=True)
    assert got["mi.MWS423MI.LstPack.calls"] == 1 and got["mi.MWS423MI.LstPack.record_count"] == 43
    assert got["mi.MWS423MI.LstContents.calls"] == 38
    assert got["mi.MWS423MI.LstContents.record_count"] == 30 + 7 + 5 + 18
    assert len([k for k in got if k.startswith("mi.")]) == 8, "two kinds, four keys each"


def test_errors_count_the_failed_calls_and_result_keeps_the_last():
    """A pick retried after M3 rejected the first posting."""
    entries = [("mi_result", {"result": "Available quantity is 1.000000 for the line",
                              "program": "MHS850MI", "transaction": "AddPickViaRepNo"}),
               ("mi_result", {"result": "OK", "program": "MHS850MI", "transaction": "AddPickViaRepNo",
                              "records": [{}]})]
    got = p.extract(entries, mi=True)
    assert got["mi.MHS850MI.AddPickViaRepNo.calls"] == 2
    assert got["mi.MHS850MI.AddPickViaRepNo.errors"] == 1
    assert got["mi.MHS850MI.AddPickViaRepNo.result"] == "OK"
    assert got["mi.MHS850MI.AddPickViaRepNo.record_count"] == 1


def test_a_call_without_program_or_transaction_groups_under_an_underscore():
    got = p.extract([("mi_result", {"result": "OK"})], mi=True)
    assert got == {"mi._._.calls": 1, "mi._._.errors": 0, "mi._._.result": "OK", "mi._._.record_count": 0}


def test_ten_kinds_are_ten_groups():
    """GetNextDeliveryByRoute on tmp-live: 15 calls of 10 kinds."""
    kinds = [("MWS420MI", "LstPickersPL", 3), ("MWS420MI", "LstPickList", 2), ("MWS420MI", "UpdPickHead", 1),
             ("MWS410MI", "GetHead", 1), ("MWS423MI", "RtvLastPkg", 1), ("MWS423MI", "LstPackages", 1),
             ("MWS422MI", "LstPickDetail", 1), ("OIS100MI", "LstLine", 2), ("MMS200MI", "LstItmAltUnitMs", 2),
             ("CUSEXTMI", "GetFieldValue", 1)]
    entries = [("mi_result", {"result": "OK", "program": pr, "transaction": tx, "records": [{}]})
               for pr, tx, n in kinds for _ in range(n)]
    got = p.extract(entries, mi=True)
    assert len(got) == 40
    assert got["mi.MWS420MI.LstPickersPL.calls"] == 3 and got["mi.OIS100MI.LstLine.calls"] == 2


def test_per_kind_mi_names_are_seeded_but_the_legacy_names_are_not():
    for name in ("mi.MHS850MI.AddPickViaRepNo.calls", "mi.MHS850MI.AddPickViaRepNo.errors",
                 "mi.MHS850MI.AddPickViaRepNo.result", "mi.MHS850MI.AddPickViaRepNo.record_count"):
        assert p.seeded(name), name
    for legacy in ("mi.program", "mi.transaction", "mi.result", "mi.record_count"):
        assert not p.seeded(legacy), legacy
    assert not p.seeded("mi.X.Y.AccessToken"), "the credential veto still wins"


def test_record_total_counts_every_record_regardless_of_the_switch():
    assert p.record_total(PICK) == 3
    assert p.record_total([("mi_result", {"result": "OK"}), ("response", {"response": "x"})]) == 0


def test_the_record_rows_extractor_is_untouched_by_the_switch():
    recs = p.records(PICK)
    assert [r["mi_transaction"] for r in recs] == ["LstBalID", "LstBalID", "AddPickViaRepNo"]


# =============================================================== 3. the record gate survives

def test_predicts_records_reads_the_bookkeeping_key_and_falls_back_to_the_legacy_one():
    assert n3._predicts_records({"attributes": {n3._MI_RECORDS_KEY: 3}})
    assert not n3._predicts_records({"attributes": {n3._MI_RECORDS_KEY: 0}})
    assert n3._predicts_records({"attributes": {"mi.record_count": 2}}), "a pre-v2 fact"
    assert not n3._predicts_records({"attributes": {}})


def test_the_normalisation_version_was_bumped():
    assert n3._NORMALISE_VERSION == 2


# =============================================================== 4. the switch

async def _row(name="Pick", **kw) -> AnalyticsTransactionRegistry:
    async with async_session() as db:
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name=name, **kw))
        db.add(AnalyticsTenantState(customer_code=CC, source_watermark=T0,
                                    history_starts_at=T0 - timedelta(days=3)))
        await db.commit()
    async with async_session() as db:
        return await db.scalar(select(AnalyticsTransactionRegistry).where(
            AnalyticsTransactionRegistry.customer_code == CC,
            AnalyticsTransactionRegistry.transaction_name == name))


async def _tickets():
    async with async_session() as db:
        return list((await db.execute(select(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC))).scalars().all())


async def test_mi_defaults_off_and_is_an_inclusion_list_like_expand():
    row = await _row()
    assert row.mi is False
    async with async_session() as db:
        assert await capture.mi_names(db, CC) == frozenset()
        await db.execute(update(AnalyticsTransactionRegistry).where(
            AnalyticsTransactionRegistry.id == row.id).values(mi=True))
        await db.commit()
    async with async_session() as db:
        assert await capture.mi_names(db, CC) == frozenset({"Pick"})


async def test_patch_mi_on_publishes_ordinary_tickets_and_off_publishes_none():
    await _row()
    async with async_session() as db:
        out = await api.set_transaction_switches("Pick", payload={"mi": True}, customer=CC, db=db)
    assert out["mi"] is True and out["tickets_published"] >= 1
    tickets = await _tickets()
    assert tickets and not any(t.refold_rollups for t in tickets), \
        "the facts themselves change, so the diff finds them; no refold flag needed"
    n = len(tickets)
    async with async_session() as db:
        out = await api.set_transaction_switches("Pick", payload={"mi": False}, customer=CC, db=db)
    assert out["mi"] is False and out["tickets_published"] == 0
    assert len(await _tickets()) == n


async def test_list_detail_and_catalog_carry_the_switch():
    await _row(mi=True)
    async with async_session() as db:
        listed = await api.list_transaction_registry(customer=CC, db=db)
        detail = await api.transaction_registry_detail("Pick", customer=CC, db=db)
        from app.services.analytics import catalog
        body = await catalog.build(db, CC)
    assert listed["transactions"][0]["mi"] is True
    assert detail["mi"] is True
    assert body["transactions"][0]["mi"] is True


# =============================================================== 5. end to end through the fold

async def _plant(*, name="Pick", mi=False, records=True):
    """One sealed ConfirmPickLine with two MI results and a bare response, plus a ticket."""
    async with async_session() as db:
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name=name, mi=mi))
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        txn = LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=T0,
                             ended_at=T0 + timedelta(seconds=2), date=T0.date(), duration_ms=100,
                             method="ConfirmPickLine", transaction_name=name, transaction_type="002001",
                             status=LogTransactionStatus.success, item_number="101998", user_name="EDA",
                             warehouse="BRI", delivery_number="27383", row_fingerprint="fp-1",
                             attributes={QF["ConfirmPickLine"]: "3.333333", "DeliveryNumber": "27383"})
        db.add(txn)
        await db.flush()
        seq = 0
        for kind, fields in (
            ("mi_result", {"result": "OK", "program": "MMS060MI", "transaction": "LstBalID",
                           "records": [{"STQT": "624"}, {"STQT": "12"}] if records else []}),
            ("mi_result", {"result": "OK", "program": "MHS850MI", "transaction": "AddPickViaRepNo",
                           "records": [{"RPNO": "1"}] if records else []}),
            ("response", {"response": "3.333333"}),
        ):
            entry = LogEntry(customer_code=CC, job_id=job.id, timestamp=T0 + timedelta(seconds=1 + seq),
                             line_number=seq + 1, raw_body=kind, entry_hash=uuid.uuid4().hex,
                             source_file="S/x.log", level="INFO", entry_type=LogEntryType(kind),
                             fields=fields)
            db.add(entry)
            await db.flush()
            db.add(LogEntryAssignment(customer_code=CC, entry_id=entry.id, entry_ts=entry.timestamp,
                                      transaction_id=txn.id, seq=seq))
            seq += 1
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()
        return txn.id


async def _fact():
    async with async_session() as db:
        return await db.scalar(select(AnalyticsFact).where(AnalyticsFact.customer_code == CC))


async def test_a_folded_pick_carries_resp_value_and_no_mi_keys_by_default():
    await _plant()
    await n3.consume_tenant(CC)
    fact = await _fact()
    assert fact is not None and fact.quantity is not None
    attrs = fact.attributes
    assert attrs["resp.value"] == "3.333333"
    assert not any(k.startswith("mi.") for k in attrs), sorted(attrs)
    assert attrs[n3._MI_RECORDS_KEY] == 3, "the record total is kept as bookkeeping for the record grain"
    assert attrs["__norm_v"] == 2


async def test_with_mi_on_the_fact_carries_per_kind_groups():
    await _plant(mi=True)
    await n3.consume_tenant(CC)
    attrs = (await _fact()).attributes
    assert attrs["mi.MMS060MI.LstBalID.calls"] == 1 and attrs["mi.MMS060MI.LstBalID.record_count"] == 2
    assert attrs["mi.MHS850MI.AddPickViaRepNo.result"] == "OK"
    assert "mi.program" not in attrs
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsFieldRegistry.field, AnalyticsFieldRegistry.captured,
                                        AnalyticsFieldRegistry.source).where(
            AnalyticsFieldRegistry.customer_code == CC,
            AnalyticsFieldRegistry.field.like("mi.%")))).all()
    assert rows and all(c and s == "mi_result" for _, c, s in rows), rows


async def test_resp_value_is_pickable_in_a_metric_preview():
    await _plant()
    await n3.consume_tenant(CC)
    async with async_session() as db:
        out = await api.preview_metric(payload={
            "name": "echoed", "dimensions": ["method"], "source": "transaction",
            "measures": [{"name": "echo", "aggregation": "sum", "field": "attr:resp.value"}],
            "filter": {"methods": ["ConfirmPickLine"], "transactions": []}, "grains": ["daily"]},
            window_hours=None, customer=CC, db=db)
    assert out["ok"], out["problems"] + out["refusals"]
    assert out["field_coverage"][0]["percent_numeric"] == 100.0


async def test_the_composition_endpoint_groups_a_sample_fact():
    await _plant(mi=True)
    await n3.consume_tenant(CC)
    async with async_session() as db:
        out = await api.transaction_composition("Pick", customer=CC, db=db)
    assert out["transaction_name"] == "Pick"
    assert out["switches"] == {"capture": True, "show": True, "expand": False, "mi": True}
    sample = out["sample"]
    assert sample["method"] == "ConfirmPickLine"
    assert sample["request"]["DeliveryNumber"] == "27383" and "QuantityPicked" in sample["request"]
    assert sample["response"] == {"resp.value": "3.333333"}
    assert sample["mi"] == {
        "MMS060MI.LstBalID": {"calls": 1, "errors": 0, "result": "OK", "record_count": 2},
        "MHS850MI.AddPickViaRepNo": {"calls": 1, "errors": 0, "result": "OK", "record_count": 1},
    }
    assert not any(k.startswith("__") for k in sample["request"]), "bookkeeping keys are not shown"
    assert out["counts"]["facts"] == 1 and out["counts"]["with_resp_value"] == 1 and out["counts"]["with_mi"] == 1
    assert {f["field"] for f in out["fields"]["response"]} >= {"resp.value"}
    assert {f["field"] for f in out["fields"]["mi"]} >= {"mi.MMS060MI.LstBalID.calls"}
    assert [(k["program"], k["transaction"]) for k in out["mi_kinds"]] == \
        [("MMS060MI", "LstBalID"), ("MHS850MI", "AddPickViaRepNo")]


async def test_the_composition_endpoint_lists_mi_kinds_even_when_the_switch_is_off():
    """The screen must show what the transaction DOES before anyone switches MI on. Kinds come from
    the latest transaction's own timeline, not from the fact."""
    await _plant(mi=False)
    await n3.consume_tenant(CC)
    async with async_session() as db:
        out = await api.transaction_composition("Pick", customer=CC, db=db)
    assert out["switches"]["mi"] is False and out["sample"]["mi"] == {}
    assert [(k["program"], k["transaction"], k["calls"], k["records"]) for k in out["mi_kinds"]] == \
        [("MMS060MI", "LstBalID", 1, 2), ("MHS850MI", "AddPickViaRepNo", 1, 1)]


async def test_the_composition_endpoint_collapses_per_method_rows_into_one_entry_per_field():
    """Field registry rows are per M3 method. A transaction served by several methods has the same
    response field registered once per method, and the cards must show the field ONCE - seen live as
    `resp.AccessToken` eleven times on Brighton Stock Pick. Approval is by name across methods
    (`approved_attributes`), so one entry carries every row id and `captured` is true if any row is."""
    await _plant()
    await n3.consume_tenant(CC)
    async with async_session() as db:
        # a second method serving the same name, with the same field registered twice more
        db.add(LogTransaction(customer_code=CC, job_id=(await db.scalar(select(Job.id).where(Job.customer_code == CC))),
                              sealed=True, started_at=T0 + timedelta(hours=2), ended_at=T0 + timedelta(hours=2),
                              date=T0.date(), duration_ms=5, method="GetNextDeliveryByRoute", transaction_name="Pick",
                              status=LogTransactionStatus.success, attributes={}))
        for method, captured in (("ConfirmPickLine", False), ("GetNextDeliveryByRoute", True)):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method=method, source="response",
                                          field="resp.AllocatedQuantity", captured=captured))
        await db.commit()
    async with async_session() as db:
        out = await api.transaction_composition("Pick", customer=CC, db=db)
    names = [f["field"] for f in out["fields"]["response"]]
    assert names.count("resp.AllocatedQuantity") == 1, names
    entry = next(f for f in out["fields"]["response"] if f["field"] == "resp.AllocatedQuantity")
    assert entry["captured"] is True, "approved on any method means approved by name"
    assert sorted(entry["methods"]) == ["ConfirmPickLine", "GetNextDeliveryByRoute"]
    assert len(entry["ids"]) == 2 and entry["id"] in entry["ids"]


async def test_observing_a_field_again_bumps_its_counters_but_never_its_decision():
    """Discovery used ON CONFLICT DO NOTHING, so `seen_count` stayed at 1 and `last_seen_at` at the
    first sighting forever: on tmp-live not one of 1,841 rows had seen_count above 1, and a field
    seen once in August looked exactly like one seen on every pick. The counters now move on every
    sighting; `captured` still never does."""
    async with async_session() as db:
        added = await capture.observe_fields(db, CC, {"ConfirmPickLine": {"resp.value", "resp.Odd"}})
        await db.commit()
    assert sorted(added) == ["resp.Odd", "resp.value"]
    async with async_session() as db:
        await db.execute(update(AnalyticsFieldRegistry).where(
            AnalyticsFieldRegistry.customer_code == CC, AnalyticsFieldRegistry.field == "resp.value")
            .values(captured=False))                       # a person un-ticks the seeded field
        await db.commit()
    async with async_session() as db:
        added = await capture.observe_fields(db, CC, {"ConfirmPickLine": {"resp.value"}})
        await db.commit()
    assert added == [], "seen before: not reported as new"
    async with async_session() as db:
        rows = {r.field: r for r in (await db.execute(select(AnalyticsFieldRegistry).where(
            AnalyticsFieldRegistry.customer_code == CC))).scalars().all()}
    assert rows["resp.value"].seen_count == 2
    assert rows["resp.value"].last_seen_at > rows["resp.value"].first_seen_at
    assert rows["resp.value"].captured is False, "the decision outlives the sighting"
    assert rows["resp.Odd"].seen_count == 1


async def test_the_composition_endpoint_reports_how_often_each_field_appears_on_recent_facts():
    """The registry says a field EXISTS for a method; only the facts say how often. Forty-seven of the
    forty-eight response fields under ConfirmPickLine on tmp-live were seen exactly once, from a
    foreign response object stitched into a pick, and the card showed them as if they were regular."""
    await _plant()
    await n3.consume_tenant(CC)
    async with async_session() as db:
        # a registry row nobody has seen on a recent fact
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="response",
                                      field="resp.Ghost", captured=False))
        await db.commit()
    async with async_session() as db:
        out = await api.transaction_composition("Pick", customer=CC, db=db)
    assert out["counts"]["recent_days"] == 14
    assert out["counts"]["recent_facts"] == 1
    by_name = {f["field"]: f for f in out["fields"]["response"]}
    assert by_name["resp.value"]["recent_facts"] == 1
    assert by_name["resp.Ghost"]["recent_facts"] == 0


async def test_the_composition_endpoint_404s_for_an_unknown_name():
    from fastapi import HTTPException
    async with async_session() as db:
        with pytest.raises(HTTPException) as exc:
            await api.transaction_composition("Nope", customer=CC, db=db)
    assert exc.value.status_code == 404
