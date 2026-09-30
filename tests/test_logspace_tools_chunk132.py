"""Chunk 132: the logspace agent's tools, over the records the feed's filter fetched.

Every tool reads `log_transactions` through the feed's own scope (app/services/log_feed/scope.py),
so what the person sees and what the agent reads are the same records. Only `trace` may look
further, and it says where it looked. No tool touches settlement, facts, metrics or lookups.
"""

import json
import uuid
from datetime import date as date_type, datetime, timezone

import pytest

from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus as S
from app.services.log_feed.scope import FeedScope
from app.services.logspace_agent.tools import LOGSPACE_TOOLS, run_logspace_tool
from app.settings import settings

TODAY = date_type(2026, 9, 30)
YESTERDAY = date_type(2026, 9, 29)
TZ = "Europe/London"


def _at(day, h, m):
    return datetime(day.year, day.month, day.day, h - 1, m, tzinfo=timezone.utc)  # BST: local h:m


async def _seed(db, cc):
    job = Job(customer_code=cc, filename="f.log", storage_key="k")
    db.add(job)
    await db.flush()
    rows = [
        # delivery D1 today, picked by PEVANS: one line exact, one zero-pick, one partial, then loaded
        dict(started_at=_at(TODAY, 6, 1), method="ConfirmPickLine", user_name="PEVANS", status=S.success,
             delivery_number="D1", item_number="I1", reqid="R1", reporting_number="L1",
             attributes={"QuantityPicked": "3.0", "ExpectedQuantity": "3.0", "FromLocation": "A1-01"}),
        dict(started_at=_at(TODAY, 6, 2), method="ConfirmPickLine", user_name="PEVANS", status=S.success,
             delivery_number="D1", item_number="I2", reqid="R2", reporting_number="L2",
             attributes={"QuantityPicked": "0.0", "ExpectedQuantity": "2.0", "QuantityToBePicked": "2.0", "FromLocation": "JIT"}),
        dict(started_at=_at(TODAY, 6, 3), method="ConfirmPickLine", user_name="PEVANS", status=S.success,
             delivery_number="D1", item_number="I3", reqid="R3", reporting_number="L3",
             attributes={"QuantityPicked": "1.0", "ExpectedQuantity": "4.0", "FromLocation": "A1-02"}),
        dict(started_at=_at(TODAY, 6, 30), method="LoadDeliveryPackage", user_name="PEVANS", status=S.success,
             delivery_number="D1", reqid="R4", attributes={}),
        # another user, an errored call carrying credentials in its error text
        dict(started_at=_at(TODAY, 7, 5), method="GetNextDeliveryByRoute", user_name="BCHAM", status=S.error,
             reqid="R5", error_text="Error requesting http://h/x?Company=915&M3Credentials=%7B%22Password%22%3A%22SECRET1%22%7D&ReqId=R5",
             attributes={"Route": "BRI04"}),
        dict(started_at=_at(TODAY, 7, 6), method="ConfirmPickLine", user_name="BCHAM", status=S.success,
             delivery_number="D3", item_number="I1", reqid="R6", reporting_number="L6",
             attributes={"QuantityPicked": "5.0", "ExpectedQuantity": "5.0"}),
        # yesterday: delivery D2, only there
        dict(started_at=_at(YESTERDAY, 9, 0), method="ConfirmPickLine", user_name="PEVANS", status=S.success,
             delivery_number="D2", item_number="I9", reqid="R7", reporting_number="L7",
             attributes={"QuantityPicked": "2.0", "ExpectedQuantity": "2.0"}),
    ]
    for r in rows:
        day = r["started_at"].date()  # the UTC and London dates agree at these hours
        db.add(LogTransaction(customer_code=cc, job_id=job.id, date=day, **r))
    await db.flush()


async def _run(db, cc, name, args, scope=None):
    scope = scope or FeedScope(day=TODAY, explicit=False)
    return json.loads(await run_logspace_tool(name, args, db, cc, scope=scope, tz_name=TZ, today=TODAY))


@pytest.fixture
async def cc(db):
    code = f"TESTCH132_{uuid.uuid4().hex[:6]}"
    await _seed(db, code)
    return code


def test_the_tool_list_is_logs_only():
    names = {t["name"] for t in LOGSPACE_TOOLS}
    assert names == {"overview", "find_transactions", "trace", "aggregate", "get_transaction", "search_entries"}
    text = json.dumps(LOGSPACE_TOOLS).lower()
    for settled in ("release", "metric", "settle", "rollup", "freshness"):
        assert settled not in text


# ---- overview -------------------------------------------------------------------------------------
async def test_overview_describes_the_fetched_records(db, cc):
    out = await _run(db, cc, "overview", {})
    assert out["records"] == "today (2026-09-30)" and out["total"] == 6
    assert out["by_status"] == {"success": 5, "error": 1}
    methods = {m["method"]: m for m in out["by_method"]}
    assert methods["ConfirmPickLine"]["count"] == 4 and methods["GetNextDeliveryByRoute"]["errors"] == 1
    assert {u["user"]: u["count"] for u in out["by_user"]} == {"PEVANS": 4, "BCHAM": 2}
    assert "QuantityPicked" in out["fields"]["ConfirmPickLine"] and "ExpectedQuantity" in out["fields"]["ConfirmPickLine"]
    assert out["first"] == "06:01:00" and out["last"] == "07:06:00"


async def test_overview_follows_the_feed_filter(db, cc):
    out = await _run(db, cc, "overview", {}, scope=FeedScope(day=TODAY, user="BCHAM", explicit=True))
    assert out["records"] == "2026-09-30 · user BCHAM" and out["total"] == 2


# ---- find_transactions ---------------------------------------------------------------------------
async def test_find_by_delivery_returns_rows_with_quantities_and_links(db, cc, monkeypatch):
    monkeypatch.setattr(settings, "app_public_base_url", "http://eye")
    out = await _run(db, cc, "find_transactions", {"delivery_number": "D1", "order": "oldest"})
    assert out["total"] == 4 and [r["reqid"] for r in out["transactions"]] == ["R1", "R2", "R3", "R4"]
    first = out["transactions"][0]
    assert first["time"] == "06:01:00" and first["QuantityPicked"] == "3.0" and first["ExpectedQuantity"] == "3.0"
    assert first["link"] == "http://eye/?date=2026-09-30&reqid=R1"


async def test_find_by_attribute_comparison_finds_short_picks(db, cc):
    out = await _run(db, cc, "find_transactions", {"method": "ConfirmPickLine", "where": ["QuantityPicked<ExpectedQuantity"]})
    assert sorted(r["reqid"] for r in out["transactions"]) == ["R2", "R3"]
    zero = await _run(db, cc, "find_transactions", {"where": ["QuantityPicked==0"]})
    assert [r["reqid"] for r in zero["transactions"]] == ["R2"]
    jit = await _run(db, cc, "find_transactions", {"where": ["FromLocation==JIT"]})
    assert [r["reqid"] for r in jit["transactions"]] == ["R2"]
    col = await _run(db, cc, "find_transactions", {"where": ["status==error"]})
    assert [r["reqid"] for r in col["transactions"]] == ["R5"]


async def test_find_stays_inside_the_filter(db, cc):
    out = await _run(db, cc, "find_transactions", {"delivery_number": "D2"})
    assert out["total"] == 0          # D2 is yesterday: outside today's records


async def test_a_bad_condition_is_a_readable_problem(db, cc):
    out = await _run(db, cc, "find_transactions", {"where": ["QuantityPicked<<3", "drop table x"]})
    assert out["error"] and len(out["problems"]) == 2


# ---- trace ---------------------------------------------------------------------------------------
async def test_trace_tells_the_story_of_a_delivery_in_time_order(db, cc):
    out = await _run(db, cc, "trace", {"key": "delivery_number", "value": "D1"})
    assert out["looked_in"] == "the records your filter fetched: today (2026-09-30)" and out["outside_filter"] is False
    assert [r["method"] for r in out["transactions"]] == ["ConfirmPickLine"] * 3 + ["LoadDeliveryPackage"]
    assert out["summary"]["by_method"] == {"ConfirmPickLine": 3, "LoadDeliveryPackage": 1}
    assert out["summary"]["users"] == ["PEVANS"] and out["summary"]["errors"] == 0


async def test_trace_widens_to_the_whole_day_then_seven_days_and_says_so(db, cc):
    narrow = FeedScope(day=TODAY, user="BCHAM", explicit=True)
    out = await _run(db, cc, "trace", {"key": "delivery_number", "value": "D1"}, scope=narrow)
    assert out["outside_filter"] is True and out["looked_in"] == "all of 2026-09-30 in this logspace (outside your filter)"
    assert out["total"] == 4
    week = await _run(db, cc, "trace", {"key": "delivery_number", "value": "D2"})
    assert week["outside_filter"] is True and week["total"] == 1
    assert week["looked_in"] == "this logspace from 2026-09-24 to 2026-09-30 (outside your filter)"


async def test_trace_of_something_that_is_nowhere_says_where_it_looked(db, cc):
    out = await _run(db, cc, "trace", {"key": "reqid", "value": "NOPE"})
    assert out["total"] == 0 and out["found"] is False
    assert "2026-09-24 to 2026-09-30" in out["looked_in"]


async def test_trace_needs_a_known_key(db, cc):
    out = await _run(db, cc, "trace", {"key": "customer", "value": "x"})
    assert "error" in out


# ---- aggregate -----------------------------------------------------------------------------------
async def test_aggregate_counts_and_sums_over_the_records(db, cc):
    out = await _run(db, cc, "aggregate", {"group_by": ["user"], "where": ["method==ConfirmPickLine"],
                                           "sum": ["QuantityPicked", "ExpectedQuantity"]})
    rows = {r["user"]: r for r in out["rows"]}
    assert out["total_rows"] == 4
    assert rows["PEVANS"]["count"] == 3 and rows["PEVANS"]["sum:QuantityPicked"] == 4.0
    assert rows["PEVANS"]["sum:ExpectedQuantity"] == 9.0 and rows["BCHAM"]["sum:QuantityPicked"] == 5.0


async def test_aggregate_by_an_attribute_and_by_hour(db, cc):
    by_loc = await _run(db, cc, "aggregate", {"group_by": ["attr:FromLocation"], "where": ["method==ConfirmPickLine"]})
    assert {r["attr:FromLocation"]: r["count"] for r in by_loc["rows"]} == {"A1-01": 1, "JIT": 1, "A1-02": 1, None: 1}
    by_hour = await _run(db, cc, "aggregate", {"group_by": ["hour"]})
    assert {r["hour"]: r["count"] for r in by_hour["rows"]} == {6: 4, 7: 2}


async def test_aggregate_rejects_unknown_groups(db, cc):
    out = await _run(db, cc, "aggregate", {"group_by": ["colour"]})
    assert out["error"] and out["problems"]


# ---- the rest ------------------------------------------------------------------------------------
async def test_get_transaction_is_tenant_scoped(db, cc):
    found = await _run(db, cc, "find_transactions", {"reqid": "R5"})
    tid = found["transactions"][0]["id"]
    assert (await _run(db, cc, "get_transaction", {"transaction_id": tid}))["transaction"]["reqid"] == "R5"
    assert "error" in await _run(db, "SOMEONE_ELSE", "get_transaction", {"transaction_id": tid})


async def test_another_tenant_sees_nothing(db, cc):
    out = await _run(db, "SOMEONE_ELSE", "overview", {})
    assert out["total"] == 0


async def test_an_unknown_tool_is_an_error_not_a_crash(db, cc):
    assert "error" in await _run(db, cc, "list_releases", {})


# ---- live run lessons ----------------------------------------------------------------------------
async def test_get_transaction_also_takes_a_request_id(db, cc):
    out = await _run(db, cc, "get_transaction", {"transaction_id": "R5"})
    assert out["transaction"]["reqid"] == "R5" and out["transaction"]["status"] == "error"


async def test_a_request_id_from_an_earlier_day_is_found_within_the_week(db, cc):
    out = await _run(db, cc, "get_transaction", {"transaction_id": "R7"})
    assert out["transaction"]["reqid"] == "R7"


async def test_an_unknown_request_id_says_so_plainly(db, cc):
    out = await _run(db, cc, "get_transaction", {"transaction_id": "13-2026-09-30_18:37:56.933-7133"})
    assert "error" in out and "request id" in out["error"]


async def test_aggregate_returns_the_base_for_n_of_m(db, cc):
    out = await _run(db, cc, "aggregate", {"method": "ConfirmPickLine", "where": ["QuantityPicked==0"]})
    assert out["total_rows"] == 1 and out["base_rows"] == 4        # 1 zero-pick of 4 ConfirmPickLine calls


def test_a_long_error_keeps_its_start_and_its_end():
    from app.services.logspace_agent.tools import _error_excerpt
    text = "Error requesting http://h/x?" + "a=1&" * 200 + " Error : System.NullReferenceException: Object reference not set."
    out = _error_excerpt(text)
    assert out.startswith("Error requesting http://h/x?") and out.endswith("Object reference not set.")
    assert " … " in out and len(out) <= 420
    assert _error_excerpt("short") == "short" and _error_excerpt(None) is None
