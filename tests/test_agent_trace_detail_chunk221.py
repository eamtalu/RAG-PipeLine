"""Chunk 221: a run's trace says who asked with which role and scope, and what each step did.

Before this chunk a trace held the person, the question, the answer, the tool calls and the usage
totals. It now also holds the role and the data scope at the time of the run (a role can change
later), the check that let the run start, what started it, every model call in order, the model's
own text between tool calls, the size of each tool result before the cut, the tables a tool read,
and the loop events that used to go only to the log file. Traces are kept 90 days.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.agent.contracts import AgentResult
from app.agent.gateway import build_context, snapshot_settings
from app.agent.runtime import trace as trace_module
from app.agent.runtime.trace import RunTrace
from app.config.database import async_session
from app.persistence.models.agent_trace import AgentTrace
from app.services import scoped_data
from app.services.agent_traces.store import PostgresTraceStore
from app.services.analytics_agent.namespaces import analytics_namespace
from tests.agent_helpers import request_for

CC = "c221"
ACCESS = {"role": "supervisor", "all_sites": False, "site_ids": ["BRI"],
          "categories": ["operations"], "capability": "ask", "outcome": "allowed"}
NOW = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)


def _finished(trace: RunTrace, **request_fields):
    request = request_for(CC, "how is BRI?", **request_fields)
    context = build_context(request, snapshot_settings())
    result = AgentResult(answer="fine", stop_reason="end_turn", model="m")
    return trace.finish(context, request, result)


def test_the_record_carries_the_role_the_scope_and_the_check_of_that_moment():
    record = _finished(RunTrace(), access=ACCESS, trigger="routine")
    assert (record.role_code, record.capability_code, record.capability_outcome) == (
        "supervisor", "ask", "allowed")
    assert record.scope == {"all_sites": False, "site_ids": ["BRI"], "categories": ["operations"]}
    assert record.trigger == "routine"


def test_a_run_with_no_access_facts_still_records():
    record = _finished(RunTrace())
    assert (record.role_code, record.scope, record.capability_code, record.trigger) == (
        None, None, None, "ask")


def test_every_model_call_is_kept_in_order_with_its_round():
    trace = RunTrace()
    trace.record_model_call(provider="p", model="m", ms=120, ok=True,
                            usage={"input_tokens": 10, "output_tokens": 3})
    trace.record_model_call(provider="p", model="m", ms=900, ok=False, usage=None)
    calls = _finished(trace).model_calls
    assert [(c["round"], c["ms"], c["ok"], c["input_tokens"]) for c in calls] == [
        (1, 120, True, 10), (2, 900, False, 0)]


def test_the_models_own_text_between_tool_calls_is_kept_redacted_and_capped():
    trace = RunTrace()
    trace.record_step("I will list the releases first.")
    trace.record_step("x" * 5000)
    steps = _finished(trace).steps
    assert steps[0] == {"round": 1, "text": "I will list the releases first."}
    assert steps[1]["round"] == 2 and steps[1]["text"].endswith(trace_module.CAP_MARKER)
    assert len(steps[1]["text"]) == trace_module.RESULT_CAP + len(trace_module.CAP_MARKER)


def test_a_loop_event_is_kept_on_the_record():
    trace = RunTrace()
    trace.record_event("settled_loop", "kept the answer written before the repeats")
    trace.record_event("retry", "ungrounded")
    assert [(e["kind"], e["detail"]) for e in _finished(trace).events] == [
        ("settled_loop", "kept the answer written before the repeats"), ("retry", "ungrounded")]


def test_a_tool_call_keeps_its_size_before_the_cut_and_the_tables_it_read():
    trace = RunTrace()
    big = '{"rows": [' + ", ".join('{"n": %d}' % i for i in range(400)) + "]}"
    trace.record_tool(namespace="analytics", name="list_releases", args={}, result=big, ms=5,
                      ok=True, notes={"tables": ["analytics_settled_rows"], "closed": []})
    trace.record_tool(namespace="analytics", name="x", args={}, result="not json", ms=1, ok=True)
    first, second = _finished(trace).tool_calls
    assert first["result_chars"] == len(big) and first["result_rows"] == 400
    assert first["result"].endswith(trace_module.CAP_MARKER)
    assert first["data"] == {"tables": ["analytics_settled_rows"], "closed": []}
    assert (second["result_chars"], second["result_rows"], second["data"]) == (8, None, None)


def test_the_row_a_store_writes_has_the_new_fields():
    row = _finished(RunTrace(), access=ACCESS).as_row()
    assert row["role_code"] == "supervisor" and row["model_calls"] == [] and row["steps"] == []
    assert row["events"] == [] and row["trigger"] == "ask"


# --- the tables a tool call read ----------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_tool_call_notes_the_tables_it_read_and_the_ones_closed_to_the_asker():
    request = request_for(CC, data_scope=scoped_data.DataScope(CC, site_ids=("BRI",)))
    context = build_context(request, snapshot_settings())
    with trace_module.collecting_call_notes() as notes:
        await analytics_namespace().runner(context, request, "find_advisory", {"words": "Ginger"})
    assert notes["tables"] == ["advisories"] and notes["closed"] == []


def test_a_closed_table_is_one_a_site_limited_person_reads_nothing_from(monkeypatch):
    # No table is closed since chunk 236 (log lines follow their transaction), so one is closed
    # here to keep the rule itself pinned for the next table that arrives without a site.
    from app.persistence.models.log_entry import LogEntry
    registry = scoped_data.registry
    monkeypatch.setitem(registry.BY_TABLE, "log_entries", registry.AllSitesOnly(LogEntry))
    limited = scoped_data.DataScope(CC, site_ids=("BRI",))
    everything = scoped_data.DataScope.for_system(CC)
    assert scoped_data.closed_tables({"log_entries", "advisories"}, limited) == ["log_entries"]
    assert scoped_data.closed_tables({"log_entries", "advisories"}, everything) == []


# --- the store: read back, list, and the 90 days ------------------------------------------------

async def _saved(store: PostgresTraceStore, *, started_at: datetime, **fields) -> str:
    record = _finished(RunTrace(), access=ACCESS, **fields)
    row = {**record.as_row(), "trace_id": uuid.uuid4().hex, "started_at": started_at,
           "finished_at": started_at}
    await store.save(row)
    return row["trace_id"]


@pytest.fixture
async def _store():
    async with async_session() as s:
        await s.execute(delete(AgentTrace).where(AgentTrace.customer_code == CC))
        await s.commit()
    yield PostgresTraceStore(async_session)
    async with async_session() as s:
        await s.execute(delete(AgentTrace).where(AgentTrace.customer_code == CC))
        await s.commit()


@pytest.mark.asyncio
async def test_a_trace_is_read_back_with_its_role_and_scope(_store):
    trace_id = await _saved(_store, started_at=NOW)
    row = await _store.get(trace_id)
    assert row["role_code"] == "supervisor" and row["scope"]["site_ids"] == ["BRI"]
    assert await _store.get("no-such-trace") is None


@pytest.mark.asyncio
async def test_traces_are_listed_newest_first_and_filtered(_store):
    old = await _saved(_store, started_at=NOW - timedelta(days=2))
    new = await _saved(_store, started_at=NOW, trigger="routine")
    listed = await _store.list_runs(CC, since=NOW - timedelta(days=7), limit=10)
    assert [r["trace_id"] for r in listed] == [new, old]
    routines = await _store.list_runs(CC, since=NOW - timedelta(days=7), limit=10,
                                      trigger="routine")
    assert [r["trace_id"] for r in routines] == [new]


@pytest.mark.asyncio
async def test_traces_older_than_the_retention_are_purged_and_newer_ones_kept(_store):
    await _saved(_store, started_at=NOW - timedelta(days=91))
    kept = await _saved(_store, started_at=NOW - timedelta(days=89))
    assert await _store.purge_once(now=NOW, retention_days=90) == 1
    assert await _store.purge_once(now=NOW, retention_days=90) == 0
    async with async_session() as s:
        mine = select(AgentTrace.trace_id).where(AgentTrace.customer_code == CC)
        left = (await s.execute(mine)).scalars()
        assert list(left) == [kept]


# --- the Teams consumer hands the facts over, and a routine run names its trace ----------------

from app import teams_consumer  # noqa: E402
from app.services.access import AccessRecord  # noqa: E402
from app.services.teams.contracts import QuestionJob  # noqa: E402


def _job(job_id: str) -> QuestionJob:
    return QuestionJob(job_id=job_id, tenant_id="", customer_code=CC, conversation_id="c",
                       sender_object_id="u-sup", sender_name="Sam", question="q", source="tab",
                       site_id="BRI")


def test_the_consumer_states_the_role_the_scope_and_the_check_for_the_trace():
    record = AccessRecord(CC, "u-sup", "Sam", "supervisor", "Supervisor", "active", False,
                          ("BRI",), {"ask": "allowed"})
    scope = scoped_data.DataScope.from_record(record)
    facts = teams_consumer._access_facts(record, scope, _job("j1"))
    assert facts == {"role": "supervisor", "all_sites": False, "site_ids": ["BRI"],
                     "categories": ["operations"], "capability": "ask", "outcome": "allowed"}
    assert teams_consumer._access_facts(None, scoped_data.DataScope(CC), _job("j1")) is None


def test_a_routines_job_is_marked_as_a_routine_run():
    assert teams_consumer._trigger(_job("task-7-1764072000")) == "routine"
    assert teams_consumer._trigger(_job("j1")) == "ask"


@pytest.mark.asyncio
async def test_a_routine_run_keeps_the_trace_of_the_run_that_answered_it(monkeypatch):
    async def answered(job, history, progress=None):
        return {"answer": "fine", "blocks": []}, "trace-9"

    async def allowed(job):
        return None

    monkeypatch.setattr(teams_consumer, "_run_agent_traced", answered)
    monkeypatch.setattr(teams_consumer, "_check_access", allowed)

    class Task:
        id, customer_code, owner_object_id, owner_name = uuid.uuid4(), CC, "u-sup", "Sam"
        query_text, title, site_id = "short products", "Shorts", "BRI"

    out = await teams_consumer._run_task_query(Task())
    assert out["trace_id"] == "trace-9" and out["answer"] == "fine"
