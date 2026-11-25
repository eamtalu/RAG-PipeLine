"""Chunk 222: the developer's view of the runs (stage D, step 46).

`scripts/trace_review.py` lists runs, shows one run as a timeline and ranks what went wrong over
a period, so the developer can see how the agent behaves before deciding what to change. It only
reads. The reading and counting live in `app/services/agent_traces/review.py`, as pure functions
over stored runs, so they are tested here without a database.
"""

from __future__ import annotations

from datetime import datetime, timezone

from app.agent.runtime.trace import RunTrace
from app.services.agent_traces import review

SITES = ("BRI", "LON", "BIC")
T0 = datetime(2026, 11, 25, 9, 0, tzinfo=timezone.utc)


def _call(name: str, args: dict | None = None, *, ok: bool = True, ms: int = 10,
          closed: tuple[str, ...] = (), at_ms: int = 0) -> dict:
    return {"namespace": "analytics", "name": name, "args": args or {}, "result": "{}", "ms": ms,
            "ok": ok, "result_chars": 2, "result_rows": 0, "at_ms": at_ms,
            "data": {"tables": ["advisories"], "closed": list(closed)}}


def _run(**fields) -> dict:
    base = {"trace_id": "t1", "started_at": T0, "principal_id": "u-sup", "role_code": "supervisor",
            "scope": {"all_sites": False, "site_ids": ["BRI"], "categories": ["operations"]},
            "trigger": "ask", "status": "ok", "stop_reason": "end_turn", "declined": None,
            "iterations": 1, "retries": 0, "duration_ms": 900, "profile_version": "v1",
            "question": "how is BRI?", "answer": "fine", "tool_calls": [], "model_calls": [],
            "steps": [], "events": [], "guards": [], "withheld_figures": [], "error": None,
            "usage": {"input_tokens": 100, "output_tokens": 20}}
    return {**base, **fields}


# --- one run ---------------------------------------------------------------------------------

def test_an_identical_tool_call_made_twice_is_a_repeat():
    run = _run(tool_calls=[_call("list_releases", {"day": "today"}),
                           _call("list_releases", {"day": "today"}),
                           _call("list_releases", {"day": "yesterday"})])
    assert review.repeated_calls(run) == [{"name": "list_releases", "args": {"day": "today"},
                                           "times": 2}]


def test_a_site_named_in_a_tools_arguments_outside_the_scope_is_found():
    run = _run(tool_calls=[_call("aggregate_releases", {"where": ["warehouse==LON"]}),
                           _call("at_risk_board", {"site": "BRI"})])
    assert review.sites_outside_scope(run, SITES) == ["LON"]
    everything = _run(scope={"all_sites": True, "site_ids": [], "categories": []},
                      tool_calls=[_call("aggregate_releases", {"where": ["warehouse==LON"]})])
    assert review.sites_outside_scope(everything, SITES) == []


def test_a_site_code_inside_a_longer_word_is_not_a_site():
    run = _run(tool_calls=[_call("find_advisory", {"words": "LONDON BRIDGE route LON5"})])
    assert review.sites_outside_scope(run, SITES) == []


def test_a_run_is_limited_by_scope_when_it_asked_outside_or_hit_a_closed_table():
    outside = _run(tool_calls=[_call("x", {"site": "LON"})])
    closed = _run(tool_calls=[_call("search_entries", closed=("log_entries",))])
    assert review.is_limited_by_scope(outside, SITES) and review.is_limited_by_scope(closed, SITES)
    assert not review.is_limited_by_scope(_run(tool_calls=[_call("x", {"site": "BRI"})]), SITES)
    assert review.closed_tables(closed) == ["log_entries"]


def test_an_older_run_without_the_new_fields_still_reads():
    old = _run(role_code=None, scope=None, model_calls=[], steps=[], events=[],
               tool_calls=[{"namespace": "a", "name": "x", "args": {}, "result": "", "ms": 1,
                            "ok": True}])
    assert not review.is_limited_by_scope(old, SITES) and review.repeated_calls(old) == []
    assert review.timeline(old)[0].startswith("run t1")


# --- the timeline ----------------------------------------------------------------------------

def test_the_timeline_puts_model_calls_tool_calls_and_events_in_the_order_they_happened():
    run = _run(
        model_calls=[{"round": 1, "model": "m", "ms": 300, "ok": True, "input_tokens": 90,
                      "output_tokens": 10, "at_ms": 0},
                     {"round": 2, "model": "m", "ms": 200, "ok": True, "input_tokens": 120,
                      "output_tokens": 30, "at_ms": 420}],
        steps=[{"round": 1, "text": "I will check the board."}],
        tool_calls=[_call("at_risk_board", {"day": "today"}, at_ms=310, ms=100)],
        events=[{"kind": "retry", "detail": "ungrounded", "at_ms": 650}])
    lines = review.timeline(run)
    kinds = [line.split()[1] for line in lines[1:] if line.startswith("  +")]
    assert kinds == ["model", "tool", "model", "event"]
    assert any("I will check the board." in line for line in lines)
    assert lines[0] == ("run t1 | 2026-11-25 09:00 | u-sup supervisor [BRI] | ask | "
                        "end_turn in 1 round, 900 ms")
    assert lines[-1] == "answer: fine"


def test_the_trace_stamps_each_call_with_its_place_in_the_run():
    trace = RunTrace()
    trace.record_model_call(provider="p", model="m", ms=5, ok=True, usage=None)
    trace.record_tool(namespace="a", name="x", args={}, result="{}", ms=3, ok=True)
    trace.record_event("retry", "ungrounded")
    stamps = [trace.model_calls_made[0]["at_ms"], trace.tool_calls[0]["at_ms"],
              trace.events[0]["at_ms"]]
    assert all(isinstance(s, int) and s >= 0 for s in stamps) and stamps == sorted(stamps)


# --- the findings over a period ----------------------------------------------------------------

def test_the_findings_count_what_went_wrong_and_name_example_runs():
    runs = [
        _run(trace_id="ok"),
        _run(trace_id="loop", iterations=10, stop_reason="max_iterations",
             tool_calls=[_call("list_releases", {"d": 1}), _call("list_releases", {"d": 1})]),
        _run(trace_id="slow", stop_reason="timeout", status="timeout",
             tool_calls=[_call("wms_activity", ms=9000)]),
        _run(trace_id="scope", tool_calls=[_call("at_risk_board", {"site": "LON"})]),
        _run(trace_id="err", tool_calls=[_call("list_releases", ok=False)]),
        _run(trace_id="no", stop_reason="declined", status="declined", declined="off_topic"),
        _run(trace_id="hid", withheld_figures=["42"], events=[{"kind": "retry", "detail": "x"}]),
    ]
    found = review.findings(runs, SITES)
    assert found["runs"] == 7
    assert found["repeated_calls"] == {"runs": 1, "examples": ["loop"]}
    assert found["round_limit"] == {"runs": 1, "examples": ["loop"]}
    assert found["timeouts"] == {"runs": 1, "examples": ["slow"]}
    assert found["limited_by_scope"] == {"runs": 1, "examples": ["scope"]}
    assert found["declined"] == {"runs": 1, "examples": ["no"]}
    assert found["withheld"] == {"runs": 1, "examples": ["hid"]}
    assert found["retried"] == {"runs": 1, "examples": ["hid"]}
    assert found["tool_errors"] == [{"tool": "list_releases", "errors": 1, "calls": 3}]
    assert found["slowest_tools"][0] == {"tool": "wms_activity", "calls": 1, "avg_ms": 9000,
                                         "max_ms": 9000}


def test_the_findings_compare_prompt_versions():
    runs = [_run(profile_version="v1", iterations=2, duration_ms=1000),
            _run(profile_version="v1", iterations=4, duration_ms=3000),
            _run(profile_version="v2", iterations=1, duration_ms=500,
                 usage={"input_tokens": 50, "output_tokens": 10})]
    assert review.findings(runs, SITES)["by_prompt_version"] == [
        {"version": "v1", "runs": 2, "avg_rounds": 3.0, "avg_ms": 2000, "avg_tokens": 120},
        {"version": "v2", "runs": 1, "avg_rounds": 1.0, "avg_ms": 500, "avg_tokens": 60}]


def test_no_runs_give_empty_findings():
    found = review.findings([], SITES)
    assert found["runs"] == 0 and found["tool_errors"] == [] and found["by_prompt_version"] == []


def test_a_run_is_one_line_in_the_list():
    line = review.list_line(_run(tool_calls=[_call("x", {"site": "LON"})]), SITES)
    assert line == ("2026-11-25 09:00  t1  u-sup supervisor [BRI]  ask  end_turn  1 round  "
                    "900 ms  scope-limited  how is BRI?")


# --- reading a whole window from the store, a page at a time ------------------------------------

import uuid  # noqa: E402
from datetime import timedelta  # noqa: E402

import pytest  # noqa: E402
from sqlalchemy import delete  # noqa: E402

from app.config.database import async_session  # noqa: E402
from app.persistence.models.agent_trace import AgentTrace  # noqa: E402
from app.services.agent_traces.store import PostgresTraceStore  # noqa: E402
from tests.test_agent_trace_detail_chunk221 import _finished  # noqa: E402

CC = "c222"


@pytest.mark.asyncio
async def test_a_window_is_read_a_page_at_a_time_and_capped():
    store = PostgresTraceStore(async_session)
    async with async_session() as s:
        await s.execute(delete(AgentTrace).where(AgentTrace.customer_code == CC))
        await s.commit()
    try:
        for minutes in range(5):
            at = T0 - timedelta(minutes=minutes)
            row = {**_finished(RunTrace()).as_row(), "tenant": CC, "trace_id": uuid.uuid4().hex,
                   "started_at": at, "finished_at": at}
            await store.save(row)
        since = T0 - timedelta(days=1)
        every = await store.all_runs(CC, since=since, page=2)
        assert len(every) == 5
        assert [r["started_at"] for r in every] == sorted((r["started_at"] for r in every),
                                                          reverse=True)
        assert len(await store.all_runs(CC, since=since, page=2, most=3)) == 3
    finally:
        async with async_session() as s:
            await s.execute(delete(AgentTrace).where(AgentTrace.customer_code == CC))
            await s.commit()
