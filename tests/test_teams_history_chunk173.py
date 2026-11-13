"""Chunk 173: the Ask history and the progress line of the eSmart Eye Teams app (stage D, step 6).

The turns table gains who asked, the site, the typed blocks and the times; one person's last
30 days are mirrored to the edge as summaries plus one full row per answer; older turns are
purged in batches. While a question runs, a progress row says which of four steps is live,
fed by the run trace's listener. The consumer records the exchange as posted and brackets the
run with the reporter.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from app.agent.runtime.trace import RunTrace
from app.config.database import async_session
from app.persistence.repositories.teams_repository import ExchangeDetails, TeamsConversationRepository
from app.services.teams import consumer as c
from app.services.teams import history, progress
from app.services.teams.contracts import AnswerPayload, ChartBlock, ChartSeries, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from app.services.teams.mirror_rows import HISTORY_TTL, KEY_LAYOUT
from tests.test_teams_consumer_chunk123 import Harness, job

CC = "acme"
USER = "u-7"
NOW = datetime(2026, 11, 12, 17, 14, tzinfo=timezone.utc)


def _job(job_id: str = "j1", *, site_id: str | None = "BRI") -> QuestionJob:
    return QuestionJob(job_id=job_id, tenant_id="t", customer_code=CC, conversation_id="conv-7",
                       sender_object_id=USER, sender_name="Jo", question=f"How many {job_id}?",
                       site_id=site_id, source="tab")


def _payload(job_id: str = "j1", *, with_chart: bool = True) -> AnswerPayload:
    blocks = ([ChartBlock(kind="bar", title="Deliveries", labels=["Mon"],
                          series=[ChartSeries(name="n", values=[1.0])])] if with_chart else [])
    return AnswerPayload(job_id=job_id, conversation_id="conv-7", status="ok",
                         answer="Two items short.\n\n| a |", blocks=blocks,
                         asked_at=NOW - timedelta(seconds=20), answered_at=NOW,
                         data_as_of=NOW - timedelta(minutes=5), provisional=True)


# ==================================================== 1. the rows and the history read


async def test_the_answer_row_carries_the_history_and_the_read_is_own_newest_first():
    conv = f"conv-{uuid.uuid4()}"
    me, other = f"u-{uuid.uuid4()}", f"u-{uuid.uuid4()}"
    async with async_session() as db:
        repo = TeamsConversationRepository(db)
        for i in range(3):
            details = ExchangeDetails(sender_object_id=me, site_id="BRI", blocks=[{"type": "chart"}],
                                      asked_at=NOW, answered_at=NOW, data_as_of=NOW, provisional=i == 0)
            await repo.append_exchange(conv, CC, question=f"q{i}", answer=f"a{i}", job_id=f"{conv}-{i}",
                                       details=details)
            await asyncio.sleep(0.01)  # distinct created_at, so the order is by time
        # another person, the same tenant; and an old-style exchange with no details at all
        await repo.append_exchange(conv, CC, question="theirs", answer="x", job_id=f"{conv}-o",
                                   details=ExchangeDetails(other, None, [], None, None, None, False))
        await repo.append_exchange(conv, CC, question="legacy", answer="y", job_id=None)

        pairs = await repo.user_exchanges(CC, me, since=NOW - timedelta(days=30), limit=10)
        assert [(q.content, a.content) for q, a in pairs] == [("q2", "a2"), ("q1", "a1"), ("q0", "a0")]
        assert pairs[-1][1].provisional is True and pairs[0][1].provisional is False
        assert pairs[0][1].blocks == [{"type": "chart"}] and pairs[0][0].sender_object_id == me
        assert len(await repo.user_exchanges(CC, me, since=NOW - timedelta(days=30), limit=2)) == 2
        assert await repo.user_exchanges("other-tenant", me, since=NOW, limit=10) == []
        # the agent's memory read is unchanged by the extra columns
        turns = await repo.recent_turns(conv, CC, limit=2)
        assert [t["role"] for t in turns] == ["user", "assistant"]
        assert await repo.purge_customer(CC) >= 10


async def test_the_purge_deletes_older_turns_a_batch_at_a_time():
    conv = f"conv-{uuid.uuid4()}"
    async with async_session() as db:
        repo = TeamsConversationRepository(db)
        for i in range(3):
            await repo.append_exchange(conv, CC, question=f"q{i}", answer=f"a{i}", job_id=f"{conv}-{i}")
        # nothing is older than a cutoff in the past; everything is older than one in the future
        assert await history.purge_once(db, now=NOW - timedelta(days=400), retention_days=30) == 0
        # the shared test database may hold other tests' turns, so count batches, not rows
        future = datetime.now(timezone.utc) + timedelta(days=31)
        assert await history.purge_once(db, now=future, retention_days=30, batch_size=4) == 4
        total = 4
        while gone := await history.purge_once(db, now=future, retention_days=30, batch_size=4):
            assert gone <= 4
            total += gone
        assert total >= 6 and await repo.recent_turns(conv, CC, limit=10) == []


# ==================================================== 2. the mirror rows


def test_the_entry_summarises_an_exchange():
    class Row:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    q = Row(content="How many?", created_at=NOW)
    a = Row(content="Two.\n\n| a |", job_id="j1", blocks=[{"type": "table"}, {"type": "followups"}],
            asked_at=None, answered_at=NOW, data_as_of=NOW - timedelta(minutes=5), provisional=True,
            site_id="BRI", created_at=NOW)
    entry = history.history_entry(q, a)
    assert entry == {"job_id": "j1", "question": "How many?", "answer_preview": "Two.",
                     "kinds": ["table"], "asked_at": NOW.isoformat(),
                     "answered_at": NOW.isoformat(),
                     "data_as_of": (NOW - timedelta(minutes=5)).isoformat(),
                     "provisional": True, "site_id": "BRI"}


async def test_the_history_rows_are_written_under_the_person():
    conv = f"conv-{uuid.uuid4()}"
    me = f"u-{uuid.uuid4()}"
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        repo = TeamsConversationRepository(db)
        await repo.append_exchange(conv, CC, question="q0", answer="a0", job_id=f"{conv}-0",
                                   details=ExchangeDetails(me, "BRI", [], NOW, NOW, None, False))
        n = await history.mirror_user_history(db, mirror, CC, me, now=NOW, retention_days=30, limit=100)
        assert n == 1
        await repo.purge_customer(CC)
    env = mirror.items[("EYE#acme", f"USER#{me}#HISTORY")]
    assert env.row_type == "user_history" and env.ttl == int(NOW.timestamp()) + HISTORY_TTL
    body = json.loads(env.payload_json)
    assert body["retention_days"] == 30 and [e["question"] for e in body["entries"]] == ["q0"]

    job_ = _job()
    await history.mirror_turn(mirror, job_, _payload(), now=NOW)
    env = mirror.items[("EYE#acme", f"USER#{USER}#TURN#j1")]
    turn = json.loads(env.payload_json)
    assert turn["question"] == "How many j1?" and turn["site_id"] == "BRI"
    assert turn["answer"]["blocks"][0]["type"] == "chart" and turn["answer"]["provisional"] is True
    assert KEY_LAYOUT["history_turn"].ttl_seconds == HISTORY_TTL == 31 * 24 * 3600


def test_details_come_off_the_job_and_the_answer():
    details = history.details_from(_job(), _payload())
    assert (details.sender_object_id, details.site_id, details.provisional) == (USER, "BRI", True)
    assert details.blocks[0]["type"] == "chart" and details.answered_at == NOW


# ==================================================== 3. the progress line


async def test_the_reporter_moves_through_the_four_steps_and_coalesces_writes():
    mirror = InMemoryMirrorWriter()
    reporter = progress.ProgressReporter(mirror, CC, "j1", clock=lambda: NOW)
    key = ("EYE#acme", "JOB#j1#PROGRESS")

    def states():
        return [(s["label"], s["state"]) for s in json.loads(mirror.items[key].payload_json)["steps"]]

    await reporter.started()
    assert states() == [(progress.UNDERSTOOD, "done"), (progress.QUERYING, "running"),
                        (progress.WRITING, "todo"), (progress.CHECKING, "todo")]
    assert mirror.items[key].ttl == int(NOW.timestamp()) + 3600
    # a model call before any tool is the gate; it changes nothing
    reporter.on_event("model_call", {})
    reporter.on_event("tool", {"name": "describe_releases"})
    reporter.on_event("tool", {"name": "show_chart"})
    writes = mirror.put_count
    await asyncio.sleep(0.01)
    assert mirror.put_count == writes + 1  # the three events cost one write
    steps = json.loads(mirror.items[key].payload_json)["steps"]
    assert steps[1] == {"label": progress.QUERYING, "state": "running",
                        "detail": "2 calls · drawing the chart"}
    reporter.on_event("model_call", {})
    await asyncio.sleep(0.01)
    assert states()[1:3] == [(progress.QUERYING, "done"), (progress.WRITING, "running")]
    reporter.on_event("guard", {"name": "ungrounded", "stage": "post", "verdict": "problem"})
    await asyncio.sleep(0.01)
    steps = json.loads(mirror.items[key].payload_json)["steps"]
    assert steps[3]["state"] == "running" and "asking again" in steps[3]["detail"]
    await reporter.finished()
    body = json.loads(mirror.items[key].payload_json)
    assert body["finished"] is True and {s["state"] for s in body["steps"]} == {"done"}
    reporter.on_event("tool", {"name": "late"})  # after the end: ignored
    await asyncio.sleep(0.01)
    assert json.loads(mirror.items[key].payload_json)["finished"] is True


async def test_a_failing_mirror_never_reaches_the_run():
    reporter = progress.ProgressReporter(InMemoryMirrorWriter(fail=True), CC, "j1")
    await reporter.started()
    reporter.on_event("tool", {"name": "x"})
    await asyncio.sleep(0.01)
    await reporter.finished()


def test_the_trace_tells_its_listener_and_survives_a_bad_one():
    heard: list[tuple[str, dict]] = []
    trace = RunTrace(listener=lambda kind, data: heard.append((kind, data)))
    trace.record_tool(namespace="analytics", name="aggregate_releases", args={}, result="{}", ms=1, ok=True)
    trace.record_model_call(provider="p", model="m", ms=1, ok=True, usage=None)
    trace.record_guard(name="fabrication", stage="post", verdict="pass", ms=0)
    assert [k for k, _ in heard] == ["tool", "model_call", "guard"]
    assert heard[0][1] == {"namespace": "analytics", "name": "aggregate_releases", "ok": True}

    def bad(kind, data):
        raise RuntimeError("boom")

    trace = RunTrace(listener=bad)
    trace.record_tool(namespace="a", name="b", args={}, result="", ms=0, ok=True)
    assert len(trace.tool_calls) == 1
    assert RunTrace(listener=bad) == RunTrace()  # the listener is not part of the record


# ==================================================== 4. the consumer


async def test_the_consumer_brackets_the_run_with_the_reporter_and_records_the_payload():
    events: list[str] = []

    class Reporter:
        async def started(self):
            events.append("started")

        async def finished(self):
            events.append("finished")

        def on_event(self, kind, data):
            events.append(kind)

    seen: list = []

    async def agent(code, question, history_):
        events.append("run")
        return {"answer": "Two.", "tool_calls": [], "blocks": [{"type": "text", "markdown": "Two."}]}

    h = Harness([job(question="q")], agent=agent)
    h.consumer._progress_factory = lambda job_: Reporter()
    original = h.consumer._record

    async def record(job_, payload):
        seen.append((job_.job_id, payload.answer, [b.type for b in payload.blocks]))
        await original(job_, payload)

    h.consumer._record = record
    await h.drain()
    assert events == ["started", "run", "finished"]
    assert seen == [(h.poster.posted[0].job_id, "Two.", ["text"])]
    assert h.recorded[0][2:4] == ("q", "Two.")


async def test_the_reporter_is_finished_even_when_the_run_fails():
    events: list[str] = []

    class Reporter:
        async def started(self):
            events.append("started")

        async def finished(self):
            events.append("finished")

        def on_event(self, kind, data):
            pass

    async def agent(code, question, history_):
        raise RuntimeError("provider down")

    h = Harness([job(question="q")], agent=agent)
    h.consumer._progress_factory = lambda job_: Reporter()
    await h.drain()
    assert events == ["started", "finished"] and h.poster.posted[0].status == "error"


@pytest.mark.parametrize("site_id", ["BRI", None])
def test_the_job_carries_the_site_the_page_looked_at(site_id):
    assert QuestionJob.model_validate_json(_job(site_id=site_id).model_dump_json()).site_id == site_id
