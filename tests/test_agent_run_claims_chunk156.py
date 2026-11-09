"""Step 5 of docs/esmart-agent-execution-plan.md: one row per run, claimed before the model runs.

SQS delivers at least once. Before this step a redelivered question ran the model again and wrote
its turns twice (job 8e47ae74 in the handoff). Now the consumer claims an `agent_runs` row by the
edge's job id first: a second delivery that finds the row running leaves the message alone, and
one that finds it done re-posts the stored answer instead of paying for the model again.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete

from app import teams_consumer
from app.config.database import async_session
from app.persistence.models.agent_run import AgentRun
from app.services.agent_runs import claims
from app.services.teams.contracts import AnswerPayload, QuestionJob
from tests.test_teams_consumer_chunk123 import FakePoster, Harness, job

CC = "test_chunk156"
STALE = timedelta(seconds=600)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _request_id() -> str:
    return f"c156-{uuid.uuid4().hex[:12]}"


async def _claim(request_id: str, *, now: datetime | None = None) -> claims.Claim:
    async with async_session() as db:
        outcome = await claims.try_claim(db, request_id=request_id, customer_code=CC, channel="teams",
                                         now=now or _now(), stale_after=STALE)
        await db.commit()
        return outcome


async def _wipe():
    async with async_session() as db:
        await db.execute(delete(AgentRun).where(AgentRun.customer_code == CC))
        await db.commit()


# ==================================================== 1. the store, on Postgres

async def test_the_first_claim_wins_and_the_second_sees_running():
    await _wipe()
    rid = _request_id()
    assert (await _claim(rid)).outcome == "claimed"
    second = await _claim(rid)
    assert second.outcome == "running" and second.stored is None


async def test_a_finished_run_hands_back_its_stored_result():
    await _wipe()
    rid = _request_id()
    await _claim(rid)
    async with async_session() as db:
        await claims.finish(db, request_id=rid, result={"answer": "42", "status": "ok"}, now=_now())
        await db.commit()
    done = await _claim(rid)
    assert done.outcome == "done" and done.stored == {"answer": "42", "status": "ok"}


async def test_a_stale_running_claim_is_taken_over():
    await _wipe()
    rid = _request_id()
    long_ago = _now() - STALE - timedelta(minutes=1)
    assert (await _claim(rid, now=long_ago)).outcome == "claimed"
    assert (await _claim(rid)).outcome == "claimed", "a dead consumer's claim is taken over"
    assert (await _claim(rid)).outcome == "running"
    async with async_session() as db:
        row = await db.get(AgentRun, (await db.execute(
            AgentRun.__table__.select().where(AgentRun.request_id == rid))).one().id)
    assert row.attempts == 2


async def test_two_concurrent_claims_yield_exactly_one_winner():
    await _wipe()
    rid = _request_id()
    outcomes = await asyncio.gather(*(_claim(rid) for _ in range(5)))
    assert sorted(o.outcome for o in outcomes) == ["claimed", "running", "running", "running", "running"]


# ==================================================== 2. the consumer, with an in-memory ledger

class MemoryClaims:
    """The same rules as the store, in a dict, so the consumer tests need no database."""

    def __init__(self):
        self.rows: dict[str, tuple[str, dict | None]] = {}

    async def claim(self, job: QuestionJob) -> claims.Claim:
        status, stored = self.rows.get(job.job_id, (None, None))
        if status is None:
            self.rows[job.job_id] = ("running", None)
            return claims.Claim("claimed", None)
        return claims.Claim(status, stored)

    async def finish(self, job: QuestionJob, payload: AnswerPayload) -> None:
        self.rows[job.job_id] = ("done", payload.model_dump(mode="json"))


async def test_a_duplicate_delivery_never_runs_the_model_twice():
    ledger = MemoryClaims()
    h = Harness([job(job_id="dup"), job(job_id="dup")], claims=ledger)
    await h.drain()
    assert len(h.agent_calls) == 1, "the second delivery must replay the stored answer"
    assert [p.answer for p in h.poster.posted] == ["Because the location was empty."] * 2
    assert sorted(h.sqs.deleted) == ["r0", "r1"]


async def test_a_message_whose_run_is_in_flight_elsewhere_is_left_in_the_queue():
    ledger = MemoryClaims()
    ledger.rows["busy"] = ("running", None)
    h = Harness([job(job_id="busy")], claims=ledger)
    await h.drain()
    assert h.agent_calls == [] and h.poster.posted == [] and h.sqs.deleted == []


async def test_the_answer_is_stored_before_it_is_posted_so_a_failed_post_is_replayed():
    ledger = MemoryClaims()
    first = Harness([job(job_id="flaky")], poster=FakePoster(fail=True), claims=ledger)
    await first.drain()
    assert first.sqs.deleted == [] and ledger.rows["flaky"][0] == "done"
    second = Harness([job(job_id="flaky")], claims=ledger)
    await second.drain()
    assert second.agent_calls == [], "the stored answer is re-posted; the model is not asked again"
    assert [p.answer for p in second.poster.posted] == ["Because the location was empty."]
    assert second.sqs.deleted == ["r0"]


# ==================================================== 3. the consumer process claims through Postgres

async def test_the_consumer_process_claims_and_finishes_through_postgres():
    await _wipe()
    q = QuestionJob(job_id=_request_id(), tenant_id="t", customer_code=CC, conversation_id="c",
                    question="why?", sender_object_id="u-1")
    assert (await teams_consumer._claim_run(q)).outcome == "claimed"
    assert (await teams_consumer._claim_run(q)).outcome == "running"
    await teams_consumer._finish_run(q, AnswerPayload(job_id=q.job_id, conversation_id="c", answer="ok"))
    done = await teams_consumer._claim_run(q)
    assert done.outcome == "done" and done.stored["answer"] == "ok"
    await _wipe()
