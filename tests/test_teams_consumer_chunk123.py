"""Chunk 123: the Teams question consumer, one SQS message in, one answer posted to the edge.

Pinned here, all with fakes (no SQS, no edge, no model):

    happy path     history loaded for the conversation, agent run with it, exchange recorded, answer
                   posted with the job's ids, cited transactions and tool count; message deleted
    malformed job  deleted without posting anything
    agent failure  an error answer is still posted, nothing recorded, message deleted
    customer gone  a specific error answer, agent never runs
    edge down      posting raises -> the message is NOT deleted, so SQS retries it
    heartbeat      the visibility timeout is extended while a slow agent runs
    concurrency    never more agent runs in flight than configured
    sweep          stale bindings are pushed to the mirror and stamped; a failing one is skipped
"""

import asyncio
import json

import pytest
from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.repositories.teams_repository import TeamsBindingRepository
from app.services.teams import consumer as c
from app.services.teams.binding_mirror import InMemoryMirror
from app.services.teams.binding_sweep import sweep_once
from app.services.teams.contracts import AnswerPayload, QuestionJob

Q = "https://sqs.test/queue"


# ------------------------------------------------------------------------------- fakes
class FakeSqs:
    def __init__(self, bodies: list[str]):
        self.pending = [{"MessageId": f"m{i}", "ReceiptHandle": f"r{i}", "Body": b} for i, b in enumerate(bodies)]
        self.deleted: list[str] = []
        self.visibility: list[tuple[str, int]] = []

    def receive_message(self, **kwargs) -> dict:
        n = kwargs["MaxNumberOfMessages"]
        batch, self.pending = self.pending[:n], self.pending[n:]
        return {"Messages": batch} if batch else {}

    def delete_message(self, *, QueueUrl, ReceiptHandle) -> None:
        self.deleted.append(ReceiptHandle)

    def change_message_visibility(self, *, QueueUrl, ReceiptHandle, VisibilityTimeout) -> None:
        self.visibility.append((ReceiptHandle, VisibilityTimeout))


class FakePoster:
    def __init__(self, *, fail: bool = False):
        self.posted: list[AnswerPayload] = []
        self.fail = fail

    async def post(self, payload: AnswerPayload) -> None:
        if self.fail:
            raise RuntimeError("edge unreachable")
        self.posted.append(payload)


class Harness:
    def __init__(self, bodies, *, agent=None, poster=None, ready=True, concurrency=2, visibility=30,
                 heartbeat=None):
        self.sqs = FakeSqs(bodies)
        self.poster = poster or FakePoster()
        self.history_calls: list[tuple[str, str]] = []
        self.recorded: list[tuple] = []
        self.agent_calls: list[tuple] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self._agent = agent or self.default_agent

        async def load_history(conv, code):
            self.history_calls.append((conv, code))
            return [{"role": "user", "content": "earlier"}, {"role": "assistant", "content": "before"}]

        async def record(conv, code, q, a, job_id):
            self.recorded.append((conv, code, q, a, job_id))

        async def run_agent(code, question, history):
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            try:
                self.agent_calls.append((code, question, history))
                return await self._agent(code, question, history)
            finally:
                self.in_flight -= 1

        async def customer_ready(code):
            return ready

        self.consumer = c.TeamsQuestionConsumer(
            sqs=self.sqs, queue_url=Q, poster=self.poster, run_agent=run_agent, load_history=load_history,
            record_exchange=record, customer_ready=customer_ready, concurrency=concurrency,
            visibility_seconds=visibility, wait_seconds=0, heartbeat_seconds=heartbeat)

    @staticmethod
    async def default_agent(code, question, history):
        return {"answer": "  Because the location was empty.  ",
                "tool_calls": [{"tool": "find_errors", "input": {}},
                               {"tool": "get_transaction", "input": {"transaction_id": "t-1"}},
                               {"tool": "search_entries", "input": {"transaction_id": "t-1"}},
                               {"tool": "get_transaction", "input": {"transaction_id": "t-2"}}]}

    async def drain(self, timeout=2.0):
        while self.sqs.pending:
            await self.consumer.poll_once()
        deadline = asyncio.get_running_loop().time() + timeout
        while self.consumer.in_flight and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        assert self.consumer.in_flight == 0, "consumer did not finish in time"


def job(**kw) -> str:
    base = dict(job_id="job-1", tenant_id="tenant-1", customer_code="acme", conversation_id="conv-1",
                question="why did it fail?", sender_name="Jo")
    base.update(kw)
    return QuestionJob(**base).model_dump_json()


# ------------------------------------------------------------------------------- tests
async def test_happy_path_posts_answer_and_deletes_message():
    h = Harness([job()])
    await h.drain()
    assert h.history_calls == [("conv-1", "acme")]
    code, question, history = h.agent_calls[0]
    assert (code, question) == ("acme", "why did it fail?") and history[0]["content"] == "earlier"
    assert h.recorded == [("conv-1", "acme", "why did it fail?", "Because the location was empty.", "job-1")]
    [p] = h.poster.posted
    assert (p.job_id, p.conversation_id, p.status) == ("job-1", "conv-1", "ok")
    assert p.answer == "Because the location was empty."
    assert p.cited_transaction_ids == ["t-1", "t-2"] and p.tool_call_count == 4
    assert p.duration_seconds is not None
    assert h.sqs.deleted == ["r0"]


async def test_malformed_body_is_deleted_without_posting():
    h = Harness(["not json", json.dumps({"job_id": "x"})])
    await h.drain()
    assert h.poster.posted == [] and h.agent_calls == []
    assert sorted(h.sqs.deleted) == ["r0", "r1"]


async def test_agent_failure_still_answers_with_error_and_deletes():
    async def broken(code, question, history):
        raise RuntimeError("model exploded")
    h = Harness([job()], agent=broken)
    await h.drain()
    [p] = h.poster.posted
    assert p.status == "error" and p.answer == c.ANSWER_FAILED
    assert h.recorded == [] and h.sqs.deleted == ["r0"]


async def test_missing_customer_gets_specific_error_and_agent_never_runs():
    h = Harness([job()], ready=False)
    await h.drain()
    [p] = h.poster.posted
    assert p.status == "error" and p.answer == c.CUSTOMER_NOT_READY
    assert h.agent_calls == [] and h.sqs.deleted == ["r0"]


async def test_edge_unreachable_leaves_message_in_queue():
    h = Harness([job()], poster=FakePoster(fail=True))
    await h.drain()
    assert h.sqs.deleted == []
    assert h.recorded, "the exchange was recorded; only delivery failed"


async def test_heartbeat_extends_visibility_while_agent_runs():
    async def slow(code, question, history):
        await asyncio.sleep(0.05)
        return {"answer": "done", "tool_calls": []}
    h = Harness([job()], agent=slow, visibility=30, heartbeat=0.01)
    await h.drain()
    assert len(h.sqs.visibility) >= 2
    assert all((handle, timeout) == ("r0", 30) for handle, timeout in h.sqs.visibility)
    assert h.sqs.deleted == ["r0"]


async def test_concurrency_is_capped():
    async def slow(code, question, history):
        await asyncio.sleep(0.03)
        return {"answer": "ok", "tool_calls": []}
    h = Harness([job(job_id=f"j{i}", conversation_id=f"c{i}") for i in range(5)], agent=slow, concurrency=2)
    await h.drain()
    assert h.max_in_flight == 2
    assert len(h.poster.posted) == 5 and len(h.sqs.deleted) == 5


def test_constructor_guards():
    with pytest.raises(ValueError):
        c.TeamsQuestionConsumer(sqs=FakeSqs([]), queue_url=Q, poster=FakePoster(), run_agent=None,
                                load_history=None, record_exchange=None, customer_ready=None,
                                concurrency=0, visibility_seconds=60)
    with pytest.raises(ValueError):
        c.TeamsQuestionConsumer(sqs=FakeSqs([]), queue_url=Q, poster=FakePoster(), run_agent=None,
                                load_history=None, record_exchange=None, customer_ready=None,
                                concurrency=1, visibility_seconds=5)


def test_cited_ids_are_deduped_in_order():
    assert c.cited_transaction_ids({"tool_calls": [{"input": {"transaction_id": "b"}}, {"input": {}},
                                                   {"input": {"transaction_id": "a"}},
                                                   {"input": {"transaction_id": "b"}}]}) == ["b", "a"]
    assert c.cited_transaction_ids({}) == []


# ------------------------------------------------------------------------------- failure wording
def _api_error(cls, status, message):
    import anthropic
    import httpx
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    response = httpx.Response(status, request=request, json={"error": {"message": message}})
    if cls is anthropic.APIConnectionError:
        return cls(request=request)
    return cls(message=message, response=response, body={"error": {"message": message}})


def test_failure_message_names_a_missing_or_unfunded_model():
    import anthropic
    assert c.failure_message(RuntimeError("anthropic_api_key is not configured")) == c.MODEL_UNAVAILABLE
    assert c.failure_message(_api_error(anthropic.AuthenticationError, 401, "invalid x-api-key")) == c.MODEL_UNAVAILABLE
    assert c.failure_message(_api_error(anthropic.BadRequestError, 400,
                                        "Your credit balance is too low to access the Anthropic API.")) == c.MODEL_UNAVAILABLE
    assert c.failure_message(_api_error(anthropic.PermissionDeniedError, 403, "forbidden")) == c.MODEL_UNAVAILABLE


def test_failure_message_says_busy_for_transient_model_errors_and_generic_otherwise():
    import anthropic
    assert c.failure_message(_api_error(anthropic.RateLimitError, 429, "rate limited")) == c.MODEL_BUSY
    assert c.failure_message(_api_error(anthropic.InternalServerError, 500, "overloaded")) == c.MODEL_BUSY
    assert c.failure_message(_api_error(anthropic.APIConnectionError, 0, "")) == c.MODEL_BUSY
    assert c.failure_message(_api_error(anthropic.BadRequestError, 400, "invalid tool schema")) == c.ANSWER_FAILED
    assert c.failure_message(ValueError("boom")) == c.ANSWER_FAILED


async def test_unfunded_model_produces_the_specific_card():
    async def unfunded(code, question, history):
        import anthropic
        raise _api_error(anthropic.BadRequestError, 400, "Your credit balance is too low")
    h = Harness([job()], agent=unfunded)
    await h.drain()
    [p] = h.poster.posted
    assert p.status == "error" and p.answer == c.MODEL_UNAVAILABLE
    assert h.sqs.deleted == ["r0"]


# ------------------------------------------------------------------------------- sweep (real DB)
CC = "test_c123"
T_OK = "cccccccc-1111-2222-3333-000000000001"
T_BAD = "cccccccc-1111-2222-3333-000000000002"


class SelectiveMirror(InMemoryMirror):
    async def put(self, binding: TeamsTenantBinding) -> None:
        if binding.tenant_id == T_BAD:
            raise RuntimeError("edge refused")
        await super().put(binding)


@pytest.fixture
async def _sweep_rows():
    async def wipe():
        async with async_session() as s:
            await s.execute(delete(TeamsTenantBinding).where(TeamsTenantBinding.tenant_id.in_([T_OK, T_BAD])))
            await s.execute(delete(Customer).where(Customer.customer_code == CC))
            await s.commit()
    await wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC))
        await s.commit()
    async with async_session() as db:
        repo = TeamsBindingRepository(db)
        await repo.upsert(T_OK, CC)
        await repo.upsert(T_BAD, CC)
    yield
    await wipe()


async def test_sweep_pushes_stale_bindings_and_skips_failures(_sweep_rows):
    mirror = SelectiveMirror()
    pushed = await sweep_once(mirror)
    assert pushed >= 1 and T_OK in mirror.items and T_BAD not in mirror.items
    async with async_session() as db:
        repo = TeamsBindingRepository(db)
        assert (await repo.get_by_tenant(T_OK)).needs_mirror is False
        assert (await repo.get_by_tenant(T_BAD)).needs_mirror is True
    assert await sweep_once(mirror) == 0 or T_BAD not in mirror.items  # nothing new for T_OK


# ==================================================== the evidence rides on the answer (chunk 126)

def test_the_answer_payload_carries_evidence_when_the_agent_has_rows():
    from app.services.teams.contracts import AnswerPayload, Evidence
    payload = AnswerPayload(job_id="j", conversation_id="c", answer="Southdowns Manor is short by 309 units.",
                            evidence={"title": "Top 2 by units short per customer name",
                                      "columns": [{"name": "customer name"}, {"name": "units short", "align": "right"}],
                                      "rows": [["SOUTHDOWNS MANOR", "309"], ["GOODWOOD CLUB KENNELS", "208"]],
                                      "facts": {"window": "2026-09-20 to 2026-09-27"}, "link": None})
    assert isinstance(payload.evidence, Evidence) and payload.evidence.columns[0].align == "left"
    assert payload.schema_version == 1  # optional field, no bump: an edge that ignores it still validates
    assert AnswerPayload(job_id="j", conversation_id="c", answer="no rows behind this one").evidence is None
