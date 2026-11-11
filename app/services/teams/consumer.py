"""The Teams question consumer: SQS job in, debugging-agent answer out, posted back to the edge.

Runs as its own process (`python -m app.teams_consumer`), deliberately NOT one of the singleton
worker's loops, so more than one copy can run and SQS will hand each copy different messages.

One message is one question. The lifecycle of a message:

  receive -> parse QuestionJob -> heartbeat starts (keeps extending the SQS visibility timeout)
          -> customer check -> history -> agent -> record memory -> post AnswerPayload -> delete

Every failure inside the agent still produces an answer (status=error, a short human sentence), so
the person in Teams is never left waiting. Only a failure to REACH the edge leaves the message in
the queue: it becomes visible again after the visibility timeout and is retried, and the queue's
redrive policy dead-letters it after a few attempts, which is the alarm.

All boto3 calls run on a worker thread; the event loop is never blocked by its synchronous client.
"""

import asyncio
import logging
import time
from typing import Awaitable, Callable, Protocol

import httpx
from pydantic import ValidationError

from app import log_context
from app.services.agent_runs.claims import Claim
from app.services.model_failures import classify
from app.services.teams.contracts import AnswerPayload, QuestionJob

logger = logging.getLogger(__name__)

ANSWER_FAILED = ("Sorry, I could not answer that one. The question has been logged so the team can "
                 "look at it.")
CUSTOMER_NOT_READY = ("Your organisation's log space is not active on our side yet. "
                      "Please contact your account manager.")
MODEL_UNAVAILABLE = ("The assistant's language model is not available right now (the AI service refused our "
                     "credentials or account). Your question has been logged; the team has been alerted.")
MODEL_BUSY = ("The AI service is busy or unreachable at the moment. Please try again in a minute.")
CUSTOMER_MISMATCH = ("This question was sent to a log space that is not your organisation's, so "
                     "it was not answered. Please try again in a minute; if it keeps happening, "
                     "contact your account manager.")
COMMAND_UNSUPPORTED = "This consumer cannot record actions yet; the check was not saved."


def failure_message(exc: BaseException) -> str:
    """Turn an agent failure into the sentence the person in Teams sees, by what the provider said.

    (Bedrock AccessDeniedException | anthropic AuthenticationError | a credit refusal)
    -> MODEL_UNAVAILABLE ; (throttling | 5xx | a call timeout | unreachable) -> MODEL_BUSY ;
    (anything else) -> ANSWER_FAILED
    """
    # The one cause worth naming: the model itself is absent, unfunded or refusing us. That is
    # an operator problem, not a data problem, and saying so stops people re-asking.
    if isinstance(exc, RuntimeError) and "anthropic_api_key" in str(exc):
        return MODEL_UNAVAILABLE
    kind = classify(exc)
    if kind in ("auth", "billing"):
        return MODEL_UNAVAILABLE
    if kind in ("throttled", "server_error", "timeout", "unreachable"):
        return MODEL_BUSY
    return ANSWER_FAILED


# ------------------------------------------------------------------------------- ports
class SqsClient(Protocol):
    """The three boto3 SQS calls we use, synchronous, exactly as boto3 spells them."""

    def receive_message(self, **kwargs) -> dict: ...
    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> None: ...
    def change_message_visibility(self, *, QueueUrl: str, ReceiptHandle: str, VisibilityTimeout: int) -> None: ...


class AnswerPoster(Protocol):
    async def post(self, payload: AnswerPayload) -> None:
        """Deliver the answer to the edge. Raise on any failure so the message is retried."""
        ...


#: Runs the agent for a job with the conversation's earlier turns; the adapter in
#: app/teams_consumer.py builds the request (who asked, where) from the job.
AgentRunner = Callable[[QuestionJob, list[dict]], Awaitable[dict]]
"""(customer_code, question, history) -> the agent's result dict ({"answer", "tool_calls", ...})."""

HistoryLoader = Callable[[str, str], Awaitable[list[dict]]]
"""(conversation_id, customer_code) -> prior turns, oldest first."""

ExchangeRecorder = Callable[[str, str, str, str, str], Awaitable[None]]
"""(conversation_id, customer_code, question, answer, job_id) -> None."""

#: The customer a Teams tenant id is bound to in Postgres, or None when the tenant is unbound,
#: the binding is disabled or the customer is inactive. The job's own customer_code is not trusted.
CustomerResolver = Callable[[str], Awaitable[str | None]]
"""customer_code -> True when the log space exists and is active."""

CommandRunner = Callable[[QuestionJob], Awaitable[str]]

#: Claims the run for a job before the model runs: whether to run, wait, or replay a stored answer.
RunClaimer = Callable[[QuestionJob], Awaitable[Claim]]

#: Stores the answer for a job before it is posted, so a redelivery can replay it.
RunFinisher = Callable[[QuestionJob, AnswerPayload], Awaitable[None]]
"""A job carrying a `command` -> the one-line confirmation to show. Raises on a refused command."""


# ------------------------------------------------------------------------------- HTTP poster
class HttpAnswerPoster:
    """POST the answer to the edge with the shared secret; retry transient failures a few times."""

    def __init__(self, url: str, secret: str, *, timeout_seconds: float = 20.0, attempts: int = 3,
                 backoff_seconds: float = 2.0):
        if not url or not secret:
            raise ValueError("edge answers URL and shared secret are required")
        self._url, self._secret = url, secret
        self._timeout, self._attempts, self._backoff = timeout_seconds, attempts, backoff_seconds

    async def post(self, payload: AnswerPayload) -> None:
        last: Exception | None = None
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            for attempt in range(1, self._attempts + 1):
                try:
                    resp = await client.post(self._url, json=payload.model_dump(mode="json"),
                                             headers={"X-Edge-Secret": self._secret})
                except httpx.HTTPError as exc:
                    last = exc
                else:
                    if resp.is_success:
                        if not resp.json().get("delivered", True):
                            logger.info("edge did not deliver job %s: %s", payload.job_id, resp.text[:200])
                        return
                    if resp.status_code == 404:
                        # the edge no longer has the conversation; nothing a retry can fix
                        logger.warning("edge has no conversation for job %s; answer dropped", payload.job_id)
                        return
                    if 400 <= resp.status_code < 500:
                        raise RuntimeError(f"edge rejected the answer: HTTP {resp.status_code} {resp.text[:200]}")
                    last = RuntimeError(f"edge HTTP {resp.status_code}")
                if attempt < self._attempts:
                    await asyncio.sleep(self._backoff * attempt)
        raise RuntimeError(f"could not deliver answer for job {payload.job_id}: {last}")


# ------------------------------------------------------------------------------- consumer
def cited_transaction_ids(result: dict) -> list[str]:
    """Transaction ids the agent actually opened or filtered by, in first-seen order."""
    seen: list[str] = []
    for call in result.get("tool_calls") or []:
        value = (call.get("input") or {}).get("transaction_id")
        if value and value not in seen:
            seen.append(str(value))
    return seen


class TeamsQuestionConsumer:
    def __init__(self, *, sqs: SqsClient, queue_url: str, poster: AnswerPoster, run_agent: AgentRunner,
                 load_history: HistoryLoader, record_exchange: ExchangeRecorder,
                 resolve_customer: CustomerResolver, concurrency: int, visibility_seconds: int,
                 wait_seconds: int = 20, heartbeat_seconds: float | None = None,
                 clock: Callable[[], float] = time.monotonic, run_command: CommandRunner | None = None,
                 run_timeout_seconds: float | None = None, claim_run: RunClaimer | None = None,
                 finish_run: RunFinisher | None = None):
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if visibility_seconds < 30:
            raise ValueError("visibility_seconds must be >= 30 (the heartbeat runs at a third of it)")
        # A run that can outlive the message's visibility would be handed to a second consumer
        # while the first is still answering it, so the deadline must sit inside the window.
        if run_timeout_seconds is not None and run_timeout_seconds >= visibility_seconds:
            raise ValueError(f"run_timeout_seconds ({run_timeout_seconds}) must be below "
                             f"visibility_seconds ({visibility_seconds})")
        self._sqs, self._queue_url, self._poster = sqs, queue_url, poster
        self._run_agent, self._load_history, self._record = run_agent, load_history, record_exchange
        self._resolve_customer = resolve_customer
        self._run_command = run_command
        self._claim_run, self._finish_run = claim_run, finish_run
        self._semaphore = asyncio.Semaphore(concurrency)
        self._concurrency = concurrency
        self._visibility, self._wait, self._clock = visibility_seconds, wait_seconds, clock
        # Extend well before the timeout expires: a third of it by default, so two extensions can be
        # lost to a network blip before SQS hands the message to another consumer.
        self._heartbeat_seconds = heartbeat_seconds if heartbeat_seconds is not None else visibility_seconds / 3
        self._in_flight: set[asyncio.Task] = set()

    @property
    def in_flight(self) -> int:
        return len(self._in_flight)

    async def run_forever(self, stop: asyncio.Event) -> None:
        logger.info("Teams consumer started (concurrency=%d, visibility=%ds)", self._concurrency, self._visibility)
        while not stop.is_set():
            try:
                dispatched = await self.poll_once()
            except Exception:
                logger.exception("Teams consumer poll failed; retrying after a pause")
                dispatched = 0
                await asyncio.sleep(5)
            if not dispatched and not stop.is_set():
                await asyncio.sleep(0)  # long poll already waited; yield and go again
        logger.info("Teams consumer stopping; waiting for %d in-flight question(s)", len(self._in_flight))
        if self._in_flight:
            await asyncio.gather(*self._in_flight, return_exceptions=True)

    async def poll_once(self) -> int:
        """Receive up to the free slots and dispatch each message as a task. Returns how many."""
        free = self._concurrency - len(self._in_flight)
        if free <= 0:
            await asyncio.sleep(0.5)
            return 0
        response = await asyncio.to_thread(
            self._sqs.receive_message,
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=min(10, free),
            WaitTimeSeconds=self._wait,
            VisibilityTimeout=self._visibility,
            MessageAttributeNames=["All"],
        )
        messages = response.get("Messages") or []
        for message in messages:
            task = asyncio.create_task(self._guarded(message))
            self._in_flight.add(task)
            task.add_done_callback(self._in_flight.discard)
        return len(messages)

    async def _guarded(self, message: dict) -> None:
        async with self._semaphore:
            try:
                await self.process_message(message)
            except Exception:
                logger.exception("Teams consumer: unhandled failure processing message %s",
                                 message.get("MessageId"))

    async def process_message(self, message: dict) -> None:
        """Answer one queued question exactly once; delete its message once the answer is delivered.

        (new job) -> runs, stores, posts, deletes ; (job done) -> re-posts the stored answer ;
        (job running elsewhere) -> leaves the message ; (edge unreachable) -> leaves the message
        """
        receipt = message["ReceiptHandle"]
        try:
            job = QuestionJob.model_validate_json(message.get("Body") or "")
        except ValidationError:
            logger.error("Teams consumer: dropping malformed job %s: %r", message.get("MessageId"),
                         (message.get("Body") or "")[:300])
            await self._delete(receipt)
            return

        # Every line of this job's processing names the job and its tenant, heartbeat included.
        with log_context.bind(request_id=job.job_id, tenant=job.customer_code):
            await self._process_job(job, receipt)

    async def _process_job(self, job: QuestionJob, receipt: str) -> None:
        heartbeat = asyncio.create_task(self._heartbeat(receipt))
        started = self._clock()
        try:
            # Step 1: Claim the run before anything else, so that a redelivered message is never
            # answered twice: a claim another consumer holds is left for the queue to show again,
            # and a finished one is replayed from the ledger without asking the model.
            claim = await self._claim_run(job) if self._claim_run is not None else None
            if claim is not None and claim.outcome == "running":
                logger.info("Teams job %s is being answered by another consumer; leaving it",
                            job.job_id)
                return
            if claim is not None and claim.outcome == "done":
                await self._poster.post(AnswerPayload.model_validate(claim.stored))
                await self._delete(receipt)
                logger.info("Teams job %s re-posted from the run ledger", job.job_id)
                return
            # Step 2: Run, then store the answer before posting it, so that a post the edge never
            # received is replayed from the ledger on redelivery instead of being computed again.
            payload = await self._answer(job)
            payload.duration_seconds = round(self._clock() - started, 1)
            if self._finish_run is not None:
                await self._finish_run(job, payload)
            await self._poster.post(payload)  # raises -> message stays visible for a retry
            await self._delete(receipt)
            logger.info("Teams job %s answered for %s in %.1fs (%s)", job.job_id, job.customer_code,
                        payload.duration_seconds, payload.status)
        finally:
            heartbeat.cancel()

    async def _answer(self, job: QuestionJob) -> AnswerPayload:
        base = dict(job_id=job.job_id, conversation_id=job.conversation_id)
        try:
            # Step 1: Resolve the customer from the tenant binding in Postgres, not from the job,
            # because the edge's copy can be stale and a forged job could name any log space.
            bound = await self._resolve_customer(job.tenant_id)
            if bound is None:
                logger.warning("Teams job %s: tenant %s unbound, disabled or its customer inactive",
                               job.job_id, job.tenant_id)
                return AnswerPayload(**base, status="error", answer=CUSTOMER_NOT_READY)
            if bound != job.customer_code:
                logger.warning("Teams job %s refused: tenant %s is bound to %s, the job names %s",
                               job.job_id, job.tenant_id, bound, job.customer_code)
                return AnswerPayload(**base, status="error", answer=CUSTOMER_MISMATCH)
            if job.command is not None:
                # An action, not a question: the agent never runs, and nothing is recorded as a turn.
                if self._run_command is None:
                    return AnswerPayload(**base, status="error", answer=COMMAND_UNSUPPORTED)
                try:
                    return AnswerPayload(**base, status="ok", answer=await self._run_command(job))
                except Exception as exc:  # noqa: BLE001 - a refused command is a sentence, never a crash
                    logger.warning("Teams job %s: command %s refused: %s", job.job_id, job.command.kind, exc)
                    return AnswerPayload(**base, status="error", answer=f"Could not record that: {exc}")
            history = await self._load_history(job.conversation_id, job.customer_code)
            result = await self._run_agent(job, history)
            answer = (result.get("answer") or "").strip() or "I could not find anything to say about that."
            await self._record(job.conversation_id, job.customer_code, job.question, answer, job.job_id)
            return AnswerPayload(**base, status="ok", answer=answer,
                                 cited_transaction_ids=cited_transaction_ids(result),
                                 tool_call_count=len(result.get("tool_calls") or []),
                                 evidence=result.get("evidence_data") or None)
        except Exception as exc:
            message = failure_message(exc)
            log = logger.critical if message == MODEL_UNAVAILABLE else logger.exception
            log("Teams job %s: agent failed for %s: %s", job.job_id, job.customer_code, exc,
                exc_info=message != MODEL_UNAVAILABLE)
            return AnswerPayload(**base, status="error", answer=message)

    async def _heartbeat(self, receipt: str) -> None:
        try:
            while True:
                await asyncio.sleep(self._heartbeat_seconds)
                try:
                    await asyncio.to_thread(self._sqs.change_message_visibility, QueueUrl=self._queue_url,
                                            ReceiptHandle=receipt, VisibilityTimeout=self._visibility)
                except Exception:  # noqa: BLE001 - losing one extension is not fatal; the next tick retries
                    logger.warning("Teams consumer: visibility extension failed", exc_info=True)
        except asyncio.CancelledError:
            pass

    async def _delete(self, receipt: str) -> None:
        await asyncio.to_thread(self._sqs.delete_message, QueueUrl=self._queue_url, ReceiptHandle=receipt)
