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

import anthropic
import httpx
from pydantic import ValidationError

from app.services.teams.contracts import AnswerPayload, QuestionJob

logger = logging.getLogger(__name__)

ANSWER_FAILED = ("Sorry, I could not answer that one. The question has been logged so the team can "
                 "look at it.")
CUSTOMER_NOT_READY = ("Your organisation's log space is not active on our side yet. "
                      "Please contact your account manager.")
MODEL_UNAVAILABLE = ("The assistant's language model is not available right now (the AI service refused our "
                     "credentials or account). Your question has been logged; the team has been alerted.")
MODEL_BUSY = ("The AI service is busy or unreachable at the moment. Please try again in a minute.")


def failure_message(exc: BaseException) -> str:
    """Turn an agent failure into the sentence the person in Teams sees.

    The generic sentence hides the one cause that is worth naming: the language model itself is
    absent, unfunded or refusing us. That is an operator problem, not a data problem, and saying so
    stops people re-asking and stops the team looking for a bug in the question.
    """
    if isinstance(exc, (anthropic.AuthenticationError, anthropic.PermissionDeniedError)):
        return MODEL_UNAVAILABLE
    if isinstance(exc, anthropic.BadRequestError) and "credit" in str(exc).lower():
        return MODEL_UNAVAILABLE
    if isinstance(exc, RuntimeError) and "anthropic_api_key" in str(exc):
        return MODEL_UNAVAILABLE
    if isinstance(exc, (anthropic.RateLimitError, anthropic.APIConnectionError, anthropic.InternalServerError)):
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


AgentRunner = Callable[[str, str, list[dict]], Awaitable[dict]]
"""(customer_code, question, history) -> the agent's result dict ({"answer", "tool_calls", ...})."""

HistoryLoader = Callable[[str, str], Awaitable[list[dict]]]
"""(conversation_id, customer_code) -> prior turns, oldest first."""

ExchangeRecorder = Callable[[str, str, str, str, str], Awaitable[None]]
"""(conversation_id, customer_code, question, answer, job_id) -> None."""

CustomerCheck = Callable[[str], Awaitable[bool]]
"""customer_code -> True when the log space exists and is active."""


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
                 customer_ready: CustomerCheck, concurrency: int, visibility_seconds: int,
                 wait_seconds: int = 20, heartbeat_seconds: float | None = None,
                 clock: Callable[[], float] = time.monotonic):
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if visibility_seconds < 30:
            raise ValueError("visibility_seconds must be >= 30 (the heartbeat runs at a third of it)")
        self._sqs, self._queue_url, self._poster = sqs, queue_url, poster
        self._run_agent, self._load_history, self._record = run_agent, load_history, record_exchange
        self._customer_ready = customer_ready
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
        receipt = message["ReceiptHandle"]
        try:
            job = QuestionJob.model_validate_json(message.get("Body") or "")
        except ValidationError:
            logger.error("Teams consumer: dropping malformed job %s: %r", message.get("MessageId"),
                         (message.get("Body") or "")[:300])
            await self._delete(receipt)
            return

        heartbeat = asyncio.create_task(self._heartbeat(receipt))
        started = self._clock()
        try:
            payload = await self._answer(job)
            payload.duration_seconds = round(self._clock() - started, 1)
            await self._poster.post(payload)  # raises -> message stays visible for a retry
            await self._delete(receipt)
            logger.info("Teams job %s answered for %s in %.1fs (%s)", job.job_id, job.customer_code,
                        payload.duration_seconds, payload.status)
        finally:
            heartbeat.cancel()

    async def _answer(self, job: QuestionJob) -> AnswerPayload:
        base = dict(job_id=job.job_id, conversation_id=job.conversation_id)
        try:
            if not await self._customer_ready(job.customer_code):
                logger.warning("Teams job %s: customer %s missing or inactive", job.job_id, job.customer_code)
                return AnswerPayload(**base, status="error", answer=CUSTOMER_NOT_READY)
            history = await self._load_history(job.conversation_id, job.customer_code)
            result = await self._run_agent(job.customer_code, job.question, history)
            answer = (result.get("answer") or "").strip() or "I could not find anything to say about that."
            await self._record(job.conversation_id, job.customer_code, job.question, answer, job.job_id)
            return AnswerPayload(**base, status="ok", answer=answer,
                                 cited_transaction_ids=cited_transaction_ids(result),
                                 tool_call_count=len(result.get("tool_calls") or []))
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
