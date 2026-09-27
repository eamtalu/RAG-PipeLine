"""Teams question consumer entrypoint: `python -m app.teams_consumer`.

Its own process, next to the web tier and the singleton background worker. Unlike the worker it may
run as several copies (one per machine, or several per machine): SQS delivers each message to one
consumer, and the edge's job store makes answer delivery idempotent, so copies never need to
coordinate. Scale by starting another instance.
"""

import asyncio
import logging
import signal

import boto3

from app.background import setup_logging
from app.config.database import async_session, engine
from app.persistence.repositories.customer_repository import CustomerRepository
from app.services.log_agent.agent import LogDebugAgent
from app.services.teams.binding_mirror import build_mirror_from_settings
from app.services.teams.binding_sweep import sweep_once
from app.services.teams.consumer import HttpAnswerPoster, TeamsQuestionConsumer
from app.services.teams.memory import ConversationMemory
from app.settings import settings

logger = logging.getLogger(__name__)


async def _run_agent(customer_code: str, question: str, history: list[dict]) -> dict:
    async with async_session() as db:
        return await LogDebugAgent(db, customer_code).ask(question, history=history)


async def _load_history(conversation_id: str, customer_code: str) -> list[dict]:
    async with async_session() as db:
        return await ConversationMemory(db, max_turns=settings.teams_history_turns).history(
            conversation_id, customer_code)


async def _record_exchange(conversation_id: str, customer_code: str, question: str, answer: str,
                           job_id: str) -> None:
    async with async_session() as db:
        await ConversationMemory(db, max_turns=settings.teams_history_turns).record(
            conversation_id, customer_code, question=question, answer=answer, job_id=job_id)


async def _customer_ready(customer_code: str) -> bool:
    async with async_session() as db:
        return await CustomerRepository(db).exists(customer_code, must_be_active=True)


async def _sweep_loop(stop: asyncio.Event) -> None:
    mirror = build_mirror_from_settings()
    while not stop.is_set():
        try:
            await sweep_once(mirror)
        except Exception:
            logger.exception("binding mirror sweep failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.teams_binding_mirror_sweep_seconds)
        except asyncio.TimeoutError:
            pass


def _require(value: str, name: str) -> str:
    if not value:
        raise SystemExit(f"{name} must be set to run the Teams consumer")
    return value


async def _amain() -> None:
    setup_logging()
    queue_url = _require(settings.teams_sqs_queue_url, "TEAMS_SQS_QUEUE_URL")
    poster = HttpAnswerPoster(_require(settings.teams_edge_answers_url, "TEAMS_EDGE_ANSWERS_URL"),
                              _require(settings.teams_edge_shared_secret, "TEAMS_EDGE_SHARED_SECRET"))
    sqs = boto3.client("sqs", region_name=settings.teams_aws_region,
                       endpoint_url=settings.teams_aws_endpoint_url or None)
    consumer = TeamsQuestionConsumer(
        sqs=sqs, queue_url=queue_url, poster=poster, run_agent=_run_agent, load_history=_load_history,
        record_exchange=_record_exchange, customer_ready=_customer_ready,
        concurrency=settings.teams_consumer_concurrency,
        visibility_seconds=settings.teams_sqs_visibility_seconds, wait_seconds=settings.teams_sqs_wait_seconds)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    try:
        await asyncio.gather(consumer.run_forever(stop), _sweep_loop(stop))
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
