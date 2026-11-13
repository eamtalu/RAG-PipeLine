"""Teams question consumer entrypoint: `python -m app.teams_consumer`.

Its own process, next to the web tier and the singleton background worker. Unlike the worker it may
run as several copies (one per machine, or several per machine): SQS delivers each message to one
consumer, and the edge's job store makes answer delivery idempotent, so copies never need to
coordinate. Scale by starting another instance.
"""

import asyncio
import logging
from decimal import Decimal
import signal
from datetime import datetime, timedelta, timezone

import boto3

from app.agent.contracts import AgentRequest, ConversationType, Principal
from app.agent.capabilities.registry import ToolRegistry
from app.agent.gateway import run_request
from app.agent.profiles import load_profile
from app.agent.router.router import ModelRouter
from app.agent.runtime.grounded import GroundedAgent
from app.background import setup_logging
from app.config.database import async_session, engine
from app.persistence.repositories.customer_repository import CustomerRepository
from app.persistence.repositories.teams_repository import TeamsBindingRepository, TeamsConversationRepository
from app.services.agent_runs import claims
from app.services.agent_runs.claims import Claim
from app.services.agent_quotas.admission import PostgresAdmission, QuotaLimits
from app.services.agent_traces.store import PostgresTraceStore
from app.services.agent_usage.store import PostgresUsageStore
from app.services.analytics_agent.namespaces import (actions_namespace, analytics_namespace, logs_namespace,
                                                     presentation_namespace)
from app.services.logspace_agent.namespaces import logspace_namespace
from app.services.teams.binding_mirror import build_mirror_from_settings
from app.services.teams.binding_sweep import sweep_once
from app.services.teams.commands import run_command
from app.services.teams.consumer import HttpAnswerPoster, TeamsQuestionConsumer
from app.services.teams.contracts import AnswerPayload, QuestionJob
from app.services.teams.home_snapshot import build_writer_from_settings
from app.services.teams.home_snapshot import sweep_once as snapshot_sweep_once
from app.services.teams.mirror import ChangeTracker, build_mirror_writer_from_settings
from app.services.teams.freshness import freshness_for
from app.services.access import approvals as access_approvals
from app.services.access import audit as access_audit
from app.services.access import mirror as access_mirror
from app.services.access import records as access_records
from app.services.access import users as access_users
from app.services.advisories import runner as advisories_runner
from app.services.notifications import dispatcher as notifications
from app.services.tasks import mirror as tasks_mirror
from app.services.tasks import runner as tasks_runner
from app.services.teams import home_snapshot
from app.services.teams.sites import list_sites as list_customer_sites
from app.services.teams import exec_home, history
from app.services.teams.memory import ConversationMemory
from app.services.teams.progress import ProgressReporter
from app.settings import settings

logger = logging.getLogger(__name__)


# This process's registry: every namespace, so the profile's list and the clash rule mean the
# same here as in the API; the runners open their own sessions on the app pool.
_REGISTRY = ToolRegistry([logs_namespace(), analytics_namespace(), logspace_namespace(),
                          presentation_namespace(settings.analytics_agent_presentation),
                          actions_namespace(settings.analytics_agent_presentation)])
# Where the gateway stores each run's redacted trace (agent_traces), in its own short session.
_TRACES = PostgresTraceStore(async_session)
# Admission before any model call: shared counters every consumer and web process changes.
_ADMISSION = PostgresAdmission(async_session, QuotaLimits.from_settings())
# One router per process: it holds the providers' breakers and bulkheads for every run here.
_ROUTER = ModelRouter()
# Where every run's model calls are summed per tenant and day (usage_daily).
_USAGE = PostgresUsageStore(async_session)


def _conversation_type(source: str) -> ConversationType:
    """The kind of conversation a job came from; both doors are one person today.

    >>> _conversation_type("tab"), _conversation_type("bot")
    ('personal', 'personal')
    """
    return "personal"  # "group" and "channel" arrive when the edge sends them (contract v3)


def framed_question(job: QuestionJob) -> str:
    r"""The question as the agent reads it: the person's words, and the context the page sent.

    >>> framed_question(QuestionJob(job_id="j", tenant_id="t", customer_code="c", conversation_id="c",
    ...                 sender_object_id="u", question="Why is this empty?", context={"advisory_id": "a-1"}))
    'Why is this empty?\n\n(About advisory a-1: call explain_advisory with this id first.)'
    >>> framed_question(QuestionJob(job_id="j", tenant_id="t", customer_code="c", conversation_id="c",
    ...                 sender_object_id="u", question="How many?"))
    'How many?'
    """
    advisory_id = (job.context or {}).get("advisory_id")
    if advisory_id:
        return f"{job.question}\n\n(About advisory {advisory_id}: call explain_advisory with this id first.)"
    return job.question


async def _run_agent(job: QuestionJob, history: list[dict], progress=None) -> dict:
    """One Teams question answered under a context built from the job; the legacy result dict.

    (job for acme by u-7, earlier turns) -> {"answer", "stop_reason", "tool_calls", ...}
    """
    # Step 1: Build the request from the job, so that who asked and where is fixed before any
    # model runs; the gateway builds the context and the runtime never reads settings.
    request = AgentRequest(
        request_id=job.job_id, tenant=job.customer_code,
        principal=Principal(tenant=job.customer_code, object_id=job.sender_object_id,
                            display_name=job.sender_name),
        profile="analytics", text=framed_question(job), conversation_id=job.conversation_id,
        conversation_type=_conversation_type(job.source), site_id=job.site_id)
    # Step 2: Name the profile; it lists its namespaces, and the registry binds their tools.
    runtime = GroundedAgent(job.customer_code, profile=load_profile("analytics"),
                            registry=_REGISTRY, router=_ROUTER)
    result = await run_request(request, runtime=runtime, prior_turns=tuple(history),
                               trace_store=_TRACES, admission=_ADMISSION, usage_store=_USAGE,
                               on_event=progress.on_event if progress is not None else None)
    return result.to_legacy_dict()


async def _load_history(conversation_id: str, customer_code: str) -> list[dict]:
    async with async_session() as db:
        return await ConversationMemory(db, max_turns=settings.teams_history_turns).history(
            conversation_id, customer_code)


async def _read_freshness(customer_code: str) -> dict:
    """When the customer's figures are from and whether they are still moving, for the answer's stamp.

    ("tmp-live" with unsealed share 0.3) -> {"data_as_of": <watermark>, "provisional": True}
    """
    async with async_session() as db:
        fresh = await freshness_for(db, customer_code, threshold=Decimal(str(settings.teams_provisional_share)))
    return {"data_as_of": fresh.data_as_of, "provisional": fresh.provisional}


async def _record_exchange(job: QuestionJob, payload: AnswerPayload) -> None:
    """Store the exchange for the agent's memory and the history, then mirror the history rows.

    (job j1 by u-7, payload) -> two turn rows ; USER#u-7#HISTORY and USER#u-7#TURN#j1 rewritten
    """
    now = history.utc_now()
    async with async_session() as db:
        await ConversationMemory(db, max_turns=settings.teams_history_turns).record(
            job.conversation_id, job.customer_code, question=job.question, answer=payload.answer,
            job_id=job.job_id, details=history.details_from(job, payload))
        # the thread's header: created on the first question, bumped on every later one (step 6b)
        await TeamsConversationRepository(db).touch_conversation(
            job.customer_code, job.conversation_id, sender_object_id=job.sender_object_id,
            title=job.question, site_id=job.site_id, now=now)
        # the mirror is a nicety: a failed write is logged, the answer is already stored and posted
        mirror = _mirror_writer()
        if mirror is None:
            return
        try:
            await history.mirror_turn(mirror, job, payload, now=now)
            await history.mirror_user_history(
                db, mirror, job.customer_code, job.sender_object_id, now=now,
                retention_days=settings.teams_history_retention_days,
                limit=settings.teams_history_entries)
        except Exception:
            logger.exception("teams.history_mirror_failed job=%s", job.job_id)


_MIRROR: list = []  # the one mirror writer of this process, built on first use


def _mirror_writer():
    if not _MIRROR:
        _MIRROR.append(build_mirror_writer_from_settings())
    return _MIRROR[0]


def _progress_factory(job: QuestionJob):
    """A reporter writing the job's progress row, or None on an edge with no mirror table."""
    mirror = _mirror_writer()
    return ProgressReporter(mirror, job.customer_code, job.job_id) if mirror is not None else None


async def _history_loop(stop: asyncio.Event) -> None:
    """Purge turns older than the retention (step 6), a batch at a time, every hour."""
    while not stop.is_set():
        try:
            while not stop.is_set():
                async with async_session() as db:
                    gone = await history.purge_once(
                        db, now=history.utc_now(),
                        retention_days=settings.teams_history_retention_days)
                if not gone:
                    break
        except Exception:
            logger.exception("teams history purge failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.teams_history_purge_seconds)
        except asyncio.TimeoutError:
            pass


async def _claim_run(job: QuestionJob) -> Claim:
    """Claim the job's run in the ledger; a claim older than the visibility window counts as dead.

    (new job) -> Claim("claimed") ; (same job again while running) -> Claim("running")
    """
    async with async_session() as db:
        claim = await claims.try_claim(
            db, request_id=job.job_id, customer_code=job.customer_code, channel="teams",
            now=datetime.now(timezone.utc),
            stale_after=timedelta(seconds=settings.teams_sqs_visibility_seconds))
        await db.commit()
        return claim


async def _finish_run(job: QuestionJob, payload: AnswerPayload) -> None:
    async with async_session() as db:
        await claims.finish(db, request_id=job.job_id, result=payload.model_dump(mode="json"),
                            now=datetime.now(timezone.utc))
        await db.commit()


async def _resolve_customer(tenant_id: str) -> str | None:
    """The active customer this Teams tenant is bound to in Postgres, or None.

    (bound, enabled, customer active) -> "acme" ; (unbound | disabled | customer inactive) -> None
    """
    async with async_session() as db:
        binding = await TeamsBindingRepository(db).get_by_tenant(tenant_id)
        if binding is None or not binding.enabled:
            return None
        if not await CustomerRepository(db).exists(binding.customer_code, must_be_active=True):
            return None
        return binding.customer_code


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


async def _snapshot_loop(stop: asyncio.Event) -> None:
    """The Teams tab's Home numbers, rewritten every minute per bound customer (chunk 127)."""
    writer = build_writer_from_settings()
    if writer is None:
        logger.info("home snapshot loop off: TEAMS_EDGE_DYNAMODB_TABLE is not set")
        return
    # The new page's rows (stage D) go to the same table under EYE#; unchanged rows are skipped but
    # rewritten well before their one-hour TTL
    mirror = build_mirror_writer_from_settings()
    tracker = ChangeTracker(max_age_seconds=settings.teams_home_snapshot_seconds * 20)
    while not stop.is_set():
        try:
            written = await snapshot_sweep_once(writer, mirror=mirror, tracker=tracker,
                                                max_sites=settings.teams_max_sites_per_customer)
            logger.debug("home snapshot written for %s", written)
        except Exception:
            logger.exception("home snapshot sweep failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.teams_home_snapshot_seconds)
        except asyncio.TimeoutError:
            pass


def _require(value: str, name: str) -> str:
    if not value:
        raise SystemExit(f"{name} must be set to run the Teams consumer")
    return value


async def _check_access(job: QuestionJob) -> str | None:
    """Whether the person may ask, at the site in view: None to run, else the refusal sentence.

    (u-7 supervisor of BRI asks about BRI) -> None ; (about LON) -> "…outside your sites…" ;
    (no user row) -> "You have no access yet…"
    """
    now = history.utc_now()
    async with async_session() as db:
        record = await access_records.access_record_for(db, job.customer_code, job.sender_object_id)
        decision = access_records.decide(record, "ask", job.site_id)
        if decision.is_allowed:
            await access_users.touch_last_active(db, job.customer_code, job.sender_object_id, now=now)
            await db.commit()
            return None
        # a refusal that changes nothing is still audited: it is the trail the Admin reads
        await access_audit.record_event(
            db, job.customer_code, actor_object_id=job.sender_object_id, actor_name=job.sender_name,
            action="ask.refused", capability_code="ask", decision="refused", site_id=job.site_id,
            request_id=job.job_id, text=f"{job.sender_name or job.sender_object_id} was refused: "
                                        f"{decision.reason}", at=now)
        await db.commit()
    if record is None:
        return NO_ACCESS_YET
    if decision.outcome == "needs_approval":
        return ASK_NEEDS_APPROVAL
    return f"You cannot ask that here: {decision.reason}. Ask your admin if that is wrong."


NO_ACCESS_YET = ("You have no access to eSmart Eye yet. Ask your admin to add you; nothing was "
                 "looked up.")
ASK_NEEDS_APPROVAL = ("Asking the assistant needs approval for your role. A request has not been "
                      "sent automatically; ask your admin or Ops manager.")


async def _access_loop(stop: asyncio.Event) -> None:
    """Rewrite the access rows for the edge every minute, expire old approvals, purge old audit."""
    mirror = _mirror_writer()
    if mirror is None:
        logger.info("access mirror loop off: TEAMS_EDGE_DYNAMODB_TABLE is not set")
        return
    tracker = ChangeTracker(max_age_seconds=settings.teams_access_sweep_seconds * 20)
    while not stop.is_set():
        try:
            async with async_session() as db:
                await access_approvals.expire_once(db, now=history.utc_now())
                while await access_audit.purge_once(
                        db, now=history.utc_now(), retention_days=settings.teams_audit_retention_days):
                    pass
            written = await access_mirror.sweep_once(mirror, tracker=tracker)
            logger.debug("access rows written %s", written)
        except Exception:
            logger.exception("access sweep failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.teams_access_sweep_seconds)
        except asyncio.TimeoutError:
            pass


async def _exec_loop(stop: asyncio.Event) -> None:
    """The executive Home (step 7d), rewritten every few minutes per bound customer."""
    mirror = _mirror_writer()
    if mirror is None:
        logger.info("executive home loop off: TEAMS_EDGE_DYNAMODB_TABLE is not set")
        return
    tracker = ChangeTracker(max_age_seconds=settings.teams_exec_home_seconds * 6)
    while not stop.is_set():
        try:
            written = await exec_home.sweep_once(mirror, tracker=tracker, target=settings.teams_fill_rate_target,
                                                 max_sites=settings.teams_max_sites_per_customer)
            logger.debug("executive home written for %s", written)
        except Exception:
            logger.exception("executive home sweep failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.teams_exec_home_seconds)
        except asyncio.TimeoutError:
            pass


async def _advisories_loop(stop: asyncio.Event) -> None:
    """The agents of the Advisories tab (step 9): every tick runs whichever agent is due per bound
    customer, announces new HIGH advisories, and rewrites the ADVISORIES row."""
    mirror = _mirror_writer()
    if mirror is None:
        logger.info("advisories loop off: TEAMS_EDGE_DYNAMODB_TABLE is not set")
        return
    tracker = ChangeTracker(max_age_seconds=settings.teams_advisories_tick_seconds * 20)
    while not stop.is_set():
        try:
            ran = await advisories_runner.sweep_once(mirror, tracker=tracker, enqueue=notifications.enqueue)
            if any(ran.values()):
                logger.info("advisory agents ran %s", ran)
        except Exception:
            logger.exception("advisories tick failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.teams_advisories_tick_seconds)
        except asyncio.TimeoutError:
            pass


async def _run_task_query(task) -> dict:
    """A task's attached question answered as its owner (step 10): the same gateway as a Teams
    question, the owner's record deciding, no conversation memory.

    (schedule "short products list" by u-7) -> {"answer", "blocks", ...}
    """
    job = QuestionJob(job_id=f"task-{task.id}-{int(datetime.now(timezone.utc).timestamp())}",
                      tenant_id="", customer_code=task.customer_code,
                      conversation_id=f"task:{task.id}", sender_object_id=task.owner_object_id,
                      sender_name=task.owner_name, question=task.query_text or task.title, source="tab",
                      site_id=task.site_id)
    refusal = await _check_access(job)
    if refusal:
        return {"answer": refusal, "blocks": []}
    return await _run_agent(job, [])


async def _read_fill_rate(customer_code: str, site_id: str | None) -> float | None:
    """The site's fill rate now, for the condition schedules."""
    async with async_session() as db:
        site = None
        if site_id and site_id != "all":
            site = next((s for s in await list_customer_sites(db, customer_code) if s.site_id == site_id), None)
        snapshot = await home_snapshot.compute(db, customer_code, site)
    return await tasks_runner.fill_rate_of(snapshot)


async def _tasks_loop(stop: asyncio.Event) -> None:
    """The reminders and schedules (step 10): every tick fires what is due, checks the condition
    schedules, and rewrites the task rows of the people touched; once a minute every task row."""
    mirror = _mirror_writer()
    if mirror is None:
        logger.info("tasks loop off: TEAMS_EDGE_DYNAMODB_TABLE is not set")
        return
    tracker = ChangeTracker(max_age_seconds=settings.teams_tasks_tick_seconds * 20)
    last_sweep = 0.0
    while not stop.is_set():
        try:
            fired = await tasks_runner.tick(mirror, run_query=_run_task_query, enqueue=notifications.enqueue,
                                            read_fill_rate=_read_fill_rate, tracker=tracker,
                                            catch_up_hours=settings.teams_tasks_catch_up_hours)
            if any(fired.values()):
                logger.info("tasks fired %s", fired)
            if asyncio.get_running_loop().time() - last_sweep >= 60:
                await tasks_mirror.sweep_once(mirror, tracker=tracker)
                last_sweep = asyncio.get_running_loop().time()
        except Exception:
            logger.exception("tasks tick failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.teams_tasks_tick_seconds)
        except asyncio.TimeoutError:
            pass


async def _amain() -> None:
    setup_logging()
    queue_url = _require(settings.teams_sqs_queue_url, "TEAMS_SQS_QUEUE_URL")
    poster = HttpAnswerPoster(_require(settings.teams_edge_answers_url, "TEAMS_EDGE_ANSWERS_URL"),
                              _require(settings.teams_edge_shared_secret, "TEAMS_EDGE_SHARED_SECRET"))
    sqs = boto3.client("sqs", region_name=settings.teams_aws_region,
                       endpoint_url=settings.teams_aws_endpoint_url or None)
    consumer = TeamsQuestionConsumer(
        sqs=sqs, queue_url=queue_url, poster=poster, run_agent=_run_agent, load_history=_load_history,
        record_exchange=_record_exchange, resolve_customer=_resolve_customer,
        concurrency=settings.teams_consumer_concurrency,
        visibility_seconds=settings.teams_sqs_visibility_seconds, wait_seconds=settings.teams_sqs_wait_seconds,
        run_command=run_command, run_timeout_seconds=settings.analytics_agent_run_timeout_seconds,
        claim_run=_claim_run, finish_run=_finish_run, read_freshness=_read_freshness,
        progress_factory=_progress_factory, check_access=_check_access)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    try:
        await asyncio.gather(consumer.run_forever(stop), _sweep_loop(stop), _snapshot_loop(stop),
                             _history_loop(stop), _access_loop(stop), _exec_loop(stop),
                             _advisories_loop(stop), _tasks_loop(stop))
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
