"""Step 6 of docs/esmart-agent-execution-plan.md: the consumer trusts the binding, not the job.

The edge resolves `customer_code` from its DynamoDB mirror of `teams_tenant_bindings`. The consumer
now resolves it again from Postgres by the job's `tenant_id` and refuses a job whose customer does
not match: a stale mirror or a forged job can never read another tenant's log space. Contract v2
also makes `sender_object_id` required, because the per-user limits and roles to come key on it.
"""

import logging
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import delete

from app import teams_consumer
from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.repositories.teams_repository import TeamsBindingRepository
from app.services.teams import consumer as c
from app.services.teams.contracts import QuestionJob
from tests.test_teams_consumer_chunk123 import Harness, job

CC = "test_chunk157"
OTHER = "test_chunk157_other"
TENANT = "tenant-chunk157"


# ==================================================== 1. the consumer refuses a mismatch

async def test_a_job_whose_tenant_is_bound_to_another_customer_is_refused_and_logged(caplog):
    async def resolve(tenant_id):
        return "someone-else"

    h = Harness([job(customer_code="acme", tenant_id="t-1")], resolve=resolve)
    with caplog.at_level(logging.WARNING, logger="app.services.teams.consumer"):
        await h.drain()
    [p] = h.poster.posted
    assert p.status == "error" and p.answer == c.CUSTOMER_MISMATCH
    assert h.agent_calls == [] and h.sqs.deleted == ["r0"]
    assert any("t-1" in r.message and "someone-else" in r.message and "acme" in r.message
               for r in caplog.records), "the refusal names the tenant, its customer and the job's"


async def test_an_unbound_tenant_is_not_ready():
    h = Harness([job()], ready=False)
    await h.drain()
    [p] = h.poster.posted
    assert p.status == "error" and p.answer == c.CUSTOMER_NOT_READY and h.agent_calls == []


async def test_a_matching_binding_runs_the_agent():
    h = Harness([job(customer_code="acme", tenant_id="t-1")])
    await h.drain()
    assert len(h.agent_calls) == 1 and h.poster.posted[0].status == "ok"


# ==================================================== 2. contract v2: the sender is required

def test_a_job_without_a_sender_object_id_is_malformed():
    with pytest.raises(ValidationError):
        QuestionJob(job_id="j", tenant_id="t", customer_code="acme", conversation_id="c",
                    question="q?")
    assert QuestionJob.model_fields["schema_version"].default == 2


async def test_a_message_without_a_sender_is_dropped_not_answered():
    body = job().replace('"sender_object_id":"u-1"', '"sender_object_id":null')
    assert '"sender_object_id":null' in body
    h = Harness([body])
    await h.drain()
    assert h.poster.posted == [] and h.agent_calls == [] and h.sqs.deleted == ["r0"]


# ==================================================== 3. the process resolves through Postgres

async def _wipe():
    async with async_session() as db:
        await db.execute(delete(TeamsTenantBinding).where(TeamsTenantBinding.tenant_id == TENANT))
        await db.execute(delete(Customer).where(Customer.customer_code.in_((CC, OTHER))))
        await db.commit()


async def test_the_consumer_process_resolves_the_customer_from_the_binding():
    await _wipe()
    try:
        assert await teams_consumer._resolve_customer(TENANT) is None, "unbound tenant"
        async with async_session() as db:
            db.add(Customer(customer_code=CC, name="c157", timezone="Europe/London", active=True))
            await db.commit()
            await TeamsBindingRepository(db).upsert(TENANT, CC, enabled=True)
        assert await teams_consumer._resolve_customer(TENANT) == CC
        async with async_session() as db:
            await TeamsBindingRepository(db).upsert(TENANT, CC, enabled=False)
        assert await teams_consumer._resolve_customer(TENANT) is None, "disabled binding"
        async with async_session() as db:
            await TeamsBindingRepository(db).upsert(TENANT, CC, enabled=True)
            cust = await db.get(Customer, (await db.execute(
                Customer.__table__.select().where(Customer.customer_code == CC))).one().id)
            cust.active = False
            await db.commit()
        assert await teams_consumer._resolve_customer(TENANT) is None, "inactive customer"
    finally:
        await _wipe()
