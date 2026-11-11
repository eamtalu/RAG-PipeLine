"""The datasets the golden questions run on: the rows chunks 124 and 132 have always planted.

`tests/test_golden_questions_chunk165.py` plants them per case; `scripts/golden_questions.py`
plants them with `--plant` so a real-model run has data on an empty database; chunk 124 keeps
its old names as thin aliases over these functions, so every test that imports them is unchanged.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.agent_in_flight import AgentInFlight
from app.persistence.models.agent_trace import AgentTrace
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.quota_window import QuotaWindow
from app.persistence.models.usage_daily import UsageDaily
from tests.test_logspace_tools_chunk132 import TODAY as LOGSPACE_TODAY
from tests.test_logspace_tools_chunk132 import TZ as LOGSPACE_TZ
from tests.test_logspace_tools_chunk132 import _seed

__all__ = [
    "BODY",
    "LOGSPACE_TODAY",
    "LOGSPACE_TZ",
    "T0",
    "fact",
    "plant_analytics",
    "plant_analytics_facts",
    "plant_logspace",
    "register_analytics_tenant",
    "window",
    "wipe_analytics",
    "wipe_logspace",
]

#: the morning the analytics facts happen on (UTC)
T0 = datetime(2026, 9, 18, 5, 44, 42, tzinfo=timezone.utc)

#: the pick_release settlement the analytics tools read
BODY = {
    "name": "pick_release",
    "description": "one row per release",
    "reads": ["ConfirmPickLine"],
    "key": ["attr:ReportingNumber"],
    "carry": ["delivery_number", "item_number", "attr:OrderLine", "user_name", "transaction_name"],
    "values": [
        {"name": "expected", "rule": "first", "field": "attr:ExpectedQuantity"},
        {"name": "picked", "rule": "sum", "field": "attr:QuantityPicked", "statuses": ["success"]},
        {"name": "calls", "rule": "count"},
        {"name": "refused", "rule": "count", "statuses": ["error"]},
        {"name": "shortfall", "rule": "difference", "left": "picked", "right": "expected"},
        {"name": "is_short", "rule": "flag", "left": "shortfall", "op": "<", "right_value": "0"},
    ],
}

# the agent's own rows for the tenant too (quotas, in-flight, traces, usage), so a tenant's
# hourly quota cannot carry over between test runs and refuse a later endpoint test
_ANALYTICS_MODELS = (
    QuotaWindow,
    AgentInFlight,
    AgentTrace,
    UsageDaily,
    AnalyticsSettledRow,
    AnalyticsSettlement,
    AnalyticsLookupValue,
    AnalyticsLookup,
    AnalyticsFieldRegistry,
    AnalyticsFact,
)


def window() -> dict:
    """The tool window around the facts, as the scripted calls name it.

    >>> window()
    {'start': '2026-09-18T04:44:42+00:00', 'end': '2026-09-18T07:44:42+00:00'}
    """
    return {
        "start": (T0 - timedelta(hours=1)).isoformat(),
        "end": (T0 + timedelta(hours=2)).isoformat(),
    }


def fact(
    cc: str,
    minutes: int,
    expected: int,
    picked: int,
    *,
    rep: str = "540551",
    status: str = "success",
    delivery: str = "27907",
    item: str = "104568",
    user: str = "FNACHONLEO",
) -> AnalyticsFact:
    """One ConfirmPickLine fact `minutes` after T0, for the given tenant."""
    when = T0 + timedelta(minutes=minutes)
    return AnalyticsFact(
        id=uuid.uuid4(),
        customer_code=cc,
        source_transaction_id=uuid.uuid4(),
        source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8],
        revision=1,
        event_time=when,
        business_date=when.date(),
        transaction_name="JIT and Shorts Pick (Brighton)",
        method="ConfirmPickLine",
        status=status,
        quantity_classification="pick" if picked > 0 else "attempt",
        warehouse="BRI",
        delivery_number=delivery,
        item_number=item,
        lot_number="2609161191",
        user_name=user,
        attributes={
            "ReportingNumber": rep,
            "ExpectedQuantity": str(expected),
            "QuantityPicked": str(picked),
            "OrderLine": "21",
        },
        created_at=when,
    )


async def wipe_analytics(cc: str) -> None:
    """Remove every analytics and agent row and the customer row of the tenant."""
    async with async_session() as db:
        for model in _ANALYTICS_MODELS:
            await db.execute(delete(model).where(model.customer_code == cc))
        await db.execute(delete(Customer).where(Customer.customer_code == cc))
        await db.commit()


async def register_analytics_tenant(cc: str) -> None:
    """The customer row and the four captured fields the settlement reads."""
    async with async_session() as db:
        db.add(Customer(customer_code=cc, name="agent probe", timezone="Europe/London"))
        for f in ("ReportingNumber", "ExpectedQuantity", "QuantityPicked", "OrderLine"):
            db.add(
                AnalyticsFieldRegistry(
                    customer_code=cc,
                    method="ConfirmPickLine",
                    source="request",
                    field=f,
                    captured=True,
                    seen_count=9,
                )
            )
        await db.commit()


async def plant_analytics_facts(cc: str) -> None:
    """Release 540551 (nine calls, one accepted), a clean release, and two short ones on another
    item; then the pick_release settlement over them."""
    facts = [fact(cc, 0, 10, 9)] + [fact(cc, 7 + i, 1, 1, status="error") for i in range(8)]
    facts += [
        fact(cc, 30, 4, 4, rep="A1", item="100606", user="DBOBOC"),
        fact(cc, 40, 7, 4, rep="B1", delivery="25810", item="100230", user="DBOBOC"),
        fact(cc, 50, 7, 0, rep="B2", delivery="25810", item="100230", user="DBOBOC"),
    ]
    async with async_session() as db:
        for f in facts:
            db.add(f)
        await db.commit()
        await api.create_settlement(body=BODY, backfill=True, customer=cc, db=db)


async def plant_analytics(cc: str) -> None:
    """A clean tenant with the facts and the settlement: wipe, register, plant."""
    await wipe_analytics(cc)
    await register_analytics_tenant(cc)
    await plant_analytics_facts(cc)


async def plant_logspace(cc: str) -> None:
    """The logspace records of chunk 132: delivery D1 today, an errored call carrying credentials,
    and yesterday's D2."""
    await wipe_logspace(cc)
    async with async_session() as db:
        await _seed(db, cc)
        await db.commit()


async def wipe_logspace(cc: str) -> None:
    async with async_session() as db:
        await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == cc))
        await db.execute(delete(Job).where(Job.customer_code == cc))
        await db.commit()
