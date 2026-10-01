"""Shared planting helpers for the demand-forecast tests (chunks 138-141).

They write a `pick_release` settlement and settled rows straight into the tables, bypassing the
fold: these tests are about what the forecast does WITH settled rows, not how rows are settled
(chunk 117 pins that). Every helper takes the tenant code so each chunk keeps its own.
"""

import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.analytics_ml import AnalyticsFeatureSet, AnalyticsPrediction
from app.persistence.models.analytics_forecast import (AnalyticsForecastAccuracy, AnalyticsForecastRun,
                                                       AnalyticsForecastSeries)
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.customer import Customer
from app.services.analytics import settle as st
from app.services.analytics import settle_store

LONDON = ZoneInfo("Europe/London")
SETTLEMENT = "pick_release"

PICK_RELEASE = st.Settlement(
    name=SETTLEMENT, reads=("ConfirmPickLine",), key=("attr:ReportingNumber",),
    carry=("delivery_number", "item_number", "warehouse", "lot_number", "user_name", "transaction_name"),
    values=(
        st.Settled(name="expected", rule=st.Rule.first, field="attr:ExpectedQuantity"),
        st.Settled(name="picked", rule=st.Rule.sum, field="attr:QuantityPicked", statuses=frozenset({"success"})),
        st.Settled(name="calls", rule=st.Rule.count),
        st.Settled(name="duration_s", rule=st.Rule.difference, left="finished_at", right="started_at"),
    ))

MODELS = (AnalyticsForecastAccuracy, AnalyticsForecastSeries, AnalyticsForecastRun, AnalyticsPrediction,
          AnalyticsFeatureSet, AnalyticsSettledRow, AnalyticsSettlement)


async def wipe(cc: str) -> None:
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == cc))
        await db.execute(delete(Customer).where(Customer.customer_code == cc))
        await db.commit()


async def seed_tenant(cc: str, *, timezone_name: str = "Europe/London") -> None:
    async with async_session() as db:
        db.add(Customer(customer_code=cc, name=f"{cc} forecast probe", timezone=timezone_name))
        db.add(AnalyticsSettlement(customer_code=cc, name=SETTLEMENT, description="probe",
                                   definition=settle_store.to_json(PICK_RELEASE), enabled=True))
        await db.commit()


def settled(cc: str, when: datetime, *, item="104568", tx="Brighton Stock Pick", user="BCHAM",
            picked="3", warehouse="BRI", duration_s="120") -> AnalyticsSettledRow:
    """One pick line settled at `when` (an aware instant)."""
    local = when.astimezone(LONDON)
    key = uuid.uuid4().hex[:10]
    return AnalyticsSettledRow(
        id=uuid.uuid4(), customer_code=cc, settlement=SETTLEMENT, key=key, key_parts=[key],
        event_time=when, business_date=local.date(), method="ConfirmPickLine", transaction_name=tx,
        warehouse=warehouse, item_number=item, delivery_number="27907", lot_number=None, user_name=user,
        attributes={"expected": picked, "picked": picked, "calls": "1", "duration_s": duration_s,
                    "item_number": item, "user_name": user, "warehouse": warehouse, "transaction_name": tx},
        calls=1, settled_at=when)


async def plant(rows) -> None:
    async with async_session() as db:
        for r in rows:
            db.add(r)
        await db.commit()


def shift_rows(cc: str, day_local: datetime, *, lines: int, pickers: int, items=("104568", "100944"),
               tx="Brighton Stock Pick") -> list[AnalyticsSettledRow]:
    """`lines` pick lines spread over a 14:00 -> 08:00 shift starting on `day_local` (a naive local
    date at midnight), by `pickers` people round-robin, items alternating."""
    users = [f"P{n:02d}" for n in range(pickers)]
    out = []
    for i in range(lines):
        hour = 14 + (i * 18) // max(lines, 1)  # 14..31 -> wraps past midnight
        when_local = day_local.replace(tzinfo=LONDON) + timedelta(hours=hour, minutes=(i * 7) % 60)
        out.append(settled(cc, when_local.astimezone(timezone.utc), item=items[i % len(items)],
                           user=users[i % pickers], tx=tx))
    return out


async def lines_between(cc: str, start, end) -> int:
    """How many settled lines fell on tenant-local days `start`..`end` inclusive. The tests compare
    the forecast's actuals to THIS rather than to a hand count, because a 14:00 -> 08:00 shift puts
    its small hours on the next calendar day."""
    from sqlalchemy import func, select
    async with async_session() as db:
        return int(await db.scalar(select(func.count()).select_from(AnalyticsSettledRow).where(
            AnalyticsSettledRow.customer_code == cc, AnalyticsSettledRow.business_date >= start,
            AnalyticsSettledRow.business_date <= end)) or 0)
