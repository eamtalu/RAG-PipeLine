"""Filling the place (company and warehouse) on at-risk deliveries closed before they had one.

A delivery on the board takes its place from its settled route rows. Deliveries closed before
November 2026 have no warehouse, and none had a company before chunk 229. Their settled route
rows are still there, so both are read from the newest one. The checks of a delivery then take
the delivery's place. Two small statements, one transaction: the tables hold a few thousand rows.
Used by `scripts/backfill_at_risk_places.py`; nothing in the serving path imports this.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text

from app.config.database import SessionFactory
from app.settings import settings

#: The newest settled route row of each delivery that names a warehouse: (delivery, company,
#: warehouse). The delivery number is the first part of the route settlement's key.
_ROUTE_ROWS = """
    SELECT DISTINCT ON (key_parts->>0) key_parts->>0 AS delivery_number, company, warehouse
      FROM analytics_settled_rows
     WHERE customer_code = :cc AND settlement = :settlement AND warehouse IS NOT NULL
     ORDER BY key_parts->>0, event_time DESC NULLS LAST
"""

#: Which deliveries a route row can still tell something: no warehouse yet, or the same warehouse
#: and no company yet. A delivery at another warehouse is left alone: that row is not its place.
_FILLABLE = """
    d.customer_code = :cc AND d.delivery_number = s.delivery_number
    AND (d.warehouse IS NULL OR (d.warehouse = s.warehouse AND d.company IS NULL
                                 AND s.company IS NOT NULL))
"""

_FILL_DELIVERIES = f"""
    WITH s AS ({_ROUTE_ROWS}),
    done AS (UPDATE analytics_at_risk_deliveries d
                SET warehouse = s.warehouse, company = COALESCE(d.company, s.company)
               FROM s WHERE {_FILLABLE} RETURNING 1)
    SELECT count(*) FROM done
"""

_COUNT_DELIVERIES = f"""
    WITH s AS ({_ROUTE_ROWS})
    SELECT count(*) FROM analytics_at_risk_deliveries d JOIN s ON {_FILLABLE}
"""

_CHECKS_WHERE = """
    c.customer_code = :cc AND c.delivery_id = d.id
    AND ((c.company IS NULL AND d.company IS NOT NULL)
         OR (c.warehouse IS NULL AND d.warehouse IS NOT NULL))
"""

_FILL_CHECKS = f"""
    WITH done AS (UPDATE analytics_at_risk_checks c
                     SET company = COALESCE(c.company, d.company),
                         warehouse = COALESCE(c.warehouse, d.warehouse)
                    FROM analytics_at_risk_deliveries d WHERE {_CHECKS_WHERE} RETURNING 1)
    SELECT count(*) FROM done
"""

_COUNT_CHECKS = f"""
    SELECT count(*) FROM analytics_at_risk_checks c
      JOIN analytics_at_risk_deliveries d ON {_CHECKS_WHERE}
"""


@dataclass(frozen=True, slots=True)
class Filled:
    """How many rows a run filled, or would fill on a dry run."""

    deliveries: int
    checks: int


async def fill(session_factory: SessionFactory, customer_code: str, *,
               settlement: str | None = None, dry_run: bool = False) -> Filled:
    """Give a customer's older at-risk deliveries, and their checks, the place of their route rows.

    (2,982 deliveries without a warehouse, all with a settled route row) -> Filled(2982, 0)
    ; (dry_run) -> the same counts, nothing written
    """
    params = {"cc": customer_code,
              "settlement": settlement or settings.analytics_at_risk_settlement}
    async with session_factory() as db:
        if dry_run:
            # A dry run cannot count the checks a filled delivery would bring along, because
            # nothing is filled; it reports the checks that could follow as things stand.
            deliveries = await db.scalar(text(_COUNT_DELIVERIES), params)
            checks = await db.scalar(text(_COUNT_CHECKS), params)
            return Filled(int(deliveries or 0), int(checks or 0))
        # Step 1: Deliveries first, because a check takes its place from its delivery.
        deliveries = await db.scalar(text(_FILL_DELIVERIES), params)
        # Step 2: The checks, in the same transaction, so a check never names a place its
        # delivery does not have.
        checks = await db.scalar(text(_FILL_CHECKS), {"cc": customer_code})
        await db.commit()
    return Filled(int(deliveries or 0), int(checks or 0))
