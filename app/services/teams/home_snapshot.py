"""Chunk 127: "today so far" for the Teams tab, computed here and mirrored to the edge.

The Home tab of the Teams app is a page served by the edge on AWS, and the edge cannot reach this
server. So the consumer computes one small snapshot per bound customer every minute, from the same
settlement reads the analytics agent uses (`aggregate_releases`, `list_releases`, on the tenant's
clock), and writes it to the edge's DynamoDB table as pk = HOME#<customer_code>, sk = SNAPSHOT.
The edge only reads. A snapshot older than its TTL disappears, so a stopped consumer shows the tab
"offline" rather than stale numbers for ever.

Shape (the edge's `HomeSnapshot` mirrors it): customer_code, site, as_of, kpis[4], attention[],
customers[], activity[], rail{}. Every value is pre-formatted text; the page never formats numbers.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.persistence.repositories.teams_repository import TeamsBindingRepository
from app.services.analytics.settle_reads import ReadProblem, UnknownSettlement
from app.services.analytics_agent.tools import aggregate_releases, list_releases

logger = logging.getLogger(__name__)

SETTLEMENT = "pick_release"
SPARK_POINTS = 9
ITEM_GROUP = ["item_number", "lookup:item description.ItemDescription"]
CUSTOMER_GROUP = ["lookup:delivery.customer_name"]
CHRONIC_MIN_RELEASES = 3


# ============================================================== compute
async def compute(db: AsyncSession, customer_code: str) -> dict | None:
    """The snapshot for one customer, or None when the customer has no pick_release settlement."""
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    now = datetime.now(timezone.utc)
    local_now = now.astimezone(tz)
    try:
        today = await aggregate_releases(db, {"day": "today", "group_by": ["hour_start"]}, customer_code)
        yesterday = await aggregate_releases(db, {"day": "yesterday", "group_by": ["hour_start"]}, customer_code)
        totals = await aggregate_releases(db, {"day": "today"}, customer_code)
        zero = await aggregate_releases(db, {"day": "today", "where": ["picked==0"]}, customer_code)
        short = await aggregate_releases(db, {"day": "today", "where": ["shortfall<0"]}, customer_code)
        zero_items = await _optional(aggregate_releases, db, {"day": "today", "where": ["picked==0"],
                                                              "group_by": ITEM_GROUP, "sort": "rows", "limit": 3},
                                     customer_code)
        chronic = await _optional(aggregate_releases, db, {"start": (now - timedelta(days=7)).isoformat(),
                                                           "end": now.isoformat(), "where": ["shortfall<0"],
                                                           "group_by": ITEM_GROUP, "sort": "rows", "limit": 5},
                                  customer_code)
        customers = await _optional(aggregate_releases, db, {"day": "today", "group_by": CUSTOMER_GROUP,
                                                             "sort": "rows", "limit": 5}, customer_code)
        problems = await list_releases(db, {"day": "today", "where": ["shortfall<0"], "limit": 6}, customer_code)
    except UnknownSettlement:
        return None
    except ReadProblem as exc:
        logger.warning("home snapshot for %s could not be read: %s", customer_code, exc.problems)
        return None

    by_hour_today = _by_hour(today["rows"])
    by_hour_yesterday = _by_hour(yesterday["rows"])
    releases = int(today.get("total_rows") or 0)
    baseline = _at_this_time(by_hour_yesterday, local_now)
    total_row = (totals.get("rows") or [{}])[0]
    deliveries = int(total_row.get("deliveries") or 0)
    expected = _dec(total_row.get("expected"))
    units_short = -_dec(((short.get("rows") or [{}])[0]).get("shortfall"))
    short_lines = int(short.get("total_rows") or 0)
    zero_picks = int(zero.get("total_rows") or 0)
    partial = max(0, short_lines - zero_picks)
    fill = None if expected <= 0 else max(Decimal(0), (expected - units_short) / expected)

    kpis = [
        {"id": "releases", "label": "Releases today", "value": _int(releases),
         "caption": _delta_caption(releases, baseline), "delta_direction": _direction(releases, baseline),
         "spark": _spark(by_hour_today, local_now)},
        {"id": "deliveries", "label": "Deliveries today", "value": _int(deliveries),
         "caption": f"{_int(releases)} lines", "delta_direction": None, "spark": []},
        {"id": "fill_rate", "label": "Fill rate", "value": _pct(fill),
         "caption": f"{_int(short_lines)} short lines", "delta_direction": None, "spark": []},
        {"id": "attention", "label": "Needs attention", "value": _int(zero_picks),
         "caption": f"{_int(zero_picks)} zero-pick · {_int(partial)} partial", "delta_direction": None, "spark": []},
    ]
    attention = _attention(zero_items, chronic)
    customer_rows = _customers(customers)
    return {
        "customer_code": customer_code,
        "site": await _site(db, customer_code),
        "as_of": now.isoformat(),
        "kpis": kpis,
        "attention": attention,
        "customers": customer_rows,
        "activity": _activity(problems["rows"], tz),
        "rail": {"releases": _int(releases), "deliveries": _int(deliveries), "fill_rate": _pct(fill),
                 "alerts": _int(zero_picks), "customers": _int(len(customer_rows))},
    }


async def _optional(read, db, args, customer_code) -> dict:
    """A read grouped by a lookup the tenant may not have declared. Without the lookup the same read
    is made on the plain field (item number without its description); a customer grouping has no
    plain field, so that panel is simply empty."""
    try:
        return await read(db, args, customer_code)
    except ReadProblem as exc:
        plain = [g for g in args.get("group_by") or [] if not g.startswith("lookup:")]
        if plain and plain != list(args.get("group_by") or []):
            try:
                return await read(db, {**args, "group_by": plain}, customer_code)
            except ReadProblem as again:
                exc = again
        logger.info("home snapshot: skipped %s: %s", args.get("group_by"), exc.problems)
        return {"rows": [], "total_rows": 0}


async def _site(db: AsyncSession, customer_code: str) -> str:
    row = await db.scalar(select(Customer).where(Customer.customer_code == customer_code))
    return (row.display_name or row.name or customer_code) if row is not None else customer_code


# ============================================================== the numbers
def _dec(value) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _hour_of(dimension) -> int | None:
    if isinstance(dimension, int):
        return dimension
    try:
        return datetime.fromisoformat(str(dimension)).hour
    except ValueError:
        return None


def _by_hour(rows: list[dict]) -> dict[int, int]:
    out: dict[int, int] = {}
    for r in rows:
        h = _hour_of((r.get("dimensions") or [None])[0])
        if h is not None:
            out[h] = out.get(h, 0) + int(r.get("rows") or 0)
    return out


def _at_this_time(by_hour: dict[int, int], local_now: datetime) -> Decimal:
    """Yesterday's releases up to this time of day: whole hours before now plus the fraction of the
    current hour that has passed, the same estimate the pick releases page makes."""
    frac = Decimal(local_now.minute * 60 + local_now.second) / Decimal(3600)
    total = Decimal(0)
    for h, n in by_hour.items():
        if h < local_now.hour:
            total += n
        elif h == local_now.hour:
            total += Decimal(n) * frac
    return total


def _spark(by_hour: dict[int, int], local_now: datetime) -> list[int]:
    hours = [h for h in range(local_now.hour - SPARK_POINTS + 1, local_now.hour + 1)]
    return [by_hour.get(h, 0) if h >= 0 else 0 for h in hours]


def _direction(value: int, baseline: Decimal) -> str | None:
    if baseline <= 0 or value == baseline:
        return None
    return "up" if value > baseline else "down"


def _delta_caption(value: int, baseline: Decimal) -> str:
    if baseline <= 0:
        return "no comparison yet"
    change = (Decimal(value) - baseline) / baseline * 100
    return f"{abs(change):.0f}% vs yesterday"  # the arrow carries the direction; "at this time" is in the docs


def _int(n) -> str:
    return f"{int(n):,}"


def _pct(fraction: Decimal | None) -> str:
    return "–" if fraction is None else f"{fraction * 100:.1f}%"


def _units(value: Decimal) -> str:
    """Up to three decimals, trailing zeros dropped: a 3.999 pick is not a 4."""
    text = f"{value:.3f}".rstrip("0").rstrip(".")
    return text or "0"


# ============================================================== the panels
def _attention(zero_items: dict, chronic: dict) -> list[dict]:
    out: list[dict] = []
    for r in zero_items.get("rows") or []:
        dims = r.get("dimensions") or []
        item, desc = str(dims[0]) if dims else "?", str(dims[1]) if len(dims) > 1 and dims[1] else ""
        label = f"{desc} ({item})" if desc else item
        n = int(r.get("rows") or 0)
        units = -_dec(r.get("shortfall"))
        out.append({"id": f"zero:{item}", "kind": "stock",
                    "title": f"{label} zero-picked {n} time{'s' if n != 1 else ''} today",
                    "detail": f"{_units(units)} units short",
                    "ask_text": f"Why was item {item} zero-picked today?"})
    for r in chronic.get("rows") or []:
        n = int(r.get("rows") or 0)
        if n < CHRONIC_MIN_RELEASES:
            continue
        dims = r.get("dimensions") or []
        item, desc = str(dims[0]) if dims else "?", str(dims[1]) if len(dims) > 1 and dims[1] else ""
        label = f"{desc} ({item})" if desc else item
        out.append({"id": f"chronic:{item}", "kind": "other",
                    "title": f"{label} short on {n} releases this week",
                    "detail": f"{_units(-_dec(r.get('shortfall')))} units short over 7 days",
                    "ask_text": f"Which customers were short of item {item} this week?"})
    return out[:4]


def _customers(customers: dict) -> list[dict]:
    rows = customers.get("rows") or []
    top = max((int(r.get("rows") or 0) for r in rows), default=0)
    out = []
    for r in rows:
        dims = r.get("dimensions") or []
        lines = int(r.get("rows") or 0)
        out.append({"name": str(dims[0]) if dims and dims[0] else "(no customer)", "lines": _int(lines),
                    "short_lines": _int(int(_dec(r.get("is_short")))),
                    "share": float(Decimal(lines) / Decimal(top)) if top else 0.0})
    return out


def _activity(rows: list[dict], tz: ZoneInfo) -> list[dict]:
    out = []
    for r in rows:
        attrs = r.get("attributes") or {}
        looked = r.get("looked_up") or {}
        picked, expected = _dec(attrs.get("picked")), _dec(attrs.get("expected"))
        item = looked.get("item description.ItemDescription") or r.get("item_number") or "?"
        customer = looked.get("delivery.customer_name")
        who = r.get("user_name") or "someone"
        text = f"{who} picked {_units(picked)} of {_units(expected)} {item}"
        if customer:
            text += f" for {customer}"
        at = r.get("event_time")
        out.append({"at": at, "text": text, "alert": picked <= 0})
    return out


# ============================================================== writing to the edge
class HomeSnapshotWriter(Protocol):
    async def put(self, customer_code: str, snapshot: dict, *, now: float | None = None) -> None: ...


class DynamoHomeSnapshotWriter:
    """Same table and credentials as the binding mirror; the snapshot is one JSON string so DynamoDB's
    no-floats rule never bites."""

    def __init__(self, table_name: str, *, region_name: str, endpoint_url: str | None = None,
                 ttl_seconds: int = 3600):
        import boto3
        self._table = boto3.resource("dynamodb", region_name=region_name, endpoint_url=endpoint_url).Table(table_name)
        self._ttl_seconds = ttl_seconds

    async def put(self, customer_code: str, snapshot: dict, *, now: float | None = None) -> None:
        stamp = int(now if now is not None else time.time())
        item = {"pk": f"HOME#{customer_code}", "sk": "SNAPSHOT", "as_of": snapshot.get("as_of", ""),
                "snapshot_json": json.dumps(snapshot, default=str), "written_at": stamp,
                "ttl": stamp + self._ttl_seconds}
        await asyncio.to_thread(lambda: self._table.put_item(Item=item))


async def sweep_once(writer: HomeSnapshotWriter, *, only: list[str] | None = None) -> list[str]:
    """Compute and write one snapshot per distinct bound, enabled customer. One failure does not
    stop the rest. Returns the customer codes written."""
    async with async_session() as db:
        if only is None:
            codes = sorted({b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        else:
            codes = list(only)
        written: list[str] = []
        for code in codes:
            try:
                snapshot = await compute(db, code)
                if snapshot is None:
                    continue
                await writer.put(code, snapshot)
                written.append(code)
            except Exception:
                logger.exception("home snapshot for %s failed", code)
    return written


def build_writer_from_settings() -> HomeSnapshotWriter | None:
    from app.settings import settings
    if not settings.teams_edge_dynamodb_table:
        return None
    return DynamoHomeSnapshotWriter(settings.teams_edge_dynamodb_table, region_name=settings.teams_aws_region,
                                    endpoint_url=settings.teams_aws_endpoint_url or None,
                                    ttl_seconds=int(settings.teams_home_snapshot_seconds * 60))
