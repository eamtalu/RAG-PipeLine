"""Chunk 124: the settlement reads as service functions, shared by the HTTP endpoints and the agent.

`grouped` is `GET /settlements/{name}/rows` and `listed` is `GET /settlements/{name}/list`, minus
FastAPI. They existed only inside the endpoints; the agent's tools needed the same work, and two
copies of lookup resolution would have drifted. Problems are raised as `ReadProblem` (a list of
sentences, the same ones the endpoint returns as a 400) so a caller that is a model can read them
and correct itself.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_settlement import AnalyticsSettlement
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics import contract
from app.services.analytics import lookup as lookup_model
from app.services.analytics import lookup_store
from app.services.analytics import settle as settle_model
from app.services.analytics import settle_query
from app.services.analytics import settle_store


class UnknownSettlement(LookupError):
    """No settlement of that name for this tenant."""


class ReadProblem(ValueError):
    """The question cannot be asked as spelled; `problems` says why, one sentence each."""

    def __init__(self, problems: list[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


async def declared(db: AsyncSession, customer: str, name: str) -> tuple[AnalyticsSettlement, settle_model.Settlement]:
    row = await db.scalar(select(AnalyticsSettlement).where(
        AnalyticsSettlement.customer_code == customer, AnalyticsSettlement.name == name))
    if row is None:
        raise UnknownSettlement(f"no settlement called {name!r} for this logspace")
    return row, settle_store.from_json(name, row.definition or {})


async def declared_all(db: AsyncSession, customer: str) -> list[tuple[AnalyticsSettlement, settle_model.Settlement]]:
    rows = (await db.execute(select(AnalyticsSettlement).where(
        AnalyticsSettlement.customer_code == customer).order_by(AnalyticsSettlement.name))).scalars().all()
    return [(r, settle_store.from_json(r.name, r.definition or {})) for r in rows]


def _filters(where: list[str]) -> tuple[settle_query.Filter, ...]:
    try:
        return tuple(settle_query.parse_filter(w) for w in where)
    except ValueError as exc:
        raise ReadProblem([str(exc)]) from None


def _stats(stat: list[str]) -> tuple[settle_query.Stat, ...]:
    try:
        return tuple(settle_query.parse_stat(x) for x in stat)
    except ValueError as exc:
        raise ReadProblem([str(exc)]) from None


async def grouped(db: AsyncSession, customer: str, name: str, *, group_by: list[str], start: datetime | None,
                  end: datetime | None, where: list[str] = (), stat: list[str] = (), limit: int = 500) -> dict:
    """Settled rows grouped and summed, with `lookup:` paths resolved exactly as they are for a
    metric: the rows are grouped by the lookup's KEY and re-labelled afterwards. Filters, stats
    and time buckets as `settle_query` spells them."""
    _row, settlement = await declared(db, customer, name)
    filters, stats = _filters(list(where)), _stats(list(stat))
    problems = settle_query.validate(settlement, filters=filters, stats=stats,
                                     group_by=tuple(g for g in group_by if not lookup_model.is_lookup_path(g)))
    if problems:
        raise ReadProblem(problems)
    lookups = await lookup_store.load(db, customer)
    try:
        translation = lookup_model.plan(tuple(group_by), lookups)
    except ValueError as exc:
        raise ReadProblem([str(exc)]) from None
    rows = await settle_store.read_grouped(db, customer, settlement, group_by=translation.stored_group_by,
                                           since=start, until=end, limit=limit, filters=filters, stats=stats,
                                           tz=ZoneInfo(await get_customer_timezone(db, customer)))
    numeric = [k for k in (rows[0].keys() if rows else []) if k not in ("dimensions",)]
    # `translate` re-keys `(instant, dims)` pairs and reads each lookup as at that instant, because
    # a metric's points are time buckets. A grouped read over settled rows has one instant: the end
    # of the window, or now, so a customer's name is the one it has as things stand.
    as_at = end or datetime.now(timezone.utc)
    points = {(as_at, tuple(g["dimensions"])): {k: g[k] for k in numeric} for g in rows}
    if translation.translates and points:
        resolver = await lookup_store.resolver(
            db, customer, translation.keys_needed(points),
            tuple(step for step in translation.steps if step is not None))
        points = lookup_model.translate(points, translation, resolver, merge=_merge)
    # Grouped by its own key a settlement has as many groups as rows - 6,252 on the live tenant -
    # and a silent cap would drop whole releases from the bottom of a drill-down. So the cap is
    # high, and hitting it is reported rather than hidden.
    return {"settlement": name, "group_by": list(group_by),
            "values": [v.name for v in settlement.values],
            "stats": [s.label for s in stats],
            "truncated": len(rows) >= limit,
            "rows": [{"dimensions": [d.replace(settle_store.KEY_SEP, " · ") if isinstance(d, str) else d
                                     for d in dims], **v} for (_at, dims), v in points.items()]}


def _merge(a: dict, b: dict) -> dict:
    """Two groups that resolve to one label: counts add as ints, settled values add as the strings
    they are stored as, and an absent side contributes nothing."""
    out = {}
    for k in set(a) | set(b):
        x, y = a.get(k), b.get(k)
        if x is None or y is None:
            out[k] = x if y is None else y
        elif isinstance(x, int) and isinstance(y, int):
            out[k] = x + y
        else:
            out[k] = format((Decimal(str(x)) + Decimal(str(y))).normalize(), "f")
    return out


async def listed(db: AsyncSession, customer: str, name: str, *, start: datetime | None, end: datetime | None,
                 search: str | None = None, where: list[str] = (), sort: str | None = None,
                 descending: bool = True, limit: int = 100, offset: int = 0) -> dict:
    """The settled rows themselves, newest first unless sorted, with what a lookup can reach from a
    carried key resolved for THESE rows only. A settled row carries the delivery number and the
    item number and nothing else about them, exactly as a fact does; the customer name and the
    item description live in the lookups, and a listing without them would show the keys and hide
    the names."""
    _row, settlement = await declared(db, customer, name)
    filters = _filters(list(where))
    problems = settle_query.validate(settlement, filters=filters, sort=sort)
    if problems:
        raise ReadProblem(problems)
    rows, total = await settle_store.list_rows(db, customer, settlement, since=start, until=end, search=search,
                                               limit=limit, offset=offset, filters=filters, sort=sort,
                                               descending=descending)
    carried = {c.split(":", 1)[1] if contract.is_attr_path(c) else c for c in settlement.carry}
    lookups = await lookup_store.load(db, customer)
    reachable = [(lk, a.name) for lk in lookups.values() if lk.key_field in carried for a in lk.attributes]
    looked_up_columns = [f"{lk.name}.{attr}" for lk, attr in reachable]
    if reachable and rows:
        needed: dict[str, set[str]] = {}
        for lk, _attr in reachable:
            for r in rows:
                key = r.get(lk.key_field) or (r.get("attributes") or {}).get(lk.key_field)
                if key:
                    needed.setdefault(lk.name, set()).add(str(key))
        resolver = await lookup_store.resolver(db, customer, needed,
                                               tuple((lk.name, attr) for lk, attr in reachable))
        now = datetime.now(timezone.utc)
        for r in rows:
            at = datetime.fromisoformat(r["event_time"]) if r.get("event_time") else now
            r["looked_up"] = {}
            for lk, attr in reachable:
                key = r.get(lk.key_field) or (r.get("attributes") or {}).get(lk.key_field)
                r["looked_up"][f"{lk.name}.{attr}"] = (
                    resolver.value(lk.name, str(key), attr, at) if key else None)
    return {"settlement": name, "key": list(settlement.key), "carry": list(settlement.carry),
            "values": [v.name for v in settlement.values], "looked_up": looked_up_columns,
            "rows": rows, "total": total, "limit": limit, "offset": offset}


async def explained(db: AsyncSession, customer: str, name: str, key: list[str]) -> dict:
    """One release: every call that carries the key, oldest first, with the fields the rules read,
    and the row they settle to, computed live from the calls. The same shape as the preview
    endpoint, so a screen and the agent show the same thing."""
    _row, settlement = await declared(db, customer, name)
    if len(key) != len(settlement.key):
        raise ReadProblem([f"{name!r} is keyed by {list(settlement.key)}; give {len(settlement.key)} value(s)"])
    calls, settled = await settle_store.read_key(db, customer, settlement, tuple(key))
    shown = []
    for c in calls:
        entry = {"event_time": c["event_time"].isoformat() if c.get("event_time") else None,
                 "status": c.get("status"), "classification": c.get("quantity_classification")}
        for v in settlement.values:
            if v.field and contract.is_attr_path(v.field):
                entry[contract.attr_key(v.field)] = (c.get("attributes") or {}).get(contract.attr_key(v.field))
        shown.append(entry)
    return {"settlement": name, "key": list(key), "calls": shown,
            "settled": None if settled is None else {
                "event_time": settled.event_time.isoformat() if settled.event_time else None,
                "carried": {k: settle_store._stringify(v) for k, v in settled.carried.items()},
                "values": {k: settle_store._stringify(v) for k, v in settled.values.items()},
                "calls": settled.calls}}
