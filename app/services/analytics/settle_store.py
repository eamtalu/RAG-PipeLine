"""Chunk 117: the settlements a tenant has declared, and keeping their settled rows current.

`settle.py` is the pure half and knows nothing about tables. This half does the reading and writing:
loading declarations, recomputing the rows for the keys a fold has just touched, and answering a
grouped read over the settled rows.

**Recompute-and-replace, per key.** A release is not final until calls stop arriving for it, and the
fold has no way to know which call is the last. So after every fold window, every key that appeared
in the window's facts is recomputed from ALL of its calls, not just the window's, and its row is
upserted. Idempotent: fold the same window twice and the rows are byte-identical. This is the same
philosophy as the roll-ups and it is why a settled row can never drift from its calls.

**No roll-up on top.** Settled rows are read directly and grouped on request. One row per release is
already the aggregation the person asked for; on tmp-live that is six thousand rows, and a grouped
read over them is a single indexed query.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import Numeric, and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.services.analytics import contract
from app.services.analytics import settle as st

#: Joins a composite key into one string. A unit separator, which no warehouse identifier contains.
KEY_SEP = "\x1f"

#: Typed columns a settled row shares with a fact row. A carried field with one of these names lands
#: in its column as well as in the attribute bag, so the existing group-by reads it.
TYPED = ("method", "transaction_name", "warehouse", "item_number", "delivery_number", "lot_number",
         "user_name")


# ============================================================== declaration <-> row

def to_json(settlement: st.Settlement) -> dict:
    """A settlement as the JSON the column holds. Sorted where order carries no meaning."""
    return {
        "reads": list(settlement.reads),
        "key": list(settlement.key),
        "carry": list(settlement.carry),
        "values": [{
            "name": v.name, "rule": v.rule.value, "field": v.field,
            "statuses": sorted(v.statuses), "only": sorted(v.only),
            "left": v.left, "right": v.right, "op": v.op,
            "right_value": None if v.right_value is None else str(v.right_value),
        } for v in settlement.values],
    }


def from_json(name: str, doc: Mapping[str, Any]) -> st.Settlement:
    """The pure value from a stored or posted document. Raises ValueError on a shape it cannot read;
    a shape it CAN read but that is wrong is `settle.validate`'s job."""
    values = []
    for raw in doc.get("values") or []:
        try:
            rule = st.Rule(raw["rule"])
        except (KeyError, ValueError):
            raise ValueError(f"settled value {raw.get('name')!r} has an unknown rule "
                             f"{raw.get('rule')!r}; use one of {', '.join(r.value for r in st.Rule)}")
        rv = raw.get("right_value")
        values.append(st.Settled(
            name=str(raw.get("name") or "").strip(), rule=rule, field=raw.get("field") or None,
            statuses=frozenset(raw.get("statuses") or ()), only=frozenset(raw.get("only") or ()),
            left=raw.get("left") or None, right=raw.get("right") or None, op=raw.get("op") or None,
            right_value=None if rv is None or rv == "" else Decimal(str(rv))))
    return st.Settlement(
        name=name,
        reads=tuple(str(m) for m in (doc.get("reads") or ())),
        key=tuple(str(k) for k in (doc.get("key") or ())),
        carry=tuple(str(c) for c in (doc.get("carry") or ())),
        values=tuple(values))


async def load(db: AsyncSession, customer_code: str, *, enabled_only: bool = True
               ) -> dict[str, st.Settlement]:
    """Every declared settlement for a tenant, by name."""
    q = select(AnalyticsSettlement).where(AnalyticsSettlement.customer_code == customer_code)
    if enabled_only:
        q = q.where(AnalyticsSettlement.enabled.is_(True))
    rows = (await db.execute(q)).scalars().all()
    return {r.name: from_json(r.name, r.definition or {}) for r in rows}


# ============================================================== keeping rows current

def keys_touched(facts: Iterable[Mapping[str, Any]], settlement: st.Settlement) -> set[tuple[str, ...]]:
    """The keys among these facts that this settlement would settle. One pass over memory, no query,
    because the fold has just built these rows anyway."""
    return set(st.settle(facts, settlement).keys())


async def _calls_for(db: AsyncSession, customer_code: str, settlement: st.Settlement,
                     keys: Sequence[tuple[str, ...]]) -> list[dict]:
    """EVERY call for these keys, whatever window it fell in. A release is recomputed from its whole
    history, which is what makes the result independent of how the folds were batched."""
    if not keys:
        return []
    conditions = []
    for key in keys:
        parts = []
        for name, value in zip(settlement.key, key):
            if contract.is_attr_path(name):
                parts.append(AnalyticsFact.attributes[contract.attr_key(name)].astext == value)
            else:
                parts.append(getattr(AnalyticsFact, name) == value)
        conditions.append(and_(*parts))
    rows = (await db.execute(
        select(AnalyticsFact).where(AnalyticsFact.customer_code == customer_code,
                                    AnalyticsFact.method.in_(settlement.reads),
                                    or_(*conditions)))).scalars().all()
    return [{c.name: getattr(r, c.name) for c in AnalyticsFact.__table__.columns} for r in rows]


def _stringify(value: Any) -> Any:
    """Attribute-bag values as the fact rows store them: EVERY number as a string, a time as ISO.

    Counts and flags are ints in memory and would otherwise land as JSON numbers beside Decimals
    that landed as strings, and then one coercion could not read both. The fact rows chose strings
    for the same reason."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value if isinstance(value, str) else str(value)


def _to_row(customer_code: str, settlement: st.Settlement, settled: st.SettledRow,
            now: datetime) -> dict:
    attributes = {k: _stringify(v) for k, v in settled.carried.items()}
    attributes.update({k: _stringify(v) for k, v in settled.values.items()})
    row = {
        "id": uuid.uuid4(), "customer_code": customer_code, "settlement": settlement.name,
        "key": KEY_SEP.join(settled.key), "key_parts": list(settled.key),
        "event_time": settled.event_time,
        "business_date": settled.event_time.date() if settled.event_time else None,
        "method": settlement.reads[0] if len(settlement.reads) == 1 else None,
        "attributes": attributes, "calls": settled.calls, "settled_at": now,
    }
    for col in TYPED:
        if col in settled.carried and col != "method":
            row[col] = _stringify(settled.carried[col])
    return row


async def settle_keys(db: AsyncSession, customer_code: str, settlement: st.Settlement,
                      keys: Iterable[tuple[str, ...]], *, now: datetime | None = None) -> int:
    """Recompute these keys from all their calls and upsert their rows. Does NOT commit. Returns how
    many rows were written."""
    keys = sorted(set(keys))
    if not keys:
        return 0
    now = now or datetime.now(timezone.utc)
    calls = await _calls_for(db, customer_code, settlement, keys)
    settled = st.settle(calls, settlement)
    rows = [_to_row(customer_code, settlement, s, now) for s in settled.values()]
    if not rows:
        return 0
    stmt = pg_insert(AnalyticsSettledRow).values(rows)
    update_cols = {c: getattr(stmt.excluded, c) for c in rows[0] if c not in ("id", "customer_code", "settlement", "key")}
    await db.execute(stmt.on_conflict_do_update(constraint="uq_analytics_settled_rows_key",
                                                set_=update_cols))
    return len(rows)


async def settle_touched(db: AsyncSession, customer_code: str, facts: Sequence[Mapping[str, Any]],
                         settlements: Mapping[str, st.Settlement] | None = None) -> dict[str, int]:
    """The fold's hook: for every enabled settlement, recompute the keys these facts touched. Does
    NOT commit; the caller's transaction covers it, so a settled row can never describe a fact that
    was rolled back."""
    settlements = settlements if settlements is not None else await load(db, customer_code)
    out: dict[str, int] = {}
    for name, s in settlements.items():
        touched = keys_touched(facts, s)
        if touched:
            out[name] = await settle_keys(db, customer_code, s, touched)
    return out


async def resettle_all(db: AsyncSession, customer_code: str, settlement: st.Settlement) -> int:
    """Every key this settlement has, from scratch. For a new or edited settlement, whose rows do not
    exist yet or were made by the old rules. Does NOT commit."""
    key_exprs = []
    for name in settlement.key:
        key_exprs.append(AnalyticsFact.attributes[contract.attr_key(name)].astext
                         if contract.is_attr_path(name) else getattr(AnalyticsFact, name))
    rows = (await db.execute(
        select(*key_exprs).where(AnalyticsFact.customer_code == customer_code,
                                 AnalyticsFact.method.in_(settlement.reads))
        .group_by(*key_exprs))).all()
    keys = [tuple(str(p) for p in r) for r in rows if all(p not in (None, "") for p in r)]
    written = 0
    for i in range(0, len(keys), 500):
        written += await settle_keys(db, customer_code, settlement, keys[i:i + 500])
    return written


# ============================================================== reading

def _group_expr(name: str):
    """A group-by field on a settled row: a typed column, or a name in the attribute bag."""
    if name in TYPED or name in ("event_time", "business_date"):
        return getattr(AnalyticsSettledRow, name)
    return AnalyticsSettledRow.attributes[contract.attr_key(name) if contract.is_attr_path(name) else name].astext


async def read_grouped(db: AsyncSession, customer_code: str, settlement: st.Settlement, *,
                       group_by: Sequence[str], since: datetime | None, until: datetime | None,
                       limit: int = 500) -> list[dict]:
    """Settled rows grouped and summed on request. Every settled value that is a number is summed;
    `calls` is summed; rows are counted. No roll-up stands between the reader and the rows."""
    numeric = [v.name for v in settlement.values
               if v.rule in (st.Rule.sum, st.Rule.count, st.Rule.first, st.Rule.last, st.Rule.min,
                             st.Rule.max, st.Rule.distinct_count, st.Rule.difference, st.Rule.flag)
               and v.field != "event_time"]
    groups = [_group_expr(g).label(f"g{i}") for i, g in enumerate(group_by)]
    sums = [func.sum(func.nullif(AnalyticsSettledRow.attributes[n].astext, "").cast(
        Numeric(30, 6))).label(n) for n in numeric]
    q = select(*groups, func.count().label("rows"), func.sum(AnalyticsSettledRow.calls).label("calls"), *sums).where(
        AnalyticsSettledRow.customer_code == customer_code,
        AnalyticsSettledRow.settlement == settlement.name)
    if since is not None:
        q = q.where(AnalyticsSettledRow.event_time >= since)
    if until is not None:
        q = q.where(AnalyticsSettledRow.event_time < until)
    if groups:
        q = q.group_by(*groups)
    q = q.order_by(func.count().desc()).limit(limit)
    out = []
    for r in (await db.execute(q)).mappings().all():
        entry = {"dimensions": [r[f"g{i}"] for i in range(len(group_by))],
                 "rows": int(r["rows"]), "calls": int(r["calls"] or 0)}
        for n in numeric:
            v = r[n]
            entry[n] = None if v is None else format(Decimal(v).normalize(), "f")
        out.append(entry)
    return out


async def read_key(db: AsyncSession, customer_code: str, settlement: st.Settlement,
                   key: tuple[str, ...]) -> tuple[list[dict], st.SettledRow | None]:
    """The preview: every call for one key, and the row they settle to. Computed live from the
    calls, so it is right even before the fold has run."""
    calls = await _calls_for(db, customer_code, settlement, [key])
    settled = st.settle(calls, settlement).get(key)
    calls.sort(key=lambda r: (r.get("event_time") is None, r.get("event_time")))
    return calls, settled
