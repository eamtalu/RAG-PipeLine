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

import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import Date, Integer, Numeric, and_, case, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics import contract
from app.services.analytics import settle as st
from app.services.analytics import settle_query as sq

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
            **({"methods": list(v.methods), "match": list(v.match), "window_s": v.window_s}
               if v.rule in st.NEARBY else {}),
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
            right_value=None if rv is None or rv == "" else Decimal(str(rv)),
            methods=tuple(str(m) for m in (raw.get("methods") or ())),
            match=tuple(str(m) for m in (raw.get("match") or ())),
            window_s=int(raw["window_s"]) if raw.get("window_s") not in (None, "") else None))
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


async def _tenant_zone(db: AsyncSession, customer_code: str) -> tzinfo:
    """The zone a handheld's clock is in, so a time an attribute carries without one can be read.
    The same source the normaliser uses for `business_date`."""
    return ZoneInfo(await get_customer_timezone(db, customer_code))


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
            now: datetime, tz: tzinfo | None = None) -> dict:
    attributes = {k: _stringify(v) for k, v in settled.carried.items()}
    attributes.update({k: _stringify(v) for k, v in settled.values.items()})
    row = {
        "id": uuid.uuid4(), "customer_code": customer_code, "settlement": settlement.name,
        "key": KEY_SEP.join(settled.key), "key_parts": list(settled.key),
        "event_time": settled.event_time,
        # The tenant's day, as the facts use: a release confirmed at 00:30 BST is today's, not the
        # day before (chunk 129 found 93 of one morning's 394 releases filed under the UTC day).
        "business_date": (settled.event_time.astimezone(tz) if tz else settled.event_time).date()
                         if settled.event_time else None,
        "method": settlement.reads[0] if len(settlement.reads) == 1 else None,
        "attributes": attributes, "calls": settled.calls, "settled_at": now,
    }
    # Every typed column is present on EVERY row, None where the settlement did not carry it or
    # the calls had nothing to carry. A multi-row insert needs every row to name the same columns,
    # and on the live tenant 1,025 of 6,245 releases have no lot: the first backfill was one
    # 500 for as long as that column was only set when a value existed.
    for col in TYPED:
        if col == "method":
            continue
        value = settled.carried.get(col)
        row[col] = None if value is None else _stringify(value)
    return row


async def _context_for(db: AsyncSession, customer_code: str, settlement: st.Settlement,
                       calls: Sequence[Mapping[str, Any]]) -> list[dict]:
    """Chunk 129: the calls of other methods the settlement's `nearby_*` rules may read, for these
    releases. Bounded three ways: only the methods the rules name, only the match values the calls
    carry, and only from the widest window before the earliest call to the latest call."""
    rules = [v for v in settlement.values if v.rule in st.NEARBY]
    timed = [c for c in calls if c.get("event_time") is not None]
    if not rules or not timed:
        return []
    methods = sorted({m for v in rules for m in v.methods})
    window = max(v.window_s or 0 for v in rules)
    lo = min(c["event_time"] for c in timed) - timedelta(seconds=window)
    hi = max(c["event_time"] for c in timed)
    conditions = [AnalyticsFact.customer_code == customer_code, AnalyticsFact.method.in_(methods),
                  AnalyticsFact.event_time >= lo, AnalyticsFact.event_time <= hi]
    for field in sorted({m for v in rules for m in v.match}):
        wanted = sorted({str(x) for x in (st._read(c, field) for c in calls) if x is not None})
        if not wanted:
            return []
        column = (AnalyticsFact.attributes[contract.attr_key(field)].astext if contract.is_attr_path(field)
                  else getattr(AnalyticsFact, field))
        conditions.append(column.in_(wanted))
    rows = (await db.execute(select(AnalyticsFact).where(*conditions))).scalars().all()
    return [{c.name: getattr(r, c.name) for c in AnalyticsFact.__table__.columns} for r in rows]


#: How many rounds of lookup loading a settlement may take: one per `lookup` rule that keys on
#: another lookup's answer (a designated location's zone), plus the first.
_MAX_LOOKUP_ROUNDS = 6


async def _settle_with_lookups(db: AsyncSession, customer_code: str, settlement: st.Settlement,
                               calls: list[dict], context: list[dict], tz) -> dict:
    """Settle, loading exactly the lookup values the `lookup` rules ask for.

    A rule may key on an earlier rule's answer, so the keys are not all known up front. Each round
    settles with what is loaded, records what was asked for and missing, loads that, and settles
    again; it ends when a round asks for nothing new. The pure half never touches the database."""
    from app.services.analytics import lookup_store

    if not any(v.rule is st.Rule.lookup for v in settlement.values):
        return st.settle(calls, settlement, tz=tz, context=context)
    loaded: dict[tuple[str, str, str], Any] = {}
    fetched: set[tuple[str, str, str]] = set()
    settled: dict = {}
    for _ in range(_MAX_LOOKUP_ROUNDS):
        missing: set[tuple[str, str, str]] = set()

        def resolve(lookup: str, attribute: str, key: str) -> Any:
            triple = (lookup, attribute, key)
            if triple not in fetched:
                missing.add(triple)
            return loaded.get(triple)

        settled = st.settle(calls, settlement, tz=tz, context=context, resolve=resolve)
        if not missing:
            break
        by_lookup: dict[str, set[str]] = {}
        for lookup, _attribute, key in missing:
            by_lookup.setdefault(lookup, set()).add(key)
        attributes = {(lookup, attribute) for lookup, attribute, _key in missing}
        resolver = await lookup_store.resolver(db, customer_code, by_lookup, attributes)
        now = datetime.now(timezone.utc)
        for lookup, attribute, key in missing:
            fetched.add((lookup, attribute, key))
            loaded[(lookup, attribute, key)] = resolver.value(lookup, key, attribute, now)
    return settled


async def settle_keys(db: AsyncSession, customer_code: str, settlement: st.Settlement,
                      keys: Iterable[tuple[str, ...]], *, now: datetime | None = None) -> int:
    """Recompute these keys from all their calls and upsert their rows. Does NOT commit. Returns how
    many rows were written."""
    keys = sorted(set(keys))
    if not keys:
        return 0
    now = now or datetime.now(timezone.utc)
    calls = await _calls_for(db, customer_code, settlement, keys)
    context = await _context_for(db, customer_code, settlement, calls)
    tz = await _tenant_zone(db, customer_code)
    settled = await _settle_with_lookups(db, customer_code, settlement, calls, context, tz)
    rows = [_to_row(customer_code, settlement, s, now, tz) for s in settled.values()]
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

#: What a stored settled value must look like to be summed: a plain decimal, nothing else.
NUMBER_SHAPE = r"^\s*-?[0-9]+(\.[0-9]+)?\s*$"

def _field_expr(name: str):
    """A field on a settled row as text: the key, a typed column, or a name in the bag."""
    if name == "key":
        return AnalyticsSettledRow.key
    if name == "calls":
        return AnalyticsSettledRow.calls
    if name in TYPED or name in ("event_time", "business_date"):
        return getattr(AnalyticsSettledRow, name)
    return AnalyticsSettledRow.attributes[sq.plain(name)].astext


def _numeric_expr(name: str):
    """The same field as a number, NULL where it is not shaped like one, so a time or a blank
    never fails the cast inside PostgreSQL."""
    if name == "calls":
        return AnalyticsSettledRow.calls
    text = _field_expr(name)
    return case((text.op("~")(NUMBER_SHAPE), text.cast(Numeric(30, 6))), else_=None)


def _local(tz: tzinfo | None):
    """`event_time` on the tenant's clock, so an hour bucket is the hour the pickers lived in."""
    zone = getattr(tz, "key", None) or "UTC"
    return func.timezone(zone, AnalyticsSettledRow.event_time)


def _group_expr(name: str, tz: tzinfo | None = None):
    """A group-by field: the key itself, a typed column, a name in the bag, or a time bucket.

    `key` is what lets a reader drill all the way down to one release: grouped by it, every row is
    one settled row and the sums are the row's own values. The buckets are in the tenant's zone."""
    if name == "hour":
        return func.extract("hour", _local(tz)).cast(Integer)
    if name == "hour_start":
        return func.date_trunc("hour", _local(tz))
    if name == "day":
        return _local(tz).cast(Date)
    if name == "week":
        return func.date_trunc("week", _local(tz)).cast(Date)
    if name == "month":
        return func.date_trunc("month", _local(tz)).cast(Date)
    return _field_expr(name)


def _filter_clause(f: sq.Filter):
    """One `where`. A number compares as a number, a time as a time, anything else as text."""
    if f.field in ("event_time",):
        left, right = AnalyticsSettledRow.event_time, datetime.fromisoformat(f.value)
    elif f.field == "business_date":
        left, right = AnalyticsSettledRow.business_date, datetime.fromisoformat(f.value).date()
    elif re.match(NUMBER_SHAPE, f.value) and f.field not in ("key", *TYPED):
        left, right = _numeric_expr(f.field), Decimal(f.value)
    else:
        left, right = _field_expr(f.field), f.value
    ops = {"==": left == right, "!=": left != right, "<": left < right, "<=": left <= right,
           ">": left > right, ">=": left >= right}
    return ops[f.op]


def _stat_expr(s: sq.Stat):
    if s.kind == "distinct":
        return func.count(func.distinct(_field_expr(s.field)))
    num = _numeric_expr(s.field)
    if s.kind == "mean":
        return func.avg(num)
    if s.kind == "min":
        return func.min(num)
    if s.kind == "max":
        return func.max(num)
    quantile = {"median": 0.5, "p90": 0.9, "p95": 0.95, "p99": 0.99}[s.kind]
    return func.percentile_cont(quantile).within_group(num)


def _dimension_json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _number_json(value: Any) -> str | None:
    return None if value is None else format(Decimal(str(value)).normalize(), "f")


async def read_grouped(db: AsyncSession, customer_code: str, settlement: st.Settlement, *,
                       group_by: Sequence[str], since: datetime | None, until: datetime | None,
                       limit: int = 500, filters: Sequence[sq.Filter] = (), stats: Sequence[sq.Stat] = (),
                       tz: tzinfo | None = None, order_by: str | None = None, descending: bool = True) -> list[dict]:
    """Settled rows grouped and summed on request. Every settled value that is a number is summed;
    `calls` is summed; rows are counted. No roll-up stands between the reader and the rows.

    `filters` keep only the rows that pass every one. `stats` add a median, a percentile, a mean,
    an extreme or a distinct count per group, named `median_duration_s`, `distinct_user_name`.
    A settled value may be a time rather than a number: `started_at`, or a `max` of `event_time`.
    Only what looks like a number is cast, so a time sums to nothing instead of failing the read."""
    numeric = [v.name for v in settlement.values]
    groups = [_group_expr(g, tz).label(f"g{i}") for i, g in enumerate(group_by)]
    sums = [func.sum(_numeric_expr(n)).label(n) for n in numeric]
    extras = [_stat_expr(s).label(s.label) for s in stats]
    q = select(*groups, func.count().label("rows"), func.sum(AnalyticsSettledRow.calls).label("calls"),
               *sums, *extras).where(
        AnalyticsSettledRow.customer_code == customer_code,
        AnalyticsSettledRow.settlement == settlement.name)
    if since is not None:
        q = q.where(AnalyticsSettledRow.event_time >= since)
    if until is not None:
        q = q.where(AnalyticsSettledRow.event_time < until)
    for f in filters:
        q = q.where(_filter_clause(f))
    if groups:
        q = q.group_by(*groups)
    # The order matters under a limit: "top 5 by units short" must sort by the shortfall sum on the
    # server, or the cap keeps the most frequent groups and drops the biggest.
    sums_by_name = {c.name: c for c in sums}
    stats_by_name = {c.name: c for c in extras}
    if order_by in (None, "rows"):
        key = func.count()
    elif order_by == "calls":
        key = func.sum(AnalyticsSettledRow.calls)
    elif order_by in sums_by_name:
        key = sums_by_name[order_by]
    else:
        key = stats_by_name[order_by]
    q = q.order_by(key.desc().nullslast() if descending else key.asc().nullslast()).limit(limit)
    out = []
    for r in (await db.execute(q)).mappings().all():
        entry = {"dimensions": [_dimension_json(r[f"g{i}"]) for i in range(len(group_by))],
                 "rows": int(r["rows"]), "calls": int(r["calls"] or 0)}
        for n in numeric:
            entry[n] = _number_json(r[n])
        for s in stats:
            v = r[s.label]
            entry[s.label] = int(v) if s.kind == "distinct" and v is not None else _number_json(v)
        out.append(entry)
    return out


async def read_key(db: AsyncSession, customer_code: str, settlement: st.Settlement,
                   key: tuple[str, ...]) -> tuple[list[dict], st.SettledRow | None]:
    """The preview: every call for one key, and the row they settle to. Computed live from the
    calls, so it is right even before the fold has run."""
    calls = await _calls_for(db, customer_code, settlement, [key])
    context = await _context_for(db, customer_code, settlement, calls)
    settled = (await _settle_with_lookups(db, customer_code, settlement, calls, context,
                                          await _tenant_zone(db, customer_code))).get(key)
    calls.sort(key=lambda r: (r.get("event_time") is None, r.get("event_time")))
    return calls, settled


def _row_json(r: AnalyticsSettledRow) -> dict:
    return {"key": r.key.split(KEY_SEP), "event_time": r.event_time.isoformat() if r.event_time else None,
            "business_date": r.business_date.isoformat() if r.business_date else None,
            "method": r.method, "transaction_name": r.transaction_name, "warehouse": r.warehouse,
            "item_number": r.item_number, "delivery_number": r.delivery_number,
            "lot_number": r.lot_number, "user_name": r.user_name,
            "attributes": r.attributes or {}, "calls": r.calls,
            "settled_at": r.settled_at.isoformat() if r.settled_at else None}


async def list_rows(db: AsyncSession, customer_code: str, settlement: st.Settlement, *,
                    since: datetime | None, until: datetime | None, search: str | None,
                    limit: int, offset: int, filters: Sequence[sq.Filter] = (),
                    sort: str | None = None, descending: bool = True) -> tuple[list[dict], int]:
    """The settled rows themselves, newest first unless sorted otherwise, with a total so a screen
    can page.

    `search` matches the key, the delivery, the item or the lot, because those are the four things
    somebody types when they are looking for one release. `filters` narrow to "the zero-picks" or
    "lines over five minutes"; `sort` orders by any field, as a number when it is one, so "the
    twenty longest lines" is one call. A grouped read answers "how much"; this answers "show me"."""
    q = select(AnalyticsSettledRow).where(AnalyticsSettledRow.customer_code == customer_code,
                                          AnalyticsSettledRow.settlement == settlement.name)
    if since is not None:
        q = q.where(AnalyticsSettledRow.event_time >= since)
    if until is not None:
        q = q.where(AnalyticsSettledRow.event_time < until)
    if search:
        needle = f"%{search.strip()}%"
        q = q.where(or_(AnalyticsSettledRow.key.ilike(needle),
                        AnalyticsSettledRow.delivery_number.ilike(needle),
                        AnalyticsSettledRow.item_number.ilike(needle),
                        AnalyticsSettledRow.lot_number.ilike(needle)))
    for f in filters:
        q = q.where(_filter_clause(f))
    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    if sort is None or sort == "event_time":
        primary = AnalyticsSettledRow.event_time
    elif sort in ("key", "business_date", *TYPED):
        primary = _field_expr(sort)
    else:
        primary = _numeric_expr(sort)
    ordered = primary.desc().nullslast() if descending else primary.asc().nullslast()
    rows = (await db.execute(q.order_by(ordered, AnalyticsSettledRow.event_time.desc().nullslast(),
                                        AnalyticsSettledRow.key)
                             .limit(limit).offset(offset))).scalars().all()
    return [_row_json(r) for r in rows], int(total or 0)
