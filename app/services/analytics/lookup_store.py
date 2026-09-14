"""Chunk 100: reading and writing the lookup tables. The only half that touches a database.

`lookup.py` decides what a lookup MEANS and is pure. This module reads declarations, records what the
fold observed, and builds a `Resolver` for one answer. The split is the same one `read.py` makes, and
for the same reason: every interesting failure here is a decision, and a decision is something a test
should be able to inspect without a database.

**How a value's period is decided.**

An attribute is declared `stable` when it belongs to its key for life. A delivery's customer is: across
114 live delivery keys, not one ever named a second customer, and across 322 item keys not one ever had
a second description. For a stable attribute the first value learned is written from
`lookup.BEGINNING` and stays, so a delivery named AFTER its picks still names them; a later, different
value is a CONTRADICTION rather than a change, and it is counted and left alone under `first_wins`.

An attribute declared not stable genuinely changes over time. There the first value still runs from the
beginning, and a later, different value closes the open period at the instant it was observed and opens
its own. That is what makes "the name as it was at the time" true rather than aspirational, and it is
why the periods exist at all even though nothing in the live data has yet used them.
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.services.analytics import lookup as lk

logger = logging.getLogger(__name__)


# ==================================================================== declarations

def to_row(lookup: lk.Lookup) -> list[dict]:
    """A declaration's attributes as the JSON the column holds. Sorted, so the stored form is stable."""
    return [{"name": a.name, "stable": a.stable, "on_conflict": a.on_conflict,
             "sources": [{"method": s.method, "key_field": s.key_field, "value_field": s.value_field}
                         for s in a.sources]}
            for a in sorted(lookup.attributes, key=lambda a: a.name)]


def from_row(row) -> lk.Lookup:
    """One `analytics_lookups` row as the pure value. Tolerant of a missing key, never of a wrong one."""
    attributes = []
    for raw in (row.attributes or []):
        sources = tuple(lk.Source(method=s["method"], key_field=s["key_field"],
                                  value_field=s["value_field"])
                        for s in (raw.get("sources") or []))
        attributes.append(lk.Attribute(name=raw["name"], sources=sources,
                                       stable=bool(raw.get("stable", True)),
                                       on_conflict=raw.get("on_conflict") or "first_wins"))
    return lk.Lookup(name=row.name, key_field=row.key_field,
                     attributes=tuple(sorted(attributes, key=lambda a: a.name)))


async def load(db: AsyncSession, customer_code: str, *,
               enabled_only: bool = True) -> dict[str, lk.Lookup]:
    """Every declared lookup for this tenant, by name.

    Read once per request or per fold run, exactly like the registry switches: a lookup consulted
    per row would be a query per row, and a set read twice in one run could disagree with itself.
    """
    stmt = select(AnalyticsLookup).where(AnalyticsLookup.customer_code == customer_code)
    if enabled_only:
        stmt = stmt.where(AnalyticsLookup.enabled.is_(True))
    return {row.name: from_row(row) for row in (await db.execute(stmt)).scalars().all()}


# ==================================================================== observations

def _reduce(observations: Iterable[lk.Observation]) -> dict[tuple[str, str, str], list[lk.Observation]]:
    grouped: dict[tuple[str, str, str], list[lk.Observation]] = defaultdict(list)
    for o in observations:
        grouped[(o.lookup, o.key, o.attribute)].append(o)
    for group in grouped.values():
        group.sort(key=lambda o: o.at)
    return grouped


async def record(db: AsyncSession, customer_code: str, observations: Sequence[lk.Observation],
                 lookups: Mapping[str, lk.Lookup]) -> dict:
    """Write what the fold observed. Does NOT commit.

    Returns counts, including `conflicts`: a stable attribute told two different values for one key.
    Zero on every relationship measured so far, and worth surfacing rather than hiding, because a
    non-zero conflict count means either the declaration names the wrong source or the assumption that
    the attribute is stable is wrong.
    """
    stats = {"observed": len(observations), "inserted": 0, "extended": 0, "changed": 0,
             "conflicts": 0}
    if not observations:
        return stats
    grouped = _reduce(observations)

    wanted = {(lookup, key) for lookup, key, _attribute in grouped}
    existing: dict[tuple[str, str, str], list[AnalyticsLookupValue]] = defaultdict(list)
    if wanted:
        rows = (await db.execute(
            select(AnalyticsLookupValue).where(
                AnalyticsLookupValue.customer_code == customer_code,
                AnalyticsLookupValue.lookup.in_(sorted({l for l, _ in wanted})),
                AnalyticsLookupValue.key.in_(sorted({k for _, k in wanted}))))).scalars().all()
        for row in rows:
            existing[(row.lookup, row.key, row.attribute)].append(row)

    now = datetime.now(timezone.utc)
    fresh: list[dict] = []
    for (lookup_name, key, attribute_name), group in grouped.items():
        declared = lookups.get(lookup_name)
        attribute = declared.attribute(attribute_name) if declared else None
        stable = True if attribute is None else attribute.stable
        latest_wins = bool(attribute and attribute.on_conflict == "latest_wins")
        periods = sorted(existing.get((lookup_name, key, attribute_name), ()),
                         key=lambda r: lk._aware(r.valid_from))

        if not periods:
            # The rule that makes a late name work: valid from the beginning of the key's life, not
            # from the instant somebody happened to say it.
            chosen = group[-1] if latest_wins else group[0]
            fresh.append({
                "id": uuid.uuid4(), "customer_code": customer_code, "lookup": lookup_name,
                "key": key, "attribute": attribute_name, "value": chosen.value,
                "valid_from": lk.BEGINNING, "valid_to": None, "origin": "observed",
                "source_method": chosen.source_method, "observations": len(group),
                "first_seen_at": group[0].at, "last_seen_at": group[-1].at,
            })
            stats["inserted"] += 1
            # Anything in the same batch that disagrees is already a contradiction.
            if stable and len({o.value for o in group}) > 1:
                stats["conflicts"] += 1
                logger.warning("Analytics [%s]: lookup %s key %s attribute %s was told %d different "
                               "values in one run; the declaration may name the wrong source",
                               customer_code, lookup_name, key, attribute_name,
                               len({o.value for o in group}))
            continue

        open_period = periods[-1]
        for o in group:
            if o.value == open_period.value:
                open_period.observations += 1
                open_period.last_seen_at = max(lk._aware(open_period.last_seen_at), lk._aware(o.at))
                stats["extended"] += 1
                continue
            if stable:
                stats["conflicts"] += 1
                logger.warning("Analytics [%s]: lookup %s key %s attribute %s is declared stable but "
                               "%s says %r where %r was already recorded; keeping the %s value",
                               customer_code, lookup_name, key, attribute_name, o.source_method,
                               o.value, open_period.value, "latest" if latest_wins else "first")
                if latest_wins:
                    open_period.value = o.value
                    open_period.source_method = o.source_method
                    open_period.last_seen_at = max(lk._aware(open_period.last_seen_at),
                                                   lk._aware(o.at))
                continue
            # A genuine change: close what was true and open what is.
            if open_period.valid_to is None and lk._aware(o.at) > lk._aware(open_period.valid_from):
                open_period.valid_to = o.at
                fresh.append({
                    "id": uuid.uuid4(), "customer_code": customer_code, "lookup": lookup_name,
                    "key": key, "attribute": attribute_name, "value": o.value,
                    "valid_from": o.at, "valid_to": None, "origin": "observed",
                    "source_method": o.source_method, "observations": 1,
                    "first_seen_at": o.at, "last_seen_at": o.at,
                })
                stats["changed"] += 1
                open_period = _Pending(o.value, o.at)

    if fresh:
        # ON CONFLICT DO NOTHING rather than an update: two fold runs racing on the same key must not
        # both write a period, and the loser has nothing to add - the winner wrote the same value.
        await db.execute(pg_insert(AnalyticsLookupValue).values(fresh).on_conflict_do_nothing(
            constraint="uq_analytics_lookup_values_key"))
    return stats


class _Pending:
    """The period just opened in this batch, so a second change in the same batch chains off it.

    A plain object rather than a row because the row does not exist yet: it is in `fresh`, and reading
    it back mid-batch would be a query per observation.
    """

    __slots__ = ("value", "valid_from", "valid_to", "observations", "last_seen_at", "source_method")

    def __init__(self, value: str, valid_from: datetime):
        self.value, self.valid_from, self.valid_to = value, valid_from, None
        self.observations, self.last_seen_at, self.source_method = 1, valid_from, None


# ==================================================================== resolving

async def resolver(db: AsyncSession, customer_code: str,
                   needed: Mapping[str, Iterable[str]],
                   attributes: Iterable[tuple[str, str]] = ()) -> lk.Resolver:
    """A `Resolver` holding only the keys one answer names.

    Never a scan. An answer has as many keys as it has groups - a few hundred at most, because a
    grouping with more lines than that is not something anybody reads - while the table behind it
    grows with distinct entities.
    """
    out = lk.Resolver()
    wanted_attributes = {a for _lookup, a in attributes}
    for lookup_name, keys in needed.items():
        keys = [str(k) for k in keys]
        if not keys:
            continue
        stmt = select(AnalyticsLookupValue).where(
            AnalyticsLookupValue.customer_code == customer_code,
            AnalyticsLookupValue.lookup == lookup_name,
            AnalyticsLookupValue.key.in_(keys))
        if wanted_attributes:
            stmt = stmt.where(AnalyticsLookupValue.attribute.in_(sorted(wanted_attributes)))
        for row in (await db.execute(stmt)).scalars().all():
            out.add(row.lookup, row.key, row.attribute,
                    lk.Period(value=row.value, valid_from=row.valid_from, valid_to=row.valid_to))
    return out


# ==================================================================== backfill and suggestion

async def backfill(db: AsyncSession, customer_code: str, declared: lk.Lookup, *,
                   days: int = 60) -> dict:
    """Fill a newly declared lookup from the facts already stored. Does NOT commit.

    This is what lets a lookup be declared at any time and answer immediately for data collected months
    ago. Copying the value onto each fact instead would mean REWRITING those facts: 6,586 of them after
    two days of live data, for an item description alone, and millions after a year.

    Only the methods the declaration names are read, which is usually a small fraction of the table.
    """
    from app.persistence.models.analytics_fact import AnalyticsFact

    methods = sorted(declared.sources_by_method())
    if not methods:
        return {"observed": 0, "inserted": 0, "extended": 0, "changed": 0, "conflicts": 0,
                "facts_read": 0, "methods": []}
    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows = (await db.execute(
        select(AnalyticsFact)
        .where(AnalyticsFact.customer_code == customer_code,
               AnalyticsFact.method.in_(methods),
               AnalyticsFact.event_time >= since)
        .order_by(AnalyticsFact.event_time))).scalars().all()
    facts = [{c.name: getattr(r, c.name) for c in AnalyticsFact.__table__.columns} for r in rows]
    stats = await record(db, customer_code, lk.harvest(facts, [declared]), {declared.name: declared})
    return {**stats, "facts_read": len(facts), "methods": methods}


#: How much of a field's values must be known keys before it is called a spelling of the key rather
#: than a field that merely overlaps one.
KEY_SPELLING_SHARE = 0.9


async def suggest_sources(db: AsyncSession, customer_code: str, *, key_field: str,
                          days: int = 14) -> dict:
    """Propose where a key's attributes could be harvested from, by looking at the data.

    Nobody should have to discover by hand that a delivery number is `DeliveryNumber` on a packing
    record and `resp.DeliveryNumber` on a routing one. Both spellings occur live: the typed column is
    populated on 653 `NewDeliveryPackage` facts and on 0 `GetNextDeliveryByRoute` facts, where the
    number only ever comes back in the response.

    A spelling is recognised as the key when nearly all of its values ARE known keys, which is why the
    known key set is read first. It is small by construction: one value per entity, not per record.
    """
    since = datetime.now(timezone.utc) - timedelta(days=days)
    keys = [k for k in (await db.execute(text(
        f"SELECT DISTINCT {key_field} FROM analytics_facts "
        f"WHERE customer_code = :c AND event_time >= :since AND {key_field} IS NOT NULL"),
        {"c": customer_code, "since": since})).scalars().all() if k]

    rows = (await db.execute(text("""
        WITH pairs AS (
            SELECT f.method, kv.key AS field, kv.value #>> '{}' AS value
            FROM analytics_facts f, jsonb_each(f.attributes) kv
            WHERE f.customer_code = :c AND f.event_time >= :since AND kv.key NOT LIKE '\\_\\_%'
        )
        SELECT method, field, count(*) AS facts, count(DISTINCT value) AS distinct_values,
               count(*) FILTER (WHERE value = ANY(:keys)) AS matches_a_key
        FROM pairs GROUP BY method, field ORDER BY method, facts DESC"""),
        {"c": customer_code, "since": since, "keys": keys or [""]})).all()

    typed = {m for m, in (await db.execute(text(
        f"SELECT DISTINCT method FROM analytics_facts WHERE customer_code = :c "
        f"AND event_time >= :since AND {key_field} IS NOT NULL"),
        {"c": customer_code, "since": since})).all()}

    per_method: dict[str, dict] = {}
    for method, field, facts, distinct_values, matches in rows:
        entry = per_method.setdefault(method, {"method": method, "facts": 0,
                                               "key_spellings": [], "candidates": []})
        entry["facts"] = max(entry["facts"], facts)
        item = {"field": field, "facts": facts, "distinct_values": distinct_values}
        if keys and matches >= facts * KEY_SPELLING_SHARE and distinct_values > 1:
            entry["key_spellings"].append(item)
        else:
            entry["candidates"].append(item)
    for method, entry in per_method.items():
        if method in typed:
            # The typed column is the simplest spelling of all and needs no recognising.
            entry["key_spellings"].insert(0, {"field": key_field, "facts": entry["facts"],
                                              "distinct_values": len(keys), "typed_column": True})
        entry["candidates"] = sorted(entry["candidates"],
                                     key=lambda c: (-c["distinct_values"], c["field"]))[:40]
    usable = [e for e in per_method.values() if e["key_spellings"]]
    return {"key_field": key_field, "days": days, "keys_seen": len(keys),
            "methods": sorted(usable, key=lambda e: -e["facts"])}
