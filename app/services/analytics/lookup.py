"""Chunk 100: key to attribute lookups, resolved at READ time.

A fact records what one exchange said, and that is often not enough to answer the question somebody
actually has. Measured on tmp-live: a pick carries its delivery number on every one of 1,343 records
and the customer name on NONE of them. The name is on the packing and routing records instead, under
three different spellings across four methods.

**Why the value is not copied onto the fact.** Three reasons, all measured rather than assumed.

1. You need this map either way. Stamping the customer onto a pick means first knowing the customer for
   that delivery, and knowing that is exactly what this module is. Copying as well buys nothing and
   costs the second and third points.
2. Adding a lookup later would mean rewriting history. After two days of live data, stamping in an
   item description would touch 6,586 facts and an order-to-customer 2,310. After a year it is
   millions, every time somebody thinks of a new thing to look up. Resolved at read time, a new lookup
   answers for data collected months ago the moment it is declared.
3. It would break the fold's skip. `consume` skips a transaction whose source fingerprint has not
   moved. A fact whose contents depend on OTHER facts can be stale while its own source is unchanged,
   so the skip would serve old values - and the fix would be a reverse index from every key to every
   fact using it, which is more machinery than this module.

**The first value learned is true from the beginning.** `valid_from` on a key's first observation is
`BEGINNING`, not the instant it was seen. Without that, a delivery named after its picks would leave
those picks permanently unattributed. On the current data the rule is not yet load-bearing - the route
lookup names a delivery before anyone picks it, so 0 of 1,343 picks were picked before their name
existed - but it costs nothing and it is the difference between a correct report and a blank one the
first time somebody starts a delivery without the route call.

**Stability, measured.** 114 delivery keys, 0 whose customer ever changed. 322 item keys, 0 whose
description ever changed. So the validity periods are insurance rather than daily machinery, and they
exist because history cannot be reconstructed later.

This module is PURE: no database, no clock beyond what it is handed. The store reads and writes rows;
everything about what a lookup MEANS is decided here, so a test can inspect it without a database.
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from datetime import date as date_type, datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

#: How a read-time grouping names a looked-up attribute: `lookup:delivery.customer_name`.
#: A third spelling beside a bare fact column and `attr:` inside the attributes bag, and deliberately
#: distinct from both, because the three resolve in completely different places.
LOOKUP_PREFIX = "lookup:"

#: The `valid_from` of a key's FIRST observed value: "true since this key existed".
#:
#: 1970 rather than year 1, deliberately. Year 1 is the honest spelling of "the beginning", but a
#: `timestamptz` that old converts through local-mean-time offsets (pre-1884 zones are minutes-and-
#: seconds off UTC), so it does not always come back as the value that went in. 1970 predates every
#: warehouse log by decades and has no such hazard.
BEGINNING = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _aware(value: datetime) -> datetime:
    """A datetime as UTC-aware. Defensive: a naive value from any driver must not raise mid-comparison."""
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

#: What a conflicting later value does. `first_wins` suits an attribute that belongs to its key for
#: life, which is every relationship measured so far.
CONFLICT_RULES = ("first_wins", "latest_wins")

#: Where a value came from. `imported` beats `observed`, so a customer list loaded from M3 can later
#: override what was inferred from traffic without a migration. Only `observed` is written today.
ORIGINS = ("observed", "imported")


def is_lookup_path(name: str) -> bool:
    """Whether `name` addresses a looked-up attribute rather than anything on the row."""
    return name.startswith(LOOKUP_PREFIX)


def split_path(name: str) -> tuple[str, str]:
    """`lookup:delivery.customer_name` -> `("delivery", "customer_name")`.

    Split on the FIRST dot, so an attribute may contain one and a lookup name may not. Raises rather
    than returning a half-parsed pair: a malformed path is a caller error, and silently grouping by
    nothing is the failure mode this codebase keeps refusing.
    """
    if not is_lookup_path(name):
        raise ValueError(f"{name!r} is not a lookup path; one starts with {LOOKUP_PREFIX!r}")
    body = name[len(LOOKUP_PREFIX):]
    lookup, dot, attribute = body.partition(".")
    if not dot or not lookup or not attribute:
        raise ValueError(f"{name!r} is not a lookup path; the shape is "
                         f"{LOOKUP_PREFIX}<lookup>.<attribute>")
    return lookup, attribute


def path(lookup: str, attribute: str) -> str:
    return f"{LOOKUP_PREFIX}{lookup}.{attribute}"


@dataclass(frozen=True)
class Source:
    """One place an attribute can be harvested from.

    Both field names are explicit because the spelling genuinely differs per method: a delivery number
    is `DeliveryNumber` on a packing record and `resp.DeliveryNumber` on a routing one. Guessing across
    namespaces is how an approved request field would silently authorise an unapproved response one.
    """

    method: str
    key_field: str
    value_field: str


@dataclass(frozen=True)
class Attribute:
    """One looked-up attribute of a key, and where it comes from."""

    name: str
    sources: tuple[Source, ...]
    #: True when the attribute belongs to its key for life (a delivery's customer). Recorded rather
    #: than acted on: it is what tells a reader whether a second value is a change or a contradiction.
    stable: bool = True
    on_conflict: str = "first_wins"

    def __post_init__(self) -> None:
        if self.on_conflict not in CONFLICT_RULES:
            raise ValueError(f"unknown conflict rule {self.on_conflict!r}; "
                             f"the rules are {', '.join(CONFLICT_RULES)}")


@dataclass(frozen=True)
class Lookup:
    """A declared key-to-attributes relationship. Data, editable from the interface, never code."""

    name: str
    #: The field on the FACT that holds the key. A plain column (`delivery_number`) or an `attr:` path.
    key_field: str
    attributes: tuple[Attribute, ...] = ()

    def attribute(self, name: str) -> Attribute | None:
        return next((a for a in self.attributes if a.name == name), None)

    def sources_by_method(self) -> dict[str, list[tuple[str, Source]]]:
        """`{method: [(attribute name, source), ...]}`, so one pass over facts serves every attribute."""
        out: dict[str, list[tuple[str, Source]]] = {}
        for attribute in self.attributes:
            for source in attribute.sources:
                out.setdefault(source.method, []).append((attribute.name, source))
        return out


def validate(lookup: Lookup, *, fact_fields: Sequence[str],
             known_attributes: Iterable[str] | None = None) -> list[str]:
    """Problems with a declaration, empty when it is registrable.

    A list rather than an exception, for the same reason `definition.validate` returns one: the
    interface shows every problem at once instead of one per save.
    """
    problems: list[str] = []
    if not lookup.name:
        problems.append("a lookup needs a name")
    if "." in lookup.name:
        problems.append(f"{lookup.name!r} may not contain a dot; the dot separates the attribute in "
                        f"{LOOKUP_PREFIX}<lookup>.<attribute>")
    known = set(known_attributes or ())
    key = lookup.key_field
    if key.startswith("attr:"):
        if key[len("attr:"):] not in known:
            problems.append(f"key field {key!r} names an attribute nobody has ticked for capture, so "
                            f"the lookup would silently key on nothing")
    elif key not in fact_fields:
        problems.append(f"key field {key!r} is not a field on the fact row")
    if not lookup.attributes:
        problems.append(f"lookup {lookup.name!r} declares no attributes, so nothing could be looked up")
    for attribute in lookup.attributes:
        if not attribute.sources:
            problems.append(f"attribute {attribute.name!r} has no source, so it could never be filled")
        if "." in attribute.name:
            problems.append(f"attribute {attribute.name!r} may not contain a dot")
    return problems


# ==================================================================== harvesting

@dataclass(frozen=True)
class Observation:
    """One (key, attribute, value) seen on one fact, with when it was seen."""

    lookup: str
    key: str
    attribute: str
    value: str
    at: datetime
    source_method: str


def _text(value: Any) -> str | None:
    """A harvested value as text, or None when it says nothing.

    Empty and whitespace-only are NOTHING, not a value. The live data is full of `""` for fields the
    handheld had no answer for, and storing those would let a blank beat a real name under
    `first_wins`.
    """
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text or None


def _read(row: Mapping[str, Any], name: str) -> Any:
    """One field of a fact row, plain column or attributes key. Mirrors `contract.resolve_field`.

    Kept local rather than imported so this module stays free of the fact contract: a lookup source
    names a key inside `attributes` far more often than a column, and `contract` has no opinion about
    a source's spelling.
    """
    if name.startswith("attr:"):
        name = name[len("attr:"):]
    attributes = row.get("attributes")
    if isinstance(attributes, dict) and name in attributes:
        return attributes[name]
    return row.get(name)


def harvest(rows: Iterable[Mapping[str, Any]], lookups: Iterable[Lookup]) -> list[Observation]:
    """Every (key, attribute, value) pair the given facts can supply.

    Called with the rows the fold already holds, so it costs one pass over memory and no query. A row
    is only consulted for the attributes whose source names its method, which is why the sources are
    indexed by method first.
    """
    indexed = [(lookup, lookup.sources_by_method()) for lookup in lookups]
    out: list[Observation] = []
    for row in rows:
        method = row.get("method")
        at = row.get("event_time")
        if at is None:
            continue
        for lookup, by_method in indexed:
            for attribute_name, source in by_method.get(method, ()):
                key = _text(_read(row, source.key_field))
                value = _text(_read(row, source.value_field))
                if key is None or value is None:
                    continue
                out.append(Observation(lookup=lookup.name, key=key, attribute=attribute_name,
                                       value=value, at=at, source_method=method))
    return out


# ==================================================================== resolving

@dataclass(frozen=True)
class Period:
    """One value of one attribute, and the span it was true for. `valid_to` None means still current."""

    value: str
    valid_from: datetime
    valid_to: datetime | None = None

    def covers(self, at: datetime) -> bool:
        at = _aware(at)
        if at < _aware(self.valid_from):
            return False
        return self.valid_to is None or at < _aware(self.valid_to)


def _as_datetime(at: Any) -> datetime | None:
    """A bucket label as an instant. Hourly buckets are datetimes; daily and monthly ones are dates."""
    if isinstance(at, datetime):
        return at if at.tzinfo else at.replace(tzinfo=timezone.utc)
    if isinstance(at, date_type):
        return datetime(at.year, at.month, at.day, tzinfo=timezone.utc)
    return None


@dataclass
class Resolver:
    """Key to value, as at an instant. Built from the rows of one answer, never from the whole table."""

    periods: dict[tuple[str, str, str], list[Period]] = dc_field(default_factory=dict)

    def add(self, lookup: str, key: str, attribute: str, period: Period) -> None:
        self.periods.setdefault((lookup, key, attribute), []).append(period)

    def value(self, lookup: str, key: str | None, attribute: str, at: Any) -> str | None:
        """The attribute of `key` as at `at`, or None when nothing is known.

        None rather than a placeholder, so an unknown key reads as "not known" through the same path
        that a fact with no key at all does. Reporting a blank or a zero for either would be the
        "absent is never zero" rule broken at the last step.
        """
        if key is None:
            return None
        periods = self.periods.get((lookup, key, attribute))
        if not periods:
            return None
        when = _as_datetime(at)
        if when is None:
            return periods[0].value
        for period in sorted(periods, key=lambda p: _aware(p.valid_from), reverse=True):
            if period.covers(when):
                return period.value
        return None


@dataclass(frozen=True)
class Plan:
    """How a requested grouping maps onto one the stored tier can answer.

    `stored_group_by` is what the rollups are read by: every lookup path replaced by its key field.
    `steps` is one entry per requested position, either None (pass the value through) or the
    (lookup, attribute) to translate it into.
    """

    requested: tuple[str, ...]
    stored_group_by: tuple[str, ...]
    steps: tuple[tuple[str, str] | None, ...]

    @property
    def translates(self) -> bool:
        return any(step is not None for step in self.steps)

    def keys_needed(self, points: Mapping[tuple, Any]) -> dict[str, set[str]]:
        """`{lookup name: {key, ...}}` present in an answer, so the store loads only what it needs."""
        needed: dict[str, set[str]] = {}
        for (_bucket, dims) in points:
            for index, step in enumerate(self.steps):
                if step is None or index >= len(dims):
                    continue
                value = dims[index]
                if value is not None:
                    needed.setdefault(step[0], set()).add(str(value))
        return needed


def plan(group_by: Sequence[str], lookups: Mapping[str, Lookup]) -> Plan:
    """Turn a requested grouping into one the stored tier can answer, plus the translation back.

    A lookup path costs the slot its KEY occupies, which is the price of resolving at read time. It is
    a cheap price: delivery number alone compresses the tenant's facts 39-fold at day grain, and the
    same rollup then answers both "by delivery" and "by customer".
    """
    stored: list[str] = []
    steps: list[tuple[str, str] | None] = []
    for name in group_by:
        if not is_lookup_path(name):
            stored.append(name)
            steps.append(None)
            continue
        lookup_name, attribute = split_path(name)
        lookup = lookups.get(lookup_name)
        if lookup is None:
            raise ValueError(f"{name!r} names lookup {lookup_name!r}, which is not declared")
        if lookup.attribute(attribute) is None:
            raise ValueError(f"lookup {lookup_name!r} declares no attribute {attribute!r}")
        stored.append(lookup.key_field)
        steps.append((lookup_name, attribute))
    return Plan(requested=tuple(group_by), stored_group_by=tuple(stored), steps=tuple(steps))


def translate(points: Mapping[tuple, dict], plan_: Plan, resolver: Resolver, *, merge) -> dict:
    """Re-key an answer from stored keys to looked-up values, merging groups that now coincide.

    Exact for every role a rollup can hold, which is the property that makes read-time resolution
    viable at all: sums and counts add, minimums and maximums take extremes, sketches union and
    histogram bands add. `merge` is `definition.add_roles`, passed in so this module stays free of the
    definition model.

    Several keys usually map to one value - 114 deliveries to 69 customers on the live tenant - so the
    merge is the normal path, not an edge case.
    """
    if not plan_.translates:
        return dict(points)
    out: dict = {}
    for (bucket, dims), roles in points.items():
        translated = []
        for index, step in enumerate(plan_.steps):
            value = dims[index] if index < len(dims) else None
            if step is None:
                translated.append(value)
            else:
                lookup_name, attribute = step
                translated.append(resolver.value(lookup_name, None if value is None else str(value),
                                                 attribute, bucket))
        key = (bucket, tuple(translated))
        out[key] = merge(out.get(key, {}), roles)
    return out
