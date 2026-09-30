"""Chunk 116: settling many call rows into one row per key.

Every metric is folded from the call rows, and one class of question cannot be answered from them. A
pick-list release is confirmed in several calls and its expected quantity is stamped on every one, so
adding it across calls gives a number nothing ever expected. Measured on tmp-live on 18 September
2026: release 540551 is one successful pick of 9 followed by eight attempts to confirm the last unit,
every one refused by M3 and every one still carrying `QuantityPicked = 1`. Summed, it picked 17. Nine
units moved. Across 6,160 releases the shortfall reads -5,576 summed every call and -4,344 settled.

A SETTLEMENT is a named rule for turning the rows that share a key into one row:

- which method's rows it reads;
- the KEY, one or more fields, whose distinct values become the rows;
- which fields to CARRY across, copied once from the first call that has a value, so the settled
  row has the shape of a fact row and every existing group-by works on it;
- which VALUES to settle, each with a rule and an optional row filter.

The rules are the vocabulary the measures already have, plus `first` and `last`, plus two that read
other settled values rather than calls: `difference` and `flag`.

This module is PURE. It has no database and never learns where rows come from or go to, exactly as
`definition.py` and `lookup.py` do not. Nothing about picking is coded into it; picking is the first
thing it is used for.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping

from app.services.analytics import contract


class Rule(str, enum.Enum):
    """How one settled value is made from a key's calls.

    `first` and `last` order by the row's own `event_time`, never by arrival: a fold reads whatever
    the query returns and a rebuild hands rows back in any order. `difference` and `flag` read two
    settled values that were computed BEFORE them in the settlement, never the calls.
    """

    first = "first"
    last = "last"
    # Chunk 129: the first value AS TEXT, for codes (a location) that are neither numbers nor times.
    first_text = "first_text"
    sum = "sum"
    count = "count"
    min = "min"
    max = "max"
    distinct_count = "distinct_count"
    difference = "difference"
    flag = "flag"
    # Chunk 129: a value looked up by a key the row already has (a carried field or an earlier value).
    lookup = "lookup"
    # Chunk 129: other calls of the SAME user on the SAME item (the `match` fields) in `methods`,
    # from `window_s` seconds before the release's first call up to its last call. On tmp-live a
    # picker checks a location on the handheld seconds before confirming a pick; these rules attach
    # those checks to the release without either side carrying a key the other has.
    nearby_count = "nearby_count"
    nearby_distinct = "nearby_distinct"
    nearby_last = "nearby_last"
    nearby_has = "nearby_has"


#: Rules that read a field from every qualifying call.
READS_A_FIELD = frozenset({Rule.first, Rule.last, Rule.first_text, Rule.sum, Rule.min, Rule.max,
                           Rule.distinct_count})
#: Rules that read two earlier settled values instead.
READS_SETTLED = frozenset({Rule.difference, Rule.flag})
#: Rules that read the context calls of other methods.
NEARBY = frozenset({Rule.nearby_count, Rule.nearby_distinct, Rule.nearby_last, Rule.nearby_has})
#: Nearby rules that read a field of the context calls.
NEARBY_WITH_FIELD = frozenset({Rule.nearby_distinct, Rule.nearby_last, Rule.nearby_has})
#: How a `lookup` rule names what it looks up: `lookup:<lookup>.<attribute>`, as a grouping does.
LOOKUP_PREFIX = "lookup:"
#: Comparisons a `flag` may make.
FLAG_OPS = ("<", "<=", "==", "!=", ">=", ">")


@dataclass(frozen=True)
class Settled:
    """One value on the settled row and the rule that makes it."""

    name: str
    rule: Rule
    #: For a rule that reads calls: a fact column (`event_time`, `lot_number`) or an `attr:` path.
    field: str | None = None
    #: Row filters, the same two every measure already has. Empty means every call.
    statuses: frozenset = dc_field(default_factory=frozenset)
    only: frozenset = dc_field(default_factory=frozenset)
    #: For `difference` and `flag`: names of earlier settled values.
    left: str | None = None
    right: str | None = None
    #: For `flag`: the comparison and, when comparing to a constant, the constant.
    op: str | None = None
    right_value: Decimal | None = None
    #: For the `nearby_*` rules: which methods' calls, which fields must be equal between them and
    #: the release's first call, and how many seconds before that first call to look.
    methods: tuple[str, ...] = ()
    match: tuple[str, ...] = ()
    window_s: int | None = None


@dataclass(frozen=True)
class Settlement:
    name: str
    #: M3 methods whose rows are read. Every other row is ignored.
    reads: tuple[str, ...]
    #: Fields whose distinct combination is one settled row.
    key: tuple[str, ...]
    #: Fields copied onto the row once, from the first call that carries a non-empty value.
    carry: tuple[str, ...]
    values: tuple[Settled, ...]


@dataclass
class SettledRow:
    """One row per key. `carried` and `values` both land in the row's attribute bag when stored, so
    their names must not collide; `validate` refuses that."""

    key: tuple[str, ...]
    #: When the key was first seen, so a settled row can be placed in time like a fact row.
    event_time: datetime | None
    carried: dict[str, Any]
    values: dict[str, Any]
    calls: int


# ============================================================== reading one call

def _read(row: Mapping[str, Any], name: str) -> Any:
    """A field off a call row: a typed column or an `attr:` path. Empty string is absent."""
    value = contract.resolve_field(row, name) if contract.is_attr_path(name) else row.get(name)
    if value == "":
        return None
    return value


def _carry_name(name: str) -> str:
    """How a carried field is spelled on the settled row: `attr:OrderLine` becomes `OrderLine`, a
    typed column keeps its name. Both then live in one attribute bag."""
    return contract.attr_key(name) if contract.is_attr_path(name) else name


def _qualifies(row: Mapping[str, Any], settled: Settled) -> bool:
    if settled.only and row.get("quantity_classification") not in settled.only:
        return False
    if settled.statuses and row.get("status") not in settled.statuses:
        return False
    return True


def _numeric_or_time(value: Any, tz: tzinfo | None) -> Any:
    """`min`/`max`/`first`/`last` may read a time as readily as a number; `sum` needs a number.

    A time may arrive as the row's own `event_time`, or as a STRING an attribute carries. M3 stamps
    `StartDateTime` on every ConfirmPickLine call as `2026-09-18 06:42:34.003`: the handheld's clock,
    in the tenant's zone, with no zone written. `tz` is that zone. A number is tried first, so a
    bare M3 date such as `20260918` stays the number it is."""
    if isinstance(value, datetime):
        return _aware(value, tz)
    number = contract.numeric_or_none(value)
    if number is not None:
        return number
    return _time_or_none(value, tz)


def _text_or_none(value: Any) -> str | None:
    """`first_text` (chunk 129): the value as written, for codes such as a location `A03A` that are
    neither a number nor a time and that `first` therefore reads as nothing."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _time_or_none(value: Any, tz: tzinfo | None) -> datetime | None:
    if not isinstance(value, str) or len(value) < 10 or value[4] != "-":
        return None
    try:
        return _aware(datetime.fromisoformat(value.strip()), tz)
    except ValueError:
        return None


def _aware(when: datetime, tz: tzinfo | None) -> datetime:
    return when if when.tzinfo is not None else when.replace(tzinfo=tz or timezone.utc)


def _subtract(a: Any, b: Any) -> Decimal | None:
    """Two numbers give a number. Two times give the SECONDS between them, exact to the microsecond,
    so `last confirm minus first start` is how long a release took. Anything else is unknown."""
    if isinstance(a, Decimal) and isinstance(b, Decimal):
        return a - b
    if isinstance(a, datetime) and isinstance(b, datetime):
        gap: timedelta = a - b
        return Decimal(gap.days * 86400 + gap.seconds) + Decimal(gap.microseconds) / Decimal(1_000_000)
    return None


# ============================================================== settling one key

def _settle_key(rows: list[Mapping[str, Any]], settlement: Settlement, tz: tzinfo | None,
                context: Iterable[Mapping[str, Any]] | None = None,
                resolve: "Resolve | None" = None) -> SettledRow:
    ordered = sorted(rows, key=lambda r: (r.get("event_time") is None, r.get("event_time")))
    first_at = next((r["event_time"] for r in ordered if r.get("event_time") is not None), None)

    carried: dict[str, Any] = {}
    for name in settlement.carry:
        for r in ordered:
            v = _read(r, name)
            if v is not None:
                carried[_carry_name(name)] = v
                break

    values: dict[str, Any] = {}
    for s in settlement.values:
        qualifying = [r for r in ordered if _qualifies(r, s)]
        if s.rule in READS_A_FIELD:
            assert s.field is not None
            seen = [(r, _read(r, s.field)) for r in qualifying]
            present = [(r, v) for r, v in seen if v is not None]
            if s.rule is Rule.first:
                values[s.name] = _numeric_or_time(present[0][1], tz) if present else None
            elif s.rule is Rule.last:
                values[s.name] = _numeric_or_time(present[-1][1], tz) if present else None
            elif s.rule is Rule.first_text:
                values[s.name] = _text_or_none(present[0][1]) if present else None
            elif s.rule is Rule.distinct_count:
                values[s.name] = len({str(v) for _, v in present})
            else:
                nums = [x for x in (_numeric_or_time(v, tz) for _, v in present) if x is not None]
                if s.rule is Rule.sum:
                    # Zero is a real answer here: the release picked nothing. Unlike `first`, a sum
                    # over no qualifying rows is not "unknown", it is nought.
                    values[s.name] = sum((n for n in nums if isinstance(n, Decimal)), Decimal(0))
                elif s.rule is Rule.min:
                    values[s.name] = min(nums) if nums else None
                else:
                    values[s.name] = max(nums) if nums else None
        elif s.rule is Rule.count:
            values[s.name] = len(qualifying)
        elif s.rule is Rule.difference:
            values[s.name] = _subtract(values.get(s.left), values.get(s.right))
        elif s.rule is Rule.flag:
            a = values.get(s.left)
            b = values.get(s.right) if s.right else s.right_value
            values[s.name] = _compare(a, s.op, b)
        elif s.rule is Rule.lookup:
            values[s.name] = _looked_up(s, _key_for(s.left, ordered, carried, values), resolve)
        elif s.rule in NEARBY:
            values[s.name] = _nearby(s, ordered, context, values, tz)
    return SettledRow(key=tuple(), event_time=first_at, carried=carried, values=values, calls=len(rows))


#: `(lookup, attribute, key) -> value or None`. Handed in by the store, so this module stays pure.
Resolve = Callable[[str, str, str], Any]


def _key_for(name: str | None, ordered: list[Mapping[str, Any]], carried: dict[str, Any],
             values: dict[str, Any]) -> str | None:
    """The key a `lookup` rule reads: an earlier settled value, a carried field, or a call field."""
    if not name:
        return None
    for source in (values, carried):
        if name in source:
            v = source[name]
            return None if v is None or v == "" else str(v)
    plain = _carry_name(name)
    if plain in carried:
        v = carried[plain]
        return None if v is None or v == "" else str(v)
    for r in ordered:
        v = _read(r, name)
        if v is not None:
            return str(v)
    return None


def _looked_up(s: Settled, key: str | None, resolve: Resolve | None) -> Any:
    if key is None or resolve is None or not s.field or not s.field.startswith(LOOKUP_PREFIX):
        return None
    lookup, _dot, attribute = s.field[len(LOOKUP_PREFIX):].partition(".")
    value = resolve(lookup, attribute, key)
    return None if value is None or value == "" else value


def _nearby(s: Settled, ordered: list[Mapping[str, Any]], context: Iterable[Mapping[str, Any]] | None,
            values: dict[str, Any], tz: tzinfo | None) -> Any:
    """The `nearby_*` rules over the context calls that belong to this release."""
    timed = [r for r in ordered if r.get("event_time") is not None]
    rows: list[Mapping[str, Any]] = []
    if timed and context and s.window_s:
        first, last = timed[0], timed[-1]
        lo = _aware(first["event_time"], tz) - timedelta(seconds=s.window_s)
        hi = _aware(last["event_time"], tz)
        wanted = tuple(_read(first, m) for m in s.match)
        if all(w is not None for w in wanted):
            for r in context:
                at = r.get("event_time")
                if r.get("method") not in s.methods or at is None or not _qualifies(r, s):
                    continue
                if not lo <= _aware(at, tz) <= hi:
                    continue
                if tuple(_read(r, m) for m in s.match) != wanted:
                    continue
                rows.append(r)
    rows.sort(key=lambda r: r["event_time"])
    if s.rule is Rule.nearby_count:
        return len(rows)
    seen = [str(v) for v in (_read(r, s.field) for r in rows) if v is not None]
    if s.rule is Rule.nearby_distinct:
        return len(set(seen))
    if s.rule is Rule.nearby_last:
        return seen[-1] if seen else None
    target = values.get(s.left) if s.left else None   # nearby_has
    if target is None:
        return None
    return 1 if str(target) in seen else 0


def _compare(a: Any, op: str | None, b: Any) -> int | None:
    if a is None or b is None:
        return None
    try:
        ops = {"<": a < b, "<=": a <= b, "==": a == b, "!=": a != b, ">=": a >= b, ">": a > b}
    except TypeError:
        # A time against a number. Unknown, not a crash inside the fold's transaction.
        return None
    return 1 if ops[op] else 0


def settle(rows: Iterable[Mapping[str, Any]], settlement: Settlement, *,
           tz: tzinfo | None = None, context: Iterable[Mapping[str, Any]] | None = None,
           resolve: "Resolve | None" = None) -> dict[tuple[str, ...], SettledRow]:
    """One settled row per distinct key among `rows`, ignoring rows from other methods and rows with
    no key. Pure: give it the same rows in any order and it returns the same answer. `tz` is the
    tenant's zone, used only to read a time an attribute carries as a zoneless string.

    Chunk 129: `context` is the calls of OTHER methods the `nearby_*` rules may read, and `resolve`
    answers the `lookup` rules. Both absent reproduce the pre-129 behaviour exactly: the new rules
    then settle to nothing (a count to 0), and no existing rule reads either."""
    by_key: dict[tuple[str, ...], list[Mapping[str, Any]]] = {}
    for row in rows:
        if row.get("method") not in settlement.reads:
            continue
        parts = tuple(_read(row, k) for k in settlement.key)
        if any(p is None for p in parts):
            continue
        by_key.setdefault(tuple(str(p) for p in parts), []).append(row)
    out: dict[tuple[str, ...], SettledRow] = {}
    for key, group in by_key.items():
        settled = _settle_key(group, settlement, tz, context, resolve)
        settled.key = key
        out[key] = settled
    return out


# ============================================================== validation

def validate(settlement: Settlement) -> list[str]:
    """Problems with a settlement, empty when it is usable. A list rather than an exception, so a
    screen can show every problem at once."""
    problems: list[str] = []
    if not settlement.name or not settlement.name.strip():
        problems.append("a settlement needs a name")
    if not settlement.reads:
        problems.append("a settlement must say which method it reads")
    if not settlement.key:
        problems.append("a settlement needs a key: the field or fields whose values become the rows")

    seen: set[str] = set()
    carried_names = {_carry_name(c) for c in settlement.carry}
    for s in settlement.values:
        if s.name in seen:
            problems.append(f"settled value {s.name!r} is defined twice")
        seen.add(s.name)
        if s.name in carried_names:
            problems.append(f"settled value {s.name!r} has the same name as a carried field; both "
                            f"land on the row and one would overwrite the other")
        if s.rule in READS_A_FIELD and not s.field:
            problems.append(f"settled value {s.name!r} is a {s.rule.value} and must name a field")
        if s.rule is Rule.count and s.field:
            problems.append(f"settled value {s.name!r} is a count, which reads no field, but names one")
        if s.rule in READS_SETTLED:
            for side, ref in (("left", s.left), ("right", s.right)):
                if s.rule is Rule.flag and side == "right" and ref is None:
                    if s.right_value is None:
                        problems.append(f"settled value {s.name!r} compares against nothing")
                    continue
                if ref is None:
                    problems.append(f"settled value {s.name!r} needs a {side}-hand settled value")
                elif ref not in seen or ref == s.name:
                    problems.append(f"settled value {s.name!r} reads {ref!r}, which is not a settled "
                                    f"value defined before it")
            if s.rule is Rule.flag and s.op not in FLAG_OPS:
                problems.append(f"settled value {s.name!r} uses comparison {s.op!r}; it must be one of "
                                f"{', '.join(FLAG_OPS)}")
        if s.rule is Rule.lookup:
            if not s.field or not s.field.startswith(LOOKUP_PREFIX) or "." not in s.field[len(LOOKUP_PREFIX):]:
                problems.append(f"settled value {s.name!r} is a lookup and must name a lookup path, "
                                f"{LOOKUP_PREFIX}<lookup>.<attribute>")
            if not s.left:
                problems.append(f"settled value {s.name!r} needs a left-hand field: the key to look up")
        if s.rule in NEARBY:
            if not s.methods:
                problems.append(f"settled value {s.name!r} must say which methods' calls it reads")
            if not s.match:
                problems.append(f"settled value {s.name!r} must name the fields a nearby call must match, "
                                f"for example user_name and item_number")
            if not s.window_s or s.window_s <= 0:
                problems.append(f"settled value {s.name!r} needs a window in seconds greater than 0")
            if s.rule in NEARBY_WITH_FIELD and not s.field:
                problems.append(f"settled value {s.name!r} is a {s.rule.value} and must name a field")
            if s.rule is Rule.nearby_has:
                if not s.left:
                    problems.append(f"settled value {s.name!r} needs a left-hand settled value to look for")
                elif s.left not in seen or s.left == s.name:
                    problems.append(f"settled value {s.name!r} reads {s.left!r}, which is not a settled "
                                    f"value defined before it")
    return problems
