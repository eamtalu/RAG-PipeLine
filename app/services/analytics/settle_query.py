"""Chunk 120: the small query language a settlement's rows can be asked in.

The grouped read could sum and count, and nothing else. A page that shows "the zero-picks, newest
first" or "median seconds per picker" or "lines per hour" had to pull every row down and compute in
the browser: about 7 MB a week. These five additions make those one call each:

- FILTERS: `where=is_short==1`, `where=picked==0`, `where=duration_s>300`, `where=user_name==BCHAM`.
  A numeric value compares as a number, a text value as text, a time as a time.
- STATS: `stat=median:duration_s`, `stat=p95:duration_s`, `stat=mean:duration_s`,
  `stat=min:...`, `stat=max:...`, and `stat=distinct:user_name` for "how many different".
- TIME BUCKETS in the grouping, in the tenant's zone: `hour` (0-23), `hour_start`, `day`, `week`.
- SORT on the list, numeric when the field is a number, with a direction.
- validation that names a bad field or operator before any SQL runs, so the message is usable.

This module is PURE: it parses and validates. The store turns the result into SQL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.services.analytics import contract
from app.services.analytics import settle as st

OPS = ("==", "!=", "<=", ">=", "<", ">")
STAT_KINDS = ("median", "p90", "p95", "p99", "mean", "min", "max", "distinct")
BUCKETS = ("hour", "hour_start", "day", "week")
TYPED = ("method", "transaction_name", "warehouse", "item_number", "delivery_number", "lot_number",
         "user_name")

_FILTER = re.compile(r"^\s*(?P<field>[^<>=!]+?)\s*(?P<op>==|!=|<=|>=|<|>)\s*(?P<value>.*?)\s*$")
_STAT = re.compile(r"^\s*(?P<kind>[a-z0-9]+)\s*:\s*(?P<field>.+?)\s*$")


@dataclass(frozen=True)
class Filter:
    field: str
    op: str
    value: str


@dataclass(frozen=True)
class Stat:
    kind: str
    field: str

    @property
    def label(self) -> str:
        """How the answer is named on a group: `median_duration_s`, `distinct_user_name`."""
        return f"{self.kind}_{plain(self.field)}"


def plain(name: str) -> str:
    """`attr:OrderLine` is stored as `OrderLine`; every other name is itself."""
    return contract.attr_key(name) if contract.is_attr_path(name) else name


def parse_filter(text: str) -> Filter:
    m = _FILTER.match(text or "")
    if not m or not m.group("field"):
        raise ValueError(f"{text!r} is not a filter; write it as field, comparison, value: is_short==1")
    return Filter(field=m.group("field"), op=m.group("op"), value=m.group("value"))


def parse_stat(text: str) -> Stat:
    m = _STAT.match(text or "")
    if not m:
        raise ValueError(f"{text!r} is not a stat; write it as kind:field, for example median:duration_s")
    kind = m.group("kind")
    if kind not in STAT_KINDS:
        raise ValueError(f"{kind!r} is not a stat; use one of {', '.join(STAT_KINDS)}")
    return Stat(kind=kind, field=m.group("field"))


def known_fields(settlement: st.Settlement) -> set[str]:
    """Every name a filter, stat or sort may use on this settlement's rows."""
    out = {"key", "event_time", "business_date", "calls", *TYPED}
    out.update(plain(c) for c in settlement.carry)
    out.update(v.name for v in settlement.values)
    return out


def validate(settlement: st.Settlement, *, filters: tuple[Filter, ...] = (), stats: tuple[Stat, ...] = (),
             group_by: tuple[str, ...] = (), sort: str | None = None) -> list[str]:
    """Problems, in words, empty when the question can be asked. A list so a screen or an agent
    sees every problem at once rather than the first."""
    known = known_fields(settlement)
    problems: list[str] = []
    for f in filters:
        if plain(f.field) not in known:
            problems.append(f"filter field {f.field!r} is not on the settled row; "
                            f"fields are {', '.join(sorted(known))}")
        if f.op not in OPS:
            problems.append(f"filter {f.field!r} uses {f.op!r}; comparisons are {', '.join(OPS)}")
    for s in stats:
        if plain(s.field) not in known:
            problems.append(f"stat field {s.field!r} is not on the settled row")
        if s.kind != "distinct" and plain(s.field) in ("key", *TYPED):
            problems.append(f"{s.kind} of {s.field!r} makes no sense: it is text; only distinct does")
    for g in group_by:
        if g in BUCKETS or g in ("key",) or plain(g) in known:
            continue
        problems.append(f"group field {g!r} is not on the settled row and is not one of "
                        f"{', '.join(BUCKETS)}")
    if sort is not None and plain(sort) not in known:
        problems.append(f"sort field {sort!r} is not on the settled row")
    return problems
