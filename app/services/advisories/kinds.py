"""What kind of thing an advisory is, and what it is about, in the words the tab shows (stage D,
step 17). The agents name a kind when they open a finding; the mirror lists each kind with its
label and its one-sentence meaning, so the edge groups the feed without parsing text, and a new
agent brings its words here. Used by the agents, the store, the mirror and the migration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

OTHER = "other"


@dataclass(frozen=True)
class KindSpec:
    """One kind of finding: its code, the words on the group, and what it means to a reader."""

    kind: str
    label: str
    meaning: str


KINDS: tuple[KindSpec, ...] = (
    KindSpec("count_gap", "Stock count gaps",
             "A count found a different amount from what the system expects."),
    KindSpec("chronic_short", "Items short this week",
             "The same item was short on several releases this week."),
    KindSpec("empty_location", "Empty locations",
             "Pickers found this location empty more than once today."),
    KindSpec("delivery_at_risk", "Deliveries at risk",
             "This delivery will likely leave lines behind unless something changes."),
    KindSpec(OTHER, "Other findings", "A finding that fits no group above."),
)

_BY_KIND = {k.kind: k for k in KINDS}
#: The kind of each `source.kind` the agents wrote before step 17 (the migration's backfill).
_FROM_SOURCE = {"count": "count_gap", "item": "chronic_short", "location": "empty_location",
                "delivery": "delivery_at_risk"}


def spec_of(kind: str | None) -> KindSpec:
    """The words of a kind; an unknown code reads as "other" rather than failing the row.

    >>> spec_of("count_gap").label, spec_of("nope").kind
    ('Stock count gaps', 'other')
    """
    return _BY_KIND.get(kind or OTHER, _BY_KIND[OTHER])


def kind_from_source(source: Mapping[str, Any] | None) -> str:
    """The kind an older row's `source.kind` means.

    >>> kind_from_source({"kind": "count"}), kind_from_source({"kind": "delivery"}), kind_from_source({})
    ('count_gap', 'delivery_at_risk', 'other')
    """
    return _FROM_SOURCE.get(str((source or {}).get("kind") or ""), OTHER)


def area_of(location: str | None) -> str:
    """The area a location code belongs to: its leading letters, a presentation convention for
    grouping ("M02C" and "M01B" sit together); a code with no leading letters is its own area.

    >>> area_of("M02C"), area_of("A-03-04"), area_of("JIT"), area_of("0712"), area_of(None)
    ('M', 'A', 'JIT', '0712', '')
    """
    code = (location or "").strip()
    letters = ""
    for ch in code:
        if ch.isalpha():
            letters += ch
        else:
            break
    return letters or code


def about_item(item: str, description: str | None = None, *, where: str | None = None) -> dict:
    """What an item finding is about, named in words when the description is known.

    >>> about_item("104568", "Ginger", where="A-01-02")
    {'kind': 'item', 'id': '104568', 'label': 'Ginger (104568)', 'where': 'A-01-02', 'area': 'A'}
    >>> about_item("104568")
    {'kind': 'item', 'id': '104568', 'label': 'Item 104568', 'where': None, 'area': ''}
    """
    label = f"{description} ({item})" if description else f"Item {item}"
    return {"kind": "item", "id": str(item), "label": label, "where": where, "area": area_of(where)}


def about_location(location: str, *, is_staging: bool = False) -> dict:
    """What a location finding is about.

    >>> about_location("A-03-04")
    {'kind': 'location', 'id': 'A-03-04', 'label': 'Location A-03-04', 'where': None, 'area': 'A'}
    >>> about_location("JIT", is_staging=True)["label"]
    'JIT staging'
    """
    label = f"{location} staging" if is_staging else f"Location {location}"
    return {"kind": "location", "id": str(location), "label": label, "where": None, "area": area_of(location)}


def about_delivery(delivery_number: str, route: str | None, customer: str | None = None) -> dict:
    """What a delivery finding is about: the route in words, the customer as where it goes.

    >>> about_delivery("D-1", "R14", "Hove Deli")
    {'kind': 'delivery', 'id': 'D-1', 'label': 'Route R14', 'where': 'Hove Deli', 'area': ''}
    """
    return {"kind": "delivery", "id": str(delivery_number), "label": f"Route {route}" if route else str(delivery_number),
            "where": customer, "area": ""}


def about_from_source(source: Mapping[str, Any] | None) -> dict:
    """What an older row is about, read from its `source` (the migration's backfill).

    >>> about_from_source({"kind": "count", "item": "1", "location": "M02C"})["area"]
    'M'
    >>> about_from_source({"kind": "delivery", "delivery_number": "D-1", "route": "R14"})["label"]
    'Route R14'
    >>> about_from_source({})
    {}
    """
    src = dict(source or {})
    kind = str(src.get("kind") or "")
    if kind in ("count", "item") and src.get("item"):
        return about_item(str(src["item"]), where=src.get("location"))
    if kind == "location" and src.get("location"):
        return about_location(str(src["location"]), is_staging=str(src["location"]) == "JIT")
    if kind == "delivery" and src.get("delivery_number"):
        return about_delivery(str(src["delivery_number"]), src.get("route"))
    return {}


def kinds_payload(rows: Iterable[Any]) -> list[dict]:
    """The kinds with a live row, in the registry's order, each with its words and open counts.

    (rows: 3 open count_gap of which 2 high, 1 acknowledged empty_location)
    -> [{kind: count_gap, label, meaning, open: 3, high: 2}, {kind: empty_location, ..., open: 0, high: 0}]
    """
    seen: dict[str, dict] = {}
    for row in rows:
        kind = spec_of(getattr(row, "kind", None)).kind
        entry = seen.setdefault(kind, {"open": 0, "high": 0})
        if getattr(row, "status", "open") == "open":
            entry["open"] += 1
            entry["high"] += int(getattr(row, "severity", "") == "high")
    return [{"kind": k.kind, "label": k.label, "meaning": k.meaning, **seen[k.kind]} for k in KINDS if k.kind in seen]
