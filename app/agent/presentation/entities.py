"""The watchable things an answer was built on (stage D, step 14c): deliveries, routes, items,
locations and zones read straight from the data tools' results in the trace, never parsed out of
the answer text. The page draws each as a chip with the eye toggle; the consumer marks which are
already watched. Pure; blocks.py adds the block to the answer.
"""

from __future__ import annotations

import json
from typing import Any

MAX_ITEMS = 6
#: The group_by names of aggregate_releases that name a watchable thing
GROUP_KINDS = {"item_number": "product", "from_location": "location", "designated_location": "location",
               "from_zone": "zone", "designated_zone": "zone", "route": "route", "delivery_number": "delivery"}
ITEM_NAME_GROUP = "lookup:item description.ItemDescription"
AT_RISK_TOOLS = {"at_risk_history", "at_risk_board", "at_risk_vans", "explain_at_risk_delivery"}


class _Items:
    """The entities in order of first appearance, one per kind and id."""

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}

    def add(self, kind: str, entity_id: Any, label: str | None = None) -> None:
        key = str(entity_id or "").strip()
        if not key or key in ("None", "–", "?") or (kind, key) in self.items:
            return
        self.items[(kind, key)] = {"kind": kind, "id": key, "label": (label or key).strip()[:200]}


def _parsed(call: dict) -> dict | None:
    raw = call.get("result")
    if not isinstance(raw, str) or raw.lstrip().startswith(('{"error"', '{"problems"')):
        return None
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _from_at_risk(result: dict, items: _Items) -> None:
    for row in result.get("rows") or []:
        if not isinstance(row, dict):
            continue
        customer = row.get("customer")
        items.add("delivery", row.get("delivery"), f"{row.get('delivery')} · {customer}" if customer else None)
        items.add("route", row.get("route"), f"Route {row.get('route')}")
    for van in result.get("vans") or []:
        if isinstance(van, dict):
            items.add("route", van.get("route"), f"Route {van.get('route')}")
    if result.get("delivery"):
        customer = result.get("customer")
        items.add("delivery", result["delivery"], f"{result['delivery']} · {customer}" if customer else None)
    if result.get("route"):
        items.add("route", result["route"], f"Route {result['route']}")


def _from_releases(result: dict, items: _Items) -> None:
    for row in result.get("rows") or []:
        if not isinstance(row, dict):
            continue
        looked = row.get("looked_up") or {}
        attributes = row.get("attributes") or {}
        if row.get("delivery_number"):
            customer = looked.get("delivery.customer_name")
            items.add("delivery", row["delivery_number"], f"{row['delivery_number']} · {customer}" if customer else None)
        if row.get("item_number"):
            name = looked.get("item description.ItemDescription")
            items.add("product", row["item_number"], f"{name} · {row['item_number']}" if name else None)
        location = row.get("from_location") or attributes.get("FromLocation")
        if location:
            items.add("location", location, f"Location {location}")


def _from_aggregate(result: dict, items: _Items) -> None:
    group_by = [str(g) for g in (result.get("group_by") or [])]
    if not group_by:
        return
    name_at = group_by.index(ITEM_NAME_GROUP) if ITEM_NAME_GROUP in group_by else None
    for row in result.get("rows") or []:
        dims = row.get("dimensions") if isinstance(row, dict) else None
        if not isinstance(dims, list):
            continue
        for i, group in enumerate(group_by):
            kind = GROUP_KINDS.get(group)
            if kind is None or i >= len(dims) or dims[i] in (None, ""):
                continue
            label = None
            if kind == "product" and name_at is not None and name_at < len(dims) and dims[name_at]:
                label = f"{dims[name_at]} · {dims[i]}"
            elif kind == "route":
                label = f"Route {dims[i]}"
            elif kind in ("location", "zone"):
                label = f"{kind.capitalize()} {dims[i]}"
            items.add(kind, dims[i], label)


def _from_advisory(result: dict, items: _Items) -> None:
    source = ((result.get("advisory") or {}).get("source")) or {}
    kind = source.get("kind")
    if kind == "delivery" and source.get("delivery_number"):
        items.add("delivery", source["delivery_number"])
    elif kind in ("count", "item") and source.get("item"):
        items.add("product", source["item"])
    elif kind == "location" and source.get("location"):
        items.add("location", source["location"], f"Location {source['location']}")


def entities_from_trace(trace: list[dict]) -> dict | None:
    """The entities block for a trace, or None when no data call named a watchable thing.

    >>> trace = [{"tool": "at_risk_board", "input": {}, "result": '{"rows": [{"delivery": "D-1", "route": "R14", "customer": "Hove Deli"}]}'}]
    >>> entities_from_trace(trace)
    {'type': 'entities', 'items': [{'kind': 'delivery', 'id': 'D-1', 'label': 'D-1 · Hove Deli'}, {'kind': 'route', 'id': 'R14', 'label': 'Route R14'}]}
    >>> entities_from_trace([{"tool": "aggregate_releases", "input": {}, "result": '{"group_by": ["item_number", "lookup:item description.ItemDescription"], "rows": [{"dimensions": ["104568", "Ginger"]}]}'}])
    {'type': 'entities', 'items': [{'kind': 'product', 'id': '104568', 'label': 'Ginger · 104568'}]}
    >>> entities_from_trace([{"tool": "list_releases", "input": {}, "result": '{"error": "x"}'}]) is None
    True
    """
    items = _Items()
    for call in trace:
        tool = call.get("tool")
        result = _parsed(call)
        if result is None:
            continue
        if tool in AT_RISK_TOOLS:
            _from_at_risk(result, items)
        elif tool == "list_releases":
            _from_releases(result, items)
        elif tool == "aggregate_releases":
            _from_aggregate(result, items)
        elif tool == "explain_advisory":
            _from_advisory(result, items)
    if not items.items:
        return None
    return {"type": "entities", "items": list(items.items.values())[:MAX_ITEMS]}
