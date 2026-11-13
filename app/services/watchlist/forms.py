"""The add_watch form of screen 14 (stage D, step 14c): what Ask hands back when a person asks
for a watch with a rule ("tell me if chilled zone C goes above 90%"), pre-filled with the thing,
the rule, the level and the live value; and how a confirmed form is read back into a watch_add.
Used by the watch tools (building) and by commands.py (reading).
"""

from __future__ import annotations

from typing import Any, Mapping

from app.services.teams.contracts import FormBlock, FormField
from app.services.watchlist import registry
from app.services.watchlist.search import Resolved

FORM_ID = "add_watch"
KIND_LABELS = {"delivery": "Delivery", "route": "Route", "location": "Location", "zone": "Zone",
               "product": "Product", "measure": "Measure"}
TELL = ["Goes above this level", "Drops below this level", "Changes at all", "Don't tell me, just show it"]
UNTIL = ["I stop it", "The end of today", "7 days from now"]


def tell_text(rule: str | None) -> str:
    """The "Tell me when" option for a rule word.

    >>> tell_text("above"), tell_text("below"), tell_text(None), tell_text("show_only")
    ('Goes above this level', 'Drops below this level', 'Changes at all', "Don't tell me, just show it")
    """
    return {"above": TELL[0], "below": TELL[1], "show_only": TELL[3]}.get(rule or "", TELL[2])


def add_watch_form(resolved: Resolved, *, rule: str | None = None, level: float | None = None,
                   live_text: str = "") -> FormBlock:
    """The form for one resolved thing, pre-filled with what the person said.

    (zone C, rule "above", level 90, live "64% full") -> FormBlock(add_watch, what "Zone C", level "90")
    """
    kind_label = KIND_LABELS.get(resolved.kind, resolved.kind)
    shown_level = "" if level is None else f"{level * 100:g}" if level <= 1 else f"{level:g}"
    fields = [
        FormField(name="what", label="What to watch", kind="text", required=True, value=resolved.label[:200]),
        FormField(name="kind", label="What it is", kind="select", options=[kind_label], value=kind_label),
        FormField(name="tell_me_when", label="Tell me when it", kind="select", options=TELL, value=tell_text(rule)),
        FormField(name="level", label="This level (%)", kind="number", required=False,
                  value=shown_level or (f"{registry.DEFAULT_LEVEL * 100:g}" if resolved.kind == "measure" else None),
                  placeholder="for a measure"),
        FormField(name="until", label="Keep watching until", kind="select", options=UNTIL, value=UNTIL[0]),
    ]
    note = f"Right now: {live_text}. " if live_text else ""
    return FormBlock(form_id=FORM_ID, title="Add to watchlist", fields=fields, submit_label="Add to watchlist",
                     note=(note + "It tells you here in the app, every time it changes status; you can change the alert "
                           "afterwards on the Watchlist tab.")[:300])


def kind_from_form(values: Mapping[str, Any]) -> str | None:
    """The kind a confirmed form names.

    >>> kind_from_form({"kind": "Route"}), kind_from_form({"kind": "van"})
    ('route', None)
    """
    label = str(values.get("kind") or "").strip().lower()
    return next((k for k, v in KIND_LABELS.items() if v.lower() == label), None)


def alert_from_form(kind: str, values: Mapping[str, Any]) -> dict[str, Any]:
    """The alert rule a confirmed form asks for, in the shape the store keeps.

    >>> alert_from_form("measure", {"tell_me_when": "Drops below this level", "level": "95"})
    {'rule': 'below', 'level': 0.95, 'band': 0.005}
    >>> alert_from_form("delivery", {"tell_me_when": "Changes at all"}), alert_from_form("zone", {"tell_me_when": "Don't tell me, just show it"})
    ({'rule': 'tier_from', 'tier': 'at_risk'}, {'rule': 'show_only'})
    """
    tell = str(values.get("tell_me_when") or TELL[2])
    if tell == TELL[3]:
        return {"rule": "show_only"}
    if kind == "measure" and tell in (TELL[0], TELL[1]):
        return registry.clean_alert(kind, {"rule": "above" if tell == TELL[0] else "below", "level": values.get("level") or registry.DEFAULT_LEVEL * 100})
    return registry.default_alert(kind)


def until_from_form(values: Mapping[str, Any]) -> str | None:
    """The end the form asks for, in the words commands.py reads.

    >>> until_from_form({"until": "7 days from now"}), until_from_form({"until": "I stop it"})
    ('7_days', None)
    """
    chosen = str(values.get("until") or UNTIL[0])
    return {UNTIL[1]: "end_of_day", UNTIL[2]: "7_days"}.get(chosen)
