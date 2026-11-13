"""The watch tools (stage D, step 14c), on the actions namespace: how the model acts on "watch
Route 14", "keep an eye on delivery 33188", "tell me if fill rate drops below 95%" and "stop
watching 33188". A plain watch is added at once as the asker (decision 12) and answers with the
receipt; a watch with a rule and a level comes back as the add_watch form; a name matching
several things, or a rule without a level, comes back as one choice block whose options carry
the ready command; nothing matching, no access, or a full list is a problem the model reads out
(the full list's own choice block travels too). The model never says something is watched unless
a receipt came back.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.access import records
from app.services.access.commands import Refused
from app.services.teams.contracts import Command, QuestionJob
from app.services.watchlist import commands as watch_commands
from app.services.watchlist import forms, readers, registry, search, store

WATCH_NOW = "watch_now"
STOP_WATCHING = "stop_watching"
WATCH_TOOL_NAMES = frozenset({WATCH_NOW, STOP_WATCHING})
KIND_WORDS = {"delivery": "delivery", "deliveries": "delivery", "route": "route", "van": "route", "location": "location",
              "slot": "location", "pick face": "location", "zone": "zone", "area": "zone", "product": "product",
              "item": "product", "sku": "product", "measure": "measure", "fill rate": "measure", "kpi": "measure"}

WATCH_TOOLS: list[dict[str, Any]] = [
    {
        "name": WATCH_NOW,
        "description": (
            "Watch something for the person, when they ask to watch, keep an eye on, follow, track or be "
            "told about a delivery, a route, an item or product, a location, a zone, or a measure such as "
            "the fill rate. Call it with their words for the thing, and with the rule and level when they "
            "gave a line (\"tell me if fill rate drops below 95%\"). Do not call a data tool first. What "
            "comes back is one of: a receipt (it is watched now; say so in one sentence with the alert it "
            "got), a form (ask them to check it and confirm; never say it is watched), a choice (ask the "
            "one question it carries and nothing else; the options are tapped), or problems (say the "
            "reason in one sentence)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "what": {"type": "string", "description": "The thing in their words: a delivery number, a route code, an item number or name, a location code, a zone, or \"fill rate\"."},
                "kind": {"type": "string", "description": "delivery | route | product | location | zone | measure, when clear from the words; empty otherwise."},
                "rule": {"type": "string", "description": "above | below when they named a direction (\"goes above\", \"drops below\"); empty for a plain watch."},
                "level": {"type": "number", "description": "The level in percent when they said one, e.g. 95."},
            },
            "required": ["what"],
            "additionalProperties": False,
        },
        "side_effects": True,
    },
    {
        "name": STOP_WATCHING,
        "description": (
            "Stop watching something for the person, when they ask to stop watching, unwatch, drop or "
            "remove a thing from their watchlist. Call it with their words for the thing. A receipt means "
            "it is stopped (say so in one sentence); a choice asks which of their watches they mean; "
            "problems say why not."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"what": {"type": "string", "description": "The thing in their words."}},
            "required": ["what"],
            "additionalProperties": False,
        },
        "side_effects": True,
    },
]


def kind_word(raw: str | None, what: str) -> str | None:
    """The watch kind the model named, or the one the person's words carry.

    >>> kind_word("route", "14"), kind_word(None, "watch route 14"), kind_word(None, "ginger"), kind_word("van", "R3")
    ('route', 'route', None, 'route')
    """
    text = str(raw or "").strip().lower()
    if text in KIND_WORDS:
        return KIND_WORDS[text]
    words = what.lower()
    for word, kind in KIND_WORDS.items():
        if word in words.split() or (len(word) > 4 and word in words):
            return kind
    return None


def _job(customer_code: str, actor_object_id: str, actor_name: str | None, site_id: str | None, kind: str, payload: dict) -> QuestionJob:
    return QuestionJob(job_id=f"ask-{uuid.uuid4()}", tenant_id="", customer_code=customer_code, conversation_id="ask",
                       sender_object_id=actor_object_id, sender_name=actor_name, question=f"watch: {kind}", source="tab",
                       site_id=site_id, command=Command(kind=kind, by=actor_name, payload=payload))


def _choice(question: str, options: list[dict], *, allow_typed: bool = True) -> str:
    return json.dumps({"shown": "choice", "block": {"type": "choice", "question": question, "options": options, "allow_typed": allow_typed}})


def _problems(*reasons: str) -> str:
    return json.dumps({"problems": list(reasons)})


async def _run_command(db: AsyncSession, job: QuestionJob) -> str:
    """Run a watch command as the asker on the tool's own session; the receipt or the choice as the
    tool's result."""
    try:
        outcome = await watch_commands.run(job, db=db)
    except Refused as exc:
        return _problems(str(exc))
    block = outcome.blocks[0] if outcome.blocks else None
    if block is None:
        return json.dumps({"shown": "text", "text": outcome.text})
    return json.dumps({"shown": block.get("type", "receipt"), "text": outcome.text, "block": block})


async def watch_now(db: AsyncSession, customer_code: str, actor_object_id: str, actor_name: str | None,
                    site_id: str | None, args: Mapping[str, Any]) -> str:
    """Resolve the thing and watch it, or hand back the form or the one question that is needed.

    ({"what": "route 14"}) -> receipt ; ({"what": "zone C"}, two zones) -> choice ;
    ({"what": "fill rate", "rule": "below"}) -> choice of levels ; (with level 95) -> add_watch form
    """
    what = " ".join(str(args.get("what") or "").split()).strip()
    if not what:
        return _problems("say what to watch: a delivery number, a route, an item, a location, a zone or a measure")
    record = await records.access_record_for(db, customer_code, actor_object_id)
    if record is None or not record.is_active:
        return _problems("you have no active access record")
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    kind = kind_word(args.get("kind"), what)
    rule = str(args.get("rule") or "").strip().lower() or None
    level = args.get("level")
    # Step 1: Find what the words name within the person's sites; one thing goes on, several
    # become the one question, none is a problem the model reads out
    candidates = await search.find(db, customer_code, record, what, kind=kind, tz=tz)
    code = search.code_of(what)
    exact = [c for c in candidates if c.entity_id.lower() == what.lower() or c.label.lower() == what.lower()
             or (c.kind == "route" and search.code_of(c.entity_id) == code)]
    if not candidates:
        return _problems(f"nothing called {what!r} to watch here; try the delivery number, the route code, "
                         "the item number or name, the location code, the zone, or \"fill rate\"")
    if len(candidates) > 1 and not exact:
        options = [{"id": f"{c.kind}:{c.entity_id}", "label": c.label, "detail": c.detail, "suggested": i == 0,
                    "command": {"kind": "watch_add", "payload": {"kind": c.kind, "entity_id": c.entity_id, "via": "ask",
                                                                 **({"alert": {"rule": rule, "level": level}} if rule in ("above", "below") and level is not None else {})}}}
                   for i, c in enumerate(candidates[:6])]
        return _choice(f"{len(candidates)} things match {what!r}. Which one do you mean?", options)
    target = exact[0] if exact else candidates[0]
    # Step 2: A rule needs its level; without one the question offers levels around the figure
    if rule in ("above", "below") and level is None:
        current = (readers.fill_rate(customer_code, target.entity_id.split(":")[-1]) or {}).get("fill_rate") if target.kind == "measure" else None
        base = round(current * 100) if current is not None else 95
        levels = sorted({max(1, min(100, base - 5)), max(1, min(100, base)), max(1, min(100, base + 3))})
        options = [{"id": f"level:{lv}", "label": f"{lv}%", "detail": f"now {registry.pct(current)}" if current is not None and lv == base else "",
                    "suggested": lv == base,
                    "command": {"kind": "watch_add", "payload": {"kind": target.kind, "entity_id": target.entity_id, "via": "ask",
                                                                 "alert": {"rule": rule, "level": lv}}}} for lv in levels]
        word = "goes above" if rule == "above" else "drops below"
        return _choice(f"At what level? Tell you when {target.label} {word}…", options)
    if rule in ("above", "below"):
        resolved = await search.resolve(db, customer_code, record, target.kind, target.entity_id, tz=tz)
        block = forms.add_watch_form(resolved, rule=rule, level=float(level), live_text=resolved.detail)
        return json.dumps({"shown": "form", "block": block.model_dump(mode="json")})
    # Step 3: A plain watch is added now, with the usual alert for its kind (decision 12)
    payload = {"kind": target.kind, "entity_id": target.entity_id, "via": "ask"}
    return await _run_command(db, _job(customer_code, actor_object_id, actor_name, site_id, "watch_add", payload))


async def stop_watching(db: AsyncSession, customer_code: str, actor_object_id: str, actor_name: str | None,
                        site_id: str | None, args: Mapping[str, Any]) -> str:
    """Stop one of the asker's watches by its words, or ask which.

    ({"what": "33188"}) -> receipt ; ({"what": "route"}, two routes watched) -> choice ; (none) -> problems
    """
    words = " ".join(str(args.get("what") or "").split()).strip()
    what = words.lower()
    watches = await store.active_for_owner(db, customer_code, actor_object_id)
    if not watches:
        return _problems("you are not watching anything")
    code = search.code_of(what)
    matches = [w for w in watches if what and (what in w.label.lower() or what == w.entity_id.lower()
                                                  or (w.kind == "route" and search.code_of(w.entity_id) == code and code))]
    if not matches:
        return _problems(f"nothing on your watchlist matches {words!r}; you watch " + ", ".join(w.label for w in watches[:10]))
    if len(matches) > 1:
        options = [{"id": str(w.id), "label": w.label, "detail": registry.status_text(w.last_status), "suggested": i == 0,
                    "command": {"kind": "watch_stop", "payload": {"watch_id": str(w.id), "via": "ask"}}} for i, w in enumerate(matches[:6])]
        return _choice(f"You watch {len(matches)} things like that. Which one do you want to stop?", options, allow_typed=False)
    return await _run_command(db, _job(customer_code, actor_object_id, actor_name, site_id, "watch_stop",
                                   {"watch_id": str(matches[0].id), "via": "ask"}))


async def run_watch_tool(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                         name: str, args: Mapping[str, Any], *, actor_name: str | None = None) -> str:
    """Run one watch tool: a receipt, a form or a choice under "block", or the problems.

    ("watch_now", {"what": "route 14"}) -> '{"shown": "receipt", "text": "Watching · …", "block": {...}}'
    """
    if name == WATCH_NOW:
        return await watch_now(db, customer_code, actor_object_id, actor_name, actor_site_id, args)
    if name == STOP_WATCHING:
        return await stop_watching(db, customer_code, actor_object_id, actor_name, actor_site_id, args)
    return _problems(f"unknown watch tool {name!r}")
