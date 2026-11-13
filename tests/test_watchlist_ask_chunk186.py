"""Chunk 186: the watchlist in Ask (stage D, step 14c).

An answer carries the watchable things its data calls named, each marked watched or not, and at
most one suggestion to watch the thing the question was about: never when it is watched, when the
person said "Not now" today, when the list is full, when the thing is outside their sites, or on a
turn that already acts. "Watch route 14" is one tool call that adds the watch as the asker and
answers with the receipt; a rule with a level comes back as the add_watch form, which a confirmed
form turns into the watch; a rule without a level, or a name matching several things, is one
choice whose options carry the ready command; "stop watching 33188" stops it.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from sqlalchemy import select

from app.agent.presentation.blocks import blocks_from_trace
from app.agent.presentation.entities import entities_from_trace
from app.config.database import async_session
from app.persistence.models.watch import Watch
from app.services.analytics_agent import watch_tools
from app.services.analytics_agent.action_tools import ACTION_TOOL_NAMES, ACTION_TOOLS, run_action_tool
from app.services.teams import home_snapshot
from app.services.access.commands import Refused
from app.services.teams.commands import run_command
from app.services.teams.contracts import QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from app.services.watchlist import ask_hooks, forms, store
from app.services.watchlist import commands as watch_commands
from app.teams_consumer import framed_question
from tests.test_watchlist_chunk185 import CC, NOW, _at_risk, _command, _customer, _record  # noqa: F401

pytestmark = pytest.mark.usefixtures("_customer")


@pytest.fixture(autouse=True)
def _in_memory_mirror(monkeypatch):
    """The commands the tools run mirror into memory, so no test touches DynamoDB."""
    monkeypatch.setattr(watch_commands, "build_mirror_writer_from_settings", InMemoryMirrorWriter)


def _trace(tool: str, result: dict) -> list[dict]:
    return [{"tool": tool, "input": {}, "result": json.dumps(result)}]


def _job(sender: str, question: str, context: dict | None = None) -> QuestionJob:
    return QuestionJob(job_id="j-186", tenant_id="t", customer_code=CC, conversation_id="c", sender_object_id=sender,
                       sender_name="Sam Super", question=question, source="tab", site_id="BRI", context=context)


async def _seed_rows() -> None:
    async with async_session() as s:
        s.add_all([_at_risk("D-1", "at_risk"), _at_risk("D-2", "none", route="R15"),
                   _at_risk("D-9", "none", route="R90", warehouse="LON")])
        await s.commit()


# ==================================================== 1. the entities an answer names

def test_entities_come_from_the_data_results_and_join_the_blocks_in_order():
    trace = _trace("at_risk_board", {"rows": [{"delivery": "D-1", "route": "R14", "customer": "Hove Deli"},
                                              {"delivery": "D-2", "route": "R14", "customer": None}]})
    block = entities_from_trace(trace)
    assert [i["id"] for i in block["items"]] == ["D-1", "R14", "D-2"]
    assert block["items"][0]["label"] == "D-1 · Hove Deli" and block["items"][2]["label"] == "D-2"
    trace += _trace("list_releases", {"rows": [{"delivery_number": "D-7", "item_number": "104568", "from_location": "A-01-02",
                                                "looked_up": {"item description.ItemDescription": "Ginger"}}]})
    trace += _trace("aggregate_releases", {"group_by": ["from_zone"], "rows": [{"dimensions": ["C"]}]})
    trace += _trace("explain_advisory", {"advisory": {"source": {"kind": "count", "item": "104568"}}, "facts": []})
    kinds = [(i["kind"], i["id"]) for i in entities_from_trace(trace)["items"]]
    assert kinds == [("delivery", "D-1"), ("route", "R14"), ("delivery", "D-2"), ("delivery", "D-7"),
                     ("product", "104568"), ("location", "A-01-02")], "six at most, one per kind and id"
    assert entities_from_trace(_trace("at_risk_board", {"error": "down"})) is None
    assert entities_from_trace([{"tool": "search_logs", "input": {}, "result": '{"rows": [{"delivery": "D-1"}]}'}]) is None
    # the entities block leads the answer's blocks, and an action's block is lifted after the table
    trace += [{"tool": "show_table", "input": {"title": "t", "columns": ["a"], "rows": [["1"]]}, "result": "shown"},
              {"tool": "watch_now", "input": {"what": "route 14"},
               "result": json.dumps({"shown": "receipt", "block": {"type": "receipt", "text": "Watching · Route R14"}})}]
    assert [b["type"] for b in blocks_from_trace(trace)] == ["entities", "table", "receipt"]


# ==================================================== 2. the consumer's hook: watched marks and the one suggestion

async def test_the_answer_marks_what_is_watched_and_suggests_the_thing_the_question_named_once():
    await _seed_rows()
    await run_command(_command("u-sup", "watch_add", {"kind": "delivery", "entity_id": "D-2"}))
    rows = {"rows": [{"delivery": "D-1", "route": "R14", "customer": "Hove Deli"}, {"delivery": "D-2", "route": "R15", "customer": "Hove Deli"},
                     {"delivery": "D-9", "route": "R90", "customer": "Far Away"}]}
    result = {"answer": "…", "blocks": [entities_from_trace(_trace("at_risk_board", rows))]}
    out = await ask_hooks.decorate(result, _job("u-sup", "Where is delivery D-1?"), now=NOW)
    items = {i["id"]: i for i in out["blocks"][0]["items"]}
    assert items["D-2"]["watched"] is True and items["D-1"]["watched"] is False
    assert items["D-9"]["can_watch"] is False, "LON is outside Sam's access"
    assert items["D-1"]["site_id"] == "BRI"
    suggestion = out["blocks"][-1]
    assert suggestion["type"] == "watch_suggestion" and suggestion["entity"]["id"] == "D-1"
    assert suggestion["text"] == "Keep an eye on D-1 · Hove Deli for you?" and suggestion["default_alert"] == "Tells you if it is likely to leave short or late"
    # the question that names the watched thing, or names nothing, gets no suggestion
    for question in ("How is D-2 doing?", "What is short today?"):
        out = await ask_hooks.decorate({"blocks": [entities_from_trace(_trace("at_risk_board", rows))]}, _job("u-sup", question), now=NOW)
        assert [b["type"] for b in out["blocks"]] == ["entities"], question
    # Not now holds it for a day; a full list holds it; an action turn holds it; a thing out of reach holds it
    async with async_session() as s:
        await store.not_now(s, CC, "u-sup", "delivery", "D-1", now=NOW)
        await s.commit()
    out = await ask_hooks.decorate({"blocks": [entities_from_trace(_trace("at_risk_board", rows))]}, _job("u-sup", "Where is D-1?"), now=NOW)
    assert [b["type"] for b in out["blocks"]] == ["entities"]
    out = await ask_hooks.decorate({"blocks": [entities_from_trace(_trace("at_risk_board", rows))]}, _job("u-sup", "Where is D-1?"), now=NOW + timedelta(hours=25))
    assert out["blocks"][-1]["type"] == "watch_suggestion"
    out = await ask_hooks.decorate({"blocks": [entities_from_trace(_trace("at_risk_board", rows)), {"type": "receipt", "text": "x"}]},
                                   _job("u-sup", "Where is D-1?"), now=NOW + timedelta(hours=25))
    assert [b["type"] for b in out["blocks"]] == ["entities", "receipt"]
    out = await ask_hooks.decorate({"blocks": [entities_from_trace(_trace("at_risk_board", rows))]}, _job("u-sup", "Where is D-9?"), now=NOW + timedelta(hours=25))
    assert [b["type"] for b in out["blocks"]] == ["entities"]
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ask_hooks.settings, "teams_watches_limit", 1)
        out = await ask_hooks.decorate({"blocks": [entities_from_trace(_trace("at_risk_board", rows))]}, _job("u-sup", "Where is D-1?"), now=NOW + timedelta(hours=25))
        assert [b["type"] for b in out["blocks"]] == ["entities"]
    # a failing hook never costs the answer
    assert await ask_hooks.decorate_safely({"blocks": [{"type": "entities", "items": [{"kind": "x"}]}]}, _job("u-sup", "?")) == {"blocks": [{"type": "entities", "items": [{"kind": "x"}]}]}


# ==================================================== 3. the watch tools: receipt, choice, form, problems

async def test_watch_now_adds_a_plain_watch_asks_one_question_or_hands_back_the_form():
    await _seed_rows()
    assert {"watch_now", "stop_watching"} <= ACTION_TOOL_NAMES and [t["name"] for t in ACTION_TOOLS][-2:] == ["watch_now", "stop_watching"]
    async with async_session() as s:
        out = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "watch_now", {"what": "route 14"}, actor_name="Sam Super"))
        assert out["shown"] == "receipt" and out["block"]["type"] == "receipt"
        assert out["text"].startswith("Watching · Route R14") and "1 of 10 places used" in out["text"]
        assert out["block"]["undo"]["action"] == "stop"
        # the same words again: one watch, the receipt says so
        again = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "watch_now", {"what": "Route 14"}))
        assert again["shown"] == "receipt"
        watches = (await s.execute(select(Watch).where(Watch.customer_code == CC, Watch.status == "active"))).scalars().all()
        assert [(w.kind, w.entity_id, w.created_via) for w in watches] == [("route", "R14", "ask")]
        # nothing by that name, and something outside the person's sites, are problems
        out = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "watch_now", {"what": "unicorn"}))
        assert out["problems"] == ["nothing called 'unicorn' to watch here; try the delivery number, the route code, "
                                   "the item number or name, the location code, the zone, or \"fill rate\""]
        out = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "watch_now", {"what": "D-9"}))
        assert out["problems"], "a LON delivery is not Sam's to watch"
        # several matches: the one question, each option carrying the ready command
        out = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "watch_now", {"what": "Hove Deli"}))
        assert out["shown"] == "choice" and out["block"]["question"] == "2 things match 'Hove Deli'. Which one do you mean?"
        assert [o["command"]["payload"]["entity_id"] for o in out["block"]["options"]] == ["D-1", "D-2"]
        assert out["block"]["options"][0]["suggested"] is True and out["block"]["options"][0]["command"]["kind"] == "watch_add"
        # a rule without its level: levels around the live figure
        home_snapshot._LATEST_FILL_RATES[(CC, "BRI")] = {"fill_rate": 0.964, "as_of": NOW.isoformat(), "provisional": False, "expected": 100}
        out = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "watch_now", {"what": "fill rate", "rule": "below"}))
        assert out["shown"] == "choice" and out["block"]["question"].startswith("At what level?")
        assert [o["label"] for o in out["block"]["options"]] == ["91%", "96%", "99%"]
        assert out["block"]["options"][1]["detail"] == "now 96.4%" and out["block"]["options"][1]["suggested"]
        assert out["block"]["options"][1]["command"]["payload"]["alert"] == {"rule": "below", "level": 96}
        # a rule with its level: the add_watch form, pre-filled, nothing watched yet
        out = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "watch_now", {"what": "fill rate", "rule": "below", "level": 95}))
        assert out["shown"] == "form" and out["block"]["form_id"] == "add_watch"
        values = {f["name"]: f["value"] for f in out["block"]["fields"]}
        assert values == {"what": "Fill rate · Brighton", "kind": "Measure", "tell_me_when": "Drops below this level",
                          "level": "95", "until": "I stop it"}
        assert out["block"]["note"].startswith("Right now: 96.4% today so far.")
        assert len((await s.execute(select(Watch).where(Watch.customer_code == CC, Watch.status == "active"))).scalars().all()) == 1


async def test_a_confirmed_form_becomes_the_watch_and_stop_watching_finds_it_by_its_words():
    await _seed_rows()
    home_snapshot._LATEST_FILL_RATES[(CC, "BRI")] = {"fill_rate": 0.964, "as_of": NOW.isoformat(), "provisional": False, "expected": 100}
    values = {"what": "Fill rate · Brighton", "kind": "Measure", "tell_me_when": "Drops below this level", "level": "95",
              "until": "7 days from now"}
    text = (await run_command(_command("u-sup", "watch_add", {"form_id": forms.FORM_ID, "values": values, "via": "ask"}))).text
    assert text.startswith("Watching · Fill rate · Brighton · Tells you if it drops below 95% · 1 of 10 places used · until Fri 20 Nov")
    async with async_session() as s:
        watch = (await s.execute(select(Watch).where(Watch.customer_code == CC, Watch.status == "active"))).scalar_one()
        assert watch.kind == "measure" and watch.alert == {"rule": "below", "level": 0.95, "band": 0.005}
        assert watch.until is not None and watch.created_via == "ask"
        # a form naming nothing is refused in words
        with pytest.raises(Refused, match="nothing called 'unicorn' to watch"):
            await run_command(_command("u-sup", "watch_add", {"form_id": forms.FORM_ID, "values": {**values, "what": "unicorn"}}))
        # stop watching: by the words, or one question when two match, or a problem when none do
        await run_command(_command("u-sup", "watch_add", {"kind": "route", "entity_id": "R14"}))
        await run_command(_command("u-sup", "watch_add", {"kind": "route", "entity_id": "R15"}))
        out = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "stop_watching", {"what": "route"}))
        assert out["shown"] == "choice" and out["block"]["allow_typed"] is False
        assert [o["command"]["kind"] for o in out["block"]["options"]] == ["watch_stop", "watch_stop"]
        out = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "stop_watching", {"what": "route 15"}))
        assert out["shown"] == "receipt" and out["text"].startswith("Stopped watching · Route R15")
        out = json.loads(await run_action_tool(s, CC, "u-sup", "BRI", "stop_watching", {"what": "zone Q"}))
        assert out["problems"][0].startswith("nothing on your watchlist matches 'zone Q'; you watch ")
        out = json.loads(await run_action_tool(s, CC, "u-ops", "BRI", "stop_watching", {"what": "route 14"}))
        assert out["problems"] == ["you are not watching anything"]
    # the typed answer to a choice is framed for the model
    framed = framed_question(_job("u-sup", "95", context={"choice": {"question": "At what level?"}}))
    assert framed == '95\n\n(This answers your question "At what level?": carry on with what you were doing, using this answer; do not ask it again.)'
