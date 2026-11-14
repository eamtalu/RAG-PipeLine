"""Chunk 178: create_user end to end through Ask (stage D, step 8). The model proposes the form
from the asker's own record (filled, and marked for approval when the matrix says so); the form
is lifted off the trace as a block; the confirmed form is the access_set_user command, which
checks, queues or creates, audits, and answers with a receipt whose Undo withdraws the request
or deactivates the new user within the window.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete

from app.agent.presentation import blocks as answer_blocks
from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.tenant_site import TenantSite
from app.services.access import approvals, commands as access_commands, users
from app.services.analytics_agent import action_tools
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from tests.test_teams_consumer_chunk123 import Harness, job

CC = "test_c178"
NOW = datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")


async def _wipe() -> None:
    async with async_session() as s:
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C178"))
        s.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                   TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2)])
        await s.commit()
        await users.create_user(s, CC, "u-adm", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Ada Admin", role_code="admin", all_sites=True))
        await users.create_user(s, CC, "u-sup", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Sam Super", role_code="supervisor", site_ids=("BRI",)))
        await users.create_user(s, CC, "u-pick", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Pat Picker", role_code="picker", site_ids=("BRI",)))
        await s.commit()
    yield
    await _wipe()


def _command(sender: str, kind: str, payload: dict, name: str = "Someone") -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC, conversation_id="c",
                       sender_object_id=sender, sender_name=name, question=f"admin: {kind}", source="tab",
                       site_id="BRI", command=Command(kind=kind, by=name, payload=payload))


# ==================================================== 1. the proposed form


async def test_the_form_is_filled_from_the_asker_and_marked_for_approval_when_the_matrix_says_so():
    async with async_session() as db:
        raw = await action_tools.run_action_tool(db, CC, "u-adm", "BRI", "propose_create_user",
                                                 {"display_name": "Dan Roberts", "shift": "night", "note": "handheld only"})
        admin = json.loads(raw)
        raw = await action_tools.run_action_tool(db, CC, "u-sup", "LON", "propose_create_user",
                                                 {"display_name": "Dan Roberts", "role": "Shift lead", "site_id": "LON"})
        sup = json.loads(raw)
        raw = await action_tools.run_action_tool(db, CC, "u-pick", "BRI", "propose_create_user", {"display_name": "X"})
        picker = json.loads(raw)
        raw = await action_tools.run_action_tool(db, CC, "u-adm", "BRI", "propose_create_user", {})
        empty = json.loads(raw)
    assert admin["shown"] == "form" and admin["requires_approval"] is False
    block = admin["block"]
    assert block["type"] == "form" and block["form_id"] == "create_user" and block["submit_label"] == "Create user"
    fields = {f["name"]: f for f in block["fields"]}
    assert fields["display_name"]["value"] == "Dan Roberts" and fields["role"]["value"] == "picker"
    assert fields["site_id"]["value"] == "BRI" and fields["site_id"]["options"] == ["BRI", "LON"]
    assert fields["shift"]["value"] == "Night · 22:00 – 06:00"
    assert fields["note"]["value"] == "Night · 22:00 – 06:00 · handheld only"
    assert fields["object_id"]["required"] is True and fields["object_id"]["value"] is None
    assert sup["requires_approval"] is True and sup["block"]["submit_label"] == "Create & send for approval"
    sup_fields = {f["name"]: f for f in sup["block"]["fields"]}
    assert sup_fields["role"]["value"] == "shift_lead" and sup_fields["site_id"]["value"] == "LON"
    assert "Needs approval" in sup["block"]["note"]
    # step 18: one gate in the registry refuses before any builder runs, with one wording
    assert picker["problems"][0].startswith("you cannot do that: manage_users is not allowed")
    assert "display_name is required" in empty["problems"][0]


def test_the_form_is_lifted_off_the_trace_from_the_tools_result():
    form = {"type": "form", "form_id": "create_user", "title": "New user", "fields": [], "requires_approval": True}
    trace = [
        {"tool": "propose_create_user", "input": {"display_name": "Dan"}, "result": json.dumps({"shown": "form", "block": form})},
        {"tool": "propose_create_user", "input": {}, "result": json.dumps({"problems": ["display_name is required"]})},
    ]
    assert answer_blocks.blocks_from_trace(trace) == [form]
    assert answer_blocks.data_trace(trace) == []  # an action call is never evidence of a data call
    assert answer_blocks.is_presentation_call({"tool": "propose_create_user"})


# ==================================================== 2. the confirmed form, the receipt, the undo


async def test_an_admins_confirmed_form_creates_the_user_with_a_receipt_that_can_be_undone():
    mirror = InMemoryMirrorWriter()
    form_payload = {"object_id": "u-dan", "display_name": "Dan Roberts", "email": "", "role": "picker",
                    "site_id": "BRI", "shift": "Night · 22:00 – 06:00", "note": "handheld only", "via": "ask"}
    outcome = await access_commands.run(_command("u-adm", "access_set_user", form_payload, "Ada"), mirror=mirror, now=NOW)
    assert outcome.text == "created Dan Roberts as Picker for BRI"
    receipt = outcome.blocks[0]
    assert receipt["type"] == "receipt" and receipt["status"] == "done" and receipt["undo"] == {"object_id": "u-dan"}
    assert receipt["undo_until"].startswith("2026-11-13T09:10")
    async with async_session() as db:
        dan = await users.get_user(db, CC, "u-dan")
        assert dan.note == "Night · 22:00 – 06:00 · handheld only" and dan.email is None and dan.created_by == "u-adm"
    # another admin cannot undo it; the creator can, within the window; later it is refused
    with pytest.raises(access_commands.Refused, match="not created by you"):
        await access_commands.run(_command("u-sup", "access_undo", {"object_id": "u-dan"}), mirror=mirror, now=NOW)
    with pytest.raises(access_commands.Refused, match="window has passed"):
        await access_commands.run(_command("u-adm", "access_undo", {"object_id": "u-dan"}), mirror=mirror,
                                  now=NOW + timedelta(minutes=11))
    undone = await access_commands.run(_command("u-adm", "access_undo", {"object_id": "u-dan"}, "Ada"), mirror=mirror,
                                       now=NOW + timedelta(minutes=5))
    assert undone.text == "undone · Dan Roberts deactivated"
    async with async_session() as db:
        assert (await users.get_user(db, CC, "u-dan")).status == "deactivated"


async def test_a_supervisors_confirmed_form_is_a_request_with_a_receipt_that_withdraws_it():
    mirror = InMemoryMirrorWriter()
    form_payload = {"object_id": "u-dan", "display_name": "Dan Roberts", "role": "picker", "site_id": "BRI", "via": "ask"}
    outcome = await access_commands.run(_command("u-sup", "access_set_user", form_payload, "Sam"), mirror=mirror, now=NOW)
    assert outcome.text == "sent for approval · USR-1"
    receipt = outcome.blocks[0]
    assert receipt["status"] == "needs_approval" and receipt["reference"] == "USR-1" and receipt["undo"] == {"reference": "USR-1"}
    async with async_session() as db:
        queue = await approvals.pending(db, CC)
        assert queue[0].requested_via == "ask" and queue[0].payload["site_ids"] == ["BRI"]
    # only the requester may take it back
    with pytest.raises(ValueError, match="not requested by you"):
        await access_commands.run(_command("u-adm", "access_undo", {"reference": "USR-1"}), mirror=mirror, now=NOW)
    undone = await access_commands.run(_command("u-sup", "access_undo", {"reference": "USR-1"}, "Sam"), mirror=mirror,
                                       now=NOW + timedelta(minutes=2))
    assert undone.text == "withdrawn · USR-1"
    async with async_session() as db:
        assert await approvals.pending(db, CC) == []
        queue = json.loads(mirror.items[(f"EYE#{CC}", "APPROVALS")].payload_json)
        assert queue["recent"][0]["status"] == "withdrawn"


async def test_the_consumer_carries_the_receipt_in_the_answer():
    async def run_command(job_):
        return access_commands.CommandOutcome("created X", (access_commands.receipt("Created X", undo={"object_id": "x"}, now=NOW),))

    h = Harness([job(question="admin", command=Command(kind="access_set_user", by="A", payload={"object_id": "x"}))])
    h.consumer._run_command = run_command
    await h.drain()
    posted = h.poster.posted[0]
    assert posted.status == "ok" and posted.answer == "created X"
    assert posted.blocks[0].type == "receipt" and posted.blocks[0].undo == {"object_id": "x"}
    assert posted.answered_at is not None
