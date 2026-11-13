"""Chunk 187: the Tasks tab becomes Routines (stage D, step 15a, backend).

A routine is a task the person runs or does again and again: a check (a question), a report (a
question sent on), or an action (something to do). It runs by the clock as before, by hand ("Run
now", recorded at once with the answer, the clock untouched, nothing posted), or the person ticks
it done or skipped with a note (a one-off is finished by that, a repeating one keeps its clock).
Every run is a ledger row saying how it was triggered and by whom; the person's `USER#<id>#TASK_RUNS`
row lists the last 30 days newest first; Plan again and Copy make a new routine from an old one
without retyping; Repeat turns a one-off into a repeating one; runs older than 30 days are purged.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.access import AuditEvent
from app.persistence.models.task import TeamsTask, TeamsTaskRun
from app.services.access import commands as access_commands
from app.services.tasks import commands, runner, store
from app.services.tasks import mirror as tasks_mirror
from app.services.teams.mirror import InMemoryMirrorWriter
from tests.test_teams_tasks_chunk180 import CC, NOW, TZ, _command, _customer, _reminder  # noqa: F401

pytestmark = pytest.mark.usefixtures("_customer")


def _row(mirror: InMemoryMirrorWriter, suffix: str) -> dict:
    return json.loads(mirror.items[(f"EYE#{CC}", f"USER#u-sup#{suffix}")].payload_json)


async def _answer(task):
    return {"answer": "GINGER was short on 3 releases.\nTwo more lines.", "blocks": [{"type": "table", "title": "t", "columns": [{"name": "a"}], "rows": [["1"]]}]}


# ==================================================== 1. the shapes and the Today list

def test_the_shape_of_a_routine_and_the_today_list():
    check = TeamsTask(kind="schedule", query_text="short products", target="me")
    report = TeamsTask(kind="schedule", query_text="short products", target="channel")
    action = TeamsTask(kind="reminder", query_text=None, target="me")
    assert (store.shape_of(check), store.shape_of(report), store.shape_of(action)) == ("check", "report", "action")
    rows = [
        {"id": "a", "status": "active", "next_fire_at": "2026-11-13T17:45:00+00:00", "last_result": None},
        {"id": "b", "status": "active", "next_fire_at": "2026-11-13T08:00:00+00:00", "last_result": None},
        {"id": "c", "status": "active", "next_fire_at": "2026-11-16T08:00:00+00:00", "last_result": None},
        {"id": "d", "status": "done", "next_fire_at": None, "last_result": {"at": "2026-11-13T08:30:00+00:00", "outcome": "done"}},
        {"id": "e", "status": "active", "next_fire_at": "2026-11-16T16:00:00+00:00", "last_result": {"at": "2026-11-13T09:00:00+00:00", "outcome": "sent"}},
    ]
    today = tasks_mirror.today_list(rows, tz=TZ, now=NOW)
    assert [(t["id"], t["is_done"], t["is_overdue"]) for t in today] == [("b", False, True), ("a", False, False), ("e", True, False), ("d", True, False)]


# ==================================================== 2. by hand, ticked, the ledger and the rows

async def test_run_now_records_a_by_hand_run_and_a_tick_records_done_or_skipped_with_a_note():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        check = await _reminder(db, kind="schedule", title="Short products list", query_text="short products today",
                                when={"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "16:00"})
        action = await _reminder(db, title="Move a picker to Route 14")
        await db.commit()
        check_id, action_id = str(check.id), str(action.id)
    # Step 1: Run now answers the question at once and leaves the clock alone
    out = await commands.run(_command("u-sup", "task_update", {"task_id": check_id, "action": "run_now"}), mirror=mirror, now=NOW, run_query=_answer)
    assert out.text == "ran · Short products list · GINGER was short on 3 releases."
    with pytest.raises(access_commands.Refused, match="cannot be run from here"):
        await commands.run(_command("u-sup", "task_update", {"task_id": check_id, "action": "run_now"}), mirror=mirror, now=NOW)
    # Step 2: A tick with a note finishes a one-off action; a tick on a repeating check keeps it
    out = await commands.run(_command("u-sup", "task_update", {"task_id": action_id, "action": "done", "note": "Dan moved at 09:05"}), mirror=mirror, now=NOW + timedelta(minutes=5))
    assert out.text == "done · Move a picker to Route 14 · Dan moved at 09:05"
    out = await commands.run(_command("u-sup", "task_update", {"task_id": check_id, "action": "skip", "note": "bank holiday"}), mirror=mirror, now=NOW + timedelta(minutes=6))
    assert out.text == "skipped · Short products list · bank holiday"
    async with async_session() as db:
        rows = {t.title: t for t in (await db.execute(select(TeamsTask).where(TeamsTask.customer_code == CC))).scalars()}
        assert rows["Short products list"].status == "active" and rows["Short products list"].next_fire_at == datetime(2026, 11, 13, 16, 0, tzinfo=timezone.utc)
        assert rows["Short products list"].fire_count == 0, "a by-hand run is not a firing"
        assert rows["Move a picker to Route 14"].status == "done" and rows["Move a picker to Route 14"].done_at == NOW + timedelta(minutes=5)
        runs = sorted((await db.execute(select(TeamsTaskRun).where(TeamsTaskRun.customer_code == CC))).scalars(), key=lambda r: r.started_at)
        assert [(r.trigger, r.outcome, r.by_name, r.note) for r in runs] == [
            ("hand", "sent", "Sam", None), ("tick", "done", "Sam", "Dan moved at 09:05"), ("tick", "skipped", "Sam", "bank holiday")]
        assert runs[0].blocks and runs[0].text.startswith("GINGER")
        actions = sorted(e.action for e in (await db.execute(select(AuditEvent).where(AuditEvent.customer_code == CC))).scalars() if e.action.startswith("task."))
        assert actions == ["task.done", "task.run_now", "task.skip"]
    # Step 3: The rows the tab reads: the Today list and the runs, newest first, blocks on the newest
    tasks_row = _row(mirror, "TASKS")
    assert tasks_row["counts"]["due_today"] == 1 and [t["is_done"] for t in tasks_row["today"]] == [False, True, True]
    assert {r["shape"] for r in tasks_row["schedules"] + tasks_row["reminders"]} == {"check", "action"}
    assert next(r for r in tasks_row["reminders"] if r["title"].startswith("Move"))["last_outcome"] == "done"
    runs_row = _row(mirror, "TASK_RUNS")
    assert runs_row["retention_days"] == 30
    assert [(r["title"], r["trigger"], r["outcome"], r["note"]) for r in runs_row["runs"]] == [
        ("Short products list", "tick", "skipped", "bank holiday"), ("Move a picker to Route 14", "tick", "done", "Dan moved at 09:05"),
        ("Short products list", "hand", "sent", None)]
    assert runs_row["runs"][2]["blocks"][0]["type"] == "table" and runs_row["runs"][2]["has_blocks"] is True
    assert runs_row["runs"][2]["text"] == "GINGER was short on 3 releases.\nTwo more lines."


# ==================================================== 3. reuse: plan again, copy, repeat; the purge

async def test_plan_again_copy_and_repeat_reuse_a_routine_and_old_runs_are_purged():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        action = await _reminder(db, title="Chase the ginger approval", note="ask Andy")
        await db.commit()
        action_id = str(action.id)
    await commands.run(_command("u-sup", "task_update", {"task_id": action_id, "action": "done"}), mirror=mirror, now=NOW)
    # Step 1: Plan again makes a new routine with the same content on the new time; the old one keeps its history
    monday = datetime(2026, 11, 16, 5, 30, tzinfo=timezone.utc)
    out = await commands.run(_command("u-sup", "task_update", {"task_id": action_id, "action": "plan_again", "when": {"kind": "once", "at": monday.isoformat()}}), mirror=mirror, now=NOW)
    assert out.text == "planned again · Mon 16 Nov, 05:30 · Chase the ginger approval"
    new_id = out.blocks[0]["reference"]
    assert new_id != action_id and out.blocks[0]["undo"] == {"task_id": new_id, "action": "delete"}
    with pytest.raises(access_commands.Refused, match="already passed"):
        await commands.run(_command("u-sup", "task_update", {"task_id": action_id, "action": "plan_again", "when": {"kind": "once", "at": (NOW - timedelta(hours=1)).isoformat()}}), mirror=mirror, now=NOW)
    # Step 2: Copy as new takes the overrides; Repeat puts the one-off on a weekday clock
    out = await commands.run(_command("u-sup", "task_update", {"task_id": action_id, "action": "copy", "fields": {"title": "Chase the pear approval", "note": "ask Bea"}}), mirror=mirror, now=NOW)
    assert out.text.startswith("copied · ") and out.text.endswith("· Chase the pear approval")
    copy_id = out.blocks[0]["reference"]
    out = await commands.run(_command("u-sup", "task_update", {"task_id": new_id, "action": "repeat", "fields": {"when": {"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "05:30"}}}), mirror=mirror, now=NOW)
    assert out.text == "repeats · Every weekday at 05:30 · Chase the ginger approval"
    with pytest.raises(access_commands.Refused, match="recurring"):
        await commands.run(_command("u-sup", "task_update", {"task_id": copy_id, "action": "repeat", "fields": {"when": {"kind": "once", "at": monday.isoformat()}}}), mirror=mirror, now=NOW)
    async with async_session() as db:
        rows = {t.title: t for t in (await db.execute(select(TeamsTask).where(TeamsTask.customer_code == CC))).scalars()}
        original = await store.get(db, CC, action_id)
        assert original.status == "done" and original.when["kind"] == "once", "the original keeps its history"
        planned = await store.get(db, CC, new_id)
        assert planned.status == "active" and planned.when["kind"] == "recurring" and planned.note == "ask Andy"
        assert planned.source == {"task_id": action_id} and planned.created_via == "plan_again"
        assert rows["Chase the pear approval"].note == "ask Bea" and rows["Chase the pear approval"].created_via == "copy"
        # Step 3: Runs older than the retention go; today's stay
        old = TeamsTaskRun(id=uuid.uuid4(), task_id=planned.id, customer_code=CC, planned_at=NOW - timedelta(days=31),
                           started_at=NOW - timedelta(days=31), finished_at=NOW - timedelta(days=31), outcome="sent", trigger="time")
        db.add(old)
        await db.commit()
    assert await runner.purge_once(retention_days=30, now=NOW) == 1
    assert await runner.purge_once(retention_days=30, now=NOW) == 0
    async with async_session() as db:
        kept = list((await db.execute(select(TeamsTaskRun).where(TeamsTaskRun.customer_code == CC))).scalars())
        assert [(r.trigger, r.outcome) for r in kept] == [("tick", "done")]
    tasks_row = _row(mirror, "TASKS")
    assert tasks_row["counts"]["todo"] == 2 and sorted(r["created_via"] for r in tasks_row["reminders"]) == ["copy", "plan_again", "tab"]
