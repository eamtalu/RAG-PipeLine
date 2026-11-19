"""Step 41 (D41) chunk 217: every DynamoDB row that carries an answer's blocks stays under the 400 KB
item limit when the answer holds the largest people-by-day heat map the contract allows. A person's
routine rows: the History row keeps the blocks of the newest runs while they fit a byte budget; the
Routines row carries no blocks at all, because the page reads only the last result's words. One
answered question's `history_turn` row holds one answer, so one grid."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from app.config.database import async_session
from app.services.tasks import commands
from app.services.teams import history
from app.services.teams.contracts import AnswerPayload, FindingsBlock, KpisBlock, QuestionJob
from app.services.tasks import mirror as tasks_mirror
from app.services.teams.contracts import HeatmapBlock
from app.services.teams.mirror import InMemoryMirrorWriter
from tests.test_teams_tasks_chunk180 import CC, NOW, _command, _customer, _reminder  # noqa: F401

pytestmark = pytest.mark.usefixtures("_customer")

DYNAMODB_ITEM_LIMIT = 400 * 1024


def _largest_grid() -> dict:
    """The biggest heat map the contract allows: 62 people by 48 days, two layers, four-digit counts."""
    values = [[1000 + r + c for c in range(48)] for r in range(62)]
    layer = {"values": values, "unit": "logins", "scale": [1, 2, 4, 8]}
    return HeatmapBlock(
        title="Logins and transactions per user per day", rows=[f"Person number {r:02d} (USER{r:04d})" for r in range(62)],
        columns=[f"Mo {c:02d}/11" for c in range(48)], **layer, totals=True,
        layers=[{"label": "Logins", **layer}, {"label": "Transactions", **layer, "unit": "transactions"}],
    ).model_dump(mode="json")


async def _grid_answer(task):
    return {"answer": "Twelve people logged in.", "blocks": [_largest_grid()]}


def _row_bytes(mirror: InMemoryMirrorWriter, suffix: str) -> tuple[int, dict]:
    env = mirror.items[(f"EYE#{CC}", f"USER#u-sup#{suffix}")]
    return len(env.model_dump_json().encode()), json.loads(env.payload_json)


# ==================================================== 1. the budget


def test_the_newest_runs_keep_their_blocks_while_they_fit_the_budget():
    small = {"blocks": [{"type": "text", "text": "x"}], "has_blocks": True}
    big = {"blocks": [{"type": "text", "text": "y" * 600}], "has_blocks": True}
    runs = [dict(big), dict(small), dict(big), dict(small)]
    kept = tasks_mirror.fit_run_blocks(runs, budget_bytes=1000, max_runs=10)
    # newest first: the first big run fits, the second big one would pass the budget
    assert ["blocks" in r for r in kept] == [True, True, False, True]
    assert all(r["has_blocks"] for r in kept), "a run without its blocks still says it has some"
    # never more than max_runs carry blocks, however small
    assert sum("blocks" in r for r in tasks_mirror.fit_run_blocks([dict(small) for _ in range(5)], budget_bytes=10_000, max_runs=3)) == 3


# ==================================================== 2. the rows


async def test_ten_runs_of_the_largest_grid_keep_both_rows_under_the_dynamodb_item_limit():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        check = await _reminder(db, kind="schedule", title="Logins per user per day", query_text="logins per user per day",
                                when={"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "16:00"})
        await db.commit()
        check_id = str(check.id)
    for i in range(10):
        await commands.run(_command("u-sup", "task_update", {"task_id": check_id, "action": "run_now"}),
                           mirror=mirror, now=NOW + timedelta(minutes=i), run_query=_grid_answer)
    runs_bytes, runs_row = _row_bytes(mirror, "TASK_RUNS")
    tasks_bytes, tasks_row = _row_bytes(mirror, "TASKS")
    assert runs_bytes < DYNAMODB_ITEM_LIMIT and tasks_bytes < DYNAMODB_ITEM_LIMIT
    assert len(runs_row["runs"]) == 10 and all(r["has_blocks"] for r in runs_row["runs"])
    assert runs_row["runs"][0]["blocks"][0]["type"] == "heatmap", "the newest run keeps its grid"
    assert not all("blocks" in r for r in runs_row["runs"]), "the oldest grids are left to the ledger"
    # the Routines row keeps the last result's words and drops its blocks
    last = tasks_row["schedules"][0]["last_result"]
    assert last["text"] == "Twelve people logged in." and "blocks" not in last


async def test_one_answer_with_the_largest_grid_fits_its_history_turn_row():
    mirror = InMemoryMirrorWriter()
    job = QuestionJob(job_id="j-grid", tenant_id="t", customer_code=CC, conversation_id="c", sender_object_id="u-sup",
                      sender_name="Sam", question="logins per user per day for the last month", site_id=None, source="tab")
    answer = AnswerPayload(
        job_id="j-grid", conversation_id="c", status="ok", answer="x" * 4000,
        blocks=[_largest_grid(),
                KpisBlock(items=[{"label": f"Figure {i}", "value": "1,234"} for i in range(6)]),
                FindingsBlock(findings=[{"text": "y" * 300, "reasoning": "z" * 300, "source": "wms_activity"}] * 6)],
        asked_at=NOW, answered_at=NOW, data_as_of=NOW, provisional=False)
    await history.mirror_turn(mirror, job, answer, now=NOW)  # the envelope refuses a payload over its cap
    env = mirror.items[(f"EYE#{CC}", "USER#u-sup#TURN#j-grid")]
    assert len(env.model_dump_json().encode()) < DYNAMODB_ITEM_LIMIT // 2
