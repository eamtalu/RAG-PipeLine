"""Chunk 172 (stage D, step 5): the Teams answer carries the blocks, the times and the freshness.

The consumer stamps every ok answer with when it was asked (the job's enqueue time), when it was
answered, and when the figures are from and whether they are still moving; the agent's blocks
ride along as they are. An old answer shape (no blocks, no times) still posts.
"""

from datetime import datetime, timezone

from tests.test_teams_consumer_chunk123 import Harness, job

CHART = {"type": "chart", "kind": "bar", "title": "t", "labels": ["a"],
         "series": [{"name": "s", "values": [1.0]}], "stacked": False, "status": False}


async def test_an_answer_carries_its_blocks_times_and_freshness():
    async def agent(code, question, history):
        return {"answer": "One item was short.", "tool_calls": [{"tool": "aggregate_releases", "input": {}}],
                "blocks": [CHART, {"type": "followups", "items": ["Why?"]}]}

    async def read_freshness(code):
        return {"data_as_of": datetime(2026, 11, 12, 17, 0, tzinfo=timezone.utc), "provisional": True}

    h = Harness([job(question="short, as a chart?")], agent=agent, read_freshness=read_freshness)
    await h.drain()
    payload = h.poster.posted[0]
    assert payload.status == "ok" and [b.type for b in payload.blocks] == ["chart", "followups"]
    assert payload.blocks[0].series[0].values == [1.0]
    assert payload.asked_at is not None and payload.answered_at is not None
    assert payload.answered_at >= payload.asked_at
    assert payload.data_as_of == datetime(2026, 11, 12, 17, 0, tzinfo=timezone.utc)
    assert payload.provisional is True


async def test_an_old_result_shape_without_blocks_still_posts():
    async def agent(code, question, history):
        return {"answer": "Fine.", "tool_calls": []}

    h = Harness([job(question="hello")], agent=agent)
    await h.drain()
    payload = h.poster.posted[0]
    assert payload.blocks == [] and payload.provisional is False and payload.data_as_of is None
    assert payload.answered_at is not None
