"""The advisory tool (fix of 13 November 2026): `explain_advisory` gives the model the finding's
own words and the facts the agent read, so "Ask why" on an advisory is answered from the tables
rather than from a search of the logs that finds nothing and ends at the iteration cap. The id
comes with the question (the edge sends it as the job's context; the consumer frames it).
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.advisories import explain as advisory_explain

EXPLAIN_ADVISORY = "explain_advisory"
ADVISORY_TOOL_NAMES = frozenset({EXPLAIN_ADVISORY})

ADVISORY_TOOLS: list[dict[str, Any]] = [
    {
        "name": EXPLAIN_ADVISORY,
        "description": (
            "The advisory the person is asking about and the facts behind it: its title, evidence, "
            "impact and recommended action, plus the rows the agent read (today's stock counts of "
            "the item at the location and the picks from it; the zero-picks per item at a location; "
            "the item's short releases this week; the at-risk board's row for a delivery). Call it "
            "FIRST, with the advisory id given with the question, whenever the question is about an "
            "advisory ('Ask why'); answer from what it returns. Do not search the logs for it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"advisory_id": {"type": "string", "description": "The advisory's id, as given with the question."}},
            "required": ["advisory_id"],
            "additionalProperties": False,
        },
    }
]


async def run_advisory_tool(db: AsyncSession, customer_code: str, name: str, args: Mapping[str, Any]) -> str:
    """Run the advisory tool: the explanation as JSON, or an error the model can read.

    ("explain_advisory", {"advisory_id": "…"}) -> '{"advisory": {...}, "facts": {...}, "note": "…"}'
    """
    if name != EXPLAIN_ADVISORY:
        return json.dumps({"error": f"unknown advisory tool {name!r}"})
    advisory_id = str(args.get("advisory_id") or "").strip()
    if not advisory_id:
        return json.dumps({"error": "advisory_id is required; it is given with the question"})
    return json.dumps(await advisory_explain.explain(db, customer_code, advisory_id), default=str)
