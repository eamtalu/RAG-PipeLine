"""The forecast tool (stage D, step 18): the agent reads the same figures Home's forecast card
shows (app/services/teams/home_forecast.py), so "what is coming tomorrow" and "which day is
busiest this week" are answered in the card's own words and never with a number the stores did
not give. Every figure comes back as a row, so the figure guards can find it.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.teams import home_forecast
from app.services.teams.sites import list_sites

FORECAST_AHEAD = "forecast_ahead"
FORECAST_TOOL_NAMES = frozenset({FORECAST_AHEAD})
MAX_DAYS = 7

FORECAST_TOOLS: list[dict[str, Any]] = [
    {
        "name": FORECAST_AHEAD,
        "description": (
            "The forecast for the days ahead, as the Home tab's forecast card shows it: for each day the "
            "expected lines (the middle estimate and the p10 to p90 range), which day ahead is busiest, "
            "the next shifts' peak hour with the pickers and lines it needs, how accurate the forecast "
            "has been, and a note on its limits. Call it for questions about tomorrow, the week ahead, "
            "tonight's peak, how many pickers are needed, or how good the forecast is. Answer only with "
            "the figures it returns and repeat its note when the figures are provisional or a backtest."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "How many days ahead, 1 to 7; 7 when not said."},
                "site_id": {"type": "string", "description": "A site id to narrow to; the whole customer when not said."},
            },
            "additionalProperties": False,
        },
    }
]

RECIPES: list[dict[str, Any]] = [
    {"question": "What is coming tomorrow?", "call": {"days": 1}},
    {"question": "Which day is busiest this week?", "call": {"days": 7}},
    {"question": "When is tonight's peak, and how many pickers does it need?", "call": {"days": 1}},
]


def rows_of(block: Mapping[str, Any], days: int) -> dict[str, Any]:
    """The card's block as the model's rows: the days ahead, the busiest, the shifts, the accuracy.

    >>> rows_of({"available": False, "note": "no run yet"}, 3)
    {'available': False, 'note': 'no run yet', 'days': [], 'shifts': []}
    """
    if not block.get("available", True) and not block.get("days"):
        return {"available": False, "note": block.get("note"), "days": [], "shifts": []}
    ahead = [d for d in block.get("days", []) if d.get("kind") == "ahead"][:days]
    day_rows = [{"day": d["label"], "lines_expected": d.get("p50"), "lines_expected_text": d.get("p50_text"),
                 "range_text": d.get("range_text")} for d in ahead]
    with_figures = [d for d in day_rows if d["lines_expected"] is not None]
    busiest = max(with_figures, key=lambda d: d["lines_expected"]) if with_figures else None
    shifts = [{"shift": s["label"], "peak_hour": s.get("peak_hour") or None, "peak_pickers": s.get("peak_pickers"),
               "peak_lines_text": s.get("peak_lines_text") or None, "picker_hours": s.get("picker_hours"),
               "quiet": s.get("quiet")} for s in block.get("shifts", [])[:max(1, days)]]
    tiles = {t["id"]: t for t in block.get("tiles", [])}
    return {
        "available": True,
        "days": day_rows,
        "busiest_day": busiest["day"] if busiest else None,
        "shifts": shifts,
        "accuracy": {"value": tiles.get("accuracy", {}).get("value"), "caption": tiles.get("accuracy", {}).get("caption")},
        "week": {"value": tiles.get("week", {}).get("value"), "caption": tiles.get("week", {}).get("caption")},
        "note": block.get("note"),
    }


async def run_forecast_tool(db: AsyncSession, customer_code: str, name: str, args: Mapping[str, Any]) -> str:
    """Run the forecast tool: the rows as JSON, or the problem.

    ("forecast_ahead", {"days": 1}) -> '{"available": true, "days": [...], ...}'
    """
    if name != FORECAST_AHEAD:
        return json.dumps({"problems": [f"unknown forecast tool {name!r}"]})
    try:
        days = max(1, min(MAX_DAYS, int(args.get("days") or MAX_DAYS)))
    except (TypeError, ValueError):
        days = MAX_DAYS
    site = None
    wanted = str(args.get("site_id") or "").strip()
    if wanted:
        site = next((s for s in await list_sites(db, customer_code) if s.site_id == wanted), None)
        if site is None:
            return json.dumps({"problems": [f"no site {wanted!r}; leave site_id out for the whole customer"]})
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    block = await home_forecast.compute(db, customer_code, datetime.now(timezone.utc), tz, site)
    return json.dumps(rows_of(block, days), default=str)
