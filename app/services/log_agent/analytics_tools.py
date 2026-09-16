"""The chat agent's window onto analytics (chunk 90, metric builder part 7).

Three tools, each a thin call into the SAME service functions the HTTP endpoints use - `catalog.build`,
`read.resolve`, `read.series`, `read.freshness`. Never SQL of their own: a number the agent quotes must
be the number the dashboard shows, and two code paths to one table is how they come to differ.

What the tools defend, in the module's own words:

*Tenant from the loop.* `customer_code` arrives from the agent, which took it from the request. No tool
takes it as an argument, so the model cannot ask about another logspace by naming it.

*Active only.* `registry.active_definitions` is the gate, exactly as `/series` uses it. A draft has no
rollups and no fixed start; answering from it would be an empty plot dressed as zero activity. The
refusal carries the API's own wording so a person comparing the two sees one message.

*Provenance in every answer.* Grain, whether rollups served it, the spans read live, whether the group-by
fell back to a bounded fact scan, whether the measure is an estimate, and where the metric's history
starts. An agent that quotes "1,240 units" without saying the last hour is provisional has told the
truth in a way that misleads; the fields are there so it need not.

*Bounded.* The window is clamped to `MAX_WINDOW_DAYS`, with a note saying so, because the live tier is a
fact scan and the model has no sense of how expensive a year is.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics import capture
from app.services.analytics import catalog
from app.services.analytics import contract
from app.services.analytics import read as n6
from app.services.analytics import registry
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

#: The longest span one question may read. 92 days covers "this quarter"; the hourly rollups are
#: retained 90 days, and beyond that the daily grain answers anyway.
MAX_WINDOW_DAYS = 92
DEFAULT_WINDOW = timedelta(hours=24)

HOW_TO_READ = (
    "Two-tier read: buckets before the analytics watermark come from pre-aggregated rollups and are "
    "settled; `live_spans` were folded from the fact table on the fly and may still change as "
    "transactions seal. Roles are additive components (sum_value, count_value, distinct_estimate); "
    "divide them yourself for an average and say which spans were provisional."
)

TOOLS: list[dict] = [
    {
        "name": "list_metrics",
        "description": (
            "List the ACTIVE analytics metrics for this logspace: name, what it means, its dimensions "
            "(the fields it can be broken down by, with recently seen values), its measures with units "
            "and whether each is an estimate, its grains, and where its history starts. Call this "
            "before query_metric so you use a real metric and measure name. Drafts are not listed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string",
                         "description": "Optional case-insensitive substring to narrow by metric name."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "query_metric",
        "description": (
            "Read one analytics measure over a time window, optionally broken down by up to four "
            "fields. Returns additive roles per bucket (sum_value, count_value, distinct_estimate) "
            "with provenance: grain, whether rollups served it, which spans were read live and are "
            "provisional, whether the breakdown fell back to a bounded fact scan (ad_hoc), and whether "
            "the measure is an estimate. Use this for 'how many', 'how much', 'per warehouse', "
            "'per day' questions about warehouse activity. For why a single transaction failed use "
            "search_transactions and get_transaction instead."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "metric": {"type": "string", "description": "Metric name, from list_metrics."},
                "measure": {"type": "string",
                            "description": "Measure name; defaults to the metric's first measure."},
                "group_by": {"type": "array", "items": {"type": "string"}, "maxItems": 6,
                             "description": "Fields to break down by, e.g. [\"warehouse\"]. Fields that "
                                            "are not dimensions of the metric fall back to a bounded "
                                            "fact scan and are labelled ad_hoc."},
                "start": {"type": "string", "description": "ISO-8601 start (inclusive). Default: 24 hours before end."},
                "end": {"type": "string", "description": "ISO-8601 end (exclusive). Default: now."},
            },
            "required": ["metric"],
            "additionalProperties": False,
        },
    },
    {
        "name": "explain_freshness",
        "description": (
            "How far analytics is behind the logs and whether the recent tail is still provisional. "
            "Call this when quoting any analytics number so you can say how current it is."
        ),
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
]


# ---------------------------------------------------------------------------------------- helpers

def _iso(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _window(args: dict) -> tuple[UtcWindow, list[str]]:
    """The requested window, bounded. Notes say what was changed and why."""
    notes: list[str] = []
    end = _parse_dt(args.get("end")) or datetime.now(timezone.utc)
    start = _parse_dt(args.get("start")) or (end - DEFAULT_WINDOW)
    if start >= end:
        raise ValueError("`start` must be before `end`.")
    cap = timedelta(days=MAX_WINDOW_DAYS)
    if end - start > cap:
        start = end - cap
        notes.append(f"window clamped to the last {MAX_WINDOW_DAYS} days ending {end.isoformat()}")
    return UtcWindow(start=start, end=end), notes


async def _state(db: AsyncSession, customer_code: str) -> AnalyticsTenantState | None:
    return (await db.execute(select(AnalyticsTenantState).where(
        AnalyticsTenantState.customer_code == customer_code))).scalar_one_or_none()


def _freshness(state: AnalyticsTenantState | None) -> dict:
    f = (n6.freshness(analytics_watermark=state.analytics_watermark,
                      source_watermark=state.source_watermark,
                      unsealed_share=state.unsealed_share,
                      oldest_unsealed_at=state.oldest_unsealed_at)
         if state is not None else
         n6.freshness(analytics_watermark=None, source_watermark=None,
                      unsealed_share=None, oldest_unsealed_at=None))
    return {**f, "analytics_watermark": _iso(f["analytics_watermark"]),
            "source_watermark": _iso(f["source_watermark"]),
            "oldest_unsealed_at": _iso(f["oldest_unsealed_at"]),
            "unsealed_share": None if f["unsealed_share"] is None else str(f["unsealed_share"])}


def _verdict(f: dict) -> str:
    """The freshness in one sentence, so the agent can repeat it rather than interpret four fields."""
    if f["never_folded"]:
        return "analytics has never folded anything for this logspace; there are no numbers to quote"
    lag = f["lag_seconds"]
    parts = [f"analytics is {int(lag)} seconds behind the logs" if lag is not None else
             "the lag is unknown"]
    if f["stale"]:
        parts.append("that is STALE: treat every recent number as out of date")
    if f["provisional"]:
        parts.append("the recent tail is PROVISIONAL: some contributing transactions are still open, "
                     "so those buckets will move")
    if not f["stale"] and not f["provisional"]:
        parts.append("settled")
    return "; ".join(parts)


async def _active(db: AsyncSession, customer_code: str, name: str):
    for definition_id, definition in await registry.active_definitions(db, customer_code):
        if definition.name == name:
            return definition_id, definition
    raise ValueError(f"No ACTIVE metric named {name!r} for this tenant. list_metrics shows what exists.")


# ---------------------------------------------------------------------------------------- the tools

async def list_metrics(db: AsyncSession, args: dict, customer_code: str) -> dict:
    body = await catalog.build(db, customer_code)
    needle = str(args.get("name") or "").strip().lower()
    metrics = [m for m in body["metrics"] if not needle or needle in m["name"].lower()]
    return {"metrics": metrics, "freshness": _freshness(await _state(db, customer_code)),
            "how_to_read": HOW_TO_READ}


async def query_metric(db: AsyncSession, args: dict, customer_code: str) -> dict:
    name = str(args.get("metric") or "").strip()
    definition_id, definition = await _active(db, customer_code, name)
    measure = str(args.get("measure") or "").strip() or definition.measures[0].name
    by_name = {m.name: m for m in definition.measures}
    if measure not in by_name:
        raise ValueError(f"{name!r} has no measure {measure!r}; it has {sorted(by_name)}.")

    raw = args.get("group_by") or ()
    dims = tuple(str(x).strip() for x in (raw if isinstance(raw, (list, tuple)) else [raw]) if str(x).strip())
    decision = n6.resolve(definition, group_by=dims)          # raises ValueError with the API's text
    ad_hoc_attrs = [g for g in dims if contract.is_attr_path(g) and g not in definition.dimensions]
    if ad_hoc_attrs:
        known = await capture.approved_attributes(db, customer_code)
        for g in ad_hoc_attrs:
            if contract.attr_key(g) not in known:
                raise ValueError(f"{g!r} names an attribute that is not approved for capture, so "
                                 f"grouping by it would be silently empty")

    window, notes = _window(args)
    state = await _state(db, customer_code)
    out = await n6.series(db, customer_code, definition_id, definition, window=window, measure=measure,
                          group_by=dims, ad_hoc=decision.ad_hoc,
                          tz=await get_customer_timezone(db, customer_code),
                          watermark=state.analytics_watermark if state else None)
    if out.get("reason"):
        notes.append(out["reason"])
    m = by_name[measure]
    approximate = m.aggregation.value in catalog._APPROXIMATE_AGGREGATIONS
    description = await db.scalar(select(AnalyticsMetric.description).where(
        AnalyticsMetric.customer_code == customer_code, AnalyticsMetric.id == definition_id))
    if m.aggregation.value == "distinct":
        notes.append("distinct_estimate is a HyperLogLog estimate, about 1.6 percent error; it does "
                     "not add across buckets")
    elif m.aggregation.value == "percentile":
        notes.append("p50 and p95 are read out of a 20-band log histogram; each band is a factor of "
                     "two wide, so quote them as approximate and never add them across buckets")
    # Chunk 109. A level is how much there IS at a moment. `sum_value` is still in the roles because
    # the reader divides it to get the average; it is a component, never an answer, and this is the
    # one consumer likely to paste it into a confident sentence.
    if m.field and contract.is_attr_path(m.field):
        levels = await capture.level_fields(db, customer_code)
        subtracts_a_level = bool(m.minus) and contract.is_attr_path(m.minus) \
            and contract.attr_key(m.minus) in levels
        if contract.attr_key(m.field) in levels and not subtracts_a_level:
            notes.append("this measure reads a LEVEL - how much there is at a moment, such as stock "
                         "on hand - so sum_value in the roles is a component of the average, not an "
                         "answer. Never quote it and never add levels across buckets: 73 on-hand "
                         "readings of one item add to 41,206 where 427 are on the shelf")
    if out.get("live_spans"):
        notes.append("buckets inside live_spans were folded from facts on the fly and are provisional")
    return {
        "metric": name, "measure": measure, "aggregation": m.aggregation.value, "field": m.field,
        "unit": m.unit, "approximate": approximate, "description": description,
        "window": {"start": window.start.isoformat(), "end": window.end.isoformat()},
        "grain": out["grain"], "group_by": list(out["group_by"]),
        "from_rollups": out["from_rollups"], "live_spans": out["live_spans"],
        "ad_hoc": decision.ad_hoc, "resolution": decision.reason,
        "rollups_from": out.get("rollups_from"),
        "points": out["points"],
        # Merged by the service from raw roles, so a distinct total is a union, never a sum of estimates.
        "totals": out["totals"], "total": out["total"],
        "notes": notes, "freshness": _freshness(state), "how_to_read": HOW_TO_READ,
    }


async def explain_freshness(db: AsyncSession, args: dict, customer_code: str) -> dict:
    state = await _state(db, customer_code)
    f = _freshness(state)
    return {"configured": state is not None, "freshness": f, "verdict": _verdict(f),
            "queue": ({"open_tickets": state.open_tickets, "abandoned_tickets": state.abandoned_tickets}
                      if state else None),
            "how_to_read": HOW_TO_READ}


DISPATCH = {
    "list_metrics": list_metrics,
    "query_metric": query_metric,
    "explain_freshness": explain_freshness,
}
