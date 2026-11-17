"""Step 39 (chunk 215): /brief and /review, answered from the routine library's reports, no model.

`/brief` is the Daily health check and the Dispatch readiness check for the site the person is on;
`/review` is Site performance, for a record that may see every site. The reports are the ones the
Routines Library shows (`app/services/teams/routine_reports.py`), drawn with Ask's own blocks:
`kpis` for what happened, `findings` for why (findings), what to do (recommendations) and what the
data cannot say yet (limitations). The command is read before the model, as "What can I ask" is, so
no tool and no tool description changes.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.access import records
from app.services.teams import routine_reports
from app.services.teams.sites import ALL_SITES_ID, Site, list_sites

#: Which reports each command (slash.REPORT_COMMANDS) answers with, in reading order.
REPORTS_BY_COMMAND: dict[str, tuple[str, ...]] = {
    "/brief": ("health_check", "dispatch_readiness"),
    "/review": ("site_performance",),
}
SOURCE = "routine library"
TONE = {"attention": "medium", "good": "good", "neutral": "neutral"}
#: The block limits of the answer contract (KpisBlock, FindingsBlock).
MAX_TILES, MAX_FINDINGS, MAX_ACTIONS, MAX_LIMITS = 6, 6, 4, 4


def _cut(text: str | None, limit: int) -> str | None:
    if text is None:
        return None
    return text if len(text) <= limit else text[: limit - 1] + "…"


def blocks_of(report: dict) -> tuple[dict | None, dict]:
    """One report as Ask's blocks: its tiles, then its findings, actions and placeholders, each cut to
    the contract's limits; a report with nothing to flag says its headline as the one finding.

    >>> kpis, f = blocks_of({"title": "T", "headline": "On track", "tiles": [], "findings": [], "actions": ["a"], "placeholders": []})
    >>> kpis, f["findings"][0]["text"], f["recommendations"]
    (None, 'On track', [{'text': 'a', 'reason': None}])
    """
    # Step 1: The tiles as kpis, so the figures read like any analyst answer's
    items = [{"label": _cut(t["label"], 40), "value": _cut(t["value"], 24), "caption": _cut(t.get("sub") or None, 80),
              "tone": TONE.get(t.get("tone"), "neutral")} for t in report["tiles"][:MAX_TILES]]
    kpis = {"type": "kpis", "title": _cut(report["title"], 120), "items": items} if items else None
    # Step 2: The findings, the actions and the placeholders in one findings block, never empty
    findings = [{"text": _cut(f["title"], 300), "reasoning": _cut(f.get("detail") or None, 300), "source": SOURCE}
                for f in report["findings"][:MAX_FINDINGS]]
    if not findings:
        findings = [{"text": _cut(report["headline"], 300), "reasoning": None, "source": SOURCE}]
    block = {"type": "findings", "findings": findings,
             "recommendations": [{"text": _cut(a, 200), "reason": None} for a in report["actions"][:MAX_ACTIONS]],
             "limitations": [_cut(f"{p['label']}: not available yet. {p['note']}", 200)
                             for p in report.get("placeholders", [])[:MAX_LIMITS]]}
    return kpis, block


async def _site(db: AsyncSession, cc: str, site_id: str) -> Site | None:
    return next((s for s in await list_sites(db, cc) if s.site_id == site_id), None)


async def answer(db: AsyncSession, command: str, customer_code: str, site_id: str | None,
                 record: records.AccessRecord | None, *, now: datetime | None = None) -> tuple[str, list[dict]]:
    """The answer to /brief or /review: one lead line and the reports' blocks, or one plain sentence
    when the person may not see what it covers.

    ("/brief", BRI, a supervisor of BRI) -> ("Brighton brief: …", [kpis, findings, kpis, findings]) ;
    ("/review", BRI, a supervisor of BRI) -> ("Site performance compares every site…", [])
    """
    # Step 1: Who may see what: /review covers every site, /brief the site the person is on
    site_id = site_id or ALL_SITES_ID
    if command == "/review":
        if not records.decide(record, "view_all_sites", ALL_SITES_ID).is_allowed:
            return ("Site performance compares every site, so it is for people who may see every site. "
                    "Try /brief for your own site."), []
        site = None
    else:
        capability = "view_all_sites" if site_id == ALL_SITES_ID else "view_own_site"
        if not records.decide(record, capability, site_id).is_allowed:
            return f"{'All sites' if site_id == ALL_SITES_ID else site_id} is not one of your sites. Choose your site and ask /brief again.", []
        site = None if site_id == ALL_SITES_ID else await _site(db, customer_code, site_id)
    # Step 2: The reports, built as the Library builds them, so the two cannot disagree
    row = await routine_reports.compute(db, customer_code, site, now=now)
    if not row["available"]:
        return f"No figures to report yet: {row.get('note') or 'the data could not be read'}.", []
    reports = [row["reports"][rid] for rid in REPORTS_BY_COMMAND[command] if rid in row["reports"]]
    blocks: list[dict] = []
    for report in reports:
        kpis, findings = blocks_of(report)
        blocks += ([kpis] if kpis else []) + [findings]
    # Step 3: One lead line that says what came back, so the answer reads before the blocks
    lead = " · ".join(f"{r['title']}: {r['headline']}" for r in reports)
    prefix = "Site performance:" if command == "/review" else f"{row['scope'][0].upper()}{row['scope'][1:]} brief:"
    if command == "/review":
        lead = reports[0]["headline"] if reports else "no report"
    return f"{prefix} {lead}.", blocks
