"""The `ADVISORIES` row the edge reads (stage D, step 9): the live advisories, the recent closed
ones, the counts the tab badge and the Home strip show, and the roster of agents with their
switches. One row per customer under EYE#<customer>, rewritten after every agent run and after
every action; unchanged payloads are skipped by the change tracker.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.advisory import AdvisoryAgent
from app.services.advisories import kinds, registry, store
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed

logger = logging.getLogger(__name__)


async def roster_payload(db: AsyncSession, customer_code: str) -> list[dict]:
    """The agents as the roster lists them: the registry's words, the customer's switch and cadence."""
    switches = {row.agent: row for row in (await db.execute(
        select(AdvisoryAgent).where(AdvisoryAgent.customer_code == customer_code))).scalars()}
    out = []
    for spec in registry.AGENTS:
        row = switches.get(spec.name)
        out.append({
            "name": spec.name, "label": spec.label, "description": spec.description, "routing": spec.routing,
            "available": spec.run is not None, "off_reason": spec.off_reason,
            "enabled": bool(row.enabled) if row is not None else spec.enabled_by_default,
            "cadence_seconds": row.cadence_seconds if row is not None else spec.cadence_seconds,
            "last_run_at": row.last_run_at.isoformat() if row is not None and row.last_run_at else None,
            "last_result": row.last_result if row is not None else None,
        })
    return out


async def advisories_payload(db: AsyncSession, customer_code: str, *, now: datetime | None = None) -> dict:
    """The whole row: live and recent advisories, the counts, the roster."""
    now = now or datetime.now(timezone.utc)
    live = await store.live_rows(db, customer_code)
    closed = await store.recent_closed(db, customer_code)
    # Step 1: Count what still needs someone (open) by severity and site, so the badge and the
    # Home strip can say "3 open · 1 high" without walking the list
    counts = {"open": 0, "high": 0, "medium": 0, "low": 0, "acknowledged": 0, "snoozed": 0}
    by_site: dict[str, dict] = {}
    for row in live:
        if row.status != "open":
            counts[row.status] += 1
            continue
        counts["open"] += 1
        counts[row.severity] = counts.get(row.severity, 0) + 1
        site = by_site.setdefault(row.site_id or "all", {"open": 0, "high": 0})
        site["open"] += 1
        site["high"] += int(row.severity == "high")
    return {
        "as_of": now.isoformat(),
        "counts": counts,
        "by_site": by_site,
        "kinds": kinds.kinds_payload(live),
        "live": [store.advisory_payload(r) for r in live],
        "recent": [store.advisory_payload(r) for r in closed],
        "roster": await roster_payload(db, customer_code),
    }


async def mirror_customer(db: AsyncSession, mirror: MirrorWriter, customer_code: str, *,
                          tracker: ChangeTracker | None = None, now: datetime | None = None) -> bool:
    """Rewrite one customer's ADVISORIES row; whether it was written (unchanged is skipped)."""
    now = now or datetime.now(timezone.utc)
    payload = await advisories_payload(db, customer_code, now=now)
    env = envelope("advisories", customer_code, payload=payload, as_of=payload["as_of"], now=now.timestamp())
    return await write_if_changed(mirror, tracker, env, now=now.timestamp())
