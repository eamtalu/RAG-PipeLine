"""The advisories table: findings become rows (one per dedup_key, kept open while the agent keeps
finding them, resolved when it stops), people act on them, and the edge reads them as one payload.
Used by runner.py (findings in), actions.py (people acting) and mirror.py (the row the edge reads).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.advisory import ADVISORY_STATUSES, SEVERITIES, Advisory
from app.services.advisories.registry import Finding

OPEN_STATUSES = ("open", "acknowledged", "snoozed")
RESOLVED_BY_AGENT = "cleared"
SEVERITY_RANK = {s: i for i, s in enumerate(SEVERITIES)}


async def reconcile(db: AsyncSession, customer_code: str, agent: str, findings: list[Finding], *,
                    now: datetime, reannounce_after: timedelta | None = None) -> tuple[int, int]:
    """Apply one run's findings: new ones open, repeated ones stay open and refresh, absent ones
    resolve; how many opened and resolved. A finding back within `reannounce_after` of its last
    announcement keeps its stamp (step 13), so a count that flaps is not a card every time.

    (2 findings, 1 already open, 1 other open row) -> (1, 1) ; (same findings again) -> (0, 0)
    """
    # Step 1: Load this agent's live rows by dedup_key so repeats refresh rather than duplicate,
    # otherwise the unique constraint fires and the tab shows the same problem twice
    live = {row.dedup_key: row for row in (await db.execute(
        select(Advisory).where(Advisory.customer_code == customer_code, Advisory.agent == agent,
                               Advisory.status.in_(OPEN_STATUSES)))).scalars()}
    opened = 0
    seen: set[str] = set()
    for f in findings:
        seen.add(f.dedup_key)
        row = live.get(f.dedup_key)
        if row is None:
            # Step 2: A finding resolved earlier may come back (a count variance recounted and
            # found again); its old row is reopened so the history stays on one row
            row = await db.scalar(select(Advisory).where(Advisory.customer_code == customer_code,
                                                         Advisory.dedup_key == f.dedup_key))
            if row is None:
                row = Advisory(id=uuid.uuid4(), customer_code=customer_code, agent=agent, dedup_key=f.dedup_key,
                               opened_at=now, status="open")
                db.add(row)
            else:
                row.status, row.opened_at, row.resolved_at, row.resolved_reason = "open", now, None, None
                row.acknowledged_by = row.assignee_object_id = row.assignee_name = None
                row.snoozed_until = None
                # Step 2b: Keep the stamp when the finding was announced recently, otherwise a count
                # that flaps every run would be a card every run and the cap would fill with repeats
                is_recent = (reannounce_after is not None and row.notified_at is not None
                             and row.notified_at > now - reannounce_after)
                if not is_recent:
                    row.notified_at = None
            opened += 1
        # Step 3: Refresh the words and the severity every run, because the delivery's progress
        # and the count's gap move while the row stays open
        row.site_id, row.severity, row.title = f.site_id, f.severity, f.title[:200]
        row.evidence, row.impact, row.recommended = f.evidence, f.impact, f.recommended
        row.confidence, row.due_at, row.source, row.actions = f.confidence, f.due_at, dict(f.source), list(f.actions)
        row.kind, row.about = f.kind, dict(f.about)
        row.last_seen_at = row.updated_at = now
        # Step 4: A snooze ends early when the finding worsens, so a HIGH never hides behind a snooze
        if row.status == "snoozed" and f.severity == "high" and row.snoozed_until and row.snoozed_until > now:
            row.status, row.snoozed_until = "open", None
    # Step 5: Everything the agent no longer finds is cleared, with the reason, so the tab's
    # history says the agent resolved it rather than a person
    resolved = 0
    for key, row in live.items():
        if key not in seen:
            row.status, row.resolved_at, row.resolved_reason, row.updated_at = "resolved", now, RESOLVED_BY_AGENT, now
            resolved += 1
    await db.flush()
    return opened, resolved


async def wake_snoozed(db: AsyncSession, customer_code: str, *, now: datetime) -> int:
    """Snoozes that have run out go back to open; how many woke."""
    rows = list((await db.execute(
        select(Advisory).where(Advisory.customer_code == customer_code, Advisory.status == "snoozed",
                               Advisory.snoozed_until <= now))).scalars())
    for row in rows:
        row.status, row.snoozed_until, row.updated_at = "open", None, now
    await db.flush()
    return len(rows)


async def live_rows(db: AsyncSession, customer_code: str, *, limit: int = 200) -> list[Advisory]:
    """The open, acknowledged and snoozed advisories, worst and newest first."""
    rows = list((await db.execute(
        select(Advisory).where(Advisory.customer_code == customer_code, Advisory.status.in_(OPEN_STATUSES))
        .order_by(Advisory.opened_at.desc()).limit(limit))).scalars())
    return sorted(rows, key=lambda r: (SEVERITY_RANK.get(r.severity, 9), r.status != "open", -r.opened_at.timestamp()))


async def recent_closed(db: AsyncSession, customer_code: str, *, limit: int = 50) -> list[Advisory]:
    """The latest resolved and dismissed advisories, newest first, for the tab's history filter."""
    return list((await db.execute(
        select(Advisory).where(Advisory.customer_code == customer_code, Advisory.status.in_(("resolved", "dismissed")))
        .order_by(Advisory.resolved_at.desc().nulls_last()).limit(limit))).scalars())


async def get(db: AsyncSession, customer_code: str, advisory_id: str) -> Advisory | None:
    try:
        key = uuid.UUID(str(advisory_id))
    except ValueError:
        return None
    return await db.scalar(select(Advisory).where(Advisory.customer_code == customer_code, Advisory.id == key))


async def unnotified(db: AsyncSession, customer_code: str) -> list[Advisory]:
    """Open advisories nobody has been told about yet, oldest first (step 12 routes them by group)."""
    return list((await db.execute(
        select(Advisory).where(Advisory.customer_code == customer_code, Advisory.status == "open",
                               Advisory.notified_at.is_(None))
        .order_by(Advisory.opened_at))).scalars())


def advisory_payload(row: Advisory) -> dict:
    """One advisory as the tab draws it."""
    assert row.status in ADVISORY_STATUSES
    return {
        "id": str(row.id), "agent": row.agent, "severity": row.severity, "status": row.status,
        "kind": row.kind or "other", "about": dict(row.about or {}),
        "site_id": row.site_id, "title": row.title, "evidence": row.evidence, "impact": row.impact,
        "recommended": row.recommended, "confidence": row.confidence,
        "assignee": ({"object_id": row.assignee_object_id, "name": row.assignee_name}
                     if row.assignee_object_id else None),
        "acknowledged_by": row.acknowledged_by,
        "snoozed_until": _iso(row.snoozed_until), "due_at": _iso(row.due_at),
        "source": dict(row.source or {}), "actions": list(row.actions or []),
        "opened_at": _iso(row.opened_at), "updated_at": _iso(row.updated_at), "last_seen_at": _iso(row.last_seen_at),
        "resolved_at": _iso(row.resolved_at), "resolved_reason": row.resolved_reason,
    }


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None
