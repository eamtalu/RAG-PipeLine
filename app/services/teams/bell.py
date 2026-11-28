"""The Teams bell (program step 3, slice 4): telling one person something under their Activity
bell.

The backend decides what to tell whom and writes it as a `teams_notifications` row in the same
transaction as what it is about. `deliver_pending` hands the queued rows to the edge, which
speaks to Microsoft and holds the Microsoft secret; this server never does. The edge's answer
decides what becomes of a row: sent, refused (never tried again), tried again later, or the
whole bell switched off because the edge is not set up. Used by the tasks runner (queueing) and
the consumer's tasks loop (delivering).
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.teams_notification import KINDS, TeamsNotification
from app.persistence.repositories.teams_repository import TeamsBindingRepository

logger = logging.getLogger(__name__)

#: Teams shows this much of the preview line.
PREVIEW_CHARS = 150
MAX_ATTEMPTS = 3
#: A notification older than this is not sent late: a Monday restart must not ring Friday's runs.
MAX_AGE = timedelta(hours=12)
BATCH = 20


@dataclass(frozen=True)
class Handed:
    """What the edge said about one notification."""

    #: sent | refused | retry | not_configured
    outcome: str
    reason: str | None = None


class BellPoster(Protocol):
    async def post(self, payload: dict) -> Handed:
        """Hand one notification to the edge; what it said.

        (a person with the app installed) -> Handed("sent") ; (Microsoft said no) ->
        Handed("refused", why) ; (the edge is down) -> Handed("retry", why)
        """


def first_line(text: str | None) -> str:
    """The first line of an answer, cut to what Teams shows.

    >>> first_line("42 picks are open\\nBrighton, as of 14:00"), first_line(None), len(first_line("x" * 400))
    ('42 picks are open', '', 150)
    """
    return (text or "").strip().split("\n", 1)[0].strip()[:PREVIEW_CHARS]


async def queue(db: AsyncSession, customer_code: str, *, object_id: str, kind: str,
                subject_id: str, title: str, text: str, tab: str = "home",
                now: datetime | None = None) -> None:
    """Write one notification for one person, once per kind and subject; nothing commits here.

    ("u-sam", "routine_ran", run 9, "Open picks", "42 picks are open") -> one queued row ;
    (the same run again) -> nothing more
    """
    if kind not in KINDS:
        raise ValueError(f"unknown notification kind {kind!r}")
    await db.execute(pg_insert(TeamsNotification).values(
        id=uuid.uuid4(), customer_code=customer_code, object_id=object_id, kind=kind,
        subject_id=subject_id, title=title[:200], text=first_line(text), tab=tab, status="queued",
        attempts=0, created_at=now or datetime.now(timezone.utc),
    ).on_conflict_do_nothing(constraint="uq_teams_notifications_subject"))


def handed_from(status_code: int, body: dict) -> Handed:
    """The edge's HTTP answer as what to do with the notification.

    >>> handed_from(200, {"sent": True}), handed_from(502, {}).outcome
    (Handed(outcome='sent', reason=None), 'retry')
    >>> handed_from(200, {"sent": False, "reason": "Forbidden"})
    Handed(outcome='refused', reason='Forbidden')
    >>> handed_from(503, {"detail": "the bell is not set up"}).outcome
    'not_configured'
    """
    detail = str(body.get("reason") or body.get("detail") or "")[:300] or None
    if status_code == 503:
        return Handed("not_configured", detail or "the bell is not set up on the edge")
    if 200 <= status_code < 300:
        return Handed("sent") if body.get("sent") else Handed("refused", detail or "not sent")
    if 400 <= status_code < 500:
        # the edge refused the request itself; sending the same again cannot help
        return Handed("refused", detail or f"edge HTTP {status_code}")
    return Handed("retry", detail or f"edge HTTP {status_code}")


class HttpBellPoster:
    """POST a notification to the edge with the shared secret; one try, the ledger retries."""

    def __init__(self, url: str, secret: str, *, timeout_seconds: float = 15.0):
        if not url or not secret:
            raise ValueError("edge notifications URL and shared secret are required")
        self._url, self._secret, self._timeout = url, secret, timeout_seconds

    async def post(self, payload: dict) -> Handed:
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(self._url, json=payload,
                                         headers={"X-Edge-Secret": self._secret})
        except httpx.HTTPError as exc:
            return Handed("retry", f"{type(exc).__name__}: {exc}"[:300])
        try:
            body = resp.json()
        except ValueError:
            body = {}
        return handed_from(resp.status_code, body if isinstance(body, dict) else {})


def notifications_url(answers_url: str) -> str:
    """The edge's notification route, beside the answers route it already serves.

    >>> notifications_url("https://edge.example/internal/answers")
    'https://edge.example/internal/notifications'
    >>> notifications_url("")
    ''
    """
    return answers_url.rsplit("/", 1)[0] + "/notifications" if answers_url else ""


async def deliver_pending(poster: BellPoster, *, session_factory=None, now: datetime | None = None,
                          only: list[str] | None = None) -> dict[str, int]:
    """Hand the queued notifications to the edge, oldest first; how many ended in each outcome.

    (one queued, the edge sends it) -> {"sent": 1} ; (the edge has no Microsoft secret) -> every
    queued row becomes not_configured after one try ; (nothing queued) -> {}
    """
    from app.config.database import async_session

    factory = session_factory or async_session
    now = now or datetime.now(timezone.utc)
    counts: dict[str, int] = {}
    async with factory() as db:
        where = [TeamsNotification.status == "queued"]
        if only is not None:
            where.append(TeamsNotification.customer_code.in_(only))
        rows = list((await db.execute(select(TeamsNotification).where(*where).order_by(
            TeamsNotification.created_at).limit(BATCH).with_for_update(skip_locked=True))).scalars())
        # the Entra tenant of each customer, which Microsoft needs to find the person
        tenants = {b.customer_code: b.tenant_id
                   for b in await TeamsBindingRepository(db).list_all() if b.enabled} if rows else {}
        is_off: str | None = None
        for row in rows:
            # Step 1: What would be pointless to try is closed without a call: the bell is not
            # set up (said once this pass), the moment has passed, or the customer has no tenant
            if is_off is not None:
                outcome = Handed("not_configured", is_off)
            elif now - row.created_at > MAX_AGE:
                outcome = Handed("expired", "too old to send late")
            else:
                tenant = tenants.get(row.customer_code)
                if tenant is None:
                    outcome = Handed("refused", "the customer has no Teams tenant bound")
                else:
                    # Step 2: Hand it over; the edge's answer says what becomes of it
                    outcome = await poster.post({
                        "notification_id": str(row.id), "tenant_id": tenant,
                        "user_object_id": row.object_id, "kind": row.kind, "title": row.title,
                        "text": row.text, "tab": row.tab})
                    if outcome.outcome == "not_configured":
                        is_off = outcome.reason or "the bell is not set up on the edge"
            _settle(row, outcome, now=now, counts=counts)
        await db.commit()
    if counts:
        logger.info("bell.delivered %s", " ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    return counts


def _settle(row: TeamsNotification, handed: Handed, *, now: datetime, counts: dict[str, int]) -> None:
    """Write what became of one notification onto its row.

    (sent) -> status sent, sent_at ; (retry, second attempt) -> still queued ;
    (retry, third attempt) -> failed ; (refused) -> refused with the reason
    """
    result = handed.outcome
    if result == "retry":
        row.attempts += 1
        row.error = handed.reason
        if row.attempts >= MAX_ATTEMPTS:
            result, row.status = "failed", "failed"
    elif result == "sent":
        row.status, row.sent_at, row.error = "sent", now, None
    else:
        row.status, row.error = result, handed.reason
    counts[result] = counts.get(result, 0) + 1
