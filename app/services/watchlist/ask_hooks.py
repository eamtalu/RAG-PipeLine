"""What the consumer adds to an answer for the asker's watchlist (stage D, step 14c): the entity
chips marked watched or not, and at most one suggestion to watch the thing the question was
about, held back when it is already watched, when the person said "Not now" within a day, when
their list is full, or when the thing is outside their sites. Used by app/teams_consumer.py after
the agent returns; a failure here never costs the answer.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.access import records
from app.services.teams.contracts import QuestionJob
from app.services.watchlist import registry, search, store
from app.settings import settings

logger = logging.getLogger(__name__)
ACTION_BLOCKS = {"receipt", "form", "choice"}


def named_in(question: str, items: list[dict]) -> dict | None:
    """The one entity the question names by its id, else the only entity, else None.

    >>> items = [{"kind": "delivery", "id": "D-1"}, {"kind": "route", "id": "R14"}]
    >>> named_in("Where is delivery D-1?", items)["id"], named_in("How is route 14?", items)["id"]
    ('D-1', 'R14')
    >>> named_in("What is short today?", items) is None, named_in("x", items[:1])["id"]
    (True, 'D-1')
    """
    words = question.lower()
    for item in items:
        key = str(item["id"]).lower()
        if re.search(rf"(?<![\w-]){re.escape(key)}(?![\w-])", words):
            return item
        if item["kind"] == "route" and re.search(rf"route\s*{re.escape(search.code_of(key).lower())}(?!\d)", words):
            return item
    return items[0] if len(items) == 1 else None


async def decorate(result: dict, job: QuestionJob, *, session_factory=None, now: datetime | None = None) -> dict:
    """Mark the answer's entities as watched or not and add the one suggestion the rules allow;
    the result, changed in place.

    (answer naming D-1, not watched, list not full) -> entities[0].watched False + a watch_suggestion ;
    (D-1 watched) -> watched True, no suggestion ; (an action turn) -> entities marked, no suggestion
    """
    from app.config.database import async_session

    blocks = list(result.get("blocks") or [])
    entities = next((b for b in blocks if b.get("type") == "entities"), None)
    if entities is None:
        return result
    now = now or datetime.now(timezone.utc)
    factory = session_factory or async_session
    async with factory() as db:
        record = await records.access_record_for(db, job.customer_code, job.sender_object_id)
        if record is None or not record.is_active:
            return result
        tz = ZoneInfo(await get_customer_timezone(db, job.customer_code))
        watched = {(w.kind, w.entity_id): w for w in await store.active_for_owner(db, job.customer_code, job.sender_object_id)}
        # Step 1: Every chip says whether it is watched and whether it may be, so the page draws
        # the right state of the eye without a second read
        for item in entities["items"]:
            key = (item["kind"], item["id"])
            item["watched"] = key in watched
            item["can_watch"] = True
            item["site_id"] = None
            if key not in watched and item["kind"] in ("delivery", "route"):
                try:
                    resolved = await search.resolve(db, job.customer_code, record, item["kind"], item["id"], tz=tz)
                    item["site_id"] = resolved.site_id
                except search.NoAccess:
                    item["can_watch"] = False
                except search.NotFound:
                    item["can_watch"] = False
        # Step 2: One suggestion at most, for the thing the question named, and only when the
        # rules of decision 17 allow it
        if any(b.get("type") in ACTION_BLOCKS for b in blocks):
            return result
        target = named_in(job.question, entities["items"])
        if target is None or target["watched"] or not target["can_watch"]:
            return result
        if len(watched) >= int(settings.teams_watches_limit):
            return result
        if await store.is_not_now(db, job.customer_code, job.sender_object_id, target["kind"], target["id"], now=now):
            return result
        alert = registry.alert_text(target["kind"], registry.default_alert(target["kind"]))
        blocks.append({"type": "watch_suggestion", "entity": dict(target),
                       "text": f"Keep an eye on {target['label']} for you?", "default_alert": alert[:120]})
        result["blocks"] = blocks[:8]
    return result


async def decorate_safely(result: dict, job: QuestionJob) -> dict:
    """decorate(), never raising: an answer is worth more than its chips."""
    try:
        return await decorate(result, job)
    except Exception:
        logger.warning("watch hooks skipped for %s", job.job_id, exc_info=True)
        return result
