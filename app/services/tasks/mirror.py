"""The `USER#<id>#TASKS` row the edge reads (stage D, step 10): one person's reminders and
schedules, what is coming up next, the counts the tab shows, and the suggestions made from their
own history (the same question at about the same time on most recent weekdays). Since step 15
also the Today list and, beside it, the `USER#<id>#TASK_RUNS` row with the runs of the last 30
days. Rewritten after every change and firing, and by the consumer's sweep every minute
(unchanged rows are skipped).
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import distinct, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.task import TeamsTask, TeamsTaskProposal
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.persistence.repositories.teams_repository import TeamsConversationRepository
from app.services import preferences
from app.services.tasks import proposals, store
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed
from app.settings import settings

logger = logging.getLogger(__name__)

SUGGEST_DAYS = 7
SUGGEST_MIN_DAYS = 4
SUGGEST_WINDOW_MINUTES = 45
COMING_UP = 6
RUNS_WITH_BLOCKS = 10
RUNS_LIMIT = 200
#: The bytes of blocks the History row may carry. A row's payload is capped at 390,000 characters
#: (`MirrorEnvelope`, under DynamoDB's 400 KB item); 200 runs of words take about 150 KB, so the
#: blocks get the rest with room to spare (step 41: one month grid is up to about 55 KB).
RUN_BLOCKS_BUDGET_BYTES = 150_000


def normalise(question: str) -> str:
    """A question reduced to the words that make it the same question.

    >>> normalise("Which deliveries are at risk?"), normalise("which deliveries are at risk")
    ('which deliveries are at risk', 'which deliveries are at risk')
    """
    return re.sub(r"[^a-z0-9 ]+", "", question.lower()).strip()


def suggestions(exchanges: list[tuple[str, datetime]], *, tz: ZoneInfo, scheduled: set[str],
                min_days: int = SUGGEST_MIN_DAYS) -> list[dict]:
    """Questions asked at about the same time on at least `min_days` distinct weekdays of the
    window, as schedule suggestions; a question already scheduled is left out.

    >>> tz = ZoneInfo("Europe/London")
    >>> asked = [("Which deliveries are at risk?", datetime(2026, 11, 9 + d, 15, m, tzinfo=timezone.utc))
    ...          for d, m in ((0, 0), (1, 10), (2, 5), (3, 20))]
    >>> [s["time"] for s in suggestions(asked, tz=tz, scheduled=set())], suggestions(asked[:3], tz=tz, scheduled=set())
    (['15:00'], [])
    """
    by_question: dict[str, list[datetime]] = defaultdict(list)
    wording: dict[str, str] = {}
    for question, at in exchanges:
        key = normalise(question)
        if key and key not in scheduled:
            by_question[key].append(at.astimezone(tz))
            wording.setdefault(key, question.strip())
    out = []
    for key, times in by_question.items():
        # Step 1: The time of day is the earliest asking; days count when their asking falls
        # within the window of it, so "about 15:00" is one habit and "9:00 and 15:00" is not
        anchor = min(times, key=lambda t: t.hour * 60 + t.minute)
        anchor_minutes = anchor.hour * 60 + anchor.minute
        days = {t.date() for t in times if abs(t.hour * 60 + t.minute - anchor_minutes) <= SUGGEST_WINDOW_MINUTES
                and t.weekday() < 5}
        if len(days) >= min_days:
            out.append({"question": wording[key], "time": f"{anchor.hour:02d}:{anchor.minute:02d}",
                        "days": len(days), "weekdays": True})
    return out


async def user_payload(db: AsyncSession, customer_code: str, owner_object_id: str, *, now: datetime) -> dict:
    """The whole row for one person."""
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    tasks = await store.list_for_owner(db, customer_code, owner_object_id)
    rows = [store.task_payload(t, tz) for t in tasks]
    reminders = [r for r in rows if r["kind"] == "reminder"]
    schedules = [r for r in rows if r["kind"] == "schedule"]
    local_today = now.astimezone(tz).date()
    done_today = [r for r in reminders if r["status"] == "done" and r["done_at"]
                  and datetime.fromisoformat(r["done_at"]).astimezone(tz).date() == local_today]
    coming = sorted((r for r in rows if r["status"] == "active" and r["next_fire_at"]), key=lambda r: r["next_fire_at"])
    today = today_list(rows, tz=tz, now=now)
    # Step 1: The suggestions come from the person's own last week of questions, so nothing is
    # suggested that they did not themselves ask
    repo = TeamsConversationRepository(db)
    pairs = await repo.user_exchanges(customer_code, owner_object_id, since=now - timedelta(days=SUGGEST_DAYS), limit=200)
    asked = [(q.content, q.asked_at or q.created_at) for q, _ in pairs if q.content]
    scheduled = {normalise(t.query_text) for t in tasks if t.query_text and t.status == "active"}
    # Step 2: What a colleague set up for them and what they set up for others (program step
    # 3), and how much room the customer's limit leaves. The colleagues to propose to are not
    # here: the edge takes them from the GROUPS row, so a person with no routine yet has them.
    return {
        "owner_object_id": owner_object_id,
        "as_of": now.isoformat(),
        "timezone": str(tz),
        "counts": {"todo": sum(1 for r in reminders if r["status"] == "active"), "done_today": len(done_today),
                   "running": sum(1 for r in schedules if r["status"] == "active"),
                   "due_today": sum(1 for t in today if not t["is_done"])},
        "reminders": reminders,
        "schedules": schedules,
        "coming_up": coming[:COMING_UP],
        "today": today,
        "suggested": suggestions(asked, tz=tz, scheduled=scheduled),
        "proposals": await proposals.for_person(db, customer_code, owner_object_id, tz=tz),
        "limit": {"max_active": await preferences.max_active_routines(db, customer_code),
                  "active": sum(1 for r in rows if r["status"] == "active")},
    }


def today_list(rows: list[dict], *, tz: ZoneInfo, now: datetime) -> list[dict]:
    """The Today view (step 15): every routine due today in time order, then the ones already
    done or run today, each as {id, at, is_done, is_overdue}.

    (active at 17:45 today, done at 09:10 today, active Monday) -> [17:45 row, 09:10 row done]
    """
    local_today = now.astimezone(tz).date()

    def local(value: str | None) -> datetime | None:
        return datetime.fromisoformat(value).astimezone(tz) if value else None

    due, done = [], []
    for r in rows:
        next_at, last = local(r.get("next_fire_at")), r.get("last_result") or {}
        last_at = local(last.get("at"))
        if r["status"] == "active" and next_at is not None and next_at.date() == local_today:
            due.append({"id": r["id"], "at": r["next_fire_at"], "is_done": False, "is_overdue": next_at < now.astimezone(tz)})
        if last_at is not None and last_at.date() == local_today and last.get("outcome") in ("sent", "done", "skipped"):
            done.append({"id": r["id"], "at": last.get("at"), "is_done": True, "is_overdue": False, "outcome": last.get("outcome")})
    due.sort(key=lambda t: t["at"])
    done.sort(key=lambda t: t["at"], reverse=True)
    return due + done


def fit_run_blocks(runs: list[dict], *, budget_bytes: int, max_runs: int) -> list[dict]:
    """The runs, newest first, with blocks kept on the newest ones while they fit the byte budget and
    at most `max_runs` of them; a run past either keeps `has_blocks`, so the page says so.

    ([big 600 B, small, big, small], budget 1000) -> blocks on [yes, yes, no, yes]
    """
    used, kept = 0, 0
    out = []
    for run in runs:
        blocks = run.get("blocks")
        if blocks is None:
            out.append(run)
            continue
        size = len(json.dumps(blocks, default=str).encode())
        if kept < max_runs and used + size <= budget_bytes:
            used, kept = used + size, kept + 1
            out.append(run)
        else:
            out.append({k: v for k, v in run.items() if k != "blocks"})
    return out


async def runs_payload(db: AsyncSession, customer_code: str, owner_object_id: str, *, now: datetime) -> dict:
    """The USER#<id>#TASK_RUNS row (step 15): the person's runs of the retention window, newest
    first; the newest few carry their blocks while they fit the row, the rest say they have some."""
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    since = now - timedelta(days=settings.teams_task_runs_retention_days)
    pairs = await store.list_runs(db, customer_code, owner_object_id, since=since, limit=RUNS_LIMIT)
    return {
        "owner_object_id": owner_object_id,
        "as_of": now.isoformat(),
        "retention_days": settings.teams_task_runs_retention_days,
        "runs": fit_run_blocks([store.run_payload(run, task, tz, with_blocks=i < RUNS_WITH_BLOCKS)
                                for i, (run, task) in enumerate(pairs)],
                               budget_bytes=RUN_BLOCKS_BUDGET_BYTES, max_runs=RUNS_WITH_BLOCKS),
    }


async def mirror_user(db: AsyncSession, mirror: MirrorWriter, customer_code: str, owner_object_id: str, *,
                      tracker: ChangeTracker | None = None, now: datetime | None = None) -> bool:
    """Rewrite one person's USER#<id>#TASKS row; whether it was written (unchanged is skipped)."""
    now = now or datetime.now(timezone.utc)
    payload = await user_payload(db, customer_code, owner_object_id, now=now)
    env = envelope("user_tasks", customer_code, payload=payload, as_of=payload["as_of"], now=now.timestamp(),
                   user_id=owner_object_id)
    written = await write_if_changed(mirror, tracker, env, now=now.timestamp())
    # step 15: the runs row travels with the tasks row, so History is never behind a change
    runs = await runs_payload(db, customer_code, owner_object_id, now=now)
    runs_env = envelope("user_task_runs", customer_code, payload=runs, as_of=runs["as_of"], now=now.timestamp(),
                        user_id=owner_object_id)
    await write_if_changed(mirror, tracker, runs_env, now=now.timestamp())
    return written


async def owners_with_tasks(db: AsyncSession, customer_code: str) -> list[str]:
    owners = set((await db.execute(
        select(distinct(TeamsTask.owner_object_id)).where(TeamsTask.customer_code == customer_code))).scalars())
    # someone who only has a proposal, waiting for them or sent by them, has a row too
    for column in (TeamsTaskProposal.receiver_object_id, TeamsTaskProposal.sender_object_id):
        owners.update((await db.execute(
            select(distinct(column)).where(TeamsTaskProposal.customer_code == customer_code))).scalars())
    return sorted(owners)


async def sweep_once(mirror: MirrorWriter, *, only: list[str] | None = None, tracker: ChangeTracker | None = None,
                     session_factory=None, now: datetime | None = None) -> dict[str, int]:
    """Rewrite every task row of every bound customer (or `only`); rows written per customer."""
    from app.config.database import async_session
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    factory = session_factory or async_session
    now = now or datetime.now(timezone.utc)
    written: dict[str, int] = {}
    async with factory() as db:
        if only is None:
            codes = sorted({b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        else:
            codes = list(only)
        for code in codes:
            try:
                count = 0
                for owner in await owners_with_tasks(db, code):
                    count += int(await mirror_user(db, mirror, code, owner, tracker=tracker, now=now))
                written[code] = count
            except Exception:
                logger.exception("task rows for %s not mirrored", code)
    return written
