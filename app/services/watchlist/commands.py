"""What a person does with their watchlist (stage D, step 14), arriving as commands on the
question queue: add a watch (from the eye button, the search, a confirmed Ask form, or a swap when
the list is full), change its alert, stop it (and undo that), mark the day's changes seen, and say
"Not now" to a suggestion. The actor's own record decides; every change is one audit line; the
person's `USER#<id>#WATCHES` row is mirrored at once. Used by app/services/teams/commands.py.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.watch import Watch
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.access import audit, records, users
from app.services.access.commands import CommandOutcome, Refused, receipt
from app.services.teams.contracts import QuestionJob
from app.services.teams.mirror import MirrorWriter, build_mirror_writer_from_settings
from app.services.watchlist import forms, registry, search, store
from app.services.watchlist import mirror as watch_mirror
from app.settings import settings

logger = logging.getLogger(__name__)

KINDS = ("watch_add", "watch_update", "watch_stop", "watch_seen", "watch_not_now")
FORM_ID = "add_watch"


async def run(job: QuestionJob, *, session_factory=None, mirror: MirrorWriter | None = None,
              now: datetime | None = None, db: AsyncSession | None = None) -> CommandOutcome:
    """Run one watchlist command for the job's sender; the line the page shows, with its blocks.
    With `db` the command runs and commits on that session (the Ask tools' own, step 14c), because
    a second session would wait on the rows the first one has not committed yet.

    (watch_add delivery D-1) -> "Watching D-1 · Riverside Kitchens · 7 of 10 places used" + receipt with Undo ;
    (11th) -> "Your watchlist is full (10 of 10)" + choice block ; (another site) -> Refused
    """
    command = job.command
    if command is None or command.kind not in KINDS:
        raise ValueError("the job carries no watchlist command")
    if db is not None:
        return await _run_on(db, job, mirror=mirror, now=now)
    factory = session_factory or async_session
    async with factory() as own:
        return await _run_on(own, job, mirror=mirror, now=now)


async def _run_on(db: AsyncSession, job: QuestionJob, *, mirror: MirrorWriter | None, now: datetime | None) -> CommandOutcome:
    command = job.command
    assert command is not None
    now = now or datetime.now(timezone.utc)
    actor = users.Actor(job.sender_object_id, job.sender_name)
    # Step 1: The actor's own record decides, so a forged job cannot watch for someone else or
    # watch a site the role may not see
    record = await records.access_record_for(db, job.customer_code, job.sender_object_id)
    if record is None or not record.is_active:
        raise Refused("you have no active access record")
    tz = ZoneInfo(await get_customer_timezone(db, job.customer_code))
    payload = dict(command.payload)
    handler = _HANDLERS[command.kind]
    try:
        text, blocks, subject_id, detail = await handler(db, job, record, actor, payload, now, tz)
    except (search.NotFound, search.NoAccess, ValueError) as exc:
        raise Refused(str(exc))
    await audit.record_event(
        db, job.customer_code, actor_object_id=actor.object_id, actor_name=actor.name, action=f"{command.kind}",
        decision="allowed", subject_kind="watch", subject_id=subject_id, site_id=job.site_id, request_id=job.job_id,
        text=f"{actor.name or actor.object_id}: {text}", detail=detail, at=now)
    await db.commit()
    # Step 2: Mirror the person's row now, so the tab and the eye toggles follow at once
    writer = mirror if mirror is not None else build_mirror_writer_from_settings()
    if writer is not None:
        try:
            await watch_mirror.mirror_user(db, writer, job.customer_code, actor.object_id, now=now)
        except Exception:
            logger.warning("watches of %s not mirrored after %s", actor.object_id, command.kind, exc_info=True)
    return CommandOutcome(text, tuple(blocks))


def _until(raw, now: datetime, tz: ZoneInfo) -> datetime | None:
    """A watch's end from the page's words or an ISO moment; None to keep going.

    ("end_of_day") -> local midnight ; ("7_days") -> now + 7 d ; (iso) -> that ; (None | "") -> None
    """
    if not raw:
        return None
    text = str(raw).strip()
    if text == "end_of_day":
        local = now.astimezone(tz)
        return (local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).astimezone(timezone.utc)
    if text == "7_days":
        return now + timedelta(days=7)
    try:
        at = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("until must be end_of_day, 7_days or an ISO moment")
    return at if at.tzinfo else at.replace(tzinfo=tz)


def _places(db_used: int) -> str:
    return f"{db_used} of {int(settings.teams_watches_limit)} places used"


def full_choice(watches: list[Watch], limit: int, wanted: dict, label: str) -> dict:
    """The swap state as a choice block: the person's watches quietest first, each carrying the
    add that swaps it, and Never mind."""
    options = [{
        "id": str(w.id), "label": w.label, "suggested": i == 0,
        "detail": (f"changed {w.last_changed_at.strftime('%H:%M')}" if w.last_changed_at else "never changed") + f" · {registry.status_text(w.last_status)}",
        "command": {"kind": "watch_add", "payload": {**wanted, "swap_watch_id": str(w.id), "via": "swap"}},
    } for i, w in enumerate(watches)]
    options.append({"id": "never_mind", "label": "Never mind", "detail": "", "suggested": False, "command": None})
    return {"type": "choice", "question": f"Your watchlist is full ({limit} of {limit}). To watch {label}, stop watching one of these; the quietest are first.",
            "options": options, "allow_typed": False}


async def _add(db: AsyncSession, job: QuestionJob, record: records.AccessRecord, actor: users.Actor, payload: dict,
               now: datetime, tz: ZoneInfo):
    # Step 0: A confirmed Ask form (step 14c) names the thing in words; find it again within the
    # person's sites and read the rule, so one path adds every watch
    if payload.get("form_id") == forms.FORM_ID:
        values = dict(payload.get("values") or {})
        form_kind = forms.kind_from_form(values)
        found = await search.find(db, job.customer_code, record, str(values.get("what") or ""), kind=form_kind, tz=tz, limit=1)
        if not found:
            raise ValueError(f"nothing called {values.get('what')!r} to watch")
        payload = {**payload, "kind": found[0].kind, "entity_id": found[0].entity_id,
                   "alert": forms.alert_from_form(found[0].kind, values), "until": forms.until_from_form(values),
                   "via": payload.get("via") or "ask"}
    kind = str(payload.get("kind") or "").strip().lower()
    entity_id = str(payload.get("entity_id") or "").strip()
    resolved = await search.resolve(db, job.customer_code, record, kind, entity_id, tz=tz)
    alert = registry.clean_alert(resolved.kind, payload.get("alert"))
    until = _until(payload.get("until"), now, tz)
    via = str(payload.get("via") or "tab")[:16]
    # Step 1: A swap ends the named watch first, in the same transaction, so a crash between the
    # two leaves nothing half done
    swapped: Watch | None = None
    if payload.get("swap_watch_id"):
        swapped = await store.get(db, job.customer_code, payload["swap_watch_id"])
        if swapped is None or swapped.owner_object_id != actor.object_id or swapped.status != "active":
            raise ValueError("the watch to swap out is not one of yours any more")
        store.end(swapped, "swapped", now=now)
        await db.flush()
    try:
        watch, is_new = await store.add(db, job.customer_code, owner_object_id=actor.object_id, owner_name=actor.name,
                                        kind=resolved.kind, entity_id=resolved.entity_id, entity_ref=resolved.entity_ref,
                                        label=resolved.label, site_id=resolved.site_id, alert=alert, until=until, via=via, now=now)
    except store.Full as full:
        wanted = {"kind": resolved.kind, "entity_id": resolved.entity_id, "alert": alert,
                  **({"until": payload.get("until")} if payload.get("until") else {})}
        text = f"Your watchlist is full ({full.limit} of {full.limit})"
        return text, [full_choice(full.watches, full.limit, wanted, resolved.label)], None, {"kind": resolved.kind, "entity_id": resolved.entity_id, "full": True}
    used = await store.count_active(db, job.customer_code, actor.object_id)
    if not is_new:
        text = f"Already watching · {watch.label}"
        return text, [receipt(text, reference=str(watch.id), now=now)], str(watch.id), {"kind": watch.kind, "existing": True}
    ends = watch_mirror.ends_text(watch, tz)
    text = (f"{'Swapped · ' if swapped is not None else ''}Watching · {watch.label} · {registry.alert_text(watch.kind, alert)}"
            f" · {_places(used)}" + (f" · {ends}" if ends else ""))
    undo = {"watch_id": str(watch.id), "action": "stop", "via": "undo", **({"resume_watch_id": str(swapped.id)} if swapped else {})}
    return text, [receipt(text, reference=str(watch.id), now=now, undo=undo)], str(watch.id), \
        {"kind": watch.kind, "entity_id": watch.entity_id, "alert": alert, "via": via, "swapped": str(swapped.id) if swapped else None}


async def _own(db: AsyncSession, job: QuestionJob, actor: users.Actor, payload: dict) -> Watch:
    watch = await store.get(db, job.customer_code, payload.get("watch_id"))
    if watch is None:
        raise ValueError("that watch no longer exists")
    if watch.owner_object_id != actor.object_id:
        raise ValueError("that watch is not yours")
    return watch


async def _update(db: AsyncSession, job: QuestionJob, record: records.AccessRecord, actor: users.Actor, payload: dict,
                  now: datetime, tz: ZoneInfo):
    watch = await _own(db, job, actor, payload)
    action = str(payload.get("action") or "set_alert")
    if action == "resume":
        # Step 1: Undo of a stop is only good while the window is open, otherwise "Undo" on an old
        # receipt would quietly re-add something the person stopped on purpose
        if watch.status != "ended" or watch.ended_at is None or now - watch.ended_at > timedelta(minutes=settings.teams_undo_minutes):
            raise ValueError("the undo window has passed; add it again from the eye button")
        await store.resume(db, watch, now=now)
        text = f"Watching again · {watch.label}"
        return text, [receipt(text, reference=str(watch.id), now=now)], str(watch.id), {"action": action}
    if action != "set_alert":
        raise ValueError("action must be set_alert or resume")
    if watch.status != "active":
        raise ValueError("that watch has ended")
    old_alert, old_until = dict(watch.alert or {}), watch.until
    alert = registry.clean_alert(watch.kind, payload.get("alert"))
    until = _until(payload.get("until"), now, tz) if "until" in payload else watch.until
    store.set_alert(watch, alert, until=until, now=now)
    text = f"Alert changed · {watch.label} · {registry.alert_text(watch.kind, alert)}"
    undo = {"watch_id": str(watch.id), "action": "set_alert", "alert": old_alert,
            "until": old_until.isoformat() if old_until else None, "via": "undo"}
    return text, [receipt(text, reference=str(watch.id), now=now, undo=undo)], str(watch.id), {"action": action, "alert": alert}


async def _stop(db: AsyncSession, job: QuestionJob, record: records.AccessRecord, actor: users.Actor, payload: dict,
                now: datetime, tz: ZoneInfo):
    watch = await _own(db, job, actor, payload)
    if watch.status != "active":
        text = f"Already stopped · {watch.label}"
        return text, [receipt(text, reference=str(watch.id), now=now)], str(watch.id), {"already": True}
    store.end(watch, "stopped", now=now)
    # Undo of a swap's own undo: the swapped-out watch comes back with the new one gone
    if payload.get("resume_watch_id"):
        old = await store.get(db, job.customer_code, payload["resume_watch_id"])
        if old is not None and old.owner_object_id == actor.object_id and old.status == "ended":
            await db.flush()
            await store.resume(db, old, now=now)
    text = f"Stopped watching · {watch.label}"
    undo = {"watch_id": str(watch.id), "action": "resume", "via": "undo"}
    return text, [receipt(text, reference=str(watch.id), now=now, undo=undo)], str(watch.id), {"via": payload.get("via")}


async def _seen(db: AsyncSession, job: QuestionJob, record: records.AccessRecord, actor: users.Actor, payload: dict,
                now: datetime, tz: ZoneInfo):
    n = await store.mark_seen(db, job.customer_code, actor.object_id, payload.get("event_ids"), now=now)
    return f"marked seen · {n} change{'s' if n != 1 else ''}", [], None, {"count": n}


async def _not_now(db: AsyncSession, job: QuestionJob, record: records.AccessRecord, actor: users.Actor, payload: dict,
                   now: datetime, tz: ZoneInfo):
    kind = str(payload.get("kind") or "").strip().lower()
    entity_id = str(payload.get("entity_id") or "").strip()
    if kind not in registry.LABELS or not entity_id:
        raise ValueError("a Not now names a kind and an entity_id")
    until = await store.not_now(db, job.customer_code, actor.object_id, kind, entity_id, now=now)
    return f"Not now · {entity_id} · no suggestion until {until.astimezone(tz).strftime('%a %H:%M')}", [], None, {"kind": kind, "entity_id": entity_id}


_HANDLERS = {
    "watch_add": _add,
    "watch_update": _update,
    "watch_stop": _stop,
    "watch_seen": _seen,
    "watch_not_now": _not_now,
}
