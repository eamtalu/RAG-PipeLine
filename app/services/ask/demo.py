"""The demo command (stage D, step 18): a confirmed demo form arrives as `demo_act`, is written to
the audit as a demo so the trail shows what the customer tried, changes nothing else, and answers
with a receipt that says so in its first words. Used by app/services/teams/commands.py.
"""

from __future__ import annotations

from datetime import datetime, timezone

from app.config.database import async_session
from app.services.access import audit, records, users
from app.services.access.commands import CommandOutcome, Refused, receipt
from app.services.ask import actions
from app.services.teams.contracts import QuestionJob

DEMO_STATUS = "demo"


async def run(job: QuestionJob, *, session_factory=None, now: datetime | None = None) -> CommandOutcome:
    """Record one demo action for the job's sender; the demo line, with a receipt that cannot be undone.

    (supervisor: demo_act add_device {device_name: HH-0421, holder: Fiona Nachon, site_id: BRI})
    -> "Demo · nothing was changed · would add handheld HH-0421 for Fiona Nachon at BRI" ;
    (picker) -> Refused ; (unknown action) -> ValueError
    """
    command = job.command
    if command is None or command.kind != "demo_act":
        raise ValueError("the job carries no demo action")
    now = now or datetime.now(timezone.utc)
    payload = dict(command.payload)
    action_id = str(payload.get("action_id") or "")
    spec = actions.spec_of(action_id)
    if spec is None or spec.mode != actions.DEMO:
        raise ValueError(f"unknown demo action {action_id!r}")
    values = dict(payload.get("values") or {})
    actor = users.Actor(job.sender_object_id, job.sender_name)
    factory = session_factory or async_session
    async with factory() as db:
        # Step 1: The actor's own record decides, as for a real action, so a demo never shows a
        # picker a path a real action would refuse
        record = await records.access_record_for(db, job.customer_code, job.sender_object_id)
        if record is None or not record.is_active:
            raise Refused("you have no active access record")
        decision = records.decide(record, spec.capability)
        if not decision.is_allowed:
            raise Refused(decision.reason or f"{spec.label} is not allowed for you")
        # Step 2: One audit line marked demo, so the trail says what was tried and that nothing
        # changed; no other row is touched and nothing is mirrored
        text = actions.demo_text(action_id, values)
        await audit.record_event(
            db, job.customer_code, actor_object_id=actor.object_id, actor_name=actor.name,
            action=f"demo.{action_id}", capability_code=spec.capability, decision=DEMO_STATUS,
            subject_kind="demo", subject_id=action_id, site_id=job.site_id, text=text,
            detail={"form_id": spec.form_id, "values": values}, at=now)
        await db.commit()
    return CommandOutcome(text, (receipt(text, status=DEMO_STATUS, reference=f"DEMO-{action_id}", now=now),))
