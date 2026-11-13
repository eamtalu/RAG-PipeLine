"""Actions a Teams job can carry instead of a question (chunk 150).

Today there is one family: marking a delivery on the at-risk board as checked, or taking that back.
The consumer hands the whole job here; this module opens its own short session, calls the same
store function the web API uses, and returns the one-line confirmation the tab shows. A refused
command raises, and the consumer turns that into a short error sentence.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from app.config.database import async_session
from app.services.analytics_at_risk import check_store
from app.services.access import commands as access_commands
from app.services.teams.contracts import ACCESS_COMMAND_KINDS, QuestionJob


def _departure_date(text: str | None) -> date | None:
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise ValueError(f"departure_date {text!r} is not YYYY-MM-DD")


async def run_command(job: QuestionJob) -> str:
    command = job.command
    if command is None:
        raise ValueError("the job carries no command")
    # the Admin tab's actions (step 7c) live with the access service
    if command.kind in ACCESS_COMMAND_KINDS:
        return await access_commands.run(job)
    if not command.delivery_number:
        raise ValueError("an at-risk command names a delivery_number")
    actor = (command.by or job.sender_name or check_store.DEFAULT_ACTOR).strip()[:128]
    departure_date = _departure_date(command.departure_date)
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        if command.kind == "at_risk_check":
            row = await check_store.check(db, job.customer_code, command.delivery_number, actor=actor, note=command.note,
                                          now=now, departure_date=departure_date)
            await db.commit()
            return f"checked · delivery {row.delivery_number} · {actor}"
        if command.kind == "at_risk_uncheck":
            row = await check_store.uncheck(db, job.customer_code, command.delivery_number, actor=actor, now=now,
                                            departure_date=departure_date)
            await db.commit()
            return f"check removed · delivery {row.delivery_number} · {actor}"
    raise ValueError(f"unknown command {command.kind!r}")
