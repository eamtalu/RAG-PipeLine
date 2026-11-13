"""The Ask history of the eSmart Eye Teams app (stage D, step 6): one person's questions of the
last 30 days, kept in `teams_conversation_turns` and mirrored to the edge under `EYE#<customer>`.

Two rows per person go to the mirror: `user_history`, the summaries the history rail lists
(rebuilt from Postgres after every answer, so a purge is reflected on the next question), and one
`history_turn` per answer with the payload as it was posted, so the page can reopen an old answer
with its blocks without a backend call. Retention is `settings.teams_history_retention_days`;
the purge below deletes older turns in batches and the mirror rows expire a day later on their own.
Used by app/teams_consumer.py.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.teams_conversation_turn import TeamsConversationTurn
from app.persistence.repositories.teams_repository import ExchangeDetails, TeamsConversationRepository
from app.services.teams.contracts import AnswerPayload, QuestionJob
from app.services.teams.mirror import MirrorWriter, envelope

logger = logging.getLogger(__name__)

PREVIEW_CHARS = 160
PURGE_BATCH = 500


def details_from(job: QuestionJob, payload: AnswerPayload) -> ExchangeDetails:
    """What the history keeps about an exchange, read off the job and the answer as posted.

    (job by u-7 at BRI, payload with a chart, provisional) -> ExchangeDetails(sender u-7, site BRI,
    blocks [chart], asked_at, answered_at, data_as_of, provisional True)
    """
    return ExchangeDetails(
        sender_object_id=job.sender_object_id,
        site_id=job.site_id,
        blocks=[b.model_dump(mode="json") for b in payload.blocks],
        asked_at=payload.asked_at,
        answered_at=payload.answered_at,
        data_as_of=payload.data_as_of,
        provisional=payload.provisional,
    )


def preview_of(text: str, limit: int = PREVIEW_CHARS) -> str:
    """The first line of an answer, cut to the limit, for the history rail.

    >>> preview_of("Two items short.\\n\\n| a |")
    'Two items short.'
    >>> preview_of("x" * 200)[-1]
    '…'
    """
    first = text.strip().split("\n", 1)[0].strip()
    return first if len(first) <= limit else first[: limit - 1].rstrip() + "…"


def kinds_of(blocks: list | None) -> list[str]:
    """The block types an answer carries, once each, in the order they appear.

    >>> kinds_of([{"type": "table"}, {"type": "chart"}, {"type": "table"}, {"type": "followups"}])
    ['table', 'chart']
    >>> kinds_of(None)
    []
    """
    seen: list[str] = []
    for block in blocks or []:
        kind = block.get("type") if isinstance(block, dict) else None
        if kind in ("table", "chart", "form", "receipt") and kind not in seen:
            seen.append(kind)
    return seen


def history_entry(question: TeamsConversationTurn, answer: TeamsConversationTurn) -> dict:
    """One exchange as the history rail lists it: the question, a preview, kinds and times.

    (q "How many?", a "Two." with a chart, asked 17:13, answered 17:14) -> {"job_id", "question",
    "answer_preview": "Two.", "kinds": ["chart"], "asked_at", "answered_at", "data_as_of",
    "provisional", "site_id"}
    """
    asked = answer.asked_at or question.created_at
    return {
        "job_id": answer.job_id,
        "question": question.content,
        "answer_preview": preview_of(answer.content),
        "kinds": kinds_of(answer.blocks),
        "asked_at": asked.isoformat() if asked else None,
        "answered_at": (answer.answered_at or answer.created_at).isoformat(),
        "data_as_of": answer.data_as_of.isoformat() if answer.data_as_of else None,
        "provisional": bool(answer.provisional),
        "site_id": answer.site_id,
    }


async def mirror_user_history(db: AsyncSession, mirror: MirrorWriter, customer_code: str,
                              sender_object_id: str, *, now: datetime, retention_days: int,
                              limit: int) -> int:
    """Rewrite one person's `user_history` row from Postgres; the number of entries written.

    (u-7 with 3 exchanges this month) -> 3, the row lists them newest first ; (none) -> 0, an
    empty row so the page shows "nothing yet" rather than "offline"
    """
    # Step 1: Read the person's exchanges within the retention, so that a row already purged (or
    # about to be) never comes back through the mirror
    since = now - timedelta(days=retention_days)
    pairs = await TeamsConversationRepository(db).user_exchanges(
        customer_code, sender_object_id, since=since, limit=limit)
    entries = [history_entry(q, a) for q, a in pairs]
    payload = {"customer_code": customer_code, "sender_object_id": sender_object_id,
               "retention_days": retention_days, "entries": entries}
    # Step 2: Write the whole list every time, because the row is small (summaries only) and a
    # rebuild is simpler to trust than an edit of a list held in another store
    await mirror.put(envelope("user_history", customer_code, payload=payload,
                              as_of=now.isoformat(), now=now.timestamp(),
                              user_id=sender_object_id))
    return len(entries)


async def mirror_turn(mirror: MirrorWriter, job: QuestionJob, payload: AnswerPayload, *,
                      now: datetime) -> None:
    """Write one answered question in full as its `history_turn` row, so it can be reopened.

    (job j1 by u-7, payload) -> row USER#u-7#TURN#j1 holding the question and the payload
    """
    body = {"job_id": job.job_id, "question": job.question, "site_id": job.site_id,
            "answer": payload.model_dump(mode="json")}
    await mirror.put(envelope("history_turn", job.customer_code, payload=body,
                              as_of=now.isoformat(), now=now.timestamp(),
                              user_id=job.sender_object_id, job_id=job.job_id))


async def purge_once(db: AsyncSession, *, now: datetime, retention_days: int,
                     batch_size: int = PURGE_BATCH) -> int:
    """Delete turns older than the retention, one batch per call; how many rows went.

    (retention 30, 1,200 older rows, batch 500) -> 500 (call again until 0) ; (none) -> 0
    """
    cutoff = now - timedelta(days=retention_days)
    gone = await TeamsConversationRepository(db).purge_older_than(cutoff, batch_size=batch_size)
    if gone:
        logger.info("teams.history_purged rows=%d cutoff=%s", gone, cutoff.isoformat())
    return gone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
