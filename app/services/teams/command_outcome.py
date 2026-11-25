"""What every Teams command answers with (stage D, step 8): the one line, the receipt block with its
undo window, and `Refused` for an action the sender may not take. Shared by every command family
(access, advisories, groups, tasks, watchlist, Ask actions); routed by app/services/teams/commands.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.services.teams.contracts import ReceiptBlock
from app.settings import settings


class Refused(ValueError):
    """The actor may not do this; the sentence is what the page shows."""


@dataclass(frozen=True)
class CommandOutcome:
    """What a command answers with: one line, and the blocks the page draws (a receipt)."""

    text: str
    blocks: tuple[dict, ...] = ()


def receipt(text: str, *, status: str = "done", reference: str | None = None, undo: dict | None = None,
            now: datetime | None = None, minutes: int | None = None) -> dict:
    """A receipt block (step 8): what happened, its reference, and the undo while it is open.

    >>> r = receipt("Request sent", status="needs_approval", reference="USR-1", undo={"reference": "USR-1"},
    ...             now=datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc), minutes=10)
    >>> r["type"], r["undo_until"], r["undo"]
    ('receipt', '2026-11-13T09:10:00Z', {'reference': 'USR-1'})
    """
    now = now or datetime.now(timezone.utc)
    minutes = settings.teams_undo_minutes if minutes is None else minutes
    return ReceiptBlock(text=text, reference=reference, status=status,
                        undo_until=(now + timedelta(minutes=minutes)) if undo else None,
                        undo=undo).model_dump(mode="json")
