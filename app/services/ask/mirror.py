"""The ASK_CATALOGUE row (stage D, step 18): the topics, their starters, the actions and the slash
commands, unfiltered, written once per bound customer by the consumer's access loop (and again
only when its words change); the edge filters it per record. Used by app/teams_consumer.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.ask import actions, catalogue, slash
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed


def catalogue_payload() -> dict[str, Any]:
    """The whole row: the version, every topic with its starters, every action, the shortcut commands.

    () -> {"version": "18.1", "topics": [7 topics], "actions": [9 actions], "shortcuts": ["/graph", …]}
    """
    return {"version": catalogue.CATALOGUE_VERSION, "topics": catalogue.topics_payload(),
            "actions": actions.actions_payload(), "shortcuts": list(slash.SHORTCUTS) + list(slash.REPORT_COMMANDS)}


async def mirror_catalogue(mirror: MirrorWriter, customer_code: str, *, tracker: ChangeTracker | None = None,
                           now: datetime | None = None) -> bool:
    """Write the customer's ASK_CATALOGUE row; whether it was written (False when unchanged).

    (first call) -> True ; (same words again, with the tracker) -> False
    """
    now = now or datetime.now(timezone.utc)
    env = envelope("ask_catalogue", customer_code, payload=catalogue_payload(), as_of=now.isoformat(),
                   now=now.timestamp())
    return bool(await write_if_changed(mirror, tracker, env, now=now.timestamp()))
