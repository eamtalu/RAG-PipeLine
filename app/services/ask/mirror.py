"""The ASK_CATALOGUE row (stage D, step 18): the topics, their starters, the actions and the slash
commands, unfiltered, written once per customer with the access rows; the edge filters it per
record. Used by app/services/access/mirror.py.
"""

from __future__ import annotations

from typing import Any

from app.services.ask import actions, catalogue, slash


def catalogue_payload() -> dict[str, Any]:
    """The whole row: the version, every topic with its starters, every action, the shortcut commands.

    () -> {"version": "18.1", "topics": [7 topics], "actions": [9 actions], "shortcuts": ["/graph", …]}
    """
    return {"version": catalogue.CATALOGUE_VERSION, "topics": catalogue.topics_payload(),
            "actions": actions.actions_payload(), "shortcuts": list(slash.SHORTCUTS)}
