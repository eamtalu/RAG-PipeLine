"""The slash commands of the composer (stage D, step 18): `/remind me at 5 check route 14`,
`/schedule …`, `/graph …`, `/create-user …`. A command is read here, on the backend, and turned
into the question the model sees, naming the tool to call, so the extraction of what was said
stays with the model and the words stay in one place. Used by app/teams_consumer.py.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.services.access import records
from app.services.ask import actions, catalogue

#: Commands that are shortcuts for a question rather than an action.
SHORTCUTS = {"/graph": "Show {words} as a chart", "/chart": "Show {words} as a chart", "/table": "Show {words} as a table"}
_COMMAND = re.compile(r"^\s*(/[a-z][a-z-]*)(?:\s+me)?\b\s*(.*)$", re.I | re.S)


@dataclass(frozen=True)
class Slash:
    """What a slash command turned into: the text the model sees, or the words to answer with."""

    text: str
    #: A direct answer (the list of commands) when the command is unknown; None otherwise.
    answer: str | None = None


def parse(message: str) -> tuple[str, str] | None:
    """The command and the words after it, or None when the message is not a command.

    >>> parse("/remind me at 5 check route 14"), parse("/graph fill rate this week")
    (('/remind-me', 'at 5 check route 14'), ('/graph', 'fill rate this week'))
    >>> parse("remind me at 5"), parse("/schedule")
    (None, ('/schedule', ''))
    """
    m = _COMMAND.match(message or "")
    if not m:
        return None
    command = m.group(1).lower()
    if command == "/remind":
        command = "/remind-me"
    return command, m.group(2).strip()


def commands_for(record: records.AccessRecord | None) -> list[str]:
    """The slash commands a record may use, in the registry's order plus the shortcuts.

    (a picker) -> ['/remind-me', '/schedule', '/graph', …] ; (an admin) -> every command
    """
    out = [a.slash for a in actions.ACTIONS if catalogue.may_use(record, a.capability)]
    return out + list(SHORTCUTS)


def resolve(message: str, record: records.AccessRecord | None) -> Slash | None:
    """What a message that starts with a slash means; None when it is not a command.

    >>> resolve("/graph fill rate this week", None).text
    'Show fill rate this week as a chart'
    >>> anyone = type("R", (), {"is_active": True, "state": lambda self, code: "allowed"})()
    >>> resolve("/remind-me at 5 check route 14", anyone).text
    '(Command /remind-me: call propose_reminder with these words and nothing else first.) at 5 check route 14'
    >>> resolve("/remind-me at 5", None).answer[:22]
    'I do not know /remind-'
    """
    parsed = parse(message)
    if parsed is None:
        return None
    command, words = parsed
    if command in SHORTCUTS:
        if not words:
            return Slash(text=message, answer=f"Tell me what to draw after {command}, for example: {command} fill rate this week")
        return Slash(text=SHORTCUTS[command].format(words=words))
    spec = actions.spec_of_slash(command)
    if spec is None or not catalogue.may_use(record, spec.capability):
        allowed = ", ".join(commands_for(record))
        return Slash(text=message, answer=f"I do not know {command}. The commands you can use: {allowed}.")
    tool = actions.DEMO_TOOL if spec.mode == actions.DEMO else spec.tool_name
    hint = f" with action_id {spec.id}" if spec.mode == actions.DEMO else ""
    framed = f"(Command {command}: call {tool}{hint} with these words and nothing else first.)"
    return Slash(text=f"{framed} {words}".strip())
