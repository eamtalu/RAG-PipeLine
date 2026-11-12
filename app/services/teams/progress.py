"""What the agent is doing right now, for the "Checking eSmart Eye…" line of the Teams app
(stage D, step 6): a `job_progress` row under `EYE#<customer>` rewritten as the run goes.

The runtime's `RunTrace` tells the reporter about every tool call, model call and guard verdict
(its `listener`); the reporter keeps four steps and writes them to the mirror after each event,
coalescing writes so a burst of events costs one put. The row expires within the hour on its own.
Used by app/teams_consumer.py; read by the edge's poll while the job is pending.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable

from app.services.teams.mirror import MirrorWriter, envelope

logger = logging.getLogger(__name__)

UNDERSTOOD = "Understood the question"
QUERYING = "Querying eSmart Eye"
WRITING = "Writing the answer"
CHECKING = "Checking the figures"
STEP_ORDER = (UNDERSTOOD, QUERYING, WRITING, CHECKING)

#: The presentation tools draw rather than read; the step says so.
DRAWING_TOOLS = {"show_chart": "drawing the chart", "show_table": "laying out the table",
                 "suggest_followups": "suggesting follow-ups"}


def friendly_tool(name: str) -> str:
    """A tool name as a person would read it in the progress line.

    >>> friendly_tool("aggregate_releases"), friendly_tool("show_chart")
    ('aggregate releases', 'drawing the chart')
    """
    return DRAWING_TOOLS.get(name, name.replace("_", " "))


class ProgressReporter:
    """The four steps of one run and their state, written to the mirror as events arrive."""

    def __init__(self, mirror: MirrorWriter, customer_code: str, job_id: str, *,
                 clock: Callable[[], datetime] | None = None):
        self._mirror, self._customer_code, self._job_id = mirror, customer_code, job_id
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._current = UNDERSTOOD
        self._detail: dict[str, str] = {}
        self._tool_calls = 0
        self._finished = False
        self._dirty = False
        self._flush: asyncio.Task | None = None
        self._tasks: set[asyncio.Task] = set()

    # --- events, called by the trace listener (synchronously, inside the run's loop)

    def on_event(self, kind: str, data: dict) -> None:
        """Move the steps on for one trace event and schedule a write.

        ("tool", {"name": "aggregate_releases"}) -> Querying running "1 call · aggregate releases" ;
        ("model_call", {}) after a tool -> Writing running ; ("guard", {"stage": "post"}) -> Checking
        """
        if self._finished:
            return
        if kind == "tool":
            self._tool_calls += 1
            self._current = QUERYING
            self._detail[QUERYING] = (f"{self._tool_calls} call{'s' if self._tool_calls != 1 else ''}"
                                      f" · {friendly_tool(str(data.get('name', '')))}")
        elif kind == "model_call" and self._tool_calls:
            self._current = WRITING
        elif kind == "guard" and data.get("stage") == "post":
            self._current = CHECKING
            if data.get("verdict") == "problem":
                self._detail[CHECKING] = "a figure did not match the data, asking again"
        else:
            return
        self._schedule()

    async def started(self) -> None:
        """Write the first state: the question is understood, the data is about to be read."""
        self._current = QUERYING
        await self._write()

    async def finished(self) -> None:
        """Mark every step done and write once more; later events are ignored."""
        self._finished = True
        self._current = None
        if self._flush is not None:
            self._flush.cancel()
        await self._write()

    # --- the row

    def steps(self) -> list[dict]:
        """The four steps as the page draws them: done before the current one, running at it.

        (current Writing) -> [Understood done, Querying done, Writing running, Checking todo]
        """
        out: list[dict] = []
        state = "done"
        for label in STEP_ORDER:
            if self._current is not None and label == self._current:
                out.append({"label": label, "state": "running", "detail": self._detail.get(label)})
                state = "todo"
                continue
            out.append({"label": label, "state": state, "detail": self._detail.get(label)})
        return out

    def _schedule(self) -> None:
        self._dirty = True
        if self._flush is None or self._flush.done():
            self._flush = asyncio.get_running_loop().create_task(self._flush_soon())
            self._tasks.add(self._flush)
            self._flush.add_done_callback(self._tasks.discard)

    async def _flush_soon(self) -> None:
        # one write per burst: events inside the same tick collapse into it
        await asyncio.sleep(0)
        while self._dirty:
            self._dirty = False
            await self._write()

    async def _write(self) -> None:
        now = self._clock()
        payload = {"job_id": self._job_id, "steps": self.steps(), "updated_at": now.isoformat(),
                   "finished": self._finished}
        try:
            await self._mirror.put(envelope("job_progress", self._customer_code, payload=payload,
                                            as_of=now.isoformat(), now=now.timestamp(),
                                            job_id=self._job_id))
        except Exception as exc:  # noqa: BLE001 - progress is a nicety; the answer must never wait on it
            logger.warning("teams.progress_write_failed job=%s error=%s", self._job_id, exc)
