"""The agents of the Advisories tab (stage D, step 9): what each watches, how often it runs, and
whether it is on. An agent is a function from the customer's data to findings; the runner turns
findings into advisories. The agents that need data we do not have stay off with their reason,
not faked (the prompt's rule).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class Finding:
    """One thing an agent found this run; the same dedup_key on the next run keeps it open."""

    dedup_key: str
    severity: str  # high | medium | low
    title: str
    evidence: str | None = None
    impact: str | None = None
    recommended: str | None = None
    confidence: float | None = None
    site_id: str | None = None
    due_at: datetime | None = None
    source: dict[str, Any] = field(default_factory=dict)
    actions: tuple[str, ...] = ("ask_why", "acknowledge", "assign", "snooze")


AgentFn = Callable[[AsyncSession, str, datetime, ZoneInfo], Awaitable[list[Finding]]]


@dataclass(frozen=True)
class AgentSpec:
    name: str
    label: str
    description: str  # what it watches, as the roster says it
    cadence_seconds: int
    enabled_by_default: bool
    off_reason: str | None  # why it stays off, when it does
    run: AgentFn | None
    routing: str  # how its findings reach people, as the roster says it


def _lazy(module: str, attr: str) -> AgentFn:
    async def call(db, customer_code, now, tz):
        import importlib

        return await getattr(importlib.import_module(module), attr)(db, customer_code, now, tz)

    return call


AGENTS: tuple[AgentSpec, ...] = (
    AgentSpec(
        name="delivery", label="Delivery",
        description="Learned ready time per route · warns before departure when lines will be left behind",
        cadence_seconds=300, enabled_by_default=True, off_reason=None,
        run=_lazy("app.services.advisories.agents.delivery", "run"),
        routing="High → channel immediately · Medium → in app",
    ),
    AgentSpec(
        name="stock", label="Stock controller",
        description="Stock counts that disagree with the system, and items short on several releases this week",
        cadence_seconds=900, enabled_by_default=True, off_reason=None,
        run=_lazy("app.services.advisories.agents.stock", "run"),
        routing="High → channel · Medium → in app",
    ),
    AgentSpec(
        name="location", label="Location",
        description="Locations that keep turning up empty on picks, and locations with repeated count variances",
        cadence_seconds=1800, enabled_by_default=True, off_reason=None,
        run=_lazy("app.services.advisories.agents.location", "run"),
        routing="Medium → in app · Low → digest later",
    ),
    AgentSpec(
        name="forecast", label="Forecast",
        description="Compares forecast lines to rostered capacity, 7 days ahead",
        cadence_seconds=86400, enabled_by_default=False,
        off_reason="needs the rostered capacity per shift; no roster source exists",
        run=None, routing="Medium → in app",
    ),
    AgentSpec(
        name="shortage", label="Shortage",
        description="Watches stock against tomorrow's releases and supplier ETAs",
        cadence_seconds=900, enabled_by_default=False,
        off_reason="needs supplier purchase-order due dates; not in the data",
        run=None, routing="High → channel immediately",
    ),
    AgentSpec(
        name="data_quality", label="Data quality",
        description="Sealing lag above the threshold stamps every figure as provisional",
        cadence_seconds=1800, enabled_by_default=False,
        off_reason="the freshness stamp does this on every tile and answer already; an advisory is a later choice",
        run=None, routing="Low → digest",
    ),
    AgentSpec(
        name="access_watch", label="Access watch",
        description="Dormant accounts, role drift, approvals older than 24 h",
        cadence_seconds=86400, enabled_by_default=False,
        off_reason="arrives with the daily digest of step 11",
        run=None, routing="Low → digest",
    ),
)

BY_NAME = {a.name: a for a in AGENTS}


def spec(name: str) -> AgentSpec:
    """The agent's spec, or ValueError for a name the registry does not know.

    >>> spec("delivery").label, spec("forecast").run is None
    ('Delivery', True)
    """
    if name not in BY_NAME:
        raise ValueError(f"unknown agent {name!r}")
    return BY_NAME[name]
