# timefmt.py — per-customer display-timezone conversion for log timestamps.
#
#   Log timestamps are STORED as true UTC instants (timestamptz). Each customer/ingestor has its OWN
#   timezone (customers.timezone): the ingestor localizes the log's naive wall-clock with it to store
#   correct UTC (host-independent), and reads convert UTC back to it for human display.
#
#   The active zone for a given request/cycle is carried in a ContextVar set at each boundary that
#   knows the customer (API deps, regroup, the notification worker). When unset it falls back to
#   settings.display_timezone — so any context that forgets to set it still behaves like the old
#   single-zone default (safe, never raises). Using IANA zones makes DST automatic.

import contextvars
from datetime import datetime, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

from app.settings import settings

# Active display-zone NAME for the current request/cycle; None → fall back to the global default.
_active_tz_name: contextvars.ContextVar[str | None] = contextvars.ContextVar("active_tz_name", default=None)


@lru_cache(maxsize=128)
def _zone(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def set_display_timezone(tz_name: str | None) -> None:
    """Set the active display zone for the current context (request/regroup/worker-customer)."""
    _active_tz_name.set(tz_name)


def active_timezone_name() -> str:
    """The IANA name of the active display zone (the customer's, else the global default)."""
    return _active_tz_name.get() or settings.display_timezone


def active_zone() -> ZoneInfo:
    return _zone(active_timezone_name())


def to_display(dt: datetime | None) -> datetime | None:
    """A UTC (or naive-assumed-UTC) datetime → an aware datetime in the active display zone."""
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: a naive value coming from the DB is a UTC instant
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(active_zone())


def iso_display(dt: datetime | None) -> str | None:
    """ISO-8601 in the active display zone, e.g. '2026-06-10T14:44:25.983000+01:00' (None passes through)."""
    d = to_display(dt)
    return d.isoformat() if d else None


def from_display_to_utc(dt: datetime | None) -> datetime | None:
    """Inbound filter bound → UTC-aware for the WHERE clause.

    A naive value (e.g. '?time_from=2026-06-10T14:40:00') is interpreted in the active display zone, so
    a user filtering by the local times they SEE matches the UTC instants stored. An already-aware
    value is respected (just normalised to UTC)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=active_zone())
    return dt.astimezone(timezone.utc)
