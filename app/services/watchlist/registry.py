"""The pure rules of the watchlist (stage D, step 14): the six kinds, their cadences, their status
sets and what each status means, the default alert per kind, how a row or an aggregate becomes a
status, which transitions are alerts, which are silent, and the words the cards carry. No I/O;
readers.py reads, runner.py decides with these.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from app.persistence.models.watch import KINDS

LABELS = {"delivery": "Delivery", "route": "Route", "location": "Location", "zone": "Zone",
          "product": "Product", "measure": "Measure"}
CADENCE_SECONDS = {"delivery": 60, "route": 60, "measure": 60, "product": 300, "location": 300, "zone": 300}

#: The at-risk tiers in watch words; the board's "watch" tier reads "needs a look" here, because
#: "watch" already means something else on this page
TIER_STATUS = {"none": "on_track", "watch": "needs_look", "at_risk": "at_risk", "left_behind": "left_behind"}
TIER_RANK = {"on_track": 0, "needs_look": 1, "at_risk": 2, "left_behind": 3}
CLOSED_STATUS = {"loaded_in_time": "gone_in_time", "picked_in_time": "gone_in_time",
                 "loaded_late": "gone_late", "picked_late": "gone_late",
                 "never_loaded": "never_loaded", "unknown": "unknown"}
CLOSED_STATUSES = frozenset(CLOSED_STATUS.values())

STATUSES = {
    "delivery": ("on_track", "needs_look", "at_risk", "left_behind", "gone_in_time", "gone_late", "never_loaded", "unknown"),
    "route": ("idle", "on_track", "needs_look", "at_risk", "left_behind", "done"),
    "measure": ("above", "below", "unknown"),
    "product": ("no_activity", "picking", "zero_picked", "counted_empty"),
    "location": ("quiet", "picking", "empty_picks", "count_variance"),
    "zone": ("quiet", "picking", "empty_picks", "count_variance"),
}
#: Entering the day's resting status is never an event, otherwise every standing watch would wake
#: its owner at midnight with nothing to say
BASELINE = {"route": "idle", "product": "no_activity", "location": "quiet", "zone": "quiet"}

LEVEL = {
    "on_track": "fine", "needs_look": "medium", "at_risk": "high", "left_behind": "high",
    "gone_in_time": "fine", "gone_late": "high", "never_loaded": "high", "unknown": "medium",
    "idle": "fine", "done": "fine", "above": "fine", "below": "high",
    "no_activity": "fine", "picking": "fine", "zero_picked": "high", "counted_empty": "high",
    "quiet": "fine", "empty_picks": "high", "count_variance": "high",
}
STATUS_TEXT = {
    "on_track": "On track", "needs_look": "Needs a look", "at_risk": "At risk", "left_behind": "Left behind",
    "gone_in_time": "Gone, in time", "gone_late": "Gone late", "never_loaded": "Never loaded",
    "unknown": "Closed, outcome unknown", "idle": "No deliveries yet", "done": "Done for today",
    "above": "Above your line", "below": "Below your line",
    "no_activity": "No picks yet today", "picking": "Picking fine", "zero_picked": "A pick found it empty",
    "counted_empty": "Counted empty", "quiet": "No picks yet today", "empty_picks": "Picks found it empty",
    "count_variance": "A count disagrees",
}

RULES = {
    "delivery": ("tier_from", "show_only"),
    "route": ("tier_from", "show_only"),
    "measure": ("below", "above", "show_only"),
    "product": ("status_change", "show_only"),
    "location": ("status_change", "show_only"),
    "zone": ("status_change", "show_only"),
}
DEFAULT_LEVEL = 0.97
DEFAULT_BAND = 0.005
ZERO_PICKS_FROM = 3


def default_alert(kind: str) -> dict[str, Any]:
    """The alert a plain "watch X" gets for its kind.

    >>> default_alert("delivery"), default_alert("measure")
    ({'rule': 'tier_from', 'tier': 'at_risk'}, {'rule': 'below', 'level': 0.97, 'band': 0.005})
    """
    if kind in ("delivery", "route"):
        return {"rule": "tier_from", "tier": "at_risk"}
    if kind == "measure":
        return {"rule": "below", "level": DEFAULT_LEVEL, "band": DEFAULT_BAND}
    if kind in ("location", "zone"):
        return {"rule": "status_change", "count": ZERO_PICKS_FROM}
    return {"rule": "status_change"}


def clean_alert(kind: str, raw: Any) -> dict[str, Any]:
    """The alert rule a page or a form sent, checked for the kind; ValueError when it makes no sense.

    >>> clean_alert("measure", {"rule": "above", "level": "90"})
    {'rule': 'above', 'level': 0.9, 'band': 0.005}
    >>> clean_alert("delivery", {"rule": "tier_from", "tier": "watch"})
    {'rule': 'tier_from', 'tier': 'needs_look'}
    >>> clean_alert("route", None)
    {'rule': 'tier_from', 'tier': 'at_risk'}
    """
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {', '.join(KINDS)}")
    if not raw:
        return default_alert(kind)
    rule = str((raw or {}).get("rule") or "").strip().lower()
    if rule not in RULES[kind]:
        raise ValueError(f"a {kind} watch's rule must be one of {', '.join(RULES[kind])}")
    if rule == "show_only":
        return {"rule": "show_only"}
    if rule == "tier_from":
        tier = str(raw.get("tier") or "at_risk").strip().lower()
        tier = TIER_STATUS.get(tier, tier)
        if tier not in ("needs_look", "at_risk", "left_behind"):
            raise ValueError("tier must be needs_look, at_risk or left_behind")
        return {"rule": "tier_from", "tier": tier}
    if rule in ("below", "above"):
        try:
            level = float(raw.get("level"))
        except (TypeError, ValueError):
            raise ValueError("the level must be a number")
        level = level / 100 if level > 1 else level
        if not 0 < level <= 1:
            raise ValueError("the level must be between 1 and 100 percent")
        return {"rule": rule, "level": round(level, 4), "band": DEFAULT_BAND}
    count = raw.get("count")
    if count is None:
        return {"rule": "status_change", **({"count": ZERO_PICKS_FROM} if kind in ("location", "zone") else {})}
    try:
        count = max(1, int(count))
    except (TypeError, ValueError):
        raise ValueError("count must be a whole number")
    return {"rule": "status_change", "count": count}


def alert_text(kind: str, alert: dict[str, Any]) -> str:
    """The alert rule in the card's words.

    >>> alert_text("delivery", {"rule": "tier_from", "tier": "at_risk"})
    'Tells you if it is likely to leave short or late'
    >>> alert_text("measure", {"rule": "below", "level": 0.97})
    'Tells you if it drops below 97%'
    """
    rule = alert.get("rule")
    if rule == "show_only":
        return "Shows it; never alerts"
    if kind == "delivery":
        return ("Tells you if it needs a look, or is likely to leave short or late" if alert.get("tier") == "needs_look"
                else "Tells you if it is likely to leave short or late")
    if kind == "route":
        return ("Tells you when a delivery on it needs a look" if alert.get("tier") == "needs_look"
                else "Tells you when a delivery on it falls behind")
    if kind == "measure":
        pct = f"{float(alert.get('level') or DEFAULT_LEVEL) * 100:g}%"
        return f"Tells you if it {'drops below' if rule == 'below' else 'goes above'} {pct}"
    if kind == "product":
        return "Tells you when a pick finds it empty or a count says it is"
    return f"Tells you when picks find it empty ({int(alert.get('count') or ZERO_PICKS_FROM)} or more today) or a count disagrees"


def level_of(status: str | None) -> str:
    """How much a status needs a look: high, medium or fine.

    >>> level_of("at_risk"), level_of("on_track"), level_of(None)
    ('high', 'fine', 'fine')
    """
    return LEVEL.get(status or "", "fine")


def status_text(status: str | None) -> str:
    return STATUS_TEXT.get(status or "", "Not checked yet")


# ============================================================== statuses from data

def delivery_status(row_status: str, tier: str | None, outcome: str | None) -> str:
    """A delivery row's watch status.

    >>> delivery_status("open", "watch", None), delivery_status("closed", "none", "loaded_late")
    ('needs_look', 'gone_late')
    """
    if row_status == "closed":
        return CLOSED_STATUS.get(outcome or "unknown", "unknown")
    return TIER_STATUS.get(tier or "none", "on_track")


def route_status(open_by_tier: dict[str, int], closed_today: int) -> str:
    """A route's watch status from its open deliveries' tiers and what already went today.

    >>> route_status({"none": 3, "at_risk": 1}, 0), route_status({}, 4), route_status({}, 0)
    ('at_risk', 'done', 'idle')
    """
    if not open_by_tier or sum(open_by_tier.values()) == 0:
        return "done" if closed_today else "idle"
    worst = max((TIER_STATUS.get(t, "on_track") for t, n in open_by_tier.items() if n), key=lambda s: TIER_RANK[s])
    return worst


def measure_status(alert: dict[str, Any], value: float | None, last_status: str | None) -> str:
    """Above or below the person's line, with a band on the way back so a figure on the line
    does not flip every minute; unknown without a figure.

    >>> a = {"rule": "below", "level": 0.97, "band": 0.005}
    >>> measure_status(a, 0.96, None), measure_status(a, 0.972, "below"), measure_status(a, 0.976, "below")
    ('below', 'below', 'above')
    >>> measure_status({"rule": "show_only"}, 0.5, None), measure_status(a, None, "above")
    ('below', 'unknown')
    """
    if value is None:
        return "unknown"
    level = float(alert.get("level") or DEFAULT_LEVEL)
    band = float(alert.get("band") or DEFAULT_BAND)
    if alert.get("rule") == "show_only":
        return "above" if value >= level else "below"
    if value < level:
        return "below"
    if last_status == "below" and value < level + band:
        return "below"
    return "above"


def product_status(picks: int, zero_picks: int, is_counted_empty: bool) -> str:
    """A product's watch status from today's picks and the latest count.

    >>> product_status(0, 0, False), product_status(5, 0, False), product_status(5, 1, False), product_status(0, 0, True)
    ('no_activity', 'picking', 'zero_picked', 'counted_empty')
    """
    if is_counted_empty:
        return "counted_empty"
    if zero_picks:
        return "zero_picked"
    return "picking" if picks else "no_activity"


def location_status(picks: int, zero_picks: int, has_variance: bool, zero_picks_from: int) -> str:
    """A location's (or a zone's) watch status from today's picks and the latest counts.

    >>> location_status(0, 0, False, 3), location_status(4, 2, False, 3), location_status(4, 3, False, 3)
    ('quiet', 'picking', 'empty_picks')
    >>> location_status(0, 0, True, 3)
    'count_variance'
    """
    if has_variance:
        return "count_variance"
    if zero_picks >= max(1, zero_picks_from):
        return "empty_picks"
    return "picking" if picks or zero_picks else "quiet"


# ============================================================== transitions

def is_downgrade(kind: str, old: str | None, new: str) -> bool:
    """Whether a change is a step down that should settle before it counts (a tier easing, a
    figure going missing), so a bounce is not an event.

    >>> is_downgrade("delivery", "at_risk", "needs_look"), is_downgrade("delivery", "on_track", "at_risk")
    (True, False)
    >>> is_downgrade("measure", "above", "unknown"), is_downgrade("product", "zero_picked", "picking")
    (True, False)
    """
    if old is None:
        return False
    if kind in ("delivery", "route") and old in TIER_RANK and new in TIER_RANK:
        return TIER_RANK[new] < TIER_RANK[old]
    if kind == "measure":
        return new == "unknown"
    return False


def is_silent(kind: str, old: str | None, new: str) -> bool:
    """Whether a change makes no event at all: the first status after adding, or the day's
    resting status coming round.

    >>> is_silent("route", None, "on_track"), is_silent("route", "done", "idle"), is_silent("route", "idle", "on_track")
    (True, True, False)
    """
    return old is None or new == BASELINE.get(kind)


def is_alert(kind: str, alert: dict[str, Any], old: str | None, new: str, value: dict[str, Any] | None = None) -> bool:
    """Whether a change is one the alert rule names (a toast and a red count), as against a
    quieter change that only lands in "What changed today".

    >>> is_alert("delivery", {"rule": "tier_from", "tier": "at_risk"}, "on_track", "at_risk")
    True
    >>> is_alert("delivery", {"rule": "tier_from", "tier": "at_risk"}, "on_track", "needs_look")
    False
    >>> is_alert("delivery", {"rule": "show_only"}, "on_track", "left_behind")
    False
    >>> is_alert("measure", {"rule": "below", "level": 0.97}, "above", "below"), is_alert("measure", {"rule": "above", "level": 0.9}, "below", "above")
    (True, True)
    >>> is_alert("product", {"rule": "status_change"}, "picking", "zero_picked"), is_alert("product", {"rule": "status_change"}, "zero_picked", "picking")
    (True, False)
    """
    rule = alert.get("rule")
    if rule == "show_only":
        return False
    if kind in ("delivery", "route"):
        if new in ("gone_late", "never_loaded"):
            return True
        if new == "done":
            return bool((value or {}).get("late_today"))
        if new in TIER_RANK and TIER_RANK[new] >= TIER_RANK.get(alert.get("tier", "at_risk"), 2):
            return old is None or old not in TIER_RANK or TIER_RANK[new] > TIER_RANK[old]
        return False
    if kind == "measure":
        return new == ("below" if rule == "below" else "above")
    return level_of(new) == "high"


def day_start(now: datetime, tz: ZoneInfo) -> datetime:
    """Local midnight before the moment, in that zone.

    >>> from datetime import timezone
    >>> day_start(datetime(2026, 11, 13, 15, 10, tzinfo=timezone.utc), ZoneInfo("Europe/London")).isoformat()
    '2026-11-13T00:00:00+00:00'
    """
    return now.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)


def lines_total(expected: int | None, confirmed: int | None) -> int | None:
    """The lines a delivery is measured against: the expected count when the release states it,
    else the confirmed lines (every live row today), else nothing.

    >>> lines_total(36, 31), lines_total(None, 8), lines_total(None, 0), lines_total(None, None)
    (36, 8, None, None)
    """
    if expected:
        return int(expected)
    if confirmed:
        return int(confirmed)
    return None


def lines_words(picked: int, expected: int | None, confirmed: int | None, *, short: int = 0) -> str:
    """The picking figure in the card's words, never "of 0".

    >>> lines_words(31, 36, 31), lines_words(7, None, 8, short=1), lines_words(1, None, 0)
    ('31 of 36 lines picked', '7 of 8 lines picked · 1 short', '1 line picked')
    """
    total = lines_total(expected, confirmed)
    words = (f"{picked} of {total} lines picked" if total else f"{picked} line{'s' if picked != 1 else ''} picked")
    return f"{words} · {short} short" if short else words


def route_words(open_count: int, by_tier: dict[str, int] | None = None) -> str:
    """A route's open deliveries in a person's words: how many are still to go, and how many of
    them the board has flagged, named by their tier.

    >>> route_words(3, {"none": 3}), route_words(1, {"at_risk": 1}), route_words(0, {})
    ('3 deliveries to go', '1 delivery to go · 1 at risk', 'nothing to go')
    >>> route_words(4, {"none": 1, "watch": 2, "left_behind": 1})
    '4 deliveries to go · 1 left behind · 2 need a look'
    """
    if open_count <= 0:
        return "nothing to go"
    words = f"{open_count} deliver{'ies' if open_count != 1 else 'y'} to go"
    tiers = by_tier or {}
    flagged = []
    if tiers.get("left_behind"):
        flagged.append(f"{tiers['left_behind']} left behind")
    if tiers.get("at_risk"):
        flagged.append(f"{tiers['at_risk']} at risk")
    if tiers.get("watch"):
        n = tiers["watch"]
        flagged.append(f"{n} need{'s' if n == 1 else ''} a look")
    return " · ".join([words, *flagged])


def when_words(moment, now: datetime, tz: ZoneInfo) -> str:
    """A departure in the card's words: the clock alone today, "tomorrow HH:MM", else the day too.

    >>> from datetime import timezone
    >>> now = datetime(2026, 11, 13, 15, 10, tzinfo=timezone.utc); tz = ZoneInfo("Europe/London")
    >>> when_words("2026-11-13T18:10:00+00:00", now, tz), when_words("2026-11-14T11:30:00+00:00", now, tz)
    ('18:10', 'tomorrow 11:30')
    >>> when_words("2026-11-16T11:30:00+00:00", now, tz), when_words(None, now, tz)
    ('Mon 16 Nov 11:30', '–')
    """
    if not moment:
        return "–"
    at = datetime.fromisoformat(moment) if isinstance(moment, str) else moment
    local, today = at.astimezone(tz), now.astimezone(tz).date()
    clock = local.strftime("%H:%M")
    days = (local.date() - today).days
    if days == 0:
        return clock
    if days == 1:
        return f"tomorrow {clock}"
    if days == -1:
        return f"yesterday {clock}"
    return f"{local.strftime('%a')} {local.day} {local.strftime('%b')} {clock}"


def pct(value: float | None) -> str:
    """A fraction as the card writes it.

    >>> pct(0.9764), pct(None)
    ('97.6%', '–')
    """
    return "–" if value is None else f"{Decimal(str(value)) * 100:.1f}%"
