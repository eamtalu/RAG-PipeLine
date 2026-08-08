"""Collapse a flood of alerts from one rule into a single summary card.

Pacing (step 5) protects the *webhook*. It does nothing for the *person reading the channel*: 500
alerts delivered politely over twenty minutes is still 500 cards, and the channel becomes unusable
exactly when someone needs it.

So past a per-rule burst cap, further matches stop being delivered individually. They are still
recorded — every one keeps its `notification_events` row and a `notification_deliveries` row marked
`suppressed` — and one summary card per window represents them:

    ⚠ 473 more errors from "WMS errors" in the last 5 min
      Top: PurchaseOrderLine (310), StockTransaction (98)

Three properties this design is built around:

**Suppressed is recorded, not skipped.** The cursor has already moved past those transactions, so if
the rows were simply not created there would be no record anywhere of what the summary covered.
"Which transactions were in that rollup?" stays answerable.

**Only completed windows are summarised.** Firing on the current window would undercount, and its
dedup key would then block the correct summary from ever publishing. Same rule, and the same window
arithmetic, as the existing digest evaluators — an operator learns one notion of "window".

**The summary is an ordinary alert.** It goes through the same outbox, the same pacing and the same
retry path. Nothing about it is a special case except how its content is derived.
"""

import logging
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from app.config.database import async_session
from app.persistence.models.notification import (
    DeliveryStatus, NotificationDelivery, NotificationEvent as NotificationEventRow,
    NotificationRule,
)
from app.services.notifications.bus import bus
from app.services.notifications.events import NotificationEvent
from app.settings import settings

logger = logging.getLogger(__name__)

#: Where a rule may override the global cap, inside the `match` JSONB it already carries.
_CAP_KEY = "burst_cap"

#: Marks a summary card. The dispatcher exempts this type from the burst cap — a summary carries its
#: rule's id for provenance, so without the exemption the cap would suppress the very card that
#: reports the suppression and the whole flood would disappear in silence.
EVENT_TYPE = "rollup"

#: How many distinct offenders the summary names before it stops. Enough to be actionable, few enough
#: that the card stays glanceable — the point is to replace 473 cards, not to reproduce them.
_TOP_N = 3


def burst_cap(match: dict | None) -> int:
    """Individual cards this rule may send per window before the rest are summarised.

    Zero is honoured rather than treated as unset: "summarise everything, never send an individual
    card" is a legitimate choice for a rule watching something inherently noisy. A malformed value
    falls back to the default, because `match` is operator-edited JSONB and a typo must not stop that
    rule alerting altogether.
    """
    default = settings.notification_rule_burst_cap
    raw = (match or {}).get(_CAP_KEY)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


def window_index(at: datetime, *, window_seconds: int | None = None) -> int:
    """Which fixed time bucket `at` falls in.

    Deliberately the same arithmetic as `ErrorDigestEvaluator`, so rollups and digests bucket time
    identically and an operator only has to learn one notion of a window.
    """
    window = window_seconds or settings.notification_rollup_window_seconds
    return int(at.timestamp()) // window


def build_summary(*, customer_code: str, rule_id, rule_name: str, titles: list[str],
                  window_index: int, severity: str,
                  target_channel_ids) -> NotificationEvent:
    """The one card that stands in for everything suppressed in this window.

    Counts and the top offenders both matter: a bare number says something is wrong but not what, and
    the whole point is that nobody should have to read the cards it replaced to find out.

    `dedup_key` is stable per (rule, window), which is what makes the rollup idempotent — the worker
    runs every tick and must publish this exactly once.
    """
    total = len(titles)
    top = Counter(titles).most_common(_TOP_N)
    minutes = max(1, settings.notification_rollup_window_seconds // 60)
    return NotificationEvent(
        event_type=EVENT_TYPE,
        customer_code=customer_code,
        severity=severity,
        title=f"[{customer_code}] {total} more alerts from “{rule_name}” in the last {minutes} min",
        summary="; ".join(f"{t} ({n})" for t, n in top) or None,
        dedup_key=f"rollup:{rule_id}:window:{window_index}",
        payload={"facts": {"Suppressed": total, "Rule": rule_name,
                           **{f"Top {i + 1}": f"{t} ({n})" for i, (t, n) in enumerate(top)}}},
        target_channel_ids=target_channel_ids,
        rule_id=str(rule_id),
    )


def should_suppress(*, already_sent: int, cap: int) -> bool:
    """Whether this match has spilled past the rule's allowance for the window."""
    return already_sent >= cap


async def count_delivered_this_window(db, rule_id, since: datetime) -> int:
    """How many individual cards this rule has already queued in the current window.

    Counts everything NOT suppressed, so a card queued but not yet sent still consumes the allowance —
    otherwise a slow drain would let a burst through before pacing ever caught up.
    """
    return await db.scalar(
        select(func.count()).select_from(NotificationDelivery)
        .join(NotificationEventRow, NotificationEventRow.id == NotificationDelivery.event_id)
        .where(NotificationEventRow.rule_id == rule_id,
               NotificationEventRow.created_at >= since,
               NotificationDelivery.status != DeliveryStatus.suppressed.value)
    ) or 0


async def _windows_to_summarise(db, window_start: datetime, window_end: datetime, idx: int):
    """(rule, suppressed titles) for every rule with suppressions in the completed window."""
    rows = (await db.execute(
        select(NotificationEventRow.rule_id, NotificationEventRow.title)
        .join(NotificationDelivery, NotificationDelivery.event_id == NotificationEventRow.id)
        .where(NotificationDelivery.status == DeliveryStatus.suppressed.value,
               NotificationEventRow.created_at >= window_start,
               NotificationEventRow.created_at < window_end,
               NotificationEventRow.rule_id.isnot(None))
    )).all()
    by_rule: dict = {}
    for rule_id, title in rows:
        by_rule.setdefault(rule_id, []).append(title)
    return by_rule


async def _summarise_rule(db, rule_id, titles: list[str], idx: int) -> int:
    """Publish one summary for this rule's window. Returns 1 if it published, 0 if it did not.

    Returns 0 rather than raising when the rule was deleted mid-window: the suppressed delivery rows
    survive as the record of what happened, and losing the summary is not worth failing the whole
    rollup pass for every other rule.
    """
    rule = await db.get(NotificationRule, rule_id)
    if rule is None:
        return 0
    event = build_summary(
        customer_code=rule.customer_code, rule_id=rule_id, rule_name=rule.name,
        titles=titles, window_index=idx, severity=rule.severity,
        target_channel_ids=rule.target_channel_ids)
    already = await db.scalar(select(NotificationEventRow.id).where(
        NotificationEventRow.dedup_key == event.dedup_key))
    if already is not None:
        return 0   # this window is already summarised
    await bus.publish(event)
    return 1


async def run_once(*, now: datetime | None = None) -> int:
    """Publish one summary per rule that suppressed anything in the last COMPLETED window.

    Returns how many summaries were published. Idempotent — a second pass in the same window
    publishes nothing, because the dedup key already exists in the outbox.
    """
    now = now or datetime.now(timezone.utc)
    window = settings.notification_rollup_window_seconds
    idx = window_index(now, window_seconds=window) - 1  # the most recent COMPLETED window
    start = datetime.fromtimestamp(idx * window, tz=timezone.utc)
    end = start + timedelta(seconds=window)

    published = 0
    async with async_session() as db:
        for rule_id, titles in (await _windows_to_summarise(db, start, end, idx)).items():
            published += await _summarise_rule(db, rule_id, titles, idx)
    if published:
        logger.info("Notification rollup: published %d summary card(s)", published)
    return published
