"""Rule engine — loads ACTIVE rules and publishes events for whatever currently matches.

Called once per worker cycle. Streaming rules are evaluated against the recent transaction tail;
digest rules emit one summary per completed window. Idempotency is by event `dedup_key` (the outbox
has a unique index), so re-running every cycle re-publishes nothing already seen.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.notification import NotificationRule
from app.persistence.repositories.notification_repository import NotificationRepository
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.notifications import cursor
from app.services.notifications.bus import bus
from app.services.notifications.rules.base import build_evaluator, is_streaming
from app.services.mnp_log_ingestion.timefmt import set_display_timezone

logger = logging.getLogger(__name__)

def _split_by_kind(rules: list[NotificationRule]) -> tuple[list, list]:
    """(streaming, windowed). Streaming rules evaluate per transaction against the cursor; window
    rules summarise a completed interval and keep their own dedup key, so they take a different path
    entirely and must not be mixed in."""
    streaming, windowed = [], []
    for r in rules:
        (streaming if is_streaming(r) else windowed).append(r)
    return streaming, windowed


async def run_rules_once() -> None:
    async with async_session() as db:
        repo = NotificationRepository(db)
        rules = await repo.list_active_rules()
        if not rules:
            return
        streaming, windowed = _split_by_kind(rules)
        if streaming:
            await _run_streaming(db, repo, streaming)
        if windowed:
            await _run_window(db, repo, windowed)


def _events_from(ev, rule: NotificationRule, txns: list[LogTransaction]) -> list:
    """One rule's events, skipping rows behind its own cursor.

    That skip is the in-memory half of sharing one query across a customer's rules: rows are fetched
    from the OLDEST cursor among them, so a rule further ahead receives rows it has already handled
    and must not re-evaluate them.
    """
    out = []
    for txn in txns:
        event = ev.evaluate(txn) if cursor.is_after(txn.created_at, rule.cursor_at) else None
        if event is not None:   # streaming evaluators are sync
            out.append(event)
    return out


def _candidates_for(rules: list[NotificationRule], txns: list[LogTransaction]) -> list:
    """Events every rule in this customer's set wants published."""
    out = []
    for rule in rules:
        ev = build_evaluator(rule)
        if ev is not None:
            out.extend(_events_from(ev, rule, txns))
    return out


async def _publish_new(repo: NotificationRepository, candidates: list) -> None:
    """Publish everything not already in the outbox. Dedupe stays the safety net, unchanged: the
    cursor stops us re-READING rows, this stops us re-SENDING them if we ever do."""
    if not candidates:
        return
    existing = await repo.existing_dedup_keys([c.dedup_key for c in candidates])
    for event in candidates:
        if event.dedup_key in existing:
            continue
        await bus.publish(event)
        existing.add(event.dedup_key)  # guard duplicates within this batch


async def _run_customer_streaming(db: AsyncSession, repo: NotificationRepository,
                                  customer_code: str, rules: list[NotificationRule],
                                  now: datetime) -> None:
    """One customer's rules, read with ONE query spanning the oldest cursor among them.

    One query rather than one per rule is a measured choice: planning against the partitioned
    `log_transactions` costs ~50 ms because the planner considers every partition, so per-rule queries
    would multiply that by the rule count. Execution is sub-millisecond either way.

    Cursors advance in the SAME transaction that published the events, so a crash can never leave a
    cursor past events that were never persisted.
    """
    window = cursor.read_window_for_group(
        [r.cursor_at for r in rules], now=now,
        lag=timedelta(seconds=settings.notification_cursor_lag_seconds),
        lookback=timedelta(seconds=settings.notification_lookback_seconds))
    if window is None:
        return  # nothing has aged past the lag yet

    set_display_timezone(await get_customer_timezone(db, customer_code))  # localize message times
    txns = list((await db.execute(
        cursor.window_stmt(customer_code, window, limit=settings.notification_candidate_limit)
    )).scalars().all())

    await _publish_new(repo, _candidates_for(rules, txns))
    for rule in rules:
        cursor.advance_rule(db, rule, window=window, rows=txns)
    await db.commit()


async def _run_streaming(db: AsyncSession, repo: NotificationRepository,
                         rules: list[NotificationRule]) -> None:
    now = datetime.now(timezone.utc)
    by_customer: dict[str, list[NotificationRule]] = defaultdict(list)
    for r in rules:
        by_customer[r.customer_code].append(r)
    for customer_code, cust_rules in by_customer.items():
        await _run_customer_streaming(db, repo, customer_code, cust_rules, now)


async def _run_window(db: AsyncSession, repo: NotificationRepository,
                      rules: list[NotificationRule]) -> None:
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    for rule in rules:
        set_display_timezone(await get_customer_timezone(db, rule.customer_code))  # localize message times
        ev = build_evaluator(rule)
        if ev is None:
            continue
        interval = ev.interval_seconds
        current_index = now_epoch // interval
        prev_index = current_index - 1  # summarize the most recent COMPLETED window
        window_start = datetime.fromtimestamp(prev_index * interval, tz=timezone.utc)
        window_end = datetime.fromtimestamp(current_index * interval, tz=timezone.utc)
        dedup_key = f"rule:{rule.id}:window:{prev_index}"

        if await repo.get_event_by_dedup_key(dedup_key) is not None:
            continue  # this window already summarized
        event = await ev.evaluate_window(db, window_start, window_end, dedup_key)
        if event is not None:
            await bus.publish(event)
