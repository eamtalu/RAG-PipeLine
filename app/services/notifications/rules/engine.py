"""Rule engine — loads ACTIVE rules and publishes events for whatever currently matches.

Called once per worker cycle. Streaming rules are evaluated against the recent transaction tail;
digest rules emit one summary per completed window. Idempotency is by event `dedup_key` (the outbox
has a unique index), so re-running every cycle re-publishes nothing already seen.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.notification import NotificationRule
from app.persistence.repositories.notification_repository import NotificationRepository
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.notifications.bus import bus
from app.services.notifications.rules.base import build_evaluator, is_streaming
from app.services.mnp_log_ingestion.timefmt import set_display_timezone

logger = logging.getLogger(__name__)

# Safety cap on transactions scanned per customer per cycle (newest first within the lookback window).
_CANDIDATE_LIMIT = 2000


async def run_rules_once() -> None:
    async with async_session() as db:
        repo = NotificationRepository(db)
        rules = await repo.list_active_rules()
        if not rules:
            return
        streaming = [r for r in rules if is_streaming(r)]
        window = [r for r in rules if not is_streaming(r)]
        if streaming:
            await _run_streaming(db, repo, streaming)
        if window:
            await _run_window(db, repo, window)


async def _run_streaming(db: AsyncSession, repo: NotificationRepository,
                         rules: list[NotificationRule]) -> None:
    now = datetime.now(timezone.utc)
    since = now - timedelta(seconds=settings.notification_lookback_seconds)

    by_customer: dict[str, list[NotificationRule]] = defaultdict(list)
    for r in rules:
        by_customer[r.customer_code].append(r)

    for customer_code, cust_rules in by_customer.items():
        set_display_timezone(await get_customer_timezone(db, customer_code))  # localize message times
        txns = (await db.execute(
            select(LogTransaction).where(
                LogTransaction.customer_code == customer_code,
                LogTransaction.started_at >= since,
            ).order_by(LogTransaction.started_at.desc()).limit(_CANDIDATE_LIMIT)
        )).scalars().all()
        if not txns:
            continue

        evaluators = [e for e in (build_evaluator(r) for r in cust_rules) if e is not None]
        candidates = []
        for txn in txns:
            for ev in evaluators:
                event = ev.evaluate(txn)  # streaming evaluators are sync
                if event is not None:
                    candidates.append(event)
        if not candidates:
            continue

        # Skip anything already published (one query), then publish the genuinely new events.
        existing = await repo.existing_dedup_keys([c.dedup_key for c in candidates])
        for event in candidates:
            if event.dedup_key in existing:
                continue
            await bus.publish(event)
            existing.add(event.dedup_key)  # guard duplicates within this batch


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
