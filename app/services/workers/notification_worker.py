"""Notification worker — drives the alerting subsystem on a poll loop.

Each tick: (1) run the rule engine, which publishes new events to the in-process bus (the dispatcher,
subscribed, persists them to the outbox and attempts immediate delivery); (2) run the redelivery
loop, which re-attempts any pending/failed deliveries whose backoff has elapsed — this is what makes
delivery resilient to a channel/internet outage (they go out once connectivity returns).

OFF by default — enable with settings.notifications_enabled.
"""

import asyncio
import logging

from app.settings import settings
from app.services.notifications.rules.engine import run_rules_once
from app.services.notifications.dispatcher import deliver_due

logger = logging.getLogger(__name__)


async def run_notification_worker() -> None:
    logger.info("Notification worker started (poll=%.1fs)", settings.notification_poll_seconds)
    while True:
        try:
            await run_rules_once()
            attempted = await deliver_due()
            if attempted:
                logger.info("Notification drain attempted %d delivery(ies)", attempted)
        except Exception:
            logger.exception("Notification worker error — retrying after sleep")
        await asyncio.sleep(settings.notification_poll_seconds)
