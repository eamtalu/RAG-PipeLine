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
from app.services import consumer_cursors
from app.services.notifications import rollup
from app.services.notifications.dispatcher import deliver_due

logger = logging.getLogger(__name__)


async def _report_position() -> None:
    """Publish notifications' consumed position into the shared registry.

    Failure here must never break the tick: this is bookkeeping for retention, and losing an update
    only means the previous (older, therefore SAFER) position stands until the next tick.
    """
    from app.config.database import async_session
    try:
        async with async_session() as db:
            await consumer_cursors.report_notifications(db)
            await db.commit()
    except Exception:
        logger.warning("could not publish the notification consumer position", exc_info=True)


async def run_notification_worker() -> None:
    logger.info("Notification worker started (poll=%.1fs)", settings.notification_poll_seconds)
    while True:
        try:
            await run_rules_once()
            # Summarise what the burst cap suppressed. Must run BEFORE the drain so a summary
            # published this tick goes out on the same tick, exactly like any other alert — and must
            # run at all, or suppressed alerts would be collapsed and then never represented, which
            # is strictly worse than not suppressing them.
            await rollup.run_once()
            attempted = await deliver_due()
            # Publish how far this subsystem has consumed, so retention will not drop a day the rules
            # have not read yet. A registry nobody writes to is worse than none: retention would read
            # a stale number and believe it was safe.
            await _report_position()
            if attempted:
                logger.info("Notification drain attempted %d delivery(ies)", attempted)
        except Exception:
            logger.exception("Notification worker error — retrying after sleep")
        await asyncio.sleep(settings.notification_poll_seconds)
