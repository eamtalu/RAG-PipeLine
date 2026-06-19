"""EventBus — a deliberately tiny in-process publish/subscribe seam.

A rule (producer) calls `await bus.publish(event)`; subscribers (the dispatcher) receive it. This is
the only coupling point between "something happened" and "where it goes" — so the producers and the
channels never import each other. It is intentionally NOT a durable broker: durability/retry live
one layer down in the dispatcher's Postgres outbox. Swapping in a real broker later means
reimplementing just this module behind the same `publish`/`subscribe` shape.
"""

import logging
from collections.abc import Awaitable, Callable

from app.services.notifications.events import NotificationEvent

logger = logging.getLogger(__name__)

Handler = Callable[[NotificationEvent], Awaitable[None]]


class EventBus:
    def __init__(self) -> None:
        self._subscribers: list[Handler] = []

    def subscribe(self, handler: Handler) -> None:
        if handler not in self._subscribers:
            self._subscribers.append(handler)

    def clear(self) -> None:
        """Drop all subscribers (used on startup to avoid double-registration on reload)."""
        self._subscribers.clear()

    async def publish(self, event: NotificationEvent) -> None:
        # A failing subscriber must not stop the others (or the producer); the dispatcher persists
        # before delivering, so a transient subscriber error doesn't lose the event.
        for handler in list(self._subscribers):
            try:
                await handler(event)
            except Exception:
                logger.exception("notification subscriber failed for event %s", event.dedup_key)


# Module-level singleton — import `bus` everywhere.
bus = EventBus()
