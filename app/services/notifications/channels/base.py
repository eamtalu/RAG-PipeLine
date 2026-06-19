"""Channel — the one interface every delivery transport implements.

A channel takes a NotificationEvent + its per-customer config (e.g. {"webhook_url": ...}) and
delivers it. It MUST raise on any failure (bad config, network error, non-2xx) so the dispatcher
records the delivery as failed and schedules a retry. Adding Slack/WhatsApp = implementing this and
registering it; nothing else in the system changes.
"""

from abc import ABC, abstractmethod

from app.services.notifications.events import NotificationEvent


class Channel(ABC):
    # stable transport key, e.g. "teams" — matches CustomerNotificationChannel.channel_type.
    channel_type: str = ""

    @abstractmethod
    async def send(self, event: NotificationEvent, config: dict) -> None:
        """Deliver `event` using `config`. Raise on any failure."""
        raise NotImplementedError
