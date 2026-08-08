"""Channel — the one interface every delivery transport implements.

A channel takes a NotificationEvent + its per-customer config (e.g. {"webhook_url": ...}) and
delivers it. It MUST raise on any failure (bad config, network error, non-2xx) so the dispatcher
records the delivery as failed and schedules a retry. Adding Slack/WhatsApp = implementing this and
registering it; nothing else in the system changes.
"""

from abc import ABC, abstractmethod

from app.services.notifications.events import NotificationEvent


class ChannelRateLimited(Exception):
    """The transport asked us to slow down (HTTP 429), rather than failing to deliver.

    Raised INSTEAD of a generic error so the dispatcher can tell the two apart. Being throttled is not
    a defect in the delivery: it must not consume the retry budget, must not move the row toward
    dead-lettering, and must not use the generic backoff ladder when the platform has told us exactly
    how long to wait.

    `retry_after` is the server's own number in seconds, or None when it did not send one.
    """

    def __init__(self, retry_after: float | None = None, message: str = "channel rate limited"):
        super().__init__(message)
        self.retry_after = retry_after


class Channel(ABC):
    # stable transport key, e.g. "teams" — matches CustomerNotificationChannel.channel_type.
    channel_type: str = ""

    @abstractmethod
    async def send(self, event: NotificationEvent, config: dict) -> None:
        """Deliver `event` using `config`. Raise on any failure."""
        raise NotImplementedError
