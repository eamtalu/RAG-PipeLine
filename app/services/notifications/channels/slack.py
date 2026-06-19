"""SlackChannel — scaffold.

Slack is also a simple incoming-webhook POST, so turning this on later is small: build a Block Kit
(or plain {"text": ...}) payload from the event and POST `config["webhook_url"]` via httpx, raising
on non-2xx — exactly mirroring TeamsChannel. Left unimplemented per scope (Teams first).
"""

from app.services.notifications.channels.base import Channel
from app.services.notifications.events import NotificationEvent


class SlackChannel(Channel):
    channel_type = "slack"

    async def send(self, event: NotificationEvent, config: dict) -> None:
        raise NotImplementedError(
            "Slack channel is scaffolded but not implemented yet. Implement build payload + "
            "httpx POST to config['webhook_url'] to enable."
        )
