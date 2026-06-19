"""Channel registry — maps a channel_type string to its adapter instance.

The dispatcher resolves `CHANNELS[delivery.channel_type]` to deliver. Adding a transport = adding a
class and one line here. `KNOWN_CHANNEL_TYPES` is what the management API validates against.
"""

from app.services.notifications.channels.base import Channel
from app.services.notifications.channels.teams import TeamsChannel
from app.services.notifications.channels.slack import SlackChannel
from app.services.notifications.channels.whatsapp import WhatsAppChannel

CHANNELS: dict[str, Channel] = {
    c.channel_type: c for c in (TeamsChannel(), SlackChannel(), WhatsAppChannel())
}

# Implemented (deliverable today) vs merely scaffolded — handy for API hints / UI.
IMPLEMENTED_CHANNEL_TYPES = {"teams"}
KNOWN_CHANNEL_TYPES = set(CHANNELS.keys())


def get_channel(channel_type: str) -> Channel | None:
    return CHANNELS.get(channel_type)
