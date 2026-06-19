"""WhatsAppChannel — scaffold.

WhatsApp needs a provider (Twilio or Meta WhatsApp Cloud API) and, for business-initiated messages,
pre-approved message templates — so config will hold provider credentials + a template id, not a
plain webhook. Left unimplemented per scope; the interface is identical to the other channels.
"""

from app.services.notifications.channels.base import Channel
from app.services.notifications.events import NotificationEvent


class WhatsAppChannel(Channel):
    channel_type = "whatsapp"

    async def send(self, event: NotificationEvent, config: dict) -> None:
        raise NotImplementedError(
            "WhatsApp channel is scaffolded but not implemented yet. Wire a provider "
            "(Twilio / Meta Cloud API) with an approved template to enable."
        )
