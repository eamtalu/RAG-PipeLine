"""TeamsChannel — posts an Adaptive Card to a Microsoft Teams channel via an incoming webhook.

Targets the modern Power Automate "Post to a channel when a webhook request is received" workflow
(the classic Office 365 connector is being retired), which accepts an Adaptive Card wrapped in a
`{"type":"message","attachments":[...]}` envelope. The card is built purely from the event, so a
retry days later renders identically without touching the original transaction.
"""

import logging

import httpx

from app.services.notifications.channels.base import Channel
from app.services.notifications.events import NotificationEvent
from app.settings import settings

logger = logging.getLogger(__name__)

# Adaptive Card colors by severity.
_SEVERITY_COLOR = {
    "error": "Attention", "warning": "Warning", "info": "Accent", "success": "Good",
}


class TeamsChannel(Channel):
    channel_type = "teams"

    async def send(self, event: NotificationEvent, config: dict) -> None:
        webhook_url = (config or {}).get("webhook_url")
        if not webhook_url:
            raise ValueError("teams channel config is missing 'webhook_url'")
        body = self.build_card(event)
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(webhook_url, json=body)
            resp.raise_for_status()  # non-2xx → delivery failure (retried)

    # ---- card construction (pure; unit-testable without network) -------------------------------
    def build_card(self, event: NotificationEvent) -> dict:
        color = _SEVERITY_COLOR.get(event.severity, "Default")
        bodies: list[dict] = [
            {"type": "TextBlock", "size": "Large", "weight": "Bolder", "wrap": True,
             "color": color, "text": event.title},
        ]
        if event.summary:
            bodies.append({"type": "TextBlock", "wrap": True, "isSubtle": True, "text": event.summary})

        facts = self._facts(event)
        if facts:
            bodies.append({"type": "FactSet",
                           "facts": [{"title": str(k), "value": str(v)} for k, v in facts]})

        card: dict = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": bodies,
        }

        url = self._link(event)
        if url:
            card["actions"] = [{"type": "Action.OpenUrl", "title": "Open", "url": url}]

        return {
            "type": "message",
            "attachments": [
                {"contentType": "application/vnd.microsoft.card.adaptive", "content": card}
            ],
        }

    @staticmethod
    def _facts(event: NotificationEvent) -> list[tuple[str, object]]:
        """Prefer an explicit ordered `facts` dict in the payload; else use scalar payload entries."""
        payload = event.payload or {}
        facts = payload.get("facts")
        if isinstance(facts, dict):
            return [(k, v) for k, v in facts.items() if v not in (None, "")]
        return [
            (k, v) for k, v in payload.items()
            if k not in ("facts", "url") and isinstance(v, (str, int, float, bool)) and v not in (None, "")
        ]

    @staticmethod
    def _link(event: NotificationEvent) -> str | None:
        payload = event.payload or {}
        if payload.get("url"):
            return str(payload["url"])
        base = settings.app_public_base_url.rstrip("/") if settings.app_public_base_url else ""
        txn_id = payload.get("transaction_id")
        if base and txn_id:
            # matches the matrix-log-explorer App Router route: src/app/transactions/[id]/page.tsx
            return f"{base}/transactions/{txn_id}"
        return None
