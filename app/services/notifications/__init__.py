"""Notifications subsystem: rules → in-process event bus → channels (durable store-and-forward).

Modules:
  events      — NotificationEvent, the normalized in-memory event a rule emits.
  bus         — EventBus, a trivial in-process publish/subscribe seam (singleton `bus`).
  dispatcher  — subscribed to the bus: persists the event to the outbox, fans it out to each of the
                customer's targeted channels, and tracks per-channel delivery (with retry/backoff).
  rules/      — data-driven, frontend-managed rules + the code evaluators that run them.
  channels/   — channel adapters behind one interface (Teams implemented; Slack/WhatsApp scaffolded).
"""
