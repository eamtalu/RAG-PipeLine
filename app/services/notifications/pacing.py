"""How much may be sent, to whom, and in what order.

Step 4 left exactly one place where HTTP happens — the outbox drain. This module supplies the two
decisions that attach there, and deliberately makes them pure so they can be reasoned about and
tested without a database, a webhook, or a clock that matters.

**Budget.** A per-channel ceiling, because Teams and Slack do not throttle alike and one tenant's
tight webhook should not force every other channel down to its rate. The global default lives in
settings; a channel overrides it in the `config` JSONB it already has, so this needs no schema change.

**Fairness.** The drain used to claim by `next_attempt_at` and take the first N. Freshly published
deliveries all have NULL, so a tenant with 500 of them filled the batch and every other tenant waited
however many ticks it took to drain. Round-robin takes the oldest from each tenant, then the next from
each — a flood delays itself rather than everyone.

One distinction runs through the whole module and through the drain that uses it:

    A delivery held back for budget is NOT a failure.

It has not been attempted. It must not increment `attempts`, must not record an error, and must never
edge toward dead-lettering. Confusing "not yet" with "went wrong" would silently discard a perfectly
good alert after 50 quiet deferrals.
"""

import random
from datetime import datetime, timedelta
from itertools import zip_longest

from app.settings import settings

#: Where a channel may override the global rate, inside the `config` JSONB it already carries.
_LIMIT_KEY = "max_per_minute"


def channel_limit(config: dict | None) -> int:
    """Sends allowed per window for this channel.

    `config` is operator-edited JSONB, so anything can be in it. A malformed or nonsensical value
    falls back to the default rather than raising: a typo in one channel's config must not take
    delivery down for every other channel in the system.
    """
    default = settings.notification_channel_max_per_minute
    try:
        value = int((config or {}).get(_LIMIT_KEY))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def allowance(*, sent_in_window: int, limit: int) -> int:
    """How many more this channel may send right now.

    Clamped at zero. A negative allowance would be read as "send this many" by any caller doing
    arithmetic on it, which is exactly the wrong direction to fail in.
    """
    return max(0, limit - sent_in_window)


def retry_at(now: datetime) -> datetime:
    """When to reconsider a delivery that was held back.

    Inside the rate window, so a slot has genuinely freed by then, and jittered so a burst deferred
    together does not come back as a synchronised thundering herd on the same instant.
    """
    window = settings.notification_rate_window_seconds
    return now + timedelta(seconds=random.uniform(window / 2, window))


def _group(items, key) -> dict:
    """Items bucketed by their group key, each bucket keeping its original order."""
    groups: dict = {}
    for item in items:
        groups.setdefault(key(item), []).append(item)
    return groups


def round_robin(items, *, key, limit: int) -> list:
    """Up to `limit` items, taking one per group in turn before taking a second from any.

    `zip_longest` across the buckets IS the interleave: it yields position 0 of every group, then
    position 1 of every group, and so on. Order within a group is therefore preserved, so the oldest
    alert for a tenant still goes first — fairness across tenants must not become unfairness inside
    one. The padding it inserts for shorter groups is dropped.
    """
    rows = zip_longest(*_group(items, key).values())
    return [item for row in rows for item in row if item is not None][:limit]
