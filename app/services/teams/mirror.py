"""Writing mirror rows to the edge's DynamoDB table (stage D, step 4).

One writer for every row type the Teams app reads, in the style of `binding_mirror.py`: a protocol,
the DynamoDB implementation, an in-memory twin for tests, and a builder from settings. The envelope
and the key layout come from `mirror_rows.py`, the file shared with the edge.

A computed row (the Home snapshot) is rewritten on a timer. `ChangeTracker` skips the write when the
payload has not changed, but never for longer than a fraction of the row's TTL, so an unchanged
row is refreshed before DynamoDB drops it.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Protocol

from app.services.teams.mirror_rows import KEY_LAYOUT, MirrorEnvelope

logger = logging.getLogger(__name__)


class MirrorWriter(Protocol):
    async def put(self, envelope: MirrorEnvelope) -> None: ...
    async def remove(self, pk: str, sk: str) -> None: ...


def envelope(row_type: str, customer_code: str, *, payload: dict, as_of: str, now: float,
             site_id: str | None = None, source_version: int = 0) -> MirrorEnvelope:
    """One row ready to write, keyed and timed from the shared layout.

    ("site_home", "acme", site_id="BRI", payload={...}, as_of="...", now=1000) -> MirrorEnvelope(
    pk="EYE#acme", sk="SITE#BRI#HOME", ttl=4600, ...) ; ("sites", ...) -> ttl None
    """
    spec = KEY_LAYOUT[row_type]
    pk, sk = spec.key(customer_code, site_id=site_id)
    stamp = int(now)
    return MirrorEnvelope(pk=pk, sk=sk, row_type=row_type, as_of=as_of, written_at=stamp,
                          source_version=source_version,
                          ttl=stamp + spec.ttl_seconds if spec.ttl_seconds else None,
                          payload_json=json.dumps(payload, default=str))


def payload_digest(payload: dict) -> str:
    """A hash of the payload without its own timestamp, so "the same figures a minute later" is a no-op.

    >>> payload_digest({"a": 1, "as_of": "10:00"}) == payload_digest({"as_of": "10:01", "a": 1})
    True
    >>> payload_digest({"a": 1}) == payload_digest({"a": 2})
    False
    """
    body = {k: v for k, v in payload.items() if k != "as_of"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


class ChangeTracker:
    """Remembers what was last written per key, so an unchanged row is skipped until it is old."""

    def __init__(self, *, max_age_seconds: float):
        self._max_age = max_age_seconds
        self._last: dict[tuple[str, str], tuple[str, float]] = {}

    def should_write(self, key: tuple[str, str], digest: str, *, now: float) -> bool:
        """Whether this payload must go out: new, changed, or written too long ago.

        (never written) -> True ; (same digest 50 s ago, max age 100) -> False ; (same, 100 s) -> True
        """
        last = self._last.get(key)
        if last is None:
            return True
        last_digest, written_at = last
        return last_digest != digest or now - written_at >= self._max_age

    def wrote(self, key: tuple[str, str], digest: str, *, now: float) -> None:
        self._last[key] = (digest, now)


class DynamoMirrorWriter:
    """Same table and credentials as the binding mirror and the snapshot writer."""

    def __init__(self, table_name: str, *, region_name: str, endpoint_url: str | None = None):
        import boto3  # local import: boto3 is only needed when the Teams integration is configured
        self._table = boto3.resource("dynamodb", region_name=region_name, endpoint_url=endpoint_url).Table(table_name)

    async def put(self, envelope: MirrorEnvelope) -> None:
        item = envelope.model_dump(exclude_none=True)
        await asyncio.to_thread(lambda: self._table.put_item(Item=item))

    async def remove(self, pk: str, sk: str) -> None:
        await asyncio.to_thread(lambda: self._table.delete_item(Key={"pk": pk, "sk": sk}))


class InMemoryMirrorWriter:
    def __init__(self, *, fail: bool = False):
        self.items: dict[tuple[str, str], MirrorEnvelope] = {}
        self.put_count = 0
        self.fail = fail

    async def put(self, envelope: MirrorEnvelope) -> None:
        if self.fail:
            raise RuntimeError("mirror down")
        self.items[(envelope.pk, envelope.sk)] = envelope
        self.put_count += 1

    async def remove(self, pk: str, sk: str) -> None:
        self.items.pop((pk, sk), None)


async def write_if_changed(writer: MirrorWriter, tracker: ChangeTracker | None, env: MirrorEnvelope, *,
                           now: float) -> bool:
    """Write the row unless the tracker says it is unchanged and fresh. Returns whether it was written.

    (no tracker) -> always True ; (tracker, same payload, young) -> False
    """
    key = (env.pk, env.sk)
    digest = payload_digest(json.loads(env.payload_json))
    if tracker is not None and not tracker.should_write(key, digest, now=now):
        return False
    await writer.put(env)
    if tracker is not None:
        tracker.wrote(key, digest, now=now)
    return True


def build_mirror_writer_from_settings() -> MirrorWriter | None:
    from app.settings import settings
    if not settings.teams_edge_dynamodb_table:
        return None
    return DynamoMirrorWriter(settings.teams_edge_dynamodb_table, region_name=settings.teams_aws_region,
                              endpoint_url=settings.teams_aws_endpoint_url or None)
