"""Step 8 of docs/esmart-agent-execution-plan.md: tenant secrets sealed at rest, never returned.

Webhook URLs sat in plain text inside `customer_notification_channels.config` and came back from
the API as typed. Now a secret goes into `tenant_secrets` under envelope encryption: a random data
key per secret seals the value, a master key from settings wraps the data key, and the master key
has an id so it can be rotated by re-wrapping without touching a single ciphertext. The channel
config keeps only a reference; the API says whether a URL is set and nothing more.
"""

import base64
import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select

from app.api.v1 import notifications as api
from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.notification import CustomerNotificationChannel
from app.persistence.models.tenant_secret import TenantSecret
from app.persistence.repositories.customer_repository import CustomerRepository
from app.persistence.repositories.notification_repository import NotificationRepository
from app.services.notifications import channel_secrets
from app.services.notifications.channels.teams import TeamsChannel
from app.services.secrets import envelope, secret_store
from app.settings import settings

CC = "test_chunk159"
URL = "https://example.invalid/hook/abc123"


def _key() -> str:
    return base64.urlsafe_b64encode(os.urandom(32)).decode()


# ==================================================== 1. the envelope, no database


def test_seal_and_open_round_trip_under_the_active_key():
    master = envelope.MasterKeys.parse(f"k1={_key()}", active="k1")
    sealed = envelope.seal(b"hunter2", master)
    assert sealed.key_id == "k1" and sealed.ciphertext != b"hunter2"
    assert envelope.open_sealed(sealed, master) == b"hunter2"


def test_a_tampered_ciphertext_is_refused():
    master = envelope.MasterKeys.parse(f"k1={_key()}", active="k1")
    sealed = envelope.seal(b"hunter2", master)
    broken = envelope.Sealed(sealed.key_id, sealed.wrapped_key, sealed.ciphertext[:-1] + b"x")
    with pytest.raises(envelope.SecretsError):
        envelope.open_sealed(broken, master)


def test_rewrap_moves_a_secret_to_the_active_key_without_touching_the_ciphertext():
    k1, k2 = _key(), _key()
    old = envelope.MasterKeys.parse(f"k1={k1}", active="k1")
    sealed = envelope.seal(b"hunter2", old)
    both = envelope.MasterKeys.parse(f"k1={k1},k2={k2}", active="k2")
    moved = envelope.rewrap(sealed, both)
    assert moved.key_id == "k2" and moved.ciphertext == sealed.ciphertext
    assert moved.wrapped_key != sealed.wrapped_key
    only_new = envelope.MasterKeys.parse(f"k2={k2}", active="k2")
    assert envelope.open_sealed(moved, only_new) == b"hunter2"
    with pytest.raises(envelope.SecretsError, match="k1"):
        envelope.open_sealed(sealed, only_new)


def test_an_unconfigured_store_refuses_to_seal(monkeypatch):
    monkeypatch.setattr(settings, "secrets_master_keys", "")
    with pytest.raises(envelope.SecretsError, match="SECRETS_MASTER_KEYS"):
        envelope.MasterKeys.from_settings()


# ==================================================== 2. the store, on Postgres


async def _wipe():
    async with async_session() as db:
        await db.execute(delete(TenantSecret).where(TenantSecret.customer_code.in_((CC, "other"))))
        await db.execute(
            delete(CustomerNotificationChannel).where(
                CustomerNotificationChannel.customer_code == CC
            )
        )
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture
async def tenant():
    await _wipe()
    async with async_session() as db:
        db.add(
            Customer(
                customer_code=CC,
                name="c159",
                timezone="Europe/London",
                active=True,
                notifications_enabled=True,
            )
        )
        await db.commit()
    yield CC
    await _wipe()


async def test_put_then_reveal_and_the_row_holds_no_plaintext(tenant):
    async with async_session() as db:
        sid = await secret_store.put(
            db, customer_code=CC, kind="webhook_url", owner="o1", plaintext=URL
        )
        await db.commit()
    async with async_session() as db:
        row = await db.get(TenantSecret, sid)
        assert URL.encode() not in row.ciphertext and URL.encode() not in row.wrapped_key
        assert await secret_store.reveal(db, sid, customer_code=CC) == URL


async def test_a_secret_is_never_revealed_to_another_tenant(tenant):
    async with async_session() as db:
        sid = await secret_store.put(
            db, customer_code=CC, kind="webhook_url", owner="o1", plaintext=URL
        )
        await db.commit()
        with pytest.raises(secret_store.SecretNotFound):
            await secret_store.reveal(db, sid, customer_code="other")


async def test_put_for_the_same_owner_replaces_the_old_secret(tenant):
    async with async_session() as db:
        first = await secret_store.put(
            db, customer_code=CC, kind="webhook_url", owner="o1", plaintext="one"
        )
        second = await secret_store.put(
            db, customer_code=CC, kind="webhook_url", owner="o1", plaintext="two"
        )
        await db.commit()
        rows = (
            (await db.execute(select(TenantSecret).where(TenantSecret.owner == "o1")))
            .scalars()
            .all()
        )
    assert first != second and [r.id for r in rows] == [second]


async def test_rewrap_all_moves_every_secret_to_the_active_key(tenant, monkeypatch):
    # Rotation is global by nature, so start from an empty table: rows other tests left
    # behind are sealed under the run's shared test key, which this test does not configure.
    async with async_session() as db:
        await db.execute(delete(TenantSecret))
        await db.commit()
    k1, k2 = _key(), _key()
    monkeypatch.setattr(settings, "secrets_master_keys", f"k1={k1}")
    monkeypatch.setattr(settings, "secrets_active_key_id", "k1")
    async with async_session() as db:
        sid = await secret_store.put(
            db, customer_code=CC, kind="webhook_url", owner="o1", plaintext=URL
        )
        await db.commit()
    monkeypatch.setattr(settings, "secrets_master_keys", f"k1={k1},k2={k2}")
    monkeypatch.setattr(settings, "secrets_active_key_id", "k2")
    async with async_session() as db:
        assert await secret_store.rewrap_all(db) == 1
        await db.commit()
    monkeypatch.setattr(settings, "secrets_master_keys", f"k2={k2}")
    async with async_session() as db:
        assert (await db.get(TenantSecret, sid)).key_id == "k2"
        assert await secret_store.reveal(db, sid, customer_code=CC) == URL


# ==================================================== 3. the channel API returns no secret


async def _create(config: dict) -> dict:
    async with async_session() as s:
        return await api.create_channel_endpoint(
            CC,
            api.CreateChannelRequest(channel_type="teams", name="ops", config=config),
            repo=NotificationRepository(s),
            customers=CustomerRepository(s),
        )


async def _db_config(channel_id: str) -> dict:
    async with async_session() as db:
        return (await db.get(CustomerNotificationChannel, uuid.UUID(channel_id))).config


async def test_create_returns_no_url_and_stores_none_in_the_channel_row(tenant):
    out = await _create({"webhook_url": URL, "max_per_minute": 5})
    assert out["config"] == {"max_per_minute": 5, "webhook_url_set": True}
    stored = await _db_config(out["id"])
    assert "webhook_url" not in stored and stored["max_per_minute"] == 5
    assert "webhook_url_secret_id" in stored
    async with async_session() as s:
        got = await api.get_channel_endpoint(out["id"], repo=NotificationRepository(s))
    assert got["config"] == {"max_per_minute": 5, "webhook_url_set": True}


async def test_an_update_that_echoes_the_masked_config_keeps_the_secret(tenant):
    out = await _create({"webhook_url": URL})
    before = (await _db_config(out["id"]))["webhook_url_secret_id"]
    async with async_session() as s:
        again = await api.update_channel_endpoint(
            out["id"],
            api.UpdateChannelRequest(config={"webhook_url_set": True, "max_per_minute": 9}),
            repo=NotificationRepository(s),
        )
    after = await _db_config(out["id"])
    assert again["config"] == {"max_per_minute": 9, "webhook_url_set": True}
    assert after["webhook_url_secret_id"] == before


async def test_an_update_with_a_new_url_replaces_the_secret(tenant):
    out = await _create({"webhook_url": URL})
    before = (await _db_config(out["id"]))["webhook_url_secret_id"]
    async with async_session() as s:
        await api.update_channel_endpoint(
            out["id"],
            api.UpdateChannelRequest(config={"webhook_url": URL + "2"}),
            repo=NotificationRepository(s),
        )
    after = (await _db_config(out["id"]))["webhook_url_secret_id"]
    assert after != before
    async with async_session() as db:
        assert await secret_store.reveal(db, uuid.UUID(after), customer_code=CC) == URL + "2"
        assert (
            await db.execute(select(TenantSecret).where(TenantSecret.customer_code == CC))
        ).scalars().all().__len__() == 1


async def test_deleting_a_channel_deletes_its_secret(tenant):
    out = await _create({"webhook_url": URL})
    async with async_session() as s:
        await api.delete_channel_endpoint(out["id"], repo=NotificationRepository(s))
    async with async_session() as db:
        assert (
            await db.execute(select(TenantSecret).where(TenantSecret.customer_code == CC))
        ).scalars().all() == []


async def test_a_channel_without_a_url_says_so(tenant):
    out = await _create({"max_per_minute": 2})
    assert out["config"] == {"max_per_minute": 2, "webhook_url_set": False}


# ==================================================== 4. sending sees the real URL


async def test_the_test_button_and_the_dispatcher_send_to_the_revealed_url(tenant, monkeypatch):
    out = await _create({"webhook_url": URL})
    seen: list[dict] = []

    async def fake_send(self, event, config):
        seen.append(config)

    monkeypatch.setattr(TeamsChannel, "send", fake_send)
    async with async_session() as s:
        await api.test_channel_endpoint(out["id"], repo=NotificationRepository(s))
    assert seen[-1]["webhook_url"] == URL
    async with async_session() as db:
        row = await db.get(CustomerNotificationChannel, uuid.UUID(out["id"]))
        assert (await channel_secrets.reveal_config(db, row))["webhook_url"] == URL


async def test_a_legacy_row_with_a_plain_url_still_sends_and_is_masked(tenant):
    async with async_session() as db:
        row = await NotificationRepository(db).create_channel(
            customer_code=CC, channel_type="teams", name="old", config={"webhook_url": URL}
        )
        assert (await channel_secrets.reveal_config(db, row))["webhook_url"] == URL
    assert api._ser_channel(row)["config"] == {"webhook_url_set": True}


# ==================================================== 5. a tenant purge takes its secrets with it


async def test_purging_a_log_space_removes_its_secrets_and_its_run_ledger(tenant):
    from app.persistence.models.agent_run import AgentRun
    from app.services.logspace_cleanup import purge_logspace

    out = await _create({"webhook_url": URL})
    async with async_session() as db:
        db.add(
            AgentRun(
                request_id=f"c159-{out['id'][:8]}",
                customer_code=CC,
                channel="teams",
                status="done",
                claimed_at=datetime.now(timezone.utc),
            )
        )
        await db.commit()
        assert await purge_logspace(db, CC) is True
    async with async_session() as db:
        secrets = (
            await db.execute(select(TenantSecret).where(TenantSecret.customer_code == CC))
        ).all()
        runs = (await db.execute(select(AgentRun).where(AgentRun.customer_code == CC))).all()
    assert secrets == [] and runs == []
