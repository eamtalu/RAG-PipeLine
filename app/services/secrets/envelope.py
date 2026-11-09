"""Envelope encryption with rotatable master keys; pure functions, no database.

A secret is sealed with a random data key (AES-256-GCM); the data key is wrapped with the active
master key and stored beside the ciphertext with the master key's id. Rotation adds a new master
key, makes it active, and re-wraps the data keys: no ciphertext is ever touched. Used by
`secret_store.py`; the keys come from `settings.secrets_master_keys` and
`settings.secrets_active_key_id`.
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.settings import settings

NONCE_BYTES = 12
KEY_BYTES = 32


class SecretsError(RuntimeError):
    """The store is not configured, a key is missing, or a sealed value does not open."""


@dataclass(frozen=True)
class Sealed:
    """A sealed value and the wrapped data key that opens it, both nonce-prefixed."""

    key_id: str
    wrapped_key: bytes
    ciphertext: bytes


@dataclass(frozen=True)
class MasterKeys:
    """The configured master keys by id, and which one wraps new secrets."""

    keys: dict[str, bytes]
    active: str

    @classmethod
    def parse(cls, raw: str, active: str = "") -> MasterKeys:
        """Keys from `kid=<urlsafe base64 of 32 bytes>,...`; the active id defaults to the only key.

        ("k1=<b64>", "k1") -> MasterKeys({"k1": 32 bytes}, "k1") ; ("", "") -> SecretsError
        """
        keys: dict[str, bytes] = {}
        for part in raw.split(","):
            if "=" not in part.strip():
                continue
            kid, encoded = (s.strip() for s in part.strip().split("=", 1))
            try:
                material = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
            except (ValueError, TypeError) as exc:
                raise SecretsError(f"master key {kid!r} is not urlsafe base64") from exc
            if len(material) != KEY_BYTES:
                raise SecretsError(
                    f"master key {kid!r} must be {KEY_BYTES} bytes, got {len(material)}"
                )
            keys[kid] = material
        if not keys:
            raise SecretsError(
                "the secrets store is not configured: set SECRETS_MASTER_KEYS to "
                "`kid=<urlsafe base64 of 32 random bytes>` (see docs/secrets-store.md)"
            )
        chosen = active.strip() or (next(iter(keys)) if len(keys) == 1 else "")
        if chosen not in keys:
            raise SecretsError(f"SECRETS_ACTIVE_KEY_ID {active!r} is not one of {sorted(keys)}")
        return cls(keys=keys, active=chosen)

    @classmethod
    def from_settings(cls) -> MasterKeys:
        return cls.parse(settings.secrets_master_keys, settings.secrets_active_key_id)


def _key_for(master: MasterKeys, key_id: str) -> bytes:
    try:
        return master.keys[key_id]
    except KeyError:
        raise SecretsError(
            f"master key {key_id!r} is not configured; it was removed before every "
            f"secret wrapped under it was re-wrapped"
        ) from None


def _wrap(data_key: bytes, master: MasterKeys) -> bytes:
    nonce = os.urandom(NONCE_BYTES)
    # The key id is bound in as associated data, so a wrapped key cannot be re-labelled.
    return nonce + AESGCM(master.keys[master.active]).encrypt(
        nonce, data_key, master.active.encode()
    )


def _unwrap(wrapped: bytes, key_id: str, master: MasterKeys) -> bytes:
    try:
        return AESGCM(_key_for(master, key_id)).decrypt(
            wrapped[:NONCE_BYTES], wrapped[NONCE_BYTES:], key_id.encode()
        )
    except InvalidTag as exc:
        raise SecretsError(
            f"the data key wrapped under {key_id!r} does not unwrap: wrong master key "
            f"or tampered row"
        ) from exc


def seal(plaintext: bytes, master: MasterKeys) -> Sealed:
    """Seal a value under a fresh data key wrapped by the active master key.

    (b"hunter2", keys active k1) -> Sealed(key_id="k1", wrapped_key=..., ciphertext=...)
    """
    data_key = AESGCM.generate_key(bit_length=256)
    nonce = os.urandom(NONCE_BYTES)
    ciphertext = nonce + AESGCM(data_key).encrypt(nonce, plaintext, None)
    return Sealed(key_id=master.active, wrapped_key=_wrap(data_key, master), ciphertext=ciphertext)


def open_sealed(sealed: Sealed, master: MasterKeys) -> bytes:
    """The value back; SecretsError when the master key is missing or anything was altered.

    (sealed under k1, keys with k1) -> b"hunter2" ; (keys without k1) -> SecretsError
    """
    data_key = _unwrap(sealed.wrapped_key, sealed.key_id, master)
    try:
        return AESGCM(data_key).decrypt(
            sealed.ciphertext[:NONCE_BYTES], sealed.ciphertext[NONCE_BYTES:], None
        )
    except InvalidTag as exc:
        raise SecretsError("the sealed value does not open: tampered or wrong data key") from exc


def rewrap(sealed: Sealed, master: MasterKeys) -> Sealed:
    """The same ciphertext, its data key now wrapped by the active master key.

    (sealed under k1, keys k1+k2 active k2) -> Sealed(key_id="k2", same ciphertext)
    """
    data_key = _unwrap(sealed.wrapped_key, sealed.key_id, master)
    return Sealed(
        key_id=master.active, wrapped_key=_wrap(data_key, master), ciphertext=sealed.ciphertext
    )
