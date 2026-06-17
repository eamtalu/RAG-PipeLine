# secrets.py — encrypt/decrypt SSH private-key material at rest.
#
#   Prefer storing a private_key_path (a file on the backend host) so no secret touches the DB. When
#   inline material must be stored, it is Fernet-encrypted with settings.ssh_secret_key. If that key
#   isn't configured, encryption (and therefore inline storage) is refused — fail closed, never store
#   a plaintext key.

from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

from app.settings import settings


class SecretsError(RuntimeError):
    """Raised when inline key material is requested but ssh_secret_key is unset/invalid."""


@lru_cache(maxsize=1)
def _fernet() -> Fernet | None:
    key = (settings.ssh_secret_key or "").strip()
    if not key:
        return None
    try:
        return Fernet(key.encode("utf-8"))
    except (ValueError, TypeError) as exc:
        raise SecretsError(
            "ssh_secret_key is set but is not a valid Fernet key (urlsafe-base64, 32 bytes). "
            "Generate one with: python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\""
        ) from exc


def encrypt(plaintext: str) -> str:
    f = _fernet()
    if f is None:
        raise SecretsError(
            "Cannot store inline private-key material: ssh_secret_key is not configured. "
            "Either set ssh_secret_key, or use private_key_path (a key file on the backend host)."
        )
    return f.encrypt(plaintext.encode("utf-8")).decode("utf-8")


def decrypt(token: str) -> str:
    f = _fernet()
    if f is None:
        raise SecretsError("ssh_secret_key is not configured, so stored key material cannot be read.")
    try:
        return f.decrypt(token.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise SecretsError("Stored key material could not be decrypted (wrong ssh_secret_key?).") from exc
