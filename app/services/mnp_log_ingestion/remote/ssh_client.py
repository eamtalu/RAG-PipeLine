# ssh_client.py — open an asyncssh connection / SFTP client to a LogSshSource (Windows OpenSSH).
#
#   Auth is by private key (a key file on the backend host via private_key_path, or inline
#   Fernet-encrypted material). We disable asyncssh's own known_hosts check and PIN the server's
#   host-key fingerprint ourselves: the first successful connect returns the fingerprint for the
#   caller to persist; every later connect must match it (MITM guard).

import asyncio
import logging
from contextlib import asynccontextmanager

import asyncssh

from app.settings import settings
from app.persistence.models.log_ssh_source import LogSshSource
from app.services.mnp_log_ingestion.remote.secrets import decrypt

logger = logging.getLogger(__name__)


class SshConfigError(RuntimeError):
    """The source is missing/incoherent config (e.g. no private key)."""


class SshConnectionError(RuntimeError):
    """Connection / auth / SFTP failure against the remote host."""


class SshHostKeyMismatch(SshConnectionError):
    """The server presented a host key different from the pinned fingerprint."""


def _load_keys(source: LogSshSource) -> tuple[list, str | None]:
    """Return (client_keys, passphrase) for asyncssh.connect from the source's stored credentials."""
    passphrase = decrypt(source.key_passphrase_enc) if source.key_passphrase_enc else None
    if source.private_key_path:
        return [source.private_key_path], passphrase
    if source.private_key_enc:
        material = decrypt(source.private_key_enc)
        try:
            key = asyncssh.import_private_key(material, passphrase)
        except asyncssh.KeyImportError as exc:
            raise SshConfigError(f"Stored private key could not be parsed: {exc}") from exc
        return [key], None
    raise SshConfigError(
        f"Source {source.name!r} has no private key configured "
        "(set private_key_path or inline private_key)."
    )


def get_fingerprint(conn: asyncssh.SSHClientConnection) -> str:
    """The connected server's host-key fingerprint, e.g. 'SHA256:abc…'."""
    return conn.get_server_host_key().get_fingerprint()


@asynccontextmanager
async def connect(source: LogSshSource):
    """Async context manager yielding (connection, server_fingerprint).

    Verifies the fingerprint against source.host_key_fingerprint when one is pinned; otherwise the
    caller should persist the returned fingerprint to pin it for next time.
    """
    keys, passphrase = _load_keys(source)
    try:
        conn = await asyncio.wait_for(
            asyncssh.connect(
                host=source.host, port=source.port, username=source.username,
                client_keys=keys, passphrase=passphrase,
                known_hosts=None,  # we pin the fingerprint ourselves below
            ),
            timeout=settings.ssh_connect_timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        raise SshConnectionError(
            f"SSH connect to {source.host}:{source.port} timed out "
            f"after {settings.ssh_connect_timeout_seconds:.0f}s"
        ) from exc
    except (OSError, asyncssh.Error) as exc:
        raise SshConnectionError(f"SSH connect to {source.host}:{source.port} failed: {exc}") from exc

    try:
        fp = get_fingerprint(conn)
        if source.host_key_fingerprint and fp != source.host_key_fingerprint:
            raise SshHostKeyMismatch(
                f"Host key for {source.host}:{source.port} changed "
                f"(pinned {source.host_key_fingerprint}, got {fp}) — refusing to connect."
            )
        yield conn, fp
    finally:
        conn.close()
        try:
            await conn.wait_closed()
        except Exception:  # best-effort close; nothing actionable on teardown
            pass


@asynccontextmanager
async def sftp(source: LogSshSource):
    """Async context manager yielding (sftp_client, server_fingerprint)."""
    async with connect(source) as (conn, fp):
        try:
            client = await conn.start_sftp_client()
        except (OSError, asyncssh.Error) as exc:
            raise SshConnectionError(f"Could not open SFTP on {source.host}: {exc}") from exc
        try:
            yield client, fp
        finally:
            client.exit()


async def test_connection(source: LogSshSource) -> dict:
    """Connect, open SFTP, and list the remote dir — used by POST /logs/ssh-source/test. Returns the
    server fingerprint (to pin) and a sample of the matched files; raises SshConnectionError on
    failure with a human-readable reason."""
    async with sftp(source) as (client, fp):
        try:
            pattern = f"{source.remote_log_dir.rstrip('/')}/{source.file_glob}"
            matches = await client.glob(pattern)
        except (OSError, asyncssh.Error) as exc:
            raise SshConnectionError(
                f"Connected, but listing {source.remote_log_dir!r} failed: {exc}"
            ) from exc
        sample = sorted(str(m) for m in matches)[:25]
        return {"fingerprint": fp, "matched_files": len(matches), "sample": sample}
