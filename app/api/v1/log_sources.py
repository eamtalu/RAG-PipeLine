"""Remote SSH log-source API — manage a tenant's Windows Servers and pull their logs.

A customer can register one or more `LogSshSource` rows (each a Windows Server running OpenSSH),
test connectivity, and trigger an on-demand pull (now, or from a timestamp). The pull reuses the
existing Stage 1 ingest + Stage 2 finalize; it runs in the background and is polled like a Job.
"""

import asyncio
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.api.deps import get_current_customer, get_active_customer
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_fetch_run import (
    LogSshFetchRun, LogSshFetchRunStatus, LogSshFetchMode,
)
from app.persistence.repositories.log_ssh_source_repository import (
    LogSshSourceRepository, get_log_ssh_source_repository,
)
from app.services.mnp_log_ingestion.remote import ssh_client, secrets
from app.services.mnp_log_ingestion.remote.remote_fetcher import run_ssh_fetch_tracked

router = APIRouter(prefix="/logs", tags=["log-sources"])

# strong refs to in-flight background fetch tasks (asyncio only weak-refs them).
_fetch_tasks: set = set()


# --------------------------------------------------------------------------- schemas
class SshSourceCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=128, description="tenant-local label, e.g. 'prod-wms-1'")
    host: str = Field(..., min_length=1)
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(..., min_length=1)
    remote_log_dir: str = Field(..., min_length=1, description="POSIX path on the server, e.g. 'C:/logs/m3'")
    file_glob: str = Field(default="*.log")
    enabled: bool = Field(default=False, description="include in the background poller")
    poll_interval_seconds: float | None = Field(default=None, ge=5)
    # auth — provide a key file path on the backend host, OR inline PEM material (encrypted at rest)
    private_key_path: str | None = None
    private_key: str | None = Field(default=None, description="PEM private key; stored Fernet-encrypted")
    key_passphrase: str | None = None


class SshSourceUpdate(BaseModel):
    host: str | None = Field(default=None, min_length=1)
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str | None = Field(default=None, min_length=1)
    remote_log_dir: str | None = Field(default=None, min_length=1)
    file_glob: str | None = None
    enabled: bool | None = None
    poll_interval_seconds: float | None = Field(default=None, ge=5)
    private_key_path: str | None = None
    private_key: str | None = None
    key_passphrase: str | None = None


class FetchRemoteRequest(BaseModel):
    source_id: uuid.UUID | None = Field(default=None, description="one source; omit to fetch all the tenant's")
    from_timestamp: datetime | None = Field(default=None, description="ensure coverage from this time")
    mode: LogSshFetchMode | None = Field(default=None, description="incremental | timestamp | full")


def _auth_method(src: LogSshSource) -> str:
    if src.private_key_path:
        return "path"
    if src.private_key_enc:
        return "inline"
    return "none"


def _to_out(src: LogSshSource) -> dict:
    """Public view — never leaks key material or passphrases."""
    return {
        "id": str(src.id),
        "customer_code": src.customer_code,
        "name": src.name,
        "host": src.host,
        "port": src.port,
        "username": src.username,
        "remote_log_dir": src.remote_log_dir,
        "file_glob": src.file_glob,
        "enabled": src.enabled,
        "poll_interval_seconds": src.poll_interval_seconds,
        "auth_method": _auth_method(src),
        "host_key_fingerprint": src.host_key_fingerprint,
        "last_ok_at": src.last_ok_at.isoformat() if src.last_ok_at else None,
        "last_error": src.last_error,
        "created_at": src.created_at.isoformat() if src.created_at else None,
        "updated_at": src.updated_at.isoformat() if src.updated_at else None,
    }


def _encrypt_auth(values: dict, *, key_path: str | None, key_pem: str | None, passphrase: str | None) -> None:
    """Fold auth fields into a values dict for create/update, encrypting inline material. A key_path
    and inline PEM are mutually exclusive — setting one clears the other."""
    try:
        if key_path is not None:
            values["private_key_path"] = key_path or None
            if key_path:
                values["private_key_enc"] = None
        if key_pem:
            values["private_key_enc"] = secrets.encrypt(key_pem)
            values["private_key_path"] = None
        if passphrase is not None:
            values["key_passphrase_enc"] = secrets.encrypt(passphrase) if passphrase else None
    except secrets.SecretsError as exc:
        raise HTTPException(400, detail=str(exc))


# --------------------------------------------------------------------------- CRUD
@router.get("/ssh-sources")
async def list_ssh_sources(customer: str = Depends(get_current_customer),
                           repo: LogSshSourceRepository = Depends(get_log_ssh_source_repository)):
    return {"sources": [_to_out(s) for s in await repo.list_for_customer(customer)]}


@router.post("/ssh-sources", status_code=201)
async def create_ssh_source(body: SshSourceCreate,
                            customer: str = Depends(get_active_customer),
                            repo: LogSshSourceRepository = Depends(get_log_ssh_source_repository)):
    if await repo.get_by_name(customer, body.name):
        raise HTTPException(409, detail=f"A source named {body.name!r} already exists for this customer.")
    if not body.private_key_path and not body.private_key:
        raise HTTPException(400, detail="Provide private_key_path or private_key for SSH key auth.")
    values = dict(customer_code=customer, name=body.name, host=body.host, port=body.port,
                  username=body.username, remote_log_dir=body.remote_log_dir, file_glob=body.file_glob,
                  enabled=body.enabled, poll_interval_seconds=body.poll_interval_seconds)
    _encrypt_auth(values, key_path=body.private_key_path, key_pem=body.private_key,
                  passphrase=body.key_passphrase)
    return _to_out(await repo.create(**values))


@router.get("/ssh-sources/{source_id}")
async def get_ssh_source(source_id: uuid.UUID, customer: str = Depends(get_current_customer),
                         repo: LogSshSourceRepository = Depends(get_log_ssh_source_repository)):
    src = await repo.get(customer, source_id)
    if not src:
        raise HTTPException(404, detail="SSH source not found")
    return _to_out(src)


@router.patch("/ssh-sources/{source_id}")
async def update_ssh_source(source_id: uuid.UUID, body: SshSourceUpdate,
                            customer: str = Depends(get_active_customer),
                            repo: LogSshSourceRepository = Depends(get_log_ssh_source_repository)):
    src = await repo.get(customer, source_id)
    if not src:
        raise HTTPException(404, detail="SSH source not found")
    fields = body.model_dump(exclude_unset=True)
    # secrets handled separately so they're encrypted, not stored raw
    key_path = fields.pop("private_key_path", None) if "private_key_path" in fields else None
    key_pem = fields.pop("private_key", None) if "private_key" in fields else None
    passphrase = fields.pop("key_passphrase", None) if "key_passphrase" in fields else None
    values = dict(fields)
    if key_path is not None or key_pem is not None or passphrase is not None:
        # if the connection details change, an old pinned fingerprint may no longer apply
        _encrypt_auth(values, key_path=key_path, key_pem=key_pem, passphrase=passphrase)
    if any(k in fields for k in ("host", "port", "username")):
        values["host_key_fingerprint"] = None  # re-pin on next connect
    return _to_out(await repo.update(src, **values))


@router.delete("/ssh-sources/{source_id}", status_code=204)
async def delete_ssh_source(source_id: uuid.UUID, customer: str = Depends(get_active_customer),
                            repo: LogSshSourceRepository = Depends(get_log_ssh_source_repository)):
    src = await repo.get(customer, source_id)
    if not src:
        raise HTTPException(404, detail="SSH source not found")
    await repo.delete(src)


@router.post("/ssh-sources/{source_id}/test")
async def test_ssh_source(source_id: uuid.UUID, customer: str = Depends(get_active_customer),
                          repo: LogSshSourceRepository = Depends(get_log_ssh_source_repository)):
    """Connect, list the remote dir, and pin the host fingerprint on first success."""
    src = await repo.get(customer, source_id)
    if not src:
        raise HTTPException(404, detail="SSH source not found")
    try:
        result = await ssh_client.test_connection(src)
    except ssh_client.SshHostKeyMismatch as exc:
        raise HTTPException(409, detail=str(exc))
    except (ssh_client.SshConnectionError, ssh_client.SshConfigError, secrets.SecretsError) as exc:
        raise HTTPException(502, detail=str(exc))
    if not src.host_key_fingerprint and result.get("fingerprint"):
        await repo.update(src, host_key_fingerprint=result["fingerprint"])
    return {"ok": True, **result}


# --------------------------------------------------------------------------- fetch
@router.post("/fetch-remote", status_code=202)
async def fetch_remote(body: FetchRemoteRequest, customer: str = Depends(get_active_customer),
                       db: AsyncSession = Depends(get_session),
                       repo: LogSshSourceRepository = Depends(get_log_ssh_source_repository)):
    """Trigger a background pull from the tenant's Windows Server(s). NON-BLOCKING: returns 202 + a
    run_id; poll GET /logs/fetch-remote/runs/{run_id} until status is completed/failed. The run pulls
    new bytes, ingests them, and finalizes — so transaction reads are current the instant it completes.
    """
    if body.source_id is not None and not await repo.get(customer, body.source_id):
        raise HTTPException(404, detail="SSH source not found")
    mode = body.mode or (LogSshFetchMode.timestamp if body.from_timestamp else LogSshFetchMode.incremental)

    run = LogSshFetchRun(customer_code=customer, source_id=body.source_id, mode=mode,
                         requested_from=body.from_timestamp)
    db.add(run)
    await db.commit()
    await db.refresh(run)

    task = asyncio.create_task(
        run_ssh_fetch_tracked(run.id, customer, body.source_id, mode, body.from_timestamp)
    )
    _fetch_tasks.add(task)
    task.add_done_callback(_fetch_tasks.discard)
    return {"run_id": str(run.id), "status": run.status.value, "mode": mode.value,
            "poll": f"/api/v1/logs/fetch-remote/runs/{run.id}"}


@router.get("/fetch-remote/runs/{run_id}")
async def get_fetch_run(run_id: uuid.UUID, customer: str = Depends(get_current_customer),
                        db: AsyncSession = Depends(get_session)):
    run = await db.get(LogSshFetchRun, run_id)
    if not run or run.customer_code != customer:
        raise HTTPException(404, detail="Fetch run not found")
    return {
        "run_id": str(run.id),
        "customer_code": run.customer_code,
        "source_id": str(run.source_id) if run.source_id else None,
        "mode": run.mode.value,
        "requested_from": run.requested_from.isoformat() if run.requested_from else None,
        "status": run.status.value,
        "phase": run.phase.value if run.phase else None,
        "progress": run.progress,
        "files_considered": run.files_considered,
        "files_fetched": run.files_fetched,
        "bytes_fetched": run.bytes_fetched,
        "entries_ingested": run.entries_ingested,
        "error": run.error,
        "result": run.result,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
    }
