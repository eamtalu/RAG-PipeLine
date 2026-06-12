"""Log watcher — polls a STAGING directory and ingests dropped log files (Stage 1).

IMPORTANT: this watcher MOVES files it processes (to processed/ or failed/), so it must only
ever watch a dedicated staging dir (settings.log_incoming_dir) — never a live rotating-log
directory. Read-only ingestion of in-place rotating logs is handled separately by /logs/scan.

MULTI-TENANT: files are dropped into a PER-CUSTOMER subdirectory of the staging dir
(`incoming/<customer_code>/file.log`); the customer is derived from that subdir name. Files placed
directly in the staging root (no customer subdir) are moved to failed/ — we never guess a tenant.
"""

import asyncio
import logging
from pathlib import Path

from app.settings import settings
from app.api.deps import normalize_customer_code
from app.config.database import async_session
from app.persistence.repositories.job_repository import JobRepository
from app.persistence.repositories.customer_repository import CustomerRepository
from app.persistence.storage.local import LocalStorage
from app.services.mnp_log_ingestion.LogIngestion import LogIngestion

logger = logging.getLogger(__name__)


async def _is_active_customer(customer_code: str) -> bool:
    """The dropped subdir must map to a registered, ACTIVE customer — same gate as the ingest API."""
    async with async_session() as db:
        return await CustomerRepository(db).exists(customer_code, must_be_active=True)


def _ensure_dirs() -> None:
    for d in (settings.log_incoming_dir, settings.log_processed_dir, settings.log_failed_dir):
        Path(d).mkdir(parents=True, exist_ok=True)


async def _ingest_file(path: Path, customer_code: str) -> int:
    """Ingest a single staged file for a customer and return the entry count."""
    data = path.read_bytes()
    storage = LocalStorage(settings.upload_dir)
    async with async_session() as db:
        svc = LogIngestion(storage, JobRepository(db))
        await svc.ingest(data, path.name, customer_code, background=False)
    return 0


def _move(path: Path, dest_dir: Path, customer_code: str | None = None) -> None:
    dest_dir = Path(dest_dir)
    if customer_code:  # preserve the per-customer subdir in processed/ and failed/
        dest_dir = dest_dir / customer_code
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / path.name
    if target.exists():  # avoid clobbering on name collision
        target = dest_dir / f"{path.stem}.{int(path.stat().st_mtime)}{path.suffix}"
    path.rename(target)


async def _drain_once() -> int:
    """Process every file in each per-customer subdir of the staging dir. Returns count processed.

    Layout: incoming/<customer_code>/file.log. The customer is the immediate subdir name. A file in
    the staging root (no customer subdir) or under an invalid code is moved to failed/ — never guessed.
    """
    _ensure_dirs()
    incoming = Path(settings.log_incoming_dir)

    # stray files dropped directly in the root have no tenant — quarantine them.
    for path in sorted(p for p in incoming.iterdir() if p.is_file() and not p.name.startswith(".")):
        logger.error("File %s has no customer subdir — moving to failed/", path.name)
        try:
            _move(path, settings.log_failed_dir)
        except Exception:
            logger.exception("Could not move %s to failed/", path.name)

    processed = 0
    for sub in sorted(p for p in incoming.iterdir() if p.is_dir()):
        customer_code = normalize_customer_code(sub.name)
        # the subdir must be a registered, active customer (same gate as the ingest API)
        valid = customer_code is not None and await _is_active_customer(customer_code)
        files = [p for p in sorted(sub.iterdir()) if p.is_file() and not p.name.startswith(".")]
        for path in files:
            processed += 1
            if not valid:
                reason = "invalid code" if customer_code is None else "unknown/inactive customer"
                logger.error("Customer dir %r is %s — moving %s to failed/", sub.name, reason, path.name)
                try:
                    _move(path, settings.log_failed_dir, sub.name if _safe(sub.name) else None)
                except Exception:
                    logger.exception("Could not move %s to failed/", path.name)
                continue
            try:
                await _ingest_file(path, customer_code)
                _move(path, settings.log_processed_dir, customer_code)
                logger.info("Ingested + moved to processed: %s/%s", customer_code, path.name)
            except Exception:
                logger.exception("Failed to ingest %s/%s — moving to failed/", customer_code, path.name)
                try:
                    _move(path, settings.log_failed_dir, customer_code)
                except Exception:
                    logger.exception("Could not move %s to failed/", path.name)
    return processed


def _safe(name: str) -> bool:
    """A filesystem-safe subdir name (no path traversal) for quarantining files under failed/."""
    return name not in ("", ".", "..") and "/" not in name and "\\" not in name


async def run_log_watcher() -> None:
    """Main watcher loop — polls the staging dir for dropped log files."""
    logger.info("Log watcher started (dir=%s, poll=%.1fs)",
                settings.log_incoming_dir, settings.log_watcher_poll_seconds)
    while True:
        try:
            processed = await _drain_once()
            if processed == 0:
                await asyncio.sleep(settings.log_watcher_poll_seconds)
        except Exception:
            logger.exception("Log watcher error — retrying after sleep")
            await asyncio.sleep(settings.log_watcher_poll_seconds * 2)
