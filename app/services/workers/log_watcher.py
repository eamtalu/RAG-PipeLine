"""Log watcher — polls a STAGING directory and ingests dropped log files (Stage 1).

IMPORTANT: this watcher MOVES files it processes (to processed/ or failed/), so it must only
ever watch a dedicated staging dir (settings.log_incoming_dir) — never a live rotating-log
directory. Read-only ingestion of in-place rotating logs is handled separately by /logs/scan.
"""

import asyncio
import logging
from pathlib import Path

from app.settings import settings
from app.config.database import async_session
from app.persistence.repositories.job_repository import JobRepository
from app.persistence.storage.local import LocalStorage
from app.services.mnp_log_ingestion.LogIngestion import LogIngestion

logger = logging.getLogger(__name__)


def _ensure_dirs() -> None:
    for d in (settings.log_incoming_dir, settings.log_processed_dir, settings.log_failed_dir):
        Path(d).mkdir(parents=True, exist_ok=True)


async def _ingest_file(path: Path) -> int:
    """Ingest a single staged file and return the entry count."""
    data = path.read_bytes()
    storage = LocalStorage(settings.upload_dir)
    async with async_session() as db:
        svc = LogIngestion(storage, JobRepository(db))
        await svc.ingest(data, path.name, background=False)
    return 0


def _move(path: Path, dest_dir: Path) -> None:
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / path.name
    if target.exists():  # avoid clobbering on name collision
        target = dest_dir / f"{path.stem}.{int(path.stat().st_mtime)}{path.suffix}"
    path.rename(target)


async def _drain_once() -> int:
    """Process every regular file currently in the staging dir. Returns count processed."""
    _ensure_dirs()
    incoming = Path(settings.log_incoming_dir)
    files = [p for p in sorted(incoming.iterdir()) if p.is_file() and not p.name.startswith(".")]
    for path in files:
        try:
            await _ingest_file(path)
            _move(path, settings.log_processed_dir)
            logger.info("Ingested + moved to processed: %s", path.name)
        except Exception:
            logger.exception("Failed to ingest %s — moving to failed/", path.name)
            try:
                _move(path, settings.log_failed_dir)
            except Exception:
                logger.exception("Could not move %s to failed/", path.name)
    return len(files)


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
