# remote_fetcher.py — pull remote log bytes over SFTP and feed the existing Stage 1 + finalize.
#
#   This is the only new ingestion path: it reads bytes from a LogSshSource's Windows Server and
#   hands each newline-aligned chunk to LogIngestion.ingest(...) exactly like the upload/scan/watcher
#   paths do, then runs finalize_pending(...) ONCE at the end so transaction reads are current.
#
#   Modes (see LogSshFetchMode):
#     - incremental : per-file byte tail (checkpointed) — the poller's mode.
#     - timestamp   : ensure coverage from a requested time; only touches the servers if Postgres
#                     doesn't already cover it, then pulls files whose mtime could contain it.
#     - full        : re-pull every matching file whole (first sync / repair).
#
#   Correctness rests on the existing entry_hash content dedup (Stage 1): re-pulling overlapping
#   bytes never duplicates rows, so the byte checkpoint is a bandwidth optimisation, not a
#   correctness dependency.

import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable
from uuid import UUID

import asyncssh
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_file_checkpoint import LogSshFileCheckpoint
from app.persistence.models.log_ssh_fetch_run import (
    LogSshFetchRun, LogSshFetchRunStatus, LogSshFetchMode, LogSshFetchPhase,
)
from app.persistence.repositories.job_repository import JobRepository
from app.persistence.storage.local import LocalStorage
from app.services.mnp_log_ingestion.LogIngestion import LogIngestion
from app.services.mnp_log_ingestion.pipeline.derive_transactions import finalize_pending
from app.services.mnp_log_ingestion.remote import ssh_client

logger = logging.getLogger(__name__)

# Async callback that records live progress for a tracked run (None for the background poller).
ProgressCb = Callable[..., Awaitable[None]]


async def _write_progress(run_id: UUID, *, phase: LogSshFetchPhase | None = None,
                          files_considered: int | None = None, progress: dict | None = None) -> None:
    """Update the run row mid-flight on its OWN short-lived session so GET /fetch-remote/runs/{id}
    reflects progress DURING the fetch. Deliberately decoupled from the fetch/finalize transaction
    (which holds a per-customer advisory lock) and best-effort: a progress write must never abort the
    fetch, so failures are logged and swallowed."""
    values: dict = {}
    if phase is not None:
        values["phase"] = phase
    if files_considered is not None:
        values["files_considered"] = files_considered
    if progress is not None:
        values["progress"] = progress
    if not values:
        return
    try:
        async with async_session() as db:
            await db.execute(update(LogSshFetchRun).where(LogSshFetchRun.id == run_id).values(**values))
            await db.commit()
    except Exception:  # never let progress reporting break the actual fetch
        logger.warning("progress write failed for run %s", run_id, exc_info=True)


def _basename(remote_path: str) -> str:
    """POSIX basename — SFTP paths are forward-slash even on Windows OpenSSH."""
    return remote_path.rstrip("/").rsplit("/", 1)[-1] or remote_path


async def _ingest_chunk(db: AsyncSession, source: LogSshSource, remote_path: str, data: bytes) -> int:
    """Ingest one chunk via Stage 1 and return the number of NEW entries inserted (dedup-aware).

    Stage 1 runs in its own session (LogIngestion does that); we read the committed chunk_count back
    on our session, like POST /logs/scan does. The filename is namespaced by source so provenance
    survives when two servers share a basename."""
    filename = f"{source.name}/{_basename(remote_path)}"
    ingestion = LogIngestion(LocalStorage(settings.upload_dir), JobRepository(db))
    job = await ingestion.ingest(data, filename, source.customer_code, background=False)
    return await db.scalar(select(Job.chunk_count).where(Job.id == job.id)) or 0


async def _pull_range(client, remote_path: str, start: int, size: int, *,
                      db: AsyncSession, source: LogSshSource) -> tuple[int, int, int]:
    """Read [start, size) of a remote file in newline-aligned windows (≤ ssh_max_file_size each),
    ingesting each. Never ingests a partial trailing line: a window that doesn't reach EOF is trimmed
    to its last newline. Returns (new_offset, bytes_read, entries_inserted)."""
    offset, bytes_read, inserted = start, 0, 0
    f = await client.open(remote_path, "rb")
    try:
        while offset < size:
            window = min(settings.ssh_max_file_size, size - offset)
            data = await f.read(size=window, offset=offset)
            if not data:
                break
            at_eof = offset + len(data) >= size
            if not at_eof:
                nl = data.rfind(b"\n")
                # a full window with no newline = one absurdly long line; ingest it whole to progress.
                data = data[: nl + 1] if nl != -1 else data
            inserted += await _ingest_chunk(db, source, remote_path, data)
            offset += len(data)
            bytes_read += len(data)
    finally:
        await f.close()
    return offset, bytes_read, inserted


async def _list(client, source: LogSshSource) -> list[tuple[str, int, float]]:
    """(path, size, mtime) for every file matching the source's dir/glob."""
    pattern = f"{source.remote_log_dir.rstrip('/')}/{source.file_glob}"
    out: list[tuple[str, int, float]] = []
    for m in await client.glob(pattern):
        path = str(m)
        try:
            attrs = await client.stat(path)
        except (OSError, asyncssh.Error):
            continue  # vanished between glob and stat
        if attrs.size is None:  # directories etc.
            continue
        out.append((path, int(attrs.size), float(attrs.mtime or 0.0)))
    return sorted(out)


async def _ckpt(db: AsyncSession, source: LogSshSource, remote_path: str) -> LogSshFileCheckpoint | None:
    return await db.scalar(select(LogSshFileCheckpoint).where(
        LogSshFileCheckpoint.source_id == source.id,
        LogSshFileCheckpoint.remote_path == remote_path,
    ))


async def _save_ckpt(db: AsyncSession, source: LogSshSource, remote_path: str,
                     size: int, mtime: float, offset: int) -> None:
    row = await _ckpt(db, source, remote_path)
    now = datetime.now(timezone.utc)
    if row is None:
        db.add(LogSshFileCheckpoint(
            source_id=source.id, customer_code=source.customer_code, remote_path=remote_path,
            last_size=size, last_mtime=mtime, last_offset=offset, last_fetched_at=now,
        ))
    else:
        row.last_size, row.last_mtime, row.last_offset, row.last_fetched_at = size, mtime, offset, now
    await db.commit()


async def _fetch_source(db: AsyncSession, source: LogSshSource, mode: LogSshFetchMode,
                        from_ts: datetime | None, *,
                        on_listed: ProgressCb | None = None,
                        on_file: ProgressCb | None = None) -> dict:
    """Fetch one server. Pins the host fingerprint on first connect; per-file incremental tail or
    whole-file (timestamp/full) reads. Returns this source's stats.

    Progress hooks (no-ops for the poller): `on_listed(considered)` fires once the remote dir is
    globbed; `on_file(files_done, files_total, current_file, bytes_so_far, entries_so_far)` fires
    after EVERY listed file — including unchanged/skipped ones — so a progress bar advances smoothly."""
    considered = fetched = entries = total_bytes = 0
    per_file: list[dict] = []
    async with ssh_client.sftp(source) as (client, fp):
        if not source.host_key_fingerprint:  # pin on first successful connect
            await db.execute(update(LogSshSource).where(LogSshSource.id == source.id)
                             .values(host_key_fingerprint=fp))
            await db.commit()

        listing = await _list(client, source)
        considered = len(listing)
        if on_listed:
            await on_listed(considered)
        # timestamp mode: narrow to the files whose mtime could hold entries at/after from_ts.
        selected = (_select_timestamp_files(listing, from_ts)
                    if (mode == LogSshFetchMode.timestamp and from_ts) else None)
        for idx, (path, size, mtime) in enumerate(listing):
            start, do_pull = 0, True
            if selected is not None and path not in selected:
                do_pull = False  # timestamp mode narrowed this file out
            elif mode == LogSshFetchMode.incremental:
                ck = await _ckpt(db, source, path)
                if ck and size == ck.last_size and mtime == ck.last_mtime:
                    do_pull = False  # unchanged — no transfer
                else:
                    start = ck.last_offset if (ck and size >= ck.last_size) else 0  # shrink ⇒ re-read whole
                    if ck and start >= size:  # metadata changed but no new bytes
                        await _save_ckpt(db, source, path, size, mtime, start)
                        do_pull = False
            # timestamp (selected) / full: do_pull stays True, start stays 0 — dedup drops any overlap

            if do_pull:
                new_off, read, ins = await _pull_range(client, path, start, size, db=db, source=source)
                await _save_ckpt(db, source, path, size, mtime, new_off)
                fetched += 1
                entries += ins
                total_bytes += read
                per_file.append({"file": path, "bytes": read, "new_entries": ins})

            if on_file:
                await on_file(idx + 1, considered, path, total_bytes, entries)

    return {"source": source.name, "files_considered": considered, "files_fetched": fetched,
            "bytes_fetched": total_bytes, "entries_ingested": entries, "by_file": per_file}


def _select_timestamp_files(listing: list[tuple[str, int, float]], from_ts: datetime) -> set[str]:
    """Files whose mtime could contain entries at/after `from_ts`: every file last modified at/after
    from_ts, PLUS the single newest file modified before it (the active file whose mtime trails its
    contents). Bytes are deduped on ingest, so over-selecting only costs bandwidth."""
    cutoff = from_ts.timestamp()
    chosen = {p for p, _, m in listing if m >= cutoff}
    older = [(m, p) for p, _, m in listing if m < cutoff]
    if older:
        chosen.add(max(older)[1])
    return chosen


async def _load_sources(db: AsyncSession, customer_code: str, source_id: UUID | None,
                        *, enabled_only: bool) -> list[LogSshSource]:
    """Resolve the sources a fetch targets: one explicit source, or all of the customer's (the poller
    restricts to enabled ones; an on-demand 'fetch all' includes disabled ones too)."""
    stmt = select(LogSshSource).where(LogSshSource.customer_code == customer_code)
    if source_id is not None:
        stmt = stmt.where(LogSshSource.id == source_id)
    elif enabled_only:
        stmt = stmt.where(LogSshSource.enabled.is_(True))
    return list((await db.execute(stmt.order_by(LogSshSource.name.asc()))).scalars().all())


async def _local_min_ts(db: AsyncSession, customer_code: str) -> datetime | None:
    return await db.scalar(
        select(func.min(LogEntry.timestamp)).where(LogEntry.customer_code == customer_code)
    )


async def fetch_now(db: AsyncSession, customer_code: str, *, source_id: UUID | None = None,
                    mode: LogSshFetchMode = LogSshFetchMode.incremental,
                    from_ts: datetime | None = None, enabled_only: bool = False,
                    on_progress: ProgressCb | None = None) -> dict:
    """Pull from the targeted source(s), then finalize ONCE so transaction reads are current.

    Per-source failures are isolated (one unreachable server doesn't abort the others) and recorded
    on that source's last_error; a reachable source updates last_ok_at. Returns aggregate stats incl.
    the finalize result. Shared by the on-demand endpoint and the background poller.

    `on_progress` (the tracked endpoint passes one; the poller doesn't) is called with keyword fields
    — phase / files_considered / progress — at each stage so the run row reflects live progress."""
    sources = await _load_sources(db, customer_code, source_id, enabled_only=enabled_only)
    agg = {"customer_code": customer_code, "mode": mode.value, "sources": len(sources),
           "files_considered": 0, "files_fetched": 0, "bytes_fetched": 0,
           "entries_ingested": 0, "by_source": [], "errors": []}

    async def _regrouping() -> None:
        if on_progress is not None:
            await on_progress(phase=LogSshFetchPhase.regrouping)

    if not sources:
        await _regrouping()
        agg["finalize"] = await finalize_pending(db, customer_code)  # idempotent (windows=0 if none)
        return agg

    # timestamp mode: if Postgres already covers from_ts, don't touch the servers at all.
    if mode == LogSshFetchMode.timestamp and from_ts is not None:
        local_min = await _local_min_ts(db, customer_code)
        if local_min is not None and from_ts >= local_min:
            agg["already_local"] = True
            await _regrouping()
            agg["finalize"] = await finalize_pending(db, customer_code)
            return agg

    # running cumulative totals across already-completed sources, so per-source hooks can report a
    # global "so far" while a later source is still mid-flight.
    base = {"considered": 0, "bytes": 0, "entries": 0}
    for idx, source in enumerate(sources):
        on_listed = on_file = None
        if on_progress is not None:
            def _prog(done: int, total: int, current_file: str | None,
                      *, _name=source.name, _idx=idx) -> dict:
                return {"current_source": _name, "source_index": _idx + 1, "sources_total": len(sources),
                        "files_total": total, "files_done": done, "current_file": current_file,
                        "bytes_so_far": base["bytes"], "entries_so_far": base["entries"]}

            async def on_listed(total: int, *, _prog=_prog) -> None:
                await on_progress(phase=LogSshFetchPhase.fetching,
                                  files_considered=base["considered"] + total,
                                  progress=_prog(0, total, None))

            async def on_file(done: int, total: int, current_file: str,
                              src_bytes: int, src_entries: int, *, _name=source.name, _idx=idx) -> None:
                await on_progress(
                    phase=LogSshFetchPhase.fetching,
                    files_considered=base["considered"] + total,
                    progress={"current_source": _name, "source_index": _idx + 1,
                              "sources_total": len(sources), "files_total": total, "files_done": done,
                              "current_file": current_file,
                              "bytes_so_far": base["bytes"] + src_bytes,
                              "entries_so_far": base["entries"] + src_entries})

        try:
            stats = await _fetch_source(db, source, mode, from_ts, on_listed=on_listed, on_file=on_file)
            await db.execute(update(LogSshSource).where(LogSshSource.id == source.id)
                             .values(last_ok_at=datetime.now(timezone.utc), last_error=None))
            await db.commit()
            agg["by_source"].append(stats)
            agg["files_considered"] += stats["files_considered"]
            agg["files_fetched"] += stats["files_fetched"]
            agg["bytes_fetched"] += stats["bytes_fetched"]
            agg["entries_ingested"] += stats["entries_ingested"]
            base["considered"] += stats["files_considered"]
            base["bytes"] += stats["bytes_fetched"]
            base["entries"] += stats["entries_ingested"]
        except Exception as exc:  # isolate one bad server
            logger.exception("SSH fetch failed for source %s/%s", customer_code, source.name)
            await db.rollback()
            await db.execute(update(LogSshSource).where(LogSshSource.id == source.id)
                             .values(last_error=str(exc)[:2000]))
            await db.commit()
            agg["errors"].append({"source": source.name, "error": str(exc)})

    # one finalize for everything the sources just fed (like the watcher's drain-empty flush).
    await _regrouping()
    agg["finalize"] = await finalize_pending(db, customer_code)
    return agg


async def run_ssh_fetch_tracked(run_id: UUID, customer_code: str, source_id: UUID | None,
                                mode: LogSshFetchMode, from_ts: datetime | None) -> None:
    """Background entry point for POST /logs/fetch-remote: run fetch_now and record the outcome on the
    log_ssh_fetch_runs row so the frontend can poll it. Own session; never raises (no caller to
    catch) — failures land as status=failed with the error text."""
    async def _cb(**fields) -> None:  # live progress writer bound to this run
        await _write_progress(run_id, **fields)

    async with async_session() as db:
        try:
            stats = await fetch_now(db, customer_code, source_id=source_id, mode=mode,
                                    from_ts=from_ts, enabled_only=False, on_progress=_cb)
            values = dict(
                status=LogSshFetchRunStatus.completed, phase=LogSshFetchPhase.done,
                files_considered=stats.get("files_considered"),
                files_fetched=stats.get("files_fetched"),
                bytes_fetched=stats.get("bytes_fetched"),
                entries_ingested=stats.get("entries_ingested"),
                result=stats, finished_at=datetime.now(timezone.utc),
            )
        except Exception as exc:
            logger.exception("Tracked SSH fetch failed (run=%s customer=%s)", run_id, customer_code)
            await db.rollback()
            values = dict(status=LogSshFetchRunStatus.failed, phase=LogSshFetchPhase.done,
                          error=str(exc), finished_at=datetime.now(timezone.utc))
        await db.execute(update(LogSshFetchRun).where(LogSshFetchRun.id == run_id).values(**values))
        await db.commit()
