# remote_fetcher.py — pull remote log bytes over SFTP and hand them to ingestion.
#
#   It reads bytes from a LogSshSource's Windows Server and hands each newline-aligned chunk to
#   ingestion, exactly like the upload/scan/watcher paths do.
#
#   This module does NOT stitch. Stage 1 writes a log_regroup_pending ticket in the same transaction
#   as its entries, and the stitch worker (app/services/workers/log_stitch_worker.py) owns draining
#   that queue. Transport has no business knowing Stage 2 exists.
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

import asyncio
import hashlib
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable
from uuid import UUID, uuid4

import asyncssh
from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import async_session, engine

# Fixed namespace (classid) for the per-host fetch advisory lock. Uses the TWO-INT advisory-lock
# keyspace `pg_(try_)advisory_lock(classid, objid)`, which is disjoint from the single-bigint space
# Stage 2 stitching uses (`pg_advisory_xact_lock(hashtext(customer_code))`) — so the two locks can
# never collide or deadlock even on a hash tie. objid = hashtext("host:port").
SSH_FETCH_LOCK_CLASSID = 0x55AA
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_source_object import LogSourceObject, SourceObjectStatus
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_file_checkpoint import LogSshFileCheckpoint
from app.persistence.models.log_ssh_fetch_run import (
    LogSshFetchRun, LogSshFetchRunStatus, LogSshFetchMode, LogSshFetchPhase,
)
from app.persistence.repositories.job_repository import JobRepository
from app.persistence.storage.local import LocalStorage
from app.services.mnp_log_ingestion.LogIngestion import LogIngestion
from app.services.mnp_log_ingestion.io_errors import is_disk_io_error, disk_io_detail
from app.services.mnp_log_ingestion.remote import ssh_client

logger = logging.getLogger(__name__)

# Async callback that records live progress for a tracked run (None for the background poller).
ProgressCb = Callable[..., Awaitable[None]]


async def _write_progress(run_id: UUID, *, phase: LogSshFetchPhase | None = None,
                          files_considered: int | None = None, progress: dict | None = None) -> None:
    """Update the run row mid-flight on its OWN short-lived session so GET /fetch-remote/runs/{id}
    reflects progress DURING the fetch. Runs on its own connection, independent of both the per-host
    fetch advisory lock (_host_lock) and Stage 2's per-customer pg_advisory_xact_lock; and
    best-effort: a progress write must never abort the fetch, so failures are logged and swallowed."""
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


@asynccontextmanager
async def _host_lock(source: LogSshSource, *, skip_if_busy: bool):
    """Session-scoped advisory lock keyed on host:port, held on a DEDICATED idle connection for the
    whole fetch. Guarantees at most one live SSH connection per server across the poller and manual
    fetches (and across tenants sharing a host), and serialises checkpoint writes. Yields whether the
    lock was acquired.

    - skip_if_busy=True (poller): non-blocking try-lock; yields False if another fetch holds it.
    - skip_if_busy=False (on-demand): blocks up to ssh_fetch_lock_wait_seconds, else raises.

    Closing the connection auto-releases the session-scoped lock, so it can never leak."""
    objid = func.hashtext(f"{source.host.strip().lower()}:{source.port}")
    conn = await engine.connect()
    acquired = False
    try:
        if skip_if_busy:
            acquired = bool(await conn.scalar(
                select(func.pg_try_advisory_lock(SSH_FETCH_LOCK_CLASSID, objid))))
        else:
            try:
                await asyncio.wait_for(
                    conn.execute(select(func.pg_advisory_lock(SSH_FETCH_LOCK_CLASSID, objid))),
                    timeout=settings.ssh_fetch_lock_wait_seconds)
                acquired = True
            except asyncio.TimeoutError as exc:
                raise ssh_client.SshConnectionError(
                    f"another fetch for {source.host}:{source.port} is in progress"
                ) from exc
        yield acquired
    finally:
        if acquired:
            try:
                await conn.scalar(select(func.pg_advisory_unlock(SSH_FETCH_LOCK_CLASSID, objid)))
            except Exception:  # the connection close below releases the lock regardless
                pass
        await conn.close()


def _basename(remote_path: str) -> str:
    """POSIX basename — SFTP paths are forward-slash even on Windows OpenSSH."""
    return remote_path.rstrip("/").rsplit("/", 1)[-1] or remote_path


async def _ingest_chunk(source: LogSshSource, remote_path: str, data: bytes) -> int:
    """Ingest one chunk via Stage 1 and return the number of NEW entries inserted (dedup-aware).

    Opens its OWN short session so no DB connection is held across the SFTP reads — the only thing
    held for the whole per-source loop is the single SFTP connection. The filename is namespaced by
    source so provenance survives when two servers share a basename."""
    filename = f"{source.name}/{_basename(remote_path)}"
    async with async_session() as db:
        ingestion = LogIngestion(LocalStorage(settings.upload_dir), JobRepository(db))
        job = await ingestion.ingest(data, filename, source.customer_code, background=False)
        return await db.scalar(select(Job.chunk_count).where(Job.id == job.id)) or 0


async def _queue_and_checkpoint(source: LogSshSource, remote_path: str, *, size: int, mtime: float,
                                offset: int, head_fp: str | None, storage_key: str,
                                start_offset: int, end_offset: int) -> None:
    """Record "these bytes still need parsing" AND advance the file checkpoint, atomically.

    This single transaction is the load-bearing guarantee of the whole decoupling. The checkpoint is
    a promise that everything behind it is handled; the queue row is what makes that promise true
    once parsing is no longer inline.

    - Checkpoint advanced WITHOUT the row -> those bytes are skipped forever and nothing reports it.
    - Row written WITHOUT the checkpoint   -> the next poll re-downloads the same bytes.

    Committing both together removes the window entirely, so a crash anywhere before COMMIT simply
    leaves the previous state and the next poll re-fetches (which is exactly today's behaviour).
    """
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        db.add(LogSourceObject(
            customer_code=source.customer_code,
            source_id=source.id,
            source_name=source.name,
            remote_path=remote_path,
            start_offset=start_offset,
            end_offset=end_offset,
            observed_size=size,
            observed_mtime=mtime,
            head_fingerprint=head_fp,
            storage_key=storage_key,
            status=SourceObjectStatus.pending,
            max_attempts=settings.log_parse_max_attempts,
        ))
        stmt = pg_insert(LogSshFileCheckpoint).values(
            source_id=source.id, customer_code=source.customer_code, remote_path=remote_path,
            last_size=size, last_mtime=mtime, last_offset=offset, last_fetched_at=now,
            head_fingerprint=head_fp,
        ).on_conflict_do_update(
            index_elements=["source_id", "remote_path"],
            set_=dict(last_size=size, last_mtime=mtime, last_offset=offset, last_fetched_at=now,
                      head_fingerprint=head_fp),
        )
        await db.execute(stmt)
        await db.commit()          # both, or neither


async def _queue_backlog_exceeded(customer_code: str) -> bool:
    """Backpressure. Today the inline await makes it impossible for the fetcher to outrun the
    database; once decoupled it can, and ./uploads would grow without limit. A tenant already
    holding more than the cap is skipped for this tick."""
    from app.services.workers.log_parse_worker import pending_count  # local: avoids an import cycle
    depth = await pending_count(customer_code)
    if depth > settings.log_parse_queue_max_pending:
        logger.warning(
            "Ingest queue backlog for %s is %d (> %d) — pausing fetch this tick so the parser can "
            "catch up", customer_code, depth, settings.log_parse_queue_max_pending)
        return True
    return False


async def _stage_range(client, remote_path: str, start: int, size: int, *,
                       source: LogSshSource, mtime: float, head_fp: str | None) -> tuple[int, int, int]:
    """Queue-mode counterpart of _pull_range: download the newline-aligned windows and SAVE them,
    but do NOT parse. Returns (new_offset, bytes_read, objects_queued).

    `mtime` and `head_fp` are threaded through rather than defaulted, because they are written onto
    the checkpoint and are what `_plan_incremental` uses to detect rotation. Writing a placeholder
    here would make every next poll think the file had been replaced and re-read it whole.

    Each window is checkpointed with its own queue row, so a long catch-up becomes several
    independently retryable units and partial progress survives a crash mid-file.
    """
    storage = LocalStorage(settings.upload_dir)
    offset, bytes_read, queued = start, 0, 0
    f = await ssh_client.op(client.open(remote_path, "rb"), f"open {remote_path}")
    try:
        while offset < size:
            window = min(settings.ssh_max_file_size, size - offset)
            data = await ssh_client.op(f.read(size=window, offset=offset), "read")
            if not data:
                break
            at_eof = offset + len(data) >= size
            if not at_eof:
                nl = data.rfind(b"\n")
                data = data[: nl + 1] if nl != -1 else data
            chunk_start, chunk_end = offset, offset + len(data)
            key = f"{source.customer_code}/{uuid4().hex}/{source.name}-{_basename(remote_path)}"
            await storage.save(key, data)
            await _queue_and_checkpoint(
                source, remote_path, size=size, mtime=mtime, offset=chunk_end, head_fp=head_fp,
                storage_key=key, start_offset=chunk_start, end_offset=chunk_end)
            queued += 1
            offset = chunk_end
            bytes_read += len(data)
    finally:
        await f.close()
    return offset, bytes_read, queued


async def _pull_range(client, remote_path: str, start: int, size: int, *,
                      source: LogSshSource) -> tuple[int, int, int]:
    """Read [start, size) of a remote file in newline-aligned windows (≤ ssh_max_file_size each),
    ingesting each. Never ingests a partial trailing line: a window that doesn't reach EOF is trimmed
    to its last newline. Every SFTP call is bounded by ssh_client.op; no DB session is held across a
    read. Returns (new_offset, bytes_read, entries_inserted)."""
    offset, bytes_read, inserted = start, 0, 0
    f = await ssh_client.op(client.open(remote_path, "rb"), f"open {remote_path}")
    try:
        while offset < size:
            window = min(settings.ssh_max_file_size, size - offset)
            data = await ssh_client.op(f.read(size=window, offset=offset), "read")
            if not data:
                break
            at_eof = offset + len(data) >= size
            if not at_eof:
                nl = data.rfind(b"\n")
                # a full window with no newline = one absurdly long line; ingest it whole to progress.
                data = data[: nl + 1] if nl != -1 else data
            inserted += await _ingest_chunk(source, remote_path, data)
            offset += len(data)
            bytes_read += len(data)
    finally:
        await f.close()
    return offset, bytes_read, inserted


async def _list(client, source: LogSshSource) -> list[tuple[str, int, float]]:
    """(path, size, mtime) for every file matching the source's dir/glob. Glob and per-file stat are
    each bounded by ssh_client.op; a file that vanishes between glob and stat is skipped, but an op
    TIMEOUT propagates (it means a hung connection, not a missing file)."""
    pattern = f"{source.remote_log_dir.rstrip('/')}/{source.file_glob}"
    out: list[tuple[str, int, float]] = []
    for m in await ssh_client.op(client.glob(pattern), "glob"):
        path = str(m)
        try:
            attrs = await ssh_client.op(client.stat(path), "stat")
        except (OSError, asyncssh.Error):
            continue  # vanished between glob and stat
        if attrs.size is None:  # directories etc.
            continue
        out.append((path, int(attrs.size), float(attrs.mtime or 0.0)))
    return sorted(out)


async def _load_ckpts(source: LogSshSource) -> dict[str, tuple[int, float, int, str | None]]:
    """Snapshot this source's checkpoints once (own short session) as
    {remote_path: (last_size, last_mtime, last_offset, head_fingerprint)}, so the per-file decision
    loop holds no DB connection during the SFTP transfer."""
    async with async_session() as db:
        rows = (await db.execute(select(LogSshFileCheckpoint).where(
            LogSshFileCheckpoint.source_id == source.id))).scalars().all()
        return {r.remote_path: (r.last_size, r.last_mtime, r.last_offset, r.head_fingerprint)
                for r in rows}


async def _save_ckpt(source: LogSshSource, remote_path: str, size: int, mtime: float, offset: int,
                     head_fingerprint: str | None = None) -> None:
    """Upsert the file checkpoint on its OWN short session. ON CONFLICT DO UPDATE so an overlapping
    write (should the per-host lock ever be bypassed) can never raise a unique-constraint error."""
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        stmt = pg_insert(LogSshFileCheckpoint).values(
            source_id=source.id, customer_code=source.customer_code, remote_path=remote_path,
            last_size=size, last_mtime=mtime, last_offset=offset, last_fetched_at=now,
            head_fingerprint=head_fingerprint,
        ).on_conflict_do_update(
            index_elements=["source_id", "remote_path"],
            set_=dict(last_size=size, last_mtime=mtime, last_offset=offset, last_fetched_at=now,
                      head_fingerprint=head_fingerprint),
        )
        await db.execute(stmt)
        await db.commit()


async def _read_head_fp(client, remote_path: str) -> str:
    """sha256 of the first ssh_fingerprint_bytes of the file — the content signature used to detect a
    rotated/replaced file at a reused path. Cheap: one small open+read+close, each timeout-bounded."""
    f = await ssh_client.op(client.open(remote_path, "rb"), f"open-head {remote_path}")
    try:
        data = await ssh_client.op(
            f.read(size=settings.ssh_fingerprint_bytes, offset=0), "read-head")
    finally:
        await f.close()
    return hashlib.sha256(data or b"").hexdigest()


async def _prune_checkpoints(source: LogSshSource, present_paths: set[str]) -> None:
    """Best-effort delete of checkpoints for paths no longer in the listing AND older than the
    retention window. Skips entirely on an empty listing so a transient empty glob never wipes
    everything. Never raises — housekeeping must not fail a fetch."""
    if not present_paths:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.ssh_checkpoint_retention_days)
    try:
        async with async_session() as db:
            await db.execute(delete(LogSshFileCheckpoint).where(
                LogSshFileCheckpoint.source_id == source.id,
                LogSshFileCheckpoint.remote_path.notin_(present_paths),
                LogSshFileCheckpoint.last_fetched_at < cutoff,
            ))
            await db.commit()
    except Exception:
        logger.warning("checkpoint prune failed for source %s", source.id, exc_info=True)


@dataclass
class _FetchPlan:
    """Per-file decision for incremental mode: whether to transfer, from what offset, and (when
    skipping) the offset to record on the path's checkpoint."""
    do_pull: bool
    start: int = 0
    save_offset: int | None = None   # skip case: upsert the path checkpoint at this consumed offset
    reason: str = ""


def _plan_incremental(ck: tuple | None, sig_consumed: dict[tuple[str, int, float], int],
                      size: int, mtime: float, head_fp: str | None, n: int) -> _FetchPlan:
    """PURE per-file decision for incremental mode (no I/O), so it is exhaustively unit-testable.

    - `ck` = (last_size, last_mtime, last_offset, head_fingerprint) for THIS path, or None.
    - `sig_consumed` = {(head_fp, size, mtime): max_offset}, a content-identity index built once from
      THIS source's pre-poll checkpoint snapshot. It lets us recognise a file whose bytes we have
      ALREADY fully ingested under a DIFFERENT path — which is exactly what rename-cascade rotation
      produces (app.txt -> app.txt.1 -> app.txt.2 ...) — and skip re-transferring it.

    Order matters: the content-identity skip (2) is checked before the same-path rotation re-read (3),
    because a cascaded file looks 'rotated' at its new path yet its bytes are already in the DB.
    """
    stored_fp = ck[3] if ck else None
    fp_reliable = ck is not None and stored_fp is not None and ck[0] >= n and size >= n
    rotated = fp_reliable and stored_fp != head_fp

    # 1. genuinely unchanged at this path -> skip (backfill a legacy NULL fingerprint if needed).
    if ck and not rotated and size == ck[0] and mtime == ck[1]:
        return _FetchPlan(False, save_offset=(ck[2] if stored_fp is None else None), reason="unchanged")

    # 2. content we've ALREADY fully ingested, now appearing at a new path (rotation cascade).
    #    Match the FULL (fingerprint, size, mtime) triple — a rename preserves all three — so a false
    #    skip is effectively impossible; any mismatch falls through to a safe (re)read, and the
    #    entry_hash dedup remains the correctness backstop for whatever we DO transfer. Requires
    #    size >= n so the head fingerprint is a reliable content identity (same rule as fp_reliable).
    if head_fp is not None and size >= n and sig_consumed.get((head_fp, size, mtime), -1) >= size:
        return _FetchPlan(False, save_offset=size, reason="rotated-content-skip")

    # 3. rotation/replace/truncation ON THIS path -> re-read whole (dedup drops any overlap).
    if rotated or (ck and size < ck[0]):
        return _FetchPlan(True, start=0, reason="rotated-reread")

    # 4. tail from the last offset (or from 0 for a brand-new path).
    start = ck[2] if ck else 0
    if ck and start >= size:  # metadata changed but no new bytes
        return _FetchPlan(False, save_offset=start, reason="no-new-bytes")
    return _FetchPlan(True, start=start, reason=("append" if ck else "new-file"))


async def _fetch_source(source: LogSshSource, mode: LogSshFetchMode,
                        from_ts: datetime | None, *,
                        on_listed: ProgressCb | None = None,
                        on_file: ProgressCb | None = None) -> dict:
    """Fetch one server. Pins the host fingerprint on first connect; per-file incremental tail or
    whole-file (timestamp/full) reads. Returns this source's stats.

    Holds NO long-lived DB session — the only thing held for the whole loop is the single SFTP
    connection; every DB touch (checkpoint preload, pin, per-file save) uses its own short session.
    The caller wraps this in _host_lock so at most one connection exists per server.

    Progress hooks (no-ops for the poller): `on_listed(considered)` fires once the remote dir is
    globbed; `on_file(files_done, files_total, current_file, bytes_so_far, entries_so_far)` fires
    after EVERY listed file — including unchanged/skipped ones — so a progress bar advances smoothly."""
    considered = fetched = entries = total_bytes = content_skipped = io_skipped = queued = 0
    per_file: list[dict] = []
    # Queue mode: download + save + record a work row, and let the parse worker do Stage 1. With the
    # flag off, every line below behaves exactly as it did before this feature existed.
    queue_mode = bool(settings.log_parse_worker_enabled)
    ckpts = await _load_ckpts(source)  # {path: (last_size, last_mtime, last_offset, head_fingerprint)}
    # Content-identity index from the SAME pre-poll snapshot: (head_fp, size, mtime) -> max consumed
    # offset. Built once and immutable during the loop, so a file processed later still sees a content
    # signature even after we've upserted the checkpoint of the path that used to hold it this poll.
    # This is what lets rename-cascade rotation skip re-downloading content already in the DB.
    sig_consumed: dict[tuple[str, int, float], int] = {}
    for (l_size, l_mtime, l_off, l_fp) in ckpts.values():
        if l_fp is not None:
            key = (l_fp, l_size, l_mtime)
            sig_consumed[key] = max(sig_consumed.get(key, -1), l_off)
    async with ssh_client.sftp(source) as (client, fp):
        if not source.host_key_fingerprint:  # pin on first successful connect
            async with async_session() as db:
                await db.execute(update(LogSshSource).where(LogSshSource.id == source.id)
                                 .values(host_key_fingerprint=fp))
                await db.commit()
            source.host_key_fingerprint = fp  # keep the in-memory copy consistent

        listing = await _list(client, source)
        considered = len(listing)
        if on_listed:
            await on_listed(considered)
        # timestamp mode: narrow to the files whose mtime could hold entries at/after from_ts.
        selected = (_select_timestamp_files(listing, from_ts)
                    if (mode == LogSshFetchMode.timestamp and from_ts) else None)
        for idx, (path, size, mtime) in enumerate(listing):
            start, do_pull, head_fp = 0, True, None
            if mode == LogSshFetchMode.seed:
                # "start from now": mark this file as fully consumed up to its current end WITHOUT
                # ingesting anything, so a later poll only picks up new appends (zero backfill).
                do_pull = False
                head_fp = await _read_head_fp(client, path)
                await _save_ckpt(source, path, size, mtime, size, head_fp)
            elif selected is not None and path not in selected:
                # timestamp resume: seed this pre-window file's checkpoint to its current end WITHOUT
                # ingesting, so once auto-polling resumes the incremental poller only appends new
                # bytes and never backfills the pre-window history (forward-only resume, §4.4).
                do_pull = False
                head_fp = await _read_head_fp(client, path)
                await _save_ckpt(source, path, size, mtime, size, head_fp)
            elif mode == LogSshFetchMode.incremental:
                # Fingerprint the file head (first N bytes) to identify content: it detects a reused
                # path (rotation) even when size + mtime coincidentally match, AND — via sig_consumed —
                # recognises content we already ingested that has cascaded to a new path, so we skip
                # re-downloading it. The first-N-bytes hash is a STABLE identity once size >= N
                # (append-only logs never rewrite their first N bytes); below N we fall back to
                # size/mtime and never mistake a small-file append for a rotation.
                head_fp = await _read_head_fp(client, path)
                plan = _plan_incremental(ckpts.get(path), sig_consumed, size, mtime, head_fp,
                                         settings.ssh_fingerprint_bytes)
                do_pull, start = plan.do_pull, plan.start
                if not do_pull and plan.save_offset is not None:
                    await _save_ckpt(source, path, size, mtime, plan.save_offset, head_fp)
                if plan.reason == "rotated-content-skip":
                    content_skipped += 1
            # timestamp (selected) / full: do_pull stays True, start stays 0 — dedup drops any overlap

            if do_pull:
                if head_fp is None:  # non-incremental path hasn't fingerprinted yet
                    head_fp = await _read_head_fp(client, path)
                try:
                    if queue_mode:
                        # Download + save + queue. _stage_range advances the checkpoint itself, in
                        # the same transaction as each queue row, so no _save_ckpt call follows.
                        new_off, read, ins = await _stage_range(
                            client, path, start, size, source=source, mtime=mtime, head_fp=head_fp)
                    else:
                        new_off, read, ins = await _pull_range(client, path, start, size, source=source)
                except Exception as exc:
                    # A dead disk sector hit while ingesting THIS file must not abort the whole source
                    # (nor the poll). Skip this file, record it, and move to the next — the checkpoint
                    # is left unadvanced so a later retry can pick it up if the sector ever reads again.
                    if not is_disk_io_error(exc):
                        raise
                    io_skipped += 1
                    logger.critical(
                        "DISK I/O ERROR fetching %s/%s: %s — skipping this file, continuing the poll. "
                        "The disk has an unreadable sector; ensure backups.",
                        source.name, _basename(path), disk_io_detail(exc),
                    )
                    per_file.append({"file": path, "io_error": disk_io_detail(exc)})
                else:
                    if queue_mode:
                        # _stage_range already committed the checkpoint with each queue row; calling
                        # _save_ckpt here would be a second, non-atomic write of the same fact.
                        queued += ins
                        per_file.append({"file": path, "bytes": read, "objects_queued": ins})
                    else:
                        await _save_ckpt(source, path, size, mtime, new_off, head_fp)
                        entries += ins
                        per_file.append({"file": path, "bytes": read, "new_entries": ins})
                    fetched += 1
                    total_bytes += read

            if on_file:
                await on_file(idx + 1, considered, path, total_bytes, entries)

        # housekeeping: drop checkpoints for paths that vanished long ago (best-effort, non-empty only)
        await _prune_checkpoints(source, {p for p, _, _ in listing})

    # `entries_ingested` stays in the shape for compatibility, but in queue mode it is necessarily 0
    # at this point: the bytes are recorded, not yet parsed. `objects_queued` is the honest number
    # for that phase, and the real entry count lands on the log_source_objects row afterwards.
    return {"source": source.name, "files_considered": considered, "files_fetched": fetched,
            "bytes_fetched": total_bytes, "entries_ingested": entries,
            "objects_queued": queued,
            "content_skipped": content_skipped, "io_skipped": io_skipped, "by_file": per_file}


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
                        *, enabled_only: bool = False, disabled_only: bool = False) -> list[LogSshSource]:
    """Resolve the sources a fetch targets: one explicit source, or all of the customer's. The poller
    restricts to enabled ones (`enabled_only`); a manual 'fetch all' restricts to disabled ones
    (`disabled_only`, the ownership contract — the poller owns enabled sources)."""
    stmt = select(LogSshSource).where(LogSshSource.customer_code == customer_code)
    if source_id is not None:
        stmt = stmt.where(LogSshSource.id == source_id)
    elif enabled_only:
        stmt = stmt.where(LogSshSource.enabled.is_(True))
    elif disabled_only:
        stmt = stmt.where(LogSshSource.enabled.is_(False))
    return list((await db.execute(stmt.order_by(LogSshSource.name.asc()))).scalars().all())


async def _local_min_ts(db: AsyncSession, customer_code: str) -> datetime | None:
    return await db.scalar(
        select(func.min(LogEntry.timestamp)).where(LogEntry.customer_code == customer_code)
    )


async def _record_success(source: LogSshSource, *, drive_breaker: bool) -> None:
    """Stamp a successful attempt: last_ok_at + last_attempt_at now, clear last_error. When driven by
    the poller, reset the circuit breaker (consecutive_failures=0, auto_disabled_at=None). Reads the
    row fresh so it is safe to call repeatedly on the same source object."""
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        row = await db.get(LogSshSource, source.id)
        if row is None:
            return
        row.last_ok_at, row.last_error, row.last_attempt_at = now, None, now
        if drive_breaker:
            row.consecutive_failures, row.auto_disabled_at = 0, None
        await db.commit()
    source.consecutive_failures, source.auto_disabled_at = 0, None  # keep caller's copy in sync


async def _record_failure(source: LogSshSource, exc: Exception, *, drive_breaker: bool) -> bool:
    """Stamp a failed attempt: last_attempt_at + last_error. When driven by the poller, increment
    consecutive_failures and — once it reaches ssh_auto_disable_after_failures (breaker, 0=off) on a
    still-enabled source — flip enabled=False + auto_disabled_at (manual-only, so no retry storm and
    no surprise backfill). Returns True if it auto-disabled. Reads the row fresh so repeated calls
    increment correctly."""
    now = datetime.now(timezone.utc)
    auto_disabled = False
    async with async_session() as db:
        row = await db.get(LogSshSource, source.id)
        if row is None:
            return False
        row.last_attempt_at, row.last_error = now, str(exc)[:2000]
        if drive_breaker:
            row.consecutive_failures = (row.consecutive_failures or 0) + 1
            thr = settings.ssh_auto_disable_after_failures
            if thr > 0 and row.consecutive_failures >= thr and row.enabled:
                row.enabled, row.auto_disabled_at = False, now
                row.last_error = (f"Auto-disabled after {row.consecutive_failures} consecutive "
                                  f"failures — re-enable and run a bounded resume. "
                                  f"Last error: {str(exc)[:1500]}")
                auto_disabled = True
        await db.commit()
        source.consecutive_failures = row.consecutive_failures if drive_breaker else source.consecutive_failures
        source.enabled = row.enabled
    return auto_disabled


async def sweep_stale_runs() -> int:
    """Mark any log_ssh_fetch_runs left in `running` (a crash/restart mid-fetch) as failed. Called
    once at startup so a run is never stuck `running` forever. Returns the number swept."""
    async with async_session() as db:
        result = await db.execute(
            update(LogSshFetchRun)
            .where(LogSshFetchRun.status == LogSshFetchRunStatus.running)
            .values(status=LogSshFetchRunStatus.failed, phase=LogSshFetchPhase.done,
                    error="Interrupted by server restart", finished_at=datetime.now(timezone.utc))
        )
        await db.commit()
        return result.rowcount or 0


async def fetch_now(db: AsyncSession, customer_code: str, *, source_id: UUID | None = None,
                    mode: LogSshFetchMode = LogSshFetchMode.incremental,
                    from_ts: datetime | None = None, enabled_only: bool = False,
                    disabled_only: bool = False, skip_if_busy: bool = False,
                    drive_breaker: bool = False, force_remote: bool = False,
                    on_progress: ProgressCb | None = None) -> dict:
    """Pull from the targeted source(s). Stitching is NOT triggered here — see the module docstring.

    Each source is fetched under a per-host advisory lock (_host_lock): `skip_if_busy=True` (the
    poller) skips a host another fetch already holds and records it under agg["skipped"];
    `skip_if_busy=False` (on-demand) waits up to ssh_fetch_lock_wait_seconds. Per-source failures are
    isolated (one unreachable server doesn't abort the others) and recorded on that source's
    last_error; a reachable source updates last_ok_at. The caller's `db` is used only for loading
    sources — it is released (rollback) before the network loop so no pooled connection is held
    across the SFTP transfer. Returns aggregate stats for the pull.

    `on_progress` (the tracked endpoint passes one; the poller doesn't) is called with keyword fields
    — phase / files_considered / progress — at each stage so the run row reflects live progress."""
    sources = await _load_sources(db, customer_code, source_id,
                                  enabled_only=enabled_only, disabled_only=disabled_only)
    agg = {"customer_code": customer_code, "mode": mode.value, "sources": len(sources),
           "files_considered": 0, "files_fetched": 0, "bytes_fetched": 0,
           "entries_ingested": 0, "objects_queued": 0,
           "content_skipped": 0, "io_skipped": 0, "by_source": [], "errors": []}

    async def _regrouping() -> None:
        if on_progress is not None:
            await on_progress(phase=LogSshFetchPhase.regrouping)

    if not sources:
        return agg

    # timestamp mode: if Postgres already covers from_ts, don't touch the servers at all — UNLESS this
    # is an explicit manual request (force_remote), which must always pull from the server (an outage
    # resume: old local data exists, so the coverage check would wrongly suppress it).
    if mode == LogSshFetchMode.timestamp and from_ts is not None and not force_remote:
        local_min = await _local_min_ts(db, customer_code)
        if local_min is not None and from_ts >= local_min:
            agg["already_local"] = True
            return agg

    # Release the caller's pooled connection before the (slow) network loop — _fetch_source and the
    # bookkeeping below use their own short sessions, so nothing is pinned across the SFTP transfer.
    # Detach the loaded `sources` FIRST: rollback() would otherwise expire them, and accessing an
    # expired attribute later (e.g. source.host in _host_lock) would attempt sync IO in the async
    # context (MissingGreenlet). Detached instances keep their already-loaded column values.
    db.expunge_all()
    await db.rollback()

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
            # Backpressure: in queue mode the fetcher no longer waits for the database, so nothing
            # else stops it downloading faster than the parse worker drains. Skip the source for
            # this tick rather than piling more bytes onto a disk that is already struggling.
            if settings.log_parse_worker_enabled and await _queue_backlog_exceeded(customer_code):
                agg.setdefault("skipped", []).append(
                    {"source": source.name,
                     "reason": f"ingest queue backlog exceeds {settings.log_parse_queue_max_pending}"})
                continue
            async with _host_lock(source, skip_if_busy=skip_if_busy) as got:
                if not got:  # poller: another fetch holds this host — skip, pick it up next tick
                    agg.setdefault("skipped", []).append(
                        {"source": source.name, "reason": "another fetch for this host is in progress"})
                    continue
                stats = await _fetch_source(source, mode, from_ts, on_listed=on_listed, on_file=on_file)
            # lock released; record success (+ reset breaker when driven by the poller)
            await _record_success(source, drive_breaker=drive_breaker)
            agg["by_source"].append(stats)
            agg["files_considered"] += stats["files_considered"]
            agg["files_fetched"] += stats["files_fetched"]
            agg["bytes_fetched"] += stats["bytes_fetched"]
            agg["entries_ingested"] += stats["entries_ingested"]
            agg["objects_queued"] += stats.get("objects_queued", 0)
            agg["content_skipped"] += stats.get("content_skipped", 0)
            agg["io_skipped"] += stats.get("io_skipped", 0)
            base["considered"] += stats["files_considered"]
            base["bytes"] += stats["bytes_fetched"]
            base["entries"] += stats["entries_ingested"]
        except Exception as exc:  # isolate one bad server
            logger.exception("SSH fetch failed for source %s/%s", customer_code, source.name)
            auto_disabled = await _record_failure(source, exc, drive_breaker=drive_breaker)
            agg["errors"].append({"source": source.name, "error": str(exc)})
            if auto_disabled:
                agg.setdefault("auto_disabled", []).append(source.name)

    # NOTE: no Stage 2 call here. Stage 1 writes a log_regroup_pending ticket in the same transaction
    # as its entries, and the stitch worker (app/services/workers/log_stitch_worker.py) owns draining
    # that queue. Transport has no business knowing stitching exists — and when every producer had to
    # remember this call, any new ingestion path that forgot it silently left data unstitched.
    await _regrouping()
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
            # Manual/on-demand semantics: always hit the server (force_remote) and, for a "fetch all"
            # (source_id is None), only touch disabled sources — the poller owns the enabled ones.
            stats = await fetch_now(db, customer_code, source_id=source_id, mode=mode,
                                    from_ts=from_ts, disabled_only=(source_id is None),
                                    force_remote=True, on_progress=_cb)
            # The run reports the PULL only. Stitching is now owned by the stitch worker and happens
            # after this run completes, so a stitch failure is no longer this run's outcome — it is
            # visible on log_regroup_pending (attempts / last_error / abandoned_at) and via
            # GET /logs/regroup/status instead.
            values = dict(
                status=LogSshFetchRunStatus.completed,
                phase=LogSshFetchPhase.done,
                files_considered=stats.get("files_considered"),
                files_fetched=stats.get("files_fetched"),
                bytes_fetched=stats.get("bytes_fetched"),
                entries_ingested=stats.get("entries_ingested"),
                error=None,
                result=stats, finished_at=datetime.now(timezone.utc),
            )
        except asyncio.CancelledError:
            # shutdown (or an explicit cancel) — mark the run terminal on a fresh session, then
            # re-raise. The startup sweep is the backstop if this best-effort write can't complete.
            logger.info("Tracked SSH fetch cancelled (run=%s customer=%s)", run_id, customer_code)
            try:
                async with async_session() as db2:
                    # only if still running — never overwrite an operator 'cancelled' terminal state
                    await db2.execute(update(LogSshFetchRun).where(
                        LogSshFetchRun.id == run_id,
                        LogSshFetchRun.status == LogSshFetchRunStatus.running).values(
                        status=LogSshFetchRunStatus.failed, phase=LogSshFetchPhase.done,
                        error="Cancelled by server shutdown", finished_at=datetime.now(timezone.utc)))
                    await db2.commit()
            except Exception:
                logger.warning("could not mark cancelled run %s failed", run_id, exc_info=True)
            raise
        except Exception as exc:
            logger.exception("Tracked SSH fetch failed (run=%s customer=%s)", run_id, customer_code)
            await db.rollback()
            values = dict(status=LogSshFetchRunStatus.failed, phase=LogSshFetchPhase.done,
                          error=str(exc), finished_at=datetime.now(timezone.utc))
        await db.execute(update(LogSshFetchRun).where(LogSshFetchRun.id == run_id).values(**values))
        await db.commit()
