"""Background-service wiring shared by the web tier and the dedicated worker process.

The application's background loops (embedding worker, log watcher, Stage 2 grouping, the SSH poll
supervisor, notifications, log-space cleanup) are meant to run in EXACTLY ONE process. Under
`gunicorn -w N` the FastAPI lifespan runs in every worker, which would start N copies of each loop.
So the loops live here, behind `start_background_tasks()`, and are started in only one place:

  - the web tier (`app.main`) starts them ONLY when settings.run_background_workers is true;
  - the dedicated worker entrypoint (`app.worker`) always starts them, guarded by a singleton
    advisory lock so even an accidental second worker can never double-run them.

Recommended production topology: `fastapirag.service` (gunicorn -w N, RUN_BACKGROUND_WORKERS=false)
for HTTP + on-demand endpoints, and `fastapirag-worker.service` (python -m app.worker) for the loops.
Web and worker coordinate purely through Postgres advisory locks (per-host fetch lock, per-customer /
per-window finalize lock), so on-demand fetches/regroups in the web tier never collide with the
poller. See docs/background-workers-web-worker-split.md.
"""

import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler

from app.settings import settings
from app.services.workers.embedding_worker import run_worker
from app.services.workers.log_watcher import run_log_watcher
from app.services.workers.log_grouping_worker import run_log_grouping_worker
from app.services.workers.ssh_log_fetcher import run_ssh_log_fetcher
from app.services.mnp_log_ingestion.remote.remote_fetcher import sweep_stale_runs
from app.services.workers.notification_worker import run_notification_worker
from app.services.workers.logspace_cleanup_worker import run_logspace_cleanup_worker
from app.services.notifications import dispatcher as notification_dispatcher

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure root logging: always to stdout/stderr, and ALSO to a rotating file when
    settings.log_file is set. Env-gated so bare `uvicorn main:app` (stdout not captured durably) can
    still keep a real log file. Idempotent — safe if called more than once."""
    fmt = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    root = logging.getLogger()
    root.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))

    # stream handler (stdout/stderr) — add one if none exists yet
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler)
               for h in root.handlers):
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)

    # optional rotating file handler
    if settings.log_file:
        path = os.path.expanduser(settings.log_file)
        already = any(isinstance(h, RotatingFileHandler)
                      and getattr(h, "baseFilename", None) == os.path.abspath(path)
                      for h in root.handlers)
        if not already:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            fh = RotatingFileHandler(path, maxBytes=settings.log_file_max_bytes,
                                     backupCount=settings.log_file_backup_count)
            fh.setFormatter(fmt)
            root.addHandler(fh)
            logging.getLogger(__name__).info("File logging enabled -> %s (maxBytes=%d, backups=%d)",
                                             path, settings.log_file_max_bytes,
                                             settings.log_file_backup_count)


async def start_background_tasks() -> list[asyncio.Task]:
    """Start every background loop the deployment enables and return the created tasks. Runs the
    startup stale-run sweep once. Called in exactly one process (see module docstring)."""
    # Fail any SSH fetch run left `running` by a prior crash/restart. See remote_fetcher.sweep_stale_runs.
    try:
        swept = await sweep_stale_runs()
        if swept:
            logger.info("Swept %d stale SSH fetch run(s) to failed", swept)
    except Exception:
        logger.warning("stale-run sweep failed at startup", exc_info=True)

    # Always-on loops: embedding worker + log watcher.
    tasks = [
        asyncio.create_task(run_worker()),
        asyncio.create_task(run_log_watcher()),
    ]
    # Stage 2 automatic incremental regroup — togglable so it can be run manually via the API instead.
    if settings.log_grouping_worker_enabled:
        tasks.append(asyncio.create_task(run_log_grouping_worker()))
    else:
        logger.info("Log grouping worker disabled (log_grouping_worker_enabled=False)")
    # Remote SSH log fetcher: the per-customer poll supervisor. ON by default and idle until a source
    # is enabled from the frontend (ssh_log_fetcher_enabled is only a global kill-switch).
    if settings.ssh_log_fetcher_enabled:
        tasks.append(asyncio.create_task(run_ssh_log_fetcher()))
    else:
        logger.info("SSH log fetcher globally disabled (kill-switch)")
    # Notifications (rules → bus → channels). Subscribe the dispatcher to the bus once, then run the
    # worker that drives rule evaluation + store-and-forward redelivery. Off unless enabled.
    if settings.notifications_enabled:
        notification_dispatcher.register()
        tasks.append(asyncio.create_task(run_notification_worker()))
    else:
        logger.info("Notifications disabled (notifications_enabled=False)")
    # Log-space cleanup: auto-expire disposables + sweep stale presence. Off unless explicitly enabled;
    # DELETE /customers/{code} performs the same purge on demand regardless of this flag.
    if settings.logspace_cleanup_worker_enabled:
        tasks.append(asyncio.create_task(run_logspace_cleanup_worker()))
    else:
        logger.info("Log-space cleanup worker disabled (logspace_cleanup_worker_enabled=False)")
    return tasks


async def stop_background_tasks(tasks: list[asyncio.Task]) -> None:
    """Cancel and await every background task. Swallows the resulting CancelledError (and any error a
    task raises while unwinding) so shutdown always completes cleanly."""
    for task in tasks:
        task.cancel()
    for task in tasks:
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
