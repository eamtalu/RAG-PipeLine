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
from app.services.workers.log_stitch_worker import run_log_stitch_worker, pending_backlog
from app.services.workers.analytics_worker import run_analytics_worker
from app.services.workers.analytics_reconcile_worker import run_analytics_reconcile_worker
from app.services.workers.ssh_log_fetcher import run_ssh_log_fetcher
from app.services.workers.log_parse_worker import run_log_parse_worker, unfinished_ingest_objects
from app.services.workers.log_partition_worker import run_log_partition_worker
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
    # Stage 2 stitch worker — the CONSUMER of the log_regroup_pending queue. Producers (the SFTP
    # transport, the watcher, the parse worker) now only write tickets; this is what drains them.
    # ON by default, because nothing else calls Stage 2 any more: turning it off would leave ingested
    # entries unstitched. It also starts when open windows exist, so a rollback cannot strand them.
    stitch_backlog = 0
    if not settings.log_stitch_worker_enabled:
        try:
            stitch_backlog = await pending_backlog()
        except Exception:
            logger.warning("could not check the stitch backlog at startup", exc_info=True)
    if settings.log_stitch_worker_enabled or stitch_backlog:
        if stitch_backlog:
            logger.warning(
                "Stitch worker starting despite log_stitch_worker_enabled=False: %d open window(s) "
                "remain and nothing else drains them.", stitch_backlog)
        tasks.append(asyncio.create_task(run_log_stitch_worker()))
    else:
        logger.info("Log stitch worker disabled (log_stitch_worker_enabled=False, queue empty)")
    # Analytics worker (N3): drains analytics_pending_windows into analytics_facts.
    #
    # STRICTLY flag-gated, and deliberately unlike the stitch and parse workers above, which start on
    # a backlog even when disabled. That auto-start exists because for them a queue nobody drains means
    # LOST DATA: the parse worker's checkpoint has already advanced past those bytes, and unstitched
    # entries are never revisited. Neither is true here. An unconsumed analytics ticket loses nothing —
    # log_transactions still holds the truth and the ticket stays open — so the failure mode is a stale
    # chart, not missing data. Auto-starting a consumer that writes to nine tables on the strength of a
    # queue depth would take that decision away from the operator for no safety gain.
    if settings.analytics_worker_enabled:
        tasks.append(asyncio.create_task(run_analytics_worker()))
    # The auditor, gated separately from the folder: it must be possible to run the platform
    # without the audit, and to run the audit alone while investigating.
    if settings.analytics_reconcile_worker_enabled:
        tasks.append(asyncio.create_task(run_analytics_reconcile_worker()))
    else:
        logger.info("Analytics worker disabled (analytics_worker_enabled=False); tickets accumulate "
                    "on analytics_pending_windows and are folded whenever it is switched on")
    # Remote SSH log fetcher: the per-customer poll supervisor. ON by default and idle until a source
    # is enabled from the frontend (ssh_log_fetcher_enabled is only a global kill-switch).
    if settings.ssh_log_fetcher_enabled:
        tasks.append(asyncio.create_task(run_ssh_log_fetcher()))
    else:
        logger.info("SSH log fetcher globally disabled (kill-switch)")
    # Ingest-queue parse worker: drains log_source_objects (bytes the fetcher downloaded but did not
    # parse). Gated by the SAME flag the fetcher checks, so the two halves of the decoupling can
    # never be half-enabled.
    #
    # It ALSO starts when the flag is off but unfinished rows exist. That is rollback safety: once a
    # row is queued the checkpoint has already advanced past those bytes, so if the flag were turned
    # back off and nothing drained them, they would never be parsed and never re-downloaded — silent
    # data loss caused purely by a rollback. With the flag off and an empty queue (the normal
    # untouched deployment) no loop is started at all.
    leftover = 0
    if not settings.log_parse_worker_enabled:
        try:
            leftover = await unfinished_ingest_objects()
        except Exception:
            logger.warning("could not check the ingest queue backlog at startup", exc_info=True)
    if settings.log_parse_worker_enabled or leftover:
        if leftover:
            logger.warning(
                "Log parse worker starting despite log_parse_worker_enabled=False: %d unfinished "
                "ingest object(s) remain. Their checkpoints have already advanced, so they must be "
                "drained or those bytes are lost. The worker exits this role once the queue empties.",
                leftover)
        tasks.append(asyncio.create_task(run_log_parse_worker()))
    else:
        logger.info("Log parse worker disabled (log_parse_worker_enabled=False, queue empty) — the "
                    "SSH fetcher parses inline, as before")
    # Partition management: keeps daily partitions provisioned ahead of ingestion and drops them past
    # retention. Deliberately NOT gated on there being work to do — unlike the queue workers, this one
    # has nothing durable to fall back on. An insert into a day with no partition fails outright, so
    # if it does not run, ingestion stops the moment the existing runway is exhausted. Turning it off
    # is therefore a decision to provision partitions by hand, and the log says so.
    if settings.log_partition_worker_enabled:
        tasks.append(asyncio.create_task(run_log_partition_worker()))
    else:
        logger.warning("Log partition worker disabled (log_partition_worker_enabled=False) — daily "
                       "partitions must now be created MANUALLY. Ingestion stops when the existing "
                       "runway runs out.")
    # Notifications (rules → bus → channels). Subscribe the dispatcher to the bus once, then run the
    # worker that drives rule evaluation + store-and-forward redelivery.
    #
    # Started UNCONDITIONALLY. Whether anything actually happens is decided per tenant, every tick,
    # by `customers.notifications_enabled` — so the switch lives in the product and takes effect
    # within one poll interval instead of needing a worker restart.
    #
    # This used to be gated on a deployment-wide `settings.notifications_enabled`, which made a UI
    # toggle impossible rather than merely inconvenient: the flag decided whether the task was ever
    # CREATED, so flipping it at runtime had nothing to observe it. A tick with no enabled tenant is
    # a couple of cheap queries that return nothing.
    notification_dispatcher.register()
    tasks.append(asyncio.create_task(run_notification_worker()))
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
