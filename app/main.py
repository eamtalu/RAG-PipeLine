import asyncio
import logging

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.settings import settings
from app.services.workers.embedding_worker import run_worker
from app.services.workers.log_watcher import run_log_watcher
from app.services.workers.log_grouping_worker import run_log_grouping_worker
from app.services.workers.ssh_log_fetcher import run_ssh_log_fetcher
from app.services.mnp_log_ingestion.remote.remote_fetcher import sweep_stale_runs
from app.api.v1.log_sources import _fetch_tasks as _ssh_fetch_tasks
from app.services.workers.notification_worker import run_notification_worker
from app.services.workers.logspace_cleanup_worker import run_logspace_cleanup_worker
from app.services.notifications import dispatcher as notification_dispatcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

# If you want to
# - create instances that would be available application wide or
# - any background service that would be running applicationwide
# you need to do it with decorator @asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Fail any SSH fetch run left `running` by a prior crash/restart (unconditional — on-demand runs
    # exist even when the poller is disabled). See remote_fetcher.sweep_stale_runs.
    try:
        swept = await sweep_stale_runs()
        if swept:
            logging.getLogger(__name__).info("Swept %d stale SSH fetch run(s) to failed", swept)
    except Exception:
        logging.getLogger(__name__).warning("stale-run sweep failed at startup", exc_info=True)
    # Start background workers: embedding worker + log watcher + log grouping (Stage 2)
    tasks = [
        asyncio.create_task(run_worker()),
        asyncio.create_task(run_log_watcher()),
    ]
    # Stage 2 automatic incremental regroup — togglable so it can be run manually via the API instead.
    if settings.log_grouping_worker_enabled:
        tasks.append(asyncio.create_task(run_log_grouping_worker()))
    else:
        logging.getLogger(__name__).info("Log grouping worker disabled (log_grouping_worker_enabled=False)")
    # Remote SSH log fetcher: the per-customer poll supervisor. ON by default and idle until a source
    # is enabled from the frontend (ssh_log_fetcher_enabled is only a global kill-switch).
    if settings.ssh_log_fetcher_enabled:
        tasks.append(asyncio.create_task(run_ssh_log_fetcher()))
    else:
        logging.getLogger(__name__).info("SSH log fetcher globally disabled (kill-switch)")
    # Notifications (rules → bus → channels). Subscribe the dispatcher to the bus once, then run the
    # worker that drives rule evaluation + store-and-forward redelivery. Off unless enabled.
    if settings.notifications_enabled:
        notification_dispatcher.register()
        tasks.append(asyncio.create_task(run_notification_worker()))
    else:
        logging.getLogger(__name__).info("Notifications disabled (notifications_enabled=False)")
    # Log-space cleanup: auto-expire disposables + sweep stale presence. Off unless explicitly enabled;
    # DELETE /customers/{code} performs the same purge on demand regardless of this flag.
    if settings.logspace_cleanup_worker_enabled:
        tasks.append(asyncio.create_task(run_logspace_cleanup_worker()))
    else:
        logging.getLogger(__name__).info("Log-space cleanup worker disabled "
                                         "(logspace_cleanup_worker_enabled=False)")
    yield
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    # cancel in-flight on-demand SSH fetch tasks too (they aren't part of `tasks`); each marks its
    # run failed on CancelledError, and the next startup sweep is the backstop.
    for t in list(_ssh_fetch_tasks.values()):
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(api_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
