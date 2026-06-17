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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

# If you want to
# - create instances that would be available application wide or
# - any background service that would be running applicationwide
# you need to do it with decorator @asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
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
    # Remote SSH log fetcher (pull from the Windows Server) — off unless explicitly enabled.
    if settings.ssh_log_fetcher_enabled:
        tasks.append(asyncio.create_task(run_ssh_log_fetcher()))
    else:
        logging.getLogger(__name__).info("SSH log fetcher disabled (ssh_log_fetcher_enabled=False)")
    yield
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(api_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
