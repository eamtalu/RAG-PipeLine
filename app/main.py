import asyncio
import logging

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.settings import settings
from app.background import setup_logging, start_background_tasks, stop_background_tasks
from app.api.v1.log_sources import _fetch_tasks as _ssh_fetch_tasks
from app.middleware.idempotency import IdempotencyMiddleware

# `setup_logging` is re-exported here so `app.main.setup_logging` keeps working (tests + habit).
__all__ = ["app", "setup_logging", "lifespan"]

setup_logging()

# If you want to
# - create instances that would be available application wide or
# - any background service that would be running applicationwide
# you need to do it with decorator @asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Background loops run in EXACTLY ONE process. When run_background_workers is false (the web tier
    # under gunicorn -w N), this process serves HTTP + on-demand endpoints only, and the dedicated
    # `app.worker` process runs the loops. Default true keeps single-process / dev deployments working
    # exactly as before. See app.background and docs/background-workers-web-worker-split.md.
    if settings.run_background_workers:
        tasks = await start_background_tasks()
    else:
        tasks = []
        logging.getLogger(__name__).info(
            "Background workers disabled in this process (run_background_workers=False) — serving "
            "HTTP + on-demand endpoints only; the dedicated worker process runs the loops")
    yield
    await stop_background_tasks(tasks)
    # cancel in-flight on-demand SSH fetch tasks too (they aren't part of `tasks`); each marks its
    # run failed on CancelledError, and the next startup sweep is the backstop.
    for t in list(_ssh_fetch_tasks.values()):
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass


app = FastAPI(title=settings.app_name, lifespan=lifespan)
# Idempotency-Key de-duplication for allowlisted mutating POSTs. Opt-in (no-op unless the request
# carries an Idempotency-Key on an allowlisted path), so it never affects reads/uploads/other routes.
app.add_middleware(IdempotencyMiddleware)
app.include_router(api_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
