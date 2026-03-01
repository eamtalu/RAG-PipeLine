import asyncio
import logging

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.settings import settings
from app.services.workers.embedding_worker import run_worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

# If you want to
# - create instances that would be available application wide or
# - any background service that would be running applicationwide
# you need to do it with decorator @asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the embedding worker as a background task
    worker_task = asyncio.create_task(run_worker())
    yield
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(api_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
