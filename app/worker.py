"""Dedicated background-worker entrypoint: `python -m app.worker`.

Runs ALL background loops (poller, Stage 2 stitching, embeddings, watcher, notifications, cleanup) in
ONE process, separate from the `gunicorn -w N` web tier (which runs with RUN_BACKGROUND_WORKERS=false
and serves only HTTP + on-demand endpoints). systemd (`fastapirag-worker.service`, Restart=always)
supplies supervision and restart-on-crash — so "exactly one instance" is an OS guarantee, not
hand-written leader election.

A startup singleton advisory lock is a second line of defense: if a second worker is ever started
(operator error, a stray dev instance on the same DB), it fails to acquire the lock and exits
immediately rather than double-running every loop. The lock is session-scoped and held on a dedicated
connection for the process lifetime; closing that connection (on exit) releases it, so the next
worker can take over. See docs/background-workers-web-worker-split.md.
"""

import asyncio
import logging
import signal

from sqlalchemy import func, select

from app.config.database import engine
from app.background import setup_logging, start_background_tasks, stop_background_tasks

logger = logging.getLogger(__name__)

# Two-int advisory-lock key for the worker singleton. Uses the (classid, objid) keyspace, which is
# disjoint from the SSH per-host fetch lock (classid 0x55AA) and from finalize's single-bigint
# hashtext locks — so it can never collide with any other advisory lock in the app.
_SINGLETON_CLASSID = 0x7A9B
_SINGLETON_OBJID = 1


async def _amain() -> None:
    setup_logging()
    # Hold the singleton lock on a dedicated connection for the whole process lifetime.
    conn = await engine.connect()
    try:
        got = bool(await conn.scalar(
            select(func.pg_try_advisory_lock(_SINGLETON_CLASSID, _SINGLETON_OBJID))))
        if not got:
            logger.error("Another fastapirag worker already holds the singleton lock — exiting so "
                         "the background loops are never double-run.")
            return
        logger.info("Background worker: singleton lock acquired — starting loops")
        tasks = await start_background_tasks()

        # Wait for a shutdown signal, then stop the loops cleanly.
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # e.g. non-main thread / platform without signal handlers
                pass
        await stop.wait()
        logger.info("Background worker: shutdown signal received — stopping loops")
        await stop_background_tasks(tasks)
    finally:
        # closing the connection releases the session-scoped singleton lock; dispose the pool too.
        await conn.close()
        await engine.dispose()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
