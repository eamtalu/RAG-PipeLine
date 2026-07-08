# Background workers: web / worker process split

This explains how the background loops run in production, why they are separated from the web tier, and how to change them safely so a future edit does not accidentally re-introduce the "N copies" bug.

## The problem this solves

The app runs several background loops: the SSH poll supervisor, Stage 2 stitching (via the poller / watcher), the embedding worker, the log watcher, the notification worker, and log-space cleanup.
These are meant to run in exactly one process.

The web tier runs under `gunicorn -w N -k uvicorn.workers.UvicornWorker`, and gunicorn runs the FastAPI `lifespan` in every worker process.
So with `-w 4`, four copies of every background loop used to start at once: four poll supervisors, four finalizers, four notification workers, all hitting the same database.
That multiplied load and retries and risked duplicate side effects (for example duplicate notifications).
It is not the same thing as the per-customer poller, which is many asyncio tasks inside one process and is correct.

## The design

Background loops live in `app/background.py` behind `start_background_tasks()` / `stop_background_tasks()`, and are started in only one place:

- The web tier (`app/main.py` lifespan) starts them only when `settings.run_background_workers` is true.
- The dedicated worker (`app/worker.py`, run as `python -m app.worker`) always starts them, and guards startup with a singleton advisory lock.

Recommended production topology, two systemd units:

- `fastapirag.service` - `gunicorn app.main:app -w N ...` with `Environment=RUN_BACKGROUND_WORKERS=false`. Serves HTTP and on-demand endpoints only.
- `fastapirag-worker.service` - `python -m app.worker` with `Restart=always`. Runs the loops. Unit file: `deploy/fastapirag-worker.service`.

Default `run_background_workers=True` means single-process and dev setups (`uvicorn main:app`, `gunicorn -w 1`) keep working exactly as before with no env to set.

## Why this is safe and battle-proof

- "Exactly one" is guaranteed by systemd (one worker process, restarted on crash), not by hand-written leader election.
- The singleton advisory lock in `app/worker.py` (`pg_try_advisory_lock(0x7A9B, 1)` on a dedicated connection held for the process lifetime) is a second line of defense: a second worker fails to acquire it and exits, so the loops can never double-run even by operator error. Closing the connection on exit releases the lock, so the next worker takes over.
- Web and worker coordinate purely through Postgres advisory locks that already exist: the per-host SSH fetch lock and the per-customer / per-window finalize lock. So an on-demand fetch or regroup triggered in the web tier never collides with the poller in the worker.
- Notifications are self-contained in the worker: the rule engine reads the database each tick, publishes to an in-process bus that the same-process dispatcher persists to the database outbox, and redelivers from that outbox. Nothing depends on the web process, so the split does not break alerting.
- Resource isolation: heavy Stage 2 stitching runs in the worker and can never slow HTTP request latency, and the web tier scales with `-w N` independently.

## One-time server setup

```bash
# on the Ubuntu box, from the repo root
sudo cp deploy/fastapirag-worker.service /etc/systemd/system/
sudo systemctl daemon-reload

# tell the existing web unit to NOT run the loops
sudo systemctl edit fastapirag.service
#   [Service]
#   Environment=RUN_BACKGROUND_WORKERS=false

sudo systemctl restart fastapirag.service
sudo systemctl enable --now fastapirag-worker.service
```

After this, `deploy.sh` restarts both units on every deploy (it skips the worker restart with a warning if the unit is not installed yet).

## How to verify only one instance runs

```bash
# the poll loop should start once per customer, in the WORKER log only
sudo journalctl -u fastapirag-worker.service | grep "SSH poll loop started"
# the WEB log should say background workers are disabled and show none of the loop start lines
sudo journalctl -u fastapirag.service | grep -E "Background workers disabled|SSH poll loop started"
# exactly one python -m app.worker process
ps -ef | grep 'app.worker' | grep -v grep
```

## How to add or change a background loop without breaking this

- Put any new long-running loop in `start_background_tasks()` in `app/background.py`. Do not start a loop directly in `app/main.py`, in a router, or in a request handler.
- Anything started in `start_background_tasks()` automatically runs in exactly one process (the worker) and is cancelled cleanly on shutdown by `stop_background_tasks()`.
- If a new loop can be triggered both on demand (web) and on a schedule (worker), make its unit of work idempotent and guard shared state with a Postgres advisory lock, matching the SSH fetch and finalize locks. That is what lets web and worker cooperate.
- Do not read `settings.run_background_workers` inside a loop to decide whether to run; that flag only gates where `start_background_tasks()` is called. The worker ignores it on purpose.
- Keep `app/worker.py` free of HTTP concerns; it deliberately does not import the FastAPI app or the router, so it stays light and starts fast.

## Tests

`tests/test_background_workers_chunk10.py` covers:

- the web lifespan starts the loops only when `run_background_workers` is true;
- `start_background_tasks()` assembles exactly the enabled loops and registers the notification dispatcher only when notifications are on;
- the singleton advisory lock is mutually exclusive (a second connection cannot acquire it), which is what stops a second worker from double-running.
