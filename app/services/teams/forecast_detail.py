"""Step 30 (chunk 208): the Teams Forecast page's row, mirrored every five minutes per site.

The web app's Forecast page (`matrix-log-explorer`, `/matrix/forecast`) reads `/analytics/forecast/*`.
The Teams tab cannot reach this server, so the consumer calls the very functions behind those routes
and mirrors what they return into one `site_forecast` row per site and for the whole customer
("all"): the two pages are built from one source and can never disagree. Each point keeps only the
fields the charts read, and `fit` drops the least needed panels if a row would not fit an item.

Every forecast series exists per warehouse, so a site's row reads its own series; pickers per hour,
the shifts, the transactions, the items and the accuracy exist for the whole customer only, and the
row names them in `all_sites_panels` so the page labels them "all sites" rather than the site's.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1 import analytics_forecast as web
from app.services.analytics_forecast import run_store
from app.services.teams.home_forecast import NO_RUN_NOTE
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed
from app.services.teams.sites import ALL_SITES_ID, Site, list_sites

logger = logging.getLogger(__name__)

ROW_TYPE = "site_forecast"
#: The windows of the web page (ForecastPage.tsx BACK and HOURS_BACK), so the charts span the same buckets.
DAYS_BACK, WEEKS_BACK, MONTHS_BACK, HOURS_BACK = 28, 12, 6, 72
METRICS = ("lines", "units")
GRAINS = ("day", "week", "month")
#: The items the page lists, biggest first; the page sorts and searches within them.
ITEMS_LIMIT = 100
#: The fields of a point the charts read (matrix-log-explorer `toFcPoints`); the rest stay home.
POINT_FIELDS = ("target", "actual", "actual_to_date", "partial", "p10", "p50", "p90", "horizon", "model", "scored")
#: Below DynamoDB's 400 KB item limit, with room for the key and the envelope.
MAX_ROW_BYTES = 350_000
#: What `fit` gives up first when a row is too big, and what it leaves in its place.
TRIM_ORDER: tuple[tuple[str, Any], ...] = (("items", []), ("by_transaction", {}))
#: The panels read for the whole customer even on a site's row.
ALL_SITES_PANELS = ["accuracy", "hourly_pickers", "heatmap", "by_transaction", "items"]


def slim(point: dict) -> dict:
    """One chart point with only the fields the page reads.

    >>> slim({"target": "2026-10-05", "start": "2026-10-05", "actual": "60", "actual_to_date": None, "partial": False,
    ...       "p10": None, "p50": None, "p90": None, "horizon": None, "model": None, "scored": False, "predicted_at": None})["actual"]
    '60'
    >>> sorted(slim({k: None for k in POINT_FIELDS + ("start", "end")})) == sorted(POINT_FIELDS)
    True
    """
    return {k: point.get(k) for k in POINT_FIELDS}


def _size(row: dict) -> int:
    return len(json.dumps(row, default=str).encode())


def fit(row: dict, *, max_bytes: int = MAX_ROW_BYTES) -> dict:
    """The row as it is when it fits, else without the items, then without the transactions, saying
    what it left out, so a busy customer's row is still written rather than refused by DynamoDB.

    >>> fit({"items": [1]}, max_bytes=100)
    {'items': [1]}
    >>> fit({"items": ["x" * 200]}, max_bytes=100)
    {'items': [], 'trimmed': ['items']}
    """
    if _size(row) <= max_bytes:
        return row
    out, trimmed = dict(row), []
    for key, empty in TRIM_ORDER:
        if key in out and out[key]:
            out[key] = empty
            trimmed.append(key)
            if _size({**out, "trimmed": trimmed}) <= max_bytes:
                break
    out["trimmed"] = trimmed
    return out


async def _series(db: AsyncSession, cc: str, metric: str, kind: str, subject: str) -> dict:
    return await web.read_series(metric=metric, subject_kind=kind, subject=subject, grains=",".join(GRAINS),
                                 days_back=DAYS_BACK, weeks_back=WEEKS_BACK, months_back=MONTHS_BACK,
                                 hours_back=HOURS_BACK, horizon="latest", customer=cc, db=db)


def _grains(series: dict) -> dict[str, list[dict]]:
    return {g: [slim(p) for p in series["grains"][g]["points"]] for g in GRAINS}


async def compute(db: AsyncSession, customer_code: str, site: Site | None = None, *,
                  now: datetime | None = None) -> dict:
    """The Forecast page's row for the whole customer or one site, or the "no run yet" row.

    (customer with a run) -> {"available": True, "series": {...}, "heatmap": {...}, ...} ;
    (Site BRI) -> its warehouse's series, the rest for all sites ; (no run) -> {"available": False, ...}
    """
    # Step 1: Say plainly that there is nothing yet, because a page of empty charts reads as zero demand
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    if await run_store.latest_completed(db, customer_code) is None:
        return {"available": False, "note": NO_RUN_NOTE, "as_of": stamp,
                "site_id": site.site_id if site else ALL_SITES_ID, "scope": site.name if site else "all sites"}
    kind, subject = ("warehouse", site.site_id) if site is not None else ("total", "total")
    # Step 2: The series of the page's main chart and tiles, for the site when one is asked
    main = {m: await _series(db, customer_code, m, kind, subject) for m in METRICS}
    lines = main["lines"]
    history = lines["grains"]["day"]
    # Step 3: The panels that exist for the whole customer, read as the web page reads them
    hourly = await web.read_series(metric="pickers", subject_kind="total", subject="total", grains="hour",
                                   days_back=DAYS_BACK, weeks_back=WEEKS_BACK, months_back=MONTHS_BACK,
                                   hours_back=HOURS_BACK, horizon="latest", customer=customer_code, db=db)
    accuracy = await web.read_accuracy(metric="lines", subject_kind="total", subject="total", grain="day",
                                       customer=customer_code, db=db)
    heatmap = await web.read_heatmap(week="next", customer=customer_code, db=db)
    status = await web.read_status(customer=customer_code, db=db)
    items = await web.list_items(metric="units", horizon="1w", sort="volume", limit=ITEMS_LIMIT, after=None, q=None,
                                 customer=customer_code, db=db)
    by_transaction: dict[str, list[dict]] = {}
    for metric in METRICS:
        subjects = await web.list_subjects(subject_kind="transaction_name", metric=metric, limit=web.ITEMS_MAX,
                                           customer=customer_code, db=db)
        by_transaction[metric] = [
            {"key": s["subject"], "series": _grains(await _series(db, customer_code, metric, "transaction_name", s["subject"]))}
            for s in subjects["subjects"]]
    # Step 4: One row, trimmed only if it would not fit, so DynamoDB never refuses the write
    latest = status.get("latest_run") or {}
    row = {
        "available": True, "as_of": stamp,
        "site_id": site.site_id if site else ALL_SITES_ID, "scope": site.name if site else "all sites",
        "all_sites_panels": ALL_SITES_PANELS if site else [],
        "timezone": lines["timezone"], "today": lines["today"], "as_of_date": lines["as_of_date"],
        "history_from": history.get("history_from"), "steady_from": history.get("steady_from"),
        "latest_run": lines["latest_run"],
        "status": {"model_version": status["model_version"], "run_hour_local": status["run_hour_local"],
                   "worker_enabled": status["worker_enabled"],
                   "latest_run": {k: latest.get(k) for k in ("status", "as_of_date", "finished_at", "model_version", "error")}
                   if latest else None},
        "series": {m: _grains(main[m]) for m in METRICS},
        "hourly_pickers": [slim(p) for p in hourly["grains"]["hour"]["points"]],
        "accuracy": accuracy, "heatmap": heatmap, "items": items["items"], "by_transaction": by_transaction,
    }
    return fit(row)


async def sweep_once(mirror: MirrorWriter, *, only: list[str] | None = None, tracker: ChangeTracker | None = None,
                     max_sites: int = 3, session_factory=None) -> list[str]:
    """Mirror the Forecast page's rows (the whole customer and each active site, at most `max_sites`)
    for every bound, enabled customer, or `only`; the codes written. One failure skips that customer.

    (acme with sites BRI, LON) -> SITE#all#FORECAST, SITE#BRI#FORECAST, SITE#LON#FORECAST ; (mirror down) -> []
    """
    from app.config.database import async_session
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    factory = session_factory or async_session
    written: list[str] = []
    async with factory() as db:
        codes = list(only) if only is not None else sorted(
            {b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        for code in codes:
            try:
                # Step 1: The whole customer first, then each site in switcher order, capped because
                # every site costs a full set of forecast reads
                targets: list[Site | None] = [None, *(await list_sites(db, code))[:max_sites]]
                for site in targets:
                    row = await compute(db, code, site)
                    stamp = datetime.now(timezone.utc).timestamp()
                    env = envelope(ROW_TYPE, code, site_id=row["site_id"], payload=row, as_of=row["as_of"], now=stamp)
                    await write_if_changed(mirror, tracker, env, now=stamp)
                written.append(code)
            except Exception:
                logger.exception("teams.forecast_detail_failed customer=%s", code)
                await db.rollback()
    return written
