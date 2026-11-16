"""Step 31 parity: dump tmp-live's inputs (the week's settled rows and each window's grouped baseline
reads, read as the sweep reads them) and the Python panels computed from them, for the web page's
own code to run on the same inputs."""
import asyncio, json, sys, time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from app.config.database import async_session
from app.services.analytics import settle_reads
from app.services.teams import releases_detail as rd, releases_stats as rs

CC = "tmp-live"

async def grouped(db, **kw):
    return await rd._grouped(db, CC, **kw)

async def main(out):
    tz = ZoneInfo("Europe/London")
    now = datetime.now(timezone.utc).astimezone(tz)
    async with async_session() as db:
        t0 = time.monotonic()
        week = rs.window_for("week", now, tz)
        raw = await rd._rows(db, CC, week.start, now, [])
        inputs = {}
        for k in rd.WINDOWS:
            w = rs.window_for(k, now, tz)
            start, end = rs.baseline_range_for(w, tz)
            inputs[k] = {
                "hours_all": await grouped(db, group_by=["hour_start"], start=start, end=end, where=[]),
                "hours_zero": await grouped(db, group_by=["hour_start"], start=start, end=end, where=["picked==0"]),
                "hours_over": await grouped(db, group_by=["hour_start"], start=start, end=end, where=["shortfall>0"]),
                "items_all": await grouped(db, group_by=["item_number", rd.ITEM_DESCRIPTION], start=start, end=w.end, where=[], limit=5000),
                "items_zero": await grouped(db, group_by=["item_number"], start=start, end=w.end, where=["picked==0"], limit=5000),
                "per_day": await grouped(db, group_by=["day"], start=start, end=end, where=[], stat=["distinct:delivery_number"], limit=400),
            }
        rows = [rs.to_release(r) for r in raw]
        panels = {}
        for k in rd.WINDOWS:
            i = inputs[k]
            base = {"hours": rs.merge_hour_buckets(i["hours_all"], i["hours_zero"], i["hours_over"]),
                    "items": rs.chronic_items(i["items_all"], i["items_zero"], rd.MIN_ITEM_RELEASES),
                    "deliveries_per_day": [rs._num(r.get("distinct_delivery_number")) for r in i["per_day"]]}
            panels[k] = rd.window_panels(k, rows, base, now, tz)
        print("rows", len(raw), "seconds", round(time.monotonic() - t0, 1))
    json.dump({"now": now.isoformat(), "rows": raw, "inputs": inputs, "python": panels}, open(out, "w"), default=str)

asyncio.run(main(sys.argv[1]))
