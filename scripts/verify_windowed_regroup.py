"""Verify scoped/windowed Stage 2 regroup is LOSSLESS and SCOPED, on real data.

Run:  PYTHONPATH="$PWD" python scripts/verify_windowed_regroup.py [customer_code]

Test A (equivalence): a windowed regroup covering the WHOLE span must reproduce regroup_all exactly
                      (identical transaction id set + identical entry->transaction assignment).
Test B (scoped+safe): a single-hour windowed regroup must (1) leave every out-of-window transaction
                      byte-identical (untouched created_at), (2) preserve the entry->transaction map
                      for ALL entries (no split, no strand), (3) keep the transaction count.
Also reports the max real transaction span vs the pad (the invariant the proof rests on).
Restores canonical state (regroup_all) at the end.
"""

import asyncio
import sys
from datetime import timedelta

from sqlalchemy import text, select, func

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline.derive_transactions import (
    regroup_all, regroup_window, _regroup_pad,
)


async def _entry_map(db, code) -> dict:
    """entry_id -> transaction_id (str or None) for the customer."""
    rows = (await db.execute(
        select(LogEntry.id, LogEntry.transaction_id).where(LogEntry.customer_code == code)
    )).all()
    return {str(e): (str(t) if t else None) for e, t in rows}


async def _txn_meta(db, code) -> dict:
    """transaction_id -> (started_at, ended_at, status, sealed, entry_count, created_at)."""
    rows = (await db.execute(
        select(LogTransaction.id, LogTransaction.started_at, LogTransaction.ended_at,
               LogTransaction.status, LogTransaction.sealed, LogTransaction.entry_count,
               LogTransaction.created_at).where(LogTransaction.customer_code == code)
    )).all()
    return {str(r[0]): r[1:] for r in rows}


def _assigned(m: dict) -> dict:
    return {k: v for k, v in m.items() if v is not None}


async def main(code: str) -> None:
    pad = _regroup_pad()
    print(f"customer={code}  pad={pad}  (seal_window={settings.log_seal_window_seconds}s)\n")

    async with async_session() as db:
        lo, hi = (await db.execute(
            select(func.min(LogEntry.timestamp), func.max(LogEntry.timestamp))
            .where(LogEntry.customer_code == code)
        )).one()
    if lo is None:
        print("no timestamped entries — nothing to verify"); return
    print(f"span: {lo} .. {hi}")

    # ---- canonical baseline via full regroup ----
    async with async_session() as db:
        s_all = await regroup_all(db, code)
    async with async_session() as db:
        map_all = await _entry_map(db, code)
        meta_all = await _txn_meta(db, code)
    n_txn_all = len(meta_all)
    n_assigned_all = len(_assigned(map_all))
    print(f"\n[baseline regroup_all] txns={n_txn_all} entries_assigned={n_assigned_all} "
          f"orphans={len(map_all)-n_assigned_all}")

    # max real transaction span — the invariant the pad must dominate
    async with async_session() as db:
        max_span_s = await db.scalar(text(
            "select max(extract(epoch from (ended_at - started_at))) from log_transactions "
            "where customer_code=:c and ended_at is not null and started_at is not null"), {"c": code})
    max_span = timedelta(seconds=float(max_span_s or 0))
    print(f"max transaction span in data = {max_span}  | pad = {pad}  -> "
          f"{'OK (pad dominates)' if max_span <= pad else 'WARNING: span exceeds pad!'}")

    # ================= TEST A: full-span window == full regroup =================
    async with async_session() as db:
        s_win = await regroup_window(db, code, lo, hi)
    async with async_session() as db:
        map_win = await _entry_map(db, code)
        meta_win = await _txn_meta(db, code)

    a_ids = set(meta_all) == set(meta_win)
    a_map = map_all == map_win
    print("\n=== TEST A: full-span regroup_window vs regroup_all ===")
    print(f"  same transaction-id set: {a_ids} ({len(meta_all)} vs {len(meta_win)})")
    print(f"  identical entry->txn map: {a_map}")
    if not a_map:
        diff = [k for k in map_all if map_all[k] != map_win.get(k)]
        print(f"    !! {len(diff)} entries differ, e.g. {diff[:3]}")
    test_a = a_ids and a_map

    # ================= TEST B: single mid-stream hour, scoped =================
    # rebuild canonical baseline first (Test A left full-window state; reset to be sure)
    async with async_session() as db:
        await regroup_all(db, code)
    async with async_session() as db:
        base_map = await _entry_map(db, code)
        base_meta = await _txn_meta(db, code)

    # pick the busiest hour as the target window
    async with async_session() as db:
        h = await db.scalar(text(
            "select date_trunc('hour', started_at) from log_transactions where customer_code=:c "
            "and started_at is not null group by 1 order by count(*) desc limit 1"), {"c": code})
    h_end = h + timedelta(hours=1) - timedelta(milliseconds=1)
    print(f"\n=== TEST B: scoped regroup_window over [{h}, {h_end}] ===")

    # transactions that should be UNTOUCHED: started outside the padded delete window [h-pad, h_end]
    del_lo = h - pad
    untouched_before = {tid: m for tid, m in base_meta.items()
                        if m[0] is not None and (m[0] < del_lo or m[0] > h_end)}

    async with async_session() as db:
        s_b = await regroup_window(db, code, h, h_end)
    async with async_session() as db:
        after_map = await _entry_map(db, code)
        after_meta = await _txn_meta(db, code)

    # (1) out-of-window transactions byte-identical (same created_at => not rebuilt)
    untouched_ok = all(tid in after_meta and after_meta[tid][5] == m[5]
                       for tid, m in untouched_before.items())
    # (2) entry->txn map identical to baseline for every entry (no split, no strand)
    map_ok = base_map == after_map
    # (3) transaction count preserved
    count_ok = len(base_meta) == len(after_meta)
    # window actually did something
    did_work = bool(s_b.get("entries_scanned"))
    print(f"  out-of-window txns untouched: {untouched_ok} ({len(untouched_before)} checked)")
    print(f"  entry->txn map preserved for ALL entries: {map_ok}")
    print(f"  transaction count preserved: {count_ok} ({len(base_meta)} vs {len(after_meta)})")
    print(f"  window scanned entries: {s_b.get('entries_scanned')} created={s_b.get('transactions_created')}")
    if not map_ok:
        diff = [k for k in base_map if base_map[k] != after_map.get(k)]
        print(f"    !! {len(diff)} entries differ, e.g. {diff[:3]}")
    test_b = untouched_ok and map_ok and count_ok and did_work

    # ---- restore canonical state ----
    async with async_session() as db:
        await regroup_all(db, code)

    print("\n================ RESULT ================")
    print(f"  TEST A (lossless equivalence): {'PASS' if test_a else 'FAIL'}")
    print(f"  TEST B (scoped + no split/strand): {'PASS' if test_b else 'FAIL'}")
    print("  ALL PASS" if (test_a and test_b) else "  *** FAILURE ***")
    sys.exit(0 if (test_a and test_b) else 1)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "mnp"))
