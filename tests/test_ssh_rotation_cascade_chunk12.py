"""Chunk 12: rename-cascade rotation must not re-download already-ingested content.

Root cause fixed (see docs/ssh-rename-cascade-reread-fix.md): the remote rotates logs by renaming a
whole chain (app.txt -> app.txt.1 -> app.txt.2 ...). The poller checkpoints by PATH, so after each
rotation every path holds different content and the old byte checkpoint no longer matches — the poller
re-downloaded the entire chain (~100 files, ~500 MB) every rotation. Bytes deduped to ~0 new entries,
but the re-transfer held the per-host lock for ~20 min, froze the tenant's poll loop, spiked CPU, and
flapped the UI to "needs attention".

Fix: a content-identity index (head_fingerprint, size, mtime) built from the pre-poll checkpoint
snapshot lets a file whose bytes we already fully ingested — now at a new path — be recognised and
skipped instead of re-downloaded.

Covered:
- _plan_incremental (pure) across every branch, including the new content-skip and its safety guards;
- a full rename cascade: rotated files are skipped, only genuinely-new content is pulled, no data loss;
- a reused path with DIFFERENT content of identical size+mtime is NOT skipped (fingerprint guard).
"""

from app.settings import settings
from app.persistence.models.log_ssh_fetch_run import LogSshFetchMode
from app.services.mnp_log_ingestion.remote import remote_fetcher

from tests.test_ssh_hardening_chunk2 import _patch_sftp, _patch_ingest_counts_lines

N = settings.ssh_fingerprint_bytes  # 4096
BIG = N + 100                       # >= N so the head fingerprint is a reliable content identity


# =============================================================== pure planner (no I/O)
def test_plan_incremental_all_branches():
    plan = remote_fetcher._plan_incremental

    # unchanged at path (fp present) -> skip, no re-save
    p = plan((BIG, 1000.0, BIG, "fpA"), {}, BIG, 1000.0, "fpA", N)
    assert p.do_pull is False and p.save_offset is None and p.reason == "unchanged"

    # unchanged but legacy NULL fingerprint -> skip + backfill at the stored offset
    p = plan((BIG, 1000.0, BIG, None), {}, BIG, 1000.0, "fpX", N)
    assert p.do_pull is False and p.save_offset == BIG and p.reason == "unchanged"

    # content already ingested, now at a NEW path (cascade) -> skip + mark consumed
    sig = {("fpA", BIG, 1000.0): BIG}
    p = plan(None, sig, BIG, 1000.0, "fpA", N)
    assert p.do_pull is False and p.save_offset == BIG and p.reason == "rotated-content-skip"

    # same fp+size but DIFFERENT mtime -> NOT skipped (safe full read); no false skip
    p = plan(None, sig, BIG, 9999.0, "fpA", N)
    assert p.do_pull is True and p.start == 0

    # same fp+mtime but DIFFERENT size -> NOT skipped
    p = plan(None, sig, BIG + 1, 1000.0, "fpA", N)
    assert p.do_pull is True

    # small file (< N) never content-skips even with a matching signature
    p = plan(None, {("fpS", 10, 1.0): 10}, 10, 1.0, "fpS", N)
    assert p.do_pull is True and p.reason == "new-file"

    # append (grown, same fp) -> tail from last offset
    p = plan((BIG, 1000.0, BIG, "fpG"), {}, BIG + 50, 1001.0, "fpG", N)
    assert p.do_pull is True and p.start == BIG and p.reason == "append"

    # rotation at same path (fp changed) -> re-read whole
    p = plan((BIG, 1000.0, BIG, "fpG"), {}, BIG, 1001.0, "fpDIFF", N)
    assert p.do_pull is True and p.start == 0 and p.reason == "rotated-reread"

    # brand-new path, no signature -> pull from 0
    p = plan(None, {}, BIG, 1.0, "fpNew", N)
    assert p.do_pull is True and p.start == 0 and p.reason == "new-file"

    # metadata changed but offset already >= size -> skip, re-save offset
    p = plan((BIG, 1000.0, BIG, "fpM"), {}, BIG, 2000.0, "fpM", N)
    assert p.do_pull is False and p.save_offset == BIG and p.reason == "no-new-bytes"


# =============================================================== end-to-end cascade (fake SFTP)
async def test_rename_cascade_skips_already_ingested_content(committed_source, monkeypatch):
    src = committed_source
    active = b"active-line\n" * 600   # 7200 bytes (>= N)
    rot1 = b"rotated-one\n" * 600
    rot2 = b"rotated-two\n" * 600
    files = {
        "C:/BEC Logs/app.txt":   (active, 3000.0),
        "C:/BEC Logs/app.txt.1": (rot1, 2000.0),
        "C:/BEC Logs/app.txt.2": (rot2, 1000.0),
    }
    _patch_sftp(monkeypatch, files)
    _patch_ingest_counts_lines(monkeypatch)

    # first poll ingests all three files
    s1 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s1["files_fetched"] == 3
    assert s1["content_skipped"] == 0
    assert s1["entries_ingested"] == 1800  # 600 lines * 3 files

    # RENAME CASCADE: every file's content shifts to the next path (mtime travels with it, as a
    # Windows rename preserves last-write-time); a fresh, small active file is created.
    files.clear()
    files.update({
        "C:/BEC Logs/app.txt":   (b"new\n" * 3, 4000.0),  # brand-new active (small)
        "C:/BEC Logs/app.txt.1": (active, 3000.0),         # was app.txt
        "C:/BEC Logs/app.txt.2": (rot1, 2000.0),           # was app.txt.1
        "C:/BEC Logs/app.txt.3": (rot2, 1000.0),           # was app.txt.2
    })
    s2 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    # the three cascaded files are recognised as already-ingested -> NOT re-downloaded;
    # only the genuinely-new active file is pulled.
    assert s2["content_skipped"] == 3
    assert s2["files_fetched"] == 1
    assert s2["entries_ingested"] == 3           # just the new active file's 3 lines — no re-ingest

    # the cascaded content is checkpointed at its new path, marked fully consumed
    ck = await remote_fetcher._load_ckpts(src)
    for p in ("C:/BEC Logs/app.txt.1", "C:/BEC Logs/app.txt.2", "C:/BEC Logs/app.txt.3"):
        assert ck[p][0] == ck[p][2]              # last_size == last_offset


async def test_reused_path_different_content_is_not_skipped(committed_source, monkeypatch):
    """Safety: a path whose content is REPLACED by different bytes of identical size+mtime must be
    re-read, never content-skipped — the fingerprint differs, so no false skip / data loss."""
    src = committed_source
    x = b"xxxxxxxxx\n" * 600
    files = {"C:/BEC Logs/app.txt.1": (x, 1000.0)}
    _patch_sftp(monkeypatch, files)
    _patch_ingest_counts_lines(monkeypatch)
    await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)

    y = b"yyyyyyyyy\n" * 600            # different content, identical size + mtime
    assert len(y) == len(x)
    files["C:/BEC Logs/app.txt.1"] = (y, 1000.0)
    s = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s["content_skipped"] == 0
    assert s["files_fetched"] == 1              # re-read (fingerprint guard), not skipped
