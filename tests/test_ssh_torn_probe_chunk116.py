"""Chunk 116: a file's identity must be sampled at ONE instant, and a file that vanishes mid-poll
must not kill the poll.

Root cause (2026-09-17, tenant tmp-live, source TMP-AZ-BEC01). `_list()` stats every file once at
poll start, and the head fingerprint was read live per file, seconds to tens of seconds later. The
remote rotates by rename cascade every few hours and the poller is nearly always mid-poll, so a
rotation between the two samples paired the OLD file's (size, mtime) with the NEW file's
fingerprint. That torn triple never matches the content-identity index (chunk 12), so the cascade
skip silently failed, the fingerprint comparison reported "rotated", and every rotated file was
re-downloaded and re-ingested from byte 0. The torn triple was then written to the checkpoint and
poisoned the next poll as well. With the historical entries gone (disk recovery), dedup had nothing
to absorb: 5,856 week-old transactions were recreated with a fresh updated_at and the notification
rule re-alerted on all of them. The same unguarded open also raised SFTPNoSuchFile when the cascade
briefly removed a path, aborting the whole source poll (06:30, 14:10 and 14:40 the same day).

Fix: `_probe()` opens the file once and takes size, mtime (handle-bound stat) and the head hash from
that ONE handle; a vanished file is skipped for this poll; and the download handle re-checks the
head so a file swapped between probe and download is left alone rather than ingested under the
wrong identity.

Covered, in both inline and queue mode (production runs queue mode):
- rotation between listing and probe: cascaded content is content-skipped, only the new active
  file is pulled, and every checkpoint is coherent (fp == hash of the bytes whose size it records);
- a path that vanishes between listing and probe is skipped, the poll completes, and that path's
  checkpoint is untouched;
- a file swapped between probe and download is neither ingested nor checkpointed, and the next
  poll recovers it via the normal rotated-reread path.
"""

import hashlib

import pytest

from app.settings import settings
from app.persistence.models.log_ssh_fetch_run import LogSshFetchMode
from app.services.mnp_log_ingestion.remote import remote_fetcher

from tests.test_ssh_hardening_chunk2 import _patch_sftp, _patch_ingest_counts_lines
from tests.test_ingest_queue_chunk17 import clean  # noqa: F401  (fixture: purges queue rows)

N = settings.ssh_fingerprint_bytes  # 4096
D = "C:/BEC Logs"

ACTIVE = b"active-line\n" * 600   # 7200 bytes (>= N)
ROT1 = b"rotated-one\n" * 600
ROT2 = b"rotated-two\n" * 600
NEW_ACTIVE = b"new\n" * 3         # 12 bytes: a freshly rotated-in live file


def _fp(data: bytes) -> str:
    return hashlib.sha256(data[:N]).hexdigest()


def _coherent(data: bytes, mtime: float) -> tuple[int, float, int, str]:
    """The checkpoint a fully consumed file MUST carry: size, mtime and fingerprint of the same bytes."""
    return (len(data), mtime, len(data), _fp(data))


def _set_mode(monkeypatch, tmp_path, queue_mode: bool) -> None:
    if queue_mode:
        monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
        monkeypatch.setattr(settings, "upload_dir", tmp_path)
    else:
        _patch_ingest_counts_lines(monkeypatch)  # pins the flag OFF, counts newlines as entries


async def _assert_coherent(src, files: dict, paths) -> None:
    ck = await remote_fetcher._load_ckpts(src)
    for p in paths:
        data, mtime = files[p]
        assert ck[p] == _coherent(data, mtime), f"torn checkpoint at {p}: {ck[p]}"


# =============================================================== 1. rotation between list and probe
@pytest.mark.parametrize("queue_mode", [False, True], ids=["inline", "queue"])
async def test_rotation_between_listing_and_probe_skips_cascaded_content(
        committed_source, monkeypatch, tmp_path, clean, queue_mode):
    src = committed_source
    _set_mode(monkeypatch, tmp_path, queue_mode)
    files = {
        f"{D}/app.txt":   (ACTIVE, 3000.0),
        f"{D}/app.txt.1": (ROT1, 2000.0),
        f"{D}/app.txt.2": (ROT2, 1000.0),
    }
    rotated = {  # a Windows rename cascade: mtime travels with the bytes, a new small live file appears
        f"{D}/app.txt":   (NEW_ACTIVE, 4000.0),
        f"{D}/app.txt.1": (ACTIVE, 3000.0),
        f"{D}/app.txt.2": (ROT1, 2000.0),
        f"{D}/app.txt.3": (ROT2, 1000.0),
    }
    armed = {"rotate": False}

    def before_open(path):
        # The listing has already been taken (pre-rotation). Rotate on the FIRST open of the poll so
        # every per-file sample happens post-rotation - the exact race seen on BEC01 at 14:10:08.
        if armed["rotate"]:
            files.clear()
            files.update(rotated)
            armed["rotate"] = False

    _patch_sftp(monkeypatch, files, before_open=before_open)

    s1 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s1["files_fetched"] == 3 and s1["content_skipped"] == 0
    await _assert_coherent(src, files, files.keys())

    armed["rotate"] = True
    s2 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    # .1 and .2 now hold bytes we already ingested (at app.txt and .1) -> recognised and skipped;
    # only the genuinely new live file is pulled. `.3` is not in this poll's listing yet.
    assert s2["content_skipped"] == 2, s2
    assert s2["files_fetched"] == 1, s2
    if not queue_mode:
        assert s2["entries_ingested"] == 3          # the 3 lines of the new live file, nothing else
    await _assert_coherent(src, files, [f"{D}/app.txt", f"{D}/app.txt.1", f"{D}/app.txt.2"])

    # Next poll: the three known paths are unchanged. `.3` enters the listing for the first time, and
    # its bytes' identity (ROT2) is no longer on record - poll 2 correctly overwrote `.2`'s checkpoint
    # with ROT1 - so it is pulled once as a new file (chunk 12 semantics: an identity lives only in
    # the checkpoint of the path that last held it). One deduped pull per mid-poll rotation, bounded.
    s3 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s3["files_considered"] == 4
    assert s3["files_fetched"] == 1 and s3["content_skipped"] == 0, s3
    assert s3["bytes_fetched"] == len(ROT2)
    await _assert_coherent(src, files, files.keys())

    # and from here on it is quiet: every checkpoint coherent, nothing transferred
    s4 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s4["files_fetched"] == 0 and s4["content_skipped"] == 0 and s4["bytes_fetched"] == 0, s4


# =============================================================== 2. path vanishes mid-poll
@pytest.mark.parametrize("queue_mode", [False, True], ids=["inline", "queue"])
async def test_vanished_file_is_skipped_and_poll_completes(
        committed_source, monkeypatch, tmp_path, clean, queue_mode):
    src = committed_source
    _set_mode(monkeypatch, tmp_path, queue_mode)
    files = {
        f"{D}/app.txt":   (ACTIVE, 3000.0),
        f"{D}/app.txt.1": (ROT1, 2000.0),
        f"{D}/app.txt.2": (ROT2, 1000.0),
    }
    gone = f"{D}/app.txt.1"
    armed = {"vanish": False}

    def before_open(path):
        if armed["vanish"] and path == gone:
            files.pop(gone, None)      # rename cascade removed it between listing and open
            armed["vanish"] = False

    _patch_sftp(monkeypatch, files, before_open=before_open)

    s1 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s1["files_fetched"] == 3
    before = (await remote_fetcher._load_ckpts(src))[gone]

    armed["vanish"] = True
    s2 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)  # must NOT raise
    assert s2["files_considered"] == 3
    assert s2["vanished"] == 1, s2
    assert s2["files_fetched"] == 0                # the other two are unchanged
    assert (await remote_fetcher._load_ckpts(src))[gone] == before, "a vanished path keeps its checkpoint"


# =============================================================== 3. swapped between probe and download
@pytest.mark.parametrize("queue_mode", [False, True], ids=["inline", "queue"])
async def test_file_swapped_between_probe_and_download_is_not_ingested(
        committed_source, monkeypatch, tmp_path, clean, queue_mode):
    src = committed_source
    _set_mode(monkeypatch, tmp_path, queue_mode)
    path = f"{D}/app.txt"
    grown = ACTIVE + b"tail-line-x\n" * 10           # 7320 bytes: a plain append
    swapped = b"swapped-lin\n" * 610                  # 7320 bytes: same size, different bytes
    files = {path: (ACTIVE, 3000.0)}
    opens = {"n": 0, "armed": False}

    def before_open(p):
        if p == path and opens["armed"]:
            opens["n"] += 1
            if opens["n"] == 2:                      # 1st open = probe, 2nd open = download
                files[path] = (swapped, 3001.0)

    _patch_sftp(monkeypatch, files, before_open=before_open)

    s1 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s1["files_fetched"] == 1
    before = (await remote_fetcher._load_ckpts(src))[path]
    assert before == _coherent(ACTIVE, 3000.0)

    files[path] = (grown, 3001.0)                    # probe will see an append ...
    opens["armed"] = True
    s2 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    # ... but the download handle opens different bytes -> identity check fails -> left alone.
    assert s2["changed"] == 1, s2
    assert s2["files_fetched"] == 0, s2
    assert s2["objects_queued"] == 0 and s2["entries_ingested"] == 0
    assert (await remote_fetcher._load_ckpts(src))[path] == before, "no checkpoint write for a swapped file"

    # next poll: the swap is now a plain replacement at the same path -> rotated-reread, coherent.
    s3 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s3["files_fetched"] == 1 and s3["changed"] == 0
    assert (await remote_fetcher._load_ckpts(src))[path] == _coherent(swapped, 3001.0)
