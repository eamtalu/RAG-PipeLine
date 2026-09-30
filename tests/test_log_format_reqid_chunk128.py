"""Chunk 128: the server log gained a request id (production switched on 2026-09-29 13:00).

New lines (fixture `tests/fixtures/m3_sample_reqid.log`, cut from the real 2026-09-16 sample):
    MoveNext - REQUEST (ReqID = <id>) - : <url>
    <SendAsync>b__1 - RESPONSE (ReqID = <id>): <json>
    LogAPICall - (ReqID = <id>):            (body follows)
    LogAPIResult - (ReqID = <id>) - MI Program: … Result: OK
Stored procedures, narration and errors still carry no id; REQUEST BODY is unchanged. One request
in the sample has an EMPTY id ("ReqID = ") and the server logs "Couldn't find a request ID".

The parser used to classify by `startswith("REQUEST:")`, so every new request and response became
`info` and Stage 2 built transactions with no request and no response (181 of 185 incomplete on the
sample; 759 of 1,226 on tmp-test since 15 Sep). Stage 2 now joins by id wherever a line has one and
keeps the (server, thread, user) rules for everything else, so both formats work, even mixed.
"""

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job, JobStatus
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.repositories.job_repository import JobRepository
from app.persistence.storage import get_storage
from app.services.mnp_log_ingestion.LogIngestion import LogIngestion
from app.services.mnp_log_ingestion.parsers.m3_dotnet_parser import M3DotNetLogParser
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.services.mnp_log_ingestion.pipeline import fingerprints as fp

FIXTURE = Path(__file__).parent / "fixtures" / "m3_sample_reqid.log"
OLD_FIXTURE = Path(__file__).parent / "fixtures" / "m3_sample.log"
CC = "test_chunk128"
SRC = "TMP-AZ-BEC02/eSmartServerLog.txt"


# ==================================================== 1. the parser

@pytest.fixture(scope="module")
def records():
    return M3DotNetLogParser().parse(FIXTURE.read_text(encoding="utf-8"))


def test_new_request_and_response_lines_are_classified_and_carry_the_id(records):
    reqs = [r for r in records if r.entry_type == "request"]
    resps = [r for r in records if r.entry_type == "response"]
    assert len(reqs) == 9 and len(resps) == 10  # 8 + 9 new, 1 + 1 old
    with_id = [r for r in reqs if r.fields.get("reqid")]
    assert len(with_id) == 7  # 8 new requests, one of them with an EMPTY id
    assert all(r.fields.get("reqid") for r in resps if "(ReqID" in r.raw_body)
    first = reqs[0]
    assert first.fields["reqid"] == "25171E0100-2026-06-12_21:18:24.784-3615"
    assert first.fields["url"].startswith("http://192.168.0.124:60948/api/server/CheckServer")
    assert first.fields["params"]["MethodName"] == "CheckServer"


def test_the_message_reads_as_before_and_the_raw_body_is_untouched(records):
    req = next(r for r in records if r.entry_type == "request" and r.fields.get("reqid"))
    resp = next(r for r in records if r.entry_type == "response" and r.fields.get("reqid"))
    assert req.message.startswith("REQUEST: http://") and "(ReqID" not in req.message
    assert resp.message.startswith("RESPONSE: ") and "(ReqID" not in resp.message
    assert "(ReqID = " in req.raw_body and "(ReqID = " in resp.raw_body
    assert resp.fields["response"]["Company"] == "920"


def test_the_header_id_agrees_with_the_url_id_and_the_body_id(records):
    for r in records:
        if r.entry_type == "request" and r.fields.get("reqid") and "ReqId" in (r.fields.get("params") or {}):
            assert r.fields["params"]["ReqId"] == r.fields["reqid"]
    post = next(r for r in records if r.entry_type == "request" and r.fields.get("url", "").endswith("LogSignOff"))
    body = next(r for r in records if r.entry_type == "request_body")
    assert post.fields["reqid"] == body.fields["ReqId"] and "ReqId" not in (post.fields.get("params") or {})


def test_m3_call_and_result_lines_carry_the_id_and_still_parse(records):
    calls = [r for r in records if r.entry_type == "mi_call"]
    results = [r for r in records if r.entry_type == "mi_result"]
    assert calls and results
    stamped = [r for r in calls + results if "(ReqID = " in r.raw_body]
    assert len(stamped) >= 2 and all(r.fields.get("reqid") for r in stamped)
    assert not any(r.fields.get("reqid") for r in calls + results if "(ReqID = " not in r.raw_body)  # the old slice
    assert all(r.mi_program and r.mi_transaction for r in calls)
    assert all(r.result_status for r in results) and not any((r.message or "").startswith("(ReqID") for r in results)


def test_an_empty_id_is_no_id(records):
    empty = next(r for r in records if r.entry_type == "request" and "ListPOLines" in r.fields.get("url", ""))
    assert "reqid" not in empty.fields and "ReqID = )" in empty.raw_body


def test_the_old_format_parses_exactly_as_before():
    old = M3DotNetLogParser().parse(OLD_FIXTURE.read_text(encoding="utf-8"))
    counts = {}
    for r in old:
        counts[r.entry_type] = counts.get(r.entry_type, 0) + 1
    assert counts["request"] > 0 and counts["response"] > 0
    assert not any("reqid" in r.fields for r in old)
    assert all(r.message.startswith("REQUEST: ") for r in old if r.entry_type == "request")


# ==================================================== 2. Stage 2 grouping

def _e(kind, at, line, *, thread, user, fields=None, message="x", result=None, src=SRC):
    return LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=uuid.uuid4(), timestamp=at, source_file=src,
                    line_number=line, level="INFO", raw_body=f"line {line}", message=message,
                    entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind), thread=str(thread),
                    user_ctx=user, fields=fields or {}, result_status=result)


T0 = datetime(2026, 9, 29, 13, 5, 0, tzinfo=timezone.utc)


def _ms(n):
    return T0 + timedelta(milliseconds=n)


def _lines(b):
    return sorted(e.line_number for e in b.entries)


def _with(groups, line):
    return next(b for b in groups if any(e.line_number == line for e in b.entries))


def test_a_response_closes_its_request_by_id_across_threads_and_users():
    entries = [
        _e("request", _ms(0), 1, thread=10, user="BECWHLO", fields={"url": "http://x/api/a?ReqId=R1", "reqid": "R1"}),
        _e("sql", _ms(5), 2, thread=10, user="BECWHLO"),
        _e("response", _ms(40), 3, thread=9, user=None, fields={"reqid": "R1", "response": "OK"}),
    ]
    groups = dt._group(entries)
    assert len(groups) == 1 and _lines(groups[0]) == [1, 2, 3]


def test_two_users_on_one_thread_with_ids_never_mix_even_when_responses_come_back_reversed():
    entries = [
        _e("request", _ms(0), 1, thread=7, user="ANNA", fields={"url": "http://x/api/a", "reqid": "A"}),
        _e("request", _ms(2), 2, thread=7, user="BOB", fields={"url": "http://x/api/b", "reqid": "B"}),
        _e("mi_call", _ms(3), 3, thread=7, user="ANNA", fields={"reqid": "A", "program": "P", "transaction": "T"}),
        _e("mi_result", _ms(9), 4, thread=7, user="ANNA", result="OK", fields={"reqid": "A"}),
        _e("response", _ms(20), 5, thread=12, user="BOB", fields={"reqid": "B", "response": "b"}),
        _e("response", _ms(30), 6, thread=12, user="ANNA", fields={"reqid": "A", "response": "a"}),
    ]
    groups = dt._group(entries)
    assert sorted(_lines(g) for g in groups) == [[1, 3, 4, 6], [2, 5]]


def test_id_less_work_on_the_request_thread_joins_the_id_builder():
    entries = [
        _e("request", _ms(0), 1, thread=10, user="BECWHLO", fields={"url": "http://x/api/a", "reqid": "R1"}),
        _e("info", _ms(1), 2, thread=10, user="BECWHLO", message="Getting Device Details"),
        _e("sql", _ms(2), 3, thread=10, user=None),  # (null) user inherits the thread's stream
        _e("mi_call", _ms(3), 4, thread=10, user="BECWHLO", fields={"reqid": "R1", "program": "P", "transaction": "T"}),
        _e("response", _ms(50), 5, thread=3, user="BECWHLO", fields={"reqid": "R1", "response": "OK"}),
    ]
    groups = dt._group(entries)
    assert len(groups) == 1 and _lines(groups[0]) == [1, 2, 3, 4, 5]


def test_m3_lines_with_an_id_join_their_request_from_any_thread():
    entries = [
        _e("request", _ms(0), 1, thread=10, user="BECWHLO", fields={"url": "http://x/api/a", "reqid": "R1"}),
        _e("mi_call", _ms(3), 2, thread=63, user="BECWHLO", fields={"reqid": "R1", "program": "P", "transaction": "T"}),
        _e("mi_result", _ms(9), 3, thread=63, user="BECWHLO", result="OK", fields={"reqid": "R1"}),
        _e("response", _ms(50), 4, thread=42, user="BECWHLO", fields={"reqid": "R1", "response": "OK"}),
    ]
    groups = dt._group(entries)
    assert len(groups) == 1 and _lines(groups[0]) == [1, 2, 3, 4]


def test_old_and_new_format_in_one_window_and_an_id_response_never_closes_an_id_less_request():
    entries = [
        _e("request", _ms(0), 1, thread=10, user="BECWHLO", fields={"url": "http://x/api/old", "params": {"User": "BECWHLO"}}),
        _e("info", _ms(1), 2, thread=10, user="BECWHLO", message="old work"),
        _e("request", _ms(2), 3, thread=11, user="BECWHLO", fields={"url": "http://x/api/new", "reqid": "N1"}),
        _e("info", _ms(3), 4, thread=11, user="BECWHLO", message="new work"),
        _e("response", _ms(10), 5, thread=9, user="BECWHLO", fields={"reqid": "N1", "response": "new"}),
        _e("response", _ms(12), 6, thread=9, user="BECWHLO", fields={"response": "old"}),
    ]
    groups = dt._group(entries)
    assert sorted(_lines(g) for g in groups) == [[1, 2, 6], [3, 4, 5]]


def test_a_response_whose_id_is_unknown_stands_alone_rather_than_stealing_work():
    entries = [
        _e("request", _ms(0), 1, thread=10, user="BECWHLO", fields={"url": "http://x/api/a", "reqid": "R1"}),
        _e("info", _ms(1), 2, thread=10, user="BECWHLO", message="work"),
        _e("response", _ms(10), 3, thread=9, user="BECWHLO", fields={"reqid": "GONE", "response": "?"}),
        _e("response", _ms(20), 4, thread=9, user="BECWHLO", fields={"reqid": "R1", "response": "OK"}),
    ]
    groups = dt._group(entries)
    assert sorted(_lines(g) for g in groups) == [[1, 2, 4], [3]]


def test_a_seeded_stream_with_an_id_is_closed_by_its_response():
    req = _e("request", _ms(0), 1, thread=10, user="BECWHLO", fields={"url": "http://x/api/a", "reqid": "R1"})
    work = _e("info", _ms(1), 2, thread=10, user="BECWHLO", message="work")
    seed = {"streams": [{"entries": [req, work], "thread": "10", "user_ctx": "BECWHLO", "open_pos": (T0, 1),
                         "is_current": True}], "pending": []}
    resp = _e("response", _ms(500), 3, thread=9, user="BECWHLO", fields={"reqid": "R1", "response": "OK"})
    groups = dt._group([resp], seed=seed)
    assert len(groups) == 1 and _lines(groups[0]) == [1, 2, 3]


def test_the_transaction_takes_its_id_from_the_header_and_strips_the_response_prefix():
    b = dt._TxnBuilder()
    b.add(_e("request", _ms(0), 1, thread=10, user="BECWHLO",
             fields={"url": "http://x/api/Activity/LogSignOff", "reqid": "3091-2026-06-12_21:27:18.266-3215"}))
    b.add(_e("request_body", _ms(0), 2, thread=10, user="BECWHLO", fields={"User": "BECWHLO", "MethodName": "LogSignOff"}))
    b.add(_e("response", _ms(18), 3, thread=11, user="BECWHLO", message='RESPONSE: "OK"',
             fields={"reqid": "3091-2026-06-12_21:27:18.266-3215", "response": "OK"}))
    values = b.compute()
    assert values["reqid"] == "3091-2026-06-12_21:27:18.266-3215" and values["method"] == "LogSignOff"
    assert values["response_summary"] == '"OK"' and values["status"].value == "success"


def test_the_derivation_version_moved():
    assert fp._DERIVE_VERSION == 5


# ==================================================== 3. the whole pipeline over the fixture

async def _wipe():
    async with async_session() as db:
        for m in (AnalyticsPendingWindow, LogRegroupPending, LogEntryAssignment, LogTransaction, LogEntry):
            await db.execute(delete(m).where(m.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="chunk128", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def test_the_fixture_stitches_into_complete_transactions_end_to_end(clean):
    async with async_session() as db:
        ingestion = LogIngestion(get_storage(), JobRepository(db))
        await ingestion.ingest(FIXTURE.read_bytes(), SRC, CC, background=False)
    async with async_session() as db:
        await dt.regroup_all(db, CC)
        txns = (await db.execute(select(LogTransaction).where(LogTransaction.customer_code == CC))).scalars().all()
        assigned = (await db.execute(
            select(LogEntryAssignment.transaction_id, LogEntry.entry_type, LogEntry.fields)
            .join(LogEntry, LogEntry.id == LogEntryAssignment.entry_id)
            .where(LogEntryAssignment.customer_code == CC))).all()
    by_txn: dict = {}
    for tid, et, fields in assigned:
        by_txn.setdefault(tid, []).append((et.value, (fields or {}).get("reqid")))
    # what the fixture itself says: stamped responses whose stamped request is in the slice, and
    # those whose request is outside it (the two responses in the 46012-46030 slice)
    recs = M3DotNetLogParser().parse(FIXTURE.read_text(encoding="utf-8"))
    req_ids = {r.fields.get("reqid") for r in recs if r.entry_type == "request" and r.fields.get("reqid")}
    resp_ids = [r.fields.get("reqid") for r in recs if r.entry_type == "response" and r.fields.get("reqid")]
    paired = sum(1 for rid in resp_ids if rid in req_ids)
    unpaired = sum(1 for rid in resp_ids if rid not in req_ids)
    assert paired == 7 and unpaired == 2
    with_response = [tid for tid, members in by_txn.items() if any(et == "response" for et, _ in members)]
    headed = [tid for tid in with_response if any(et == "request" for et, _ in by_txn[tid])]
    headless = [tid for tid in with_response if tid not in headed]
    assert len(headed) == paired + 1  # the stamped pairs plus the old-format conversation
    assert len(headless) == unpaired  # a response whose request is outside the slice stands alone
    for tid in with_response:
        ids = {rid for _, rid in by_txn[tid] if rid}
        assert len(ids) <= 1, "a transaction never carries two request ids"
    statuses = sorted(t.status.value for t in txns)
    assert statuses.count("success") + statuses.count("soft") >= paired + 1
    assert sum(1 for t in txns if t.reqid) >= paired


# ==================================================== 4. repairing rows stored before the fix

async def test_rows_stored_as_info_before_the_fix_are_reclassified_in_place_and_ticketed(clean):
    from app.services.mnp_log_ingestion.pipeline.reclassify import reclassify_stamped

    lines = FIXTURE.read_text(encoding="utf-8").splitlines()
    req_line = next(l for l in lines if " - REQUEST (ReqID = " in l)
    resp_line = next(l for l in lines if " - RESPONSE (ReqID = " in l)
    mi_line = next(l for l in lines if "LogAPIResult - (ReqID = " in l)
    at = datetime(2026, 9, 29, 13, 5, tzinfo=timezone.utc)
    job = Job(customer_code=CC, filename=SRC, document_type="transaction_log", storage_key="x", status=JobStatus.completed,
              chunk_count=0)
    async with async_session() as db:
        db.add(job)
        await db.commit()
        job_id = job.id
    wrong = [
        LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job_id, timestamp=at, source_file=SRC, line_number=1,
                 level="DEBUG", thread="10", user_ctx=None, logger="Server.CommonCode.ApiLogHandler", method="MoveNext",
                 entry_type=LogEntryType.info, message=req_line.split(" - ", 1)[1], raw_body=req_line, fields={},
                 entry_hash=uuid.uuid4().hex),
        LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job_id, timestamp=at + timedelta(seconds=1),
                 source_file=SRC, line_number=2, level="DEBUG", thread="9", user_ctx=None,
                 logger="Server.CommonCode.ApiLogHandler", method="<SendAsync>b__1", entry_type=LogEntryType.info,
                 message=resp_line.split(" - ", 1)[1], raw_body=resp_line, fields={}, entry_hash=uuid.uuid4().hex),
        LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job_id, timestamp=at + timedelta(seconds=2),
                 source_file=SRC, line_number=3, level="DEBUG", thread="10", user_ctx="BECWHLO",
                 logger="M3WebServiceClassLib.CommonCode.WebService", method="LogAPIResult",
                 entry_type=LogEntryType.mi_result, message=mi_line.split(" - ", 1)[1], raw_body=mi_line,
                 fields={"program": "MNS150MI"}, mi_program="MNS150MI", entry_hash=uuid.uuid4().hex),
    ]
    async with async_session() as db:
        db.add_all(wrong)
        await db.commit()
    async with async_session() as db:
        # the headless transaction production built from those rows, sealed
        broken = await dt.regroup_all(db, CC)
        stored = (await db.execute(select(LogTransaction).where(LogTransaction.customer_code == CC))).scalars().all()
        assert stored and all(t.status.value in ("incomplete", "soft", "success") for t in stored)
    async with async_session() as db:
        preview = await reclassify_stamped(db, CC, since=at - timedelta(minutes=1), dry_run=True)
        assert preview["changed"] == 3 and preview["ticketed"] is None and preview["transactions_dropped"] == 0
        result = await reclassify_stamped(db, CC, since=at - timedelta(minutes=1))
    assert result["scanned"] == 3 and result["changed"] == 3 and result["by_former_type"] == {"info": 2, "mi_result": 1}
    assert result["transactions_dropped"] == len(stored)
    async with async_session() as db:
        assert (await db.execute(select(LogTransaction).where(LogTransaction.customer_code == CC))).scalars().all() == []
    async with async_session() as db:
        rows = (await db.execute(select(LogEntry).where(LogEntry.customer_code == CC).order_by(LogEntry.line_number))).scalars().all()
        assert [r.entry_type.value for r in rows] == ["request", "response", "mi_result"]
        assert rows[0].message.startswith("REQUEST: http://") and rows[0].fields["reqid"] and rows[1].fields["reqid"]
        assert rows[2].fields["reqid"] and not rows[2].message.startswith("(ReqID")
        assert all(r.raw_body.count("(ReqID = ") == 1 for r in rows)  # the raw text is never touched
        tickets = (await db.execute(select(LogRegroupPending).where(LogRegroupPending.customer_code == CC))).scalars().all()
        assert len(tickets) == 1 and tickets[0].range_start <= at and tickets[0].range_end >= at + timedelta(seconds=2)
        again = await reclassify_stamped(db, CC, since=at - timedelta(minutes=1))
        assert again["changed"] == 0


# ==================================================== 5. the stamp is not a response field

def test_the_stamped_id_on_a_response_entry_is_not_projected_as_a_response_field():
    from app.services.analytics import payload as pl

    entries = [("response", {"response": {"Location": "A03A"}, "reqid": "3091-2026-09-30_07:30:55.006-6143"}),
               ("response", {"response": [{"PickListNumber": "1"}], "reqid": "3091-x"})]
    out = pl.extract(entries) if hasattr(pl, "extract") else None
    if out is None:
        import inspect
        fn = next(f for n, f in inspect.getmembers(pl, inspect.isfunction) if "entries" in inspect.signature(f).parameters)
        out = fn(entries)
    assert out.get("resp.Location") == "A03A"
    assert not any(k.endswith("reqid") for k in out), out
