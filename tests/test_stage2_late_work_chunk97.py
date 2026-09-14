"""Chunk 97 (section 18ad): late work rejoins its conversation; a GET without MethodName is named by its URL.

Measured on the two London days kept after the 2026-09-14 fresh start (9,233 transactions). The
response-ownership rules of 18ac hold - no transaction with two responses, none with a response before
its own last work line - and two structural leftovers remain:

  1. LATE WORK. The server writes RESPONSE from another thread the moment the handler returns, and the
     handler's own thread may still log one line ("Activity Logged OK", "bearer = HIDDEN") 4 to 6 ms
     later. When two requests of one user finish within those milliseconds, the response closes the
     conversation first; the tail then has no open builder for its (server, thread, user) and no pending
     request on its thread, so it opens a headless one that the user's NEXT response closes. Two live
     shapes reconstructed here: OPRACHASUK 11:39:49 (two ReportCount 3 ms apart) and HWORREL 12:33:17
     (three GetAccessToken within 22 ms).
  2. METHOD NULL. `compute()` took the method only from the `MethodName` parameter; GetAccessToken has
     none, so 26 sign-ons a day showed as method NULL. The URL's last path segment names them.
"""
import uuid
from datetime import datetime, timedelta, timezone

from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

CC = "test_chunk97"
SRC = "TMP-AZ-BEC02/eSmartServerLog.txt"
T0 = datetime(2026, 9, 14, 11, 39, 48, 900000, tzinfo=timezone.utc)


def _e(kind, ms, line, *, thread, user="OPRACHASUK", fields=None, message="x", result=None):
    return LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=uuid.uuid4(), timestamp=T0 + timedelta(milliseconds=ms),
                    source_file=SRC, line_number=line, level="INFO", raw_body="x", message=message,
                    entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind), thread=str(thread),
                    user_ctx=user, fields=fields or {}, result_status=result)


def _lines(b):
    return sorted(e.line_number for e in b.entries)


def _with(groups, line):
    return next(b for b in groups if any(e.line_number == line for e in b.entries))


def _kinds(b):
    return [e.entry_type.value for e in sorted(b.entries, key=lambda e: (e.timestamp, e.line_number))]


def _report_count(thread, first_line, start_ms, mi_ms=102):
    """A ReportCount POST as logged, without its RESPONSE and its last 'Activity Logged OK'. `mi_ms` is
    how long M3 took, which is what decides which of two concurrent calls finishes first."""
    L, m = first_line, start_ms
    return [
        _e("request", m, L, thread=thread, fields={"url": "http://172.17.0.230/api/count/ReportCount"}),
        _e("request_body", m + 13, L + 1, thread=thread,
           fields={"User": "OPRACHASUK", "MethodName": "ReportCount", "InventoryNumber": "284"}),
        _e("info", m + 15, L + 2, thread=thread, message="Calling MMS301MI - UpdStockTake"),
        _e("mi_call", m + 15, L + 3, thread=thread,
           fields={"program": "MMS301MI", "transaction": "UpdStockTake", "params": {"m3user": "OPRACHASUK"}}),
        _e("mi_result", m + 15 + mi_ms, L + 4, thread=thread, result="OK",
           fields={"program": "MMS301MI", "transaction": "UpdStockTake", "result": "OK"}),
        _e("info", m + 16 + mi_ms, L + 5, thread=thread, message="Downloaded 1 Records"),
        _e("sql", m + 17 + mi_ms, L + 6, thread=thread),
    ]


# ============================================================ 1. late work rejoins
def _case_two_report_counts():
    """11:39:48.916 thread 45 and .919 thread 13. Thread 13 finishes first ('Activity Logged OK' at
    .039); its RESPONSE is logged at .040, one millisecond after thread 45's `sql` line, so recency
    hands it to thread 45. Thread 45's own 'Activity Logged OK' follows at .044 with its RESPONSE."""
    a = _report_count(45, 100, 16, mi_ms=107)   # .916, M3 slower: its sql lands at .140
    b = _report_count(13, 200, 19, mi_ms=99)    # .919, M3 faster: its sql at .135, done at .139
    return [
        *a, *b,
        _e("info", 139, 207, thread=13, message="Activity Logged OK"),
        _e("response", 140, 300, thread=46, fields={"response": "OK"}),     # thread 13's answer
        _e("info", 144, 107, thread=45, message="Activity Logged OK"),      # thread 45's late tail
        _e("response", 144, 301, thread=39, fields={"response": "OK"}),     # thread 45's answer
    ]


def test_a_tail_logged_milliseconds_after_the_response_rejoins_its_conversation():
    groups = dt._group(_case_two_report_counts())
    a = _with(groups, 100)
    assert 107 in _lines(a), "thread 45's 'Activity Logged OK' belongs to thread 45's conversation"
    assert _kinds(a).count("response") == 1


def test_both_concurrent_conversations_end_whole_with_one_answer_each():
    groups = dt._group(_case_two_report_counts())
    a, b = _with(groups, 100), _with(groups, 200)
    assert _kinds(a).count("response") == 1 and _kinds(b).count("response") == 1
    assert 207 in _lines(b)
    assert len([g for g in groups if g.entries]) == 2, "no headless third conversation"


def _token(thread, first_line, start_ms):
    return _e("request", start_ms, first_line, thread=thread, user="HWORREL",
              fields={"url": "http://172.17.0.230/api/server/GetAccessToken",
                      "params": {"Creds": "HIDDEN"}})


def test_three_sign_ons_within_22ms_end_as_three_whole_conversations():
    """12:33:16.952/.958/.974 on threads 53/60/57. The first RESPONSE (.101) is logged BEFORE any work
    line, so it closes a pending request outright; that request's 'bearer' line arrives 6 ms later."""
    entries = [
        _token(53, 10, 0), _token(60, 20, 6), _token(57, 30, 22),
        _e("info", 149, 21, thread=60, user="HWORREL", message="bearer = HIDDEN"),
        _e("response", 149, 40, thread=11, user="HWORREL", fields={"response": {"AccessToken": "HIDDEN"}}),
        _e("info", 155, 31, thread=57, user="HWORREL", message="bearer = HIDDEN"),
        _e("response", 156, 41, thread=54, user="HWORREL", fields={"response": {"AccessToken": "HIDDEN"}}),
        _e("info", 159, 11, thread=53, user="HWORREL", message="bearer = HIDDEN"),
        _e("response", 159, 42, thread=50, user="HWORREL", fields={"response": {"AccessToken": "HIDDEN"}}),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    assert len(groups) == 3
    for g in groups:
        # the tail may be logged after the response; what matters is one of each
        assert sorted(_kinds(g)) == ["info", "request", "response"], _lines(g)
    # each 'bearer' line sits with the request of ITS thread
    for req_line, info_line in ((10, 11), (20, 21), (30, 31)):
        assert info_line in _lines(_with(groups, req_line))


def test_a_tail_beyond_the_window_does_not_rejoin():
    """Two seconds later is not a tail: the rule must not glue later narration onto a closed
    conversation. The line opens its own builder exactly as before."""
    entries = [
        *_report_count(45, 100, 16),
        _e("info", 139, 107, thread=45, message="Activity Logged OK"),
        _e("response", 140, 300, thread=46, fields={"response": "OK"}),
        _e("info", 2140, 108, thread=45, message="Revoke Failed"),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    assert 108 not in _lines(_with(groups, 100))
    assert len(groups) == 2


def test_a_new_request_on_the_reused_thread_still_opens_a_new_cycle():
    """.NET reuses the thread: a new POST body on the same thread and user inside the window is a new
    conversation, and the work after it belongs to the new one, not to the closed one."""
    entries = [
        *_report_count(45, 100, 16),
        _e("info", 139, 107, thread=45, message="Activity Logged OK"),
        _e("response", 140, 300, thread=46, fields={"response": "OK"}),
        _e("request", 150, 400, thread=45, fields={"url": "http://172.17.0.230/api/count/ReportCount"}),
        _e("request_body", 160, 401, thread=45, fields={"User": "OPRACHASUK", "MethodName": "ReportCount"}),
        _e("info", 161, 402, thread=45, message="Calling MMS301MI - UpdStockTake"),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    assert _lines(_with(groups, 400)) == [400, 401, 402]
    assert 402 not in _lines(_with(groups, 100))


def test_a_single_conversation_is_unchanged():
    entries = [
        *_report_count(45, 100, 16),
        _e("info", 139, 107, thread=45, message="Activity Logged OK"),
        _e("response", 140, 300, thread=46, fields={"response": "OK"}),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    assert len(groups) == 1 and _lines(groups[0]) == [100, 101, 102, 103, 104, 105, 106, 107, 300]


# ============================================================ 2. method from the URL
def test_a_get_without_methodname_is_named_by_its_url():
    entries = [
        _token(53, 10, 0),
        _e("info", 149, 11, thread=53, user="HWORREL", message="bearer = HIDDEN"),
        _e("response", 150, 40, thread=11, user="HWORREL", fields={"response": {"AccessToken": "HIDDEN"}}),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    assert groups[0].compute()["method"] == "GetAccessToken"


def test_methodname_wins_over_the_url_when_both_exist():
    entries = [
        _e("request", 0, 10, thread=45, fields={"url": "http://172.17.0.230/api/receiving/ReceiptPO"}),
        _e("request_body", 9, 11, thread=45, fields={"User": "OPRACHASUK", "MethodName": "ConfirmPickLine"}),
        _e("response", 30, 12, thread=46, fields={"response": "2.0"}),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    assert groups[0].compute()["method"] == "ConfirmPickLine"


def test_a_url_with_a_query_string_or_trailing_slash_still_yields_the_segment():
    assert dt._method_from_url("http://172.17.0.230/api/server/GetAccessToken?Creds=%7b%22") == "GetAccessToken"
    assert dt._method_from_url("http://172.17.0.230/api/server/CheckServer/") == "CheckServer"
    assert dt._method_from_url(None) is None
    assert dt._method_from_url("http://172.17.0.230/") is None
