"""Chunk 95 (section 18ac): which request a RESPONSE line belongs to.

Found on 2026-09-13 through the analytics composition screen: 67 of 12,131 ConfirmPickLine facts
in 14 days carried another request's response object (`resp.ItemNumber`, `resp.StockZone`,
`resp.Company`, `resp.ReceivingNumber`), and discovery registered those foreign fields under the
pick. Every one traced to one of three stitching faults in `_group`, reconstructed here line for
line from the live log (server TMP-AZ-BEC02, timestamps and threads as logged, bodies reduced to
the keys the grouper reads).

  A. Same user, shifted by one. A RESPONSE is given to the user's OLDEST open request. Once one
     response goes astray, every later response of that user lands one request late, and the
     shift runs until a request happens to get no response at all. One chain on 9 Sep 22:07-22:22
     covered 15 transactions. The server writes RESPONSE the moment a handler finishes, one
     millisecond after its last work line, so the owner is the candidate that was active most
     recently, not the one that opened first. And a candidate whose last line is an M3 call still
     waiting for its result cannot have written a response at all.
  B. A RESPONSE with no context user (a device's CheckServer answer) fell back to the oldest open
     work on the server, a pick waiting for its AddPickViaRepNo result. User-less work exists for
     it; prefer that.
  C. A POST body is paired with the most recent id-less REQUEST line regardless of who wrote it.
     Two requests 4 ms apart swapped URLs, and their responses followed the swapped lines. The URL
     line and its body are written by the same MoveNext on the same thread under the same user.
"""
import uuid
from datetime import datetime, timedelta, timezone

from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

CC = "test_chunk95"
SRC = "TMP-AZ-BEC02/eSmartServerLog.txt"
M3 = "https://mingle-ionapi.eu1.inforcloudsuite.com:443/X/M3/m3api-rest/v2/execute/"


def _e(kind, at, line, *, thread, user, fields=None, message="x", result=None):
    return LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=uuid.uuid4(), timestamp=at,
                    source_file=SRC, line_number=line, level="INFO", raw_body="x", message=message,
                    entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind), thread=str(thread),
                    user_ctx=user, fields=fields or {}, result_status=result)


def _t(hms_ms: str, day=9):
    h, m, s = hms_ms.split(":")
    sec, ms = s.split(".")
    return datetime(2026, 9, day, int(h), int(m), int(sec), int(ms) * 1000, tzinfo=timezone.utc)


def _lines(b):
    return sorted(e.line_number for e in b.entries)


def _with(groups, line):
    return next(b for b in groups if any(e.line_number == line for e in b.entries))


def _response_of(b):
    r = next((e for e in b.entries if e.entry_type.value == "response"), None)
    return None if r is None else r.fields.get("response")


def _pick(user, thread, first_line, t0: datetime, item: str):
    """A ConfirmPickLine conversation as the server logs it, minus its RESPONSE line: URL, body,
    LstBalID call and result, AddPickViaRepNo call and result, activity SQL, 'Activity Logged OK'."""
    L = first_line
    ms = lambda n: t0 + timedelta(milliseconds=n)  # noqa: E731
    return [
        _e("request", ms(0), L, thread=thread, user=user,
           fields={"url": "http://172.17.0.230/api/picking/ConfirmPickLine"}),
        _e("request_body", ms(17), L + 1, thread=thread, user=user,
           fields={"User": user, "MethodName": "ConfirmPickLine", "ItemNumber": item,
                   "Warehouse": "BRI", "TransactionType": "002001"}),
        _e("info", ms(18), L + 2, thread=thread, user=user, message="Calling MMS060MI - LstBalID"),
        _e("mi_call", ms(19), L + 3, thread=thread, user=user,
           fields={"program": "MMS060MI", "transaction": "LstBalID", "params": {"m3user": user}}),
        _e("mi_result", ms(122), L + 4, thread=thread, user=user, result="OK",
           fields={"program": "MMS060MI", "transaction": "LstBalID", "result": "OK"}),
        _e("info", ms(123), L + 5, thread=thread, user=user, message="Calling MHS850MI - AddPickViaRepNo"),
        _e("mi_call", ms(124), L + 6, thread=thread, user=user,
           fields={"program": "MHS850MI", "transaction": "AddPickViaRepNo", "params": {"m3user": user}}),
        _e("mi_result", ms(974), L + 7, thread=thread, user=user, result="OK",
           fields={"program": "MHS850MI", "transaction": "AddPickViaRepNo", "result": "OK"}),
        _e("sql", ms(975), L + 8, thread=thread, user=user),
        _e("info", ms(979), L + 9, thread=thread, user=user, message="Activity Logged OK"),
    ]


# ============================================================ A. same user, shifted by one
BALANCE = {"ItemNumber": "100953", "ItemDescription": "EGGS LIQUID YOLK PASTEURISED _1ltr",
           "Location": "A03C", "LotNumber": "2609011230", "StatusBalanceID": "",
           "PriorityDate": "20260916", "OnHandQuantity": "2"}


def _case_a():
    """9 Sep 22:52, JONEILL. An older request of the same user is still open with no response (the
    stray that started the chain). The pick finishes at .168 and its "1.0" is logged at .169 on
    another thread. A balance lookup starts at .400 and its object answer is logged at .513, one
    millisecond after its 'Sorted Data' line."""
    stale = _e("request", _t("22:51:50.000"), 40, thread=35, user="JONEILL",
               fields={"url": "http://172.17.0.230/api/picking/UnlockPickListsByUser",
                       "params": {"User": "JONEILL", "ReqId": "11-2026-09-09_23:59:15.000-1"}})
    pick = _pick("JONEILL", 9, 74, _t("22:52:21.189"), "100600")
    pick_resp = _e("response", _t("22:52:22.169"), 148, thread=7, user="JONEILL",
                   fields={"response": "1.0"})
    lookup = [
        _e("request", _t("22:52:22.400"), 149, thread=22, user="JONEILL",
           fields={"url": "http://172.17.0.230/api/balance/GetOldestItemBalanceAcrossZones",
                   "params": {"User": "JONEILL", "ReqId": "11-2026-09-09_23:59:46.932-918"}}),
        _e("info", _t("22:52:22.414"), 150, thread=22, user="JONEILL", message="Calling MMS060MI - LstBalID"),
        _e("mi_call", _t("22:52:22.414"), 151, thread=22, user="JONEILL",
           fields={"program": "MMS060MI", "transaction": "LstBalID", "params": {"m3user": "JONEILL"}}),
        _e("mi_result", _t("22:52:22.512"), 173, thread=22, user="JONEILL", result="OK",
           fields={"program": "MMS060MI", "transaction": "LstBalID", "result": "OK"}),
        _e("info", _t("22:52:22.512"), 199, thread=22, user="JONEILL", message="Downloaded 3 Records"),
        _e("info", _t("22:52:22.512"), 200, thread=22, user="JONEILL", message="Sorted Data"),
    ]
    lookup_resp = _e("response", _t("22:52:22.513"), 201, thread=8, user="JONEILL",
                     fields={"response": BALANCE})
    return [stale, *pick, pick_resp, *lookup, lookup_resp]


def test_a_finished_pick_gets_the_response_logged_one_millisecond_after_its_last_line():
    groups = dt._group(_case_a())
    pick = _with(groups, 74)
    assert _response_of(pick) == "1.0", _lines(pick)
    assert 148 in _lines(pick)


def test_a_lookup_gets_its_own_object_answer_not_the_pick():
    groups = dt._group(_case_a())
    lookup = _with(groups, 149)
    assert _response_of(lookup) == BALANCE, _lines(lookup)
    assert _lines(lookup) == [149, 150, 151, 173, 199, 200, 201]


def test_the_stray_older_request_does_not_steal_and_stays_incomplete():
    groups = dt._group(_case_a())
    stale = _with(groups, 40)
    assert _lines(stale) == [40], "the chain is contained to the request that really lost its response"
    assert _response_of(stale) is None


def test_a_candidate_waiting_for_an_m3_result_cannot_own_a_response():
    """The pick's AddPickViaRepNo call is in flight (last line is the mi_call). A same-user GET that
    opened LATER finishes first. FIFO would hand the GET's answer to the pick."""
    pick = _pick("JONEILL", 9, 74, _t("22:52:21.189"), "100600")[:7]     # ends at the mi_call
    get = _e("request", _t("22:52:21.200"), 90, thread=22, user="JONEILL",
             fields={"url": "http://172.17.0.230/api/x/GetAllReasonCodes",
                     "params": {"User": "JONEILL", "ReqId": "r-1"}})
    get_resp = _e("response", _t("22:52:21.250"), 91, thread=8, user="JONEILL",
                  fields={"response": [{"Code": "W01"}]})
    groups = dt._group([*pick, get, get_resp])
    assert _lines(_with(groups, 90)) == [90, 91]
    assert _response_of(_with(groups, 74)) is None


def test_a_single_open_conversation_still_takes_its_response():
    """No overlap, no change: the ordinary case must derive exactly as before."""
    pick = _pick("JONEILL", 9, 74, _t("22:52:21.189"), "100600")
    resp = _e("response", _t("22:52:22.169"), 84, thread=7, user="JONEILL", fields={"response": "1.0"})
    groups = dt._group([*pick, resp])
    assert len(groups) == 1 and _response_of(groups[0]) == "1.0"


def test_a_response_never_crosses_users_even_when_the_other_user_is_more_recent():
    """Recency ranks WITHIN the user's candidates. Another user's fresher work is not a candidate."""
    a = _pick("JONEILL", 9, 74, _t("22:52:21.189"), "100600")
    b = _pick("PEVANS", 16, 200, _t("22:52:21.500"), "100604")
    a_resp = _e("response", _t("22:52:22.500"), 300, thread=7, user="JONEILL", fields={"response": "1.0"})
    groups = dt._group([*a, *b, a_resp])
    assert _response_of(_with(groups, 74)) == "1.0"
    assert _response_of(_with(groups, 200)) is None


# ============================================================ B. a response with no user
HOST = {"Cipher": "78ab86ab", "Company": "915", "Division": "TMP", "Facility": "TMP", "Warehouse": "BRI"}


def _case_b():
    """12 Sep 04:30, PEVANS on thread 16 waiting for AddPickViaRepNo. A device's CheckServer GET
    (no user anywhere) runs on thread 21 and answers on thread 24, also with no user. The pick's
    own "4.0" follows 200 ms later."""
    pick = _pick("PEVANS", 16, 1833, _t("04:30:04.580", day=12), "100604")[:7]   # up to the mi_call
    check = [
        _e("request", _t("04:30:05.166", 12), 1893, thread=21, user=None,
           fields={"url": "http://172.17.0.230/api/server/CheckServer",
                   "params": {"User": "", "DeviceID": "25230E00BF", "ReqId": "25230E00BF-2026-09-12_05:33:34.661-3839"}}),
        _e("info", _t("04:30:05.181", 12), 1894, thread=21, user=None, message="Server Check by device"),
        _e("sql", _t("04:30:05.181", 12), 1896, thread=21, user=None),
        _e("info", _t("04:30:05.184", 12), 1906, thread=21, user=None, message="Device Returned HostDetails"),
    ]
    check_resp = _e("response", _t("04:30:05.184", 12), 1907, thread=24, user=None, fields={"response": HOST})
    pick_tail = [
        _e("mi_result", _t("04:30:05.373", 12), 1908, thread=16, user="PEVANS", result="OK",
           fields={"program": "MHS850MI", "transaction": "AddPickViaRepNo", "result": "OK"}),
        _e("sql", _t("04:30:05.374", 12), 1917, thread=16, user="PEVANS"),
        _e("info", _t("04:30:05.377", 12), 1921, thread=16, user="PEVANS", message="Activity Logged OK"),
    ]
    pick_resp = _e("response", _t("04:30:05.378", 12), 1922, thread=9, user="PEVANS", fields={"response": "4.0"})
    return [*pick, *check, check_resp, *pick_tail, pick_resp]


def test_b_a_user_less_response_goes_to_user_less_work_not_to_a_users_pick():
    groups = dt._group(_case_b())
    pick = _with(groups, 1833)
    assert _response_of(pick) == "4.0", _lines(pick)
    assert 1907 not in _lines(pick)
    assert _response_of(_with(groups, 1893)) == HOST


def test_b_the_pick_is_one_conversation_from_url_to_its_own_answer():
    groups = dt._group(_case_b())
    assert _lines(_with(groups, 1833)) == [1833, 1834, 1835, 1836, 1837, 1838, 1839, 1908, 1917, 1921, 1922]


def test_b_a_user_less_response_still_falls_back_when_no_user_less_work_exists():
    """Nothing anonymous is open: the pre-18ac fallback to the server's candidates is kept, so a
    response whose header lost its user still lands rather than becoming an orphan."""
    pick = _pick("PEVANS", 16, 1833, _t("04:30:04.580", 12), "100604")
    resp = _e("response", _t("04:30:05.378", 12), 1922, thread=9, user=None, fields={"response": "4.0"})
    groups = dt._group([*pick, resp])
    assert len(groups) == 1 and _response_of(groups[0]) == "4.0"


# ============================================================ C. two POSTs four milliseconds apart
RECEIPT = {"OrderNumber": "1002215", "OrderLine": "53", "ItemNumber": "104600", "ReceivingNumber": "20787001"}


def _case_c():
    """10 Sep 06:18. OPRACHASUK's ReceiptPO URL on thread 16, JBURCH's ConfirmPickLine URL on
    thread 27 four ms later, then each body on its own thread."""
    T = lambda s: _t(s, day=10)  # noqa: E731
    return [
        _e("request", T("06:18:20.423"), 764, thread=16, user="OPRACHASUK",
           fields={"url": "http://172.17.0.230/api/receiving/ReceiptPO"}),
        _e("request", T("06:18:20.427"), 765, thread=27, user="JBURCH",
           fields={"url": "http://172.17.0.230/api/picking/ConfirmPickLine"}),
        _e("request_body", T("06:18:20.436"), 766, thread=16, user="OPRACHASUK",
           fields={"User": "OPRACHASUK", "MethodName": "ReceiptPO", "TransactionType": "004001"}),
        _e("info", T("06:18:20.438"), 767, thread=16, user="OPRACHASUK", message="Calling MHS850MI - AddPOReceipt"),
        _e("mi_call", T("06:18:20.438"), 768, thread=16, user="OPRACHASUK",
           fields={"program": "MHS850MI", "transaction": "AddPOReceipt", "params": {"m3user": "OPRACHASUK"}}),
        _e("request_body", T("06:18:20.465"), 792, thread=27, user="JBURCH",
           fields={"User": "JBURCH", "MethodName": "ConfirmPickLine", "TransactionType": "002001"}),
        _e("mi_call", T("06:18:20.466"), 794, thread=27, user="JBURCH",
           fields={"program": "MMS060MI", "transaction": "LstBalID", "params": {"m3user": "JBURCH"}}),
        _e("mi_result", T("06:18:20.592"), 829, thread=27, user="JBURCH", result="OK",
           fields={"program": "MMS060MI", "transaction": "LstBalID", "result": "OK"}),
        _e("mi_call", T("06:18:20.593"), 842, thread=27, user="JBURCH",
           fields={"program": "MHS850MI", "transaction": "AddPickViaRepNo", "params": {"m3user": "JBURCH"}}),
        _e("mi_result", T("06:18:21.419"), 866, thread=27, user="JBURCH", result="OK",
           fields={"program": "MHS850MI", "transaction": "AddPickViaRepNo", "result": "OK"}),
        _e("info", T("06:18:21.427"), 879, thread=27, user="JBURCH", message="Activity Logged OK"),
        _e("response", T("06:18:21.428"), 880, thread=33, user="JBURCH", fields={"response": "2.0"}),
        _e("mi_result", T("06:18:22.065"), 881, thread=16, user="OPRACHASUK", result="OK",
           fields={"program": "MHS850MI", "transaction": "AddPOReceipt", "result": "OK"}),
        _e("info", T("06:18:22.244"), 944, thread=16, user="OPRACHASUK", message="API Returned OK"),
        _e("info", T("06:18:22.252"), 950, thread=16, user="OPRACHASUK", message="Activity Logged OK"),
        _e("response", T("06:18:22.252"), 951, thread=31, user="OPRACHASUK", fields={"response": RECEIPT}),
    ]


def test_c_each_body_takes_the_url_line_written_by_its_own_thread_and_user():
    groups = dt._group(_case_c())
    receipt = _with(groups, 766)
    pick = _with(groups, 792)
    assert 764 in _lines(receipt) and 765 in _lines(pick)
    assert receipt.compute()["method"] == "ReceiptPO"
    assert pick.compute()["method"] == "ConfirmPickLine"


def test_c_responses_follow_the_bodies_not_the_swapped_url_lines():
    groups = dt._group(_case_c())
    assert _response_of(_with(groups, 792)) == "2.0"
    assert _response_of(_with(groups, 766)) == RECEIPT
    assert _lines(_with(groups, 766)) == [764, 766, 767, 768, 881, 944, 950, 951]


def test_c_a_lone_post_still_pairs_with_the_only_pending_url_line():
    """One request, one body, nothing to disambiguate: identical to before."""
    T = lambda s: _t(s, day=10)  # noqa: E731
    groups = dt._group([
        _e("request", T("06:18:20.423"), 764, thread=16, user="OPRACHASUK",
           fields={"url": "http://172.17.0.230/api/receiving/ReceiptPO"}),
        _e("request_body", T("06:18:20.436"), 766, thread=16, user="OPRACHASUK",
           fields={"User": "OPRACHASUK", "MethodName": "ReceiptPO"}),
        _e("response", T("06:18:22.252"), 951, thread=31, user="OPRACHASUK", fields={"response": RECEIPT}),
    ])
    assert len(groups) == 1 and _lines(groups[0]) == [764, 766, 951]


def test_c_a_body_whose_url_line_has_no_user_still_pairs_by_recency():
    """A URL line logged before the context user is set has no user to match on. The old rule (most
    recent id-less URL line on the server) is the fallback, so nothing that paired before stops."""
    T = lambda s: _t(s, day=10)  # noqa: E731
    groups = dt._group([
        _e("request", T("06:18:20.423"), 764, thread=16, user=None,
           fields={"url": "http://172.17.0.230/api/receiving/ReceiptPO"}),
        _e("request_body", T("06:18:20.436"), 766, thread=16, user="OPRACHASUK",
           fields={"User": "OPRACHASUK", "MethodName": "ReceiptPO"}),
    ])
    assert _lines(_with(groups, 766)) == [764, 766]
