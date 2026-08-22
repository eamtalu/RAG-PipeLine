"""The `synthetic-load` tenant generator. Phase 0, F11.

    Fix: a dedicated `synthetic-load` tenant whose generator drives Stage 2 normally, so tickets and
    rebuilds are genuinely exercised. Excluded from every production read path and from alerting.
    It also produces the defect fixtures, so correctness and load share one harness.

One harness, two jobs. The exit criterion in Phase 7 needs 100x the measured rate -- roughly 78,000
records an hour at the 50,000-pick target -- and there was no described way to produce that without
polluting real data. The same generator emits the named defect scenarios, so a fixture and a load test
are the same code path rather than two things that can disagree.

**It emits real M3 log TEXT, not fact rows.** That is the whole point. Anything that fabricated
`log_transactions` directly would prove nothing about tickets, about rebuilds, or about the parser: it
would test the analytics layer against a mock of the pipeline it is supposed to read. Generated text
goes through `M3DotNetLogParser`, Stage 1's `entry_hash` dedup, the `log_regroup_pending` ticket and
Stage 2's real grouping, exactly as a file pulled off a WMS server does.

The line templates below are transcribed from the live server on 2026-08-21, not invented. A
`ConfirmPickLine` is a POST, so its measurable fields arrive in the REQUEST BODY's JSON rather than in
the URL query string, and `MethodName` and `QuantityPicked` are body keys. Getting that wrong would
produce text that parses into transactions with no method and no quantity, and every downstream
assertion would then be vacuously true.

Isolation is enforced here rather than left to the caller: `TENANT` is a constant, every entry point
checks it, and the tenant is created with `notifications_enabled=False` so `tenant_gate` excludes it
from alerting. Production reads are already tenant-scoped by the `X-Customer-Code` header, so the only
way to see this data is to ask for it by name.
"""

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

#: The one tenant this module may ever write to. A constant, not a parameter: a generator that could be
#: pointed at a real customer_code is one keystroke away from writing 78,000 synthetic picks into
#: production data, and no amount of care at the call site would make that safe.
TENANT = "synthetic-load"

#: Transcribed from a real line, 2026-08-05 18:59:43,915. The header layout is
#: `timestamp (user) [thread] LEVEL logger method - message`, and the parser's `_HEADER` requires every
#: one of those parts: a template missing the closing bracket of `[thread]` yields a NULL timestamp and
#: silently lands in the DEFAULT partition.
_HEADER = "{ts} ({user}) [{thread}] DEBUG {logger} {method} - {message}"
_TS_FORMAT = "%Y-%m-%d %H:%M:%S,%f"

_API_LOG = "Server.CommonCode.ApiLogHandler"


def _stamp(at: datetime) -> str:
    """`2026-08-05 18:59:43,915`. Milliseconds, not microseconds: `_TS_START` matches exactly three
    digits, so a six-digit fraction is not recognised as the start of an entry at all."""
    return at.strftime(_TS_FORMAT)[:-3]


def _line(at: datetime, user: str, thread: int, logger: str, method: str, message: str) -> str:
    return _HEADER.format(ts=_stamp(at), user=user, thread=thread,
                          logger=logger, method=method, message=message)


@dataclass(frozen=True)
class Pick:
    """One ConfirmPickLine to emit. Defaults are the shapes the live server actually produces."""

    at: datetime
    quantity: str = "10.0"
    user: str = "SYNTH"
    thread: int = 33
    item: str = "101978"
    order: str = "1000006835"
    warehouse: str = "BRI"
    from_location: str = "H01A"
    to_location: str = "BRI08-P"
    lot: str = "2608031215"
    #: Emitted only when True. A pick with no RESPONSE is the `incomplete` fixture, and it is the row
    #: that is still due to move: measured, sealed rows averaged 1.7h between their newest entry and
    #: being written.
    responded: bool = True
    #: Emitted as an ERROR-level line instead of a normal result, for the `error` fixture.
    failed: bool = False
    req_id: str | None = None


def _body(p: Pick) -> str:
    """The REQUEST BODY JSON. Key order and spelling matter: `MethodName` is how the parser names the
    transaction, and `QuantityPicked` is the only field the consumption metric measures.

    `QuantityToBePicked` is included and left empty on purpose, because that is what the live server
    sends (91% empty) and it is the field a careless implementation would measure instead.
    """
    return json.dumps({
        "Company": "915", "Warehouse": p.warehouse, "PickListSuffix": "1",
        "TransactionType": "002001", "User": p.user,
        "ReportingNumber": "298495", "QuantityToBePicked": "",
        "ReqId": p.req_id or f"20-{_stamp(p.at)}-{uuid.uuid4().hex[:4]}",
        "DeviceLocale": "en-GB", "OrderNumber": p.order, "WarehouseID": "1",
        "FromLocation": p.from_location, "ItemNumber": p.item,
        "StockTransactionType": "31", "OrderLine": "9", "UserID": "12",
        "ApiPort": "443", "Division": "TMP",
        "MethodName": "ConfirmPickLine", "QuantityPicked": p.quantity,
        "Partner": "BEC", "MessageType": "WMS", "LotNumber": p.lot,
        "PackagingType": "CARBOX", "ToLocation": p.to_location, "PackingMode": "false",
    }, separators=(",", ":"))


def pick_lines(p: Pick) -> list[str]:
    """One ConfirmPickLine transaction as real log lines, in the order the server emits them.

    Shape taken from a live transaction: REQUEST, REQUEST BODY, then the MI work, then RESPONSE. The
    work lines are not decoration -- they are what makes the transaction span more than an instant, so
    that grouping, sealing and the gap rule are all genuinely exercised.
    """
    t = p.at
    out = [
        _line(t, p.user, p.thread, _API_LOG, "MoveNext",
              "REQUEST: http://172.17.0.230/api/picking/ConfirmPickLine"),
        _line(t + timedelta(milliseconds=5), p.user, p.thread, _API_LOG, "MoveNext",
              f"REQUEST BODY: {_body(p)}"),
        _line(t + timedelta(milliseconds=40), p.user, p.thread,
              "M3WebServiceClassLib.Managers.M3PickManager", "AddPickViaReportingNumber",
              "Calling MHS850MI - AddPickViaRepNo"),
        _line(t + timedelta(milliseconds=45), p.user, p.thread,
              "M3WebServiceClassLib.Managers.M3ApiManager", "LogAPICall",
              "Calling WebService: >>\nMI Program: MHS850MI  Transaction: AddPickViaRepNo"),
    ]
    if p.failed:
        # ERROR level wins over everything in the classifier, which is what makes status `error`.
        out.append(_HEADER.format(
            ts=_stamp(t + timedelta(milliseconds=60)), user=p.user, thread=p.thread,
            logger="M3WebServiceClassLib.Managers.M3ApiManager", method="LogAPIResult",
            message="AddPickViaRepNo failed: location empty").replace("DEBUG", "ERROR"))
    else:
        out.append(_line(t + timedelta(milliseconds=60), p.user, p.thread,
                         "M3WebServiceClassLib.Managers.M3ApiManager", "LogAPIResult",
                         "MI Program: MHS850MI  Transaction: AddPickViaRepNo     Result: OK"))
    if p.responded:
        out.append(_line(t + timedelta(milliseconds=80), "(null)", p.thread, _API_LOG,
                         "<SendAsync>b__1", f'RESPONSE: "{p.quantity}"'))
    return out


def render(picks) -> str:
    """A whole log file. Lines are emitted in the order given, which is how out-of-order and
    late-arriving data are expressed: the parser and Stage 2 read in timestamp order regardless, so a
    caller can hand over a deliberately shuffled sequence."""
    lines: list[str] = []
    for p in picks:
        lines.extend(pick_lines(p))
    return "\n".join(lines) + "\n"


# ============================================================== named scenarios
def _base(at: datetime | None = None) -> datetime:
    """A fixed instant by default, so generated text is byte-stable and a test can assert on it.
    Callers doing load pass a real clock."""
    return at or datetime(2026, 8, 5, 18, 59, 43, 915000, tzinfo=timezone.utc)


def scenario(name: str, at: datetime | None = None) -> str:
    """One named defect scenario as log text.

    A registry rather than an if-chain, for the same reason the metric registry is: adding a scenario
    is a dict entry, and every caller stays generic.
    """
    try:
        return _SCENARIOS[name](_base(at))
    except KeyError:
        raise KeyError(f"unknown scenario {name!r}; known: {', '.join(sorted(_SCENARIOS))}") from None


def scenario_names() -> tuple[str, ...]:
    return tuple(sorted(_SCENARIOS))


_SCENARIOS = {
    # A full pick. The control case: everything downstream should see one transaction, ten units.
    "full pick": lambda t: render([Pick(at=t, quantity="10.0")]),

    # F8. Zero units, still reported as success. 8.3% of live confirmations.
    "zero pick": lambda t: render([Pick(at=t, quantity="0.0")]),

    # Fractional, because 0.333333 is a real live value and float would drift on it.
    "fractional pick": lambda t: render([Pick(at=t, quantity="0.333333")]),

    # No RESPONSE line: status incomplete, quantity unknown rather than zero.
    "incomplete": lambda t: render([Pick(at=t, responded=False)]),

    # An ERROR-level line: status error, and it must reach no quantity counter.
    "error": lambda t: render([Pick(at=t, failed=True)]),

    # Two picks by different users interleaved on ONE thread. The .NET server reuses a thread
    # mid-request, so this is the case `_group` keys on (thread, user) to survive; a generator that
    # only ever emitted one user at a time would never exercise it.
    "interleaved users": lambda t: render([
        Pick(at=t, user="SYNTHA", thread=33, quantity="2.0"),
        Pick(at=t + timedelta(milliseconds=20), user="SYNTHB", thread=33, quantity="3.0"),
    ]),

    # Two picks far enough apart that the open-gap rule (log_open_gap_seconds, 300s) closes the first
    # before the second starts. Two transactions, not one.
    "beyond the open gap": lambda t: render([
        Pick(at=t, quantity="2.0"),
        Pick(at=t + timedelta(seconds=400), quantity="3.0"),
    ]),

    # Same line confirmed twice, the second time for a different amount. Only the picked amounts
    # compose; ExpectedQuantity is mutable per instruction and is not a measure.
    "multi-confirmation": lambda t: render([
        Pick(at=t, quantity="0.333333", order="1000006835"),
        Pick(at=t + timedelta(seconds=5), quantity="0.666667", order="1000006835"),
    ]),
}
