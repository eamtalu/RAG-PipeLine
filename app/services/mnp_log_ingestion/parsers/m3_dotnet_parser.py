# m3_dotnet_parser.py — Two-pass parser for the M3 WMS (.NET / log4net) server log
#
#   Pass 1: split the file into entries on timestamp-prefixed line starts. Every physical line
#           that does NOT start with a timestamp is a continuation of the current entry's body
#           (LogAPICall / LogAPIResult / stored-proc / REQUEST BODY blocks span many lines).
#   Pass 2: for each entry, parse the header, classify entry_type, and extract the M3 MI fields
#           (program / transaction / result / record count / inputs / outputs / records) plus
#           request params / response bodies into `fields`.
#
#   Line anatomy:
#     2026-05-19 13:42:33,362 (BECWHLO) [94] INFO  Logger.Class MethodName - message text
#     └ timestamp ─────────┘ └user┘ └thr┘ └lvl┘ └logger┘ └method┘   └ message ┘
#
#   User is normalised: (BECWHLO) -> "BECWHLO";  ((null)) / () -> None.

import json
import re
from datetime import datetime
from urllib.parse import parse_qs, urlsplit

from app.services.mnp_log_ingestion.parsers.base import BaseLogParser
from app.services.mnp_log_ingestion.parsers.data_class.log_record import LogRecord

# A new entry begins with "YYYY-MM-DD HH:MM:SS,mmm "
_TS_START = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} ")

# Full header: timestamp, (user), [thread], level, then the rest (logger method - message)
_HEADER = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) "
    r"\((?P<user>.*?)\) "
    r"\[(?P<thread>[^\]]*)\] "
    r"(?P<level>\w+)\s+"
    r"(?P<rest>.*)$"
)
# rest -> "logger method - message"  (logger & method are space-free tokens; split on FIRST " - ")
_REST = re.compile(r"^(?P<logger>\S+) (?P<method>\S+) - (?P<message>.*)$", re.DOTALL)

# mi_result message: "MI Program: MMS200MI  Transaction: LstItmAltUnitMs     Result: OK"
_MI_RESULT = re.compile(
    r"MI Program:\s*(?P<program>\S+)\s+Transaction:\s*(?P<transaction>\S+)\s+Result:\s*(?P<result>.*)$",
    re.DOTALL,
)
# mi_call body: "MI Program: MMS200MI  Transaction: LstItmAltUnitMs  NumberOfRecs: DEFAULT"
_MI_CALL = re.compile(
    r"MI Program:\s*(?P<program>\S+)\s+Transaction:\s*(?P<transaction>\S+)(?:\s+NumberOfRecs:\s*(?P<numrecs>\S+))?"
)

_NULL_USERS = {"", "(null)", "null"}


class M3DotNetLogParser(BaseLogParser):
    """Default parser for the Infor M3 WMS .NET server log format."""

    def parse(self, text: str) -> list[LogRecord]:
        entries = self._split_entries(text)
        return [self._build_record(header_line, body_lines, line_no) for header_line, body_lines, line_no in entries]

    # ---- Pass 1: split into (header_line, body_lines, line_number) ----
    @staticmethod
    def _split_entries(text: str) -> list[tuple[str, list[str], int]]:
        entries: list[tuple[str, list[str], int]] = []
        header_line: str | None = None
        body: list[str] = []
        start_line = 0

        for idx, line in enumerate(text.splitlines(), start=1):
            if _TS_START.match(line):
                if header_line is not None:
                    entries.append((header_line, body, start_line))
                header_line = line
                body = []
                start_line = idx
            else:
                # continuation of the current entry (ignore stray pre-header lines)
                if header_line is not None:
                    body.append(line)
        if header_line is not None:
            entries.append((header_line, body, start_line))
        return entries

    # ---- Pass 2: build a LogRecord from one entry ----
    def _build_record(self, header_line: str, body_lines: list[str], line_no: int) -> LogRecord:
        raw_body = "\n".join([header_line, *body_lines])
        body_text = "\n".join(body_lines)

        m = _HEADER.match(header_line)
        if not m:
            # Unparseable header — keep it as a raw info record so nothing is lost
            return LogRecord(line_number=line_no, entry_type="info", message=header_line, raw_body=raw_body)

        user = self._normalise_user(m.group("user"))
        level = m.group("level")
        rest = m.group("rest")

        logger = method = None
        message = rest
        rm = _REST.match(rest)
        if rm:
            logger = rm.group("logger")
            method = rm.group("method")
            message = rm.group("message")

        rec = LogRecord(
            line_number=line_no,
            timestamp=self._parse_ts(m.group("ts")),
            raw_timestamp=m.group("ts"),
            user=user,
            thread=m.group("thread") or None,
            level=level,
            logger=logger,
            method=method,
            message=message,
            raw_body=raw_body,
        )

        self._classify_and_extract(rec, level, message, body_text)
        return rec

    # ---- classification + field extraction ----
    def _classify_and_extract(self, rec: LogRecord, level: str, message: str, body_text: str) -> None:
        msg = message.strip()

        # 1. real ERROR-level failure wins
        if level == "ERROR":
            rec.entry_type = "error"
            rec.result_status = msg
            return

        # 2. request / response by message prefix
        if msg.startswith("REQUEST BODY:"):
            rec.entry_type = "request_body"
            rec.fields = self._extract_json_blob(message.split("REQUEST BODY:", 1)[1].strip())
            return
        if msg.startswith("REQUEST:"):
            rec.entry_type = "request"
            rec.fields = self._extract_request(message.split("REQUEST:", 1)[1].strip())
            return
        if msg.startswith("RESPONSE:"):
            rec.entry_type = "response"
            rec.fields = self._extract_json_blob(message.split("RESPONSE:", 1)[1].strip(), key="response")
            return

        # 3. M3 MI call / result by method
        if rec.method == "LogAPICall":
            rec.entry_type = "mi_call"
            self._extract_mi_call(rec, body_text)
            return
        if rec.method == "LogAPIResult":
            rec.entry_type = "mi_result"
            self._extract_mi_result(rec, message, body_text)
            return

        # 4. SQL stored procedure (has a body with the proc text)
        if "Stored Procedure Executed is" in msg:
            rec.entry_type = "sql"
            rec.fields = {"stored_procedure": body_text.strip()}
            return

        # 5. everything else is narration
        rec.entry_type = "info"

    # ---- helpers ----
    @staticmethod
    def _normalise_user(raw: str | None) -> str | None:
        if raw is None:
            return None
        v = raw.strip()
        if v.startswith("(") and v.endswith(")"):  # leftover parens from "((null))"
            v = v[1:-1].strip()
        return None if v.lower() in _NULL_USERS else (v or None)

    @staticmethod
    def _parse_ts(ts: str) -> datetime | None:
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S,%f")
        except ValueError:
            return None

    def _extract_mi_call(self, rec: LogRecord, body_text: str) -> None:
        cm = _MI_CALL.search(body_text)
        fields: dict = {}
        if cm:
            rec.mi_program = cm.group("program")
            rec.mi_transaction = cm.group("transaction")
            fields["program"] = cm.group("program")
            fields["transaction"] = cm.group("transaction")
            if cm.group("numrecs"):
                fields["number_of_recs"] = cm.group("numrecs")
        url = self._find_line_value(body_text, "URL:")
        if url:
            fields["url"] = url
            fields["params"] = self._flat_qs(url)
        inputs = self._extract_block(body_text, "Inputs:", ("Outputs:",))
        if inputs:
            fields["inputs"] = inputs
        outputs = self._extract_output_names(body_text)
        if outputs:
            fields["outputs"] = outputs
        rec.fields = fields

    def _extract_mi_result(self, rec: LogRecord, message: str, body_text: str) -> None:
        rm = _MI_RESULT.search(message)
        fields: dict = {}
        if rm:
            rec.mi_program = rm.group("program")
            rec.mi_transaction = rm.group("transaction")
            rec.result_status = rm.group("result").strip()
            fields["program"] = rm.group("program")
            fields["transaction"] = rm.group("transaction")
            fields["result"] = rec.result_status
        records = self._extract_records(body_text)
        rec.record_count = len(records)
        if records:
            fields["records"] = records
        rec.fields = fields

    # request URL -> {url, method_name, params}
    def _extract_request(self, raw: str) -> dict:
        fields: dict = {"url": raw}
        params = self._flat_qs(raw)
        if params:
            fields["params"] = params
        return fields

    @staticmethod
    def _flat_qs(url: str) -> dict:
        try:
            qs = urlsplit(url).query
            if not qs and "?" in url:
                qs = url.split("?", 1)[1]
            return {k: v[0] if len(v) == 1 else v for k, v in parse_qs(qs, keep_blank_values=True).items()}
        except Exception:
            return {}

    @staticmethod
    def _extract_json_blob(raw: str, key: str | None = None) -> dict:
        raw = raw.strip()
        try:
            parsed = json.loads(raw)
            return {key: parsed} if key else (parsed if isinstance(parsed, dict) else {"value": parsed})
        except Exception:
            return {key or "raw": raw}

    @staticmethod
    def _find_line_value(body_text: str, prefix: str) -> str | None:
        for line in body_text.splitlines():
            s = line.strip()
            if s.startswith(prefix):
                return s[len(prefix):].strip()
        return None

    @staticmethod
    def _extract_block(body_text: str, start: str, ends: tuple[str, ...]) -> dict:
        """Parse a 'KEY : VALUE' block that begins after `start` and ends before any `ends`."""
        out: dict = {}
        collecting = False
        for line in body_text.splitlines():
            s = line.strip()
            if not collecting:
                if s.startswith(start):
                    collecting = True
                continue
            if any(s.startswith(e) for e in ends):
                break
            mkv = re.match(r"^(\w+)\s*:\s*(.*)$", s)
            if mkv:
                out[mkv.group(1)] = mkv.group(2).strip()
        return out

    @staticmethod
    def _extract_output_names(body_text: str) -> list[str]:
        names: list[str] = []
        collecting = False
        for line in body_text.splitlines():
            s = line.strip()
            if not collecting:
                if s.startswith("Outputs:"):
                    collecting = True
                continue
            if not s or ":" in s:  # next labelled section or blank -> stop
                break
            names.append(s)
        return names

    @staticmethod
    def _extract_records(body_text: str) -> list[dict]:
        """Split a LogAPIResult body into Record blocks of KEY = VALUE pairs."""
        records: list[dict] = []
        current: dict | None = None
        for line in body_text.splitlines():
            s = line.strip()
            if re.match(r"^Record:\s*\d+", s):
                if current is not None:
                    records.append(current)
                current = {}
                continue
            if current is not None:
                kv = re.match(r"^(\w+)\s*=\s*(.*)$", s)
                if kv:
                    current[kv.group(1)] = kv.group(2).strip()
        if current is not None:
            records.append(current)
        return records
