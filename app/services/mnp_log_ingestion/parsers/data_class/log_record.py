# log_record.py — Parser Output Contract (Pydantic, not a DB table)
#
#   LogRecord is the normalised output of a log parser: one record per *timestamped* entry
#   (multi-line bodies already folded in). It is the contract between Stage 1's parser and the
#   parse→insert step, which maps each LogRecord onto a log_entries row. Grouping into
#   transactions (Stage 2) happens later and is not the parser's concern.

from datetime import datetime

from pydantic import BaseModel, Field


class LogRecord(BaseModel):
    """One timestamped log entry, normalised. Maps 1:1 onto a log_entries row."""

    # --- ordering / provenance ---
    line_number: int = Field(..., description="Physical line where this entry starts (1-based)")
    timestamp: datetime | None = None
    raw_timestamp: str | None = None

    # --- header fields ---
    user: str | None = None          # normalised: (null)/()/empty -> None
    thread: str | None = None
    level: str | None = None         # INFO / DEBUG / ERROR
    logger: str | None = None        # dotted class, e.g. M3WebServiceClassLib.Managers.M3ItemManager
    method: str | None = None        # e.g. LogAPICall, MoveNext, <SendAsync>b__1

    # --- classification ---
    entry_type: str = "info"         # request/request_body/mi_call/mi_result/sql/response/info/error

    # --- M3 MI promoted fields ---
    mi_program: str | None = None    # e.g. MMS200MI
    mi_transaction: str | None = None  # e.g. LstItmAltUnitMs
    result_status: str | None = None  # "OK" or soft-error text
    record_count: int | None = None

    # --- content ---
    message: str = ""                # first-line text after " - "
    raw_body: str = ""               # full raw text of the entry (header line + continuation lines)
    fields: dict = Field(default_factory=dict)  # parsed inputs/outputs/records/params/body
