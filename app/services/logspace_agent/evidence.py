"""The logspace agent's evidence table (chunk 132): drawn from the rows a tool returned, never
typed by the model. A trace or a listing shows the records the answer rests on, each with its
request id linked to the explorer; an aggregate shows its groups in the order the server sorted."""

from __future__ import annotations

import json
import re

from app.services.analytics_agent.evidence import _cell

MAX_ROWS = 15
_TABLE_LINE = re.compile(r"^\s*\|.*\|\s*$", re.M)

_ROW_COLUMNS = [("time", "time"), ("method", "method"), ("status", "status"), ("user", "user"),
                ("delivery_number", "delivery"), ("item_number", "item"), ("QuantityPicked", "picked"),
                ("ExpectedQuantity", "expected"), ("FromLocation", "from")]


def _esc(value) -> str:
    return _cell(value).replace("|", "\\|").replace("\n", " ")


def _request(row: dict) -> str:
    rid = row.get("reqid") or row.get("id", "")[:8]
    return f"[{_esc(rid)}]({row['link']})" if row.get("link") else _esc(rid)


def _rows_table(rows: list[dict], total: int) -> str:
    cols = [(k, label) for k, label in _ROW_COLUMNS if any(r.get(k) not in (None, "") for r in rows)]
    head = "| " + " | ".join([label for _, label in cols] + ["request"]) + " |"
    rule = "|" + "---|" * (len(cols) + 1)
    body = ["| " + " | ".join([_esc(r.get(k)) for k, _ in cols] + [_request(r)]) + " |" for r in rows[:MAX_ROWS]]
    more = f"\n\n{len(rows[:MAX_ROWS])} of {total} records shown." if total > MAX_ROWS else ""
    return "\n".join([head, rule, *body]) + more


def _aggregate_table(result: dict) -> str:
    """Groups in the server's order; an errors column or a sum with nothing but zeros is left out."""
    rows = result["rows"][:MAX_ROWS]
    dims = list(result.get("group_by") or [])
    measures = ["count"] + [k for k in (rows[0] if rows else {}) if k == "errors" or k.startswith("sum:")]
    measures = [m for m in measures if m == "count" or any(r.get(m) not in (None, 0) for r in rows)]
    head = "| " + " | ".join([d.replace("attr:", "").replace("_", " ") for d in dims] +
                             [m[4:] if m.startswith("sum:") else m for m in measures]) + " |"
    rule = "|" + "---|" * (len(dims) + len(measures))
    body = ["| " + " | ".join([_esc(r.get(d)) for d in dims] + [_esc(r.get(m)) for m in measures]) + " |" for r in rows]
    return "\n".join([head, rule, *body])


def render(trace: list[dict]) -> str | None:
    """A markdown table from the last useful listing, trace or grouped aggregate, or None."""
    for t in reversed(trace):
        if t["tool"] not in ("trace", "find_transactions", "aggregate"):
            continue
        try:
            result = json.loads(t.get("result") or "")
        except (TypeError, ValueError):
            continue
        if not isinstance(result, dict) or "error" in result:
            continue
        if t["tool"] == "aggregate":
            if result.get("group_by") and result.get("rows"):
                return _aggregate_table(result)
            continue
        rows = result.get("transactions") or []
        if rows:
            return _rows_table(rows, int(result.get("total") or len(rows)))
    return None


def strip_own_tables(text: str) -> str:
    """The model's own markdown tables removed (its bullet points, the advice, stay)."""
    return re.sub(r"\n{3,}", "\n\n", _TABLE_LINE.sub("", text)).strip()
