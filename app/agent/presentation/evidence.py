"""Chunk 125: the answer is held to the data, in code.

Two checks that no model can talk past, both PURE (text and tool results in, text out):

- `ungrounded(answer, results, question)`: every figure in the answer must appear in a tool result
  (or in the question, or be a percentage the model divided itself). A figure that appears nowhere
  was invented, and the answer is withheld with the figures named.
- `render(trace)`: for a "top N" question the ranked table is rendered from the tool's own rows,
  sorted as the server sorted them, and the model's table is dropped. Twice on the live tenant an
  8B model re-ordered a sorted result or left rows out while copying it; the rows it copied from
  are the answer.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation

#: A figure in prose: 1,200 · -809 · 92.58 · 11.5% · 843s. Not a date, a time or a key (handled below).
#: It never ends on a comma ("2026," is 2026) and a full stop that ends the sentence does not hide
#: it ("short by 5,999." is checked); a dot followed by more characters (1.2.3, 10.0.0.1) still does.
_NUMBER = re.compile(r"(?<![\w.\-/:])[-−]?\d(?:[\d,]*\d)?(?:\.\d+)?%?(?![\w\-/:]|\.\w)")
_MONTH = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*"
#: The year of a written date ("16 November 2026", "November 16, 2026", "November 2026"), removed
#: whole so the year is not read as a figure; a year on its own ("in 2026") stays a figure.
_WRITTEN_DATE = re.compile(
    rf"\b(?:\d{{1,2}}(?:st|nd|rd|th)?\s+{_MONTH}|{_MONTH}\s+\d{{1,2}}(?:st|nd|rd|th)?|{_MONTH}),?\s+(?:19|20)\d{{2}}\b", re.I)
_DATE_OR_TIME = re.compile(r"\d{4}-\d{2}-\d{2}|\d{1,2}:\d{2}(?::\d{2})?|\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*", re.I)
_ORDINAL = re.compile(r"^\s*\d+\.\s", re.M)
_TABLE_LINE = re.compile(r"^\s*\|.*\|\s*$", re.M)
_LIST_LINE = re.compile(r"^\s*(?:\d+\.|[-*•])\s+.*$", re.M)
_RESULT_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")
#: A thousands-grouped number inside a tool result ("777 – 1,042"), read whole for the guard's pool.
_GROUPED_NUMBER = re.compile(r"(?<![\d,])-?\d{1,3}(?:,\d{3})+(?:\.\d+)?(?![\d,])")

SMALL = 31  # a bare integer up to this is a count, an ordinal, a day or a "top N": never withheld


def _decimal(token: str) -> Decimal | None:
    try:
        return Decimal(token.replace(",", "").replace("−", "-").rstrip("%"))
    except InvalidOperation:
        return None


def figures(text: str) -> list[str]:
    """The figures a reader would take as data: dates, times, list ordinals and table rules removed.

    >>> figures("On 16 November 2026, 957 lines; short by 5,999.")
    ['957', '5,999']
    >>> figures("Build 1.2.3 on 10.0.0.1, in 2026")
    ['2026']
    """
    cleaned = _WRITTEN_DATE.sub(" ", text)
    cleaned = _DATE_OR_TIME.sub(" ", cleaned)
    cleaned = _ORDINAL.sub(" ", cleaned)
    cleaned = re.sub(r"\|[-: ]+\|", " ", cleaned)
    return [m.group(0) for m in _NUMBER.finditer(cleaned)]


def numbers_in_results(results: list[str]) -> set[Decimal]:
    """Every number any tool returned, as a Decimal, so 110.666667 and "110.67" can meet; a
    thousands-grouped number in a text value counts whole as well as by its parts.

    >>> sorted(n for n in numbers_in_results(['{"range": "777 to 1,042"}']) if n > 0)
    [Decimal('1'), Decimal('42'), Decimal('777'), Decimal('1042')]
    """
    out: set[Decimal] = set()
    for text in results:
        # Step 1: the parts as before, so nothing that was grounded stops being grounded
        tokens = [m.group(0) for m in _RESULT_NUMBER.finditer(text)]
        # Step 2: "1,042" whole too, otherwise a figure a tool wrote for people is never grounded
        tokens += [m.group(0) for m in _GROUPED_NUMBER.finditer(text)]
        for token in tokens:
            d = _decimal(token)
            if d is not None:
                out.add(d)
                out.add(-d)
    return out


def _grounded(token: str, pool: set[Decimal], question_numbers: set[Decimal]) -> bool:
    if token.endswith("%"):
        return True  # a share the model divided; the two sums it divided are checked on their own
    d = _decimal(token)
    if d is None:
        return True
    if d in pool or d in question_numbers:
        return True
    if d == d.to_integral_value() and abs(d) <= SMALL:
        return True
    # rounding: the answer's precision applied to a result number
    places = -d.as_tuple().exponent if d.as_tuple().exponent < 0 else 0
    quant = Decimal(1).scaleb(-places)
    for p in pool:
        try:
            if p.quantize(quant) == d:
                return True
        except InvalidOperation:
            continue
    return False


def ungrounded(answer: str, results: list[str], question: str = "") -> list[str]:
    """The figures in `answer` that no tool result and no part of the question contains."""
    pool = numbers_in_results(results)
    asked = {d for d in (_decimal(t) for t in figures(question)) if d is not None}
    seen: list[str] = []
    for token in figures(answer):
        if not _grounded(token, pool, asked) and token not in seen:
            seen.append(token)
    return seen


_SHARE = re.compile(r"(?<![\w.])\d[\d,]*(?:\.\d+)?%(?![\w])")


def stale_markers(text: str, given: list[str]) -> list[str]:
    """The dates, times and shares in `text` that no `given` text contains, in order of appearance:
    the marks of an answer copied from an earlier turn. `ungrounded` exempts all three on purpose,
    which is right for a draft backed by a tool call and wrong for one backed by nothing, where
    "as of 14:09 UTC, about 73% provisional" can only have been copied.

    >>> stale_markers("As of 14:09 on 2026-11-11, 20 releases, 73% provisional.", [])
    ['14:09', '2026-11-11', '73%']
    >>> stale_markers("Ready before the 11:30 departure, within 20 min.", ["before the 11:30 "])
    []
    """
    found = [(m.start(), m.group(0)) for m in _DATE_OR_TIME.finditer(text)]
    found += [(m.start(), m.group(0)) for m in _SHARE.finditer(text)]
    seen: list[str] = []
    for _, token in sorted(found):
        if not any(token in g for g in given) and token not in seen:
            seen.append(token)
    return seen


# ============================================================== the table from the rows

def _label(name: str) -> str:
    if name.startswith("lookup:"):
        return name.split(".")[-1].replace("_", " ")
    return name.replace("_", " ")


def _cell(value) -> str:
    """A cell as a person reads it: whole numbers plain, fractions to two decimals (683.108138 is
    noise on a card), text as it is."""
    if value is None:
        return ""
    if isinstance(value, str):
        d = _decimal(value)
        if d is not None:
            if d == d.to_integral_value():
                return str(int(d))
            return format(d.quantize(Decimal("0.01")).normalize(), "f")
        return value
    return str(value)


#: The most fact bullets an answer keeps beside a rendered table (step 22); a longer list is a dump
#: of the rows the table already shows.
MAX_FACT_BULLETS = 4
_SUB_POINT = re.compile(r"^\s{2,}(?:\d+\.|[-*•])\s+")


def _table_rows(table: str | None) -> list[list[str]]:
    """The data rows of a rendered markdown table, as cell lists; the header and the rule left out.

    >>> _table_rows("From the data: x\\n\\n| a | b |\\n|---|---|\\n| GINGER | 125 |")
    [['GINGER', '125']]
    """
    lines = [ln for ln in (table or "").split("\n") if _TABLE_LINE.match(ln)]
    rows = [[c.strip() for c in ln.strip().strip("|").split("|")] for ln in lines
            if not re.fullmatch(r"[|\s:-]+", ln.strip())]
    return rows[1:] if rows else []


#: A bullet with more than this many words of its own besides a row's cells says something the
#: table does not, so it is a fact, not a copy.
MAX_COPY_WORDS = 3
_WORD = re.compile(r"[A-Za-z][A-Za-z'-]+")


def _copies_a_row(line: str, rows: list[list[str]]) -> bool:
    """Whether a bullet only repeats a table row: every cell of a row of up to three cells, or all but
    one cell of a longer row, appears in it (a number by its value, a word as written), and it has
    almost no words of its own besides them.

    ("- GINGER 2 125", [["GINGER", "2", "125"]]) -> True ;
    ("- GINGER, 125 short on 2 releases, the worst by far", same) -> False
    """
    numbers = {d for d in (_decimal(f) for f in _RESULT_NUMBER.findall(line)) if d is not None}
    lowered = line.lower()
    for row in rows:
        cells = [c for c in row if c]
        if not cells:
            continue
        hits = 0
        for cell in cells:
            value = _decimal(cell)
            if value is not None:
                hits += value in numbers
            else:
                hits += cell.lower() in lowered
        if hits < (len(cells) if len(cells) <= 3 else len(cells) - 1):
            continue
        cell_words = {w.lower() for c in cells for w in _WORD.findall(c)}
        own = [w for w in _WORD.findall(line) if w.lower() not in cell_words]
        if len(own) <= MAX_COPY_WORDS:
            return True
    return False


def strip_tables(text: str, table: str | None = None) -> str:
    """The model's own markdown tables removed, and with them the lists that only repeat the rendered
    table's rows: a list of more than four points, or a bullet that copies a whole row. Up to four
    short fact bullets stay, with their sub-points (step 22: the owner asked for one bullet per fact).

    >>> strip_tables("a\\n- GINGER 2 125\\n- the night shift slipped\\nb", "| item | n | short |\\n|---|---|---|\\n| GINGER | 2 | 125 |")
    'a\\n- the night shift slipped\\nb'
    """
    rows = _table_rows(table)
    lines = text.split("\n")
    kept: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if _TABLE_LINE.match(line):
            i += 1
            continue
        if not _LIST_LINE.match(line):
            kept.append(line)
            i += 1
            continue
        # Step 1: Gather the whole list (its points and their sub-points), because the rule is about
        # the list: more than four points is a dump of the rows the table already shows
        j = i
        while j < len(lines) and _LIST_LINE.match(lines[j]):
            j += 1
        run = lines[i:j]
        points = [ln for ln in run if not _SUB_POINT.match(ln)]
        # Step 2: Keep a short list, without the bullets that copy a whole row of the table
        if len(points) <= MAX_FACT_BULLETS:
            kept.extend(ln for ln in run if not _copies_a_row(ln, rows))
        i = j
    return re.sub(r"\n{3,}", "\n\n", "\n".join(kept)).strip()


DEFAULT_ROWS = 10
MAX_ROWS = 25


#: Tools whose result carries its own `table` ({title, columns, rows, facts}): drawn as it is.
SHAPED_TABLE_TOOLS = ("at_risk_history", "at_risk_board", "at_risk_vans")


def _shaped(trace: list[dict]) -> dict | None:
    """The last at-risk table in the trace, when it is the latest table-bearing call."""
    for t in reversed(trace):
        if t["tool"] in ("aggregate_releases", "list_releases"):
            return None
        if t["tool"] not in SHAPED_TABLE_TOOLS:
            continue
        try:
            result = json.loads(t.get("result") or "")
        except (TypeError, ValueError):
            continue
        table = result.get("table") if isinstance(result, dict) else None
        if "error" in result or not table or not table.get("rows"):
            continue
        return table
    return None


def _shaped_markdown(table: dict, max_rows: int) -> str:
    columns = table["columns"]
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for r in table["rows"][:max_rows]:
        lines.append("| " + " | ".join(str(c) for c in r) + " |")
    facts = " · ".join(f"{k}: {v}" for k, v in (table.get("facts") or {}).items())
    caption = f"From the data: {table.get('title', '')}" + (f" · {facts}" if facts else "") + f" · {min(len(table['rows']), max_rows)} of {len(table['rows'])} row(s)."
    return caption + "\n\n" + "\n".join(lines)


def render(trace: list[dict], max_rows: int | None = None) -> str | None:
    """A markdown table from the last sorted aggregate or the last listing in the trace, or from the
    last at-risk tool that shaped its own table, or None. Rows shown: the limit the model asked the
    tool for, else 10, never more than 25."""
    shaped = _shaped(trace)
    if shaped is not None:
        return _shaped_markdown(shaped, max_rows or MAX_ROWS)
    for t in reversed(trace):
        if t["tool"] not in ("aggregate_releases", "list_releases"):
            continue
        try:
            result = json.loads(t.get("result") or "")
        except (TypeError, ValueError):
            continue
        if "error" in result or not result.get("rows"):
            continue
        asked = (t.get("input") or {}).get("limit")
        try:
            n = max_rows or min(int(asked), MAX_ROWS) if asked else (max_rows or DEFAULT_ROWS)
        except (TypeError, ValueError):
            n = max_rows or DEFAULT_ROWS
        if t["tool"] == "aggregate_releases":
            if not (t.get("input") or {}).get("sort") and not _is_trend(result):
                continue
            return _aggregate_table(result, n if (t.get("input") or {}).get("sort") else MAX_ROWS)
        return _list_table(result, n)
    return None


def _is_trend(result: dict) -> bool:
    """Grouped by a time bucket: a table of every bucket in time order is the answer."""
    return any(g in ("day", "hour", "hour_start", "week", "business_date") for g in result.get("group_by") or [])


def _aggregate_table(result: dict, max_rows: int) -> str:
    dims = [_label(g) for g in result.get("group_by") or []]
    sort = result.get("sort") or {}
    by, direction = sort.get("by", "rows"), sort.get("dir", "desc")
    # Few columns on purpose: a Teams card is narrow and a customer name is long. The dimensions,
    # the release count, and the one column the rows are sorted by.
    columns = list(dims) + ["releases"]
    key = None
    if by == "units_short":
        columns.append("units short")
        key = "shortfall"
    elif by not in ("rows", "calls", *(result.get("group_by") or [])) and by:
        columns.append(_label(by))
        key = by
    show_deliveries = key is None and any("deliveries" in r for r in result["rows"])
    if show_deliveries:
        columns.append("deliveries")
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for r in result["rows"][:max_rows]:
        cells = [_cell(d) for d in r.get("dimensions", [])] + [_cell(r.get("rows"))]
        if show_deliveries:
            cells.append(_cell(r.get("deliveries")))
        if key:
            v = r.get(key)
            if by == "units_short" and v is not None:
                d = _decimal(str(v))
                v = _cell(str(-d)) if d is not None else _cell(v)
            cells.append(_cell(v))
        lines.append("| " + " | ".join(cells) + " |")
    window = result.get("window") or {}
    shown = min(len(result["rows"]), max_rows)
    caption = (f"From the data: {shown} of {len(result['rows'])} group(s), sorted by {_label(by)} {direction}"
               f"{', ' + window['start'][:10] + ' to ' + window['end'][:10] if window else ''}"
               f"; {result.get('total_rows', '')} releases across the groups returned.")
    return caption + "\n\n" + "\n".join(lines)


def _list_table(result: dict, max_rows: int) -> str:
    columns = ["release", "picker", "item", "description", "expected", "picked", "duration s"]
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for r in result["rows"][:max_rows]:
        a = r.get("attributes") or {}
        looked = r.get("looked_up") or {}
        lines.append("| " + " | ".join([
            " · ".join(r.get("key") or []), r.get("user_name") or "", r.get("item_number") or "",
            looked.get("item description.ItemDescription") or "", _cell(a.get("expected")), _cell(a.get("picked")),
            _cell(a.get("duration_s"))]) + " |")
    total = result.get("total")
    caption = f"From the data: {min(len(result['rows']), max_rows)} of {total} matching release(s)."
    return caption + "\n\n" + "\n".join(lines)


# ============================================================== the same rows, as data for a card

def structured(trace: list[dict], max_rows: int | None = None, *, link: str | None = None,
               at_risk_link: str | None = None) -> dict | None:
    """The evidence as DATA, for a client that draws its own table: Teams renders an Adaptive Card
    Table from this, with widths and right-aligned numbers, instead of guessing from markdown.

    {"title", "columns": [{"name", "align"}], "rows": [[str, …]], "facts": {…}, "link"}
    Same source and same rows as `render`, so the text and the card never disagree."""
    shaped = _shaped(trace)
    if shaped is not None:
        n = max_rows or MAX_ROWS
        numeric = {"deliveries", "held", "missed", "lines", "packages"}
        out = {"title": shaped.get("title", ""), "columns": [{"name": c, "align": "right" if c in numeric else "left"} for c in shaped["columns"]],
               "rows": [[str(c) for c in r] for r in shaped["rows"][:n]], "facts": dict(shaped.get("facts") or {})}
        if at_risk_link:
            out["link"] = at_risk_link
        return out
    for t in reversed(trace):
        if t["tool"] not in ("aggregate_releases", "list_releases"):
            continue
        try:
            result = json.loads(t.get("result") or "")
        except (TypeError, ValueError):
            continue
        if "error" in result or not result.get("rows"):
            continue
        asked = (t.get("input") or {}).get("limit")
        try:
            n = max_rows or min(int(asked), MAX_ROWS) if asked else (max_rows or DEFAULT_ROWS)
        except (TypeError, ValueError):
            n = max_rows or DEFAULT_ROWS
        if t["tool"] == "aggregate_releases":
            if not (t.get("input") or {}).get("sort") and not _is_trend(result):
                continue
            out = _aggregate_data(result, n if (t.get("input") or {}).get("sort") else MAX_ROWS)
        else:
            out = _list_data(result, n)
        if link:
            out["link"] = link
        return out
    return None


def _window_words(result: dict) -> str | None:
    window = result.get("window") or {}
    if not window:
        return None
    return f"{window['start'][:10]} to {window['end'][:10]}"


def _aggregate_data(result: dict, max_rows: int) -> dict:
    dims = [_label(g) for g in result.get("group_by") or []]
    sort = result.get("sort") or {}
    by, direction = sort.get("by", "rows"), sort.get("dir", "desc")
    columns = [{"name": d, "align": "left"} for d in dims] + [{"name": "releases", "align": "right"}]
    key = None
    if by == "units_short":
        columns.append({"name": "units short", "align": "right"})
        key = "shortfall"
    elif by not in ("rows", "calls", *(result.get("group_by") or [])) and by:
        columns.append({"name": _label(by), "align": "right"})
        key = by
    show_deliveries = key is None and any("deliveries" in r for r in result["rows"])
    if show_deliveries:
        columns.append({"name": "deliveries", "align": "right"})
    rows = []
    for r in result["rows"][:max_rows]:
        cells = [_cell(d) for d in r.get("dimensions", [])] + [_cell(r.get("rows"))]
        if show_deliveries:
            cells.append(_cell(r.get("deliveries")))
        if key:
            v = r.get(key)
            if by == "units_short" and v is not None:
                d = _decimal(str(v))
                v = _cell(str(-d)) if d is not None else _cell(v)
            cells.append(_cell(v))
        rows.append(cells)
    sorted_by = "units short, biggest first" if by == "units_short" else f"{_label(by)}, {'largest' if direction == 'desc' else 'smallest'} first"
    title = f"Top {len(rows)} by {'units short' if by == 'units_short' else _label(by)}" + (f" per {' and '.join(dims)}" if dims else "")
    facts = {"sorted by": sorted_by, "groups": f"{len(rows)} of {len(result['rows'])}",
             "grain": f"{result.get('total_rows', '')} releases across the groups returned"}
    if _window_words(result):
        facts["window"] = _window_words(result)
    return {"title": title, "columns": columns, "rows": rows, "facts": facts}


def _list_data(result: dict, max_rows: int) -> dict:
    columns = [{"name": "release", "align": "left"}, {"name": "picker", "align": "left"}, {"name": "item", "align": "left"},
               {"name": "expected", "align": "right"}, {"name": "picked", "align": "right"}, {"name": "duration s", "align": "right"}]
    rows = []
    for r in result["rows"][:max_rows]:
        a = r.get("attributes") or {}
        looked = r.get("looked_up") or {}
        item = (r.get("item_number") or "") + ((" " + looked["item description.ItemDescription"]) if looked.get("item description.ItemDescription") else "")
        rows.append([" · ".join(r.get("key") or []), r.get("user_name") or "", item,
                     _cell(a.get("expected")), _cell(a.get("picked")), _cell(a.get("duration_s"))])
    facts = {"shown": f"{len(rows)} of {result.get('total')} matching releases"}
    if _window_words(result):
        facts["window"] = _window_words(result)
    return {"title": "Releases", "columns": columns, "rows": rows, "facts": facts}
