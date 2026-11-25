"""Reading stored runs: one run as a timeline, and what went wrong over a period.

Pure functions over the dicts `PostgresTraceStore` returns, so they need no database and never
change anything. `scripts/trace_review.py` prints what they return. A run stored before chunk 221
lacks the newer fields; every function here reads it without them.
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

Run = Mapping[str, Any]

#: How many example trace ids a finding names.
EXAMPLES = 5
#: How many tools the slowest-tools list holds.
SLOWEST = 10


def _calls(run: Run) -> list[Mapping[str, Any]]:
    return list(run.get("tool_calls") or [])


def _args_text(call: Mapping[str, Any]) -> str:
    """A call's arguments as one stable text, for comparing calls and searching them.

    >>> _args_text({"args": {"b": 1, "a": 2}})
    '{"a": 2, "b": 1}'
    """
    return json.dumps(call.get("args") or {}, sort_keys=True, default=str)


def repeated_calls(run: Run) -> list[dict[str, Any]]:
    """The tool calls a run made more than once with the same arguments.

    (list_releases {"d": 1} twice, {"d": 2} once) -> [{"name": "list_releases", "args": {"d": 1},
    "times": 2}]
    """
    seen = Counter((call["name"], _args_text(call)) for call in _calls(run))
    return [{"name": name, "args": json.loads(args), "times": times}
            for (name, args), times in seen.items() if times > 1]


def sites_outside_scope(run: Run, site_ids: Sequence[str]) -> list[str]:
    """The sites a run's tool arguments named that its asker may not see.

    (BRI scope, a call with "warehouse==LON") -> ["LON"] ; (all sites, or no scope stored) -> []
    """
    scope = run.get("scope")
    if not scope or scope.get("all_sites"):
        return []
    mine = set(scope.get("site_ids") or ())
    text = " ".join(_args_text(call) for call in _calls(run))
    # a site code counts only as a whole word, so "LON" is not found inside "LONDON" or "LON5"
    return sorted(site for site in site_ids if site not in mine
                  and re.search(rf"(?<![A-Za-z0-9]){re.escape(site)}(?![A-Za-z0-9])", text))


def closed_tables(run: Run) -> list[str]:
    """The tables a run tried to read that are closed to its asker.

    (one call whose data says closed=["log_entries"]) -> ["log_entries"]
    """
    return sorted({table for call in _calls(run)
                   for table in ((call.get("data") or {}).get("closed") or ())})


def is_limited_by_scope(run: Run, site_ids: Sequence[str]) -> bool:
    """Whether the asker's scope cut what this run wanted: another site, or a closed table.

    (BRI scope asking for LON) -> True ; (BRI scope asking for BRI) -> False
    """
    return bool(sites_outside_scope(run, site_ids) or closed_tables(run))


def _who(run: Run) -> str:
    """The asker with their role and sites as they were then.

    >>> _who({"principal_id": "u1", "role_code": "supervisor",
    ...       "scope": {"all_sites": False, "site_ids": ["BRI"]}})
    'u1 supervisor [BRI]'
    >>> _who({"principal_id": "u1", "role_code": None, "scope": None})
    'u1'
    """
    parts = [str(run.get("principal_id") or "?")]
    if run.get("role_code"):
        parts.append(str(run["role_code"]))
    scope = run.get("scope")
    if scope:
        sites = "all sites" if scope.get("all_sites") else ", ".join(scope.get("site_ids") or ())
        parts.append(f"[{sites or 'no sites'}]")
    return " ".join(parts)


def _rounds(run: Run) -> str:
    n = int(run.get("iterations") or 0)
    return f"{n} round" if n == 1 else f"{n} rounds"


def _when(run: Run) -> str:
    started = run.get("started_at")
    return started.strftime("%Y-%m-%d %H:%M") if started is not None else "?"


def list_line(run: Run, site_ids: Sequence[str]) -> str:
    """One run as one line of the list, with a mark when the scope limited it.

    (a BRI supervisor's run that asked for LON) -> "2026-11-25 09:00  t1  u-sup supervisor [BRI]
    ask  end_turn  1 round  900 ms  scope-limited  how is BRI?"
    """
    mark = ["scope-limited"] if is_limited_by_scope(run, site_ids) else []
    question = " ".join(str(run.get("question") or "").split())[:80]
    return "  ".join([_when(run), str(run.get("trace_id")), _who(run),
                      str(run.get("trigger") or "ask"), str(run.get("stop_reason")), _rounds(run),
                      f"{run.get('duration_ms')} ms", *mark, question])


def timeline(run: Run) -> list[str]:
    """One run as lines: a header, every model call, tool call and event in order, the answer.

    (a run with 2 model calls and 1 tool call) -> ["run t1 | ...", "  +0 model ...",
    "  +310 tool ...", "  +420 model ...", "answer: ..."]
    """
    # Step 1: Gather what happened as (when, order, text), so that things recorded in separate
    # lists are shown in the order they happened; a run without stamps keeps its listed order
    steps = {s.get("round"): s.get("text") for s in run.get("steps") or []}
    happened: list[tuple[int, int, str]] = []
    productive = 0  # model calls are numbered per run; steps per call that asked for a tool
    for n, call in enumerate(run.get("model_calls") or []):
        state = "ok" if call.get("ok") else "FAILED"
        text = (f"model round {call.get('round')} {call.get('model')} {call.get('ms')} ms {state} "
                f"in={call.get('input_tokens')} out={call.get('output_tokens')}")
        happened.append((int(call.get("at_ms") or 0), n, text))
    for n, call in enumerate(_calls(run)):
        data = call.get("data") or {}
        state = "ok" if call.get("ok") else "FAILED"
        text = (f"tool {call['name']} {_args_text(call)} {call.get('ms')} ms {state} "
                f"rows={call.get('result_rows')} chars={call.get('result_chars')} "
                f"tables={data.get('tables') or []} closed={data.get('closed') or []}")
        happened.append((int(call.get("at_ms") or 0), 1000 + n, text))
    for n, event in enumerate(run.get("events") or []):
        happened.append((int(event.get("at_ms") or 0), 2000 + n,
                         f"event {event.get('kind')}: {event.get('detail')}"))
    # Step 2: Print them, and put the model's own words under the tool call they led to, because
    # that is the sentence that explains the choice
    lines = [f"run {run.get('trace_id')} | {_when(run)} | {_who(run)} | "
             f"{run.get('trigger') or 'ask'} | {run.get('stop_reason')} in {_rounds(run)}, "
             f"{run.get('duration_ms')} ms"]
    lines.append(f"question: {' '.join(str(run.get('question') or '').split())}")
    for at_ms, _, text in sorted(happened):
        lines.append(f"  +{at_ms} {text}")
        if text.startswith("tool "):
            productive += 1
            if steps.get(productive):
                lines.append(f"      model said: {steps[productive]}")
    for guard in run.get("guards") or []:
        lines.append(f"  guard {guard.get('name')} {guard.get('stage')}: {guard.get('verdict')}")
    if run.get("withheld_figures"):
        lines.append(f"  withheld figures: {list(run['withheld_figures'])}")
    if run.get("error"):
        lines.append(f"  error: {run['error']}")
    lines.append(f"answer: {' '.join(str(run.get('answer') or '').split())}")
    return lines


def _finding(runs: Iterable[Run]) -> dict[str, Any]:
    """How many runs a finding covers and the first few to open: {"runs": 3, "examples": [ids]}."""
    ids = [str(run.get("trace_id")) for run in runs]
    return {"runs": len(ids), "examples": ids[:EXAMPLES]}


def _tokens(run: Run) -> int:
    usage = run.get("usage") or {}
    return int(usage.get("input_tokens") or 0) + int(usage.get("output_tokens") or 0)


def findings(runs: Sequence[Run], site_ids: Sequence[str]) -> dict[str, Any]:
    """What went wrong over a set of runs, each finding with its count and example trace ids.

    (7 runs, one looping, one timed out, ...) -> {"runs": 7, "repeated_calls": {"runs": 1,
    "examples": ["loop"]}, "tool_errors": [...], "slowest_tools": [...], "by_prompt_version": [...]}
    """
    # Step 1: The per-run findings, each a filter over the runs, so a finding's examples are the
    # runs to open with `show`
    found: dict[str, Any] = {
        "runs": len(runs),
        "repeated_calls": _finding(r for r in runs if repeated_calls(r)),
        "round_limit": _finding(r for r in runs if r.get("stop_reason") == "max_iterations"),
        "timeouts": _finding(r for r in runs if r.get("stop_reason") == "timeout"),
        "limited_by_scope": _finding(r for r in runs if is_limited_by_scope(r, site_ids)),
        "declined": _finding(r for r in runs if r.get("stop_reason") == "declined"),
        "withheld": _finding(r for r in runs if r.get("withheld_figures")),
        "retried": _finding(r for r in runs if any(e.get("kind") == "retry"
                                                   for e in r.get("events") or [])),
        "errors": _finding(r for r in runs if r.get("status") == "error"),
    }
    # Step 2: The per-tool figures, because a failing or slow tool hurts every run that uses it
    calls: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for run in runs:
        for call in _calls(run):
            calls[call["name"]].append(call)
    found["tool_errors"] = sorted(
        ({"tool": tool, "errors": sum(1 for c in made if not c.get("ok")), "calls": len(made)}
         for tool, made in calls.items() if any(not c.get("ok") for c in made)),
        key=lambda row: (-row["errors"], row["tool"]))
    found["slowest_tools"] = sorted(
        ({"tool": tool, "calls": len(made),
          "avg_ms": round(sum(int(c.get("ms") or 0) for c in made) / len(made)),
          "max_ms": max(int(c.get("ms") or 0) for c in made)}
         for tool, made in calls.items()),
        key=lambda row: (-row["avg_ms"], row["tool"]))[:SLOWEST]
    # Step 3: The same figures per prompt version, so that a change to the prompt can be judged
    # by what it did to rounds, time and tokens
    by_version: dict[str, list[Run]] = defaultdict(list)
    for run in runs:
        by_version[str(run.get("profile_version") or "")].append(run)
    found["by_prompt_version"] = [
        {"version": version, "runs": len(of),
         "avg_rounds": round(sum(int(r.get("iterations") or 0) for r in of) / len(of), 1),
         "avg_ms": round(sum(int(r.get("duration_ms") or 0) for r in of) / len(of)),
         "avg_tokens": round(sum(_tokens(r) for r in of) / len(of))}
        for version, of in sorted(by_version.items())]
    return found
