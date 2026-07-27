---
name: whats-changed
description: Master + component-level digest of recent changes in this repo. Explains WHAT changed and WHY, grouped by component, so the user can rebuild their mental model after AI agents (or anyone) have been coding across the codebase. Use when the user types /whats-changed, or asks "what changed", "catch me up on the code", "summarize recent changes", or "rebuild my mental model".
---

# What's Changed — mental-model rebuild digest

Produce a two-level summary of recent changes: a **master overview** (the storylines across the repo) and a **component-level breakdown** (what changed in each area and why).
The reader is the repo owner who has NOT been watching the changes land.
Optimize for "why is the code like this now", not for listing diffs.

## 1. Determine the range

Argument handling (`$ARGUMENTS`):

- No argument: use the saved marker in `.Codex/whats-changed.state` (a commit SHA). Range is `<marker>..HEAD`.
  - If the file does not exist, default to the last 7 days: `git log --since="7 days ago"`.
- A number like `10`: last 10 commits (`HEAD~10..HEAD`).
- A duration like `3d`, `2w`, `1m`: `--since` that period.
- A ref/range like `abc123`, `abc123..HEAD`, `main@{1.week.ago}`: use as given.
- `--no-mark`: do everything but skip step 5 (don't move the marker).

If the resolved range is empty, say so ("nothing new since <marker short-sha>, <date>") and stop.

## 2. Gather the evidence

Run (adjust range syntax to what step 1 produced):

```bash
git log --reverse --format='%h|%ad|%s' --date=short <range>
git diff --stat <range>
git log --format='%h %s' --name-only <range>
```

Also include uncommitted work: `git status --short` and `git diff --stat` (working tree). If present, report it as its own "Uncommitted" section — the user needs to know about in-flight work too.

## 3. Group by component

Map changed files to components using top-level paths. For this repo:

| Path | Component |
|---|---|
| `app/api/` | API layer (routes/schemas) |
| `app/services/` | Services / business logic |
| `app/persistence/` | Persistence (models, repos, vectorstore) |
| `app/config/`, `app/settings.py` | Config |
| `app/background.py`, `app/worker.py` | Background workers |
| `alembic/` | DB migrations |
| `docs/` | Docs |
| `tests/` | Tests |
| `scripts/`, `deploy/`, `deploy.sh`, `docker-compose.yml` | Ops / deploy |

Anything else goes under "Other". If the repo layout has drifted from this table, group by actual top-level directory instead — do not force-fit.

For each component with meaningful changes, read the actual diff for the files involved (`git diff <range> -- <path>`), not just commit messages. Commit messages say what; the diff says how; you must explain **why** — infer intent from the diff, related docs changes, and commit message bodies (`git log --format=%B`). If the why is genuinely unclear, say "unclear why" rather than inventing a rationale.

For large ranges (>~30 commits or very large diffs), fan out one subagent per component via the Agent tool (Explore type), each returning: what changed, why, and any cross-component contract changes. Synthesize yourself.

## 4. Write the digest

Structure (adapt headings, keep the two levels):

```
## Master overview  (<range>, N commits, M files)

2-5 short paragraphs or bullets: the storylines. Not one bullet per commit —
one bullet per *effort* (e.g. "SSH log fetching was hardened over 6 commits:
timeouts, circuit breaker, windowed resume ..."). Call out:
- new capabilities / behavior changes visible from outside
- schema or API contract changes (these break mental models the hardest)
- cross-component threads (a change that touched api + services + persistence)
- anything risky, unfinished, or marked TODO/FIXME in the diffs

## By component

### <Component> (N commits, files: ...)
- What changed, in behavior terms.
- Why (intent), citing key files as `path/file.py:line` where useful.
- New/changed public surface (routes, function signatures, env vars, tables).

(only components that actually changed; one line "docs only" style entries are fine)

## Watch-outs
Contract changes, migration steps needed, config/env additions, anything
the user must know before touching the code.
```

Keep it readable: complete sentences, no arrow-chain shorthand, plain dash not em dash.

## 5. Move the marker

Unless `--no-mark` was passed or a custom historical range was requested (a number, duration, or explicit range means "just show me", so ask nothing and still move the marker ONLY for the no-argument case):

```bash
git rev-parse HEAD > .Codex/whats-changed.state
```

Ensure `.Codex/whats-changed.state` is in `.gitignore`; add it if missing.
End the digest with one line: "Marker set to <short-sha> — next /whats-changed reports from here." (or "Marker unchanged." when skipped).
