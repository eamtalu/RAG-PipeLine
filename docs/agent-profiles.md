# Agent profiles

Since 10 November 2026 (step 12 of `docs/esmart-agent-execution-plan.md`) an agent is a profile, a YAML file under `app/agent/profiles/`, on a tool set.
The two profiles are `analytics.yaml` (picking releases, deliveries at risk, log calls; Teams and `/analytics/agent/ask`) and `logspace.yaml` (the ask box over the feed's records; `/logs/debug/ask`).
There is one runtime, `GroundedAgent(customer, profile=..., tool_set=...)`; the former `AnalyticsAgent`, `LogspaceAgent` and the Claude-only `LogDebugAgent` are gone.

## The ten keys

| key | what it is |
| --- | --- |
| `name` | must equal the file name |
| `domain` | one sentence for the topic gate: what the agent is for |
| `decline` | the fixed reply to an off-topic question or a manipulation attempt |
| `schema_tools` | tools that describe the schema rather than read data; figures resting only on them were made up |
| `tool_set` | the tool set builder the channel must hand the runtime (`analytics`, `logspace`) |
| `presentation` | how answers are finished: `analytics` (table and card with links) or `logspace` (table only) |
| `question_frame` | the text around the question, with `{today}`, `{scope_description}` and `{question}` |
| `retry_nudge` | appended to the question when a draft had figures with no data call |
| `ungrounded_nudge` | appended when a draft had figures no tool returned, with `{figures}` |
| `system_prompt` | the instructions, verbatim |

The loader (`app/agent/profiles/__init__.py`) refuses an unknown or missing key, a blank value, a name that differs from the file, a frame without `{question}`, a nudge without `{figures}`, and a presentation that does not exist.
A later step that needs a new key (guards, model policy, limits, memory) adds it to the loader on purpose.

## Writing the text

- `system_prompt` is a literal block: `|` when the prompt ends with a newline, `|-` when it does not. Lines may exceed 100 columns; the file is data the model reads, not code.
- Prose fields are folded blocks (`>-`): a line break becomes one space, so wrap only at spaces.
- The nudges are stored without their leading blank lines; the loop puts a blank line between the question and the nudge.
- `question_frame` is double-quoted so its `\n` are explicit.

## The review gate: snapshots

`tests/profile_snapshots/<name>.json` holds the five prose fields byte for byte as they were when the profile was created, and `tests/test_agent_profiles_chunk162.py` asserts equality.
A prompt edit must update the snapshot in the same commit, so every change to what the model is told is a named, reviewed diff of two files.

## Versions on the trace

`Profile.version` is the sha256 of the file (12 hex); `code_version()` is `git rev-parse --short HEAD`, read once per process, `unknown` without git.
Both are on every `AgentResult` (`profile_version`, `code_version`; not in the legacy dict the channels return) and on the gateway's `agent.run` log line.
Until step 14 stores traces, that line plus the result is the trace of a run; step 14 copies the two fields into the `agent_traces` row.

## The tool set seam

The tools read Postgres, so they stay outside `app/agent`: `app/agent/runtime/tool_set.py` names the role (`ToolSetBuilder.build(context, request) -> ToolSet(tools, tool_text, scope_description)`), and `app/services/analytics_agent/tool_set.py` and `app/services/logspace_agent/tool_set.py` implement it.
The logspace builder makes the `FeedScope` from the raw filter dict on the request and the day on the context, and describes it for the question frame.
Each channel names the profile and its tool set in one line; step 13's registry takes that over.

## Adding a profile

Write `app/agent/profiles/<name>.yaml` with the ten keys, a tool set builder with `name == <name>` where the tools live, a snapshot file, and a channel that hands the runtime to the gateway.
