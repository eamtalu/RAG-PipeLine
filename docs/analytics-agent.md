# The analytics agent on LangGraph

One question in, one answer out, over the same tools the dashboard uses.
The model is a setting; nothing else changes when it does.

## What it is

- `app/agent/profiles/analytics.yaml`: the profile (prompt, nudges, domain, schema tools), run by `GroundedAgent` in `app/agent/runtime/grounded.py` through the gateway (`docs/agent-profiles.md`, `docs/agent-gateway.md`).
  A LangGraph graph built by `langchain.agents.create_agent`: model, tool calls, tool results, repeat, answer.
- `app/services/analytics_agent/tools.py`: twelve tools bound to the tenant; each call opens its own short database session and closes it before the model is asked again.
  Eight are the debugging agent's own (`search_transactions` … `explain_freshness`), wrapped unchanged.
  Four are new and read the pick-release settlement: `describe_releases`, `aggregate_releases`, `list_releases`, `explain_release`.
- `app/services/analytics/settle_reads.py`: the settlement reads as service functions.
  The HTTP endpoints `/settlements/{name}/rows`, `/list` and `/preview` call them, and so do the tools, so there is one implementation.
- `POST /api/v1/analytics/agent/ask` with `{"question": "...", "history": [...]}` returns `{answer, tool_calls, iterations, model}`.
- The Teams consumer runs this profile for every question; the Claude-only agent and the `TEAMS_AGENT` switch were retired in step 12.

## Choosing the model

`ANALYTICS_AGENT_MODEL` is one string in the form `provider:model`.

| setting | needs | use |
| --- | --- | --- |
| `ollama:qwen3:8b` | Ollama reachable at `OLLAMA_BASE_URL` | local testing |
| `anthropic:claude-sonnet-5` | `pip install langchain-anthropic`, `ANTHROPIC_API_KEY` | production |
| `openai:gpt-…` | `pip install langchain-openai`, `OPENAI_API_KEY` | production |
| `bedrock_converse:qwen.qwen3-235b-a22b-2507-v1:0` | `langchain-aws` (in requirements), AWS credentials in the process environment, `BEDROCK_REGION` | production |

On Bedrock the credentials are the same IAM user the Teams consumer uses (`/etc/fastapirag/teams-consumer.env`, read by both the consumer and the web service units), granted `bedrock:InvokeModel` on that one model by the edge Terraform.
Qwen3 235B A22B 2507 is the instruct version, so it answers without a thinking preamble; London (`eu-west-2`) keeps the data in the UK region.

On Ollama, `ANALYTICS_AGENT_THINK=false` (the default) stops Qwen3 reasoning at length before every reply, and `ANALYTICS_AGENT_CONTEXT_TOKENS=16384` gives the fifteen tool schemas and a grouped read room.
The model must support tool calling; every hosted model above does, and Qwen3 and Llama 3.1 do on Ollama.

## Guards, and the one retry

Every answer is checked before it leaves: a figure with no successful data call in this turn, or a figure that appears in no tool result, is not shown.
Since chunk 126 such a draft is discarded and the question asked once more with a nudge that names the problem (stale figures from an earlier turn, or arithmetic the model did itself); only a second bad draft is withheld.
Prior turns are replayed to the model without their tables, ranked lists or "From the data" lines, so there are no stale figures to copy.
`retries` in the result says whether the nudge was needed.

## Local testing with Ollama on a laptop

The Matrix server has no GPU and a 2010 CPU without AVX, so the model runs on a laptop and the server reaches it through a reverse SSH tunnel:

```
ollama pull qwen3:8b                                         # on the laptop, once
ssh -N -o RemoteCommand=none -R 11434:127.0.0.1:11434 amin@192.168.0.142   # keep open while testing
```

On the server `OLLAMA_BASE_URL=http://127.0.0.1:11434` then lands on the laptop's Ollama.
Then:

```
curl -s -X POST http://localhost:8000/api/v1/analytics/agent/ask -H "X-Customer-Code: tmp-live" \
  -H "Content-Type: application/json" -d '{"question": "top 5 shorted products this week"}'
```

## What holds the answer to the data (in code, not in the prompt)

- The whole run, topic gate included, has a deadline (`analytics_agent_run_timeout_seconds`, 120 s by default); a provider that never answers is cancelled and the answer is a fixed sentence with `stop_reason: timeout`.
- An answer with figures is withheld unless at least one data tool returned a result without an error.
- Every figure in the answer must appear in a tool result (or in the question, or be a percentage); any other figure is named and the answer withheld.
- For a "top N" question the ranked table is rendered from the tool's own rows in the server's order, and the model's own table is dropped.
- The full tool trace is logged per question, so any answer can be audited from the consumer or web log.
These live in `app/agent/presentation/evidence.py` and `app/services/analytics_agent/agent.py`, and they apply to every provider.

### The presentation policy (step 19)

- One fragment, `app/agent/profiles/fragments/presentation.md`, tells every profile how to present: lead with the finding, one shape for the data, findings with their reasoning, facts apart from interpretation, plain English, never repeat the rows.
- `chart_from_data` draws a chart from the last data call's own result: the model names a measure (and at most a kind); `app/agent/presentation/charts.py` copies the labels and values, picks line for a time bucket, bar for groups, pie for 2 to 8 shares, and titles it. The model never retypes a chart number; a measure the rows lack drops the chart with a logged reason, never the answer. `show_chart` stays for compatibility.
- `show_kpis` turns 1 to 6 labelled values into the `kpis` block; `state_findings` turns findings with reasoning and source, recommendations with reasons and limitations into the `findings` block. The figure guards read their words as they read the answer, and a finding whose source names a data tool that did not answer in the turn is dropped.
- Blocks come in reading order: entities, kpis, chart, table, findings, form, receipt, choice, followups; at most eight.
- The lift draws the shape itself when the model drew none (`charts.auto_chart`, `charts.auto_kpis`): a line for a trend of three or more points, a bar for a ranking the model asked for (2 to 12 groups), key-figure tiles for a single total. Its numbers come from the data result, so no guard is needed. A presentation call the model wrote out as text (`state_findings findings: [...]`) is lifted off the answer by `written_calls.lift_written_calls` into the trace, validated, guarded and drawn as a real call.

## Rules the agent is held to

- The tenant is bound server-side; no tool has a tenant argument.
- Windows are capped at 92 days, lists at 100 rows, grouped reads at 500 groups.
- Every release answer states the grain: figures are over releases, never over handheld calls.
- Zero-pick and partial are reported apart and never added into one number.
- A ratio is computed from the numbers of one call.
- A mistyped field comes back to the model as a readable problem with the valid fields listed, so it corrects itself.
- History is plain text only; the model re-reads the data for every question.

## Tests

`tests/test_analytics_agent_chunk124.py`: the four tools against a planted settlement, the loop with a scripted model that needs no provider, the endpoint, and the Teams switch.
