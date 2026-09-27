# The analytics agent on LangGraph

One question in, one answer out, over the same tools the dashboard uses.
The model is a setting; nothing else changes when it does.

## What it is

- `app/services/analytics_agent/agent.py`: `AnalyticsAgent(db, customer_code).ask(question, history)`.
  A LangGraph graph built by `langchain.agents.create_agent`: model, tool calls, tool results, repeat, answer.
- `app/services/analytics_agent/tools.py`: twelve tools bound to the request's session and tenant.
  Eight are the debugging agent's own (`search_transactions` … `explain_freshness`), wrapped unchanged.
  Four are new and read the pick-release settlement: `describe_releases`, `aggregate_releases`, `list_releases`, `explain_release`.
- `app/services/analytics/settle_reads.py`: the settlement reads as service functions.
  The HTTP endpoints `/settlements/{name}/rows`, `/list` and `/preview` call them, and so do the tools, so there is one implementation.
- `POST /api/v1/analytics/agent/ask` with `{"question": "...", "history": [...]}` returns `{answer, tool_calls, iterations, model}`.
- The Teams consumer uses this agent when `TEAMS_AGENT=langgraph` (the default) and the Claude agent when `TEAMS_AGENT=claude`.

## Choosing the model

`ANALYTICS_AGENT_MODEL` is one string in the form `provider:model`.

| setting | needs | use |
| --- | --- | --- |
| `ollama:qwen3:8b` | Ollama reachable at `OLLAMA_BASE_URL` | local testing |
| `anthropic:claude-sonnet-5` | `pip install langchain-anthropic`, `ANTHROPIC_API_KEY` | production |
| `openai:gpt-…` | `pip install langchain-openai`, `OPENAI_API_KEY` | production |
| `bedrock:…` | `pip install langchain-aws`, AWS credentials | production on AWS |

On Ollama, `ANALYTICS_AGENT_THINK=false` (the default) stops Qwen3 reasoning at length before every reply, and `ANALYTICS_AGENT_CONTEXT_TOKENS=16384` gives the twelve tool schemas and a grouped read room.
The model must support tool calling; every hosted model above does, and Qwen3 and Llama 3.1 do on Ollama.

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
