"""The agent package: everything that talks to a model, behind one boundary.

Rules enforced by tests/test_agent_layout_chunk153.py: nothing in here imports the web, worker or
persistence layers; only the channels (app/teams_consumer.py, app/api/*) import this package; and
only gateway/context.py reads settings. Inside: contracts.py (what crosses the boundary),
gateway/ (builds the one frozen ExecutionContext and runs a request), profiles/ (what each agent
is, as YAML, with a version), capabilities/ (every tool spec under its namespace, bound per run),
runtime/ (the one loop and its guards), presentation/ (evidence from the tool trace, one set per
profile). See docs/esmart-agent-architecture.md section 12, docs/agent-gateway.md,
docs/agent-profiles.md and docs/agent-tool-registry.md.
"""
