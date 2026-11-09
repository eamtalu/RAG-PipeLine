"""The agent package: everything that talks to a model, behind one boundary.

Rules enforced by tests/test_agent_layout_chunk153.py: nothing in here imports the web, worker or
persistence layers, and only the channels (app/teams_consumer.py, app/api/*) import this package.
The layering inside (channels, gateway, runtime, capabilities, presentation) fills in step by step;
see docs/esmart-agent-architecture.md section 12 and docs/esmart-agent-execution-plan.md.
"""
