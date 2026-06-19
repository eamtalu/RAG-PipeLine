"""Data-driven rules: DB rows (NotificationRule) evaluated by code primitives.

A rule's `rule_type` selects an evaluator (see evaluators.py); `match` (JSONB) parameterizes it. The
frontend manages rules entirely via the API (create / edit / publish / deactivate) — no redeploy.
The engine (engine.py) loads ACTIVE rules each cycle and runs them.
"""
