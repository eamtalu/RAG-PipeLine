"""The loop and its guards: GroundedAgent (grounded.py) and the before/after checks (guards.py).

Since step 11 a run is `GroundedAgent.run(context, request, prior_turns)`: the context comes
from the gateway and carries the model settings, the budgets and the deadline, so nothing in here
reads settings. The profile and registry layering of docs/esmart-agent-architecture.md section 4
is built on top in the following steps.
"""
