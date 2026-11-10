"""The loop and its guards: GroundedAgent (grounded.py), the checks (guards.py), the tool set port.

A runtime is `GroundedAgent(customer, profile=load_profile(name), tool_set=<builder>)`: the
profile is data (app/agent/profiles), the tool set builder lives outside the package because the
tools read the database (tool_set.py names the role). A run is `run(context, request,
prior_turns)` under the context the gateway built, so nothing in here reads settings.
"""
