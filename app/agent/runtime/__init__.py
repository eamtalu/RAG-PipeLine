"""The loop and its guards: GroundedAgent (grounded.py) and the checks (guards.py).

A runtime is `GroundedAgent(customer, profile=load_profile(name), registry=<ToolRegistry>)`: the
profile is data (app/agent/profiles), the registry holds every tool under its namespace
(app/agent/capabilities) and the profile lists which namespaces it may use. A run is
`run(context, request, prior_turns)` under the context the gateway built, so nothing in here
reads settings.
"""
