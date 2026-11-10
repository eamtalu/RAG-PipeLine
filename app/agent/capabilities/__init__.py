"""Capabilities: what the agent can do, as specs in one registry, selected by namespace.

`spec.py` holds ToolSpec and Namespace (a family of tools with one runner); `registry.py` holds
every spec and builds the offered set for a profile's namespaces per run, with the executor
wrapper around each call. The tools themselves live outside the package because they read the
database; a namespace is registered by the channel that composes the registry. Later steps add
per-tenant sources and health filtering, side-effect confirmation and the query connectors.
"""
