"""Who is calling, for which tenant, with what budget: the one place a run's context is built.

`snapshot_settings` is the only reader of `app.settings` under `app/agent` (a layout test holds
it to that); `build_context` turns a request and a snapshot into the frozen `ExecutionContext`;
`run_request` is the entry every channel calls. Admission (quotas, claims) and the conversation
store join here in later steps of docs/esmart-agent-execution-plan.md.
"""

from app.agent.gateway.context import build_context, snapshot_settings
from app.agent.gateway.ports import AgentRuntime
from app.agent.gateway.run import run_request

__all__ = ["AgentRuntime", "build_context", "run_request", "snapshot_settings"]
