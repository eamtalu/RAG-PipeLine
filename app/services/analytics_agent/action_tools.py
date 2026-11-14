"""The action tools of the `actions` namespace (stage D, step 8; step 14c the watch tools; step 18
the registry): the tools the model is offered to propose a form the person confirms, generated
from app/services/ask/actions.py (one `propose_<id>` per real action, one `demo_action` for the
demo stubs), plus the watch tools, which answer with receipts, forms and choices of their own.
The form builders live in app/services/ask/forms.py; this module is the namespace's face.
"""

from __future__ import annotations

from typing import Any, Mapping

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.analytics_agent.watch_tools import WATCH_TOOL_NAMES, WATCH_TOOLS, run_watch_tool
from app.services.ask import actions as action_registry
from app.services.ask.forms import SHIFTS, create_user_form, role_note, shift_text  # noqa: F401 - the step 8 names

PROPOSE_CREATE_USER = "propose_create_user"
PROPOSE_REMINDER = "propose_reminder"
PROPOSE_SCHEDULE = "propose_schedule"
DEMO_ACTION = action_registry.DEMO_TOOL

ACTION_TOOLS: list[dict[str, Any]] = [*action_registry.tool_specs(), *WATCH_TOOLS]
ACTION_TOOL_NAMES = action_registry.tool_names() | WATCH_TOOL_NAMES


async def run_action_tool(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                          name: str, args: Mapping[str, Any], *, actor_name: str | None = None) -> str:
    """Run one action tool: the form block as JSON under "block", the question to ask back under
    "ask", or the problems the model can fix.

    ("propose_create_user", {"display_name": "Dan"}) -> '{"shown": "form", "block": {...}}' ;
    ("watch_now", {"what": "Route 14"}) -> the watch tool's receipt, form or choice ;
    (a picker asking) -> '{"problems": ["you cannot do that: …"]}'
    """
    if name in WATCH_TOOL_NAMES:
        return await run_watch_tool(db, customer_code, actor_object_id, actor_site_id, name, args, actor_name=actor_name)
    return await action_registry.run(db, customer_code, actor_object_id, actor_site_id, name, args)
