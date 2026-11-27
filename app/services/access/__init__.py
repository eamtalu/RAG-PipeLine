"""Access control of the eSmart Eye Teams app: who a person is to us, which tenant and customer they
belong to, what they may do, and how people are added, approved and removed. Read README.md here.

This module is the ONLY door: code outside the package imports `app.services.access` and uses the
names below, never a submodule (tests/test_access_boundary_chunk219.py). Inside, the package is:
- authorization/  the role and capability matrix, a person's record and `decide`
- people/         users (create, invite, claim, expire, deactivate) and approvals
- tenancy/        tenant to customer bindings, their email domains, their copy on the edge
- audit           the permission audit log
- edge_mirror     the rows the edge checks on every request
- admin_commands  the Admin tab's access_* commands
- usage           who has access and who was active, for the usage tool
- tables          whose each access table's rows are, for a data limit (step 45)
"""

from app.services.access.admin_commands import run as run_admin_command
from app.services.access.admin_commands import run as run_role_command  # same door, role kinds
from app.services.access.audit import purge_once as purge_audit_events
from app.services.access.audit import record_event as record_audit_event
from app.services.access.authorization.matrix import ALLOWED, NO, STATES, WITH_APPROVAL
from app.services.access.authorization.matrix import load as load_matrix
from app.services.access.authorization.records import (
    ALL_SITES,
    AccessRecord,
    Decision,
    access_record_for,
    decide,
    record_for_user,
    set_matrix_cell,
)
from app.services.access.authorization.roles import role_names
from app.services.access.edge_mirror import mirror_customer, roles_payload
from app.services.access.edge_mirror import sweep_once as sweep_edge_rows
from app.services.access.tables import ACCESS_TABLES, AccessTable
from app.services.access.people.approvals import expire_once as expire_approvals
from app.services.access.people.approvals import pending as pending_approvals
from app.services.access.people.approvals import request as request_approval
from app.services.access.people.users import (
    Actor,
    UserChange,
    create_user,
    expire_invites_once,
    get_user,
    invite_user,
    list_users,
    touch_last_active,
    update_user,
    user_payload,
)
from app.services.access.tenancy.binding_mirror import BindingMirror, InMemoryMirror as InMemoryBindingMirror
from app.services.access.tenancy.binding_mirror import build_mirror_from_settings as build_binding_mirror
from app.services.access.tenancy.binding_sweep import sweep_once as sweep_bindings
from app.services.access.tenancy.site_admin import KINDS as SITE_COMMAND_KINDS
from app.services.access.tenancy.site_admin import overview as sites_overview
from app.services.access.tenancy.site_admin import run as run_site_command
from app.services.access.tenancy.site_admin import sweep_once as sweep_site_rows
from app.services.access.usage import PeopleActivity, audit_action_counts, audit_actor_counts, people_activity

__all__ = [
    # authorization: what a person may do
    "ALLOWED", "WITH_APPROVAL", "NO", "STATES", "ALL_SITES", "AccessRecord", "Decision", "decide",
    "ACCESS_TABLES", "AccessTable",
    "access_record_for", "record_for_user", "load_matrix", "set_matrix_cell",
    # people: who has access and how they get it
    "Actor", "UserChange", "create_user", "update_user", "invite_user", "get_user", "list_users",
    "user_payload", "touch_last_active", "expire_invites_once", "role_names",
    "pending_approvals", "request_approval", "expire_approvals",
    # audit
    "record_audit_event", "purge_audit_events",
    # tenancy
    "BindingMirror", "InMemoryBindingMirror", "build_binding_mirror", "sweep_bindings",
    # the customer's sites and their places, set up from Teams
    "SITE_COMMAND_KINDS", "run_site_command", "sites_overview", "sweep_site_rows",
    # the edge's copy and the Admin tab's commands
    "mirror_customer", "roles_payload", "sweep_edge_rows", "run_admin_command",
    "run_role_command",
    # usage
    "PeopleActivity", "people_activity", "audit_action_counts", "audit_actor_counts",
]
