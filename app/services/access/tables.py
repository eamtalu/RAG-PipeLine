"""The access tables as a data limit needs to know them: whose each table's rows are.

Other packages never import the access models (tests/test_access_boundary_chunk219.py), so the
package itself says how its tables may be read: by customer, by customer and site, through the
user that owns the row, or by anyone because the list names no one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from app.persistence.models.access import (
    AccessCapability,
    AccessRole,
    AccessRoleCapability,
    AuditEvent,
    TeamsUser,
    TeamsUserCapability,
    TeamsUserSite,
)
from app.persistence.models.teams_binding import TeamsTenantBinding

Ownership = Literal["customer", "site", "owner", "shared"]


@dataclass(frozen=True, slots=True)
class AccessTable:
    """One access table and whose its rows are; `key` is the site column or the owner's key."""

    model: Any
    ownership: Ownership
    key: str | None = None
    owner: Any = None


ACCESS_TABLES: tuple[AccessTable, ...] = (
    AccessTable(TeamsUser, "customer"),
    AccessTable(TeamsUserSite, "customer"),
    AccessTable(AccessRoleCapability, "customer"),
    AccessTable(TeamsTenantBinding, "customer"),
    AccessTable(AuditEvent, "site", key="site_id"),
    AccessTable(TeamsUserCapability, "owner", key="user_id", owner=TeamsUser),
    AccessTable(AccessRole, "shared"),
    AccessTable(AccessCapability, "shared"),
)
