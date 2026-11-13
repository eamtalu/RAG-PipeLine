# access.py — who may do what in the eSmart Eye Teams app (stage D, step 7)
#
#   The matrix of screen 07 as data: roles and capabilities (seeded from
#   app/services/access/matrix.yaml), one state per cell per customer (an Admin may change a cell
#   in the app), and the people: a user is an Entra object id within a customer, with a role, the
#   sites they may see (or all of them) and, per capability, an override the Admin set or an
#   approval granted. Approvals queue the "with approval" requests; audit_events keep every
#   permission decision that changed data, append-only, purged after 90 days.
#
#   customer_code is a real foreign key everywhere here: a user of a missing log space is a
#   config error, as a site of one is.

import uuid
from datetime import date, datetime, timezone

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

USER_STATUSES = ("active", "pending", "dormant", "deactivated")
APPROVAL_STATUSES = ("pending", "approved", "declined", "expired", "withdrawn")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AccessRole(Base):
    __tablename__ = "access_roles"

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")


class AccessCapability(Base):
    __tablename__ = "access_capabilities"

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")


class AccessRoleCapability(Base):
    """One cell of a customer's matrix: allowed | with_approval | no."""

    __tablename__ = "access_role_capabilities"

    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), primary_key=True)
    role_code: Mapped[str] = mapped_column(
        String(32), ForeignKey("access_roles.code"), primary_key=True)
    capability_code: Mapped[str] = mapped_column(
        String(32), ForeignKey("access_capabilities.code"), primary_key=True)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now, nullable=False)
    updated_by: Mapped[str | None] = mapped_column(String(128), nullable=True)


class TeamsUser(Base):
    __tablename__ = "teams_users"
    __table_args__ = (
        UniqueConstraint("customer_code", "object_id", name="uq_teams_users_customer_object"),
        Index("ix_teams_users_customer_status", "customer_code", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    object_id: Mapped[str] = mapped_column(String(128), nullable=False)  # Entra object id
    display_name: Mapped[str] = mapped_column(String(128), nullable=False)
    email: Mapped[str | None] = mapped_column(String(256), nullable=True)
    role_code: Mapped[str] = mapped_column(String(32), ForeignKey("access_roles.code"), nullable=False)
    all_sites: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active")
    starts_on: Mapped[date | None] = mapped_column(Date, nullable=True)
    note: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now, nullable=False)
    last_active_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TeamsUserSite(Base):
    """A site the user may see; no rows when the user has all_sites."""

    __tablename__ = "teams_user_sites"
    __table_args__ = (
        ForeignKeyConstraint(["customer_code", "site_id"],
                             ["tenant_sites.customer_code", "tenant_sites.site_id"],
                             ondelete="CASCADE"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("teams_users.id", ondelete="CASCADE"), primary_key=True)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    site_id: Mapped[str] = mapped_column(String(16), primary_key=True)


class TeamsUserCapability(Base):
    """A per-user override of one cell: set by an Admin, or granted through an approval."""

    __tablename__ = "teams_user_capabilities"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("teams_users.id", ondelete="CASCADE"), primary_key=True)
    capability_code: Mapped[str] = mapped_column(
        String(32), ForeignKey("access_capabilities.code"), primary_key=True)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    granted_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
    approval_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class AccessApproval(Base):
    __tablename__ = "access_approvals"
    __table_args__ = (
        UniqueConstraint("customer_code", "reference", name="uq_access_approvals_reference"),
        Index("ix_access_approvals_customer_status", "customer_code", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    reference: Mapped[str] = mapped_column(String(16), nullable=False)  # USR-2041, ACC-311, ACT-17
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_by: Mapped[str] = mapped_column(String(128), nullable=False)  # object id
    requested_by_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    requested_via: Mapped[str] = mapped_column(String(16), nullable=False, default="ask", server_default="ask")
    site_id: Mapped[str | None] = mapped_column(String(16), nullable=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    detail: Mapped[str | None] = mapped_column(String(300), nullable=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    decided_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    changes: Mapped[dict | None] = mapped_column(JSONB, nullable=True)  # what the approver altered
    note: Mapped[str | None] = mapped_column(String(300), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)


class AuditEvent(Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        Index("ix_audit_events_customer_at", "customer_code", "at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
    actor_object_id: Mapped[str] = mapped_column(String(128), nullable=False)
    actor_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    actor_kind: Mapped[str] = mapped_column(String(16), nullable=False, default="user", server_default="user")
    action: Mapped[str] = mapped_column(String(64), nullable=False)  # area.action
    capability_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    decision: Mapped[str] = mapped_column(String(16), nullable=False)
    subject_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    subject_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    site_id: Mapped[str | None] = mapped_column(String(16), nullable=True)
    request_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    trace_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)  # the sentence the audit list shows
    detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
