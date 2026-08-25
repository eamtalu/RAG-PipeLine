"""R1. One row per `(method, field)`: the allowlist that DISCOVERS.

Why not a denylist
------------------
`_SENSITIVE` (`derive_transactions.py:45`) is a five-word denylist - password, accesstoken,
m3credentials, m3usercredentials, cipher - and it guards a table that is `KEEP_FOREVER`. Measured over
400 live response payloads, the two most frequent keys of the 145 distinct ones are `AccessToken` and
`M3UserCredentials`. A denylist in front of permanent storage means one renamed credential field
becomes permanent, silently.

Why not a plain allowlist either
--------------------------------
A static allowlist of the 145 known fields silently DROPS a field the WMS starts logging tomorrow -
which loses exactly the future-metric history the capture exists to buy, and loses it just as silently.

So: unknown means recorded, not captured
----------------------------------------
    key already in the registry, captured=true   ->  captured
    key already in the registry, captured=false  ->  skipped
    key never seen before                        ->  row created with captured=false,
                                                     surfaced for review, value NEVER stored

Only the field NAME is written for an unknown key. That is what makes the discovery record itself safe:
a newly appearing `SessionSecret` is reported by name so somebody can decide, and its value never
touches the database.

The consequence worth stating: the registry stops being a filter and becomes the SCHEMA.
`definition.validate` checks a metric's `attr:` dimension against these rows rather than against a
hardcoded tuple, so a typo is still refused but a newly discovered field becomes usable without a
release (R1b).
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class AnalyticsFieldRegistry(Base):
    """One observable field on one method, and whether analytics may keep its value."""

    __tablename__ = "analytics_field_registry"
    __table_args__ = (
        UniqueConstraint("customer_code", "method", "source", "field",
                         name="uq_analytics_field_registry_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    #: The method the field was observed on. Per method rather than global because the same name can
    #: mean different things on different endpoints, and because a reviewer approving `STQT` on
    #: `MMS060MI/LstBalID` has not thereby approved every field called `STQT` anywhere.
    method: Mapped[str] = mapped_column(String(128), nullable=False)

    #: Which half of the exchange it came from: `request`, `response`, or `mi_result`. Part of the key
    #: because request and response both carry `ItemNumber` and they are not the same observation -
    #: which is also why capture namespaces them (`resp.*`, `mi.*`) rather than flat-merging.
    source: Mapped[str] = mapped_column(String(16), nullable=False)

    field: Mapped[str] = mapped_column(String(128), nullable=False)

    #: FALSE for anything discovered rather than approved. The default is the whole safety property:
    #: a field nobody has looked at is reported, not stored.
    captured: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False,
                                           server_default="false")

    #: Bookkeeping for the review screen: when it first appeared, when it was last seen, and how many
    #: times. A field seen once six weeks ago is a different review decision from one arriving on every
    #: transaction, and without the count the list is unsortable by anything that matters.
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                    default=lambda: datetime.now(timezone.utc))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                   default=lambda: datetime.now(timezone.utc))
    seen_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    #: Deliberately absent: any column that could hold a VALUE. There is nowhere in this table to put
    #: one, so a discovery record cannot leak a secret even by mistake.
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reviewed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))
