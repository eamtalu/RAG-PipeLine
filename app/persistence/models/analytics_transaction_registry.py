"""R1. One row per transaction, holding what analytics is allowed to do with it.

Keyed on `transaction_name`, NOT on `method`
-------------------------------------------
Measured on the live projection: 7 distinct `transaction_name` values against 24 distinct `method`
values, and the mapping is many-to-many. `ConfirmPickLine` appears under BOTH "Brighton Stock Pick"
and "JIT and Shorts Pick (Brighton)", so a method-keyed switch cannot express "one on, the other off".
`transaction_name` is also the better control surface for a person: seven recognisable names against
twenty-four opaque endpoints.

Three switches, because they have opposite economics
----------------------------------------------------
    capture  gates whether a FACT ROW is written at all.
             Turning it off cannot be undone by turning it back on: raw `log_entries` drop at 60 days,
             so the history for that gap is gone. Defaults ON for exactly this reason.

    show     gates whether the facts reach charts, rollups and breakdowns.
             Free and fully reversible: the facts were captured all along, so switching it on fills in
             complete history at the cost of one recompute.

    expand   gates per-record expansion of `mi_result.records[]` (R4, not built).
             This is the ~200k rows/day one, so it is opt-in per transaction.

`transaction_name IS NULL` gets NO ROW
--------------------------------------
57 transactions carry no name: `CheckOperator` and `CheckServer`, which are connectivity probes rather
than warehouse activity. They cannot be keyed by name, so the rule is fixed in code instead of stored
here - always captured, never shown. Captured because a probe that starts failing is exactly the kind
of thing someone will want to measure later, and 60 days from now the entries are gone; never shown
because they are not warehouse activity and would distort every default chart.

A transaction seen for the first time gets `capture = true, show = true`
-----------------------------------------------------------------------
Neither default is "safe" in the abstract, and the two failure modes are not symmetric:

    capture off by default  ->  loses history IRREVERSIBLY, because entries expire at 60 days
    show off by default     ->  UNDER-COUNTS every chart, silently, until somebody reviews a row

An under-counting total is the exact failure this architecture exists to prevent - it looks plausible
and nothing says it is wrong. A newly seen transaction appearing on a chart unreviewed is real
warehouse activity being reported, which is at worst surprising. So both default on, and
`reviewed_at IS NULL` is what the interface reads to say "needs review": the review is SURFACED rather
than enforced by hiding data.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class AnalyticsTransactionRegistry(Base):
    """What analytics may do with one transaction, for one tenant."""

    __tablename__ = "analytics_transaction_registry"
    __table_args__ = (
        # Per tenant, so one customer's choices never speak for another's. NOT partitioned: this is a
        # handful of rows per tenant, so there is nothing to prune and partitioning would add planning
        # cost for no gain - the same reasoning that keeps `analytics_monthly_rollups` unpartitioned.
        UniqueConstraint("customer_code", "transaction_name",
                         name="uq_analytics_txn_registry_name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    #: NOT NULL, deliberately. A NULL here would be a second way to say "the unnamed transactions",
    #: competing with the code rule that handles them - and `UNIQUE` treats NULLs as distinct by
    #: default, so it would not even be one row.
    transaction_name: Mapped[str] = mapped_column(String(128), nullable=False)

    capture: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True,
                                          server_default="true")
    show: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True,
                                       server_default="true")
    expand: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False,
                                         server_default="false")

    #: When this transaction was first seen in the projection. Shown in the interface so a reviewer can
    #: tell a transaction that appeared this morning from one that has been running for months.
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                    default=lambda: datetime.now(timezone.utc))
    #: Who last changed a switch, and when. `NULL` means nobody has: the row is still at its defaults,
    #: which is what the interface reads to say "needs review".
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reviewed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))
