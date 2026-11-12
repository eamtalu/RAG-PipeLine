"""How fresh and how settled a customer's figures are, for the Teams app (stage D, step 4).

The analytics fold records, per tenant, the newest event it has folded (`analytics_watermark`) and
the share of contributing handheld calls that were still unsealed (`unsealed_share`). The design
stamps every tile and answer "figures provisional" when that share is above a threshold; this module
is the one place that rule lives, so the snapshot and the answer payload (step 5) agree.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_tenant_state import AnalyticsTenantState


@dataclass(frozen=True)
class Freshness:
    """When the figures are from, and whether they are still moving."""

    data_as_of: datetime | None
    unsealed_share: Decimal | None
    provisional: bool

    def as_dict(self) -> dict:
        """The snapshot's `freshness` block, every value JSON-ready.

        (as_of 10:00, share 0.3, provisional) -> {"data_as_of": "...", "unsealed_share": 0.3,
        "provisional": True, "text": "70% of handheld calls sealed, figures provisional"}
        """
        return {
            "data_as_of": self.data_as_of.isoformat() if self.data_as_of else None,
            "unsealed_share": float(self.unsealed_share) if self.unsealed_share is not None else None,
            "provisional": self.provisional,
            "text": provisional_text(self.unsealed_share) if self.provisional else "",
        }


def is_provisional(share: Decimal | None, *, threshold: Decimal) -> bool:
    """Whether figures with this unsealed share must be marked provisional.

    >>> is_provisional(Decimal("0.25"), threshold=Decimal("0.20"))
    True
    >>> is_provisional(Decimal("0.20"), threshold=Decimal("0.20"))
    False
    >>> is_provisional(None, threshold=Decimal("0.20"))
    False
    """
    return share is not None and share > threshold


def provisional_text(share: Decimal | None) -> str:
    """The notice under the greeting, worded as the design has it.

    >>> provisional_text(Decimal("0.25"))
    '75% of handheld calls sealed, figures provisional'
    >>> provisional_text(None)
    ''
    """
    if share is None:
        return ""
    sealed = int(round((Decimal(1) - share) * 100))
    return f"{sealed}% of handheld calls sealed, figures provisional"


async def freshness_for(db: AsyncSession, customer_code: str, *, threshold: Decimal) -> Freshness:
    """The tenant's freshness from its analytics state; a tenant with no state is simply not provisional.

    (watermark 09:57, share 0.3, threshold 0.2) -> Freshness(09:57, 0.3, True) ; (no row) -> (None, None, False)
    """
    state = await db.scalar(select(AnalyticsTenantState).where(AnalyticsTenantState.customer_code == customer_code))
    if state is None:
        return Freshness(data_as_of=None, unsealed_share=None, provisional=False)
    share = Decimal(str(state.unsealed_share)) if state.unsealed_share is not None else None
    return Freshness(data_as_of=state.analytics_watermark, unsealed_share=share,
                     provisional=is_provisional(share, threshold=threshold))
