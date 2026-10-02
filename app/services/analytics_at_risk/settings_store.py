"""The tenant's floors and knobs: one row, or the defaults when there is none."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import (DEFAULT_CLOSE_GRACE_MIN, DEFAULT_COVERAGE, DEFAULT_LOAD_FLOOR_MIN,
                                                      DEFAULT_MIN_SAMPLE, DEFAULT_PICK_FLOOR_MIN, DEFAULT_WINDOW_DAYS,
                                                      AnalyticsAtRiskSettings)

#: What `put` may change, and the closed range each must fall in.
RANGES: dict[str, tuple[Decimal, Decimal]] = {
    "load_floor_min": (Decimal(0), Decimal(1440)),
    "pick_floor_min": (Decimal(0), Decimal(1440)),
    "min_sample": (Decimal(1), Decimal(1000)),
    "window_days": (Decimal(7), Decimal(90)),
    "close_grace_min": (Decimal(0), Decimal(1440)),
    "coverage": (Decimal("0.5"), Decimal("0.99")),
}
FIELDS = ("enabled", *RANGES, "updated_by")


@dataclass(frozen=True)
class Settings:
    enabled: bool
    load_floor_min: int
    pick_floor_min: int
    min_sample: int
    window_days: int
    close_grace_min: int
    coverage: Decimal
    #: True when no row exists and these are the defaults.
    defaulted: bool
    updated_by: str | None = None
    updated_at: datetime | None = None


DEFAULTS = Settings(enabled=True, load_floor_min=DEFAULT_LOAD_FLOOR_MIN, pick_floor_min=DEFAULT_PICK_FLOOR_MIN,
                    min_sample=DEFAULT_MIN_SAMPLE, window_days=DEFAULT_WINDOW_DAYS, close_grace_min=DEFAULT_CLOSE_GRACE_MIN,
                    coverage=Decimal(DEFAULT_COVERAGE), defaulted=True)


def _from_row(row: AnalyticsAtRiskSettings) -> Settings:
    return Settings(enabled=bool(row.enabled), load_floor_min=int(row.load_floor_min), pick_floor_min=int(row.pick_floor_min),
                    min_sample=int(row.min_sample), window_days=int(row.window_days), close_grace_min=int(row.close_grace_min),
                    coverage=Decimal(str(row.coverage)), defaulted=False, updated_by=row.updated_by, updated_at=row.updated_at)


async def _row(db: AsyncSession, cc: str) -> AnalyticsAtRiskSettings | None:
    return await db.scalar(select(AnalyticsAtRiskSettings).where(AnalyticsAtRiskSettings.customer_code == cc))


async def effective(db: AsyncSession, cc: str) -> Settings:
    row = await _row(db, cc)
    return DEFAULTS if row is None else _from_row(row)


def validate(changes: dict) -> list[str]:
    """Problems with a set of changes, empty when they may be written."""
    problems = []
    for name, value in changes.items():
        if name not in FIELDS:
            problems.append(f"{name!r} is not a setting")
            continue
        if name in RANGES:
            low, high = RANGES[name]
            try:
                number = Decimal(str(value))
            except Exception:
                problems.append(f"{name} must be a number")
                continue
            if not (low <= number <= high):
                problems.append(f"{name} must be between {low} and {high}")
            if name != "coverage" and number != number.to_integral_value():
                problems.append(f"{name} must be a whole number of minutes or days")
    return problems


async def put(db: AsyncSession, cc: str, **changes) -> Settings:
    """Create or update the tenant's row with these changes. Does NOT commit. Ranges are the caller's
    business to check first (`validate`); this writes what it is given."""
    row = await _row(db, cc)
    if row is None:
        row = AnalyticsAtRiskSettings(customer_code=cc)
        db.add(row)
    for name, value in changes.items():
        if name not in FIELDS:
            raise ValueError(f"{name!r} is not a setting")
        if name == "coverage":
            value = Decimal(str(value))
        elif name in RANGES:
            value = int(Decimal(str(value)))
        setattr(row, name, value)
    row.updated_at = datetime.now(timezone.utc)
    await db.flush()
    return _from_row(row)
