"""Prices and the usage rows: what a run's model calls cost, summed per provider and model.

The price list is data the operator supplies (`ANALYTICS_AGENT_PRICES`), never guessed here: a
model without a price is recorded with no cost and said so in the log. The rows go to the
`UsageStore` port as plain dicts, like the trace rows.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Mapping

from app.agent.contracts import ExecutionContext, ModelPrice
from app.agent.runtime.trace import RunTrace

logger = logging.getLogger(__name__)

SETTING = "ANALYTICS_AGENT_PRICES"
PRICE_KEYS = {"input_per_million", "output_per_million", "cached_input_per_million"}


def parse_prices(raw: str) -> dict[str, ModelPrice]:
    """The price list of the setting's JSON, refused with the setting's name when malformed.

    >>> parse_prices('{"m": {"input_per_million": 1, "output_per_million": 4}}')["m"]
    ModelPrice(input_per_million=1.0, output_per_million=4.0, cached_input_per_million=None)
    >>> parse_prices("")
    {}
    """
    if not raw.strip():
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{SETTING} is not valid JSON: {exc}") from None
    if not isinstance(data, dict):
        raise ValueError(f"{SETTING} must be a JSON object of model string to price")
    prices: dict[str, ModelPrice] = {}
    for model, price in data.items():
        if (
            not isinstance(price, dict)
            or not {"input_per_million", "output_per_million"} <= set(price)
            or not set(price) <= PRICE_KEYS
        ):
            raise ValueError(
                f"{SETTING}: {model!r} needs input_per_million and output_per_million "
                f"(and optionally cached_input_per_million)"
            )
        cached = price.get("cached_input_per_million")
        prices[model] = ModelPrice(
            input_per_million=float(price["input_per_million"]),
            output_per_million=float(price["output_per_million"]),
            cached_input_per_million=None if cached is None else float(cached),
        )
    return prices


def cost_of(
    price: ModelPrice | None, *, input_tokens: int, output_tokens: int, cached_tokens: int
) -> float | None:
    """The USD cost of the tokens at the price, or None without a price.

    >>> price = ModelPrice(1.0, 4.0)
    >>> cost_of(price, input_tokens=1_000_000, output_tokens=250_000, cached_tokens=0)
    2.0
    >>> cost_of(None, input_tokens=5, output_tokens=5, cached_tokens=0) is None
    True
    """
    if price is None:
        return None
    cached_price = (
        price.cached_input_per_million
        if price.cached_input_per_million is not None
        else price.input_per_million
    )
    fresh = max(0, input_tokens - cached_tokens)
    return (
        fresh * price.input_per_million
        + cached_tokens * cached_price
        + output_tokens * price.output_per_million
    ) / 1_000_000


def usage_rows(
    trace: RunTrace, context: ExecutionContext, prices: Mapping[str, ModelPrice]
) -> list[dict[str, Any]]:
    """The run's calls as rows for the usage store, one per provider and model, priced.

    (a run of 2 calls on one model, priced) -> [{"tenant", "day", "provider", "model",
    "calls": 2, ..., "cost_usd": 0.0123}] ; (no calls) -> []
    """
    usage = trace.usage
    if not usage:
        return []
    rows = []
    for row in usage["by_model"]:
        price = prices.get(row["model"])
        if price is None:
            logger.info("agent.cost.unpriced model=%s tenant=%s", row["model"], context.tenant)
        rows.append(
            {
                "tenant": context.tenant,
                "day": context.started_at.date(),
                "provider": row["provider"],
                "model": row["model"],
                "calls": row["calls"],
                "failed_calls": row["failed_calls"],
                "input_tokens": row["input_tokens"],
                "output_tokens": row["output_tokens"],
                "cached_tokens": row["cached_tokens"],
                "cost_usd": cost_of(
                    price,
                    input_tokens=row["input_tokens"],
                    output_tokens=row["output_tokens"],
                    cached_tokens=row["cached_tokens"],
                ),
                "latency_ms": row["latency_ms"],
            }
        )
    return rows
