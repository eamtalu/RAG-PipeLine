"""Push tenant bindings to the edge's DynamoDB table.

The edge must decide "is this tenant onboarded?" without calling this server. So the rule is: Postgres
decides, DynamoDB reflects. Every write to a binding is followed by a mirror; if the mirror fails the
row keeps `mirrored_at` behind `updated_at` and the consumer's periodic sweep pushes it again.

Key layout matches the edge's `DynamoTenantBindingStore`: pk = TENANT#<tenant_id>, sk = BINDING.
"""

import asyncio
import logging
from typing import Protocol

from app.persistence.models.teams_binding import TeamsTenantBinding

logger = logging.getLogger(__name__)


class BindingMirror(Protocol):
    async def put(self, binding: TeamsTenantBinding) -> None: ...
    async def remove(self, tenant_id: str) -> None: ...


class DynamoBindingMirror:
    def __init__(self, table_name: str, *, region_name: str, endpoint_url: str | None = None):
        import boto3  # local import: boto3 is only needed when the Teams integration is configured
        self._table = boto3.resource("dynamodb", region_name=region_name,
                                     endpoint_url=endpoint_url).Table(table_name)

    async def put(self, binding: TeamsTenantBinding) -> None:
        item = {"pk": f"TENANT#{binding.tenant_id}", "sk": "BINDING",
                "customer_code": binding.customer_code, "enabled": binding.enabled}
        if binding.display_name:
            item["display_name"] = binding.display_name
        await asyncio.to_thread(lambda: self._table.put_item(Item=item))

    async def remove(self, tenant_id: str) -> None:
        await asyncio.to_thread(
            lambda: self._table.delete_item(Key={"pk": f"TENANT#{tenant_id}", "sk": "BINDING"}))


class UnconfiguredMirror:
    """Used when no edge table is configured: writes succeed locally and are reported as unmirrored."""

    async def put(self, binding: TeamsTenantBinding) -> None:
        raise RuntimeError("teams_edge_dynamodb_table is not configured; binding not mirrored")

    async def remove(self, tenant_id: str) -> None:
        raise RuntimeError("teams_edge_dynamodb_table is not configured; binding not removed from edge")


class InMemoryMirror:
    def __init__(self, *, fail: bool = False):
        self.items: dict[str, dict] = {}
        self.fail = fail

    async def put(self, binding: TeamsTenantBinding) -> None:
        if self.fail:
            raise RuntimeError("mirror down")
        self.items[binding.tenant_id] = {"customer_code": binding.customer_code,
                                         "enabled": binding.enabled,
                                         "display_name": binding.display_name}

    async def remove(self, tenant_id: str) -> None:
        if self.fail:
            raise RuntimeError("mirror down")
        self.items.pop(tenant_id, None)


def build_mirror_from_settings() -> BindingMirror:
    from app.settings import settings
    if not settings.teams_edge_dynamodb_table:
        return UnconfiguredMirror()
    return DynamoBindingMirror(settings.teams_edge_dynamodb_table, region_name=settings.teams_aws_region,
                               endpoint_url=settings.teams_aws_endpoint_url or None)
