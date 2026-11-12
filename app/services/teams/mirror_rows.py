# The rows the backend mirrors into the edge's DynamoDB table (key layout and envelope).
# Copied from the Teams edge repo (app/core/mirror_rows_dto.py) by scripts/sync_contract.sh;
# edit it there, run scripts/contract_schema.py --write, then sync. Both repos test the snapshot.
"""The rows the backend mirrors into the edge's DynamoDB table for the eSmart Eye Teams app.

Shared with the backend (`app/services/teams/mirror_rows.py` there); this copy is canonical and
the backend's is copied from it, so keep it dependency-free apart from pydantic. Every row shares one
envelope and lives under one key prefix, `EYE#<customer_code>`, so the consumer's IAM policy names
one prefix and the key layout is a table in code rather than a convention in prose.

Row types so far (stage D, step 4):
- site_home: the Home snapshot for one site, or for the whole customer under the site id "all".
- sites: the customer's site list for the switcher.
Later steps add their rows here, and the edge's `KEY_LAYOUT` test keeps the Terraform prefix list
in step with this table.
"""

from pydantic import BaseModel, Field

MIRROR_SCHEMA_VERSION = 1
KEY_PREFIX = "EYE"


class KeySpec(BaseModel):
    """How one row type is keyed and how long it lives; patterns take customer_code and site_id."""

    pk: str
    sk: str
    #: None means the row never expires: the mirror removes it when it is gone.
    ttl_seconds: int | None = None

    def key(self, customer_code: str, *, site_id: str | None = None) -> tuple[str, str]:
        """The concrete pk and sk for one customer (and site).

        (site_home, "acme", site_id="BRI") -> ("EYE#acme", "SITE#BRI#HOME")
        """
        values = {"customer_code": customer_code, "site_id": site_id or ""}
        return self.pk.format(**values), self.sk.format(**values)


KEY_LAYOUT: dict[str, KeySpec] = {
    "site_home": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="SITE#{site_id}#HOME", ttl_seconds=3600
    ),
    "sites": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="SITES", ttl_seconds=None),
}


class MirrorEnvelope(BaseModel):
    """One DynamoDB item: the key, what it is, when it is from, and the payload as one JSON string
    (so DynamoDB's no-floats rule never bites and the edge decodes one attribute)."""

    pk: str
    sk: str
    row_type: str
    schema_version: int = MIRROR_SCHEMA_VERSION
    #: When the payload's figures are from (ISO 8601); the edge shows it and judges staleness by it.
    as_of: str
    #: Epoch seconds of the write.
    written_at: int
    #: A version of the source row where there is one (updated_at as epoch), else 0.
    source_version: int = 0
    #: Epoch seconds when DynamoDB may drop the row; absent for rows that never expire.
    ttl: int | None = None
    payload_json: str = Field(..., max_length=390_000)
