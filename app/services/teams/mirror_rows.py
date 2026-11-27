# The rows the backend mirrors into the edge's DynamoDB table (key layout and envelope).
# Copied from the Teams edge repo (app/core/mirror_rows_dto.py) by scripts/sync_contract.sh;
# edit it there, run scripts/contract_schema.py --write, then sync. Both repos test the snapshot.
"""The rows the backend mirrors into the edge's DynamoDB table for the eSmart Eye Teams app.

Shared with the backend (`app/services/teams/mirror_rows.py` there); this copy is canonical and
the backend's is copied from it, so keep it dependency-free apart from pydantic. Every row shares one
envelope and lives under one key prefix, `EYE#<customer_code>`, so the consumer's IAM policy names
one prefix and the key layout is a table in code rather than a convention in prose.

Row types so far:
- site_home (step 4): the Home snapshot for one site, or for the whole customer under "all".
- sites (step 4): the customer's site list for the switcher.
- user_history (step 6): one person's questions of the last 30 days, newest first, as summaries
  (question, answer preview, kinds, times, data age); rebuilt from Postgres after every answer.
- history_turn (step 6): one answered question in full (the AnswerPayload as posted), so the
  history can reopen it without a backend call; expires with the retention.
- job_progress (step 6): the steps of a run in progress, rewritten as the agent works; read by
  the poll while the job is pending, expires within the hour.
- user_access (step 7): one person's access record (role, sites, each capability's effective
  state, status); every /api/v2 route checks it. Removed when the person is deactivated.
- roles (step 7): the customer's role matrix and the role and capability names, for the Admin
  tab and the role chip.
- users (step 7): the customer's users as the Admin tab lists them (no secrets).
- approvals (step 7): the pending and recent approvals, for the queue and the badge.
- audit (step 7): the latest audit events, for the Admin tab's list.
- invite_access (step 43): one pending invite, keyed by the hash of the invited email (never the
  email itself), so the edge can tell a first sign-in that it has been invited. Removed when the
  invite is claimed, withdrawn or expires.
- advisories (step 9): the open, acknowledged and snoozed advisories with the agent roster and
  the counts, rewritten after every agent run and every action.
- exec_home (step 7d): the executive Home of screen 02: the week's figures per site and for the
  whole customer, four periods, the 30-day fill-rate series, the decisions waiting; rewritten
  every few minutes.
- user_prefs (program step 2): one person's preferences: every area resolved for them (built-in
  default, then their role's, then their own, narrowed by their access) and what they set
  themselves; rewritten after every preference command.
- site_admin (program step 4): every site with its name, whether it is active and the WMS
  company and warehouse pairs it stands for, plus the pairs seen in the last 30 days of data
  with the site each belongs to; rewritten after every site command and by the sweep.
Later steps add their rows here, and the edge's `KEY_LAYOUT` test keeps the Terraform prefix list
in step with this table.
"""

import hashlib

from pydantic import BaseModel, Field

MIRROR_SCHEMA_VERSION = 1
KEY_PREFIX = "EYE"
#: History rows outlive the 30-day retention by a day, so the purge in Postgres always runs first.
HISTORY_TTL = 31 * 24 * 3600


class KeySpec(BaseModel):
    """How one row type is keyed and how long it lives; the patterns take customer_code and, per
    row type, a site_id, a user_id (the Entra object id) or a job_id."""

    pk: str
    sk: str
    #: None means the row never expires: the mirror removes it when it is gone.
    ttl_seconds: int | None = None

    def key(
        self,
        customer_code: str,
        *,
        site_id: str | None = None,
        user_id: str | None = None,
        job_id: str | None = None,
    ) -> tuple[str, str]:
        """The concrete pk and sk for one customer and the ids the row type needs.

        (site_home, "acme", site_id="BRI") -> ("EYE#acme", "SITE#BRI#HOME") ;
        (history_turn, "acme", user_id="u-7", job_id="j1") -> ("EYE#acme", "USER#u-7#TURN#j1")
        """
        values = {
            "customer_code": customer_code,
            "site_id": site_id or "",
            "user_id": user_id or "",
            "job_id": job_id or "",
        }
        return self.pk.format(**values), self.sk.format(**values)


KEY_LAYOUT: dict[str, KeySpec] = {
    "site_home": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="SITE#{site_id}#HOME", ttl_seconds=3600
    ),
    "sites": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="SITES", ttl_seconds=None),
    "user_history": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="USER#{user_id}#HISTORY", ttl_seconds=HISTORY_TTL
    ),
    "history_turn": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}",
        sk="USER#{user_id}#TURN#{job_id}",
        ttl_seconds=HISTORY_TTL,
    ),
    "job_progress": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="JOB#{job_id}#PROGRESS", ttl_seconds=3600
    ),
    "user_access": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="USER#{user_id}#ACCESS", ttl_seconds=None
    ),
    "roles": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="ROLES", ttl_seconds=None),
    "users": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="USERS", ttl_seconds=None),
    "approvals": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="APPROVALS", ttl_seconds=None),
    "audit": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="AUDIT", ttl_seconds=None),
    #: step 43: a pending invite; user_id carries invite_key(email), not an object id
    "invite_access": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="INVITE#{user_id}", ttl_seconds=None
    ),
    "exec_home": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="EXEC#HOME", ttl_seconds=7200),
    "advisories": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="ADVISORIES", ttl_seconds=None),
    #: step 18: the Ask catalogue (topics, starters, actions, slash commands), unfiltered; the edge
    #: filters it per record. Written with the access rows.
    "ask_catalogue": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="ASK_CATALOGUE", ttl_seconds=None
    ),
    #: step 10: one row per person with their reminders, schedules, what is coming up and the suggestions
    "user_tasks": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="USER#{user_id}#TASKS", ttl_seconds=None
    ),
    #: step 15: one row per person with the runs of their routines in the last 30 days, newest first
    "user_task_runs": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="USER#{user_id}#TASK_RUNS", ttl_seconds=None
    ),
    #: step 12: the customer's notification groups and channels, and one row per person with their groups
    "groups": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="GROUPS", ttl_seconds=None),
    "user_groups": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="USER#{user_id}#GROUPS", ttl_seconds=None
    ),
    #: step 14: one row per person with their watches, today's changes and the unseen count; and the
    #: customer's watchable things (open deliveries, routes, sites, zones, locations, items)
    "user_watches": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="USER#{user_id}#WATCHES", ttl_seconds=None
    ),
    #: program step 2: one row per person with their preferences, resolved and their own
    "user_prefs": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="USER#{user_id}#PREFS", ttl_seconds=None
    ),
    #: program step 4: what the Admin tab's Sites section draws
    "site_admin": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="SITES#ADMIN", ttl_seconds=None),
    "watchables": KeySpec(pk=f"{KEY_PREFIX}#{{customer_code}}", sk="WATCHABLES", ttl_seconds=600),
    #: step 30: the Forecast page opened from Overview, per site and for "all", rewritten every five minutes
    "site_forecast": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="SITE#{site_id}#FORECAST", ttl_seconds=3600
    ),
    #: step 31: the Pick releases page opened from Overview, per site and for "all", three fixed
    #: windows, rewritten every five minutes
    "site_releases": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="SITE#{site_id}#RELEASES", ttl_seconds=3600
    ),
    #: step 34: Overview's all-sites view (every site side by side, fill rate by site by day, the last
    #: 8 weeks against the target, what changed since last week), rewritten every five minutes
    #: step 38: the routine library's reports per site and for "all" (what happened, why, what to do),
    #: rewritten every five minutes
    "site_reports": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="SITE#{site_id}#REPORTS", ttl_seconds=3600
    ),
    "all_sites": KeySpec(
        pk=f"{KEY_PREFIX}#{{customer_code}}", sk="SITE#all#SITES", ttl_seconds=3600
    ),
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


def normalize_email(email: str | None) -> str:
    """The form an email is stored and compared in: trimmed and lower case, "" for none.

    >>> normalize_email("  Jo.Bloggs@Cust.COM ")
    'jo.bloggs@cust.com'
    >>> normalize_email(None)
    ''
    """
    return (email or "").strip().lower()


def invite_key(email: str) -> str:
    """The key part of an invite row: a hash of the normalised email, so the row names nobody.

    >>> invite_key("Jo@Cust.com") == invite_key(" jo@cust.com")
    True
    >>> len(invite_key("jo@cust.com"))
    64
    """
    return hashlib.sha256(normalize_email(email).encode("utf-8")).hexdigest()


def email_domain(email: str | None) -> str:
    """The domain of an email in normalised form, "" when there is none.

    >>> email_domain("Jo@Cust.COM"), email_domain("no-at-sign")
    ('cust.com', '')
    """
    _, at, domain = normalize_email(email).rpartition("@")
    return domain if at else ""
