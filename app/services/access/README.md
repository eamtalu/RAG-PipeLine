# Access control

This package is everything about **who may use eSmart Eye and what they may do**.
It covers who a person is to us, which tenant and customer they belong to, what each role may do, and how people are added, approved and removed.
Postgres is the record, and this package is the only code that writes it.
The Teams edge only reads a copy of it.

## The one door

Code outside this package imports `app.services.access` and uses the names in `__init__.py`.
It never imports a submodule.
`tests/test_access_boundary_chunk219.py` enforces three rules:

- Callers import only the front door, never `access.records`, `access.people.users` and so on.
- This package never imports a feature (Ask, groups, tasks, watchlist, advisories, analytics, the agents, notifications) or the web layer.
  Features depend on access control, never the other way round.
- The access tables are reached only through this package.
  The persistence layer that defines them, and the superadmin's tenant-binding route, are the only exceptions.

## Concepts and where they live

```
┌──────────────────┬───────────────────────────────┬──────────────────────────────┐
│ Concept          │ Question it answers           │ Where                        │
├──────────────────┼───────────────────────────────┼──────────────────────────────┤
│ Identity         │ Who is signing in?            │ the edge: tab_auth.py checks │
│                  │                               │ the Teams token (tid, oid,   │
│                  │                               │ email); nothing here         │
├──────────────────┼───────────────────────────────┼──────────────────────────────┤
│ Tenancy          │ Which customer does a tenant  │ tenancy/bindings.py,         │
│                  │ belong to? Which email        │ binding_mirror.py,           │
│                  │ domains are theirs?           │ binding_sweep.py             │
├──────────────────┼───────────────────────────────┼──────────────────────────────┤
│ Authorization    │ What may this person do,      │ authorization/matrix.yaml,   │
│                  │ at which site?                │ matrix.py, records.py        │
│                  │                               │ (AccessRecord, decide)       │
├──────────────────┼───────────────────────────────┼──────────────────────────────┤
│ People           │ How does someone get, change  │ people/users.py (create,     │
│                  │ or lose access?               │ invite, claim, expire,       │
│                  │                               │ deactivate), approvals.py    │
├──────────────────┼───────────────────────────────┼──────────────────────────────┤
│ Audit            │ Who changed what, and when?   │ audit.py                     │
├──────────────────┼───────────────────────────────┼──────────────────────────────┤
│ Distribution     │ What does the edge check on   │ edge_mirror.py               │
│                  │ every request?                │ (USER#, INVITE#, USERS,      │
│                  │                               │ ROLES, APPROVALS, AUDIT)     │
├──────────────────┼───────────────────────────────┼──────────────────────────────┤
│ Admin surfaces   │ How do Admins change it?      │ admin_commands.py (Admin     │
│                  │                               │ tab), the ops API, and       │
│                  │                               │ scripts/teams_users.py       │
├──────────────────┼───────────────────────────────┼──────────────────────────────┤
│ Usage            │ Who has access, and who was   │ usage.py                     │
│                  │ active?                       │                              │
└──────────────────┴───────────────────────────────┴──────────────────────────────┘
```

## Tables owned

- In `app/persistence/models/access.py`:
  - `access_roles`, `access_capabilities` and `access_role_capabilities`: the matrix, one cell per customer
  - `teams_users`, `teams_user_sites` and `teams_user_capabilities`: a person, their sites and their overrides
  - `access_approvals`: the queue of "with approval" requests
  - `audit_events`: append-only, purged after 90 days
- In `app/persistence/models/teams_binding.py`: `teams_tenant_bindings`, which maps a tenant to a customer and holds its `email_domains`.

A person's rights come from `record_for_user`: the customer's matrix cell for their role, replaced by any override set for that person.
Their sites come from `teams_user_sites`, or all sites.
`decide(record, capability, site)` answers allowed, needs approval or refused.
The edge holds a copy of it, written word for word alike (`teams-agent-edge/app/features/services/access/record.py`).

## Who calls it

- **The Teams consumer** (`app/teams_consumer.py`):
  - checks every question (`access_record_for`, `decide`) and stamps `touch_last_active`
  - every minute, expires approvals and invites, purges old audit rows, and rewrites the edge rows (`sweep_edge_rows`, `sweep_bindings`)
- **The command router** (`app/services/teams/commands.py`) sends `access_*` commands to `run_admin_command`, after unwrapping a confirmed Ask form.
- **The features** (advisories, groups, tasks, watchlist, Ask, the usage and watch tools):
  - read a record and `decide` before acting
  - write a line with `record_audit_event`
  - answer with `teams/command_outcome.py`
- **The superadmin**:
  - `PUT /api/v1/teams/bindings/{tenant}`, for the tenant, the customer and the email domains
  - `/api/v1/teams/access/{cc}/users|invites|matrix`
  - `scripts/teams_users.py`

## Main flows

1. **Install:** the superadmin binds the tenant, sets its email domains, and invites the first Admin by email.
2. **First sign-in:** the edge finds the `INVITE#<hash>` row and queues `access_claim`.
   `claim_invite` then links the person's object id, and the edge row `USER#<oid>#ACCESS` appears.
3. **Everyone else:** an Admin or Ops manager invites from the Admin tab.
   A Supervisor's invite becomes an approval request first.
4. **Every request:** the edge reads `USER#<oid>#ACCESS` and refuses with 403 unless `decide` allows it.
   The backend decides again before any change.
5. **Leaver:** deactivating someone removes their edge row, so they are refused on their next click.

The architecture background is in `docs/esmart-agent-architecture.md`.
The Teams side is in `teams-agent-edge/docs/teams-tab.md`.
