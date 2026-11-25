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

## Entity relationships

Every table except the two lookups carries `customer_code`.
It is a real foreign key to `customers`, which the data side owns.
Those lines are left out of the drawing so the access relationships stay readable.

```
┌─────────────────────────┐                ┌─────────────────────────┐
│ teams_tenant_bindings   │                │ access_capabilities     │
│ PK id                   │                │ PK code  (8, from yaml) │
│ UK tenant_id   (Entra)  │                └──┬───────────────────┬──┘
│    customer_code        │                   │1                  │1
│    email_domains[]      │                   │N                  │
│    enabled, mirrored_at │      ┌────────────▼────────────┐      │
└─────────────────────────┘      │ access_role_capabilities│      │
                                 │ PK customer_code,       │      │
┌─────────────────────────┐ 1  N │    role_code,           │      │
│ access_roles            ├─────▶│    capability_code      │      │
│ PK code  (6, from yaml) │      │    state                │      │
└────────────┬────────────┘      └─────────────────────────┘      │
             │1                                                   │
             │N                                                   │N
┌────────────▼────────────┐ 1  N ┌─────────────────────────┐      │
│ teams_users             ├─────▶│ teams_user_capabilities │◀─────┘
│ PK id                   │      │ PK user_id,             │
│ UK customer, object_id  │      │    capability_code      │
│ UK customer, email      │      │    state, approval_id ┄┄┼┄┄┐
│    role_code, status    │      └─────────────────────────┘  ┆
│    all_sites            │                                   ┆
│    claimed_at           │ 1  N ┌─────────────────────────┐  ┆
│    last_active_at       ├─────▶│ teams_user_sites        │  ┆
└────────────┬────────────┘      │ PK user_id, site_id     │  ┆
             ┆                   │ FK (customer, site) to  │  ┆
             ┆ object_id         │    tenant_sites         │  ┆
             ┆ (no FK)           └─────────────────────────┘  ┆
┌────────────▼────────────┐      ┌─────────────────────────┐  ┆
│ audit_events            │      │ access_approvals        │◀┄┘
│ PK id                   │      │ PK id                   │
│    actor_object_id      │      │ UK customer, reference  │
│    action, decision     │      │    kind, requested_by   │
│    subject_kind, _id    │      │    payload, status      │
│    text                 │      │    expires_at  (24 h)   │
└─────────────────────────┘      └─────────────────────────┘

 ─────▶  real foreign key, 1 to N
 ┄┄┄┄▶  link by value only, no foreign key
```

- `teams_users.object_id` is the person's Entra object id.
  It is NULL while an email invite is pending and is filled in at their first sign-in.
- `access_approvals.requested_by` and `audit_events.actor_object_id` hold an object id.
  They are not foreign keys.
  The history must survive the person being removed.
- `teams_user_capabilities.approval_id` points at the approval that granted the override.
  It is empty when an Admin set the override directly.
- `teams_tenant_bindings` has no link to people.
  It only says which customer a tenant belongs to, and which email domains an invite may use.

## What the edge reads

The edge never reaches Postgres.
`edge_mirror.py` and `tenancy/binding_mirror.py` copy what it needs into DynamoDB every minute.

```
┌──────────────────────────────┬───────────────────────────────┬────────────────┐
│ Postgres tables              │ DynamoDB row                  │ Edge uses it   │
├──────────────────────────────┼───────────────────────────────┼────────────────┤
│ teams_tenant_bindings        │ TENANT#<tid> / BINDING        │ every request: │
│                              │                               │ tenant to      │
│                              │                               │ customer       │
├──────────────────────────────┼───────────────────────────────┼────────────────┤
│ teams_users (active)         │ EYE#<cc> /                    │ every request: │
│ + teams_user_sites           │ USER#<oid>#ACCESS             │ decide()       │
│ + teams_user_capabilities    │                               │                │
│ + access_role_capabilities   │                               │                │
├──────────────────────────────┼───────────────────────────────┼────────────────┤
│ teams_users (pending invite) │ EYE#<cc> /                    │ first sign-in  │
│                              │ INVITE#<sha256(email)>        │ only           │
├──────────────────────────────┼───────────────────────────────┼────────────────┤
│ teams_users + sites          │ EYE#<cc> / USERS              │ Admin tab list │
├──────────────────────────────┼───────────────────────────────┼────────────────┤
│ access_roles, capabilities,  │ EYE#<cc> / ROLES              │ Admin tab      │
│ access_role_capabilities     │                               │ matrix         │
├──────────────────────────────┼───────────────────────────────┼────────────────┤
│ access_approvals (pending)   │ EYE#<cc> / APPROVALS          │ Approvals list │
├──────────────────────────────┼───────────────────────────────┼────────────────┤
│ audit_events (newest 100)    │ EYE#<cc> / AUDIT              │ Audit list     │
└──────────────────────────────┴───────────────────────────────┴────────────────┘
```

## Call flow, step by step, with the tables each step touches

`R` is a read, `W` is a write.
Every change also writes one `audit_events` row; it is listed once per flow, not on every line.

### Flow A: install (the superadmin, once per customer)

```
┌───┬────────────────────────────────┬──────────────────────────┬────────────────┐
│ # │ What happens                   │ Postgres                 │ Edge row       │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 1 │ Bind the tenant to the         │ W teams_tenant_bindings  │ TENANT#<tid>   │
│   │ customer and set its email     │   (mirrored_at set once  │ BINDING        │
│   │ domains: PUT /api/v1/teams/    │   the edge row is        │                │
│   │ bindings/{tenant}              │   written)               │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 2 │ The customer's matrix is       │ R access_roles,          │ ROLES          │
│   │ seeded from matrix.yaml on     │   access_capabilities    │                │
│   │ first use (ensure_matrix)      │ W access_role_           │                │
│   │                                │   capabilities (48 rows) │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 3 │ Invite the first Admin by      │ R teams_tenant_bindings  │ INVITE#<hash>  │
│   │ email: /api/v1/teams/access/   │   (domain check)         │ USERS, AUDIT   │
│   │ {cc}/invites or                │ W teams_users  (pending, │                │
│   │ scripts/teams_users.py         │   object_id NULL)        │                │
│   │                                │ W teams_user_sites       │                │
│   │                                │ W audit_events           │                │
└───┴────────────────────────────────┴──────────────────────────┴────────────────┘
```

### Flow B: first sign-in (the invited person opens the app)

```
┌───┬────────────────────────────────┬──────────────────────────┬────────────────┐
│ # │ What happens                   │ Postgres                 │ Edge row       │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 1 │ The edge checks the Teams      │ (none, edge only)        │ R TENANT#<tid> │
│   │ token and maps the tenant to   │                          │   BINDING      │
│   │ the customer                   │                          │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 2 │ No USER#<oid>#ACCESS yet, so   │ (none, edge only)        │ R USER#<oid>   │
│   │ the edge looks for an invite   │                          │ R INVITE#<hash>│
│   │ for the token's email, checks  │                          │                │
│   │ the domain, queues             │                          │                │
│   │ access_claim; page shows       │                          │                │
│   │ "setting up"                   │                          │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 3 │ The consumer runs claim_invite │ R teams_users  (pending, │                │
│   │                                │   by email)              │                │
│   │                                │ W teams_users  object_id,│                │
│   │                                │   status active,         │                │
│   │                                │   claimed_at             │                │
│   │                                │ W audit_events           │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 4 │ The next sweep writes the      │ R teams_users, sites,    │ W USER#<oid>   │
│   │ person's record; the page      │   user_capabilities,     │   #ACCESS      │
│   │ polls and opens                │   role_capabilities      │ INVITE# removed│
│   │                                │                          │   after a day  │
└───┴────────────────────────────────┴──────────────────────────┴────────────────┘
```

### Flow C: an Admin, Ops manager or Supervisor adds someone

```
┌───┬────────────────────────────────┬──────────────────────────┬────────────────┐
│ # │ What happens                   │ Postgres                 │ Edge row       │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 1 │ Invite from the Admin tab; the │ (none, edge only)        │ R USER#<oid>   │
│   │ edge checks manage_users and   │                          │   #ACCESS      │
│   │ queues access_invite           │                          │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 2 │ The consumer decides again     │ R teams_users,           │                │
│   │ (manage_users)                 │   role_capabilities,     │                │
│   │                                │   user_capabilities      │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 3a│ Allowed (Admin, Ops manager):  │ W teams_users  (pending) │ INVITE#<hash>  │
│   │ the invite is made at once     │ W teams_user_sites       │ USERS          │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 3b│ With approval (Supervisor):    │ W access_approvals       │ APPROVALS      │
│   │ a request for an Ops manager,  │   kind create_user,      │                │
│   │ open 24 h                      │   status pending         │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 4 │ An Ops manager approves; the   │ W access_approvals       │ INVITE#<hash>  │
│   │ invite is made as in 3a        │   status approved        │ USERS          │
│   │                                │ W teams_users  (pending) │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 5 │ Every change above             │ W audit_events           │ AUDIT          │
└───┴────────────────────────────────┴──────────────────────────┴────────────────┘
```

The new person then signs in as in Flow B.

### Flow D: every request after that

```
┌───┬────────────────────────────────┬──────────────────────────┬────────────────┐
│ # │ What happens                   │ Postgres                 │ Edge row       │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 1 │ The edge refuses with 403      │ (none, edge only)        │ R USER#<oid>   │
│   │ unless decide() allows the     │                          │   #ACCESS      │
│   │ capability at that site        │                          │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 2 │ A question or change goes to   │ R teams_users,           │                │
│   │ the consumer, which builds the │   teams_user_sites,      │                │
│   │ record and decides again       │   user_capabilities,     │                │
│   │ (access_record_for, decide)    │   role_capabilities      │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 3 │ It stamps the person as active │ W teams_users            │                │
│   │                                │   last_active_at         │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 4 │ A change that needs approval   │ W access_approvals       │ APPROVALS      │
│   │ (grant_capability, add_sites)  │ on approve:              │ USER#<oid>     │
│   │ waits for an approver          │ W teams_user_capabilities│   #ACCESS      │
│   │                                │   or teams_user_sites    │                │
└───┴────────────────────────────────┴──────────────────────────┴────────────────┘
```

### Flow E: leaver, and the minute-by-minute housekeeping

```
┌───┬────────────────────────────────┬──────────────────────────┬────────────────┐
│ # │ What happens                   │ Postgres                 │ Edge row       │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 1 │ An Admin deactivates someone   │ W teams_users  status    │ USER#<oid>     │
│   │                                │   deactivated            │   #ACCESS      │
│   │                                │ W audit_events           │   removed      │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 2 │ Every minute: expire           │ W access_approvals       │ APPROVALS      │
│   │ approvals older than 24 h      │   status expired         │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 3 │ Every minute: expire invites   │ W teams_users  status    │ INVITE#        │
│   │ nobody claimed in time         │   expired                │   removed      │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 4 │ Every minute: purge audit      │ W audit_events  (delete) │ AUDIT          │
│   │ rows older than 90 days        │                          │                │
├───┼────────────────────────────────┼──────────────────────────┼────────────────┤
│ 5 │ Every minute: rewrite changed  │ R all of the above       │ all EYE#<cc>   │
│   │ edge rows (sweep_edge_rows,    │ W teams_tenant_bindings  │ rows, TENANT#  │
│   │ sweep_bindings)                │   mirrored_at            │                │
└───┴────────────────────────────────┴──────────────────────────┴────────────────┘
```

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
