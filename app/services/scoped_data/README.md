# Scoped data

The one seam between the agent's data tools and the data tables.
It answers one question: which rows may this run read.

## What it holds

- `scope.py` - `DataScope`: the customer, the sites and the data categories one run may read.
  It is built from a person's access record, or for a system caller that has no person.
- `registry.py` - the rule for every table a data tool may read.
  A table with no rule cannot be read through a scoped session.
- `session.py` - `limit_session` and `open_session`: a database session that adds the scope to every select it runs, wherever the query is built.

## The rules

- `SiteLimited` - rows of one customer and one site, named by a column.
- `SubjectLimited` - forecast rows; a site-limited person reads their sites' subjects and never the customer total.
- `ThroughTransaction` - log lines and their assignment rows, which carry no site; read by whoever may read the transaction they belong to.
- `AllSitesOnly` - operational rows that have no site; only a person who may see every site reads them.
  No table uses it today; a new table without a site starts here.
- `CustomerWide` - settings and reference data.
- `OwnedBy` - a child table with no customer column, limited through the row that owns it.
- `Shared` - a list that is the same for every customer and names no one.

## Adding a table

Add one line to `RULES` in `registry.py` with the rule that fits.
Access control classifies its own tables in `app/services/access/tables.py`, because no other package imports the access models.

## Boundary

- This package imports access control through its front door, the data models, and SQLAlchemy.
  It imports no feature, no agent code and no web code.
- Other code imports `app.services.scoped_data` itself, never a file inside it.
- `tests/test_scoped_data_boundary_chunk220.py` holds both rules.

## Who calls it

- `app/teams_consumer.py` builds the asker's `DataScope` for every question and every routine run.
- `app/services/analytics_agent/namespaces.py` opens a scoped session for every call of a logs or analytics tool.
- A caller with no person (the web's service token, the golden questions) reads every site of its customer.
