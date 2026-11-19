# User by day grid in Ask and routines (logins and transactions, company filter)

Status: approved and built on 19 Nov 2026 (step 41, chunk 217); not committed or deployed yet.

## Goal

A person asks the eSmart Eye Teams app "logins per user per day for the last month, company 920".
The answer shows a grid with users down the left and dates across the top.
A switch above the grid changes the cells between logins and transactions.
The same question can be saved as a scheduled routine and gives the same grid each time it runs.

The reference result is the grid made by hand on 19 Nov 2026 for TMP Test, company 920, 19 Oct to 19 Nov:
41 users, 19 days with logins, 202 logins in total.

## What exists today

- `wms_activity` (`app/services/analytics_agent/activity_tools.py`) groups by day, or by user, never both at once.
  Its only grid is day by hour (`matrix_of`, lines 155-187).
- `auto_heatmap` (`app/agent/presentation/charts.py:351`) turns that grid into a `HeatmapBlock`.
  Its title is fixed to "Active users by hour".
- The edge's `HeatmapBlockView.tsx` draws any labelled rows by columns with a number in each cell and scrolls sideways.
  So a users by dates grid needs no new grid component.
- `HeatmapBlock` allows up to 62 rows and 48 columns.
  A month is 32 columns; TMP Live has 24 users and TMP Test has 41.
- Login rows (`CheckOperator`) carry no company code; in TMP Test every tagged row is company 920.
- Scheduled routines run an Ask question (`app/services/tasks/commands.py`), so they get the grid with no extra work.

## Design

### 1. Company filter (backend, `activity_tools.py`)

- New optional tool argument `company`.
- One rule for every query in the tool: a row belongs to company X when it is tagged X,
  or when it is untagged and its user did at least one X-tagged transaction in the window.
- This makes logins (always untagged) follow the people who work for that company.
- Known limit: a person working for two companies has their untagged rows (logins) counted in both.
  This is stated in the tool's `notes` when the filter is used.

### 2. User by day grid (backend, `activity_tools.py`)

- Built when the call has `grain` "day" and `by` "user".
  The existing `rows`, `breakdown` and `people` outputs stay as they are.
- New result key `user_day` (not `matrix`, because `matrix` already means day by hour to `auto_chart` and `auto_heatmap`).
- Two layers over the same rows and columns:
  - `logins`: successful `CheckOperator` calls (status is not error).
  - `transactions`: every call except `GetAccessToken` and `CheckServer`, which are background calls, not work.
- Rows: one per user, labelled "Full name (ID)" (40 characters at most), busiest by logins first, then by transactions.
  More than 62 users keeps the top 62 and adds a note.
- Columns: every day of the window, labelled like "Mo 19/10"; a quiet day is a column of zeros.
  A window longer than 48 days returns no grid and a note asking for a shorter window.
- Each layer carries its own unit and band scale.
  Logins use fixed bands `[1, 2, 4, 8]`.
  Transactions use bands from the layer's own values (1, then the median, upper quartile and 90th percentile of non-zero cells, kept strictly rising).
- Tool description and `RECIPES` gain "logins per user per day last month" -> `{grain: day, by: user, days: 30}`.

### 3. Answer block (backend, `charts.py`)

- New `auto_user_day_heatmap(tool, result)` builds the `HeatmapBlock` from `user_day`.
  Title: "Logins and transactions per user per day · <start> to <end>", plus " · company 920" when filtered.
- Wired next to `auto_heatmap` wherever that is called; `SOURCE_KEYS` gains `user_day`.

### 4. Contract (edge repo is canonical: `app/core/contracts_dto.py`)

- `HeatmapBlock` gains two optional fields, so older pages keep working:
  - `layers`: up to 3 of `{label, values, unit, scale}`, each the same shape as `rows` by `columns`.
    The block's own `values`, `unit` and `scale` stay equal to the first layer, for a page that does not know layers.
  - `totals`: when true, the page shows a total per row and per column.
- Then `scripts/contract_schema.py --write` and `scripts/sync_contract.sh` copy it into the backend.
  Both repos run `test_contract_schema.py`.

### 5. Page (edge repo, `HeatmapBlockView.tsx`)

- When `layers` is present, a two-button switch above the legend ("Logins" | "Transactions") picks the layer.
  The legend and cell bands follow the chosen layer.
- When `totals` is true, a "Total" column and a "Total" row are drawn unshaded, so a total is never read as a band.
- `api/types.ts` gains the two fields.

## Alternative considered

The backend could send two heat maps one under the other, one for logins and one for transactions.
That needs no contract or page change, but the person scrolls past a 41-row grid to reach the second one.
The owner chose a switch (19 Nov 2026), so the plan changes the contract.

## Tests first

Backend, new `tests/test_wms_activity_user_day_chunk217.py`:
- a user by day grid comes back for `grain=day, by=user` with both layers and every day of the window;
- logins count only successful `CheckOperator` calls; transactions leave out `GetAccessToken` and `CheckServer`;
- the company rule keeps untagged logins of a company's users and drops a user with no tagged work;
- over 62 users keeps the top 62 with a note; over 48 days returns no grid with a note;
- the transaction bands always rise;
- `auto_user_day_heatmap` returns a block the contract accepts, with `layers` and `totals`.

Backend, golden questions (`tests/golden`): "logins per user per day for the last month for company 920" calls
`wms_activity` with `grain` day, `by` user, `company` 920.

Edge:
- `tests/test_answer_blocks.py`: `layers` with a wrong shape is refused; a block without `layers` still validates.
- `Blocks.test.tsx`: the switch changes the cell numbers and the legend; the totals row and column are right and unshaded.

The existing `test_wms_activity_chunk195.py`, chart tests and contract tests stay green.

## End to end check

1. Deploy the backend to the Matrix server and the edge to AWS, the usual way.
2. In Teams, Ask on TMP Test: "logins per user per day for the last month for company 920".
3. The grid must match the hand-made grid of 19 Nov 2026 for the same days: 42 people (41 with a login), 202 logins,
   Puskar Bhandari 20 on 19 Oct and 38 in total, Craig Price 31.
4. Flip the switch to transactions and check two users against the "Activity per hour" sheet of
   `TMP_User_Logins_2026-10-19_to_2026-11-19.xlsx`.
5. Save the question as a weekly routine, run it with "Run now", and check that the same grid comes back.
6. Check the grid at Teams desktop width and on a narrow window for overlaps and sideways scroll.

## Out of scope

- A per-company filter on any tool other than `wms_activity`.
- Exporting the grid to Excel from Teams.
- A built-in routine in the routine library (`routine_reports.py`); a saved Ask schedule covers this request.

## What changed while building it

- The company people are read once as a list (`company_workers`), then filtered by that list.
  A sub-select in the filter was planned again for every daily partition of `log_transactions`:
  a month of TMP Live ran for more than 3 minutes on the server.
  With the list, TMP Test company 920 for a month takes 59 ms for the people and 53 ms for the grid.
- A person's routine rows now stay under DynamoDB's 400 KB item limit (`MirrorEnvelope` caps the payload at 390,000 characters).
  The History row keeps blocks on the newest runs while they fit 150 KB (`fit_run_blocks`), at most ten as before.
  The Routines row no longer carries `last_result.blocks`, which the page never reads.
  Without this, ten runs of a month grid made the mirror write fail and the person's rows stopped updating.
- The people table is left out when the grid is drawn, because the grid already names every person.
- The grid is built only when the day grain is asked for together with the user cut.
  "Who was active today" (a user cut with no grain) keeps its people table.
- The golden question has no company, because the golden data carries no company tags; the unit tests cover the company.
- On the page: row labels stay in view while the days scroll sideways, day labels sit on two lines,
  counts carry thousands separators, and on a phone a long name is cut with an ellipsis (the full name on hover).
- The tab preview has a canned answer for "logins per user per day", with sample people only.
