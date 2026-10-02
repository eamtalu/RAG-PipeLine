"""Deliveries at risk: which deliveries are behind their route's normal rhythm before the van leaves.

Pure module (no database, no clock, no settings): `model`. Store modules (`*_store.py`) read the
settled rows, lookups and facts that describe a delivery, and write the board, the check ledger, the
route profiles and the settings. `runner` evaluates one tenant; the worker in
`app.services.workers.analytics_at_risk_worker` calls it every minute and learns the route profiles
once a day.
"""

#: Bumped whenever the tier rule, the outcome rule or the learning changes. A profile or an outcome
#: row carries the version it was decided under, so a rule change never rewrites history quietly.
#: v2 (2026-10-03): a pick line counts as done once confirmed, a short pick included; v1 judged on
#: lines that moved stock and flagged one closed delivery in four as late on the live data.
#: v3 (2026-10-03): a package is known only from a pick line that moved stock; hand-made packages no pick
#: filled and short lines' package numbers made half of v2's "never loaded" rows.
RULE_VERSION = "at-risk-v3"
