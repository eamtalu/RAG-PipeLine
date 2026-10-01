"""Demand forecasting over the settled pick rows.

Pure modules (no database, no clock, no settings): `series`, `models`, `backtest`, `hourly`,
`staffing`, `plan`. Store modules (`*_store.py`) read history and write predictions. `runner` ties
them together for one tenant; the worker in `app.services.workers.analytics_forecast_worker` calls
it nightly.
"""

#: Bumped whenever the candidate models, selection or interval maths change. Two versions of the
#: same forecast are two rows to compare, never one row overwritten, so accuracy history survives.
MODEL_VERSION = "forecast-v1"
