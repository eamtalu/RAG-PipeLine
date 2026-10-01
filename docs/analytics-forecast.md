# Demand forecast (chunks 134-141)

A nightly forecast of pick lines, units per item and pickers needed per hour, for each tenant with a `pick_release` settlement.
It is built to be judged: every prediction is stored before the fact, and once the day has passed the actual is written next to it.

## What it reads

The history is `analytics_settled_rows` for the configured settlement, read through `settle_store.read_grouped`, the same function behind the pick-releases screen.
Training, scoring and the chart's actuals all go through `app/services/analytics_forecast/history_store.py`, so a forecast and an actual never come from two code paths.
A rollout ramp at the front of the history is trimmed automatically (`series.steady_start`) and the trim is recorded in the run's `detail.history`; `ANALYTICS_FORECAST_HISTORY_START` overrides it per tenant.

## What it writes

- `analytics_predictions`: one row per (metric, grain, subject, horizon, target).
  Daily targets 1 to 14 days ahead, weekly targets for the week in progress and the next four, monthly targets for the month in progress and the next three, and hourly lines and pickers for the next eight days.
  `value` is the median, `p10` and `p90` the interval, `detail` the model, its backtest and the demand class.
- `analytics_forecast_series`: the latest word on each forecasted series (class, model, backtest WAPE, recent volume).
- `analytics_forecast_accuracy`: rolling out-of-sample scores per series and horizon, recomputed each night from the scored predictions.
- `analytics_forecast_runs`: the run ledger.

## How a series is forecast

1. Classify the demand (Syntetos-Boylan on the average interval between demand days and the size variability).
2. Race the eligible candidates in `models.py`: seasonal naive, 7-day moving average, weekday mean, Holt-Winters with an additive weekly season (only with 21 or more steady days and non-intermittent demand), Croston/TSB (only for intermittent or lumpy demand), and the mean of the two best.
3. Score each by rolling-origin backtest (`backtest.py`): train on a prefix, predict the next seven days, repeat on up to six origins.
   The lowest WAPE wins; ties go to the simpler model.
4. Walk the winner forward and size the interval per step from the backtest residuals at that step.
5. A week or month is the actual so far plus the forecast over the days still to come, flagged `partial`.
6. Hours come from the measured hour-of-day profile per weekday (`hourly.profile`) and pickers from the median lines per picker-hour (`hourly.throughput`) with a 10% buffer (`staffing.pickers_needed`).

## When it runs

`app/services/workers/analytics_forecast_worker.py`, behind `ANALYTICS_FORECAST_WORKER_ENABLED`, on the worker process.
It wakes every `ANALYTICS_FORECAST_POLL_SECONDS` and runs each tenant once per tenant-local day once that clock has passed `ANALYTICS_FORECAST_RUN_HOUR_LOCAL`, forecasting as of the day before.
`POST /api/v1/analytics/forecast/runs` runs a tenant now and answers 202 with a poll URL.

## Scoring

A bucket is scored `ANALYTICS_FORECAST_SCORE_LAG_HOURS` after it closes (two hours, so the 03:00 run can score the day that ended at midnight).
Settled rows can still change after that, so the trailing 28 days are re-scored every night and the accuracy table recomputed.
`GET /analytics/forecast/accuracy` returns `no_out_of_sample_yet` and `first_scorable_on` until a bucket has been predicted and then observed; the backtest score is reported separately and must never be shown as live accuracy.

## Endpoints

All under `/api/v1/analytics/forecast`, tenant-scoped by `X-Customer-Code`: `series`, `accuracy`, `heatmap`, `items`, `runs`, `runs/{id}`, `status`.
`app/api/v1/analytics_forecast.py` documents the parameters and shapes.

## Enabling on the server

1. `alembic upgrade head` (migration `d8f3a1c2e7b4`).
2. Set `ANALYTICS_FORECAST_WORKER_ENABLED=true` in `.env`; only the worker process starts loops.
3. Trigger once with `curl -X POST -H "X-Customer-Code: tmp-live" http://localhost:8000/api/v1/analytics/forecast/runs` and poll the returned URL.
4. Read `detail.warnings` on the run: with fewer than 21 steady days Holt-Winters is ineligible and the run says so.
