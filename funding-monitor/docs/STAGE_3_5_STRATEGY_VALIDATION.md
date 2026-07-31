# Stage 3.5 Strategy Validation Engine

## Purpose

Stage 3.5 adds a historical replay and validation layer for the positive funding
strategy:

```text
LONG Spot + SHORT USD-M perpetual futures
```

The module answers a research question:

```text
What would have happened if the system had historically opened a delta-neutral
position by these signal rules?
```

It is observation and research only. It does not connect to private exchange
APIs, place orders, manage balances, or change Candidate Engine behavior.

## Current Data Audit

The current PostgreSQL schema has enough data for funding-only validation:

- confirmed `funding_events.actual_funding_rate`
- historical predicted funding snapshots in `funding_snapshots`
- `instrument_mappings` for Spot/Futures eligibility
- optional `candidate_evaluations`
- optional `funding_interval_summaries`

The current schema does not store the full market data required for a real
economic backtest:

- historical spot entry price
- historical futures entry price
- historical spot exit price
- historical futures exit price
- bid/ask spread
- order book depth
- actual fee tier
- realized slippage
- exchange lot and tick-size constrained sizing

Because of that, Stage 3.5 supports two explicit modes:

1. `funding_only`
2. `full_economic`

`funding_only` stores gross funding PnL and gross funding yield only. It does not
report net PnL.

`full_economic` calculates net PnL only when a historical market-data provider
returns complete spot and futures entry/exit prices. The default provider is
intentionally unavailable and returns `insufficient_market_data`.

## Architecture

The package lives in:

```text
src/funding_monitor/strategy_validation/
```

Modules:

- `models.py`: immutable config, run/result/aggregate models, statuses, reasons
- `signal_detector.py`: signal detection and entry timing without lookahead
- `data_quality.py`: snapshot and mapping quality checks
- `market_data.py`: historical market-data provider protocol
- `economics.py`: Decimal-only funding and full-economic calculations
- `replay_engine.py`: per-event replay orchestration without SQL
- `parameter_grid.py`: deterministic parameter grid with a max-combination guard
- `repository.py`: PostgreSQL read/write layer
- `service.py`: run orchestration and persistence
- `reporting.py`: aggregate metrics, reports, CSV export helpers

Architecture rules:

- Repository reads and writes only.
- Replay Engine does not execute SQL.
- Calculators are pure and unit-testable without PostgreSQL.
- Financial arithmetic uses `Decimal`.
- Timestamps are normalized to UTC.
- Candidate Engine status, score, and rejection logic are not changed.

## Signal Detection

Two entry modes are supported:

- `fixed_time`: use the latest snapshot at or before
  `funding_time - entry_minutes_before_funding`
- `first_qualifying_signal`: enter after predicted funding has stayed above the
  configured threshold for `signal_confirmation_minutes`

Entry decisions use only snapshots with `event_time <= entry_time`. Later
snapshots can be used for after-the-fact result analysis, but never for deciding
whether the trade would have been entered.

Signal filters include:

- minimum predicted funding threshold
- minimum history duration
- maximum snapshot age
- minimum persistence ratio
- maximum funding standard deviation
- maximum prediction drop before entry

## Outcomes

Per-event outcomes are stored in `strategy_validation_results`.

Important statuses:

- `funding_only`: valid funding-only result
- `full_economic`: complete economic result with market prices
- `insufficient_market_data`: full economic mode requested but prices are missing
- `rejected`: event was not eligible or no signal was detected
- `invalid_data`: event data failed quality checks

Success in funding-only mode means:

```text
eligible signal and confirmed realized funding rate > 0
```

This indicates a gross funding receipt for a short perpetual leg. It is not a
net profitability claim.

## Database Tables

Migration `006_strategy_validation.sql` adds:

- `strategy_validation_runs`
- `strategy_validation_results`
- `strategy_validation_aggregates`

Runs store immutable configuration JSON and a deterministic configuration hash.
Results store one row per `(run_id, exchange, symbol, funding_time, config_hash)`.
Aggregates store grouped metrics as JSONB.

## CLI

Run the migration first:

```powershell
python -m funding_monitor migrate
```

Run a funding-only validation:

```powershell
python -m funding_monitor validate-strategy --limit 500
```

Run a focused validation:

```powershell
python -m funding_monitor validate-strategy `
  --from 2024-01-01T00:00:00+00:00 `
  --to 2024-02-01T00:00:00+00:00 `
  --symbols BTCUSDT,ETHUSDT `
  --funding-threshold-rate 0.0003 `
  --entry-minutes-before-funding 60
```

Run a grid comparison:

```powershell
python -m funding_monitor validate-grid `
  --funding-threshold-rates 0.0002,0.0003,0.0005 `
  --entry-minutes-grid 30,60,120 `
  --minimum-persistence-ratios 0.50,0.70 `
  --max-combinations 100
```

CLI funding thresholds are decimal rates, not percentage display values:

```text
0.0003 = 0.03%
0.03   = 3%
```

Show a persisted report:

```powershell
python -m funding_monitor validation-report --run-id 1
```

Export results:

```powershell
python -m funding_monitor validation-report --run-id 1 --export-csv data/validation_run_1.csv
```

Compare runs:

```powershell
python -m funding_monitor validation-compare --run-id 1 --run-id 2
```

## Bias Protections

Implemented protections:

- no lookahead in entry signal detection
- immutable config per run
- config hash persisted with results
- rejected and invalid cases are stored, not silently removed
- missing market data is explicit
- funding-only results do not report net PnL
- repository does not calculate metrics
- replay engine does not query PostgreSQL

Remaining research risks:

- survivorship bias if symbol history begins after the real market listing
- incomplete funding intervals when collector coverage is sparse
- no true economic result until spot/futures tradeable prices and costs are saved
- no liquidity or slippage model yet
- no exchange-constrained sizing yet

## Future Market Data Integration

To enable full economic simulation, add a provider implementing
`HistoricalMarketDataProvider` and store or fetch historical spot/futures prices
at entry and exit timestamps.

The replay engine already consumes this provider abstraction, so the core
validation logic does not need to be rewritten when a real market-data source is
added.
