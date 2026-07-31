# funding-monitor

## Purpose

`funding-monitor` collects public Binance USD-M Futures data for predicted and actual funding rates. Runtime storage uses Supabase PostgreSQL through `asyncpg`.

## Current Project Status

### Completed Functionality

- Python + asyncio runtime.
- Binance REST access through `httpx`.
- Binance WebSocket access through `websockets`.
- Supabase PostgreSQL storage through `asyncpg`.
- SQL migrations with `schema_migrations`.
- `check-db` command for PostgreSQL connectivity.
- 529 Binance USD-M perpetual symbols have been synchronized in live use.
- WebSocket endpoint:
  `wss://fstream.binance.com/market/ws/!markPrice@arr@1s`
- WebSocket collector stores snapshots in PostgreSQL.
- Funding events are created from stored snapshots.
- Funding event lifecycle works: `waiting` to `confirmed`, or `confirmation_failed`.
- Confirmed funding rates are stored without duplicate confirmations.
- Automatic WebSocket reconnect is implemented.
- Funding History Engine maintains an in-memory per-symbol snapshot window loaded from PostgreSQL.
- `history` and `metrics` commands expose cache and per-symbol window metrics.
- Spot to Futures Instrument Mapping stores metadata-only execution eligibility for supported USDT instruments.
- Stage 3 Positive Funding Candidate Engine evaluates current positive funding opportunities with rule-based statuses, score components, rejection reasons, and persistence into PostgreSQL.
- Funding Interval Analytics Foundation links confirmed realized funding events with their predicted snapshots for future reliability analysis.
- Funding Intelligence foundation prepares exchange-aware symbol profiles without using them yet.
- Stage 3.5 Strategy Validation Engine replays confirmed historical events for the positive funding long Spot plus short perpetual strategy.
- Strategy validation supports funding-only mode now and full economic mode only when complete historical market prices are available.
- Stage 3.6 Data Pipeline Reliability adds explicit snapshot persistence policy, batch snapshot flushes, collector health diagnostics, pipeline status, manual candidate evaluation, confirmation backfill, and interval summary backfill.
- Live confirmation has been verified for `COTIUSDT`, `DEXEUSDT`, `ERAUSDT`, and `ESPORTSUSDT`.
- `confirmation_failed` was `0` in the verified live run.
- `ruff`, `mypy`, and `pytest` pass for the current implementation.

### Architecture

The project uses Python, `asyncio`, `httpx`, `websockets`, `asyncpg`, and Supabase PostgreSQL.

It does not use SQLAlchemy, Docker, FastAPI, private Binance endpoints, API keys, or trading orders.

### Strategy Rules

1. Stage 3 candidate detection evaluates only positive funding.
2. The supported strategy is long Spot plus short USD-M perpetual futures.
3. The minimum gross predicted funding threshold is `0.03%`, represented as `Decimal("0.0003")` in Binance API values.
4. Raw data continues to be collected for all active symbols and all funding directions.
5. Symbols below the threshold are not removed from historical collection. They are only excluded from candidate workflows.
6. A high current predicted funding value is not a signal by itself.
7. Candidate quality also considers persistence, stability, trend, signal lifetime, time to funding, spot hedge availability, and data quality.
8. Realized funding from confirmed events must not be replaced by maximum predicted funding inside an interval.
9. The first mode is observation-only. The project must not place orders or perform real trading.

### Roadmap

- Stage 1: collector and confirmation - completed.
- Stage 2: analytical data foundation - completed.
- Stage 2.2: Funding History Engine - completed.
- Stage 2.3: Instrument Mapping - completed.
- Stage 3: Positive Funding Candidate Engine - completed.
- Stage 3.5: Strategy Validation Engine - completed.
- Stage 3.6: data collection reliability and pipeline completion - completed.
- Stage 4: market risk, spread, fees, depth, and slippage.
- Stage 5: net edge.
- Stage 6: symbol reliability.
- Stage 7: realized candidate outcomes.
- Stage 8: notifications/UI/paper trading.

## Requirements

Python 3.12, public internet access to Binance endpoints, and a Supabase PostgreSQL connection string.

## Windows PowerShell Install

```powershell
cd funding-monitor
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Environment

Create `.env` from `.env.example` and set `DATABASE_URL`. Do not commit `.env`.

Optional analytics settings:

```text
ABS_MIN_FUNDING_RATE=0.0003
DEFAULT_FUNDING_INTERVAL_HOURS=8
ANALYTICS_OBSERVATION_ONLY=true
WINDOW_CACHE_MINUTES=120
DEFAULT_METRICS_WINDOW=60
BINANCE_SPOT_BASE_URL=https://api.binance.com
SUPPORTED_SPOT_QUOTE_ASSET=USDT
INSTRUMENT_MAPPING_SYNC_ON_STARTUP=false
CANDIDATE_ENGINE_ENABLED=true
CANDIDATE_MIN_FUNDING_RATE=0.0003
CANDIDATE_MIN_HISTORY_MINUTES=15
CANDIDATE_PRIMARY_WINDOW_MINUTES=30
CANDIDATE_SHORT_WINDOW_MINUTES=5
CANDIDATE_LONG_WINDOW_MINUTES=60
CANDIDATE_MIN_SNAPSHOT_COUNT=10
CANDIDATE_MAX_SNAPSHOT_AGE_SECONDS=120
CANDIDATE_MIN_PERSISTENCE_RATIO=0.70
CANDIDATE_MAX_STD_DEV=0.0002
CANDIDATE_MAX_THRESHOLD_CROSSINGS=4
CANDIDATE_MAX_DIRECTION_CHANGES=8
CANDIDATE_LATE_SPIKE_LOOKBACK_MINUTES=5
CANDIDATE_LATE_SPIKE_MIN_JUMP_RATIO=1.50
CANDIDATE_DETERIORATION_LOOKBACK_MINUTES=5
CANDIDATE_MAX_NEGATIVE_VELOCITY=-0.00002
CANDIDATE_MIN_MINUTES_TO_FUNDING=5
CANDIDATE_MAX_MINUTES_TO_FUNDING=480
CANDIDATE_STRONG_SCORE=80
CANDIDATE_MIN_SCORE=60
CANDIDATE_PERSIST_INTERVAL_SECONDS=60
CANDIDATE_MAX_RESULTS=50
FUNDING_INTERVAL_POINT_TOLERANCE_SECONDS=90
FUNDING_INTERVAL_SUMMARY_BATCH_SIZE=500
SNAPSHOT_PERSIST_INTERVAL_SECONDS=60
SNAPSHOT_BATCH_SIZE=500
SNAPSHOT_FLUSH_INTERVAL_SECONDS=5
COLLECTOR_HEALTH_WINDOW_MINUTES=5
COLLECTOR_HEALTH_MAX_SNAPSHOT_AGE_SECONDS=180
COLLECTOR_HEALTH_MIN_COVERAGE_RATIO=0.80
CANDIDATE_EVALUATION_INTERVAL_SECONDS=60
INTERVAL_SUMMARY_BUILD_INTERVAL_SECONDS=300
CONFIRMATION_BACKFILL_INTERVAL_SECONDS=300
CONFIRMATION_BACKFILL_BATCH_SIZE=100
CONFIRMATION_OVERDUE_GRACE_MINUTES=30
```

`ABS_MIN_FUNDING_RATE` is used for status and aggregate reporting. It does not stop snapshots below the threshold from being saved.

`WINDOW_CACHE_MINUTES` controls the in-memory per-symbol history cache. `DEFAULT_METRICS_WINDOW` controls the default window used by the `metrics` command.

`BINANCE_SPOT_BASE_URL` is used only for public Spot `exchangeInfo`. `SUPPORTED_SPOT_QUOTE_ASSET` is currently `USDT`. `INSTRUMENT_MAPPING_SYNC_ON_STARTUP` is present for future startup integration and defaults to `false`.

Candidate velocity and acceleration use funding-rate change per second, matching `FundingHistoryService`.

In Supabase, get the PostgreSQL connection string from Project Settings, Database, Connection string. For local development, use the Session Pooler connection string when available.

```powershell
Copy-Item .env.example .env
notepad .env
```

## Run Commands

```powershell
python -m funding_monitor migrate
python -m funding_monitor check-db
python -m funding_monitor sync-symbols
python -m funding_monitor collect
python -m funding_monitor status
python -m funding_monitor snapshot-stats
python -m funding_monitor snapshot-stats --minutes 60
python -m funding_monitor collector-health
python -m funding_monitor collector-health --json
python -m funding_monitor pipeline-status
python -m funding_monitor coverage-report --minutes 60 --limit 20
python -m funding_monitor history
python -m funding_monitor metrics BTCUSDT
python -m funding_monitor metrics BTCUSDT --window-minutes 15
python -m funding_monitor sync-instrument-mappings
python -m funding_monitor instrument-mappings
python -m funding_monitor instrument-mappings --status matched
python -m funding_monitor instrument-mappings --status ambiguous
python -m funding_monitor instrument-mappings --symbol BTCUSDT
python -m funding_monitor candidates
python -m funding_monitor candidates --top 20
python -m funding_monitor candidates --min-score 60
python -m funding_monitor candidates --status candidate
python -m funding_monitor candidates --status strong_candidate
python -m funding_monitor candidates --symbol BTCUSDT
python -m funding_monitor candidates --include-rejected
python -m funding_monitor candidates --no-persist
python -m funding_monitor candidates --json
python -m funding_monitor evaluate-candidates
python -m funding_monitor evaluate-candidates --symbols BTCUSDT,ETHUSDT --dry-run
python -m funding_monitor candidate-rejections
python -m funding_monitor build-funding-interval-summaries
python -m funding_monitor backfill-confirmations
python -m funding_monitor backfill-funding-intervals
python -m funding_monitor backfill-funding-intervals --from 2024-01-01T00:00:00+00:00 --to 2024-01-02T00:00:00+00:00 --symbols BTCUSDT
python -m funding_monitor validate-strategy --limit 500
python -m funding_monitor validate-strategy --symbols BTCUSDT,ETHUSDT --funding-threshold-rate 0.0003
python -m funding_monitor validate-grid --funding-threshold-rates 0.0002,0.0003 --entry-minutes-grid 30,60
python -m funding_monitor validation-report --run-id 1
python -m funding_monitor validation-report --run-id 1 --export-csv data/validation_run_1.csv
python -m funding_monitor validation-compare --run-id 1 --run-id 2
python -m funding_monitor recent-events --limit 20
python -m funding_monitor export-csv --output data/funding_events.csv
```

`python -m funding_monitor init-db` is kept as an alias for `migrate`.

For continuous operation, run:

```powershell
python -m funding_monitor collect
```

Stop it with `Ctrl+C`. The collector handles SIGINT/SIGTERM by stopping the
WebSocket loop, flushing the current snapshot batch, cancelling background
pipeline tasks, closing confirmation tasks, and closing the PostgreSQL pool.

## Data Pipeline Reliability

The snapshot persistence policy is explicit:

- outside the funding window, save one heartbeat snapshot per symbol every
  `SNAPSHOT_PERSIST_INTERVAL_SECONDS`;
- inside the configured pre/post funding window, use
  `DETAILED_SNAPSHOT_INTERVAL_SECONDS`;
- accumulate snapshots in memory and flush them in batches controlled by
  `SNAPSHOT_BATCH_SIZE` and `SNAPSHOT_FLUSH_INTERVAL_SECONDS`;
- keep storing all active symbols and all funding directions.

With 529 active symbols and `SNAPSHOT_PERSIST_INTERVAL_SECONDS=60`, the normal
expected volume is about `529 * 1,440 = 761,760` snapshots per day, or about
`22,852,800` snapshots per 30-day month before detailed funding-window writes.
If the detailed interval remains `1` second for three 15-minute funding windows
per day, the daily volume can rise to about `2,166,255` snapshots.

The collector also starts a lightweight operational orchestrator:

```text
collect snapshots
-> create/observe funding events
-> confirm due funding events
-> evaluate candidates on a schedule
-> build funding interval summaries for confirmed events
-> repeat
```

Use `collector-health` for critical collection health and `pipeline-status` for
all pipeline stages. `collector-health --json` is machine-readable and exits
non-zero when snapshots are stale, coverage is too low, confirmation backlog is
critical, or candidate evaluation has stopped.

Pipeline warning codes:

- `SNAPSHOT_COLLECTION_STALE`
- `LOW_SYMBOL_COVERAGE`
- `CONFIRMATION_BACKLOG`
- `CANDIDATE_PIPELINE_NOT_RUNNING`
- `INTERVAL_SUMMARY_BACKLOG`

Manual backfill commands are safe to rerun. They do not create artificial market
data and do not replace incomplete intervals with false complete summaries.

## Tables

`schema_migrations` stores applied SQL migration filenames and timestamps.

`symbols` stores active USDT perpetual contract metadata and the funding interval in hours.

`funding_snapshots` stores WebSocket mark price snapshots. Decimal values are PostgreSQL `NUMERIC` values and timestamps are UTC `TIMESTAMPTZ` values. Snapshot analytics include `funding_rate`, `seconds_to_funding`, `premium_rate`, `funding_direction`, and `funding_interval_hours`.

`funding_events` stores one row per symbol and funding time, including checkpoint predictions, actual funding rate, prediction error, confirmation status, and the next predicted rate seen after funding.

`instrument_mappings` stores metadata-only Spot to Futures mapping status and strategy availability for futures symbols. It is not tied to current funding thresholds and does not create trading signals.

`candidate_evaluations` stores controlled current candidate evaluation snapshots with status, score components, penalties, rejection reasons, warning flags, metrics details, and engine version.

`funding_interval_summaries` stores aggregated predicted funding behavior inside a funding interval and links it to the confirmed realized funding event. It is exchange-aware and includes peak predicted timestamp, signal start, and positive streak fields for future analytics.

`symbol_funding_profiles` is a prepared table for the future Funding Intelligence Engine. Stage 3 does not populate it.

`strategy_validation_runs` stores immutable Strategy Validation Engine run configuration, dataset selection, status, and counts.

`strategy_validation_results` stores one historical replay result per funding event. Funding-only results store gross funding PnL/yield and never claim net PnL without market prices.

`strategy_validation_aggregates` stores grouped validation metrics for reports and run comparisons.

## Funding History Engine

`FundingHistoryService` is the analytical history layer for later candidate detection, stability, scoring, and net edge stages. PostgreSQL remains the source of truth. On reload, the service loads the last `WINDOW_CACHE_MINUTES` minutes once, then new collector snapshots update the in-memory cache without a SQL query per snapshot.

`WindowCache` keeps one `collections.deque` per symbol and prunes snapshots older than the configured window.

`FundingMetrics` is calculated from snapshots for a single symbol and window. It includes current, min, max, mean, median, standard deviation, absolute mean, threshold persistence, threshold crossings, direction changes, deltas, velocity, acceleration, history duration, and snapshot count.

This layer does not implement candidate detection, scoring, net edge, borrow checks, notifications, UI, or trading.

## Instrument Mapping

Instrument mapping checks whether a USD-M perpetual futures symbol has a safe, active USDT spot pair. PostgreSQL is the source of truth through the `instrument_mappings` table.

Funding snapshots and history metrics continue to be stored for every active futures symbol even when no spot pair exists. Missing or ambiguous spot mapping affects execution eligibility only; it does not stop observation or historical analysis.

Positive funding is theoretically executed as short perpetual plus long spot. `positive_strategy_available` is true only when the futures contract is a trading USDT perpetual margined in USDT and the matching spot pair is trading with spot trading allowed.

Negative funding is theoretically executed as long perpetual plus short spot or borrowed spot asset. Margin borrow checks are not implemented yet, so `negative_strategy_available` remains false and matched instruments use `negative_strategy_status=borrow_check_not_implemented`.

Mapping statuses are `matched`, `missing`, `ambiguous`, `unsupported`, and `spot_trading_disabled`.

Multiplier contracts such as `1000PEPEUSDT` are marked `ambiguous`. They are not automatically mapped to `PEPEUSDT` because the quantity multiplier must be explicit before execution logic can be safe.

The mapping layer does not implement Margin API, API keys, borrow availability, fees, order book data, scoring, candidate detection, paper trading, or real orders.

## Stage 3 Candidate Engine

Stage 3 evaluates only positive funding opportunities for long Spot plus short perpetual futures. It requires a matched instrument mapping and an active spot pair before a symbol can become a candidate.

Statuses are `strong_candidate`, `candidate`, `weak_candidate`, `observing`, `deteriorating`, `funding_falling`, `unstable`, `late_spike`, `too_early`, `too_late`, `stale`, `insufficient_history`, `rejected`, and `expired`.

Score components are funding magnitude, persistence, stability, trend, signal lifetime, and time to funding. Penalties cover late spikes, deterioration, stale data, insufficient history, instability, threshold crossings, and too-close-to-funding risk.

Score calculation is split into dedicated calculators. `CandidateScoringService` only assembles the score and keeps the existing formulas unchanged.

The command `python -m funding_monitor candidates` computes current evaluations from existing snapshots, history metrics, and instrument mappings. It persists one row per symbol per configured time bucket unless `--no-persist` is used.

The command `python -m funding_monitor candidate-rejections` aggregates machine-readable rejection reasons from latest persisted evaluations.

The command `python -m funding_monitor build-funding-interval-summaries` builds idempotent summaries that compare predicted snapshots with confirmed realized funding. Realized funding always comes from confirmed `funding_events.actual_funding_rate`.

See `STAGE_3_CANDIDATE_ENGINE.md` for full details.

## Funding Intelligence Foundation

The future Funding Intelligence Engine will operate on `funding_interval_summaries`, not on current WebSocket snapshots. It will build exchange-aware symbol profiles and keep Signal Frequency separate from Realized Reliability.

Signal Frequency means how often predicted funding became high. Realized Reliability means how often a high predicted signal ended as a high confirmed payout.

The placeholder module is `src/funding_monitor/funding_intelligence.py`.

## Stage 3.5 Strategy Validation Engine

Strategy Validation Engine is a research/backtesting layer for the positive funding strategy: long Spot plus short USD-M perpetual futures. It replays confirmed historical funding events using stored snapshots and instrument mappings.

The default mode is `funding_only`. It validates signal quality, realized funding, prediction error, persistence, success frequency, and gross funding yield. It does not report net PnL. CLI funding thresholds are decimal rates: `--funding-threshold-rate 0.0003` means `0.03%`.

`full_economic` mode is available as an explicit status path, but the default market-data provider is unavailable. Until real historical spot/futures entry and exit prices, fees, spread, depth, and slippage are stored or supplied, full economic runs return `insufficient_market_data` for otherwise eligible events.

See `docs/STAGE_3_5_STRATEGY_VALIDATION.md` for details.

## Predicted Rate And Actual Rate

The predicted rate is Binance's funding rate value from the mark price stream before the funding time. The actual rate is the final funding rate returned later by the Binance funding rate history REST endpoint.

## SQLite

SQLite is no longer used at runtime. Existing `data/funding_monitor.db` files are not deleted, changed, or imported automatically. Historical SQLite import is not implemented yet.

## Safety Notice

This application does not trade, place orders, read private account data, or use API keys.
