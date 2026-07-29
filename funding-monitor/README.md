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
- Live confirmation has been verified for `COTIUSDT`, `DEXEUSDT`, `ERAUSDT`, and `ESPORTSUSDT`.
- `confirmation_failed` was `0` in the verified live run.
- `ruff`, `mypy`, and `pytest` pass for the current implementation.

### Architecture

The project uses Python, `asyncio`, `httpx`, `websockets`, `asyncpg`, and Supabase PostgreSQL.

It does not use SQLAlchemy, Docker, FastAPI, private Binance endpoints, API keys, or trading orders.

### Strategy Rules

1. The system must work with both positive and negative funding.
2. The minimum absolute gross funding threshold is `0.03%`, represented as `Decimal("0.0003")` in Binance API values.
3. The first candidate threshold is `abs(funding_rate) >= Decimal("0.0003")`.
4. Positive funding direction means short perpetual plus long spot. Negative funding direction means long perpetual plus short spot or borrowed spot asset.
5. Raw data continues to be collected for all active symbols.
6. Symbols below the threshold are not removed from historical collection. They are only excluded from later expensive analytics and signal workflows.
7. A high current funding value is not a signal by itself.
8. Later analytics must consider time to funding, persistence above threshold, funding direction, sign changes, late spikes, funding drops before payment, funding velocity, funding acceleration, mark/index premium, price movement, volatility, liquidity, spread, depth, slippage, spot hedge availability, borrow availability and cost, fees, funding interval, caps/floors, expected net edge, and historical symbol reliability.
9. The first mode is observation-only. The project must not place orders or perform real trading.

### Roadmap

- Stage 1: collector and confirmation - completed.
- Stage 2: analytical data foundation - completed.
- Stage 2.2: funding history engine.
- Stage 3: candidate detection.
- Stage 4: candidate detection.
- Stage 5: time-window logic.
- Stage 6: stability engine.
- Stage 7: market risk and liquidity.
- Stage 8: net edge.
- Stage 9: scoring.
- Stage 10: symbol reliability.
- Stage 11: observation mode.
- Stage 12: notifications/UI/paper trading.

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
```

`ABS_MIN_FUNDING_RATE` is used for status and aggregate reporting. It does not stop snapshots below the threshold from being saved.

`WINDOW_CACHE_MINUTES` controls the in-memory per-symbol history cache. `DEFAULT_METRICS_WINDOW` controls the default window used by the `metrics` command.

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
python -m funding_monitor history
python -m funding_monitor metrics BTCUSDT
python -m funding_monitor metrics BTCUSDT --window-minutes 15
python -m funding_monitor recent-events --limit 20
python -m funding_monitor export-csv --output data/funding_events.csv
```

`python -m funding_monitor init-db` is kept as an alias for `migrate`.

## Tables

`schema_migrations` stores applied SQL migration filenames and timestamps.

`symbols` stores active USDT perpetual contract metadata and the funding interval in hours.

`funding_snapshots` stores WebSocket mark price snapshots. Decimal values are PostgreSQL `NUMERIC` values and timestamps are UTC `TIMESTAMPTZ` values. Snapshot analytics include `funding_rate`, `seconds_to_funding`, `premium_rate`, `funding_direction`, and `funding_interval_hours`.

`funding_events` stores one row per symbol and funding time, including checkpoint predictions, actual funding rate, prediction error, confirmation status, and the next predicted rate seen after funding.

## Funding History Engine

`FundingHistoryService` is the analytical history layer for later candidate detection, stability, scoring, and net edge stages. PostgreSQL remains the source of truth. On reload, the service loads the last `WINDOW_CACHE_MINUTES` minutes once, then new collector snapshots update the in-memory cache without a SQL query per snapshot.

`WindowCache` keeps one `collections.deque` per symbol and prunes snapshots older than the configured window.

`FundingMetrics` is calculated from snapshots for a single symbol and window. It includes current, min, max, mean, median, standard deviation, absolute mean, threshold persistence, threshold crossings, direction changes, deltas, velocity, acceleration, history duration, and snapshot count.

This layer does not implement candidate detection, scoring, net edge, spot mapping, borrow checks, notifications, UI, or trading.

## Predicted Rate And Actual Rate

The predicted rate is Binance's funding rate value from the mark price stream before the funding time. The actual rate is the final funding rate returned later by the Binance funding rate history REST endpoint.

## SQLite

SQLite is no longer used at runtime. Existing `data/funding_monitor.db` files are not deleted, changed, or imported automatically. Historical SQLite import is not implemented yet.

## Safety Notice

This application does not trade, place orders, read private account data, or use API keys.
