# funding-monitor

## Purpose

`funding-monitor` collects public Binance USD-M Futures data for predicted and actual funding rates. Runtime storage uses Supabase PostgreSQL through `asyncpg`.

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
python -m funding_monitor recent-events --limit 20
python -m funding_monitor export-csv --output data/funding_events.csv
```

`python -m funding_monitor init-db` is kept as an alias for `migrate`.

## Tables

`schema_migrations` stores applied SQL migration filenames and timestamps.

`symbols` stores active USDT perpetual contract metadata and the funding interval in hours.

`funding_snapshots` stores WebSocket mark price snapshots. Decimal values are PostgreSQL `NUMERIC` values and timestamps are UTC `TIMESTAMPTZ` values.

`funding_events` stores one row per symbol and funding time, including checkpoint predictions, actual funding rate, prediction error, confirmation status, and the next predicted rate seen after funding.

## Predicted Rate And Actual Rate

The predicted rate is Binance's funding rate value from the mark price stream before the funding time. The actual rate is the final funding rate returned later by the Binance funding rate history REST endpoint.

## SQLite

SQLite is no longer used at runtime. Existing `data/funding_monitor.db` files are not deleted, changed, or imported automatically. Historical SQLite import is not implemented yet.

## Safety Notice

This application does not trade, place orders, read private account data, or use API keys.
