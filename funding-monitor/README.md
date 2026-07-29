# funding-monitor

## Purpose

`funding-monitor` collects public Binance USD-M Futures data for predicted and actual funding rates. It stores symbols, funding snapshots, and funding events in SQLite.

## Requirements

Python 3.12 and public internet access to `https://fapi.binance.com` and `wss://fstream.binance.com`.

## Windows PowerShell Install

```powershell
cd funding-monitor
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Run Commands

```powershell
python -m funding_monitor init-db
python -m funding_monitor sync-symbols
python -m funding_monitor collect
python -m funding_monitor status
python -m funding_monitor recent-events --limit 20
python -m funding_monitor export-csv --output data/funding_events.csv
```

## Tables

`symbols` stores active USDT perpetual contract metadata and the funding interval in hours.

`funding_snapshots` stores WebSocket mark price snapshots. Decimal values are stored as text and timestamps are stored in UTC.

`funding_events` stores one row per symbol and funding time, including checkpoint predictions, actual funding rate, prediction error, confirmation status, and the next predicted rate seen after funding.

## Predicted Rate And Actual Rate

The predicted rate is Binance's funding rate value from the mark price stream before the funding time. The actual rate is the final funding rate returned later by the Binance funding rate history REST endpoint.

## Safety Notice

This application does not trade, place orders, read private account data, or use API keys.
