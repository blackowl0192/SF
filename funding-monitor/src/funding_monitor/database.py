from __future__ import annotations

from pathlib import Path

import aiosqlite

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS symbols (
    symbol TEXT PRIMARY KEY,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    contract_type TEXT NOT NULL,
    status TEXT NOT NULL,
    funding_interval_hours INTEGER NOT NULL DEFAULT 8,
    is_active INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS funding_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    event_time TEXT NOT NULL,
    received_at TEXT NOT NULL,
    mark_price TEXT NOT NULL,
    index_price TEXT,
    estimated_settle_price TEXT,
    predicted_funding_rate TEXT NOT NULL,
    interest_rate TEXT,
    next_funding_time TEXT NOT NULL,
    seconds_until_funding INTEGER NOT NULL,
    capture_mode TEXT NOT NULL,
    UNIQUE(symbol, event_time, capture_mode)
);

CREATE TABLE IF NOT EXISTS funding_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    funding_time TEXT NOT NULL,
    funding_interval_hours INTEGER NOT NULL,
    first_predicted_rate TEXT,
    predicted_rate_10m_before TEXT,
    predicted_rate_5m_before TEXT,
    predicted_rate_1m_before TEXT,
    last_predicted_rate TEXT,
    actual_funding_rate TEXT,
    prediction_error TEXT,
    mark_price_at_funding TEXT,
    next_predicted_rate TEXT,
    confirmed_at TEXT,
    status TEXT NOT NULL DEFAULT 'waiting',
    UNIQUE(symbol, funding_time)
);

CREATE INDEX IF NOT EXISTS idx_funding_snapshots_symbol_event_time
ON funding_snapshots(symbol, event_time);

CREATE INDEX IF NOT EXISTS idx_funding_snapshots_next_funding_time
ON funding_snapshots(next_funding_time);

CREATE INDEX IF NOT EXISTS idx_funding_events_symbol_funding_time
ON funding_events(symbol, funding_time);

CREATE INDEX IF NOT EXISTS idx_funding_events_status_funding_time
ON funding_events(status, funding_time);
"""


async def initialize_database(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(database_path) as db:
        await db.executescript(SCHEMA_SQL)
        await db.commit()
