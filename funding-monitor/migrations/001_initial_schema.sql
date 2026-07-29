CREATE TABLE IF NOT EXISTS symbols (
    symbol TEXT PRIMARY KEY,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    contract_type TEXT NOT NULL,
    status TEXT NOT NULL,
    funding_interval_hours INTEGER NOT NULL DEFAULT 8,
    is_active BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS funding_snapshots (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    mark_price NUMERIC(38, 18) NOT NULL,
    index_price NUMERIC(38, 18),
    estimated_settle_price NUMERIC(38, 18),
    predicted_funding_rate NUMERIC(38, 18) NOT NULL,
    interest_rate NUMERIC(38, 18),
    next_funding_time TIMESTAMPTZ NOT NULL,
    seconds_until_funding INTEGER NOT NULL,
    capture_mode TEXT NOT NULL,
    UNIQUE(symbol, event_time, capture_mode)
);

CREATE TABLE IF NOT EXISTS funding_events (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    funding_time TIMESTAMPTZ NOT NULL,
    funding_interval_hours INTEGER NOT NULL,
    first_predicted_rate NUMERIC(38, 18),
    predicted_rate_10m_before NUMERIC(38, 18),
    predicted_rate_5m_before NUMERIC(38, 18),
    predicted_rate_1m_before NUMERIC(38, 18),
    last_predicted_rate NUMERIC(38, 18),
    actual_funding_rate NUMERIC(38, 18),
    prediction_error NUMERIC(38, 18),
    mark_price_at_funding NUMERIC(38, 18),
    next_predicted_rate NUMERIC(38, 18),
    confirmed_at TIMESTAMPTZ,
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
