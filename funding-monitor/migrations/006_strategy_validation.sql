CREATE TABLE IF NOT EXISTS strategy_validation_runs (
    id BIGSERIAL PRIMARY KEY,
    strategy_name TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    exchange TEXT NOT NULL DEFAULT 'BINANCE',
    status TEXT NOT NULL,
    validation_mode TEXT NOT NULL,
    configuration JSONB NOT NULL,
    configuration_hash TEXT NOT NULL,
    dataset JSONB NOT NULL,
    period_start TIMESTAMPTZ,
    period_end TIMESTAMPTZ,
    requested_symbols JSONB NOT NULL DEFAULT '[]'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    total_events INTEGER NOT NULL DEFAULT 0,
    processed_events INTEGER NOT NULL DEFAULT 0,
    successful_events INTEGER NOT NULL DEFAULT 0,
    failed_events INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    CHECK (status IN ('running', 'completed', 'failed')),
    CHECK (validation_mode IN ('funding_only', 'full_economic')),
    CHECK (total_events >= 0),
    CHECK (processed_events >= 0),
    CHECK (successful_events >= 0),
    CHECK (failed_events >= 0)
);

CREATE INDEX IF NOT EXISTS idx_strategy_validation_runs_started_at
ON strategy_validation_runs(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_validation_runs_config_hash
ON strategy_validation_runs(configuration_hash);

CREATE INDEX IF NOT EXISTS idx_strategy_validation_runs_exchange_period
ON strategy_validation_runs(exchange, period_start, period_end);

CREATE TABLE IF NOT EXISTS strategy_validation_results (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES strategy_validation_runs(id) ON DELETE CASCADE,
    exchange TEXT NOT NULL DEFAULT 'BINANCE',
    symbol TEXT NOT NULL,
    spot_symbol TEXT,
    funding_time TIMESTAMPTZ NOT NULL,
    strategy_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    signal_detected BOOLEAN NOT NULL,
    signal_started_at TIMESTAMPTZ,
    signal_confirmed_at TIMESTAMPTZ,
    entry_time TIMESTAMPTZ,
    entry_minutes_before_funding NUMERIC(18, 6),
    predicted_funding_at_entry NUMERIC(38, 18),
    peak_predicted_funding NUMERIC(38, 18),
    peak_predicted_at TIMESTAMPTZ,
    last_predicted_funding NUMERIC(38, 18),
    realized_funding_rate NUMERIC(38, 18),
    prediction_error NUMERIC(38, 18),
    prediction_drop_from_entry NUMERIC(38, 18),
    prediction_drop_from_peak NUMERIC(38, 18),
    persistence_at_entry NUMERIC(10, 6),
    funding_std_at_entry NUMERIC(38, 18),
    funding_velocity_at_entry NUMERIC(38, 18),
    threshold_crossings_before_entry INTEGER,
    late_spike BOOLEAN NOT NULL,
    deteriorating_signal BOOLEAN NOT NULL,
    spot_pair_exists BOOLEAN NOT NULL,
    positive_strategy_available BOOLEAN NOT NULL,
    enough_history BOOLEAN NOT NULL,
    fresh_data BOOLEAN NOT NULL,
    eligible BOOLEAN NOT NULL,
    rejection_reason TEXT,
    validation_mode TEXT NOT NULL,
    market_data_complete BOOLEAN NOT NULL,
    missing_data_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    position_notional NUMERIC(38, 18) NOT NULL,
    gross_funding_pnl NUMERIC(38, 18),
    spot_price_pnl NUMERIC(38, 18),
    futures_price_pnl NUMERIC(38, 18),
    basis_pnl NUMERIC(38, 18),
    spot_fees NUMERIC(38, 18),
    futures_fees NUMERIC(38, 18),
    slippage_cost NUMERIC(38, 18),
    additional_cost NUMERIC(38, 18),
    net_pnl NUMERIC(38, 18),
    gross_return_rate NUMERIC(38, 18),
    net_return_rate NUMERIC(38, 18),
    outcome_status TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    profitable BOOLEAN,
    data_quality_status TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, exchange, symbol, funding_time, config_hash),
    CHECK (
        validation_mode IN ('funding_only', 'full_economic')
    ),
    CHECK (
        outcome_status IN (
            'funding_only',
            'full_economic',
            'insufficient_market_data',
            'rejected',
            'invalid_data'
        )
    ),
    CHECK (
        data_quality_status IN ('good', 'partial', 'poor', 'invalid')
    ),
    CHECK (position_notional > 0),
    CHECK (
        threshold_crossings_before_entry IS NULL
        OR threshold_crossings_before_entry >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_validation_results_run_id
ON strategy_validation_results(run_id);

CREATE INDEX IF NOT EXISTS idx_strategy_validation_results_symbol_time
ON strategy_validation_results(exchange, symbol, funding_time DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_validation_results_outcome
ON strategy_validation_results(run_id, outcome_status);

CREATE INDEX IF NOT EXISTS idx_strategy_validation_results_success
ON strategy_validation_results(run_id, success);

CREATE TABLE IF NOT EXISTS strategy_validation_aggregates (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES strategy_validation_runs(id) ON DELETE CASCADE,
    grouping_type TEXT NOT NULL,
    grouping_key TEXT NOT NULL,
    metrics JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, grouping_type, grouping_key)
);

CREATE INDEX IF NOT EXISTS idx_strategy_validation_aggregates_run_group
ON strategy_validation_aggregates(run_id, grouping_type, grouping_key);
