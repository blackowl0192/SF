ALTER TABLE candidate_evaluations
ADD COLUMN IF NOT EXISTS exchange TEXT NOT NULL DEFAULT 'BINANCE';

ALTER TABLE candidate_evaluations
DROP CONSTRAINT IF EXISTS candidate_evaluations_futures_symbol_evaluated_at_bucket_engine_version_key;

ALTER TABLE candidate_evaluations
ADD CONSTRAINT candidate_evaluations_exchange_symbol_bucket_version_key
UNIQUE(exchange, futures_symbol, evaluated_at_bucket, engine_version);

CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_exchange_symbol_evaluated
ON candidate_evaluations(exchange, futures_symbol, evaluated_at DESC);

ALTER TABLE funding_interval_summaries
ADD COLUMN IF NOT EXISTS exchange TEXT NOT NULL DEFAULT 'BINANCE',
ADD COLUMN IF NOT EXISTS peak_predicted_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS signal_started_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS longest_positive_streak_seconds INTEGER;

ALTER TABLE funding_interval_summaries
DROP CONSTRAINT IF EXISTS funding_interval_summaries_futures_symbol_funding_time_key;

ALTER TABLE funding_interval_summaries
ADD CONSTRAINT funding_interval_summaries_exchange_symbol_funding_time_key
UNIQUE(exchange, futures_symbol, funding_time);

ALTER TABLE funding_interval_summaries
DROP CONSTRAINT IF EXISTS chk_funding_interval_summaries_positive_streak;

ALTER TABLE funding_interval_summaries
ADD CONSTRAINT chk_funding_interval_summaries_positive_streak
CHECK (
    longest_positive_streak_seconds IS NULL
    OR longest_positive_streak_seconds >= 0
);

CREATE INDEX IF NOT EXISTS idx_funding_interval_summaries_exchange_symbol_time
ON funding_interval_summaries(exchange, futures_symbol, funding_time DESC);

CREATE TABLE IF NOT EXISTS symbol_funding_profiles (
    id BIGSERIAL PRIMARY KEY,
    exchange TEXT NOT NULL DEFAULT 'BINANCE',
    symbol TEXT NOT NULL,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    positive_events INTEGER NOT NULL DEFAULT 0,
    negative_events INTEGER NOT NULL DEFAULT 0,
    positive_ratio NUMERIC(10, 6),
    average_realized_funding NUMERIC(38, 18),
    median_realized_funding NUMERIC(38, 18),
    average_positive_funding NUMERIC(38, 18),
    high_positive_event_count INTEGER NOT NULL DEFAULT 0,
    high_positive_ratio NUMERIC(10, 6),
    signal_frequency NUMERIC(10, 6),
    realized_reliability NUMERIC(10, 6),
    average_prediction_error NUMERIC(38, 18),
    average_drop_before_funding NUMERIC(38, 18),
    current_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(exchange, symbol),
    CHECK (positive_events >= 0),
    CHECK (negative_events >= 0),
    CHECK (high_positive_event_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_symbol_funding_profiles_exchange_symbol
ON symbol_funding_profiles(exchange, symbol);

CREATE INDEX IF NOT EXISTS idx_symbol_funding_profiles_last_updated
ON symbol_funding_profiles(last_updated DESC);
