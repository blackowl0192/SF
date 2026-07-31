CREATE TABLE IF NOT EXISTS candidate_evaluations (
    id BIGSERIAL PRIMARY KEY,
    futures_symbol TEXT NOT NULL,
    spot_symbol TEXT,
    evaluated_at TIMESTAMPTZ NOT NULL,
    evaluated_at_bucket TIMESTAMPTZ NOT NULL,
    next_funding_time TIMESTAMPTZ,
    predicted_funding_rate NUMERIC(38, 18) NOT NULL,
    minimum_funding_rate NUMERIC(38, 18) NOT NULL,
    minutes_to_funding NUMERIC(18, 6),
    status TEXT NOT NULL,
    total_score NUMERIC(10, 4) NOT NULL,
    funding_score NUMERIC(10, 4) NOT NULL,
    persistence_score NUMERIC(10, 4) NOT NULL,
    stability_score NUMERIC(10, 4) NOT NULL,
    trend_score NUMERIC(10, 4) NOT NULL,
    lifetime_score NUMERIC(10, 4) NOT NULL,
    timing_score NUMERIC(10, 4) NOT NULL,
    total_penalty NUMERIC(10, 4) NOT NULL,
    persistence_ratio NUMERIC(10, 6),
    standard_deviation NUMERIC(38, 18),
    velocity NUMERIC(38, 18),
    acceleration NUMERIC(38, 18),
    threshold_crossings INTEGER,
    direction_changes INTEGER,
    signal_started_at TIMESTAMPTZ,
    signal_age_seconds INTEGER,
    snapshot_count INTEGER NOT NULL,
    history_duration_seconds INTEGER,
    latest_snapshot_at TIMESTAMPTZ,
    rejection_reasons JSONB NOT NULL,
    warning_flags JSONB NOT NULL,
    score_details JSONB NOT NULL,
    metrics_details JSONB NOT NULL,
    engine_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(futures_symbol, evaluated_at_bucket, engine_version),
    CHECK (
        status IN (
            'observing',
            'candidate',
            'strong_candidate',
            'weak_candidate',
            'unstable',
            'late_spike',
            'deteriorating',
            'funding_falling',
            'too_early',
            'too_late',
            'stale',
            'insufficient_history',
            'rejected',
            'expired'
        )
    ),
    CHECK (total_score >= 0 AND total_score <= 100),
    CHECK (funding_score >= 0 AND funding_score <= 30),
    CHECK (persistence_score >= 0 AND persistence_score <= 25),
    CHECK (stability_score >= 0 AND stability_score <= 15),
    CHECK (trend_score >= 0 AND trend_score <= 15),
    CHECK (lifetime_score >= 0 AND lifetime_score <= 10),
    CHECK (timing_score >= 0 AND timing_score <= 5),
    CHECK (total_penalty >= 0),
    CHECK (snapshot_count >= 0),
    CHECK (signal_age_seconds IS NULL OR signal_age_seconds >= 0),
    CHECK (history_duration_seconds IS NULL OR history_duration_seconds >= 0)
);

CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_symbol_evaluated
ON candidate_evaluations(futures_symbol, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_status_evaluated
ON candidate_evaluations(status, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_next_funding_time
ON candidate_evaluations(next_funding_time);

CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_total_score
ON candidate_evaluations(total_score DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_evaluated_at
ON candidate_evaluations(evaluated_at DESC);

CREATE TABLE IF NOT EXISTS funding_interval_summaries (
    id BIGSERIAL PRIMARY KEY,
    futures_symbol TEXT NOT NULL,
    funding_time TIMESTAMPTZ NOT NULL,
    interval_started_at TIMESTAMPTZ,
    interval_ended_at TIMESTAMPTZ NOT NULL,
    realized_funding_rate NUMERIC(38, 18) NOT NULL,
    first_predicted_rate NUMERIC(38, 18),
    last_predicted_rate NUMERIC(38, 18),
    minimum_predicted_rate NUMERIC(38, 18),
    maximum_predicted_rate NUMERIC(38, 18),
    mean_predicted_rate NUMERIC(38, 18),
    median_predicted_rate NUMERIC(38, 18),
    predicted_rate_120m_before NUMERIC(38, 18),
    predicted_rate_60m_before NUMERIC(38, 18),
    predicted_rate_30m_before NUMERIC(38, 18),
    predicted_rate_15m_before NUMERIC(38, 18),
    predicted_rate_5m_before NUMERIC(38, 18),
    positive_snapshot_ratio NUMERIC(10, 6),
    above_threshold_snapshot_ratio NUMERIC(10, 6),
    above_threshold_duration_seconds INTEGER,
    maximum_above_threshold_streak_seconds INTEGER,
    threshold_crossings INTEGER,
    direction_changes INTEGER,
    prediction_error NUMERIC(38, 18),
    absolute_prediction_error NUMERIC(38, 18),
    snapshot_count INTEGER NOT NULL,
    history_coverage_ratio NUMERIC(10, 6),
    summary_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(futures_symbol, funding_time),
    CHECK (
        summary_status IN (
            'pending_confirmation',
            'complete',
            'partial_history',
            'insufficient_history',
            'confirmation_failed',
            'invalid'
        )
    ),
    CHECK (snapshot_count >= 0),
    CHECK (
        above_threshold_duration_seconds IS NULL
        OR above_threshold_duration_seconds >= 0
    ),
    CHECK (
        maximum_above_threshold_streak_seconds IS NULL
        OR maximum_above_threshold_streak_seconds >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_funding_interval_summaries_symbol_funding_time
ON funding_interval_summaries(futures_symbol, funding_time);

CREATE INDEX IF NOT EXISTS idx_funding_interval_summaries_status_funding_time
ON funding_interval_summaries(summary_status, funding_time DESC);

CREATE INDEX IF NOT EXISTS idx_funding_interval_summaries_funding_time
ON funding_interval_summaries(funding_time DESC);
