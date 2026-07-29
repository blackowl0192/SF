CREATE TABLE IF NOT EXISTS instrument_mappings (
    id BIGSERIAL PRIMARY KEY,
    futures_symbol TEXT NOT NULL,
    futures_pair TEXT NOT NULL,
    futures_base_asset TEXT NOT NULL,
    futures_quote_asset TEXT NOT NULL,
    futures_margin_asset TEXT NOT NULL,
    futures_contract_type TEXT NOT NULL,
    futures_status TEXT NOT NULL,
    spot_symbol TEXT,
    spot_base_asset TEXT,
    spot_quote_asset TEXT,
    spot_status TEXT,
    spot_trading_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    spot_pair_exists BOOLEAN NOT NULL DEFAULT FALSE,
    spot_mapping_status TEXT NOT NULL,
    mapping_reason TEXT,
    positive_strategy_available BOOLEAN NOT NULL DEFAULT FALSE,
    negative_strategy_available BOOLEAN NOT NULL DEFAULT FALSE,
    negative_strategy_status TEXT NOT NULL DEFAULT 'not_applicable',
    mapping_source TEXT NOT NULL DEFAULT 'binance_exchange_info',
    mapping_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(futures_symbol),
    CHECK (
        spot_mapping_status IN (
            'matched',
            'missing',
            'ambiguous',
            'unsupported',
            'spot_trading_disabled'
        )
    ),
    CHECK (
        negative_strategy_status IN (
            'borrow_check_not_implemented',
            'borrow_check_pending',
            'borrow_available',
            'borrow_unavailable',
            'borrow_cost_too_high',
            'not_applicable'
        )
    ),
    CHECK (
        mapping_reason IS NULL
        OR mapping_reason IN (
            'exact_base_asset_match',
            'spot_pair_missing',
            'spot_trading_disabled',
            'multiplier_contract',
            'unsupported_quote_asset',
            'unsupported_margin_asset',
            'unsupported_contract_type',
            'unsupported_futures_status',
            'unsupported_base_asset',
            'multiple_spot_matches',
            'metadata_mismatch'
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_instrument_mappings_spot_symbol
ON instrument_mappings(spot_symbol);

CREATE INDEX IF NOT EXISTS idx_instrument_mappings_spot_mapping_status
ON instrument_mappings(spot_mapping_status);

CREATE INDEX IF NOT EXISTS idx_instrument_mappings_positive_strategy_available
ON instrument_mappings(positive_strategy_available);

CREATE INDEX IF NOT EXISTS idx_instrument_mappings_negative_strategy_available
ON instrument_mappings(negative_strategy_available);

CREATE INDEX IF NOT EXISTS idx_instrument_mappings_mapping_updated_at
ON instrument_mappings(mapping_updated_at);
