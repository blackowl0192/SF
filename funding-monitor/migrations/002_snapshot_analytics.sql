ALTER TABLE funding_snapshots
ADD COLUMN IF NOT EXISTS funding_rate NUMERIC(38, 18),
ADD COLUMN IF NOT EXISTS seconds_to_funding INTEGER,
ADD COLUMN IF NOT EXISTS premium_rate NUMERIC(38, 18),
ADD COLUMN IF NOT EXISTS funding_direction TEXT,
ADD COLUMN IF NOT EXISTS funding_interval_hours INTEGER;

UPDATE funding_snapshots
SET
    funding_rate = COALESCE(
        funding_rate,
        predicted_funding_rate
    ),
    seconds_to_funding = COALESCE(
        seconds_to_funding,
        GREATEST(0, seconds_until_funding)
    ),
    premium_rate = COALESCE(
        premium_rate,
        CASE
            WHEN index_price IS NOT NULL
                 AND index_price <> 0
                 AND mark_price IS NOT NULL
            THEN (mark_price - index_price) / index_price
            ELSE NULL
        END
    ),
    funding_direction = COALESCE(
        funding_direction,
        CASE
            WHEN COALESCE(funding_rate, predicted_funding_rate) > 0
                THEN 'positive'
            WHEN COALESCE(funding_rate, predicted_funding_rate) < 0
                THEN 'negative'
            ELSE 'neutral'
        END
    ),
    funding_interval_hours = COALESCE(
        funding_interval_hours,
        8
    );

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM funding_snapshots
        WHERE funding_rate IS NULL
    ) THEN
        RAISE EXCEPTION
            'Cannot make funding_rate NOT NULL: existing rows contain NULL';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM funding_snapshots
        WHERE seconds_to_funding IS NULL
    ) THEN
        RAISE EXCEPTION
            'Cannot make seconds_to_funding NOT NULL: existing rows contain NULL';
    END IF;
END
$$;

ALTER TABLE funding_snapshots
ALTER COLUMN funding_rate SET NOT NULL,
ALTER COLUMN seconds_to_funding SET NOT NULL,
ALTER COLUMN funding_direction SET NOT NULL,
ALTER COLUMN funding_interval_hours SET DEFAULT 8,
ALTER COLUMN funding_interval_hours SET NOT NULL;

ALTER TABLE funding_snapshots
DROP CONSTRAINT IF EXISTS chk_funding_snapshots_direction;

ALTER TABLE funding_snapshots
ADD CONSTRAINT chk_funding_snapshots_direction
CHECK (
    funding_direction IN ('positive', 'negative', 'neutral')
);

ALTER TABLE funding_snapshots
DROP CONSTRAINT IF EXISTS chk_funding_snapshots_seconds_to_funding;

ALTER TABLE funding_snapshots
ADD CONSTRAINT chk_funding_snapshots_seconds_to_funding
CHECK (seconds_to_funding >= 0);

ALTER TABLE funding_snapshots
DROP CONSTRAINT IF EXISTS chk_funding_snapshots_interval;

ALTER TABLE funding_snapshots
ADD CONSTRAINT chk_funding_snapshots_interval
CHECK (funding_interval_hours > 0);

CREATE INDEX IF NOT EXISTS idx_funding_snapshots_symbol_next_funding_time
ON funding_snapshots(symbol, next_funding_time);

CREATE INDEX IF NOT EXISTS idx_funding_snapshots_funding_direction
ON funding_snapshots(funding_direction);

CREATE INDEX IF NOT EXISTS idx_funding_snapshots_abs_funding_rate
ON funding_snapshots((ABS(funding_rate)));