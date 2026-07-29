from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .database import PostgresDatabase
from .models import (
    FundingEvent,
    FundingSnapshot,
    SymbolRecord,
    calculate_premium_rate,
    calculate_seconds_to_funding,
    decimal_from_text,
    ensure_utc,
    funding_direction_from_rate,
)

UPSERT_SYMBOLS_SQL = """
INSERT INTO symbols (
    symbol,
    base_asset,
    quote_asset,
    contract_type,
    status,
    funding_interval_hours,
    is_active,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT(symbol) DO UPDATE SET
    base_asset = excluded.base_asset,
    quote_asset = excluded.quote_asset,
    contract_type = excluded.contract_type,
    status = excluded.status,
    funding_interval_hours = excluded.funding_interval_hours,
    is_active = excluded.is_active,
    created_at = symbols.created_at,
    updated_at = excluded.updated_at
"""

INSERT_SNAPSHOT_SQL = """
INSERT INTO funding_snapshots (
    symbol,
    event_time,
    received_at,
    mark_price,
    index_price,
    estimated_settle_price,
    predicted_funding_rate,
    funding_rate,
    interest_rate,
    next_funding_time,
    seconds_until_funding,
    seconds_to_funding,
    premium_rate,
    funding_direction,
    funding_interval_hours,
    capture_mode
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT(symbol, event_time, capture_mode) DO NOTHING
RETURNING id
"""

UPSERT_FUNDING_EVENT_SQL = """
INSERT INTO funding_events (
    symbol,
    funding_time,
    funding_interval_hours,
    first_predicted_rate,
    last_predicted_rate,
    status
)
VALUES ($1, $2, $3, $4, $5, 'waiting')
ON CONFLICT(symbol, funding_time) DO UPDATE SET
    funding_interval_hours = excluded.funding_interval_hours,
    first_predicted_rate = COALESCE(
        funding_events.first_predicted_rate,
        excluded.first_predicted_rate
    ),
    last_predicted_rate = excluded.last_predicted_rate
RETURNING *
"""

UPDATE_EVENT_PREDICTIONS_SQL = """
UPDATE funding_events
SET
    predicted_rate_10m_before = $1,
    predicted_rate_5m_before = $2,
    predicted_rate_1m_before = $3,
    last_predicted_rate = $4
WHERE symbol = $5 AND funding_time = $6
"""

CONFIRM_EVENT_SQL = """
UPDATE funding_events
SET
    actual_funding_rate = $1,
    prediction_error = CASE
        WHEN last_predicted_rate IS NULL THEN NULL
        ELSE $1 - last_predicted_rate
    END,
    mark_price_at_funding = $2,
    confirmed_at = $3,
    status = 'confirmed'
WHERE symbol = $4 AND funding_time = $5
"""

UPDATE_NEXT_PREDICTED_RATE_SQL = """
UPDATE funding_events
SET next_predicted_rate = COALESCE(next_predicted_rate, $1)
WHERE symbol = $2 AND funding_time = $3
"""


class FundingRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    async def upsert_symbols(self, symbols: Iterable[SymbolRecord]) -> int:
        rows = list(symbols)
        if not rows:
            return 0
        async with self.database.acquire() as connection:
            await connection.executemany(
                UPSERT_SYMBOLS_SQL,
                [
                    (
                        item.symbol,
                        item.base_asset,
                        item.quote_asset,
                        item.contract_type,
                        item.status,
                        item.funding_interval_hours,
                        item.is_active,
                        ensure_utc(item.created_at),
                        ensure_utc(item.updated_at),
                    )
                    for item in rows
                ],
            )
        return len(rows)

    async def active_symbols(self) -> dict[str, SymbolRecord]:
        async with self.database.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT *
                FROM symbols
                WHERE is_active = TRUE
                ORDER BY symbol
                """
            )
        return {row["symbol"]: self._row_to_symbol(row) for row in rows}

    async def insert_snapshot(self, snapshot: FundingSnapshot) -> bool:
        async with self.database.acquire() as connection:
            row = await connection.fetchrow(
                INSERT_SNAPSHOT_SQL,
                snapshot.symbol,
                ensure_utc(snapshot.event_time),
                ensure_utc(snapshot.received_at),
                snapshot.mark_price,
                snapshot.index_price,
                snapshot.estimated_settle_price,
                snapshot.predicted_funding_rate,
                snapshot.funding_rate,
                snapshot.interest_rate,
                ensure_utc(snapshot.next_funding_time),
                snapshot.seconds_until_funding,
                snapshot.seconds_to_funding,
                snapshot.premium_rate,
                snapshot.funding_direction,
                snapshot.funding_interval_hours,
                snapshot.capture_mode,
            )
        return row is not None

    async def create_or_get_funding_event(
        self,
        symbol: str,
        funding_time: datetime,
        funding_interval_hours: int,
        first_predicted_rate: Decimal,
    ) -> FundingEvent:
        async with self.database.acquire() as connection:
            row = await connection.fetchrow(
                UPSERT_FUNDING_EVENT_SQL,
                symbol,
                ensure_utc(funding_time),
                funding_interval_hours,
                first_predicted_rate,
                first_predicted_rate,
            )
        if row is None:
            raise RuntimeError("funding event was not created")
        return self._row_to_event(row)

    async def get_funding_event(
        self, symbol: str, funding_time: datetime
    ) -> FundingEvent | None:
        async with self.database.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT *
                FROM funding_events
                WHERE symbol = $1 AND funding_time = $2
                """,
                symbol,
                ensure_utc(funding_time),
            )
        return self._row_to_event(row) if row is not None else None

    async def update_event_predictions(
        self,
        symbol: str,
        funding_time: datetime,
        *,
        predicted_rate_10m_before: Decimal | None,
        predicted_rate_5m_before: Decimal | None,
        predicted_rate_1m_before: Decimal | None,
        last_predicted_rate: Decimal | None,
    ) -> None:
        async with self.database.acquire() as connection:
            await connection.execute(
                UPDATE_EVENT_PREDICTIONS_SQL,
                predicted_rate_10m_before,
                predicted_rate_5m_before,
                predicted_rate_1m_before,
                last_predicted_rate,
                symbol,
                ensure_utc(funding_time),
            )

    async def snapshots_for_event(
        self, symbol: str, funding_time: datetime
    ) -> list[FundingSnapshot]:
        async with self.database.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT *
                FROM funding_snapshots
                WHERE symbol = $1 AND next_funding_time = $2
                ORDER BY event_time
                """,
                symbol,
                ensure_utc(funding_time),
            )
        return [self._row_to_snapshot(row) for row in rows]

    async def recent_snapshots(self, minutes: int) -> list[FundingSnapshot]:
        async with self.database.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT *
                FROM funding_snapshots
                WHERE received_at >= NOW() - make_interval(mins => $1::int)
                ORDER BY symbol, event_time
                """,
                minutes,
            )
        return [self._row_to_snapshot(row) for row in rows]

    async def mark_event_confirmed(
        self,
        symbol: str,
        funding_time: datetime,
        actual_funding_rate: Decimal,
        mark_price_at_funding: Decimal | None,
        confirmed_at: datetime,
    ) -> None:
        async with self.database.acquire() as connection:
            await connection.execute(
                CONFIRM_EVENT_SQL,
                actual_funding_rate,
                mark_price_at_funding,
                ensure_utc(confirmed_at),
                symbol,
                ensure_utc(funding_time),
            )

    async def mark_confirmation_failed(
        self, symbol: str, funding_time: datetime
    ) -> None:
        async with self.database.acquire() as connection:
            await connection.execute(
                """
                UPDATE funding_events
                SET status = 'confirmation_failed'
                WHERE symbol = $1 AND funding_time = $2 AND status != 'confirmed'
                """,
                symbol,
                ensure_utc(funding_time),
            )

    async def update_next_predicted_rate(
        self,
        symbol: str,
        previous_funding_time: datetime,
        next_predicted_rate: Decimal,
    ) -> None:
        async with self.database.acquire() as connection:
            await connection.execute(
                UPDATE_NEXT_PREDICTED_RATE_SQL,
                next_predicted_rate,
                symbol,
                ensure_utc(previous_funding_time),
            )

    async def status_summary(
        self, abs_threshold: Decimal = Decimal("0.0003")
    ) -> dict[str, Any]:
        async with self.database.acquire() as connection:
            active_symbols = await connection.fetchval(
                "SELECT COUNT(*) FROM symbols WHERE is_active = TRUE"
            )
            snapshot_count = await connection.fetchval(
                "SELECT COUNT(*) FROM funding_snapshots"
            )
            event_count = await connection.fetchval(
                "SELECT COUNT(*) FROM funding_events"
            )
            last_snapshot = await connection.fetchval(
                "SELECT MAX(event_time) FROM funding_snapshots"
            )
            snapshot_stats = await connection.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE funding_direction = 'positive')
                        AS positive_snapshots,
                    COUNT(*) FILTER (WHERE funding_direction = 'negative')
                        AS negative_snapshots,
                    COUNT(*) FILTER (WHERE funding_direction = 'neutral')
                        AS neutral_snapshots,
                    COUNT(*) FILTER (WHERE ABS(funding_rate) >= $1)
                        AS snapshots_above_abs_threshold,
                    COUNT(*) FILTER (WHERE ABS(funding_rate) < $1)
                        AS snapshots_below_abs_threshold,
                    MIN(next_funding_time) AS next_funding_time_min,
                    MAX(received_at) AS latest_received_at
                FROM funding_snapshots
                """,
                abs_threshold,
            )
            status_rows = await connection.fetch(
                """
                SELECT status, COUNT(*) AS count
                FROM funding_events
                GROUP BY status
                """
            )
        statuses = {row["status"]: int(row["count"]) for row in status_rows}
        return {
            "active_symbols": int(active_symbols or 0),
            "snapshot_count": int(snapshot_count or 0),
            "event_count": int(event_count or 0),
            "last_snapshot": last_snapshot,
            "waiting": statuses.get("waiting", 0),
            "confirmed": statuses.get("confirmed", 0),
            "confirmation_failed": statuses.get("confirmation_failed", 0),
            "positive_snapshots": int(snapshot_stats["positive_snapshots"] or 0),
            "negative_snapshots": int(snapshot_stats["negative_snapshots"] or 0),
            "neutral_snapshots": int(snapshot_stats["neutral_snapshots"] or 0),
            "snapshots_above_abs_threshold": int(
                snapshot_stats["snapshots_above_abs_threshold"] or 0
            ),
            "snapshots_below_abs_threshold": int(
                snapshot_stats["snapshots_below_abs_threshold"] or 0
            ),
            "next_funding_time_min": snapshot_stats["next_funding_time_min"],
            "latest_received_at": snapshot_stats["latest_received_at"],
        }

    async def snapshot_stats(
        self, *, abs_threshold: Decimal, minutes: int | None = None
    ) -> dict[str, Any]:
        if minutes is None:
            query = _SNAPSHOT_STATS_SQL
            args: tuple[Any, ...] = (abs_threshold,)
        else:
            query = (
                f"{_SNAPSHOT_STATS_SQL}\n"
                "WHERE received_at >= NOW() - make_interval(mins => $2::int)"
            )
            args = (abs_threshold, minutes)

        async with self.database.acquire() as connection:
            row = await connection.fetchrow(query, *args)

        return {
            "total_snapshots": int(row["total_snapshots"] or 0),
            "symbols_represented": int(row["symbols_represented"] or 0),
            "positive_count": int(row["positive_count"] or 0),
            "negative_count": int(row["negative_count"] or 0),
            "neutral_count": int(row["neutral_count"] or 0),
            "above_threshold_count": int(row["above_threshold_count"] or 0),
            "below_threshold_count": int(row["below_threshold_count"] or 0),
            "min_funding_rate": row["min_funding_rate"],
            "max_funding_rate": row["max_funding_rate"],
            "average_absolute_funding_rate": row["average_absolute_funding_rate"],
            "earliest_next_funding": row["earliest_next_funding"],
            "latest_next_funding": row["latest_next_funding"],
            "newest_snapshot": row["newest_snapshot"],
            "oldest_snapshot": row["oldest_snapshot"],
        }

    async def recent_events(self, limit: int) -> list[FundingEvent]:
        async with self.database.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT *
                FROM funding_events
                ORDER BY funding_time DESC, symbol
                LIMIT $1
                """,
                limit,
            )
        return [self._row_to_event(row) for row in rows]

    async def export_events_csv(self, output_path: Path) -> int:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        async with self.database.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT *
                FROM funding_events
                ORDER BY funding_time, symbol
                """
            )
        fieldnames = [
            "symbol",
            "funding_time",
            "funding_interval_hours",
            "first_predicted_rate",
            "predicted_rate_10m_before",
            "predicted_rate_5m_before",
            "predicted_rate_1m_before",
            "last_predicted_rate",
            "actual_funding_rate",
            "prediction_error",
            "mark_price_at_funding",
            "next_predicted_rate",
            "confirmed_at",
            "status",
        ]
        with output_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({name: row[name] for name in fieldnames})
        return len(rows)

    def _row_to_symbol(self, row: Mapping[str, Any]) -> SymbolRecord:
        return SymbolRecord(
            symbol=row["symbol"],
            base_asset=row["base_asset"],
            quote_asset=row["quote_asset"],
            contract_type=row["contract_type"],
            status=row["status"],
            funding_interval_hours=int(row["funding_interval_hours"]),
            is_active=bool(row["is_active"]),
            created_at=ensure_utc(row["created_at"]),
            updated_at=ensure_utc(row["updated_at"]),
        )

    def _row_to_snapshot(self, row: Mapping[str, Any]) -> FundingSnapshot:
        return FundingSnapshot(
            symbol=row["symbol"],
            event_time=ensure_utc(row["event_time"]),
            received_at=ensure_utc(row["received_at"]),
            mark_price=decimal_from_text(row["mark_price"]),
            index_price=decimal_from_text(row["index_price"])
            if row["index_price"] is not None
            else None,
            estimated_settle_price=decimal_from_text(row["estimated_settle_price"])
            if row["estimated_settle_price"] is not None
            else None,
            predicted_funding_rate=decimal_from_text(row["predicted_funding_rate"]),
            funding_rate=decimal_from_text(
                _row_value(row, "funding_rate", row["predicted_funding_rate"])
            ),
            interest_rate=decimal_from_text(row["interest_rate"])
            if row["interest_rate"] is not None
            else None,
            next_funding_time=ensure_utc(row["next_funding_time"]),
            seconds_until_funding=int(row["seconds_until_funding"]),
            seconds_to_funding=int(
                _row_value(
                    row,
                    "seconds_to_funding",
                    calculate_seconds_to_funding(
                        row["event_time"], row["next_funding_time"]
                    ),
                )
            ),
            premium_rate=decimal_from_text(_row_value(row, "premium_rate", None))
            if _row_value(row, "premium_rate", None) is not None
            else calculate_premium_rate(
                decimal_from_text(row["mark_price"]),
                decimal_from_text(row["index_price"])
                if row["index_price"] is not None
                else None,
            ),
            funding_direction=_row_value(
                row,
                "funding_direction",
                funding_direction_from_rate(
                    decimal_from_text(row["predicted_funding_rate"])
                ),
            ),
            funding_interval_hours=int(_row_value(row, "funding_interval_hours", 8)),
            capture_mode=row["capture_mode"],
        )

    def _row_to_event(self, row: Mapping[str, Any]) -> FundingEvent:
        return FundingEvent(
            symbol=row["symbol"],
            funding_time=ensure_utc(row["funding_time"]),
            funding_interval_hours=int(row["funding_interval_hours"]),
            first_predicted_rate=self._optional_decimal(row, "first_predicted_rate"),
            predicted_rate_10m_before=self._optional_decimal(
                row, "predicted_rate_10m_before"
            ),
            predicted_rate_5m_before=self._optional_decimal(
                row, "predicted_rate_5m_before"
            ),
            predicted_rate_1m_before=self._optional_decimal(
                row, "predicted_rate_1m_before"
            ),
            last_predicted_rate=self._optional_decimal(row, "last_predicted_rate"),
            actual_funding_rate=self._optional_decimal(row, "actual_funding_rate"),
            prediction_error=self._optional_decimal(row, "prediction_error"),
            mark_price_at_funding=self._optional_decimal(row, "mark_price_at_funding"),
            next_predicted_rate=self._optional_decimal(row, "next_predicted_rate"),
            confirmed_at=ensure_utc(row["confirmed_at"])
            if row["confirmed_at"] is not None
            else None,
            status=row["status"],
        )

    def _optional_decimal(self, row: Mapping[str, Any], key: str) -> Decimal | None:
        return decimal_from_text(row[key]) if row[key] is not None else None


_SNAPSHOT_STATS_SQL = """
SELECT
    COUNT(*) AS total_snapshots,
    COUNT(DISTINCT symbol) AS symbols_represented,
    COUNT(*) FILTER (WHERE funding_direction = 'positive') AS positive_count,
    COUNT(*) FILTER (WHERE funding_direction = 'negative') AS negative_count,
    COUNT(*) FILTER (WHERE funding_direction = 'neutral') AS neutral_count,
    COUNT(*) FILTER (WHERE ABS(funding_rate) >= $1) AS above_threshold_count,
    COUNT(*) FILTER (WHERE ABS(funding_rate) < $1) AS below_threshold_count,
    MIN(funding_rate) AS min_funding_rate,
    MAX(funding_rate) AS max_funding_rate,
    AVG(ABS(funding_rate)) AS average_absolute_funding_rate,
    MIN(next_funding_time) AS earliest_next_funding,
    MAX(next_funding_time) AS latest_next_funding,
    MAX(received_at) AS newest_snapshot,
    MIN(received_at) AS oldest_snapshot
FROM funding_snapshots
"""


def _row_value(row: Mapping[str, Any], key: str, default: Any) -> Any:
    try:
        return row[key]
    except KeyError:
        return default
