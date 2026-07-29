from __future__ import annotations

import asyncio
import csv
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiosqlite

from .models import (
    FundingEvent,
    FundingSnapshot,
    SymbolRecord,
    datetime_to_text,
    decimal_from_text,
    decimal_to_text,
    text_to_datetime,
)


class FundingRepository:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self._write_lock = asyncio.Lock()

    @asynccontextmanager
    async def _connect(self) -> AsyncIterator[aiosqlite.Connection]:
        db = await aiosqlite.connect(self.database_path)
        db.row_factory = aiosqlite.Row
        try:
            yield db
        finally:
            await db.close()

    async def upsert_symbols(self, symbols: Iterable[SymbolRecord]) -> int:
        rows = list(symbols)
        if not rows:
            return 0
        async with self._write_lock, self._connect() as db:
            await db.executemany(
                """
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
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        base_asset = excluded.base_asset,
                        quote_asset = excluded.quote_asset,
                        contract_type = excluded.contract_type,
                        status = excluded.status,
                        funding_interval_hours = excluded.funding_interval_hours,
                        is_active = excluded.is_active,
                        created_at = symbols.created_at,
                        updated_at = excluded.updated_at
                    """,
                [
                    (
                        item.symbol,
                        item.base_asset,
                        item.quote_asset,
                        item.contract_type,
                        item.status,
                        item.funding_interval_hours,
                        1 if item.is_active else 0,
                        datetime_to_text(item.created_at),
                        datetime_to_text(item.updated_at),
                    )
                    for item in rows
                ],
            )
            await db.commit()
        return len(rows)

    async def active_symbols(self) -> dict[str, SymbolRecord]:
        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT *
                FROM symbols
                WHERE is_active = 1
                ORDER BY symbol
                """
            )
            rows = await cursor.fetchall()
        return {row["symbol"]: self._row_to_symbol(row) for row in rows}

    async def insert_snapshot(self, snapshot: FundingSnapshot) -> bool:
        async with self._write_lock, self._connect() as db:
            cursor = await db.execute(
                """
                    INSERT OR IGNORE INTO funding_snapshots (
                        symbol,
                        event_time,
                        received_at,
                        mark_price,
                        index_price,
                        estimated_settle_price,
                        predicted_funding_rate,
                        interest_rate,
                        next_funding_time,
                        seconds_until_funding,
                        capture_mode
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                (
                    snapshot.symbol,
                    datetime_to_text(snapshot.event_time),
                    datetime_to_text(snapshot.received_at),
                    decimal_to_text(snapshot.mark_price),
                    decimal_to_text(snapshot.index_price)
                    if snapshot.index_price is not None
                    else None,
                    decimal_to_text(snapshot.estimated_settle_price)
                    if snapshot.estimated_settle_price is not None
                    else None,
                    decimal_to_text(snapshot.predicted_funding_rate),
                    decimal_to_text(snapshot.interest_rate)
                    if snapshot.interest_rate is not None
                    else None,
                    datetime_to_text(snapshot.next_funding_time),
                    snapshot.seconds_until_funding,
                    snapshot.capture_mode,
                ),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def create_or_get_funding_event(
        self,
        symbol: str,
        funding_time: datetime,
        funding_interval_hours: int,
        first_predicted_rate: Decimal,
    ) -> FundingEvent:
        async with self._write_lock, self._connect() as db:
            await db.execute(
                """
                    INSERT INTO funding_events (
                        symbol,
                        funding_time,
                        funding_interval_hours,
                        first_predicted_rate,
                        last_predicted_rate,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, 'waiting')
                    ON CONFLICT(symbol, funding_time) DO UPDATE SET
                        funding_interval_hours = excluded.funding_interval_hours,
                        first_predicted_rate = COALESCE(
                            funding_events.first_predicted_rate,
                            excluded.first_predicted_rate
                        ),
                        last_predicted_rate = excluded.last_predicted_rate
                    """,
                (
                    symbol,
                    datetime_to_text(funding_time),
                    funding_interval_hours,
                    decimal_to_text(first_predicted_rate),
                    decimal_to_text(first_predicted_rate),
                ),
            )
            await db.commit()
        event = await self.get_funding_event(symbol, funding_time)
        if event is None:
            raise RuntimeError("funding event was not created")
        return event

    async def get_funding_event(
        self, symbol: str, funding_time: datetime
    ) -> FundingEvent | None:
        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT *
                FROM funding_events
                WHERE symbol = ? AND funding_time = ?
                """,
                (symbol, datetime_to_text(funding_time)),
            )
            row = await cursor.fetchone()
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
        async with self._write_lock, self._connect() as db:
            await db.execute(
                """
                    UPDATE funding_events
                    SET
                        predicted_rate_10m_before = ?,
                        predicted_rate_5m_before = ?,
                        predicted_rate_1m_before = ?,
                        last_predicted_rate = ?
                    WHERE symbol = ? AND funding_time = ?
                    """,
                (
                    decimal_to_text(predicted_rate_10m_before)
                    if predicted_rate_10m_before is not None
                    else None,
                    decimal_to_text(predicted_rate_5m_before)
                    if predicted_rate_5m_before is not None
                    else None,
                    decimal_to_text(predicted_rate_1m_before)
                    if predicted_rate_1m_before is not None
                    else None,
                    decimal_to_text(last_predicted_rate)
                    if last_predicted_rate is not None
                    else None,
                    symbol,
                    datetime_to_text(funding_time),
                ),
            )
            await db.commit()

    async def snapshots_for_event(
        self, symbol: str, funding_time: datetime
    ) -> list[FundingSnapshot]:
        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT *
                FROM funding_snapshots
                WHERE symbol = ? AND next_funding_time = ?
                ORDER BY event_time
                """,
                (symbol, datetime_to_text(funding_time)),
            )
            rows = await cursor.fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    async def mark_event_confirmed(
        self,
        symbol: str,
        funding_time: datetime,
        actual_funding_rate: Decimal,
        mark_price_at_funding: Decimal | None,
        confirmed_at: datetime,
    ) -> None:
        event = await self.get_funding_event(symbol, funding_time)
        prediction_error = None
        if event and event.last_predicted_rate is not None:
            prediction_error = actual_funding_rate - event.last_predicted_rate
        async with self._write_lock, self._connect() as db:
            await db.execute(
                """
                    UPDATE funding_events
                    SET
                        actual_funding_rate = ?,
                        prediction_error = ?,
                        mark_price_at_funding = ?,
                        confirmed_at = ?,
                        status = 'confirmed'
                    WHERE symbol = ? AND funding_time = ?
                    """,
                (
                    decimal_to_text(actual_funding_rate),
                    decimal_to_text(prediction_error)
                    if prediction_error is not None
                    else None,
                    decimal_to_text(mark_price_at_funding)
                    if mark_price_at_funding is not None
                    else None,
                    datetime_to_text(confirmed_at),
                    symbol,
                    datetime_to_text(funding_time),
                ),
            )
            await db.commit()

    async def mark_confirmation_failed(
        self, symbol: str, funding_time: datetime
    ) -> None:
        async with self._write_lock, self._connect() as db:
            await db.execute(
                """
                    UPDATE funding_events
                    SET status = 'confirmation_failed'
                    WHERE symbol = ? AND funding_time = ? AND status != 'confirmed'
                    """,
                (symbol, datetime_to_text(funding_time)),
            )
            await db.commit()

    async def update_next_predicted_rate(
        self,
        symbol: str,
        previous_funding_time: datetime,
        next_predicted_rate: Decimal,
    ) -> None:
        async with self._write_lock, self._connect() as db:
            await db.execute(
                """
                    UPDATE funding_events
                    SET next_predicted_rate = COALESCE(next_predicted_rate, ?)
                    WHERE symbol = ? AND funding_time = ?
                    """,
                (
                    decimal_to_text(next_predicted_rate),
                    symbol,
                    datetime_to_text(previous_funding_time),
                ),
            )
            await db.commit()

    async def status_summary(self) -> dict[str, Any]:
        async with self._connect() as db:
            active_symbols = await self._single_value(
                db, "SELECT COUNT(*) FROM symbols WHERE is_active = 1"
            )
            snapshot_count = await self._single_value(
                db, "SELECT COUNT(*) FROM funding_snapshots"
            )
            event_count = await self._single_value(
                db, "SELECT COUNT(*) FROM funding_events"
            )
            last_snapshot = await self._single_value(
                db, "SELECT MAX(event_time) FROM funding_snapshots"
            )
            status_rows = await db.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM funding_events
                GROUP BY status
                """
            )
            statuses = {row["status"]: row["count"] for row in await status_rows.fetchall()}
        return {
            "active_symbols": active_symbols,
            "snapshot_count": snapshot_count,
            "event_count": event_count,
            "last_snapshot": last_snapshot,
            "waiting": statuses.get("waiting", 0),
            "confirmed": statuses.get("confirmed", 0),
            "confirmation_failed": statuses.get("confirmation_failed", 0),
        }

    async def recent_events(self, limit: int) -> list[FundingEvent]:
        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT *
                FROM funding_events
                ORDER BY funding_time DESC, symbol
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
        return [self._row_to_event(row) for row in rows]

    async def export_events_csv(self, output_path: Path) -> int:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT *
                FROM funding_events
                ORDER BY funding_time, symbol
                """
            )
            rows = await cursor.fetchall()
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

    async def _single_value(self, db: aiosqlite.Connection, query: str) -> Any:
        cursor = await db.execute(query)
        row = await cursor.fetchone()
        return row[0] if row else None

    def _row_to_symbol(self, row: aiosqlite.Row) -> SymbolRecord:
        return SymbolRecord(
            symbol=row["symbol"],
            base_asset=row["base_asset"],
            quote_asset=row["quote_asset"],
            contract_type=row["contract_type"],
            status=row["status"],
            funding_interval_hours=int(row["funding_interval_hours"]),
            is_active=bool(row["is_active"]),
            created_at=text_to_datetime(row["created_at"]),
            updated_at=text_to_datetime(row["updated_at"]),
        )

    def _row_to_snapshot(self, row: aiosqlite.Row) -> FundingSnapshot:
        return FundingSnapshot(
            symbol=row["symbol"],
            event_time=text_to_datetime(row["event_time"]),
            received_at=text_to_datetime(row["received_at"]),
            mark_price=decimal_from_text(row["mark_price"]),
            index_price=decimal_from_text(row["index_price"])
            if row["index_price"] is not None
            else None,
            estimated_settle_price=decimal_from_text(row["estimated_settle_price"])
            if row["estimated_settle_price"] is not None
            else None,
            predicted_funding_rate=decimal_from_text(row["predicted_funding_rate"]),
            interest_rate=decimal_from_text(row["interest_rate"])
            if row["interest_rate"] is not None
            else None,
            next_funding_time=text_to_datetime(row["next_funding_time"]),
            seconds_until_funding=int(row["seconds_until_funding"]),
            capture_mode=row["capture_mode"],
        )

    def _row_to_event(self, row: aiosqlite.Row) -> FundingEvent:
        return FundingEvent(
            symbol=row["symbol"],
            funding_time=text_to_datetime(row["funding_time"]),
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
            confirmed_at=text_to_datetime(row["confirmed_at"])
            if row["confirmed_at"] is not None
            else None,
            status=row["status"],
        )

    def _optional_decimal(self, row: aiosqlite.Row, key: str) -> Decimal | None:
        return decimal_from_text(row[key]) if row[key] is not None else None
