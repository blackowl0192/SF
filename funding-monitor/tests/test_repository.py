from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.models import FundingSnapshot
from funding_monitor.repository import (
    CONFIRM_EVENT_SQL,
    INSERT_SNAPSHOT_SQL,
    UPDATE_EVENT_PREDICTIONS_SQL,
    UPDATE_NEXT_PREDICTED_RATE_SQL,
    UPSERT_FUNDING_EVENT_SQL,
    UPSERT_SYMBOLS_SQL,
    FundingRepository,
)


def test_repository_uses_postgresql_placeholders() -> None:
    sql = (
        f"{UPSERT_SYMBOLS_SQL}\n"
        f"{INSERT_SNAPSHOT_SQL}\n"
        f"{UPSERT_FUNDING_EVENT_SQL}\n"
        f"{UPDATE_EVENT_PREDICTIONS_SQL}\n"
        f"{CONFIRM_EVENT_SQL}\n"
        f"{UPDATE_NEXT_PREDICTED_RATE_SQL}"
    )

    assert "?" not in sql
    assert "$1" in sql


def test_on_conflict_keeps_old_semantics() -> None:
    assert "ON CONFLICT(symbol, event_time, capture_mode) DO NOTHING" in (
        INSERT_SNAPSHOT_SQL
    )
    assert "ON CONFLICT(symbol) DO UPDATE" in UPSERT_SYMBOLS_SQL
    assert "created_at = symbols.created_at" in UPSERT_SYMBOLS_SQL
    assert "ON CONFLICT(symbol, funding_time) DO UPDATE" in UPSERT_FUNDING_EVENT_SQL
    assert "COALESCE(" in UPSERT_FUNDING_EVENT_SQL
    assert "last_predicted_rate = excluded.last_predicted_rate" in (
        UPSERT_FUNDING_EVENT_SQL
    )


def test_repository_passes_decimal_and_utc_datetime_without_float_conversion() -> None:
    connection = RecordingConnection()
    repository = FundingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]
    funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)
    snapshot = FundingSnapshot(
        symbol="BTCUSDT",
        event_time=funding_time - timedelta(minutes=20),
        received_at=funding_time - timedelta(minutes=20),
        mark_price=Decimal("43000.1000"),
        index_price=Decimal("42999.9000"),
        estimated_settle_price=None,
        predicted_funding_rate=Decimal("0.00010000"),
        interest_rate=None,
        next_funding_time=funding_time,
        seconds_until_funding=1200,
        capture_mode="normal",
    )

    import asyncio

    inserted = asyncio.run(repository.insert_snapshot(snapshot))

    assert inserted
    assert isinstance(connection.args[3], Decimal)
    assert isinstance(connection.args[6], Decimal)
    assert connection.args[3] == Decimal("43000.1000")
    assert connection.args[1].tzinfo is UTC
    assert connection.args[8].tzinfo is UTC


class RecordingConnection:
    def __init__(self) -> None:
        self.args = ()

    async def fetchrow(self, _sql, *args):
        self.args = args
        return {"id": 1}


class RecordingDatabase:
    def __init__(self, connection: RecordingConnection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection
