import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.database import initialize_database
from funding_monitor.models import FundingSnapshot, SymbolRecord
from funding_monitor.repository import FundingRepository


def run(coro):
    return asyncio.run(coro)


def test_upsert_symbols_is_idempotent(tmp_path) -> None:
    async def scenario() -> None:
        database_path = tmp_path / "funding.db"
        await initialize_database(database_path)
        repository = FundingRepository(database_path)
        now = datetime(2024, 1, 1, tzinfo=UTC)
        symbol = SymbolRecord(
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            contract_type="PERPETUAL",
            status="TRADING",
            funding_interval_hours=8,
            is_active=True,
            created_at=now,
            updated_at=now,
        )

        assert await repository.upsert_symbols([symbol]) == 1
        assert await repository.upsert_symbols([symbol]) == 1
        active_symbols = await repository.active_symbols()
        summary = await repository.status_summary()

        assert list(active_symbols) == ["BTCUSDT"]
        assert summary["active_symbols"] == 1

    run(scenario())


def test_snapshot_insert_and_event_creation(tmp_path) -> None:
    async def scenario() -> None:
        database_path = tmp_path / "funding.db"
        await initialize_database(database_path)
        repository = FundingRepository(database_path)
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

        assert await repository.insert_snapshot(snapshot)
        assert not await repository.insert_snapshot(snapshot)
        event = await repository.create_or_get_funding_event(
            "BTCUSDT", funding_time, 8, Decimal("0.00010000")
        )
        duplicate = await repository.create_or_get_funding_event(
            "BTCUSDT", funding_time, 8, Decimal("0.00020000")
        )
        summary = await repository.status_summary()

        assert event.status == "waiting"
        assert duplicate.first_predicted_rate == Decimal("0.00010000")
        assert duplicate.last_predicted_rate == Decimal("0.00020000")
        assert summary["snapshot_count"] == 1
        assert summary["event_count"] == 1

    run(scenario())
