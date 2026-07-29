import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from funding_monitor.instrument_mapping import (
    InstrumentMapping,
    MappingReason,
    NegativeStrategyStatus,
    SpotMappingStatus,
)
from funding_monitor.instrument_repository import (
    UPSERT_INSTRUMENT_MAPPING_SQL,
    InstrumentMappingRepository,
)


def test_instrument_mapping_upsert_uses_batch_and_unique_symbol() -> None:
    connection = RecordingConnection()
    repository = InstrumentMappingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]

    count = asyncio.run(repository.upsert_mappings([mapping("BTCUSDT")]))

    assert count == 1
    assert connection.executemany_calls == 1
    assert len(connection.args) == 1
    assert connection.args[0][0] == "BTCUSDT"
    assert connection.args[0][13] == "matched"
    assert connection.args[0][17] == "borrow_check_not_implemented"


def test_repeated_sync_updates_existing_mapping_without_duplicate_symbol() -> None:
    assert "ON CONFLICT(futures_symbol) DO UPDATE SET" in UPSERT_INSTRUMENT_MAPPING_SQL
    assert "created_at = instrument_mappings.created_at" in (
        UPSERT_INSTRUMENT_MAPPING_SQL
    )


def test_instrument_mapping_summary_aggregates() -> None:
    connection = SummaryConnection()
    repository = InstrumentMappingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]

    summary = asyncio.run(repository.summary())

    assert summary.table_available
    assert summary.futures_symbols_processed == 4
    assert summary.futures_with_spot == 2
    assert summary.futures_without_spot == 2
    assert summary.matched == 1
    assert summary.missing == 1
    assert summary.ambiguous == 1
    assert summary.unsupported == 1
    assert summary.positive_strategy_available == 1
    assert summary.negative_strategy_available == 0
    assert summary.negative_strategy_pending_borrow_implementation == 1


def test_get_mapping_reads_mapping_by_futures_symbol() -> None:
    connection = GetMappingConnection()
    repository = InstrumentMappingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]

    result = asyncio.run(repository.get_mapping("BTCUSDT"))

    assert connection.args == ("BTCUSDT",)
    assert result is not None
    assert result.futures_symbol == "BTCUSDT"
    assert result.spot_mapping_status == SpotMappingStatus.MATCHED
    assert result.mapping_reason == MappingReason.EXACT_BASE_ASSET_MATCH


def test_list_mappings_filters_by_status() -> None:
    connection = ListMappingsConnection()
    repository = InstrumentMappingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]

    results = asyncio.run(
        repository.list_mappings(status=SpotMappingStatus.AMBIGUOUS)
    )

    assert connection.args == ("ambiguous",)
    assert "WHERE spot_mapping_status = $1" in connection.sql
    assert len(results) == 1


def mapping(futures_symbol: str) -> InstrumentMapping:
    timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    return InstrumentMapping(
        futures_symbol=futures_symbol,
        futures_pair=futures_symbol,
        futures_base_asset="BTC",
        futures_quote_asset="USDT",
        futures_margin_asset="USDT",
        futures_contract_type="PERPETUAL",
        futures_status="TRADING",
        spot_symbol="BTCUSDT",
        spot_base_asset="BTC",
        spot_quote_asset="USDT",
        spot_status="TRADING",
        spot_trading_allowed=True,
        spot_pair_exists=True,
        spot_mapping_status=SpotMappingStatus.MATCHED,
        mapping_reason=MappingReason.EXACT_BASE_ASSET_MATCH,
        positive_strategy_available=True,
        negative_strategy_available=False,
        negative_strategy_status=NegativeStrategyStatus.BORROW_CHECK_NOT_IMPLEMENTED,
        mapping_source="binance_exchange_info",
        mapping_updated_at=timestamp,
        created_at=timestamp,
        updated_at=timestamp,
    )


def row() -> dict[str, object]:
    item = mapping("BTCUSDT")
    return {
        "futures_symbol": item.futures_symbol,
        "futures_pair": item.futures_pair,
        "futures_base_asset": item.futures_base_asset,
        "futures_quote_asset": item.futures_quote_asset,
        "futures_margin_asset": item.futures_margin_asset,
        "futures_contract_type": item.futures_contract_type,
        "futures_status": item.futures_status,
        "spot_symbol": item.spot_symbol,
        "spot_base_asset": item.spot_base_asset,
        "spot_quote_asset": item.spot_quote_asset,
        "spot_status": item.spot_status,
        "spot_trading_allowed": item.spot_trading_allowed,
        "spot_pair_exists": item.spot_pair_exists,
        "spot_mapping_status": item.spot_mapping_status.value,
        "mapping_reason": item.mapping_reason.value,
        "positive_strategy_available": item.positive_strategy_available,
        "negative_strategy_available": item.negative_strategy_available,
        "negative_strategy_status": item.negative_strategy_status.value,
        "mapping_source": item.mapping_source,
        "mapping_updated_at": item.mapping_updated_at,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


class RecordingConnection:
    def __init__(self) -> None:
        self.executemany_calls = 0
        self.args = []

    async def executemany(self, _sql, args):
        self.executemany_calls += 1
        self.args = list(args)


class SummaryConnection:
    async def fetchrow(self, _sql, *args):
        return {
            "futures_symbols_processed": 4,
            "futures_with_spot": 2,
            "futures_without_spot": 2,
            "matched": 1,
            "missing": 1,
            "ambiguous": 1,
            "unsupported": 1,
            "spot_trading_disabled": 0,
            "positive_strategy_available": 1,
            "negative_strategy_available": 0,
            "negative_strategy_pending_borrow_implementation": 1,
            "mappings_last_updated_at": datetime(2024, 1, 1, tzinfo=UTC),
        }


class GetMappingConnection:
    def __init__(self) -> None:
        self.args = ()

    async def fetchrow(self, _sql, *args):
        self.args = args
        return row()


class ListMappingsConnection:
    def __init__(self) -> None:
        self.sql = ""
        self.args = ()

    async def fetch(self, sql, *args):
        self.sql = sql
        self.args = args
        return [row()]


class RecordingDatabase:
    def __init__(self, connection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection
