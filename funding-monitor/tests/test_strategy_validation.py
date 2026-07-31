import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_monitor.instrument_mapping import (
    InstrumentMapping,
    MappingReason,
    NegativeStrategyStatus,
    SpotMappingStatus,
)
from funding_monitor.models import FundingEvent, FundingSnapshot
from funding_monitor.strategy_validation.data_quality import DataQualityEvaluator
from funding_monitor.strategy_validation.economics import EconomicCalculator
from funding_monitor.strategy_validation.market_data import HistoricalMarketDataProvider
from funding_monitor.strategy_validation.models import (
    DataQualityStatus,
    EntryMode,
    MarketPrice,
    MarketPriceSet,
    OutcomeStatus,
    RejectionReason,
    StrategyValidationConfig,
    StrategyValidationDataset,
    StrategyValidationEvent,
    ValidationMode,
)
from funding_monitor.strategy_validation.parameter_grid import (
    ParameterGridTooLargeError,
    StrategyParameterGrid,
)
from funding_monitor.strategy_validation.replay_engine import StrategyReplayEngine
from funding_monitor.strategy_validation.repository import (
    INSERT_RESULT_SQL,
    StrategyValidationRepository,
)
from funding_monitor.strategy_validation.signal_detector import SignalDetector

FUNDING_TIME = datetime(2024, 1, 1, 8, tzinfo=UTC)
DEFAULT_MAPPING = object()


def test_config_hash_is_stable_and_changes_with_parameters() -> None:
    config = StrategyValidationConfig()
    same_config = StrategyValidationConfig()
    changed_config = StrategyValidationConfig(
        funding_threshold=Decimal("0.0005")
    )

    assert config.config_hash() == same_config.config_hash()
    assert config.config_hash() != changed_config.config_hash()
    assert config.to_dict()["strategy_name"] == (
        "positive_funding_spot_long_perp_short"
    )


def test_fixed_entry_detector_does_not_look_ahead_after_entry_time() -> None:
    config = StrategyValidationConfig(
        entry_mode=EntryMode.FIXED_TIME,
        minimum_history_minutes=0,
        minimum_persistence_ratio=Decimal(0),
        maximum_funding_std=Decimal(1),
        maximum_prediction_drop=Decimal(1),
        entry_minutes_before_funding=60,
    )
    snapshots = [
        snapshot(minutes_before=65, rate=Decimal("0.0003")),
        snapshot(minutes_before=60, rate=Decimal("0.0004")),
        snapshot(minutes_before=30, rate=Decimal("0.0010")),
    ]

    signal = SignalDetector(config).detect(snapshots, FUNDING_TIME)

    assert signal.signal_detected
    assert signal.entry_time == FUNDING_TIME - timedelta(minutes=60)
    assert signal.predicted_funding_at_entry == Decimal("0.0004")


def test_first_qualifying_signal_waits_for_confirmation_window() -> None:
    config = StrategyValidationConfig(
        entry_mode=EntryMode.FIRST_QUALIFYING_SIGNAL,
        signal_confirmation_minutes=2,
        minimum_history_minutes=0,
        minimum_persistence_ratio=Decimal(0),
        maximum_funding_std=Decimal(1),
    )
    snapshots = [
        snapshot(minutes_before=65, rate=Decimal("0.0001")),
        snapshot(minutes_before=59, rate=Decimal("0.0004")),
        snapshot(minutes_before=58, rate=Decimal("0.0004")),
        snapshot(minutes_before=57, rate=Decimal("0.0004")),
    ]

    signal = SignalDetector(config).detect(snapshots, FUNDING_TIME)

    assert signal.signal_detected
    assert signal.signal_started_at == FUNDING_TIME - timedelta(minutes=59)
    assert signal.entry_time == FUNDING_TIME - timedelta(minutes=57)


def test_data_quality_marks_missing_snapshots_invalid() -> None:
    result = DataQualityEvaluator(StrategyValidationConfig()).evaluate(
        [],
        mapping(),
    )

    assert result.status == DataQualityStatus.INVALID
    assert result.reasons == ("no_snapshots",)


def test_data_quality_flags_large_snapshot_gap() -> None:
    result = DataQualityEvaluator(
        StrategyValidationConfig(maximum_snapshot_age_seconds=60)
    ).evaluate(
        [
            snapshot(minutes_before=70, rate=Decimal("0.0004")),
            snapshot(minutes_before=65, rate=Decimal("0.0004")),
        ],
        mapping(),
    )

    assert result.status == DataQualityStatus.POOR
    assert "large_snapshot_gap" in result.reasons
    assert result.maximum_gap_seconds == 300


def test_funding_only_economics_does_not_report_net_profit() -> None:
    config = StrategyValidationConfig(validation_mode=ValidationMode.FUNDING_ONLY)

    result = EconomicCalculator().evaluate(
        config=config,
        realized_funding_rate=Decimal("0.0005"),
        market_prices=MarketPriceSet(entry=None, exit=None),
    )

    assert result.gross_funding_pnl == Decimal("0.5000")
    assert result.gross_return_rate == Decimal("0.0005")
    assert result.net_pnl is None
    assert result.net_return_rate is None


def test_full_economic_economics_requires_market_prices() -> None:
    config = StrategyValidationConfig(validation_mode=ValidationMode.FULL_ECONOMIC)

    result = EconomicCalculator().evaluate(
        config=config,
        realized_funding_rate=Decimal("0.0005"),
        market_prices=MarketPriceSet(entry=None, exit=None),
    )

    assert not result.market_data_complete
    assert result.net_pnl is None


def test_replay_engine_returns_insufficient_market_data_for_full_mode_without_provider() -> None:
    config = StrategyValidationConfig(
        validation_mode=ValidationMode.FULL_ECONOMIC,
        minimum_history_minutes=0,
        minimum_persistence_ratio=Decimal(0),
        maximum_funding_std=Decimal(1),
    )
    event = validation_event()

    result = asyncio.run(
        StrategyReplayEngine(config).replay_event(event, run_id=1)
    )

    assert result.eligible
    assert result.signal_detected
    assert result.outcome_status == OutcomeStatus.INSUFFICIENT_MARKET_DATA
    assert result.gross_funding_pnl == Decimal("0.5000")
    assert result.net_pnl is None


def test_replay_engine_calculates_full_economic_result_when_prices_exist() -> None:
    config = StrategyValidationConfig(
        validation_mode=ValidationMode.FULL_ECONOMIC,
        minimum_history_minutes=0,
        minimum_persistence_ratio=Decimal(0),
        maximum_funding_std=Decimal(1),
    )
    event = validation_event()

    result = asyncio.run(
        StrategyReplayEngine(
            config,
            market_data_provider=StaticMarketDataProvider(),
        ).replay_event(event, run_id=1)
    )

    assert result.outcome_status == OutcomeStatus.FULL_ECONOMIC
    assert result.market_data_complete
    assert result.net_pnl is not None
    assert result.profitable


def test_replay_engine_rejects_unmapped_symbols() -> None:
    config = StrategyValidationConfig(
        minimum_history_minutes=0,
        minimum_persistence_ratio=Decimal(0),
        maximum_funding_std=Decimal(1),
    )
    event = validation_event(mapping_value=None)

    result = asyncio.run(
        StrategyReplayEngine(config).replay_event(event, run_id=1)
    )

    assert not result.eligible
    assert result.rejection_reason == RejectionReason.MISSING_SPOT_MAPPING
    assert result.outcome_status == OutcomeStatus.REJECTED
    assert result.gross_funding_pnl is None
    assert result.gross_return_rate is None


def test_parameter_grid_is_deterministic_and_guarded() -> None:
    grid = StrategyParameterGrid(
        funding_thresholds=(Decimal("0.0002"), Decimal("0.0003")),
        entry_minutes_before_funding=(30, 60),
    )

    configs = grid.iter_configs(StrategyValidationConfig())

    assert [config.funding_threshold for config in configs] == [
        Decimal("0.0002"),
        Decimal("0.0002"),
        Decimal("0.0003"),
        Decimal("0.0003"),
    ]
    assert [config.entry_minutes_before_funding for config in configs] == [
        30,
        60,
        30,
        60,
    ]
    with pytest.raises(ParameterGridTooLargeError):
        StrategyParameterGrid(
            funding_thresholds=(Decimal("0.0002"), Decimal("0.0003")),
            entry_minutes_before_funding=(30, 60),
            max_combinations=3,
        ).iter_configs(StrategyValidationConfig())


def test_strategy_validation_sql_is_idempotent() -> None:
    assert (
        "ON CONFLICT(run_id, exchange, symbol, funding_time, config_hash)"
        in INSERT_RESULT_SQL
    )
    assert "$1" in INSERT_RESULT_SQL
    assert "?" not in INSERT_RESULT_SQL


def test_repository_serializes_result_json_and_decimals() -> None:
    repository = StrategyValidationRepository(
        RecordingDatabase(RecordingConnection())  # type: ignore[arg-type]
    )
    result = asyncio.run(
        StrategyReplayEngine(
            StrategyValidationConfig(
                minimum_history_minutes=0,
                minimum_persistence_ratio=Decimal(0),
                maximum_funding_std=Decimal(1),
            )
        ).replay_event(validation_event(), run_id=7)
    )

    args = repository._result_args(result)

    assert args[0] == 7
    assert args[2] == "BTCUSDT"
    assert isinstance(args[16], Decimal)
    assert args[32] == "funding_only"
    assert args[34].startswith("[")
    assert args[51].startswith("{")


def test_repository_create_run_serializes_config_and_dataset() -> None:
    connection = RecordingConnection(fetchval_value=11)
    repository = StrategyValidationRepository(RecordingDatabase(connection))  # type: ignore[arg-type]

    run_id = asyncio.run(
        repository.create_run(
            StrategyValidationConfig(),
            StrategyValidationDataset(
                period_start=FUNDING_TIME - timedelta(days=1),
                period_end=FUNDING_TIME,
                requested_symbols=("BTCUSDT",),
                limit=10,
            ),
        )
    )

    assert run_id == 11
    assert connection.args[0] == "positive_funding_spot_long_perp_short"
    assert connection.args[3] == "funding_only"
    assert '"requested_symbols":["BTCUSDT"]' in connection.args[6]
    assert connection.args[9] == '["BTCUSDT"]'


class StaticMarketDataProvider(HistoricalMarketDataProvider):
    async def get_spot_futures_prices_at(
        self,
        *,
        exchange: str,
        futures_symbol: str,
        spot_symbol: str,
        timestamp: datetime,
    ) -> MarketPrice:
        if timestamp < FUNDING_TIME:
            return MarketPrice(
                spot_price=Decimal(100),
                futures_price=Decimal(101),
                timestamp=timestamp,
            )
        return MarketPrice(
            spot_price=Decimal(102),
            futures_price=Decimal(101),
            timestamp=timestamp,
        )


def validation_event(
    mapping_value: InstrumentMapping | None | object = DEFAULT_MAPPING,
) -> StrategyValidationEvent:
    resolved_mapping = mapping() if mapping_value is DEFAULT_MAPPING else mapping_value
    return StrategyValidationEvent(
        exchange="BINANCE",
        symbol="BTCUSDT",
        funding_event=FundingEvent(
            symbol="BTCUSDT",
            funding_time=FUNDING_TIME,
            funding_interval_hours=8,
            actual_funding_rate=Decimal("0.0005"),
            status="confirmed",
        ),
        snapshots=(
            snapshot(minutes_before=70, rate=Decimal("0.0004")),
            snapshot(minutes_before=60, rate=Decimal("0.0005")),
        ),
        mapping=resolved_mapping if isinstance(resolved_mapping, InstrumentMapping) else None,
    )


def snapshot(*, minutes_before: int, rate: Decimal) -> FundingSnapshot:
    event_time = FUNDING_TIME - timedelta(minutes=minutes_before)
    return FundingSnapshot(
        symbol="BTCUSDT",
        event_time=event_time,
        received_at=event_time,
        mark_price=Decimal(101),
        index_price=Decimal(100),
        estimated_settle_price=None,
        predicted_funding_rate=rate,
        funding_rate=rate,
        interest_rate=None,
        next_funding_time=FUNDING_TIME,
        seconds_until_funding=minutes_before * 60,
        seconds_to_funding=minutes_before * 60,
        premium_rate=Decimal("0.01"),
        funding_direction="positive",
        funding_interval_hours=8,
        capture_mode="normal",
    )


def mapping() -> InstrumentMapping:
    timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    return InstrumentMapping(
        futures_symbol="BTCUSDT",
        futures_pair="BTCUSDT",
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


class RecordingConnection:
    def __init__(self, fetchval_value=1) -> None:
        self.fetchval_value = fetchval_value
        self.args = ()

    async def fetchval(self, _sql, *args):
        self.args = args
        return self.fetchval_value


class RecordingDatabase:
    def __init__(self, connection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection
