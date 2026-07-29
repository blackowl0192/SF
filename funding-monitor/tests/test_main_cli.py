from datetime import UTC, datetime

from funding_monitor.instrument_mapping import (
    InstrumentMapping,
    InstrumentMappingSummary,
    InstrumentMappingSyncResult,
    MappingReason,
    NegativeStrategyStatus,
    SpotMappingStatus,
)
from funding_monitor.main import (
    _build_parser,
    _print_instrument_mapping_summary,
    _print_mapping_details,
    _print_mapping_status_summary,
    _print_mapping_sync_result,
)


def test_parser_accepts_instrument_mapping_commands() -> None:
    parser = _build_parser()

    sync_args = parser.parse_args(["sync-instrument-mappings"])
    status_args = parser.parse_args(["instrument-mappings", "--status", "matched"])
    symbol_args = parser.parse_args(["instrument-mappings", "--symbol", "BTCUSDT"])

    assert sync_args.command == "sync-instrument-mappings"
    assert status_args.status == "matched"
    assert symbol_args.symbol == "BTCUSDT"


def test_sync_instrument_mappings_prints_aggregates(capsys) -> None:
    _print_mapping_sync_result(
        InstrumentMappingSyncResult(
            futures_symbols_processed=2,
            matched=1,
            missing=1,
            ambiguous=0,
            unsupported=0,
            spot_trading_disabled=0,
            positive_strategy_available=1,
            negative_strategy_available=0,
            negative_strategy_pending_borrow_implementation=1,
            synchronization_duration_seconds=1.25,
            updated_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
    )

    output = capsys.readouterr().out

    assert "futures_symbols_processed: 2" in output
    assert "matched: 1" in output
    assert "negative_strategy_available: 0" in output


def test_instrument_mappings_default_output(capsys) -> None:
    _print_instrument_mapping_summary(summary())

    output = capsys.readouterr().out

    assert "futures_symbols_processed: 2" in output
    assert "futures_with_spot: 1" in output
    assert "positive_strategy_available: 1" in output


def test_status_mapping_statistics_output(capsys) -> None:
    _print_mapping_status_summary(summary())

    output = capsys.readouterr().out

    assert "instrument_mappings: 2" in output
    assert "ambiguous_spot_mappings: 0" in output
    assert "negative_strategy_pending_borrow_check: 1" in output


def test_instrument_mappings_detail_output(capsys) -> None:
    _print_mapping_details([mapping()])

    output = capsys.readouterr().out

    assert "futures_symbol futures_base_asset" in output
    assert "BTCUSDT BTC PERPETUAL TRADING BTCUSDT matched" in output


def summary() -> InstrumentMappingSummary:
    return InstrumentMappingSummary(
        table_available=True,
        futures_symbols_processed=2,
        futures_with_spot=1,
        futures_without_spot=1,
        matched=1,
        missing=1,
        ambiguous=0,
        unsupported=0,
        spot_trading_disabled=0,
        positive_strategy_available=1,
        negative_strategy_available=0,
        negative_strategy_pending_borrow_implementation=1,
        mappings_last_updated_at=datetime(2024, 1, 1, tzinfo=UTC),
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
