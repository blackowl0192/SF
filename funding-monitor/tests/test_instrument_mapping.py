import asyncio
from datetime import UTC, datetime

import pytest

from funding_monitor.instrument_mapping import (
    FuturesInstrument,
    InstrumentMappingService,
    MappingReason,
    NegativeStrategyStatus,
    SpotExchangeInfoError,
    SpotInstrument,
    SpotMappingStatus,
    SpotSymbolService,
    parse_spot_exchange_info,
)


def test_parse_active_btcusdt_spot_pair() -> None:
    symbols = parse_spot_exchange_info(
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "isSpotTradingAllowed": True,
                }
            ]
        },
        supported_quote_asset="USDT",
    )

    assert symbols == [
        SpotInstrument(
            symbol="BTCUSDT",
            status="TRADING",
            base_asset="BTC",
            quote_asset="USDT",
            trading_allowed=True,
        )
    ]


def test_parse_spot_trading_disabled() -> None:
    symbols = parse_spot_exchange_info(
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "isSpotTradingAllowed": False,
                }
            ]
        },
        supported_quote_asset="USDT",
    )

    assert symbols[0].trading_allowed is False


def test_parse_spot_status_not_trading() -> None:
    symbols = parse_spot_exchange_info(
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "BREAK",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "isSpotTradingAllowed": True,
                }
            ]
        },
        supported_quote_asset="USDT",
    )

    assert symbols[0].trading_allowed is False


def test_parse_unsupported_spot_quote_asset_is_skipped() -> None:
    symbols = parse_spot_exchange_info(
        {
            "symbols": [
                {
                    "symbol": "BTCUSDC",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDC",
                    "isSpotTradingAllowed": True,
                }
            ]
        },
        supported_quote_asset="USDT",
    )

    assert symbols == []


def test_parse_missing_optional_permissions_uses_explicit_flag() -> None:
    symbols = parse_spot_exchange_info(
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "isSpotTradingAllowed": True,
                }
            ]
        },
        supported_quote_asset="USDT",
    )

    assert symbols[0].trading_allowed is True


def test_invalid_spot_record_is_skipped() -> None:
    symbols = parse_spot_exchange_info(
        {
            "symbols": [
                {
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "isSpotTradingAllowed": True,
                }
            ]
        },
        supported_quote_asset="USDT",
    )

    assert symbols == []


def test_btcusdt_futures_maps_to_btcusdt_spot() -> None:
    mapping = build_mapping(futures("BTCUSDT", "BTC"), [spot("BTCUSDT", "BTC")])

    assert mapping.spot_mapping_status == SpotMappingStatus.MATCHED
    assert mapping.mapping_reason == MappingReason.EXACT_BASE_ASSET_MATCH
    assert mapping.spot_symbol == "BTCUSDT"


def test_futures_without_spot_pair_is_missing() -> None:
    mapping = build_mapping(futures("NOPEUSDT", "NOPE"), [])

    assert mapping.spot_mapping_status == SpotMappingStatus.MISSING
    assert mapping.mapping_reason == MappingReason.SPOT_PAIR_MISSING


def test_spot_pair_with_disabled_trading_is_not_matched() -> None:
    mapping = build_mapping(
        futures("BTCUSDT", "BTC"),
        [spot("BTCUSDT", "BTC", trading_allowed=False)],
    )

    assert mapping.spot_mapping_status == SpotMappingStatus.SPOT_TRADING_DISABLED
    assert mapping.mapping_reason == MappingReason.SPOT_TRADING_DISABLED


def test_multiplier_contract_is_ambiguous() -> None:
    mapping = build_mapping(
        futures("1000PEPEUSDT", "1000PEPE"),
        [spot("PEPEUSDT", "PEPE")],
    )

    assert mapping.spot_mapping_status == SpotMappingStatus.AMBIGUOUS
    assert mapping.mapping_reason == MappingReason.MULTIPLIER_CONTRACT
    assert not mapping.positive_strategy_available


def test_multiple_matching_spot_pairs_are_ambiguous() -> None:
    mapping = build_mapping(
        futures("BTCUSDT", "BTC"),
        [spot("BTCUSDT", "BTC"), spot("BTCTESTUSDT", "BTC")],
    )

    assert mapping.spot_mapping_status == SpotMappingStatus.AMBIGUOUS
    assert mapping.mapping_reason == MappingReason.MULTIPLE_SPOT_MATCHES


def test_unsupported_futures_contract_type() -> None:
    mapping = build_mapping(
        futures("BTCUSDT_240628", "BTC", contract_type="CURRENT_QUARTER"),
        [spot("BTCUSDT", "BTC")],
    )

    assert mapping.spot_mapping_status == SpotMappingStatus.UNSUPPORTED
    assert mapping.mapping_reason == MappingReason.UNSUPPORTED_CONTRACT_TYPE


def test_unsupported_futures_quote_asset() -> None:
    mapping = build_mapping(
        futures("BTCUSDC", "BTC", quote_asset="USDC", margin_asset="USDC"),
        [spot("BTCUSDT", "BTC")],
    )

    assert mapping.spot_mapping_status == SpotMappingStatus.UNSUPPORTED
    assert mapping.mapping_reason == MappingReason.UNSUPPORTED_QUOTE_ASSET


def test_metadata_mismatch_is_ambiguous() -> None:
    mapping = build_mapping(futures("BTCUSDT", "BTC"), [spot("XBTUSDT", "BTC")])

    assert mapping.spot_mapping_status == SpotMappingStatus.AMBIGUOUS
    assert mapping.mapping_reason == MappingReason.METADATA_MISMATCH


def test_positive_strategy_available_only_for_matched_active_pair() -> None:
    matched = build_mapping(futures("BTCUSDT", "BTC"), [spot("BTCUSDT", "BTC")])
    missing = build_mapping(futures("NOPEUSDT", "NOPE"), [])
    ambiguous = build_mapping(
        futures("1000PEPEUSDT", "1000PEPE"),
        [spot("PEPEUSDT", "PEPE")],
    )
    disabled = build_mapping(
        futures("BTCUSDT", "BTC"),
        [spot("BTCUSDT", "BTC", trading_allowed=False)],
    )

    assert matched.positive_strategy_available is True
    assert missing.positive_strategy_available is False
    assert ambiguous.positive_strategy_available is False
    assert disabled.positive_strategy_available is False


def test_negative_strategy_status_before_borrow_checks() -> None:
    matched = build_mapping(futures("BTCUSDT", "BTC"), [spot("BTCUSDT", "BTC")])
    missing = build_mapping(futures("NOPEUSDT", "NOPE"), [])
    unsupported = build_mapping(
        futures("BTCUSDC", "BTC", quote_asset="USDC", margin_asset="USDC"),
        [spot("BTCUSDT", "BTC")],
    )

    assert matched.negative_strategy_available is False
    assert (
        matched.negative_strategy_status
        == NegativeStrategyStatus.BORROW_CHECK_NOT_IMPLEMENTED
    )
    assert missing.negative_strategy_status == NegativeStrategyStatus.NOT_APPLICABLE
    assert unsupported.negative_strategy_status == NegativeStrategyStatus.NOT_APPLICABLE


def test_spot_api_failure_does_not_upsert_mappings() -> None:
    repository = FakeMappingRepository()
    service = InstrumentMappingService(
        repository=repository,
        futures_client=FakeFuturesClient(),
        spot_service=SpotSymbolService(
            FailingSpotClient(),
            supported_quote_asset="USDT",
        ),
        supported_quote_asset="USDT",
    )

    async def scenario() -> None:
        with pytest.raises(SpotExchangeInfoError):
            await service.sync_mappings()

    asyncio.run(scenario())

    assert repository.upsert_calls == 0


def test_execution_eligibility_is_metadata_only() -> None:
    repository = FakeMappingRepository()
    repository.mapping = build_mapping(
        futures("BTCUSDT", "BTC"),
        [spot("BTCUSDT", "BTC")],
    )
    service = InstrumentMappingService(repository=repository)

    eligibility = asyncio.run(service.get_execution_eligibility("BTCUSDT"))

    assert eligibility.futures_symbol == "BTCUSDT"
    assert eligibility.spot_symbol == "BTCUSDT"
    assert eligibility.positive_strategy_available is True
    assert eligibility.negative_strategy_available is False
    assert "borrow_check_not_implemented" in eligibility.rejection_reasons


def build_mapping(
    futures_instrument: FuturesInstrument, spots: list[SpotInstrument]
):
    return InstrumentMappingService(
        repository=FakeMappingRepository(),
        supported_quote_asset="USDT",
    ).build_mapping(
        futures_instrument,
        spots,
        updated_at=datetime(2024, 1, 1, tzinfo=UTC),
    )


def futures(
    symbol: str,
    base_asset: str,
    *,
    contract_type: str = "PERPETUAL",
    status: str = "TRADING",
    quote_asset: str = "USDT",
    margin_asset: str = "USDT",
) -> FuturesInstrument:
    return FuturesInstrument(
        symbol=symbol,
        pair=symbol,
        contract_type=contract_type,
        status=status,
        base_asset=base_asset,
        quote_asset=quote_asset,
        margin_asset=margin_asset,
    )


def spot(
    symbol: str,
    base_asset: str,
    *,
    status: str = "TRADING",
    trading_allowed: bool = True,
) -> SpotInstrument:
    return SpotInstrument(
        symbol=symbol,
        status=status,
        base_asset=base_asset,
        quote_asset="USDT",
        trading_allowed=trading_allowed,
    )


class FakeMappingRepository:
    def __init__(self) -> None:
        self.upsert_calls = 0
        self.mapping = None

    async def upsert_mappings(self, mappings):
        self.upsert_calls += 1
        rows = list(mappings)
        self.mapping = rows[0] if rows else None
        return len(rows)

    async def get_mapping(self, futures_symbol: str):
        if self.mapping is None or self.mapping.futures_symbol != futures_symbol:
            return None
        return self.mapping


class FakeFuturesClient:
    async def get_exchange_info(self):
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "pair": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                }
            ]
        }


class FailingSpotClient:
    async def get_exchange_info(self):
        raise TimeoutError("spot exchangeInfo timeout")
