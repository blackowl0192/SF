from __future__ import annotations

import logging
import re
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

import asyncpg
import httpx

from .models import ensure_utc, utc_now

logger = logging.getLogger(__name__)

MAPPING_SOURCE = "binance_exchange_info"
MULTIPLIER_SYMBOL_PATTERN = re.compile(r"^\d+[A-Z0-9]+USDT$")


class SpotMappingStatus(StrEnum):
    MATCHED = "matched"
    MISSING = "missing"
    AMBIGUOUS = "ambiguous"
    UNSUPPORTED = "unsupported"
    SPOT_TRADING_DISABLED = "spot_trading_disabled"


class NegativeStrategyStatus(StrEnum):
    BORROW_CHECK_NOT_IMPLEMENTED = "borrow_check_not_implemented"
    BORROW_CHECK_PENDING = "borrow_check_pending"
    BORROW_AVAILABLE = "borrow_available"
    BORROW_UNAVAILABLE = "borrow_unavailable"
    BORROW_COST_TOO_HIGH = "borrow_cost_too_high"
    NOT_APPLICABLE = "not_applicable"


class MappingReason(StrEnum):
    EXACT_BASE_ASSET_MATCH = "exact_base_asset_match"
    SPOT_PAIR_MISSING = "spot_pair_missing"
    SPOT_TRADING_DISABLED = "spot_trading_disabled"
    MULTIPLIER_CONTRACT = "multiplier_contract"
    UNSUPPORTED_QUOTE_ASSET = "unsupported_quote_asset"
    UNSUPPORTED_MARGIN_ASSET = "unsupported_margin_asset"
    UNSUPPORTED_CONTRACT_TYPE = "unsupported_contract_type"
    UNSUPPORTED_FUTURES_STATUS = "unsupported_futures_status"
    UNSUPPORTED_BASE_ASSET = "unsupported_base_asset"
    MULTIPLE_SPOT_MATCHES = "multiple_spot_matches"
    METADATA_MISMATCH = "metadata_mismatch"


@dataclass(frozen=True)
class FuturesInstrument:
    symbol: str
    pair: str
    contract_type: str
    status: str
    base_asset: str
    quote_asset: str
    margin_asset: str


@dataclass(frozen=True)
class SpotInstrument:
    symbol: str
    status: str
    base_asset: str
    quote_asset: str
    trading_allowed: bool


@dataclass(frozen=True)
class InstrumentMapping:
    futures_symbol: str
    futures_pair: str
    futures_base_asset: str
    futures_quote_asset: str
    futures_margin_asset: str
    futures_contract_type: str
    futures_status: str
    spot_symbol: str | None
    spot_base_asset: str | None
    spot_quote_asset: str | None
    spot_status: str | None
    spot_trading_allowed: bool
    spot_pair_exists: bool
    spot_mapping_status: SpotMappingStatus
    mapping_reason: MappingReason | None
    positive_strategy_available: bool
    negative_strategy_available: bool
    negative_strategy_status: NegativeStrategyStatus
    mapping_source: str
    mapping_updated_at: datetime
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class InstrumentMappingSummary:
    table_available: bool
    futures_symbols_processed: int
    futures_with_spot: int
    futures_without_spot: int
    matched: int
    missing: int
    ambiguous: int
    unsupported: int
    spot_trading_disabled: int
    positive_strategy_available: int
    negative_strategy_available: int
    negative_strategy_pending_borrow_implementation: int
    mappings_last_updated_at: datetime | None


@dataclass(frozen=True)
class InstrumentMappingSyncResult:
    futures_symbols_processed: int
    matched: int
    missing: int
    ambiguous: int
    unsupported: int
    spot_trading_disabled: int
    positive_strategy_available: int
    negative_strategy_available: int
    negative_strategy_pending_borrow_implementation: int
    synchronization_duration_seconds: float
    updated_at: datetime


@dataclass(frozen=True)
class ExecutionEligibility:
    futures_symbol: str
    spot_symbol: str | None
    spot_mapping_status: SpotMappingStatus | None
    positive_strategy_available: bool
    negative_strategy_available: bool
    negative_strategy_status: NegativeStrategyStatus | None
    rejection_reasons: tuple[str, ...]


class FuturesExchangeInfoClient(Protocol):
    async def get_exchange_info(self) -> dict[str, Any]:
        ...


class SpotExchangeInfoClient(Protocol):
    async def get_exchange_info(self) -> dict[str, Any]:
        ...


class InstrumentMappingStore(Protocol):
    async def upsert_mappings(self, mappings: Iterable[InstrumentMapping]) -> int:
        ...

    async def get_mapping(self, futures_symbol: str) -> InstrumentMapping | None:
        ...


class SpotExchangeInfoError(RuntimeError):
    pass


class InstrumentMappingSyncError(RuntimeError):
    pass


class SpotSymbolService:
    def __init__(
        self,
        spot_client: SpotExchangeInfoClient,
        *,
        supported_quote_asset: str,
    ) -> None:
        self.spot_client = spot_client
        self.supported_quote_asset = supported_quote_asset

    async def load_symbols(self) -> list[SpotInstrument]:
        try:
            exchange_info = await self.spot_client.get_exchange_info()
            return parse_spot_exchange_info(
                exchange_info,
                supported_quote_asset=self.supported_quote_asset,
            )
        except (TimeoutError, httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            raise SpotExchangeInfoError(
                f"could not load Binance spot exchangeInfo: {exc}"
            ) from exc


class InstrumentMappingService:
    def __init__(
        self,
        *,
        repository: InstrumentMappingStore,
        futures_client: FuturesExchangeInfoClient | None = None,
        spot_service: SpotSymbolService | None = None,
        supported_quote_asset: str = "USDT",
    ) -> None:
        self.repository = repository
        self.futures_client = futures_client
        self.spot_service = spot_service
        self.supported_quote_asset = supported_quote_asset

    async def sync_mappings(self) -> InstrumentMappingSyncResult:
        if self.futures_client is None or self.spot_service is None:
            raise InstrumentMappingSyncError("futures and spot clients are required")

        started_at = time.perf_counter()
        updated_at = utc_now()
        try:
            futures_exchange_info = await self.futures_client.get_exchange_info()
            futures_instruments = parse_futures_exchange_info(futures_exchange_info)
            spot_instruments = await self.spot_service.load_symbols()
            mappings = [
                self.build_mapping(
                    futures_instrument,
                    spot_instruments,
                    updated_at=updated_at,
                )
                for futures_instrument in futures_instruments
            ]
            await self.repository.upsert_mappings(mappings)
        except SpotExchangeInfoError:
            raise
        except (asyncpg.PostgresError, httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            raise InstrumentMappingSyncError(f"instrument mapping sync failed: {exc}") from exc

        duration = time.perf_counter() - started_at
        return _sync_result_from_mappings(
            mappings,
            synchronization_duration_seconds=duration,
            updated_at=updated_at,
        )

    def build_mapping(
        self,
        futures: FuturesInstrument,
        spot_instruments: Iterable[SpotInstrument],
        *,
        updated_at: datetime | None = None,
    ) -> InstrumentMapping:
        timestamp = ensure_utc(updated_at or utc_now())
        if futures.contract_type != "PERPETUAL":
            return self._mapping(
                futures,
                status=SpotMappingStatus.UNSUPPORTED,
                reason=MappingReason.UNSUPPORTED_CONTRACT_TYPE,
                updated_at=timestamp,
            )
        if futures.status != "TRADING":
            return self._mapping(
                futures,
                status=SpotMappingStatus.UNSUPPORTED,
                reason=MappingReason.UNSUPPORTED_FUTURES_STATUS,
                updated_at=timestamp,
            )
        if futures.quote_asset != self.supported_quote_asset:
            return self._mapping(
                futures,
                status=SpotMappingStatus.UNSUPPORTED,
                reason=MappingReason.UNSUPPORTED_QUOTE_ASSET,
                updated_at=timestamp,
            )
        if futures.margin_asset != self.supported_quote_asset:
            return self._mapping(
                futures,
                status=SpotMappingStatus.UNSUPPORTED,
                reason=MappingReason.UNSUPPORTED_MARGIN_ASSET,
                updated_at=timestamp,
            )
        if not futures.base_asset:
            return self._mapping(
                futures,
                status=SpotMappingStatus.UNSUPPORTED,
                reason=MappingReason.UNSUPPORTED_BASE_ASSET,
                updated_at=timestamp,
            )
        if _is_multiplier_contract(futures):
            return self._mapping(
                futures,
                status=SpotMappingStatus.AMBIGUOUS,
                reason=MappingReason.MULTIPLIER_CONTRACT,
                updated_at=timestamp,
            )

        candidates = [
            spot
            for spot in spot_instruments
            if spot.base_asset == futures.base_asset
            and spot.quote_asset == self.supported_quote_asset
        ]
        if not candidates:
            return self._mapping(
                futures,
                status=SpotMappingStatus.MISSING,
                reason=MappingReason.SPOT_PAIR_MISSING,
                updated_at=timestamp,
            )
        if len(candidates) > 1:
            return self._mapping(
                futures,
                spot=candidates[0],
                status=SpotMappingStatus.AMBIGUOUS,
                reason=MappingReason.MULTIPLE_SPOT_MATCHES,
                spot_pair_exists=True,
                updated_at=timestamp,
            )

        spot = candidates[0]
        if futures.pair != spot.symbol:
            return self._mapping(
                futures,
                spot=spot,
                status=SpotMappingStatus.AMBIGUOUS,
                reason=MappingReason.METADATA_MISMATCH,
                spot_pair_exists=True,
                updated_at=timestamp,
            )
        if spot.status != "TRADING" or not spot.trading_allowed:
            return self._mapping(
                futures,
                spot=spot,
                status=SpotMappingStatus.SPOT_TRADING_DISABLED,
                reason=MappingReason.SPOT_TRADING_DISABLED,
                spot_pair_exists=True,
                updated_at=timestamp,
            )
        return self._mapping(
            futures,
            spot=spot,
            status=SpotMappingStatus.MATCHED,
            reason=MappingReason.EXACT_BASE_ASSET_MATCH,
            spot_pair_exists=True,
            positive_strategy_available=True,
            negative_strategy_status=NegativeStrategyStatus.BORROW_CHECK_NOT_IMPLEMENTED,
            updated_at=timestamp,
        )

    async def get_execution_eligibility(
        self, futures_symbol: str
    ) -> ExecutionEligibility:
        mapping = await self.repository.get_mapping(futures_symbol)
        if mapping is None:
            return ExecutionEligibility(
                futures_symbol=futures_symbol,
                spot_symbol=None,
                spot_mapping_status=None,
                positive_strategy_available=False,
                negative_strategy_available=False,
                negative_strategy_status=None,
                rejection_reasons=("mapping_not_found",),
            )
        rejection_reasons: list[str] = []
        if not mapping.positive_strategy_available:
            rejection_reasons.append(mapping.mapping_reason.value if mapping.mapping_reason else mapping.spot_mapping_status.value)
        if not mapping.negative_strategy_available:
            rejection_reasons.append(mapping.negative_strategy_status.value)
        return ExecutionEligibility(
            futures_symbol=mapping.futures_symbol,
            spot_symbol=mapping.spot_symbol,
            spot_mapping_status=mapping.spot_mapping_status,
            positive_strategy_available=mapping.positive_strategy_available,
            negative_strategy_available=mapping.negative_strategy_available,
            negative_strategy_status=mapping.negative_strategy_status,
            rejection_reasons=tuple(rejection_reasons),
        )

    def _mapping(
        self,
        futures: FuturesInstrument,
        *,
        status: SpotMappingStatus,
        reason: MappingReason,
        updated_at: datetime,
        spot: SpotInstrument | None = None,
        spot_pair_exists: bool = False,
        positive_strategy_available: bool = False,
        negative_strategy_status: NegativeStrategyStatus = (
            NegativeStrategyStatus.NOT_APPLICABLE
        ),
    ) -> InstrumentMapping:
        return InstrumentMapping(
            futures_symbol=futures.symbol,
            futures_pair=futures.pair,
            futures_base_asset=futures.base_asset,
            futures_quote_asset=futures.quote_asset,
            futures_margin_asset=futures.margin_asset,
            futures_contract_type=futures.contract_type,
            futures_status=futures.status,
            spot_symbol=spot.symbol if spot is not None else None,
            spot_base_asset=spot.base_asset if spot is not None else None,
            spot_quote_asset=spot.quote_asset if spot is not None else None,
            spot_status=spot.status if spot is not None else None,
            spot_trading_allowed=spot.trading_allowed if spot is not None else False,
            spot_pair_exists=spot_pair_exists,
            spot_mapping_status=status,
            mapping_reason=reason,
            positive_strategy_available=positive_strategy_available,
            negative_strategy_available=False,
            negative_strategy_status=negative_strategy_status,
            mapping_source=MAPPING_SOURCE,
            mapping_updated_at=updated_at,
            created_at=updated_at,
            updated_at=updated_at,
        )


def parse_futures_exchange_info(exchange_info: dict[str, Any]) -> list[FuturesInstrument]:
    _raise_if_binance_api_error(exchange_info, source="futures")
    symbols = exchange_info.get("symbols")
    if not isinstance(symbols, list):
        raise TypeError("futures exchangeInfo response does not contain symbols")

    instruments: list[FuturesInstrument] = []
    for item in symbols:
        if not isinstance(item, dict):
            logger.warning("ignoring invalid futures symbol entry")
            continue
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            logger.warning("ignoring futures symbol entry without symbol")
            continue
        instruments.append(
            FuturesInstrument(
                symbol=symbol,
                pair=_optional_text(item, "pair", symbol),
                contract_type=_optional_text(item, "contractType", ""),
                status=_optional_text(item, "status", ""),
                base_asset=_optional_text(item, "baseAsset", ""),
                quote_asset=_optional_text(item, "quoteAsset", ""),
                margin_asset=_optional_text(item, "marginAsset", ""),
            )
        )
    return instruments


def parse_spot_exchange_info(
    exchange_info: dict[str, Any], *, supported_quote_asset: str
) -> list[SpotInstrument]:
    _raise_if_binance_api_error(exchange_info, source="spot")
    symbols = exchange_info.get("symbols")
    if not isinstance(symbols, list):
        raise TypeError("spot exchangeInfo response does not contain symbols")

    instruments: list[SpotInstrument] = []
    seen_symbols: set[str] = set()
    for item in symbols:
        try:
            instrument = parse_spot_symbol(
                item,
                supported_quote_asset=supported_quote_asset,
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("ignoring invalid spot symbol entry: %s", exc)
            continue
        if instrument is None:
            continue
        if instrument.symbol in seen_symbols:
            logger.warning("duplicate spot symbol in exchangeInfo: %s", instrument.symbol)
        seen_symbols.add(instrument.symbol)
        instruments.append(instrument)
    return instruments


def parse_spot_symbol(
    item: Any, *, supported_quote_asset: str
) -> SpotInstrument | None:
    if not isinstance(item, dict):
        raise TypeError("spot symbol entry is not an object")

    quote_asset = _required_text(item, "quoteAsset")
    if quote_asset != supported_quote_asset:
        return None

    status = _required_text(item, "status")
    permission_allowed = _spot_permission_allowed(item)
    return SpotInstrument(
        symbol=_required_text(item, "symbol"),
        status=status,
        base_asset=_required_text(item, "baseAsset"),
        quote_asset=quote_asset,
        trading_allowed=status == "TRADING" and permission_allowed,
    )


def _spot_permission_allowed(item: dict[str, Any]) -> bool:
    explicit_allowed = item.get("isSpotTradingAllowed")
    if explicit_allowed is not None:
        if not isinstance(explicit_allowed, bool):
            raise TypeError("isSpotTradingAllowed is not a boolean")
        return explicit_allowed

    permissions = item.get("permissions")
    if permissions is not None:
        if not isinstance(permissions, list) or not all(
            isinstance(value, str) for value in permissions
        ):
            raise TypeError("permissions has unsupported format")
        return "SPOT" in permissions

    permission_sets = item.get("permissionSets")
    if permission_sets is not None:
        flattened = _flatten_permission_sets(permission_sets)
        return "SPOT" in flattened

    return True


def _flatten_permission_sets(permission_sets: Any) -> set[str]:
    if not isinstance(permission_sets, list):
        raise TypeError("permissionSets has unsupported format")
    values: set[str] = set()
    for entry in permission_sets:
        if isinstance(entry, str):
            values.add(entry)
            continue
        if isinstance(entry, list) and all(isinstance(value, str) for value in entry):
            values.update(entry)
            continue
        raise TypeError("permissionSets has unsupported format")
    return values


def _sync_result_from_mappings(
    mappings: Iterable[InstrumentMapping],
    *,
    synchronization_duration_seconds: float,
    updated_at: datetime,
) -> InstrumentMappingSyncResult:
    rows = list(mappings)
    statuses = Counter(mapping.spot_mapping_status for mapping in rows)
    return InstrumentMappingSyncResult(
        futures_symbols_processed=len(rows),
        matched=statuses[SpotMappingStatus.MATCHED],
        missing=statuses[SpotMappingStatus.MISSING],
        ambiguous=statuses[SpotMappingStatus.AMBIGUOUS],
        unsupported=statuses[SpotMappingStatus.UNSUPPORTED],
        spot_trading_disabled=statuses[SpotMappingStatus.SPOT_TRADING_DISABLED],
        positive_strategy_available=sum(
            1 for mapping in rows if mapping.positive_strategy_available
        ),
        negative_strategy_available=sum(
            1 for mapping in rows if mapping.negative_strategy_available
        ),
        negative_strategy_pending_borrow_implementation=sum(
            1
            for mapping in rows
            if mapping.negative_strategy_status
            == NegativeStrategyStatus.BORROW_CHECK_NOT_IMPLEMENTED
        ),
        synchronization_duration_seconds=synchronization_duration_seconds,
        updated_at=ensure_utc(updated_at),
    )


def _is_multiplier_contract(futures: FuturesInstrument) -> bool:
    return bool(
        futures.base_asset
        and futures.base_asset[0].isdigit()
        and MULTIPLIER_SYMBOL_PATTERN.match(futures.symbol)
    )


def _required_text(item: dict[str, Any], key: str) -> str:
    value = item[key]
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def _optional_text(item: dict[str, Any], key: str, default: str) -> str:
    value = item.get(key, default)
    return value if isinstance(value, str) else default


def _raise_if_binance_api_error(exchange_info: dict[str, Any], *, source: str) -> None:
    if "code" not in exchange_info:
        return
    code = exchange_info.get("code")
    message = exchange_info.get("msg", "")
    raise ValueError(f"{source} exchangeInfo API error code {code}: {message}")
