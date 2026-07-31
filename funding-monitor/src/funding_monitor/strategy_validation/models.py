from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from funding_monitor.candidate_engine import DEFAULT_EXCHANGE
from funding_monitor.instrument_mapping import InstrumentMapping
from funding_monitor.models import FundingEvent, FundingSnapshot, ensure_utc

STRATEGY_NAME = "positive_funding_spot_long_perp_short"
STRATEGY_VALIDATION_VERSION = "1.0"


class EntryMode(StrEnum):
    FIXED_TIME = "fixed_time"
    FIRST_QUALIFYING_SIGNAL = "first_qualifying_signal"


class ExitRule(StrEnum):
    FIXED_AFTER_FUNDING = "fixed_after_funding"
    FUNDING_RECEIVED = "funding_received"
    BASIS_NORMALIZED = "basis_normalized"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    SIGNAL_INVALIDATED = "signal_invalidated"


class ValidationMode(StrEnum):
    FUNDING_ONLY = "funding_only"
    FULL_ECONOMIC = "full_economic"


class ValidationRunStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class OutcomeStatus(StrEnum):
    FUNDING_ONLY = "funding_only"
    FULL_ECONOMIC = "full_economic"
    INSUFFICIENT_MARKET_DATA = "insufficient_market_data"
    REJECTED = "rejected"
    INVALID_DATA = "invalid_data"


class DataQualityStatus(StrEnum):
    GOOD = "good"
    PARTIAL = "partial"
    POOR = "poor"
    INVALID = "invalid"


class RejectionReason(StrEnum):
    MISSING_SPOT_MAPPING = "missing_spot_mapping"
    INACTIVE_INSTRUMENT = "inactive_instrument"
    SPOT_TRADING_DISABLED = "spot_trading_disabled"
    POSITIVE_STRATEGY_UNAVAILABLE = "positive_strategy_unavailable"
    INSUFFICIENT_HISTORY = "insufficient_history"
    STALE_SNAPSHOTS = "stale_snapshots"
    NO_QUALIFYING_SIGNAL = "no_qualifying_signal"
    MISSING_REALIZED_FUNDING = "missing_realized_funding"
    MISSING_PRICE_DATA = "missing_price_data"
    INVALID_DATA = "invalid_data"
    FUNDING_BELOW_THRESHOLD = "funding_below_threshold"
    PERSISTENCE_TOO_LOW = "persistence_too_low"
    VOLATILITY_TOO_HIGH = "volatility_too_high"
    PREDICTION_DROP_TOO_HIGH = "prediction_drop_too_high"
    DATA_QUALITY_INVALID = "data_quality_invalid"


class MissingMarketDataReason(StrEnum):
    SPOT_ENTRY_PRICE_MISSING = "spot_entry_price_missing"
    FUTURES_ENTRY_PRICE_MISSING = "futures_entry_price_missing"
    SPOT_EXIT_PRICE_MISSING = "spot_exit_price_missing"
    FUTURES_EXIT_PRICE_MISSING = "futures_exit_price_missing"
    PROVIDER_UNAVAILABLE = "provider_unavailable"


class SizingMode(StrEnum):
    IDEALIZED_SIZING = "idealized_sizing"
    EXCHANGE_CONSTRAINED_SIZING = "exchange_constrained_sizing"


@dataclass(frozen=True)
class StrategyValidationConfig:
    exchange: str = DEFAULT_EXCHANGE
    funding_threshold: Decimal = Decimal("0.0003")
    entry_mode: EntryMode = EntryMode.FIXED_TIME
    entry_minutes_before_funding: int = 60
    signal_confirmation_minutes: int = 5
    minimum_persistence_ratio: Decimal = Decimal("0.70")
    maximum_funding_std: Decimal = Decimal("0.0002")
    maximum_prediction_drop: Decimal = Decimal("0.0002")
    minimum_history_minutes: int = 15
    maximum_snapshot_age_seconds: int = 120
    exit_minutes_after_funding: int = 0
    spot_entry_fee_rate: Decimal = Decimal(0)
    spot_exit_fee_rate: Decimal = Decimal(0)
    futures_entry_fee_rate: Decimal = Decimal(0)
    futures_exit_fee_rate: Decimal = Decimal(0)
    spot_slippage_rate: Decimal = Decimal(0)
    futures_slippage_rate: Decimal = Decimal(0)
    additional_cost_rate: Decimal = Decimal(0)
    position_notional: Decimal = Decimal(1000)
    require_positive_strategy: bool = True
    require_matched_spot: bool = True
    validation_mode: ValidationMode = ValidationMode.FUNDING_ONLY
    exit_rule: ExitRule = ExitRule.FIXED_AFTER_FUNDING
    strategy_name: str = STRATEGY_NAME
    strategy_version: str = STRATEGY_VALIDATION_VERSION

    def __post_init__(self) -> None:
        if not self.exchange:
            raise ValueError("exchange is required")
        if self.funding_threshold <= 0:
            raise ValueError("funding_threshold must be positive")
        if self.entry_minutes_before_funding < 0:
            raise ValueError("entry_minutes_before_funding must be non-negative")
        if self.signal_confirmation_minutes < 0:
            raise ValueError("signal_confirmation_minutes must be non-negative")
        if not Decimal(0) <= self.minimum_persistence_ratio <= Decimal(1):
            raise ValueError("minimum_persistence_ratio must be in 0..1")
        if self.maximum_funding_std < 0:
            raise ValueError("maximum_funding_std must be non-negative")
        if self.maximum_prediction_drop < 0:
            raise ValueError("maximum_prediction_drop must be non-negative")
        if self.minimum_history_minutes < 0:
            raise ValueError("minimum_history_minutes must be non-negative")
        if self.maximum_snapshot_age_seconds < 0:
            raise ValueError("maximum_snapshot_age_seconds must be non-negative")
        if self.exit_minutes_after_funding < 0:
            raise ValueError("exit_minutes_after_funding must be non-negative")
        if self.position_notional <= 0:
            raise ValueError("position_notional must be positive")
        for name in (
            "spot_entry_fee_rate",
            "spot_exit_fee_rate",
            "futures_entry_fee_rate",
            "futures_exit_fee_rate",
            "spot_slippage_rate",
            "futures_slippage_rate",
            "additional_cost_rate",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")

    def to_dict(self) -> dict[str, object]:
        return {
            "exchange": self.exchange,
            "funding_threshold": _decimal_text(self.funding_threshold),
            "entry_mode": self.entry_mode.value,
            "entry_minutes_before_funding": self.entry_minutes_before_funding,
            "signal_confirmation_minutes": self.signal_confirmation_minutes,
            "minimum_persistence_ratio": _decimal_text(self.minimum_persistence_ratio),
            "maximum_funding_std": _decimal_text(self.maximum_funding_std),
            "maximum_prediction_drop": _decimal_text(self.maximum_prediction_drop),
            "minimum_history_minutes": self.minimum_history_minutes,
            "maximum_snapshot_age_seconds": self.maximum_snapshot_age_seconds,
            "exit_minutes_after_funding": self.exit_minutes_after_funding,
            "spot_entry_fee_rate": _decimal_text(self.spot_entry_fee_rate),
            "spot_exit_fee_rate": _decimal_text(self.spot_exit_fee_rate),
            "futures_entry_fee_rate": _decimal_text(self.futures_entry_fee_rate),
            "futures_exit_fee_rate": _decimal_text(self.futures_exit_fee_rate),
            "spot_slippage_rate": _decimal_text(self.spot_slippage_rate),
            "futures_slippage_rate": _decimal_text(self.futures_slippage_rate),
            "additional_cost_rate": _decimal_text(self.additional_cost_rate),
            "position_notional": _decimal_text(self.position_notional),
            "require_positive_strategy": self.require_positive_strategy,
            "require_matched_spot": self.require_matched_spot,
            "validation_mode": self.validation_mode.value,
            "exit_rule": self.exit_rule.value,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
        }

    def config_hash(self) -> str:
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class StrategyValidationDataset:
    period_start: datetime | None
    period_end: datetime | None
    requested_symbols: tuple[str, ...]
    limit: int | None

    def to_dict(self) -> dict[str, object]:
        return {
            "period_start": ensure_utc(self.period_start).isoformat()
            if self.period_start is not None
            else None,
            "period_end": ensure_utc(self.period_end).isoformat()
            if self.period_end is not None
            else None,
            "requested_symbols": list(self.requested_symbols),
            "limit": self.limit,
        }


@dataclass(frozen=True)
class SignalDetection:
    signal_detected: bool
    signal_started_at: datetime | None
    signal_confirmed_at: datetime | None
    entry_time: datetime | None
    entry_minutes_before_funding: Decimal | None
    predicted_funding_at_entry: Decimal | None
    persistence_at_entry: Decimal | None
    funding_std_at_entry: Decimal | None
    funding_velocity_at_entry: Decimal | None
    threshold_crossings_before_entry: int
    late_spike: bool
    deteriorating_signal: bool
    continuous_signal: bool
    rejection_reason: RejectionReason | None


@dataclass(frozen=True)
class DataQualityResult:
    status: DataQualityStatus
    reasons: tuple[str, ...]
    maximum_gap_seconds: int | None
    duplicate_count: int


@dataclass(frozen=True)
class MarketPrice:
    spot_price: Decimal | None
    futures_price: Decimal | None
    timestamp: datetime
    missing_reasons: tuple[MissingMarketDataReason, ...] = ()


@dataclass(frozen=True)
class MarketPriceSet:
    entry: MarketPrice | None
    exit: MarketPrice | None

    @property
    def complete(self) -> bool:
        return (
            self.entry is not None
            and self.exit is not None
            and self.entry.spot_price is not None
            and self.entry.futures_price is not None
            and self.exit.spot_price is not None
            and self.exit.futures_price is not None
        )

    def missing_reasons(self) -> tuple[MissingMarketDataReason, ...]:
        reasons: list[MissingMarketDataReason] = []
        if self.entry is None:
            reasons.extend(
                [
                    MissingMarketDataReason.SPOT_ENTRY_PRICE_MISSING,
                    MissingMarketDataReason.FUTURES_ENTRY_PRICE_MISSING,
                ]
            )
        else:
            reasons.extend(self.entry.missing_reasons)
            if self.entry.spot_price is None:
                reasons.append(MissingMarketDataReason.SPOT_ENTRY_PRICE_MISSING)
            if self.entry.futures_price is None:
                reasons.append(MissingMarketDataReason.FUTURES_ENTRY_PRICE_MISSING)
        if self.exit is None:
            reasons.extend(
                [
                    MissingMarketDataReason.SPOT_EXIT_PRICE_MISSING,
                    MissingMarketDataReason.FUTURES_EXIT_PRICE_MISSING,
                ]
            )
        else:
            reasons.extend(self.exit.missing_reasons)
            if self.exit.spot_price is None:
                reasons.append(MissingMarketDataReason.SPOT_EXIT_PRICE_MISSING)
            if self.exit.futures_price is None:
                reasons.append(MissingMarketDataReason.FUTURES_EXIT_PRICE_MISSING)
        return tuple(dict.fromkeys(reasons))


@dataclass(frozen=True)
class HedgeSizingResult:
    sizing_mode: SizingMode
    requested_notional: Decimal
    spot_quantity: Decimal | None
    futures_quantity: Decimal | None
    spot_entry_notional: Decimal | None
    futures_entry_notional: Decimal | None
    initial_hedge_mismatch_rate: Decimal | None


@dataclass(frozen=True)
class EconomicResult:
    validation_mode: ValidationMode
    market_data_complete: bool
    missing_data_reasons: tuple[MissingMarketDataReason, ...]
    position_notional: Decimal
    gross_funding_pnl: Decimal | None
    spot_price_pnl: Decimal | None
    futures_price_pnl: Decimal | None
    basis_pnl: Decimal | None
    spot_fees: Decimal | None
    futures_fees: Decimal | None
    slippage_cost: Decimal | None
    additional_cost: Decimal | None
    net_pnl: Decimal | None
    gross_return_rate: Decimal | None
    net_return_rate: Decimal | None
    hedge_sizing: HedgeSizingResult | None


@dataclass(frozen=True)
class StrategyValidationEvent:
    exchange: str
    symbol: str
    funding_event: FundingEvent
    snapshots: tuple[FundingSnapshot, ...]
    mapping: InstrumentMapping | None
    candidate_status: str | None = None
    candidate_score: Decimal | None = None
    interval_summary_status: str | None = None

    @property
    def spot_symbol(self) -> str | None:
        return self.mapping.spot_symbol if self.mapping is not None else None


@dataclass(frozen=True)
class StrategyValidationResult:
    run_id: int | None
    exchange: str
    symbol: str
    spot_symbol: str | None
    funding_time: datetime
    strategy_version: str
    config_hash: str
    signal_detected: bool
    signal_started_at: datetime | None
    signal_confirmed_at: datetime | None
    entry_time: datetime | None
    entry_minutes_before_funding: Decimal | None
    predicted_funding_at_entry: Decimal | None
    peak_predicted_funding: Decimal | None
    peak_predicted_at: datetime | None
    last_predicted_funding: Decimal | None
    realized_funding_rate: Decimal | None
    prediction_error: Decimal | None
    prediction_drop_from_entry: Decimal | None
    prediction_drop_from_peak: Decimal | None
    persistence_at_entry: Decimal | None
    funding_std_at_entry: Decimal | None
    funding_velocity_at_entry: Decimal | None
    threshold_crossings_before_entry: int | None
    late_spike: bool
    deteriorating_signal: bool
    spot_pair_exists: bool
    positive_strategy_available: bool
    enough_history: bool
    fresh_data: bool
    eligible: bool
    rejection_reason: RejectionReason | None
    validation_mode: ValidationMode
    market_data_complete: bool
    missing_data_reasons: tuple[MissingMarketDataReason, ...]
    position_notional: Decimal
    gross_funding_pnl: Decimal | None
    spot_price_pnl: Decimal | None
    futures_price_pnl: Decimal | None
    basis_pnl: Decimal | None
    spot_fees: Decimal | None
    futures_fees: Decimal | None
    slippage_cost: Decimal | None
    additional_cost: Decimal | None
    net_pnl: Decimal | None
    gross_return_rate: Decimal | None
    net_return_rate: Decimal | None
    outcome_status: OutcomeStatus
    success: bool
    profitable: bool | None
    data_quality_status: DataQualityStatus
    metadata: dict[str, Any]


@dataclass(frozen=True)
class StrategyValidationRun:
    id: int
    strategy_name: str
    strategy_version: str
    exchange: str
    status: ValidationRunStatus
    validation_mode: ValidationMode
    configuration: dict[str, object]
    configuration_hash: str
    period_start: datetime | None
    period_end: datetime | None
    requested_symbols: tuple[str, ...]
    started_at: datetime
    completed_at: datetime | None
    total_events: int
    processed_events: int
    successful_events: int
    failed_events: int
    error_message: str | None


@dataclass(frozen=True)
class StrategyValidationAggregate:
    run_id: int
    grouping_type: str
    grouping_key: str
    metrics: dict[str, object]


@dataclass(frozen=True)
class StrategyValidationSummary:
    run_id: int | None
    total_events: int
    processed_events: int
    successful_events: int
    failed_events: int
    aggregates: tuple[StrategyValidationAggregate, ...]


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")
