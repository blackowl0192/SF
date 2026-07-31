from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from decimal import Decimal

from funding_monitor.instrument_mapping import SpotMappingStatus
from funding_monitor.models import FundingSnapshot, ensure_utc

from .data_quality import DataQualityEvaluator
from .economics import EconomicCalculator
from .market_data import (
    HistoricalMarketDataProvider,
    UnavailableHistoricalMarketDataProvider,
)
from .models import (
    DataQualityStatus,
    MarketPriceSet,
    OutcomeStatus,
    RejectionReason,
    SignalDetection,
    StrategyValidationConfig,
    StrategyValidationEvent,
    StrategyValidationResult,
    ValidationMode,
)
from .signal_detector import SignalDetector

ZERO = Decimal(0)


class StrategyReplayEngine:
    def __init__(
        self,
        config: StrategyValidationConfig,
        *,
        signal_detector: SignalDetector | None = None,
        data_quality_evaluator: DataQualityEvaluator | None = None,
        economic_calculator: EconomicCalculator | None = None,
        market_data_provider: HistoricalMarketDataProvider | None = None,
    ) -> None:
        self.config = config
        self.signal_detector = signal_detector or SignalDetector(config)
        self.data_quality_evaluator = data_quality_evaluator or DataQualityEvaluator(
            config
        )
        self.economic_calculator = economic_calculator or EconomicCalculator()
        self.market_data_provider = (
            market_data_provider or UnavailableHistoricalMarketDataProvider()
        )

    async def replay_event(
        self,
        event: StrategyValidationEvent,
        *,
        run_id: int | None,
    ) -> StrategyValidationResult:
        funding_time = ensure_utc(event.funding_event.funding_time)
        snapshots = tuple(
            snapshot
            for snapshot in sorted(event.snapshots, key=lambda item: item.event_time)
            if ensure_utc(snapshot.event_time) <= funding_time
        )
        data_quality = self.data_quality_evaluator.evaluate(snapshots, event.mapping)
        mapping_rejection = _mapping_rejection(
            event,
            require_positive_strategy=self.config.require_positive_strategy,
            require_matched_spot=self.config.require_matched_spot,
        )
        realized_rate = event.funding_event.actual_funding_rate
        signal = (
            self.signal_detector.detect(snapshots, funding_time)
            if mapping_rejection is None
            and realized_rate is not None
            and data_quality.status != DataQualityStatus.INVALID
            else _missing_signal()
        )

        rejection_reason = _primary_rejection_reason(
            mapping_rejection,
            realized_rate,
            data_quality.status,
            signal,
        )
        eligible = rejection_reason is None
        market_prices = await self._market_prices(event, signal)
        economics = self.economic_calculator.evaluate(
            config=self.config,
            realized_funding_rate=realized_rate if eligible else None,
            market_prices=market_prices,
        )
        outcome_status = _outcome_status(
            eligible=eligible,
            validation_mode=self.config.validation_mode,
            market_data_complete=economics.market_data_complete,
            data_quality_status=data_quality.status,
        )
        peak_snapshot = _peak_snapshot(snapshots)
        latest_snapshot = snapshots[-1] if snapshots else None
        prediction_error = (
            latest_snapshot.predicted_funding_rate - realized_rate
            if latest_snapshot is not None and realized_rate is not None
            else None
        )
        prediction_drop_from_entry = (
            signal.predicted_funding_at_entry - realized_rate
            if signal.predicted_funding_at_entry is not None
            and realized_rate is not None
            else None
        )
        prediction_drop_from_peak = (
            peak_snapshot.predicted_funding_rate - realized_rate
            if peak_snapshot is not None and realized_rate is not None
            else None
        )
        success = bool(eligible and realized_rate is not None and realized_rate > ZERO)
        profitable = (
            economics.net_pnl > ZERO if economics.net_pnl is not None else None
        )

        return StrategyValidationResult(
            run_id=run_id,
            exchange=event.exchange,
            symbol=event.symbol,
            spot_symbol=event.spot_symbol,
            funding_time=funding_time,
            strategy_version=self.config.strategy_version,
            config_hash=self.config.config_hash(),
            signal_detected=signal.signal_detected,
            signal_started_at=signal.signal_started_at,
            signal_confirmed_at=signal.signal_confirmed_at,
            entry_time=signal.entry_time,
            entry_minutes_before_funding=signal.entry_minutes_before_funding,
            predicted_funding_at_entry=signal.predicted_funding_at_entry,
            peak_predicted_funding=peak_snapshot.predicted_funding_rate
            if peak_snapshot is not None
            else None,
            peak_predicted_at=ensure_utc(peak_snapshot.event_time)
            if peak_snapshot is not None
            else None,
            last_predicted_funding=latest_snapshot.predicted_funding_rate
            if latest_snapshot is not None
            else None,
            realized_funding_rate=realized_rate,
            prediction_error=prediction_error,
            prediction_drop_from_entry=prediction_drop_from_entry,
            prediction_drop_from_peak=prediction_drop_from_peak,
            persistence_at_entry=signal.persistence_at_entry,
            funding_std_at_entry=signal.funding_std_at_entry,
            funding_velocity_at_entry=signal.funding_velocity_at_entry,
            threshold_crossings_before_entry=signal.threshold_crossings_before_entry,
            late_spike=signal.late_spike,
            deteriorating_signal=signal.deteriorating_signal,
            spot_pair_exists=bool(
                event.mapping.spot_pair_exists if event.mapping is not None else False
            ),
            positive_strategy_available=bool(
                event.mapping.positive_strategy_available
                if event.mapping is not None
                else False
            ),
            enough_history=_enough_history(signal, rejection_reason),
            fresh_data=_fresh_data(signal, rejection_reason),
            eligible=eligible,
            rejection_reason=rejection_reason,
            validation_mode=self.config.validation_mode,
            market_data_complete=economics.market_data_complete,
            missing_data_reasons=economics.missing_data_reasons,
            position_notional=economics.position_notional,
            gross_funding_pnl=economics.gross_funding_pnl,
            spot_price_pnl=economics.spot_price_pnl,
            futures_price_pnl=economics.futures_price_pnl,
            basis_pnl=economics.basis_pnl,
            spot_fees=economics.spot_fees,
            futures_fees=economics.futures_fees,
            slippage_cost=economics.slippage_cost,
            additional_cost=economics.additional_cost,
            net_pnl=economics.net_pnl,
            gross_return_rate=economics.gross_return_rate,
            net_return_rate=economics.net_return_rate,
            outcome_status=outcome_status,
            success=success,
            profitable=profitable,
            data_quality_status=data_quality.status,
            metadata={
                "strategy_name": self.config.strategy_name,
                "funding_threshold": str(self.config.funding_threshold),
                "entry_mode": self.config.entry_mode.value,
                "entry_minutes_before_funding_config": (
                    self.config.entry_minutes_before_funding
                ),
                "signal_confirmation_minutes": (
                    self.config.signal_confirmation_minutes
                ),
                "minimum_persistence_ratio": str(
                    self.config.minimum_persistence_ratio
                ),
                "maximum_funding_std": str(self.config.maximum_funding_std),
                "exit_minutes_after_funding": self.config.exit_minutes_after_funding,
                "funding_interval_hours": event.funding_event.funding_interval_hours,
                "snapshot_count": len(snapshots),
                "data_quality_reasons": list(data_quality.reasons),
                "data_quality_maximum_gap_seconds": data_quality.maximum_gap_seconds,
                "data_quality_duplicate_count": data_quality.duplicate_count,
                "signal_rejection_reason": signal.rejection_reason.value
                if signal.rejection_reason is not None
                else None,
                "candidate_status": event.candidate_status,
                "candidate_score": str(event.candidate_score)
                if event.candidate_score is not None
                else None,
                "interval_summary_status": event.interval_summary_status,
            },
        )

    async def _market_prices(
        self,
        event: StrategyValidationEvent,
        signal: SignalDetection,
    ) -> MarketPriceSet:
        if signal.entry_time is None or event.spot_symbol is None:
            return MarketPriceSet(entry=None, exit=None)
        exit_time = ensure_utc(event.funding_event.funding_time) + timedelta(
            minutes=self.config.exit_minutes_after_funding
        )
        entry_price = await self.market_data_provider.get_spot_futures_prices_at(
            exchange=event.exchange,
            futures_symbol=event.symbol,
            spot_symbol=event.spot_symbol,
            timestamp=signal.entry_time,
        )
        exit_price = await self.market_data_provider.get_spot_futures_prices_at(
            exchange=event.exchange,
            futures_symbol=event.symbol,
            spot_symbol=event.spot_symbol,
            timestamp=exit_time,
        )
        return MarketPriceSet(entry=entry_price, exit=exit_price)


def _mapping_rejection(
    event: StrategyValidationEvent,
    *,
    require_positive_strategy: bool,
    require_matched_spot: bool,
) -> RejectionReason | None:
    mapping = event.mapping
    if mapping is None:
        return RejectionReason.MISSING_SPOT_MAPPING
    if mapping.futures_status != "TRADING":
        return RejectionReason.INACTIVE_INSTRUMENT
    if require_matched_spot and (
        not mapping.spot_pair_exists
        or mapping.spot_mapping_status != SpotMappingStatus.MATCHED
    ):
        return RejectionReason.MISSING_SPOT_MAPPING
    if not mapping.spot_trading_allowed:
        return RejectionReason.SPOT_TRADING_DISABLED
    if require_positive_strategy and not mapping.positive_strategy_available:
        return RejectionReason.POSITIVE_STRATEGY_UNAVAILABLE
    return None


def _primary_rejection_reason(
    mapping_rejection: RejectionReason | None,
    realized_rate: Decimal | None,
    data_quality_status: DataQualityStatus,
    signal: SignalDetection,
) -> RejectionReason | None:
    if mapping_rejection is not None:
        return mapping_rejection
    if realized_rate is None:
        return RejectionReason.MISSING_REALIZED_FUNDING
    if data_quality_status == DataQualityStatus.INVALID:
        return RejectionReason.DATA_QUALITY_INVALID
    if not signal.signal_detected:
        return signal.rejection_reason or RejectionReason.NO_QUALIFYING_SIGNAL
    return None


def _outcome_status(
    *,
    eligible: bool,
    validation_mode: ValidationMode,
    market_data_complete: bool,
    data_quality_status: DataQualityStatus,
) -> OutcomeStatus:
    if data_quality_status == DataQualityStatus.INVALID:
        return OutcomeStatus.INVALID_DATA
    if not eligible:
        return OutcomeStatus.REJECTED
    if validation_mode == ValidationMode.FULL_ECONOMIC:
        if not market_data_complete:
            return OutcomeStatus.INSUFFICIENT_MARKET_DATA
        return OutcomeStatus.FULL_ECONOMIC
    return OutcomeStatus.FUNDING_ONLY


def _enough_history(
    signal: SignalDetection,
    rejection_reason: RejectionReason | None,
) -> bool:
    return rejection_reason != RejectionReason.INSUFFICIENT_HISTORY and (
        signal.signal_detected or signal.rejection_reason != RejectionReason.INSUFFICIENT_HISTORY
    )


def _fresh_data(
    signal: SignalDetection,
    rejection_reason: RejectionReason | None,
) -> bool:
    return rejection_reason != RejectionReason.STALE_SNAPSHOTS and (
        signal.signal_detected or signal.rejection_reason != RejectionReason.STALE_SNAPSHOTS
    )


def _missing_signal() -> SignalDetection:
    return SignalDetection(
        signal_detected=False,
        signal_started_at=None,
        signal_confirmed_at=None,
        entry_time=None,
        entry_minutes_before_funding=None,
        predicted_funding_at_entry=None,
        persistence_at_entry=None,
        funding_std_at_entry=None,
        funding_velocity_at_entry=None,
        threshold_crossings_before_entry=0,
        late_spike=False,
        deteriorating_signal=False,
        continuous_signal=False,
        rejection_reason=None,
    )


def _peak_snapshot(
    snapshots: Sequence[FundingSnapshot],
) -> FundingSnapshot | None:
    if not snapshots:
        return None
    return max(snapshots, key=lambda item: item.predicted_funding_rate)
