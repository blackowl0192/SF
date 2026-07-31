from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from itertools import pairwise
from typing import Protocol

from .history_service import (
    FundingHistoryService,
    FundingMetrics,
    calculate_funding_metrics,
)
from .instrument_mapping import InstrumentMapping, SpotMappingStatus
from .models import FundingEvent, FundingSnapshot, ensure_utc, utc_now

logger = logging.getLogger(__name__)

CANDIDATE_ENGINE_VERSION = "1.0"
DEFAULT_EXCHANGE = "BINANCE"
ZERO = Decimal(0)
ONE = Decimal(1)
SCORE_QUANT = Decimal("0.0001")
RATIO_QUANT = Decimal("0.000001")


class CandidateStatus(StrEnum):
    OBSERVING = "observing"
    CANDIDATE = "candidate"
    STRONG_CANDIDATE = "strong_candidate"
    WEAK_CANDIDATE = "weak_candidate"
    UNSTABLE = "unstable"
    LATE_SPIKE = "late_spike"
    DETERIORATING = "deteriorating"
    FUNDING_FALLING = "funding_falling"
    TOO_EARLY = "too_early"
    TOO_LATE = "too_late"
    STALE = "stale"
    INSUFFICIENT_HISTORY = "insufficient_history"
    REJECTED = "rejected"
    EXPIRED = "expired"


class RejectionReason(StrEnum):
    FUNDING_NOT_POSITIVE = "funding_not_positive"
    FUNDING_BELOW_THRESHOLD = "funding_below_threshold"
    FUTURES_INACTIVE = "futures_inactive"
    SPOT_MAPPING_MISSING = "spot_mapping_missing"
    SPOT_MAPPING_AMBIGUOUS = "spot_mapping_ambiguous"
    SPOT_TRADING_DISABLED = "spot_trading_disabled"
    POSITIVE_STRATEGY_UNAVAILABLE = "positive_strategy_unavailable"
    NEXT_FUNDING_TIME_MISSING = "next_funding_time_missing"
    FUNDING_TIME_EXPIRED = "funding_time_expired"
    STALE_SNAPSHOT = "stale_snapshot"
    INSUFFICIENT_HISTORY = "insufficient_history"
    INSUFFICIENT_SNAPSHOT_COUNT = "insufficient_snapshot_count"
    PERSISTENCE_TOO_LOW = "persistence_too_low"
    VOLATILITY_TOO_HIGH = "volatility_too_high"
    TOO_MANY_THRESHOLD_CROSSINGS = "too_many_threshold_crossings"
    TOO_MANY_DIRECTION_CHANGES = "too_many_direction_changes"
    NEGATIVE_VELOCITY = "negative_velocity"
    NEGATIVE_ACCELERATION = "negative_acceleration"
    LATE_SPIKE_DETECTED = "late_spike_detected"
    FUNDING_DETERIORATING = "funding_deteriorating"
    TOO_EARLY = "too_early"
    TOO_LATE = "too_late"
    INVALID_DATA = "invalid_data"
    CALCULATION_ERROR = "calculation_error"


class PenaltyCode(StrEnum):
    LATE_SPIKE = "late_spike_penalty"
    DETERIORATION = "deterioration_penalty"
    STALE_DATA = "stale_data_penalty"
    INSUFFICIENT_HISTORY = "insufficient_history_penalty"
    INSTABILITY = "instability_penalty"
    THRESHOLD_CROSSING = "threshold_crossing_penalty"
    TOO_CLOSE_TO_FUNDING = "too_close_to_funding_penalty"


class FundingIntervalSummaryStatus(StrEnum):
    PENDING_CONFIRMATION = "pending_confirmation"
    COMPLETE = "complete"
    PARTIAL_HISTORY = "partial_history"
    INSUFFICIENT_HISTORY = "insufficient_history"
    CONFIRMATION_FAILED = "confirmation_failed"
    INVALID = "invalid"


@dataclass(frozen=True)
class CandidateEngineConfig:
    enabled: bool = True
    min_funding_rate: Decimal = Decimal("0.0003")
    min_history_minutes: int = 15
    primary_window_minutes: int = 30
    short_window_minutes: int = 5
    long_window_minutes: int = 60
    min_snapshot_count: int = 10
    max_snapshot_age_seconds: int = 120
    min_persistence_ratio: Decimal = Decimal("0.70")
    max_std_dev: Decimal = Decimal("0.0002")
    max_threshold_crossings: int = 4
    max_direction_changes: int = 8
    late_spike_lookback_minutes: int = 5
    late_spike_min_jump_ratio: Decimal = Decimal("1.50")
    deterioration_lookback_minutes: int = 5
    max_negative_velocity: Decimal = Decimal("-0.00002")
    min_minutes_to_funding: int = 5
    max_minutes_to_funding: int = 480
    strong_score: Decimal = Decimal(80)
    min_score: Decimal = Decimal(60)
    persist_interval_seconds: int = 60
    max_results: int = 50
    interval_point_tolerance_seconds: int = 90
    interval_summary_batch_size: int = 500
    engine_version: str = CANDIDATE_ENGINE_VERSION


@dataclass(frozen=True)
class FundingMetricsCollection:
    primary: FundingMetrics
    short: FundingMetrics
    long: FundingMetrics


@dataclass(frozen=True)
class FundingSnapshotCollection:
    primary: tuple[FundingSnapshot, ...]
    short: tuple[FundingSnapshot, ...]
    long: tuple[FundingSnapshot, ...]


@dataclass(frozen=True)
class ScoreComponents:
    funding_score: Decimal
    persistence_score: Decimal
    stability_score: Decimal
    trend_score: Decimal
    lifetime_score: Decimal
    timing_score: Decimal
    penalties: dict[str, Decimal]
    total_penalty: Decimal
    total_score: Decimal

    def details(self) -> dict[str, object]:
        return {
            "funding_score": _decimal_text(self.funding_score),
            "persistence_score": _decimal_text(self.persistence_score),
            "stability_score": _decimal_text(self.stability_score),
            "trend_score": _decimal_text(self.trend_score),
            "lifetime_score": _decimal_text(self.lifetime_score),
            "timing_score": _decimal_text(self.timing_score),
            "penalties": {
                key: _decimal_text(value) for key, value in self.penalties.items()
            },
            "total_penalty": _decimal_text(self.total_penalty),
            "total_score": _decimal_text(self.total_score),
        }


@dataclass(frozen=True)
class CandidateInput:
    exchange: str
    futures_symbol: str
    spot_symbol: str | None
    mapping_status: SpotMappingStatus | None
    positive_strategy_available: bool
    spot_trading_allowed: bool
    futures_status: str | None
    current_predicted_funding_rate: Decimal | None
    next_funding_time: datetime | None
    observed_at: datetime | None
    evaluated_at: datetime
    metrics: FundingMetricsCollection
    snapshots: FundingSnapshotCollection

    @property
    def primary_metrics(self) -> FundingMetrics:
        return self.metrics.primary

    @property
    def short_metrics(self) -> FundingMetrics:
        return self.metrics.short

    @property
    def long_metrics(self) -> FundingMetrics:
        return self.metrics.long

    @property
    def primary_snapshots(self) -> tuple[FundingSnapshot, ...]:
        return self.snapshots.primary

    @property
    def short_snapshots(self) -> tuple[FundingSnapshot, ...]:
        return self.snapshots.short

    @property
    def long_snapshots(self) -> tuple[FundingSnapshot, ...]:
        return self.snapshots.long


@dataclass(frozen=True)
class CandidateRuleResult:
    hard_status: CandidateStatus | None
    rejection_reasons: tuple[RejectionReason, ...]
    warning_flags: tuple[RejectionReason, ...]
    persistence_ratio: Decimal | None
    positive_threshold_crossings: int | None
    signal_started_at: datetime | None
    signal_age_seconds: int | None
    latest_snapshot_age_seconds: int | None
    minutes_to_funding: Decimal | None
    late_spike: bool
    deteriorating: bool
    funding_falling: bool
    unstable: bool
    too_early: bool
    too_late: bool


@dataclass(frozen=True)
class CandidateEvaluation:
    exchange: str
    futures_symbol: str
    spot_symbol: str | None
    evaluated_at: datetime
    evaluated_at_bucket: datetime
    next_funding_time: datetime | None
    predicted_funding_rate: Decimal
    minimum_funding_rate: Decimal
    minutes_to_funding: Decimal | None
    status: CandidateStatus
    score_components: ScoreComponents
    persistence_ratio: Decimal | None
    standard_deviation: Decimal | None
    velocity: Decimal | None
    acceleration: Decimal | None
    threshold_crossings: int | None
    direction_changes: int | None
    signal_started_at: datetime | None
    signal_age_seconds: int | None
    snapshot_count: int
    history_duration_seconds: int | None
    latest_snapshot_at: datetime | None
    rejection_reasons: tuple[RejectionReason, ...]
    warning_flags: tuple[RejectionReason, ...]
    score_details: dict[str, object]
    metrics_details: dict[str, object]
    engine_version: str

    @property
    def total_score(self) -> Decimal:
        return self.score_components.total_score


@dataclass(frozen=True)
class CandidateRejectionAggregate:
    reason: RejectionReason
    symbol_count: int
    percentage: Decimal
    examples: tuple[str, ...]


@dataclass(frozen=True)
class FundingIntervalSummary:
    exchange: str
    futures_symbol: str
    funding_time: datetime
    interval_started_at: datetime | None
    interval_ended_at: datetime
    realized_funding_rate: Decimal
    first_predicted_rate: Decimal | None
    last_predicted_rate: Decimal | None
    minimum_predicted_rate: Decimal | None
    maximum_predicted_rate: Decimal | None
    peak_predicted_at: datetime | None
    mean_predicted_rate: Decimal | None
    median_predicted_rate: Decimal | None
    predicted_rate_120m_before: Decimal | None
    predicted_rate_60m_before: Decimal | None
    predicted_rate_30m_before: Decimal | None
    predicted_rate_15m_before: Decimal | None
    predicted_rate_5m_before: Decimal | None
    positive_snapshot_ratio: Decimal | None
    above_threshold_snapshot_ratio: Decimal | None
    above_threshold_duration_seconds: int | None
    maximum_above_threshold_streak_seconds: int | None
    signal_started_at: datetime | None
    longest_positive_streak_seconds: int | None
    threshold_crossings: int | None
    direction_changes: int | None
    prediction_error: Decimal | None
    absolute_prediction_error: Decimal | None
    snapshot_count: int
    history_coverage_ratio: Decimal | None
    summary_status: FundingIntervalSummaryStatus


@dataclass(frozen=True)
class FundingIntervalBuildResult:
    processed: int
    created: int
    updated: int
    partial: int
    skipped: int
    failed: int


class CandidateEvaluationStore(Protocol):
    async def upsert_evaluations(
        self,
        evaluations: Iterable[CandidateEvaluation],
    ) -> int:
        ...


class FundingIntervalSummaryStore(Protocol):
    async def confirmed_events_for_interval_summaries(
        self,
        limit: int,
    ) -> list[FundingEvent]:
        ...

    async def existing_interval_summary_keys(
        self,
        events: Iterable[FundingEvent],
    ) -> set[tuple[str, str, datetime]]:
        ...

    async def snapshots_for_interval(
        self,
        symbol: str,
        funding_time: datetime,
    ) -> list[FundingSnapshot]:
        ...

    async def upsert_interval_summaries(
        self,
        summaries: Iterable[FundingIntervalSummary],
    ) -> int:
        ...


class CandidateRuleEvaluator:
    def __init__(self, config: CandidateEngineConfig) -> None:
        self.config = config

    def evaluate(self, candidate: CandidateInput) -> CandidateRuleResult:
        reasons: list[RejectionReason] = []
        warnings: list[RejectionReason] = []
        evaluated_at = ensure_utc(candidate.evaluated_at)
        current_rate = candidate.current_predicted_funding_rate
        latest_snapshot_at = _latest_snapshot_at(candidate)
        latest_age = (
            int((evaluated_at - ensure_utc(latest_snapshot_at)).total_seconds())
            if latest_snapshot_at is not None
            else None
        )
        minutes_to_funding = _minutes_to_funding(
            candidate.next_funding_time,
            evaluated_at,
        )
        persistence_ratio = _positive_persistence_ratio(
            candidate.primary_snapshots,
            self.config.min_funding_rate,
        )
        positive_crossings = _positive_threshold_crossings(
            candidate.primary_snapshots,
            self.config.min_funding_rate,
        )
        signal_started_at = _signal_started_at(
            candidate.primary_snapshots,
            self.config.min_funding_rate,
        )
        signal_age = (
            int(
                (
                    ensure_utc(latest_snapshot_at) - ensure_utc(signal_started_at)
                ).total_seconds()
            )
            if latest_snapshot_at is not None and signal_started_at is not None
            else None
        )

        hard_status = self._hard_filter_status(
            candidate,
            current_rate,
            latest_age,
            minutes_to_funding,
            reasons,
        )

        unstable = self._is_unstable(
            candidate,
            positive_crossings,
            reasons,
            warnings,
        )
        too_early = (
            minutes_to_funding is not None
            and minutes_to_funding > Decimal(self.config.max_minutes_to_funding)
        )
        too_late = (
            minutes_to_funding is not None
            and minutes_to_funding < Decimal(self.config.min_minutes_to_funding)
        )
        if too_early:
            _add_unique(reasons, RejectionReason.TOO_EARLY)
        if too_late:
            _add_unique(reasons, RejectionReason.TOO_LATE)

        deteriorating, funding_falling = self._deterioration_state(
            candidate,
            reasons,
            warnings,
        )
        late_spike = self._is_late_spike(
            candidate,
            persistence_ratio,
            signal_age,
            minutes_to_funding,
            reasons,
            warnings,
        )
        if (
            persistence_ratio is not None
            and persistence_ratio < self.config.min_persistence_ratio
            and hard_status is None
        ):
            _add_unique(reasons, RejectionReason.PERSISTENCE_TOO_LOW)
            _add_unique(warnings, RejectionReason.PERSISTENCE_TOO_LOW)

        return CandidateRuleResult(
            hard_status=hard_status,
            rejection_reasons=tuple(reasons),
            warning_flags=tuple(warnings),
            persistence_ratio=persistence_ratio,
            positive_threshold_crossings=positive_crossings,
            signal_started_at=signal_started_at,
            signal_age_seconds=signal_age,
            latest_snapshot_age_seconds=latest_age,
            minutes_to_funding=minutes_to_funding,
            late_spike=late_spike,
            deteriorating=deteriorating,
            funding_falling=funding_falling,
            unstable=unstable,
            too_early=too_early,
            too_late=too_late,
        )

    def classify(
        self,
        candidate: CandidateInput,
        rules: CandidateRuleResult,
        score: Decimal,
    ) -> CandidateStatus:
        if rules.hard_status is not None:
            return rules.hard_status
        if rules.too_late:
            return CandidateStatus.TOO_LATE
        if rules.too_early:
            return CandidateStatus.TOO_EARLY
        if rules.late_spike:
            return CandidateStatus.LATE_SPIKE
        if rules.funding_falling:
            return CandidateStatus.FUNDING_FALLING
        if rules.deteriorating:
            return CandidateStatus.DETERIORATING
        if rules.unstable:
            return CandidateStatus.UNSTABLE
        if (
            rules.persistence_ratio is not None
            and rules.persistence_ratio < self.config.min_persistence_ratio
        ):
            return CandidateStatus.OBSERVING
        if score >= self.config.strong_score:
            return CandidateStatus.STRONG_CANDIDATE
        if score >= self.config.min_score:
            return CandidateStatus.CANDIDATE
        if candidate.current_predicted_funding_rate is not None:
            return CandidateStatus.WEAK_CANDIDATE
        return CandidateStatus.REJECTED

    def _hard_filter_status(
        self,
        candidate: CandidateInput,
        current_rate: Decimal | None,
        latest_age_seconds: int | None,
        minutes_to_funding: Decimal | None,
        reasons: list[RejectionReason],
    ) -> CandidateStatus | None:
        rejected = False
        if current_rate is None or not current_rate.is_finite():
            _add_unique(reasons, RejectionReason.INVALID_DATA)
            rejected = True
        elif current_rate <= ZERO:
            _add_unique(reasons, RejectionReason.FUNDING_NOT_POSITIVE)
            rejected = True
        elif current_rate < self.config.min_funding_rate:
            _add_unique(reasons, RejectionReason.FUNDING_BELOW_THRESHOLD)
            rejected = True

        if candidate.futures_status != "TRADING":
            _add_unique(reasons, RejectionReason.FUTURES_INACTIVE)
            rejected = True
        if candidate.mapping_status in {None, SpotMappingStatus.MISSING}:
            _add_unique(reasons, RejectionReason.SPOT_MAPPING_MISSING)
            rejected = True
        elif candidate.mapping_status == SpotMappingStatus.AMBIGUOUS:
            _add_unique(reasons, RejectionReason.SPOT_MAPPING_AMBIGUOUS)
            rejected = True
        elif candidate.mapping_status == SpotMappingStatus.SPOT_TRADING_DISABLED:
            _add_unique(reasons, RejectionReason.SPOT_TRADING_DISABLED)
            rejected = True
        elif candidate.mapping_status != SpotMappingStatus.MATCHED:
            _add_unique(reasons, RejectionReason.SPOT_MAPPING_MISSING)
            rejected = True

        if not candidate.positive_strategy_available:
            _add_unique(reasons, RejectionReason.POSITIVE_STRATEGY_UNAVAILABLE)
            rejected = True
        if not candidate.spot_trading_allowed:
            _add_unique(reasons, RejectionReason.SPOT_TRADING_DISABLED)
            rejected = True

        if candidate.next_funding_time is None:
            _add_unique(reasons, RejectionReason.NEXT_FUNDING_TIME_MISSING)
            rejected = True
        elif minutes_to_funding is not None and minutes_to_funding <= ZERO:
            _add_unique(reasons, RejectionReason.FUNDING_TIME_EXPIRED)
            return CandidateStatus.EXPIRED

        if latest_age_seconds is None:
            _add_unique(reasons, RejectionReason.STALE_SNAPSHOT)
            rejected = True
        elif latest_age_seconds > self.config.max_snapshot_age_seconds:
            _add_unique(reasons, RejectionReason.STALE_SNAPSHOT)
            return CandidateStatus.STALE

        if candidate.primary_metrics.snapshot_count < self.config.min_snapshot_count:
            _add_unique(reasons, RejectionReason.INSUFFICIENT_SNAPSHOT_COUNT)
            return CandidateStatus.INSUFFICIENT_HISTORY
        if (
            candidate.primary_metrics.history_duration
            < self.config.min_history_minutes * 60
        ):
            _add_unique(reasons, RejectionReason.INSUFFICIENT_HISTORY)
            return CandidateStatus.INSUFFICIENT_HISTORY

        return CandidateStatus.REJECTED if rejected else None

    def _is_unstable(
        self,
        candidate: CandidateInput,
        positive_crossings: int,
        reasons: list[RejectionReason],
        warnings: list[RejectionReason],
    ) -> bool:
        unstable = False
        std_rate = candidate.primary_metrics.std_rate
        if std_rate is not None and std_rate > self.config.max_std_dev:
            _add_unique(reasons, RejectionReason.VOLATILITY_TOO_HIGH)
            _add_unique(warnings, RejectionReason.VOLATILITY_TOO_HIGH)
            unstable = True
        if positive_crossings > self.config.max_threshold_crossings:
            _add_unique(reasons, RejectionReason.TOO_MANY_THRESHOLD_CROSSINGS)
            _add_unique(warnings, RejectionReason.TOO_MANY_THRESHOLD_CROSSINGS)
            unstable = True
        if (
            candidate.primary_metrics.direction_changes
            > self.config.max_direction_changes
        ):
            _add_unique(reasons, RejectionReason.TOO_MANY_DIRECTION_CHANGES)
            _add_unique(warnings, RejectionReason.TOO_MANY_DIRECTION_CHANGES)
            unstable = True
        return unstable

    def _is_late_spike(
        self,
        candidate: CandidateInput,
        persistence_ratio: Decimal | None,
        signal_age_seconds: int | None,
        minutes_to_funding: Decimal | None,
        reasons: list[RejectionReason],
        warnings: list[RejectionReason],
    ) -> bool:
        current = candidate.current_predicted_funding_rate
        previous_mean = _previous_mean_before_lookback(
            candidate.primary_snapshots,
            lookback_minutes=self.config.late_spike_lookback_minutes,
        )
        if (
            current is None
            or previous_mean is None
            or previous_mean <= ZERO
            or persistence_ratio is None
            or signal_age_seconds is None
            or minutes_to_funding is None
        ):
            return False

        jump_ratio = current / previous_mean
        settlement_is_close = minutes_to_funding <= Decimal(60)
        signal_is_young = signal_age_seconds <= (
            self.config.late_spike_lookback_minutes * 60
        )
        persistence_is_low = persistence_ratio < self.config.min_persistence_ratio
        trend_is_up = (
            candidate.short_metrics.rate_velocity is not None
            and candidate.short_metrics.rate_velocity > ZERO
            and candidate.short_metrics.rate_acceleration is not None
            and candidate.short_metrics.rate_acceleration >= ZERO
        )
        is_late_spike = (
            jump_ratio >= self.config.late_spike_min_jump_ratio
            and settlement_is_close
            and signal_is_young
            and persistence_is_low
            and trend_is_up
        )
        if is_late_spike:
            _add_unique(reasons, RejectionReason.LATE_SPIKE_DETECTED)
            _add_unique(warnings, RejectionReason.LATE_SPIKE_DETECTED)
        return is_late_spike

    def _deterioration_state(
        self,
        candidate: CandidateInput,
        reasons: list[RejectionReason],
        warnings: list[RejectionReason],
    ) -> tuple[bool, bool]:
        current = candidate.current_predicted_funding_rate
        if current is None:
            return False, False

        velocity = candidate.short_metrics.rate_velocity
        acceleration = candidate.short_metrics.rate_acceleration
        short_mean = candidate.short_metrics.mean_rate
        primary_mean = candidate.primary_metrics.mean_rate
        consecutive_declines = _consecutive_declines(candidate.short_snapshots)

        checks = [
            velocity is not None and velocity < ZERO,
            acceleration is not None and acceleration < ZERO,
            short_mean is not None and current < short_mean,
            short_mean is not None
            and primary_mean is not None
            and short_mean < primary_mean,
            current <= self.config.min_funding_rate * Decimal("1.25"),
            consecutive_declines >= 3,
        ]
        point_count = sum(1 for value in checks if value)
        if checks[0]:
            _add_unique(warnings, RejectionReason.NEGATIVE_VELOCITY)
        if checks[1]:
            _add_unique(warnings, RejectionReason.NEGATIVE_ACCELERATION)

        funding_falling = (
            current >= self.config.min_funding_rate
            and point_count >= 4
            and (
                velocity is not None
                and velocity <= self.config.max_negative_velocity
                or consecutive_declines >= 4
            )
        )
        deteriorating = current >= self.config.min_funding_rate and point_count >= 3

        if funding_falling or deteriorating:
            _add_unique(reasons, RejectionReason.FUNDING_DETERIORATING)
            _add_unique(warnings, RejectionReason.FUNDING_DETERIORATING)
        return deteriorating, funding_falling


class FundingScoreCalculator:
    def __init__(self, config: CandidateEngineConfig) -> None:
        self.config = config

    def calculate(self, current_rate: Decimal | None) -> Decimal:
        # TODO: replace the current linear formula with sigmoid/log/saturating
        # scoring when Stage 4+ has reliable market-cost inputs.
        if current_rate is None or current_rate <= ZERO:
            return ZERO
        ratio = current_rate / self.config.min_funding_rate
        return _quantize_score(_clamp(ratio * Decimal(10), ZERO, Decimal(30)))


class PersistenceScoreCalculator:
    def calculate(self, persistence_ratio: Decimal | None) -> Decimal:
        if persistence_ratio is None:
            return ZERO
        return _quantize_score(_clamp(persistence_ratio * Decimal(25), ZERO, Decimal(25)))


class StabilityScoreCalculator:
    def __init__(self, config: CandidateEngineConfig) -> None:
        self.config = config

    def calculate(
        self,
        candidate: CandidateInput,
        rules: CandidateRuleResult,
    ) -> Decimal:
        score = Decimal(15)
        std_rate = candidate.primary_metrics.std_rate
        if std_rate is not None and self.config.max_std_dev > ZERO:
            std_ratio = _clamp(std_rate / self.config.max_std_dev, ZERO, ONE)
            score -= std_ratio * Decimal(8)
        crossings = rules.positive_threshold_crossings or 0
        if self.config.max_threshold_crossings > 0:
            crossing_ratio = _clamp(
                Decimal(crossings) / Decimal(self.config.max_threshold_crossings),
                ZERO,
                ONE,
            )
            score -= crossing_ratio * Decimal(4)
        if self.config.max_direction_changes > 0:
            direction_ratio = _clamp(
                Decimal(candidate.primary_metrics.direction_changes)
                / Decimal(self.config.max_direction_changes),
                ZERO,
                ONE,
            )
            score -= direction_ratio * Decimal(3)
        return _quantize_score(_clamp(score, ZERO, Decimal(15)))


class TrendScoreCalculator:
    def calculate(self, candidate: CandidateInput) -> Decimal:
        score = Decimal(8)
        velocity = candidate.short_metrics.rate_velocity
        acceleration = candidate.short_metrics.rate_acceleration
        current = candidate.current_predicted_funding_rate
        short_mean = candidate.short_metrics.mean_rate
        primary_mean = candidate.primary_metrics.mean_rate

        if velocity is not None:
            score += Decimal(3) if velocity >= ZERO else Decimal(-3)
        if acceleration is not None:
            score += Decimal(2) if acceleration >= ZERO else Decimal(-2)
        if current is not None and short_mean is not None:
            score += Decimal(2) if current >= short_mean else Decimal(-2)
        if short_mean is not None and primary_mean is not None:
            score += Decimal(2) if short_mean >= primary_mean else Decimal(-2)
        if current is not None and primary_mean is not None and current >= primary_mean:
            score += Decimal(1)

        return _quantize_score(_clamp(score, ZERO, Decimal(15)))


class LifetimeScoreCalculator:
    def __init__(self, config: CandidateEngineConfig) -> None:
        self.config = config

    def calculate(self, signal_age_seconds: int | None) -> Decimal:
        if signal_age_seconds is None:
            return ZERO
        required_seconds = Decimal(self.config.min_history_minutes * 60)
        if required_seconds <= ZERO:
            return Decimal(10)
        ratio = Decimal(signal_age_seconds) / required_seconds
        return _quantize_score(_clamp(ratio * Decimal(10), ZERO, Decimal(10)))


class TimingScoreCalculator:
    def __init__(self, config: CandidateEngineConfig) -> None:
        self.config = config

    def calculate(self, minutes_to_funding: Decimal | None) -> Decimal:
        if minutes_to_funding is None:
            return ZERO
        if minutes_to_funding < Decimal(self.config.min_minutes_to_funding):
            return ZERO
        if minutes_to_funding <= Decimal(15):
            return Decimal(2)
        if minutes_to_funding <= Decimal(120):
            return Decimal(5)
        if minutes_to_funding <= Decimal(self.config.max_minutes_to_funding):
            return Decimal(3)
        return Decimal(1)


class PenaltyCalculator:
    def __init__(self, config: CandidateEngineConfig) -> None:
        self.config = config

    def calculate(
        self,
        candidate: CandidateInput,
        rules: CandidateRuleResult,
    ) -> dict[str, Decimal]:
        penalties: dict[str, Decimal] = {}
        if rules.late_spike:
            penalties[PenaltyCode.LATE_SPIKE.value] = Decimal(20)
        if rules.funding_falling:
            penalties[PenaltyCode.DETERIORATION.value] = Decimal(15)
        elif rules.deteriorating:
            penalties[PenaltyCode.DETERIORATION.value] = Decimal(10)
        if rules.unstable:
            penalties[PenaltyCode.INSTABILITY.value] = Decimal(8)
        if (
            rules.positive_threshold_crossings is not None
            and rules.positive_threshold_crossings
            > self.config.max_threshold_crossings
        ):
            penalties[PenaltyCode.THRESHOLD_CROSSING.value] = Decimal(5)
        if rules.too_late:
            penalties[PenaltyCode.TOO_CLOSE_TO_FUNDING.value] = Decimal(5)
        if candidate.primary_metrics.snapshot_count < self.config.min_snapshot_count:
            penalties[PenaltyCode.INSUFFICIENT_HISTORY.value] = Decimal(10)
        if (
            rules.latest_snapshot_age_seconds is not None
            and rules.latest_snapshot_age_seconds
            > self.config.max_snapshot_age_seconds
        ):
            penalties[PenaltyCode.STALE_DATA.value] = Decimal(20)
        return penalties


class CandidateScoringService:
    def __init__(
        self,
        config: CandidateEngineConfig,
        *,
        funding_calculator: FundingScoreCalculator | None = None,
        persistence_calculator: PersistenceScoreCalculator | None = None,
        stability_calculator: StabilityScoreCalculator | None = None,
        trend_calculator: TrendScoreCalculator | None = None,
        lifetime_calculator: LifetimeScoreCalculator | None = None,
        timing_calculator: TimingScoreCalculator | None = None,
        penalty_calculator: PenaltyCalculator | None = None,
    ) -> None:
        self.config = config
        self.funding_calculator = funding_calculator or FundingScoreCalculator(config)
        self.persistence_calculator = (
            persistence_calculator or PersistenceScoreCalculator()
        )
        self.stability_calculator = (
            stability_calculator or StabilityScoreCalculator(config)
        )
        self.trend_calculator = trend_calculator or TrendScoreCalculator()
        self.lifetime_calculator = lifetime_calculator or LifetimeScoreCalculator(
            config
        )
        self.timing_calculator = timing_calculator or TimingScoreCalculator(config)
        self.penalty_calculator = penalty_calculator or PenaltyCalculator(config)

    def score(
        self,
        candidate: CandidateInput,
        rules: CandidateRuleResult,
    ) -> ScoreComponents:
        if rules.hard_status in {
            CandidateStatus.REJECTED,
            CandidateStatus.EXPIRED,
            CandidateStatus.STALE,
            CandidateStatus.INSUFFICIENT_HISTORY,
        }:
            return zero_score_components()

        funding_score = self.funding_calculator.calculate(
            candidate.current_predicted_funding_rate
        )
        persistence_score = self.persistence_calculator.calculate(
            rules.persistence_ratio
        )
        stability_score = self.stability_calculator.calculate(candidate, rules)
        trend_score = self.trend_calculator.calculate(candidate)
        lifetime_score = self.lifetime_calculator.calculate(rules.signal_age_seconds)
        timing_score = self.timing_calculator.calculate(rules.minutes_to_funding)
        penalties = self.penalty_calculator.calculate(candidate, rules)
        total_penalty = _quantize_score(sum(penalties.values(), ZERO))
        base_score = (
            funding_score
            + persistence_score
            + stability_score
            + trend_score
            + lifetime_score
            + timing_score
        )
        total_score = _clamp(base_score - total_penalty, ZERO, Decimal(100))
        return ScoreComponents(
            funding_score=funding_score,
            persistence_score=persistence_score,
            stability_score=stability_score,
            trend_score=trend_score,
            lifetime_score=lifetime_score,
            timing_score=timing_score,
            penalties=penalties,
            total_penalty=total_penalty,
            total_score=_quantize_score(total_score),
        )


class CandidateEngine:
    def __init__(
        self,
        *,
        config: CandidateEngineConfig,
        rule_evaluator: CandidateRuleEvaluator | None = None,
        scoring_service: CandidateScoringService | None = None,
    ) -> None:
        self.config = config
        self.rule_evaluator = rule_evaluator or CandidateRuleEvaluator(config)
        self.scoring_service = scoring_service or CandidateScoringService(config)

    def evaluate(self, candidate: CandidateInput) -> CandidateEvaluation:
        try:
            rules = self.rule_evaluator.evaluate(candidate)
            score = self.scoring_service.score(candidate, rules)
            status = self.rule_evaluator.classify(candidate, rules, score.total_score)
            return self._evaluation(candidate, rules, score, status)
        except Exception as exc:
            logger.exception(
                "candidate calculation failed for %s",
                candidate.futures_symbol,
            )
            return self._calculation_error(candidate, exc)

    def evaluate_many(
        self,
        candidates: Iterable[CandidateInput],
    ) -> list[CandidateEvaluation]:
        evaluations = [self.evaluate(candidate) for candidate in candidates]
        return rank_evaluations(evaluations)

    def inputs_from_history(
        self,
        mappings: Iterable[InstrumentMapping],
        history: FundingHistoryService,
        *,
        evaluated_at: datetime | None = None,
    ) -> list[CandidateInput]:
        timestamp = ensure_utc(evaluated_at or utc_now())
        inputs = [
            _candidate_input_from_history(
                mapping,
                history,
                config=self.config,
                evaluated_at=timestamp,
            )
            for mapping in mappings
        ]
        return sorted(inputs, key=lambda item: item.futures_symbol)

    def _evaluation(
        self,
        candidate: CandidateInput,
        rules: CandidateRuleResult,
        score: ScoreComponents,
        status: CandidateStatus,
    ) -> CandidateEvaluation:
        latest_snapshot_at = _latest_snapshot_at(candidate)
        threshold_crossings = rules.positive_threshold_crossings
        persistence_ratio = (
            rules.persistence_ratio.quantize(RATIO_QUANT)
            if rules.persistence_ratio is not None
            else None
        )
        return CandidateEvaluation(
            exchange=candidate.exchange,
            futures_symbol=candidate.futures_symbol,
            spot_symbol=candidate.spot_symbol,
            evaluated_at=ensure_utc(candidate.evaluated_at),
            evaluated_at_bucket=_bucket_time(
                candidate.evaluated_at,
                self.config.persist_interval_seconds,
            ),
            next_funding_time=ensure_utc(candidate.next_funding_time)
            if candidate.next_funding_time is not None
            else None,
            predicted_funding_rate=(
                candidate.current_predicted_funding_rate
                if candidate.current_predicted_funding_rate is not None
                else ZERO
            ),
            minimum_funding_rate=self.config.min_funding_rate,
            minutes_to_funding=rules.minutes_to_funding,
            status=status,
            score_components=score,
            persistence_ratio=persistence_ratio,
            standard_deviation=candidate.primary_metrics.std_rate,
            velocity=candidate.short_metrics.rate_velocity,
            acceleration=candidate.short_metrics.rate_acceleration,
            threshold_crossings=threshold_crossings,
            direction_changes=candidate.primary_metrics.direction_changes,
            signal_started_at=rules.signal_started_at,
            signal_age_seconds=rules.signal_age_seconds,
            snapshot_count=candidate.primary_metrics.snapshot_count,
            history_duration_seconds=candidate.primary_metrics.history_duration,
            latest_snapshot_at=latest_snapshot_at,
            rejection_reasons=rules.rejection_reasons,
            warning_flags=rules.warning_flags,
            score_details=score.details(),
            metrics_details=_metrics_details(candidate, rules),
            engine_version=self.config.engine_version,
        )

    def _calculation_error(
        self,
        candidate: CandidateInput,
        exc: Exception,
    ) -> CandidateEvaluation:
        score = zero_score_components()
        latest_snapshot_at = _latest_snapshot_at(candidate)
        return CandidateEvaluation(
            exchange=candidate.exchange,
            futures_symbol=candidate.futures_symbol,
            spot_symbol=candidate.spot_symbol,
            evaluated_at=ensure_utc(candidate.evaluated_at),
            evaluated_at_bucket=_bucket_time(
                candidate.evaluated_at,
                self.config.persist_interval_seconds,
            ),
            next_funding_time=candidate.next_funding_time,
            predicted_funding_rate=candidate.current_predicted_funding_rate or ZERO,
            minimum_funding_rate=self.config.min_funding_rate,
            minutes_to_funding=_minutes_to_funding(
                candidate.next_funding_time,
                candidate.evaluated_at,
            ),
            status=CandidateStatus.REJECTED,
            score_components=score,
            persistence_ratio=None,
            standard_deviation=None,
            velocity=None,
            acceleration=None,
            threshold_crossings=None,
            direction_changes=None,
            signal_started_at=None,
            signal_age_seconds=None,
            snapshot_count=candidate.primary_metrics.snapshot_count,
            history_duration_seconds=candidate.primary_metrics.history_duration,
            latest_snapshot_at=latest_snapshot_at,
            rejection_reasons=(RejectionReason.CALCULATION_ERROR,),
            warning_flags=(RejectionReason.CALCULATION_ERROR,),
            score_details=score.details(),
            metrics_details={"error_type": type(exc).__name__},
            engine_version=self.config.engine_version,
        )


class FundingIntervalAnalyticsService:
    def __init__(
        self,
        *,
        repository: FundingIntervalSummaryStore,
        config: CandidateEngineConfig,
        builder: FundingIntervalBuilder | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        self.builder = builder or FundingIntervalBuilder(
            min_funding_rate=config.min_funding_rate,
            point_tolerance_seconds=config.interval_point_tolerance_seconds,
        )

    async def build_missing_summaries(self) -> FundingIntervalBuildResult:
        events = await self.repository.confirmed_events_for_interval_summaries(
            self.config.interval_summary_batch_size
        )
        existing_keys = await self.repository.existing_interval_summary_keys(events)
        summaries: list[FundingIntervalSummary] = []
        failed = 0
        skipped = 0
        partial = 0

        for event in events:
            if event.actual_funding_rate is None:
                skipped += 1
                continue
            try:
                snapshots = await self.repository.snapshots_for_interval(
                    event.symbol,
                    event.funding_time,
                )
                summary = self.builder.build(event, snapshots)
                summaries.append(summary)
                if summary.summary_status != FundingIntervalSummaryStatus.COMPLETE:
                    partial += 1
            except Exception:
                logger.exception(
                    "funding interval summary failed for %s %s",
                    event.symbol,
                    event.funding_time.isoformat(),
                )
                failed += 1

        await self.repository.upsert_interval_summaries(summaries)
        created = sum(
            1
            for summary in summaries
            if (
                summary.exchange,
                summary.futures_symbol,
                ensure_utc(summary.funding_time),
            )
            not in existing_keys
        )
        updated = len(summaries) - created
        return FundingIntervalBuildResult(
            processed=len(events),
            created=created,
            updated=updated,
            partial=partial,
            skipped=skipped,
            failed=failed,
        )


def zero_score_components() -> ScoreComponents:
    return ScoreComponents(
        funding_score=ZERO,
        persistence_score=ZERO,
        stability_score=ZERO,
        trend_score=ZERO,
        lifetime_score=ZERO,
        timing_score=ZERO,
        penalties={},
        total_penalty=ZERO,
        total_score=ZERO,
    )


def rank_evaluations(
    evaluations: Iterable[CandidateEvaluation],
) -> list[CandidateEvaluation]:
    return sorted(
        evaluations,
        key=lambda item: (
            _STATUS_PRIORITY[item.status],
            -item.total_score,
            -item.predicted_funding_rate,
            -(item.persistence_ratio or ZERO),
            item.minutes_to_funding
            if item.minutes_to_funding is not None
            else Decimal(999999),
            item.futures_symbol,
        ),
    )


def rejection_aggregates(
    evaluations: Iterable[CandidateEvaluation],
) -> list[CandidateRejectionAggregate]:
    rows = list(evaluations)
    total_symbols = len(rows)
    reason_symbols: dict[RejectionReason, list[str]] = {}
    for evaluation in rows:
        for reason in evaluation.rejection_reasons:
            reason_symbols.setdefault(reason, []).append(evaluation.futures_symbol)

    aggregates: list[CandidateRejectionAggregate] = []
    for reason, symbols in reason_symbols.items():
        percentage = (
            (Decimal(len(symbols)) / Decimal(total_symbols) * Decimal(100))
            if total_symbols
            else ZERO
        )
        aggregates.append(
            CandidateRejectionAggregate(
                reason=reason,
                symbol_count=len(symbols),
                percentage=percentage.quantize(RATIO_QUANT),
                examples=tuple(sorted(symbols)[:5]),
            )
        )
    return sorted(aggregates, key=lambda item: (-item.symbol_count, item.reason.value))


class FundingIntervalBuilder:
    def __init__(
        self,
        *,
        min_funding_rate: Decimal,
        point_tolerance_seconds: int,
        exchange: str = DEFAULT_EXCHANGE,
    ) -> None:
        self.min_funding_rate = min_funding_rate
        self.point_tolerance_seconds = point_tolerance_seconds
        self.exchange = exchange

    def build(
        self,
        event: FundingEvent,
        snapshots: Sequence[FundingSnapshot],
    ) -> FundingIntervalSummary:
        if event.actual_funding_rate is None:
            raise ValueError("confirmed funding event must have actual_funding_rate")

        funding_time = ensure_utc(event.funding_time)
        ordered = [
            snapshot
            for snapshot in sorted(snapshots, key=lambda item: item.event_time)
            if ensure_utc(snapshot.event_time) <= funding_time
        ]
        rates = [snapshot.predicted_funding_rate for snapshot in ordered]
        interval_seconds = event.funding_interval_hours * 60 * 60
        history_duration = (
            int(
                (
                    ensure_utc(ordered[-1].event_time)
                    - ensure_utc(ordered[0].event_time)
                ).total_seconds()
            )
            if len(ordered) >= 2
            else 0
        )
        coverage_ratio = (
            _clamp(Decimal(history_duration) / Decimal(interval_seconds), ZERO, ONE)
            if interval_seconds > 0
            else None
        )
        last_predicted = rates[-1] if rates else None
        prediction_error = (
            last_predicted - event.actual_funding_rate
            if last_predicted is not None
            else None
        )
        status = _summary_status(
            snapshot_count=len(ordered),
            coverage_ratio=coverage_ratio,
        )

        return FundingIntervalSummary(
            exchange=self.exchange,
            futures_symbol=event.symbol,
            funding_time=funding_time,
            interval_started_at=ensure_utc(ordered[0].event_time) if ordered else None,
            interval_ended_at=funding_time,
            realized_funding_rate=event.actual_funding_rate,
            first_predicted_rate=rates[0] if rates else None,
            last_predicted_rate=last_predicted,
            minimum_predicted_rate=min(rates) if rates else None,
            maximum_predicted_rate=max(rates) if rates else None,
            peak_predicted_at=_peak_predicted_at(ordered),
            mean_predicted_rate=_mean(rates) if rates else None,
            median_predicted_rate=_median(rates) if rates else None,
            predicted_rate_120m_before=_point_rate(
                ordered,
                funding_time,
                minutes_before=120,
                tolerance_seconds=self.point_tolerance_seconds,
            ),
            predicted_rate_60m_before=_point_rate(
                ordered,
                funding_time,
                minutes_before=60,
                tolerance_seconds=self.point_tolerance_seconds,
            ),
            predicted_rate_30m_before=_point_rate(
                ordered,
                funding_time,
                minutes_before=30,
                tolerance_seconds=self.point_tolerance_seconds,
            ),
            predicted_rate_15m_before=_point_rate(
                ordered,
                funding_time,
                minutes_before=15,
                tolerance_seconds=self.point_tolerance_seconds,
            ),
            predicted_rate_5m_before=_point_rate(
                ordered,
                funding_time,
                minutes_before=5,
                tolerance_seconds=self.point_tolerance_seconds,
            ),
            positive_snapshot_ratio=_positive_ratio(rates),
            above_threshold_snapshot_ratio=_above_threshold_ratio(
                rates,
                self.min_funding_rate,
            ),
            above_threshold_duration_seconds=_duration_above_threshold(
                ordered,
                self.min_funding_rate,
            ),
            maximum_above_threshold_streak_seconds=_max_above_threshold_streak(
                ordered,
                self.min_funding_rate,
            ),
            signal_started_at=_signal_started_at(ordered, self.min_funding_rate),
            longest_positive_streak_seconds=_max_positive_streak(ordered),
            threshold_crossings=_positive_threshold_crossings(
                ordered,
                self.min_funding_rate,
            ),
            direction_changes=_rate_direction_changes(ordered),
            prediction_error=prediction_error,
            absolute_prediction_error=abs(prediction_error)
            if prediction_error is not None
            else None,
            snapshot_count=len(ordered),
            history_coverage_ratio=coverage_ratio.quantize(RATIO_QUANT)
            if coverage_ratio is not None
            else None,
            summary_status=status,
        )


def build_funding_interval_summary(
    event: FundingEvent,
    snapshots: Sequence[FundingSnapshot],
    *,
    min_funding_rate: Decimal,
    point_tolerance_seconds: int,
) -> FundingIntervalSummary:
    return FundingIntervalBuilder(
        min_funding_rate=min_funding_rate,
        point_tolerance_seconds=point_tolerance_seconds,
    ).build(event, snapshots)


def _candidate_input_from_history(
    mapping: InstrumentMapping,
    history: FundingHistoryService,
    *,
    config: CandidateEngineConfig,
    evaluated_at: datetime,
) -> CandidateInput:
    symbol = mapping.futures_symbol
    primary_snapshots = tuple(history.get_window(symbol, config.primary_window_minutes))
    short_snapshots = tuple(history.get_window(symbol, config.short_window_minutes))
    long_snapshots = tuple(history.get_window(symbol, config.long_window_minutes))
    current_snapshot = long_snapshots[-1] if long_snapshots else None
    return CandidateInput(
        exchange=DEFAULT_EXCHANGE,
        futures_symbol=symbol,
        spot_symbol=mapping.spot_symbol,
        mapping_status=mapping.spot_mapping_status,
        positive_strategy_available=mapping.positive_strategy_available,
        spot_trading_allowed=mapping.spot_trading_allowed,
        futures_status=mapping.futures_status,
        current_predicted_funding_rate=current_snapshot.predicted_funding_rate
        if current_snapshot is not None
        else None,
        next_funding_time=current_snapshot.next_funding_time
        if current_snapshot is not None
        else None,
        observed_at=current_snapshot.event_time if current_snapshot is not None else None,
        evaluated_at=ensure_utc(evaluated_at),
        metrics=FundingMetricsCollection(
            primary=calculate_funding_metrics(
                primary_snapshots,
                abs_threshold=config.min_funding_rate,
            ),
            short=calculate_funding_metrics(
                short_snapshots,
                abs_threshold=config.min_funding_rate,
            ),
            long=calculate_funding_metrics(
                long_snapshots,
                abs_threshold=config.min_funding_rate,
            ),
        ),
        snapshots=FundingSnapshotCollection(
            primary=primary_snapshots,
            short=short_snapshots,
            long=long_snapshots,
        ),
    )


def _metrics_details(
    candidate: CandidateInput,
    rules: CandidateRuleResult,
) -> dict[str, object]:
    return {
        "primary_window_snapshot_count": candidate.primary_metrics.snapshot_count,
        "short_window_snapshot_count": candidate.short_metrics.snapshot_count,
        "long_window_snapshot_count": candidate.long_metrics.snapshot_count,
        "primary_window_history_duration_seconds": (
            candidate.primary_metrics.history_duration
        ),
        "short_window_history_duration_seconds": candidate.short_metrics.history_duration,
        "long_window_history_duration_seconds": candidate.long_metrics.history_duration,
        "primary_mean_rate": _optional_decimal_text(candidate.primary_metrics.mean_rate),
        "short_mean_rate": _optional_decimal_text(candidate.short_metrics.mean_rate),
        "long_mean_rate": _optional_decimal_text(candidate.long_metrics.mean_rate),
        "primary_min_rate": _optional_decimal_text(candidate.primary_metrics.min_rate),
        "primary_max_rate": _optional_decimal_text(candidate.primary_metrics.max_rate),
        "persistence_ratio": _optional_decimal_text(rules.persistence_ratio),
        "latest_snapshot_age_seconds": rules.latest_snapshot_age_seconds,
    }


def _latest_snapshot_at(candidate: CandidateInput) -> datetime | None:
    if candidate.observed_at is not None:
        return ensure_utc(candidate.observed_at)
    if candidate.long_metrics.latest_snapshot is not None:
        return ensure_utc(candidate.long_metrics.latest_snapshot)
    return None


def _minutes_to_funding(
    next_funding_time: datetime | None,
    evaluated_at: datetime,
) -> Decimal | None:
    if next_funding_time is None:
        return None
    seconds = int((ensure_utc(next_funding_time) - ensure_utc(evaluated_at)).total_seconds())
    return Decimal(seconds) / Decimal(60)


def _positive_persistence_ratio(
    snapshots: Sequence[FundingSnapshot],
    threshold: Decimal,
) -> Decimal | None:
    if not snapshots:
        return None
    above = sum(1 for snapshot in snapshots if snapshot.predicted_funding_rate >= threshold)
    return Decimal(above) / Decimal(len(snapshots))


def _positive_threshold_crossings(
    snapshots: Sequence[FundingSnapshot],
    threshold: Decimal,
) -> int:
    states = [snapshot.predicted_funding_rate >= threshold for snapshot in snapshots]
    return sum(1 for previous, current in pairwise(states) if previous != current)


def _signal_started_at(
    snapshots: Sequence[FundingSnapshot],
    threshold: Decimal,
) -> datetime | None:
    if not snapshots or snapshots[-1].predicted_funding_rate < threshold:
        return None
    index = len(snapshots) - 1
    while index > 0 and snapshots[index - 1].predicted_funding_rate >= threshold:
        index -= 1
    return ensure_utc(snapshots[index].event_time)


def _previous_mean_before_lookback(
    snapshots: Sequence[FundingSnapshot],
    *,
    lookback_minutes: int,
) -> Decimal | None:
    if not snapshots:
        return None
    cutoff = ensure_utc(snapshots[-1].event_time) - timedelta(minutes=lookback_minutes)
    previous_rates = [
        snapshot.predicted_funding_rate
        for snapshot in snapshots
        if ensure_utc(snapshot.event_time) < cutoff
    ]
    if not previous_rates:
        return None
    return _mean(previous_rates)


def _consecutive_declines(snapshots: Sequence[FundingSnapshot]) -> int:
    declines = 0
    for previous, current in reversed(list(pairwise(snapshots))):
        if current.predicted_funding_rate < previous.predicted_funding_rate:
            declines += 1
            continue
        break
    return declines


def _duration_above_threshold(
    snapshots: Sequence[FundingSnapshot],
    threshold: Decimal,
) -> int:
    total = 0
    for previous, current in pairwise(snapshots):
        if previous.predicted_funding_rate >= threshold:
            total += max(
                0,
                int(
                    (
                        ensure_utc(current.event_time)
                        - ensure_utc(previous.event_time)
                    ).total_seconds()
                ),
            )
    return total


def _max_above_threshold_streak(
    snapshots: Sequence[FundingSnapshot],
    threshold: Decimal,
) -> int:
    current_streak = 0
    max_streak = 0
    for previous, current in pairwise(snapshots):
        seconds = max(
            0,
            int(
                (
                    ensure_utc(current.event_time) - ensure_utc(previous.event_time)
                ).total_seconds()
            ),
        )
        if previous.predicted_funding_rate >= threshold:
            current_streak += seconds
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 0
    return max_streak


def _peak_predicted_at(snapshots: Sequence[FundingSnapshot]) -> datetime | None:
    if not snapshots:
        return None
    peak = max(snapshots, key=lambda item: item.predicted_funding_rate)
    return ensure_utc(peak.event_time)


def _max_positive_streak(snapshots: Sequence[FundingSnapshot]) -> int:
    current_streak = 0
    max_streak = 0
    for previous, current in pairwise(snapshots):
        seconds = max(
            0,
            int(
                (
                    ensure_utc(current.event_time) - ensure_utc(previous.event_time)
                ).total_seconds()
            ),
        )
        if previous.predicted_funding_rate > ZERO:
            current_streak += seconds
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 0
    return max_streak


def _rate_direction_changes(snapshots: Sequence[FundingSnapshot]) -> int:
    if len(snapshots) < 3:
        return 0
    deltas: list[int] = []
    for previous, current in pairwise(snapshots):
        if current.predicted_funding_rate > previous.predicted_funding_rate:
            deltas.append(1)
        elif current.predicted_funding_rate < previous.predicted_funding_rate:
            deltas.append(-1)
        else:
            deltas.append(0)
    non_zero = [value for value in deltas if value != 0]
    return sum(1 for previous, current in pairwise(non_zero) if previous != current)


def _point_rate(
    snapshots: Sequence[FundingSnapshot],
    funding_time: datetime,
    *,
    minutes_before: int,
    tolerance_seconds: int,
) -> Decimal | None:
    if not snapshots:
        return None
    target = ensure_utc(funding_time) - timedelta(minutes=minutes_before)
    candidates = [
        snapshot
        for snapshot in snapshots
        if ensure_utc(snapshot.event_time) <= ensure_utc(funding_time)
    ]
    if not candidates:
        return None
    closest = min(
        candidates,
        key=lambda item: abs((ensure_utc(item.event_time) - target).total_seconds()),
    )
    distance = abs((ensure_utc(closest.event_time) - target).total_seconds())
    if distance > tolerance_seconds:
        return None
    return closest.predicted_funding_rate


def _positive_ratio(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return (Decimal(sum(1 for value in values if value > ZERO)) / Decimal(len(values))).quantize(
        RATIO_QUANT
    )


def _above_threshold_ratio(
    values: Sequence[Decimal],
    threshold: Decimal,
) -> Decimal | None:
    if not values:
        return None
    return (
        Decimal(sum(1 for value in values if value >= threshold)) / Decimal(len(values))
    ).quantize(RATIO_QUANT)


def _summary_status(
    *,
    snapshot_count: int,
    coverage_ratio: Decimal | None,
) -> FundingIntervalSummaryStatus:
    if snapshot_count == 0:
        return FundingIntervalSummaryStatus.INSUFFICIENT_HISTORY
    if coverage_ratio is None:
        return FundingIntervalSummaryStatus.PARTIAL_HISTORY
    if coverage_ratio >= Decimal("0.80"):
        return FundingIntervalSummaryStatus.COMPLETE
    return FundingIntervalSummaryStatus.PARTIAL_HISTORY


def _mean(values: Sequence[Decimal]) -> Decimal:
    return sum(values, ZERO) / Decimal(len(values))


def _median(values: Sequence[Decimal]) -> Decimal:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _bucket_time(value: datetime, interval_seconds: int) -> datetime:
    timestamp = int(ensure_utc(value).timestamp())
    bucket = timestamp - (timestamp % interval_seconds)
    return datetime.fromtimestamp(bucket, tz=ensure_utc(value).tzinfo)


def _clamp(value: Decimal, minimum: Decimal, maximum: Decimal) -> Decimal:
    return max(minimum, min(maximum, value))


def _quantize_score(value: Decimal) -> Decimal:
    return value.quantize(SCORE_QUANT, rounding=ROUND_HALF_UP)


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return _decimal_text(value) if value is not None else None


def _add_unique(items: list[RejectionReason], item: RejectionReason) -> None:
    if item not in items:
        items.append(item)


_STATUS_PRIORITY = {
    CandidateStatus.STRONG_CANDIDATE: 0,
    CandidateStatus.CANDIDATE: 1,
    CandidateStatus.WEAK_CANDIDATE: 2,
    CandidateStatus.OBSERVING: 3,
    CandidateStatus.DETERIORATING: 4,
    CandidateStatus.FUNDING_FALLING: 5,
    CandidateStatus.UNSTABLE: 6,
    CandidateStatus.LATE_SPIKE: 7,
    CandidateStatus.TOO_EARLY: 8,
    CandidateStatus.TOO_LATE: 9,
    CandidateStatus.STALE: 10,
    CandidateStatus.INSUFFICIENT_HISTORY: 11,
    CandidateStatus.REJECTED: 12,
    CandidateStatus.EXPIRED: 13,
}
