from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import pairwise

from funding_monitor.history_service import FundingMetrics, calculate_funding_metrics
from funding_monitor.models import FundingSnapshot, ensure_utc

from .models import (
    EntryMode,
    RejectionReason,
    SignalDetection,
    StrategyValidationConfig,
)

ZERO = Decimal(0)
ONE = Decimal(1)


class SignalDetector:
    def __init__(self, config: StrategyValidationConfig) -> None:
        self.config = config

    def detect(
        self,
        snapshots: Sequence[FundingSnapshot],
        funding_time: datetime,
    ) -> SignalDetection:
        funding_time = ensure_utc(funding_time)
        ordered = tuple(
            snapshot
            for snapshot in sorted(snapshots, key=lambda item: ensure_utc(item.event_time))
            if ensure_utc(snapshot.event_time) <= funding_time
        )
        if not ordered:
            return _rejected(RejectionReason.INSUFFICIENT_HISTORY)

        if self.config.entry_mode == EntryMode.FIXED_TIME:
            target = funding_time - timedelta(
                minutes=self.config.entry_minutes_before_funding
            )
            entry = _latest_at_or_before(ordered, target)
            if entry is None:
                return _rejected(RejectionReason.STALE_SNAPSHOTS)
            return self._evaluate_entry(
                ordered,
                entry=entry,
                decision_time=target,
                funding_time=funding_time,
            )

        return self._first_qualifying_signal(ordered, funding_time)

    def _first_qualifying_signal(
        self,
        snapshots: Sequence[FundingSnapshot],
        funding_time: datetime,
    ) -> SignalDetection:
        signal_started_at: datetime | None = None
        required_seconds = self.config.signal_confirmation_minutes * 60
        for snapshot in snapshots:
            observed_at = ensure_utc(snapshot.event_time)
            if snapshot.predicted_funding_rate >= self.config.funding_threshold:
                if signal_started_at is None:
                    signal_started_at = observed_at
                if int((observed_at - signal_started_at).total_seconds()) >= required_seconds:
                    return self._evaluate_entry(
                        snapshots,
                        entry=snapshot,
                        decision_time=observed_at,
                        funding_time=funding_time,
                        forced_signal_started_at=signal_started_at,
                    )
                continue
            signal_started_at = None
        return _rejected(RejectionReason.NO_QUALIFYING_SIGNAL)

    def _evaluate_entry(
        self,
        snapshots: Sequence[FundingSnapshot],
        *,
        entry: FundingSnapshot,
        decision_time: datetime,
        funding_time: datetime,
        forced_signal_started_at: datetime | None = None,
    ) -> SignalDetection:
        entry_time = ensure_utc(entry.event_time)
        history = tuple(
            snapshot
            for snapshot in snapshots
            if ensure_utc(snapshot.event_time) <= entry_time
        )
        metrics = calculate_funding_metrics(
            history,
            abs_threshold=self.config.funding_threshold,
        )
        persistence = _persistence_ratio(history, self.config.funding_threshold)
        threshold_crossings = _threshold_crossings(history, self.config.funding_threshold)
        signal_started_at = forced_signal_started_at or _signal_started_at(
            history,
            self.config.funding_threshold,
        )
        entry_minutes_before = Decimal(
            int((funding_time - entry_time).total_seconds())
        ) / Decimal(60)
        latest_age_seconds = int((ensure_utc(decision_time) - entry_time).total_seconds())
        maximum_before_entry = max(snapshot.predicted_funding_rate for snapshot in history)
        prediction_drop = maximum_before_entry - entry.predicted_funding_rate

        rejection = _entry_rejection_reason(
            self.config,
            entry,
            metrics,
            persistence,
            latest_age_seconds,
            prediction_drop,
        )
        return SignalDetection(
            signal_detected=rejection is None,
            signal_started_at=signal_started_at if rejection is None else None,
            signal_confirmed_at=entry_time if rejection is None else None,
            entry_time=entry_time if rejection is None else None,
            entry_minutes_before_funding=entry_minutes_before if rejection is None else None,
            predicted_funding_at_entry=entry.predicted_funding_rate
            if rejection is None
            else None,
            persistence_at_entry=persistence if rejection is None else None,
            funding_std_at_entry=metrics.std_rate if rejection is None else None,
            funding_velocity_at_entry=metrics.rate_velocity if rejection is None else None,
            threshold_crossings_before_entry=threshold_crossings,
            late_spike=_late_spike(
                history,
                persistence,
                signal_started_at,
                entry_time,
                self.config,
            ),
            deteriorating_signal=(
                metrics.rate_velocity is not None and metrics.rate_velocity < ZERO
            ),
            continuous_signal=_continuous_signal_from_start(
                history,
                signal_started_at,
                self.config.funding_threshold,
            ),
            rejection_reason=rejection,
        )


def _entry_rejection_reason(
    config: StrategyValidationConfig,
    entry: FundingSnapshot,
    metrics: FundingMetrics,
    persistence: Decimal | None,
    latest_age_seconds: int,
    prediction_drop: Decimal,
) -> RejectionReason | None:
    if entry.predicted_funding_rate < config.funding_threshold:
        return RejectionReason.FUNDING_BELOW_THRESHOLD
    if metrics.history_duration < config.minimum_history_minutes * 60:
        return RejectionReason.INSUFFICIENT_HISTORY
    if latest_age_seconds > config.maximum_snapshot_age_seconds:
        return RejectionReason.STALE_SNAPSHOTS
    if persistence is not None and persistence < config.minimum_persistence_ratio:
        return RejectionReason.PERSISTENCE_TOO_LOW
    if metrics.std_rate is not None and metrics.std_rate > config.maximum_funding_std:
        return RejectionReason.VOLATILITY_TOO_HIGH
    if prediction_drop > config.maximum_prediction_drop:
        return RejectionReason.PREDICTION_DROP_TOO_HIGH
    return None


def _latest_at_or_before(
    snapshots: Sequence[FundingSnapshot],
    target: datetime,
) -> FundingSnapshot | None:
    candidates = [
        snapshot
        for snapshot in snapshots
        if ensure_utc(snapshot.event_time) <= ensure_utc(target)
    ]
    return candidates[-1] if candidates else None


def _persistence_ratio(
    snapshots: Sequence[FundingSnapshot],
    threshold: Decimal,
) -> Decimal | None:
    if not snapshots:
        return None
    above = sum(1 for snapshot in snapshots if snapshot.predicted_funding_rate >= threshold)
    return Decimal(above) / Decimal(len(snapshots))


def _threshold_crossings(
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


def _continuous_signal_from_start(
    snapshots: Sequence[FundingSnapshot],
    signal_started_at: datetime | None,
    threshold: Decimal,
) -> bool:
    if signal_started_at is None:
        return False
    return all(
        snapshot.predicted_funding_rate >= threshold
        for snapshot in snapshots
        if ensure_utc(snapshot.event_time) >= ensure_utc(signal_started_at)
    )


def _late_spike(
    snapshots: Sequence[FundingSnapshot],
    persistence: Decimal | None,
    signal_started_at: datetime | None,
    entry_time: datetime,
    config: StrategyValidationConfig,
) -> bool:
    if signal_started_at is None or persistence is None:
        return False
    signal_age = int((ensure_utc(entry_time) - ensure_utc(signal_started_at)).total_seconds())
    return (
        persistence < config.minimum_persistence_ratio
        and signal_age <= max(60, config.signal_confirmation_minutes * 60)
    )


def _rejected(reason: RejectionReason) -> SignalDetection:
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
        rejection_reason=reason,
    )
