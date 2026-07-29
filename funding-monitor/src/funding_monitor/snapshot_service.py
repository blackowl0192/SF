from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from .models import (
    FundingSnapshot,
    MarkPriceUpdate,
    calculate_premium_rate,
    calculate_seconds_to_funding,
    funding_direction_from_rate,
    utc_now,
)

CaptureMode = str


def determine_capture_mode(
    seconds_until_funding: int,
    *,
    before_seconds: int,
    after_seconds: int,
) -> CaptureMode | None:
    if seconds_until_funding > before_seconds:
        return "normal"
    if 0 <= seconds_until_funding <= before_seconds:
        return "pre_funding"
    if -after_seconds <= seconds_until_funding < 0:
        return "post_funding"
    return None


class SnapshotThrottler:
    def __init__(
        self,
        *,
        normal_interval_seconds: int,
        detailed_interval_seconds: int,
    ) -> None:
        self.normal_interval_seconds = normal_interval_seconds
        self.detailed_interval_seconds = detailed_interval_seconds
        self._last_saved: dict[tuple[str, str], datetime] = {}

    def should_save(
        self, symbol: str, event_time: datetime, capture_mode: CaptureMode
    ) -> bool:
        interval = (
            self.normal_interval_seconds
            if capture_mode == "normal"
            else self.detailed_interval_seconds
        )
        key = (symbol, capture_mode)
        previous = self._last_saved.get(key)
        if previous is None:
            self._last_saved[key] = event_time
            return True
        if (event_time - previous).total_seconds() >= interval:
            self._last_saved[key] = event_time
            return True
        return False


def snapshot_from_update(
    update: MarkPriceUpdate,
    *,
    capture_mode: CaptureMode,
    funding_interval_hours: int,
    received_at: datetime | None = None,
) -> FundingSnapshot:
    funding_rate = update.predicted_funding_rate
    return FundingSnapshot(
        symbol=update.symbol,
        event_time=update.event_time,
        received_at=received_at or utc_now(),
        mark_price=update.mark_price,
        index_price=update.index_price,
        estimated_settle_price=update.estimated_settle_price,
        predicted_funding_rate=update.predicted_funding_rate,
        funding_rate=funding_rate,
        interest_rate=update.interest_rate,
        next_funding_time=update.next_funding_time,
        seconds_until_funding=update.seconds_until_funding,
        seconds_to_funding=calculate_seconds_to_funding(
            update.event_time, update.next_funding_time
        ),
        premium_rate=calculate_premium_rate(update.mark_price, update.index_price),
        funding_direction=funding_direction_from_rate(funding_rate),
        funding_interval_hours=funding_interval_hours,
        capture_mode=capture_mode,
    )


@dataclass(frozen=True)
class CheckpointRates:
    predicted_rate_10m_before: Decimal | None
    predicted_rate_5m_before: Decimal | None
    predicted_rate_1m_before: Decimal | None
    last_predicted_rate: Decimal | None


def select_checkpoint_rates(
    snapshots: list[FundingSnapshot], funding_time: datetime
) -> CheckpointRates:
    if not snapshots:
        return CheckpointRates(None, None, None, None)

    ordered = sorted(snapshots, key=lambda item: item.event_time)
    return CheckpointRates(
        predicted_rate_10m_before=_nearest_rate(
            ordered, funding_time - timedelta(minutes=10)
        ),
        predicted_rate_5m_before=_nearest_rate(
            ordered, funding_time - timedelta(minutes=5)
        ),
        predicted_rate_1m_before=_nearest_rate(
            ordered, funding_time - timedelta(minutes=1)
        ),
        last_predicted_rate=_last_before_or_at(ordered, funding_time),
    )


def _nearest_rate(
    snapshots: list[FundingSnapshot], target_time: datetime
) -> Decimal | None:
    if not snapshots:
        return None
    closest = min(
        snapshots,
        key=lambda item: abs((item.event_time - target_time).total_seconds()),
    )
    return closest.predicted_funding_rate


def _last_before_or_at(
    snapshots: list[FundingSnapshot], funding_time: datetime
) -> Decimal | None:
    candidates = [item for item in snapshots if item.event_time <= funding_time]
    if candidates:
        return candidates[-1].predicted_funding_rate
    return snapshots[0].predicted_funding_rate
