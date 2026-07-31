from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol

from .models import (
    FundingSnapshot,
    MarkPriceUpdate,
    calculate_premium_rate,
    calculate_seconds_to_funding,
    ensure_utc,
    funding_direction_from_rate,
    utc_now,
)

CaptureMode = str


class SnapshotBatchRepository(Protocol):
    async def insert_snapshots(
        self,
        snapshots: list[FundingSnapshot],
    ) -> list[FundingSnapshot]:
        ...


@dataclass(frozen=True)
class SnapshotPersistencePolicy:
    normal_interval_seconds: int
    detailed_interval_seconds: int

    def __post_init__(self) -> None:
        if self.normal_interval_seconds <= 0:
            raise ValueError("normal_interval_seconds must be positive")
        if self.detailed_interval_seconds <= 0:
            raise ValueError("detailed_interval_seconds must be positive")


@dataclass(frozen=True)
class SnapshotBatchFlushResult:
    attempted: int
    inserted: tuple[FundingSnapshot, ...]
    reason: str

    @property
    def inserted_count(self) -> int:
        return len(self.inserted)


class SnapshotBatchBuffer:
    def __init__(
        self,
        *,
        repository: SnapshotBatchRepository,
        batch_size: int,
        flush_interval_seconds: int,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if flush_interval_seconds <= 0:
            raise ValueError("flush_interval_seconds must be positive")
        self.repository = repository
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self._pending: list[FundingSnapshot] = []
        self._last_flush_at: datetime | None = None

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def add(self, snapshot: FundingSnapshot) -> bool:
        self._pending.append(snapshot)
        return len(self._pending) >= self.batch_size

    def should_flush(self, now: datetime) -> bool:
        if not self._pending:
            return False
        if self._last_flush_at is None:
            first_pending_at = min(item.received_at for item in self._pending)
            return (
                ensure_utc(now) - ensure_utc(first_pending_at)
            ).total_seconds() >= self.flush_interval_seconds
        return (
            ensure_utc(now) - ensure_utc(self._last_flush_at)
        ).total_seconds() >= self.flush_interval_seconds

    async def flush(self, *, reason: str) -> SnapshotBatchFlushResult:
        if not self._pending:
            self._last_flush_at = utc_now()
            return SnapshotBatchFlushResult(attempted=0, inserted=(), reason=reason)
        rows = list(self._pending)
        inserted = await self.repository.insert_snapshots(rows)
        del self._pending[: len(rows)]
        self._last_flush_at = utc_now()
        return SnapshotBatchFlushResult(
            attempted=len(rows),
            inserted=tuple(inserted),
            reason=reason,
        )


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
        self.policy = SnapshotPersistencePolicy(
            normal_interval_seconds=normal_interval_seconds,
            detailed_interval_seconds=detailed_interval_seconds,
        )
        self._last_saved: dict[tuple[str, str], datetime] = {}

    def should_save(
        self, symbol: str, event_time: datetime, capture_mode: CaptureMode
    ) -> bool:
        interval = (
            self.policy.normal_interval_seconds
            if capture_mode == "normal"
            else self.policy.detailed_interval_seconds
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
