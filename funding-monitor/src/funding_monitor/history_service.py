from __future__ import annotations

import sys
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from typing import Protocol

from .models import FundingDirection, FundingSnapshot, ensure_utc

SUPPORTED_WINDOWS_MINUTES = (1, 3, 5, 10, 15, 30, 60, 120)


class SnapshotHistoryRepository(Protocol):
    async def recent_snapshots(self, minutes: int) -> list[FundingSnapshot]:
        ...


@dataclass(frozen=True)
class WindowCacheSummary:
    symbols_cached: int
    snapshots_in_cache: int
    cache_memory_estimate_bytes: int
    window_size_minutes: int
    cache_oldest: datetime | None
    cache_newest: datetime | None


@dataclass(frozen=True)
class FundingMetrics:
    current_rate: Decimal | None
    min_rate: Decimal | None
    max_rate: Decimal | None
    mean_rate: Decimal | None
    median_rate: Decimal | None
    std_rate: Decimal | None
    absolute_mean_rate: Decimal | None
    time_above_threshold: int
    threshold_crossings: int
    positive_crossings: int
    negative_crossings: int
    threshold_persistence: Decimal
    delta_1m: Decimal | None
    delta_5m: Decimal | None
    delta_15m: Decimal | None
    delta_30m: Decimal | None
    rate_velocity: Decimal | None
    rate_acceleration: Decimal | None
    current_direction: FundingDirection | None
    direction_changes: int
    history_duration: int
    snapshot_count: int
    earliest_snapshot: datetime | None
    latest_snapshot: datetime | None


class WindowCache:
    def __init__(self, *, window_minutes: int) -> None:
        if window_minutes <= 0:
            raise ValueError("window_minutes must be positive")
        self.window_minutes = window_minutes
        self._snapshots: dict[str, deque[FundingSnapshot]] = {}

    def clear(self) -> None:
        self._snapshots.clear()

    def load(self, snapshots: Iterable[FundingSnapshot]) -> None:
        self.clear()
        for snapshot in sorted(snapshots, key=lambda item: item.event_time):
            self.update(snapshot)

    def update(self, snapshot: FundingSnapshot) -> None:
        queue = self._snapshots.setdefault(snapshot.symbol, deque())
        queue.append(snapshot)
        if len(queue) > 1 and queue[-2].event_time > snapshot.event_time:
            self._snapshots[snapshot.symbol] = deque(
                sorted(queue, key=lambda item: item.event_time)
            )
        self._prune_symbol(snapshot.symbol)

    def get_window(
        self, symbol: str, window_minutes: int | None = None
    ) -> list[FundingSnapshot]:
        queue = self._snapshots.get(symbol)
        if not queue:
            return []
        minutes = window_minutes or self.window_minutes
        newest = ensure_utc(queue[-1].event_time)
        cutoff = newest - timedelta(minutes=minutes)
        return [snapshot for snapshot in queue if ensure_utc(snapshot.event_time) >= cutoff]

    def summary(self) -> WindowCacheSummary:
        all_snapshots = [
            snapshot for snapshots in self._snapshots.values() for snapshot in snapshots
        ]
        oldest = min((snapshot.event_time for snapshot in all_snapshots), default=None)
        newest = max((snapshot.event_time for snapshot in all_snapshots), default=None)
        return WindowCacheSummary(
            symbols_cached=len(self._snapshots),
            snapshots_in_cache=len(all_snapshots),
            cache_memory_estimate_bytes=self.memory_estimate_bytes(),
            window_size_minutes=self.window_minutes,
            cache_oldest=ensure_utc(oldest) if oldest is not None else None,
            cache_newest=ensure_utc(newest) if newest is not None else None,
        )

    def memory_estimate_bytes(self) -> int:
        estimate = sys.getsizeof(self._snapshots)
        for symbol, snapshots in self._snapshots.items():
            estimate += sys.getsizeof(symbol)
            estimate += sys.getsizeof(snapshots)
            estimate += sum(sys.getsizeof(snapshot) for snapshot in snapshots)
        return estimate

    def _prune_symbol(self, symbol: str) -> None:
        queue = self._snapshots[symbol]
        newest = ensure_utc(queue[-1].event_time)
        cutoff = newest - timedelta(minutes=self.window_minutes)
        while queue and ensure_utc(queue[0].event_time) < cutoff:
            queue.popleft()
        if not queue:
            del self._snapshots[symbol]


class FundingHistoryService:
    def __init__(
        self,
        *,
        repository: SnapshotHistoryRepository,
        window_cache_minutes: int,
        default_metrics_window: int,
        abs_threshold: Decimal,
    ) -> None:
        self.repository = repository
        self.window_cache_minutes = window_cache_minutes
        self.default_metrics_window = default_metrics_window
        self.abs_threshold = abs_threshold
        self.cache = WindowCache(window_minutes=window_cache_minutes)

    async def reload(self) -> None:
        snapshots = await self.repository.recent_snapshots(self.window_cache_minutes)
        self.cache.load(snapshots)

    def update(self, snapshot: FundingSnapshot) -> None:
        self.cache.update(snapshot)

    def get_window(
        self, symbol: str, window_minutes: int | None = None
    ) -> list[FundingSnapshot]:
        return self.cache.get_window(symbol, window_minutes or self.default_metrics_window)

    def get_metrics(
        self, symbol: str, window_minutes: int | None = None
    ) -> FundingMetrics:
        return calculate_funding_metrics(
            self.get_window(symbol, window_minutes),
            abs_threshold=self.abs_threshold,
        )

    def summary(self) -> WindowCacheSummary:
        return self.cache.summary()


def calculate_funding_metrics(
    snapshots: Iterable[FundingSnapshot], *, abs_threshold: Decimal
) -> FundingMetrics:
    ordered = sorted(snapshots, key=lambda item: item.event_time)
    if not ordered:
        return FundingMetrics(
            current_rate=None,
            min_rate=None,
            max_rate=None,
            mean_rate=None,
            median_rate=None,
            std_rate=None,
            absolute_mean_rate=None,
            time_above_threshold=0,
            threshold_crossings=0,
            positive_crossings=0,
            negative_crossings=0,
            threshold_persistence=Decimal(0),
            delta_1m=None,
            delta_5m=None,
            delta_15m=None,
            delta_30m=None,
            rate_velocity=None,
            rate_acceleration=None,
            current_direction=None,
            direction_changes=0,
            history_duration=0,
            snapshot_count=0,
            earliest_snapshot=None,
            latest_snapshot=None,
        )

    rates = [snapshot.funding_rate for snapshot in ordered]
    earliest = ensure_utc(ordered[0].event_time)
    latest = ensure_utc(ordered[-1].event_time)
    above_count = sum(1 for rate in rates if abs(rate) >= abs_threshold)
    return FundingMetrics(
        current_rate=rates[-1],
        min_rate=min(rates),
        max_rate=max(rates),
        mean_rate=_mean(rates),
        median_rate=_median(rates),
        std_rate=_std(rates),
        absolute_mean_rate=_mean([abs(rate) for rate in rates]),
        time_above_threshold=_time_above_threshold(ordered, abs_threshold),
        threshold_crossings=_threshold_crossings(ordered, abs_threshold),
        positive_crossings=_positive_crossings(ordered, abs_threshold),
        negative_crossings=_negative_crossings(ordered, abs_threshold),
        threshold_persistence=Decimal(above_count) / Decimal(len(rates)),
        delta_1m=_delta_since(ordered, 1),
        delta_5m=_delta_since(ordered, 5),
        delta_15m=_delta_since(ordered, 15),
        delta_30m=_delta_since(ordered, 30),
        rate_velocity=_rate_velocity(ordered),
        rate_acceleration=_rate_acceleration(ordered),
        current_direction=ordered[-1].funding_direction,
        direction_changes=_direction_changes(ordered),
        history_duration=max(0, int((latest - earliest).total_seconds())),
        snapshot_count=len(ordered),
        earliest_snapshot=earliest,
        latest_snapshot=latest,
    )


def _mean(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _median(values: list[Decimal]) -> Decimal:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _std(values: list[Decimal]) -> Decimal:
    if len(values) == 1:
        return Decimal(0)
    mean = _mean(values)
    variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values))
    return variance.sqrt()


def _time_above_threshold(
    snapshots: list[FundingSnapshot], abs_threshold: Decimal
) -> int:
    total = 0
    for previous, current in pairwise(snapshots):
        if abs(previous.funding_rate) >= abs_threshold:
            seconds = int(
                (
                    ensure_utc(current.event_time) - ensure_utc(previous.event_time)
                ).total_seconds()
            )
            total += max(0, seconds)
    return total


def _threshold_crossings(
    snapshots: list[FundingSnapshot], abs_threshold: Decimal
) -> int:
    states = [abs(snapshot.funding_rate) >= abs_threshold for snapshot in snapshots]
    return sum(1 for previous, current in pairwise(states) if previous != current)


def _positive_crossings(
    snapshots: list[FundingSnapshot], abs_threshold: Decimal
) -> int:
    return _directional_threshold_crossings(
        snapshots,
        lambda rate: rate >= abs_threshold,
    )


def _negative_crossings(
    snapshots: list[FundingSnapshot], abs_threshold: Decimal
) -> int:
    return _directional_threshold_crossings(
        snapshots,
        lambda rate: rate <= -abs_threshold,
    )


def _directional_threshold_crossings(
    snapshots: list[FundingSnapshot],
    is_inside_threshold: Callable[[Decimal], bool],
) -> int:
    states = [is_inside_threshold(snapshot.funding_rate) for snapshot in snapshots]
    return sum(
        1
        for previous, current in pairwise(states)
        if not previous and current
    )


def _delta_since(
    snapshots: list[FundingSnapshot], window_minutes: int
) -> Decimal | None:
    current = snapshots[-1]
    cutoff = ensure_utc(current.event_time) - timedelta(minutes=window_minutes)
    candidates = [
        snapshot for snapshot in snapshots if ensure_utc(snapshot.event_time) <= cutoff
    ]
    if not candidates:
        return None
    return current.funding_rate - candidates[-1].funding_rate


def _rate_velocity(snapshots: list[FundingSnapshot]) -> Decimal:
    if len(snapshots) < 2:
        return Decimal(0)
    seconds = int(
        (
            ensure_utc(snapshots[-1].event_time) - ensure_utc(snapshots[0].event_time)
        ).total_seconds()
    )
    if seconds <= 0:
        return Decimal(0)
    return (snapshots[-1].funding_rate - snapshots[0].funding_rate) / Decimal(seconds)


def _rate_acceleration(snapshots: list[FundingSnapshot]) -> Decimal:
    if len(snapshots) < 3:
        return Decimal(0)
    previous = _interval_velocity(snapshots[-3], snapshots[-2])
    current = _interval_velocity(snapshots[-2], snapshots[-1])
    return current - previous


def _interval_velocity(
    previous: FundingSnapshot, current: FundingSnapshot
) -> Decimal:
    seconds = int(
        (
            ensure_utc(current.event_time) - ensure_utc(previous.event_time)
        ).total_seconds()
    )
    if seconds <= 0:
        return Decimal(0)
    return (current.funding_rate - previous.funding_rate) / Decimal(seconds)


def _direction_changes(snapshots: list[FundingSnapshot]) -> int:
    directions = [snapshot.funding_direction for snapshot in snapshots]
    return sum(
        1
        for previous, current in pairwise(directions)
        if previous != current
    )
