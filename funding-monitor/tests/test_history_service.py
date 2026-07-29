import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.history_service import (
    FundingHistoryService,
    WindowCache,
    calculate_funding_metrics,
)
from funding_monitor.models import FundingSnapshot, funding_direction_from_rate


def test_window_cache_initialization_and_multiple_symbols() -> None:
    cache = WindowCache(window_minutes=120)
    now = datetime(2024, 1, 1, 8, tzinfo=UTC)

    cache.load(
        [
            make_snapshot("BTCUSDT", now, "0.0001"),
            make_snapshot("ETHUSDT", now, "-0.0002"),
        ]
    )
    summary = cache.summary()

    assert summary.symbols_cached == 2
    assert summary.snapshots_in_cache == 2
    assert summary.cache_oldest == now
    assert summary.cache_newest == now
    assert summary.cache_memory_estimate_bytes > 0


def test_window_cache_prunes_old_snapshots() -> None:
    cache = WindowCache(window_minutes=120)
    now = datetime(2024, 1, 1, 8, tzinfo=UTC)

    cache.update(make_snapshot("BTCUSDT", now - timedelta(minutes=121), "0.0001"))
    cache.update(make_snapshot("BTCUSDT", now, "0.0002"))

    window = cache.get_window("BTCUSDT")

    assert [snapshot.funding_rate for snapshot in window] == [Decimal("0.0002")]


def test_history_service_reload_loads_repository_once_and_update_stays_in_memory() -> None:
    now = datetime(2024, 1, 1, 8, tzinfo=UTC)
    repository = FakeHistoryRepository(
        [make_snapshot("BTCUSDT", now, "0.0001")]
    )
    service = FundingHistoryService(
        repository=repository,
        window_cache_minutes=120,
        default_metrics_window=60,
        abs_threshold=Decimal("0.0003"),
    )

    async def scenario() -> None:
        await service.reload()
        service.update(make_snapshot("BTCUSDT", now + timedelta(minutes=1), "0.0002"))

    asyncio.run(scenario())

    assert repository.calls == 1
    assert service.summary().snapshots_in_cache == 2


def test_history_service_window_selection() -> None:
    now = datetime(2024, 1, 1, 8, tzinfo=UTC)
    service = FundingHistoryService(
        repository=FakeHistoryRepository([]),
        window_cache_minutes=120,
        default_metrics_window=60,
        abs_threshold=Decimal("0.0003"),
    )
    service.update(make_snapshot("BTCUSDT", now - timedelta(minutes=61), "0.0001"))
    service.update(make_snapshot("BTCUSDT", now - timedelta(minutes=59), "0.0002"))
    service.update(make_snapshot("BTCUSDT", now, "0.0003"))

    default_window = service.get_window("BTCUSDT")
    short_window = service.get_window("BTCUSDT", 1)

    assert [snapshot.funding_rate for snapshot in default_window] == [
        Decimal("0.0002"),
        Decimal("0.0003"),
    ]
    assert [snapshot.funding_rate for snapshot in short_window] == [Decimal("0.0003")]


def test_metrics_mean_median_std_and_persistence() -> None:
    now = datetime(2024, 1, 1, 8, tzinfo=UTC)
    metrics = calculate_funding_metrics(
        [
            make_snapshot("BTCUSDT", now, "0.0001"),
            make_snapshot("BTCUSDT", now + timedelta(minutes=1), "0.0003"),
            make_snapshot("BTCUSDT", now + timedelta(minutes=2), "0.0005"),
        ],
        abs_threshold=Decimal("0.0003"),
    )

    assert metrics.current_rate == Decimal("0.0005")
    assert metrics.mean_rate == Decimal("0.0003")
    assert metrics.median_rate == Decimal("0.0003")
    assert metrics.min_rate == Decimal("0.0001")
    assert metrics.max_rate == Decimal("0.0005")
    assert metrics.std_rate is not None
    assert metrics.std_rate > Decimal(0)
    assert metrics.absolute_mean_rate == Decimal("0.0003")
    assert metrics.threshold_persistence == Decimal(2) / Decimal(3)


def test_metrics_direction_changes_and_crossings() -> None:
    now = datetime(2024, 1, 1, 8, tzinfo=UTC)
    metrics = calculate_funding_metrics(
        [
            make_snapshot("BTCUSDT", now, "0.0001"),
            make_snapshot("BTCUSDT", now + timedelta(minutes=1), "0.0004"),
            make_snapshot("BTCUSDT", now + timedelta(minutes=2), "-0.0004"),
            make_snapshot("BTCUSDT", now + timedelta(minutes=3), "-0.0001"),
        ],
        abs_threshold=Decimal("0.0003"),
    )

    assert metrics.threshold_crossings == 2
    assert metrics.positive_crossings == 1
    assert metrics.negative_crossings == 1
    assert metrics.direction_changes == 1
    assert metrics.current_direction == "negative"


def test_metrics_velocity_acceleration_and_deltas() -> None:
    now = datetime(2024, 1, 1, 8, tzinfo=UTC)
    metrics = calculate_funding_metrics(
        [
            make_snapshot("BTCUSDT", now, "0.0001"),
            make_snapshot("BTCUSDT", now + timedelta(minutes=1), "0.0002"),
            make_snapshot("BTCUSDT", now + timedelta(minutes=2), "0.0005"),
        ],
        abs_threshold=Decimal("0.0003"),
    )

    assert metrics.delta_1m == Decimal("0.0003")
    assert metrics.rate_velocity == Decimal("0.0004") / Decimal(120)
    assert metrics.rate_acceleration == (
        Decimal("0.0003") / Decimal(60) - Decimal("0.0001") / Decimal(60)
    )


def test_metrics_empty_history() -> None:
    metrics = calculate_funding_metrics([], abs_threshold=Decimal("0.0003"))

    assert metrics.current_rate is None
    assert metrics.snapshot_count == 0
    assert metrics.threshold_persistence == Decimal(0)
    assert metrics.rate_velocity is None
    assert metrics.rate_acceleration is None


def test_metrics_single_value() -> None:
    now = datetime(2024, 1, 1, 8, tzinfo=UTC)
    metrics = calculate_funding_metrics(
        [make_snapshot("BTCUSDT", now, "0.0004")],
        abs_threshold=Decimal("0.0003"),
    )

    assert metrics.current_rate == Decimal("0.0004")
    assert metrics.mean_rate == Decimal("0.0004")
    assert metrics.median_rate == Decimal("0.0004")
    assert metrics.std_rate == Decimal(0)
    assert metrics.threshold_persistence == Decimal(1)
    assert metrics.history_duration == 0
    assert metrics.snapshot_count == 1


def make_snapshot(symbol: str, event_time: datetime, rate: str) -> FundingSnapshot:
    funding_rate = Decimal(rate)
    return FundingSnapshot(
        symbol=symbol,
        event_time=event_time,
        received_at=event_time,
        mark_price=Decimal(101),
        index_price=Decimal(100),
        estimated_settle_price=None,
        predicted_funding_rate=funding_rate,
        funding_rate=funding_rate,
        interest_rate=None,
        next_funding_time=event_time + timedelta(hours=1),
        seconds_until_funding=3600,
        seconds_to_funding=3600,
        premium_rate=Decimal("0.01"),
        funding_direction=funding_direction_from_rate(funding_rate),
        funding_interval_hours=8,
        capture_mode="normal",
    )


class FakeHistoryRepository:
    def __init__(self, snapshots: list[FundingSnapshot]) -> None:
        self.snapshots = snapshots
        self.calls = 0

    async def recent_snapshots(self, minutes: int) -> list[FundingSnapshot]:
        self.calls += 1
        return self.snapshots
