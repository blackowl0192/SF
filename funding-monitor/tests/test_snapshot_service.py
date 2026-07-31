from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.models import MarkPriceUpdate
from funding_monitor.snapshot_service import (
    SnapshotBatchBuffer,
    SnapshotPersistencePolicy,
    SnapshotThrottler,
    determine_capture_mode,
    snapshot_from_update,
)


def test_determine_capture_mode() -> None:
    assert (
        determine_capture_mode(601, before_seconds=600, after_seconds=300)
        == "normal"
    )
    assert (
        determine_capture_mode(600, before_seconds=600, after_seconds=300)
        == "pre_funding"
    )
    assert (
        determine_capture_mode(0, before_seconds=600, after_seconds=300)
        == "pre_funding"
    )
    assert (
        determine_capture_mode(-1, before_seconds=600, after_seconds=300)
        == "post_funding"
    )
    assert (
        determine_capture_mode(-300, before_seconds=600, after_seconds=300)
        == "post_funding"
    )
    assert determine_capture_mode(-301, before_seconds=600, after_seconds=300) is None


def test_snapshot_throttling_by_symbol_and_mode() -> None:
    base_time = datetime(2024, 1, 1, tzinfo=UTC)
    throttler = SnapshotThrottler(
        normal_interval_seconds=60,
        detailed_interval_seconds=1,
    )

    assert throttler.should_save("BTCUSDT", base_time, "normal")
    assert not throttler.should_save(
        "BTCUSDT", base_time + timedelta(seconds=30), "normal"
    )
    assert throttler.should_save(
        "BTCUSDT", base_time + timedelta(seconds=60), "normal"
    )

    assert throttler.should_save("BTCUSDT", base_time, "pre_funding")
    assert not throttler.should_save(
        "BTCUSDT", base_time + timedelta(milliseconds=500), "pre_funding"
    )
    assert throttler.should_save(
        "BTCUSDT", base_time + timedelta(seconds=1), "pre_funding"
    )
    assert throttler.should_save("ETHUSDT", base_time, "pre_funding")


def test_snapshot_persistence_policy_validates_positive_intervals() -> None:
    policy = SnapshotPersistencePolicy(
        normal_interval_seconds=60,
        detailed_interval_seconds=30,
    )

    assert policy.normal_interval_seconds == 60
    assert policy.detailed_interval_seconds == 30


def test_snapshot_batch_buffer_flushes_heartbeat_snapshot() -> None:
    async def scenario() -> None:
        repository = InMemorySnapshotBatchRepository()
        buffer = SnapshotBatchBuffer(
            repository=repository,
            batch_size=10,
            flush_interval_seconds=5,
        )
        funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)
        saved = snapshot_from_update(
            MarkPriceUpdate(
                symbol="BTCUSDT",
                event_time=funding_time - timedelta(minutes=10),
                mark_price=Decimal(100),
                index_price=Decimal(100),
                estimated_settle_price=None,
                predicted_funding_rate=Decimal("0.0003"),
                interest_rate=None,
                next_funding_time=funding_time,
            ),
            capture_mode="normal",
            funding_interval_hours=8,
            received_at=funding_time - timedelta(seconds=10),
        )

        assert not buffer.add(saved)
        assert buffer.should_flush(funding_time)
        result = await buffer.flush(reason="interval")

        assert result.attempted == 1
        assert result.inserted_count == 1
        assert result.reason == "interval"
        assert buffer.pending_count == 0
        assert repository.inserted == [saved]

    import asyncio

    asyncio.run(scenario())


def test_snapshot_batch_buffer_retains_pending_after_persist_error() -> None:
    async def scenario() -> None:
        repository = FlakySnapshotBatchRepository()
        buffer = SnapshotBatchBuffer(
            repository=repository,
            batch_size=10,
            flush_interval_seconds=5,
        )
        funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)
        saved = snapshot_from_update(
            MarkPriceUpdate(
                symbol="BTCUSDT",
                event_time=funding_time - timedelta(minutes=10),
                mark_price=Decimal(100),
                index_price=Decimal(100),
                estimated_settle_price=None,
                predicted_funding_rate=Decimal("0.0003"),
                interest_rate=None,
                next_funding_time=funding_time,
            ),
            capture_mode="normal",
            funding_interval_hours=8,
            received_at=funding_time,
        )

        buffer.add(saved)
        try:
            await buffer.flush(reason="interval")
        except RuntimeError:
            pass

        assert buffer.pending_count == 1
        result = await buffer.flush(reason="retry")

        assert result.inserted == (saved,)
        assert buffer.pending_count == 0

    import asyncio

    asyncio.run(scenario())


def test_snapshot_from_update_sets_analytics_fields() -> None:
    event_time = datetime(2024, 1, 1, 7, 59, tzinfo=UTC)
    update = MarkPriceUpdate(
        symbol="BTCUSDT",
        event_time=event_time,
        mark_price=Decimal(101),
        index_price=Decimal(100),
        estimated_settle_price=None,
        predicted_funding_rate=Decimal("-0.0003"),
        interest_rate=None,
        next_funding_time=event_time + timedelta(seconds=60),
    )

    snapshot = snapshot_from_update(
        update,
        capture_mode="pre_funding",
        funding_interval_hours=4,
        received_at=event_time,
    )

    assert snapshot.funding_rate == Decimal("-0.0003")
    assert snapshot.seconds_to_funding == 60
    assert snapshot.premium_rate == Decimal("0.01")
    assert snapshot.funding_direction == "negative"
    assert snapshot.funding_interval_hours == 4


class InMemorySnapshotBatchRepository:
    def __init__(self) -> None:
        self.inserted = []

    async def insert_snapshots(self, snapshots):
        self.inserted.extend(snapshots)
        return snapshots


class FlakySnapshotBatchRepository:
    def __init__(self) -> None:
        self.calls = 0

    async def insert_snapshots(self, snapshots):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary batch failure")
        return snapshots
