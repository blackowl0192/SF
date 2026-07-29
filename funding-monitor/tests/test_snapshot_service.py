from datetime import UTC, datetime, timedelta

from funding_monitor.snapshot_service import (
    SnapshotThrottler,
    determine_capture_mode,
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
