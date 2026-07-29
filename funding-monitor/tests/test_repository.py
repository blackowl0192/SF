import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.models import FundingSnapshot
from funding_monitor.repository import (
    CONFIRM_EVENT_SQL,
    INSERT_SNAPSHOT_SQL,
    UPDATE_EVENT_PREDICTIONS_SQL,
    UPDATE_NEXT_PREDICTED_RATE_SQL,
    UPSERT_FUNDING_EVENT_SQL,
    UPSERT_SYMBOLS_SQL,
    FundingRepository,
)


def test_repository_uses_postgresql_placeholders() -> None:
    sql = (
        f"{UPSERT_SYMBOLS_SQL}\n"
        f"{INSERT_SNAPSHOT_SQL}\n"
        f"{UPSERT_FUNDING_EVENT_SQL}\n"
        f"{UPDATE_EVENT_PREDICTIONS_SQL}\n"
        f"{CONFIRM_EVENT_SQL}\n"
        f"{UPDATE_NEXT_PREDICTED_RATE_SQL}"
    )

    assert "?" not in sql
    assert "$1" in sql


def test_snapshot_insert_does_not_depend_on_instrument_mappings() -> None:
    assert "instrument_mappings" not in INSERT_SNAPSHOT_SQL


def test_on_conflict_keeps_old_semantics() -> None:
    assert "ON CONFLICT(symbol, event_time, capture_mode) DO NOTHING" in (
        INSERT_SNAPSHOT_SQL
    )
    assert "ON CONFLICT(symbol) DO UPDATE" in UPSERT_SYMBOLS_SQL
    assert "created_at = symbols.created_at" in UPSERT_SYMBOLS_SQL
    assert "ON CONFLICT(symbol, funding_time) DO UPDATE" in UPSERT_FUNDING_EVENT_SQL
    assert "COALESCE(" in UPSERT_FUNDING_EVENT_SQL
    assert "last_predicted_rate = excluded.last_predicted_rate" in (
        UPSERT_FUNDING_EVENT_SQL
    )


def test_repository_passes_decimal_and_utc_datetime_without_float_conversion() -> None:
    connection = RecordingConnection()
    repository = FundingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]
    funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)
    snapshot = make_snapshot(
        funding_time=funding_time,
        seconds_before=1200,
        rate=Decimal("0.00010000"),
    )

    inserted = asyncio.run(repository.insert_snapshot(snapshot))

    assert inserted
    assert len(connection.args) == 16
    assert isinstance(connection.args[3], Decimal)
    assert isinstance(connection.args[6], Decimal)
    assert connection.args[3] == Decimal("101.0")
    assert connection.args[7] == Decimal("0.00010000")
    assert connection.args[11] == 1200
    assert connection.args[12] == Decimal("0.01")
    assert connection.args[13] == "positive"
    assert connection.args[14] == 8
    assert connection.args[1].tzinfo is UTC
    assert connection.args[9].tzinfo is UTC


def test_repository_maps_new_snapshot_columns_from_rows() -> None:
    repository = FundingRepository(RecordingDatabase(RecordingConnection()))  # type: ignore[arg-type]
    funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)

    snapshot = repository._row_to_snapshot(
        {
            "symbol": "BTCUSDT",
            "event_time": funding_time - timedelta(minutes=20),
            "received_at": funding_time - timedelta(minutes=20),
            "mark_price": Decimal("101.0"),
            "index_price": Decimal("100.0"),
            "estimated_settle_price": None,
            "predicted_funding_rate": Decimal("-0.00040000"),
            "funding_rate": Decimal("-0.00040000"),
            "interest_rate": None,
            "next_funding_time": funding_time,
            "seconds_until_funding": 1200,
            "seconds_to_funding": 1200,
            "premium_rate": Decimal("0.01"),
            "funding_direction": "negative",
            "funding_interval_hours": 4,
            "capture_mode": "normal",
        }
    )

    assert snapshot.funding_rate == Decimal("-0.00040000")
    assert snapshot.seconds_to_funding == 1200
    assert snapshot.premium_rate == Decimal("0.01")
    assert snapshot.funding_direction == "negative"
    assert snapshot.funding_interval_hours == 4


def test_status_summary_returns_snapshot_analytics() -> None:
    connection = StatusConnection()
    repository = FundingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]

    summary = asyncio.run(repository.status_summary(Decimal("0.0003")))

    assert connection.status_threshold == Decimal("0.0003")
    assert summary["active_symbols"] == 529
    assert summary["snapshot_count"] == 10
    assert summary["event_count"] == 2
    assert summary["positive_snapshots"] == 4
    assert summary["negative_snapshots"] == 3
    assert summary["neutral_snapshots"] == 3
    assert summary["snapshots_above_abs_threshold"] == 5
    assert summary["snapshots_below_abs_threshold"] == 5
    assert summary["waiting"] == 1
    assert summary["confirmed"] == 1


def test_snapshot_stats_returns_aggregates_with_optional_time_window() -> None:
    connection = SnapshotStatsConnection()
    repository = FundingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]

    stats = asyncio.run(
        repository.snapshot_stats(abs_threshold=Decimal("0.0003"), minutes=60)
    )

    assert connection.args == (Decimal("0.0003"), 60)
    assert "make_interval" in connection.sql
    assert stats["total_snapshots"] == 10
    assert stats["symbols_represented"] == 2
    assert stats["positive_count"] == 4
    assert stats["negative_count"] == 3
    assert stats["neutral_count"] == 3
    assert stats["above_threshold_count"] == 5
    assert stats["below_threshold_count"] == 5
    assert stats["average_absolute_funding_rate"] == Decimal("0.00025")


def test_recent_snapshots_loads_window_from_postgresql() -> None:
    connection = RecentSnapshotsConnection()
    repository = FundingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]

    snapshots = asyncio.run(repository.recent_snapshots(120))

    assert connection.args == (120,)
    assert "make_interval" in connection.sql
    assert len(snapshots) == 1
    assert snapshots[0].symbol == "BTCUSDT"
    assert snapshots[0].funding_rate == Decimal("0.0004")


def test_snapshots_below_threshold_continue_saving() -> None:
    connection = RecordingConnection()
    repository = FundingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]
    snapshot = make_snapshot(
        funding_time=datetime(2024, 1, 1, 8, tzinfo=UTC),
        seconds_before=1200,
        rate=Decimal("0.000299"),
    )

    inserted = asyncio.run(repository.insert_snapshot(snapshot))

    assert inserted
    assert connection.args[7] == Decimal("0.000299")


def test_negative_funding_values_are_saved() -> None:
    connection = RecordingConnection()
    repository = FundingRepository(RecordingDatabase(connection))  # type: ignore[arg-type]
    snapshot = make_snapshot(
        funding_time=datetime(2024, 1, 1, 8, tzinfo=UTC),
        seconds_before=1200,
        rate=Decimal("-0.000300"),
    )

    inserted = asyncio.run(repository.insert_snapshot(snapshot))

    assert inserted
    assert connection.args[7] == Decimal("-0.000300")
    assert connection.args[13] == "negative"


def make_snapshot(
    *, funding_time: datetime, seconds_before: int, rate: Decimal
) -> FundingSnapshot:
    event_time = funding_time - timedelta(seconds=seconds_before)
    return FundingSnapshot(
        symbol="BTCUSDT",
        event_time=event_time,
        received_at=event_time,
        mark_price=Decimal("101.0"),
        index_price=Decimal("100.0"),
        estimated_settle_price=None,
        predicted_funding_rate=rate,
        funding_rate=rate,
        interest_rate=None,
        next_funding_time=funding_time,
        seconds_until_funding=seconds_before,
        seconds_to_funding=max(0, seconds_before),
        premium_rate=Decimal("0.01"),
        funding_direction="positive" if rate > 0 else "negative",
        funding_interval_hours=8,
        capture_mode="normal",
    )


class RecordingConnection:
    def __init__(self) -> None:
        self.args = ()

    async def fetchrow(self, _sql, *args):
        self.args = args
        return {"id": 1}


class StatusConnection:
    def __init__(self) -> None:
        self.fetchval_calls = 0
        self.status_threshold = None

    async def fetchval(self, _sql, *args):
        values = [
            529,
            10,
            2,
            datetime(2024, 1, 1, 7, 59, tzinfo=UTC),
        ]
        value = values[self.fetchval_calls]
        self.fetchval_calls += 1
        return value

    async def fetchrow(self, _sql, *args):
        self.status_threshold = args[0]
        return {
            "positive_snapshots": 4,
            "negative_snapshots": 3,
            "neutral_snapshots": 3,
            "snapshots_above_abs_threshold": 5,
            "snapshots_below_abs_threshold": 5,
            "next_funding_time_min": datetime(2024, 1, 1, 8, tzinfo=UTC),
            "latest_received_at": datetime(2024, 1, 1, 7, 59, tzinfo=UTC),
        }

    async def fetch(self, _sql, *args):
        return [{"status": "waiting", "count": 1}, {"status": "confirmed", "count": 1}]


class SnapshotStatsConnection:
    def __init__(self) -> None:
        self.sql = ""
        self.args = ()

    async def fetchrow(self, sql, *args):
        self.sql = sql
        self.args = args
        return {
            "total_snapshots": 10,
            "symbols_represented": 2,
            "positive_count": 4,
            "negative_count": 3,
            "neutral_count": 3,
            "above_threshold_count": 5,
            "below_threshold_count": 5,
            "min_funding_rate": Decimal("-0.0004"),
            "max_funding_rate": Decimal("0.0005"),
            "average_absolute_funding_rate": Decimal("0.00025"),
            "earliest_next_funding": datetime(2024, 1, 1, 8, tzinfo=UTC),
            "latest_next_funding": datetime(2024, 1, 1, 16, tzinfo=UTC),
            "newest_snapshot": datetime(2024, 1, 1, 7, 59, tzinfo=UTC),
            "oldest_snapshot": datetime(2024, 1, 1, 7, 1, tzinfo=UTC),
        }


class RecentSnapshotsConnection:
    def __init__(self) -> None:
        self.sql = ""
        self.args = ()

    async def fetch(self, sql, *args):
        self.sql = sql
        self.args = args
        event_time = datetime(2024, 1, 1, 8, tzinfo=UTC)
        return [
            {
                "symbol": "BTCUSDT",
                "event_time": event_time,
                "received_at": event_time,
                "mark_price": Decimal("101.0"),
                "index_price": Decimal("100.0"),
                "estimated_settle_price": None,
                "predicted_funding_rate": Decimal("0.0004"),
                "funding_rate": Decimal("0.0004"),
                "interest_rate": None,
                "next_funding_time": event_time + timedelta(hours=1),
                "seconds_until_funding": 3600,
                "seconds_to_funding": 3600,
                "premium_rate": Decimal("0.01"),
                "funding_direction": "positive",
                "funding_interval_hours": 8,
                "capture_mode": "normal",
            }
        ]


class RecordingDatabase:
    def __init__(self, connection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection
