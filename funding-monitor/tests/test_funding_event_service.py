import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.database import initialize_database
from funding_monitor.funding_event_service import (
    ConfirmationRequest,
    FundingConfirmationScheduler,
    FundingEventService,
)
from funding_monitor.models import FundingSnapshot, utc_datetime_to_millis
from funding_monitor.repository import FundingRepository
from funding_monitor.snapshot_service import select_checkpoint_rates


def run(coro):
    return asyncio.run(coro)


def make_snapshot(
    funding_time: datetime,
    seconds_before: int,
    rate: str,
) -> FundingSnapshot:
    event_time = funding_time - timedelta(seconds=seconds_before)
    return FundingSnapshot(
        symbol="BTCUSDT",
        event_time=event_time,
        received_at=event_time,
        mark_price=Decimal("43000.0"),
        index_price=Decimal("42990.0"),
        estimated_settle_price=None,
        predicted_funding_rate=Decimal(rate),
        interest_rate=None,
        next_funding_time=funding_time,
        seconds_until_funding=seconds_before,
        capture_mode="pre_funding",
    )


def test_select_checkpoint_rates_uses_nearest_saved_snapshot() -> None:
    funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)
    snapshots = [
        make_snapshot(funding_time, 605, "0.00010"),
        make_snapshot(funding_time, 298, "0.00020"),
        make_snapshot(funding_time, 61, "0.00030"),
        make_snapshot(funding_time, 15, "0.00040"),
    ]

    checkpoints = select_checkpoint_rates(snapshots, funding_time)

    assert checkpoints.predicted_rate_10m_before == Decimal("0.00010")
    assert checkpoints.predicted_rate_5m_before == Decimal("0.00020")
    assert checkpoints.predicted_rate_1m_before == Decimal("0.00030")
    assert checkpoints.last_predicted_rate == Decimal("0.00040")


def test_funding_event_service_creates_event_and_updates_checkpoints(tmp_path) -> None:
    async def scenario() -> None:
        database_path = tmp_path / "funding.db"
        await initialize_database(database_path)
        repository = FundingRepository(database_path)
        service = FundingEventService(repository)
        funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)
        snapshots = [
            make_snapshot(funding_time, 605, "0.00010"),
            make_snapshot(funding_time, 298, "0.00020"),
            make_snapshot(funding_time, 61, "0.00030"),
            make_snapshot(funding_time, 15, "0.00040"),
        ]

        for snapshot in snapshots:
            await repository.insert_snapshot(snapshot)
            await service.observe_snapshot(snapshot, funding_interval_hours=8)

        event = await repository.get_funding_event("BTCUSDT", funding_time)
        summary = await repository.status_summary()

        assert event is not None
        assert event.status == "waiting"
        assert event.predicted_rate_10m_before == Decimal("0.00010")
        assert event.predicted_rate_5m_before == Decimal("0.00020")
        assert event.predicted_rate_1m_before == Decimal("0.00030")
        assert event.last_predicted_rate == Decimal("0.00040")
        assert summary["event_count"] == 1

    run(scenario())


def test_prediction_error_is_actual_minus_last_predicted(tmp_path) -> None:
    async def scenario() -> None:
        database_path = tmp_path / "funding.db"
        await initialize_database(database_path)
        repository = FundingRepository(database_path)
        funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)

        await repository.create_or_get_funding_event(
            "BTCUSDT", funding_time, 8, Decimal("0.00010")
        )
        await repository.update_event_predictions(
            "BTCUSDT",
            funding_time,
            predicted_rate_10m_before=None,
            predicted_rate_5m_before=None,
            predicted_rate_1m_before=None,
            last_predicted_rate=Decimal("0.00010"),
        )
        await repository.mark_event_confirmed(
            "BTCUSDT",
            funding_time,
            Decimal("0.00015"),
            Decimal("43001.0"),
            funding_time + timedelta(seconds=12),
        )

        event = await repository.get_funding_event("BTCUSDT", funding_time)

        assert event is not None
        assert event.status == "confirmed"
        assert event.prediction_error == Decimal("0.00005")

    run(scenario())


class FakeFundingRateRestClient:
    def __init__(self, funding_time: datetime) -> None:
        self.calls = 0
        self.funding_time = funding_time

    async def get_funding_rate_history(self, symbol, **kwargs):
        self.calls += 1
        if self.calls == 1:
            return []
        return [
            {
                "symbol": symbol,
                "fundingRate": "0.00020",
                "fundingTime": utc_datetime_to_millis(self.funding_time),
                "markPrice": "43002.0",
            }
        ]


def test_confirmation_retries_without_duplicate_events(tmp_path) -> None:
    async def scenario() -> None:
        database_path = tmp_path / "funding.db"
        await initialize_database(database_path)
        repository = FundingRepository(database_path)
        funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)
        fake_rest = FakeFundingRateRestClient(funding_time)

        await repository.create_or_get_funding_event(
            "BTCUSDT", funding_time, 8, Decimal("0.00010")
        )
        scheduler = FundingConfirmationScheduler(
            repository=repository,
            rest_client=fake_rest,  # type: ignore[arg-type]
            initial_delay_seconds=0,
            retry_seconds=0,
            max_attempts=2,
        )

        await scheduler._run_confirmation(
            ConfirmationRequest("BTCUSDT", funding_time)
        )
        await scheduler._run_confirmation(
            ConfirmationRequest("BTCUSDT", funding_time)
        )
        event = await repository.get_funding_event("BTCUSDT", funding_time)
        summary = await repository.status_summary()

        assert fake_rest.calls == 2
        assert event is not None
        assert event.status == "confirmed"
        assert event.actual_funding_rate == Decimal("0.00020")
        assert summary["event_count"] == 1

    run(scenario())
