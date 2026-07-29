import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.funding_event_service import (
    ConfirmationRequest,
    FundingConfirmationScheduler,
    FundingEventService,
)
from funding_monitor.models import (
    FundingEvent,
    FundingSnapshot,
    calculate_premium_rate,
    funding_direction_from_rate,
    utc_datetime_to_millis,
)
from funding_monitor.snapshot_service import select_checkpoint_rates


def make_snapshot(
    funding_time: datetime,
    seconds_before: int,
    rate: str,
) -> FundingSnapshot:
    event_time = funding_time - timedelta(seconds=seconds_before)
    funding_rate = Decimal(rate)
    mark_price = Decimal("43000.0")
    index_price = Decimal("42990.0")
    return FundingSnapshot(
        symbol="BTCUSDT",
        event_time=event_time,
        received_at=event_time,
        mark_price=mark_price,
        index_price=index_price,
        estimated_settle_price=None,
        predicted_funding_rate=funding_rate,
        funding_rate=funding_rate,
        interest_rate=None,
        next_funding_time=funding_time,
        seconds_until_funding=seconds_before,
        seconds_to_funding=max(0, seconds_before),
        premium_rate=calculate_premium_rate(mark_price, index_price),
        funding_direction=funding_direction_from_rate(funding_rate),
        funding_interval_hours=8,
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


def test_funding_event_service_creates_event_and_updates_checkpoints() -> None:
    async def scenario() -> None:
        repository = InMemoryRepository()
        service = FundingEventService(repository)  # type: ignore[arg-type]
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

    asyncio.run(scenario())


def test_prediction_error_is_actual_minus_last_predicted() -> None:
    async def scenario() -> None:
        repository = InMemoryRepository()
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

    asyncio.run(scenario())


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


def test_confirmation_retries_without_duplicate_events() -> None:
    async def scenario() -> None:
        repository = InMemoryRepository()
        funding_time = datetime(2024, 1, 1, 8, tzinfo=UTC)
        fake_rest = FakeFundingRateRestClient(funding_time)

        await repository.create_or_get_funding_event(
            "BTCUSDT", funding_time, 8, Decimal("0.00010")
        )
        scheduler = FundingConfirmationScheduler(
            repository=repository,  # type: ignore[arg-type]
            rest_client=fake_rest,  # type: ignore[arg-type]
            initial_delay_seconds=0,
            retry_seconds=0,
            max_attempts=2,
        )

        await scheduler._run_confirmation(ConfirmationRequest("BTCUSDT", funding_time))
        await scheduler._run_confirmation(ConfirmationRequest("BTCUSDT", funding_time))
        event = await repository.get_funding_event("BTCUSDT", funding_time)
        summary = await repository.status_summary()

        assert fake_rest.calls == 2
        assert event is not None
        assert event.status == "confirmed"
        assert event.actual_funding_rate == Decimal("0.00020")
        assert summary["event_count"] == 1

    asyncio.run(scenario())


class InMemoryRepository:
    def __init__(self) -> None:
        self.snapshots: list[FundingSnapshot] = []
        self.events: dict[tuple[str, datetime], FundingEvent] = {}

    async def insert_snapshot(self, snapshot: FundingSnapshot) -> bool:
        key = (snapshot.symbol, snapshot.event_time, snapshot.capture_mode)
        if any(
            (item.symbol, item.event_time, item.capture_mode) == key
            for item in self.snapshots
        ):
            return False
        self.snapshots.append(snapshot)
        return True

    async def create_or_get_funding_event(
        self,
        symbol: str,
        funding_time: datetime,
        funding_interval_hours: int,
        first_predicted_rate: Decimal,
    ) -> FundingEvent:
        key = (symbol, funding_time)
        current = self.events.get(key)
        if current is None:
            current = FundingEvent(
                symbol=symbol,
                funding_time=funding_time,
                funding_interval_hours=funding_interval_hours,
                first_predicted_rate=first_predicted_rate,
                last_predicted_rate=first_predicted_rate,
            )
        else:
            current = replace(
                current,
                funding_interval_hours=funding_interval_hours,
                last_predicted_rate=first_predicted_rate,
            )
        self.events[key] = current
        return current

    async def get_funding_event(
        self, symbol: str, funding_time: datetime
    ) -> FundingEvent | None:
        return self.events.get((symbol, funding_time))

    async def update_event_predictions(
        self,
        symbol: str,
        funding_time: datetime,
        *,
        predicted_rate_10m_before: Decimal | None,
        predicted_rate_5m_before: Decimal | None,
        predicted_rate_1m_before: Decimal | None,
        last_predicted_rate: Decimal | None,
    ) -> None:
        key = (symbol, funding_time)
        self.events[key] = replace(
            self.events[key],
            predicted_rate_10m_before=predicted_rate_10m_before,
            predicted_rate_5m_before=predicted_rate_5m_before,
            predicted_rate_1m_before=predicted_rate_1m_before,
            last_predicted_rate=last_predicted_rate,
        )

    async def snapshots_for_event(
        self, symbol: str, funding_time: datetime
    ) -> list[FundingSnapshot]:
        return [
            snapshot
            for snapshot in self.snapshots
            if snapshot.symbol == symbol and snapshot.next_funding_time == funding_time
        ]

    async def mark_event_confirmed(
        self,
        symbol: str,
        funding_time: datetime,
        actual_funding_rate: Decimal,
        mark_price_at_funding: Decimal | None,
        confirmed_at: datetime,
    ) -> None:
        key = (symbol, funding_time)
        event = self.events[key]
        prediction_error = (
            actual_funding_rate - event.last_predicted_rate
            if event.last_predicted_rate is not None
            else None
        )
        self.events[key] = replace(
            event,
            actual_funding_rate=actual_funding_rate,
            prediction_error=prediction_error,
            mark_price_at_funding=mark_price_at_funding,
            confirmed_at=confirmed_at,
            status="confirmed",
        )

    async def mark_confirmation_failed(
        self, symbol: str, funding_time: datetime
    ) -> None:
        key = (symbol, funding_time)
        event = self.events[key]
        if event.status != "confirmed":
            self.events[key] = replace(event, status="confirmation_failed")

    async def update_next_predicted_rate(
        self,
        symbol: str,
        previous_funding_time: datetime,
        next_predicted_rate: Decimal,
    ) -> None:
        key = (symbol, previous_funding_time)
        event = self.events[key]
        self.events[key] = replace(
            event,
            next_predicted_rate=event.next_predicted_rate or next_predicted_rate,
        )

    async def status_summary(self):
        return {"event_count": len(self.events)}
