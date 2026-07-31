import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.candidate_engine import CandidateEngineConfig
from funding_monitor.instrument_mapping import (
    InstrumentMapping,
    MappingReason,
    NegativeStrategyStatus,
    SpotMappingStatus,
)
from funding_monitor.models import FundingEvent, FundingSnapshot, utc_datetime_to_millis
from funding_monitor.pipeline import (
    CandidateEvaluationPipeline,
    ConfirmationBackfillService,
    FundingIntervalBackfillService,
)

NOW = datetime(2024, 1, 1, 7, 30, tzinfo=UTC)
FUNDING_TIME = datetime(2024, 1, 1, 8, tzinfo=UTC)


def test_candidate_pipeline_evaluates_and_persists_selected_symbols() -> None:
    async def scenario() -> None:
        candidate_repository = FakeCandidateRepository()
        pipeline = CandidateEvaluationPipeline(
            funding_repository=FakeFundingRepository(
                snapshots=[
                    snapshot("BTCUSDT", NOW - timedelta(minutes=15 - index), "0.0006")
                    for index in range(16)
                ]
            ),  # type: ignore[arg-type]
            mapping_repository=FakeMappingRepository(
                [mapping("BTCUSDT"), mapping("ETHUSDT")]
            ),  # type: ignore[arg-type]
            candidate_repository=candidate_repository,  # type: ignore[arg-type]
            settings=FakeSettings(),  # type: ignore[arg-type]
            config=CandidateEngineConfig(),
        )

        result = await pipeline.run(
            symbols=("BTCUSDT",),
            limit=1,
            evaluated_at=NOW,
        )

        assert result.evaluated == 1
        assert result.persisted == 1
        assert result.failed == 0
        assert candidate_repository.persisted_symbols == ["BTCUSDT"]

    asyncio.run(scenario())


def test_interval_backfill_is_idempotent_and_reports_insufficient_history() -> None:
    async def scenario() -> None:
        repository = FakeIntervalRepository(existing=True, snapshots=[])
        service = FundingIntervalBackfillService(
            repository=repository,  # type: ignore[arg-type]
            config=CandidateEngineConfig(),
        )

        result = await service.run(limit=1)

        assert result.processed == 1
        assert result.created == 0
        assert result.updated == 1
        assert result.persisted == 1
        assert result.insufficient_history == 1
        assert result.reasons == {"insufficient_history": 1}

    asyncio.run(scenario())


def test_confirmation_backfill_confirms_due_event() -> None:
    async def scenario() -> None:
        repository = FakeConfirmationRepository()
        service = ConfirmationBackfillService(
            repository=repository,  # type: ignore[arg-type]
            rest_client=FakeFundingRateRestClient(),  # type: ignore[arg-type]
        )

        result = await service.run(due_before=FUNDING_TIME, limit=10)

        assert result.checked == 1
        assert result.confirmed == 1
        assert result.not_found == 0
        assert repository.confirmed == [("BTCUSDT", FUNDING_TIME)]

    asyncio.run(scenario())


def mapping(symbol: str) -> InstrumentMapping:
    return InstrumentMapping(
        futures_symbol=symbol,
        futures_pair=symbol,
        futures_base_asset=symbol[:-4],
        futures_quote_asset="USDT",
        futures_margin_asset="USDT",
        futures_contract_type="PERPETUAL",
        futures_status="TRADING",
        spot_symbol=symbol,
        spot_base_asset=symbol[:-4],
        spot_quote_asset="USDT",
        spot_status="TRADING",
        spot_trading_allowed=True,
        spot_pair_exists=True,
        spot_mapping_status=SpotMappingStatus.MATCHED,
        mapping_reason=MappingReason.EXACT_BASE_ASSET_MATCH,
        positive_strategy_available=True,
        negative_strategy_available=False,
        negative_strategy_status=NegativeStrategyStatus.BORROW_CHECK_NOT_IMPLEMENTED,
        mapping_source="test",
        mapping_updated_at=NOW,
        created_at=NOW,
        updated_at=NOW,
    )


def snapshot(symbol: str, event_time: datetime, rate: str) -> FundingSnapshot:
    funding_rate = Decimal(rate)
    return FundingSnapshot(
        symbol=symbol,
        event_time=event_time,
        received_at=event_time,
        mark_price=Decimal(100),
        index_price=Decimal(100),
        estimated_settle_price=None,
        predicted_funding_rate=funding_rate,
        funding_rate=funding_rate,
        interest_rate=None,
        next_funding_time=FUNDING_TIME,
        seconds_until_funding=max(
            0,
            int((FUNDING_TIME - event_time).total_seconds()),
        ),
        seconds_to_funding=max(
            0,
            int((FUNDING_TIME - event_time).total_seconds()),
        ),
        premium_rate=Decimal(0),
        funding_direction="positive",
        funding_interval_hours=8,
        capture_mode="normal",
    )


class FakeSettings:
    window_cache_minutes = 120


class FakeFundingRepository:
    def __init__(self, snapshots: list[FundingSnapshot]) -> None:
        self.snapshots = snapshots

    async def recent_snapshots(self, _minutes):
        return self.snapshots


class FakeMappingRepository:
    def __init__(self, mappings: list[InstrumentMapping]) -> None:
        self.mappings = mappings

    async def list_mappings(self):
        return self.mappings


class FakeCandidateRepository:
    def __init__(self) -> None:
        self.persisted_symbols: list[str] = []

    async def upsert_evaluations(self, evaluations):
        rows = list(evaluations)
        self.persisted_symbols.extend(row.futures_symbol for row in rows)
        return len(rows)


class FakeIntervalRepository:
    def __init__(self, *, existing: bool, snapshots: list[FundingSnapshot]) -> None:
        self.existing = existing
        self.snapshots = snapshots
        self.upserted = 0

    async def confirmed_events_for_interval_summaries(self, _limit, **_kwargs):
        return [
            FundingEvent(
                symbol="BTCUSDT",
                funding_time=FUNDING_TIME,
                funding_interval_hours=8,
                actual_funding_rate=Decimal("0.0004"),
                status="confirmed",
            )
        ]

    async def existing_interval_summary_keys(self, events):
        if not self.existing:
            return set()
        return {("BINANCE", event.symbol, event.funding_time) for event in events}

    async def snapshots_for_interval(self, _symbol, _funding_time):
        return self.snapshots

    async def snapshots_for_intervals(self, events):
        return {(event.symbol, event.funding_time): self.snapshots for event in events}

    async def upsert_interval_summaries(self, summaries):
        rows = list(summaries)
        self.upserted += len(rows)
        return len(rows)


class FakeConfirmationRepository:
    def __init__(self) -> None:
        self.confirmed: list[tuple[str, datetime]] = []

    async def funding_events_for_confirmation(self, **_kwargs):
        return [
            FundingEvent(
                symbol="BTCUSDT",
                funding_time=FUNDING_TIME,
                funding_interval_hours=8,
                status="waiting",
            )
        ]

    async def mark_event_confirmed(
        self,
        symbol,
        funding_time,
        _actual_funding_rate,
        _mark_price_at_funding,
        _confirmed_at,
    ):
        self.confirmed.append((symbol, funding_time))


class FakeFundingRateRestClient:
    async def get_funding_rate_history(self, symbol, **_kwargs):
        return [
            {
                "symbol": symbol,
                "fundingRate": "0.00020",
                "fundingTime": utc_datetime_to_millis(FUNDING_TIME),
                "markPrice": "43002.0",
            }
        ]
