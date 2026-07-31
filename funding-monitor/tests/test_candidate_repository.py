import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.candidate_engine import (
    DEFAULT_EXCHANGE,
    CandidateEvaluation,
    CandidateStatus,
    FundingIntervalAnalyticsService,
    FundingIntervalBuildResult,
    RejectionReason,
    ScoreComponents,
    build_funding_interval_summary,
)
from funding_monitor.candidate_repository import (
    UPSERT_CANDIDATE_EVALUATION_SQL,
    UPSERT_INTERVAL_SUMMARY_SQL,
    CandidateRepository,
)
from funding_monitor.models import FundingEvent, FundingSnapshot

NOW = datetime(2024, 1, 1, 7, 30, tzinfo=UTC)
FUNDING_TIME = datetime(2024, 1, 1, 8, tzinfo=UTC)


def test_candidate_evaluation_sql_has_duplicate_bucket_protection() -> None:
    assert (
        "ON CONFLICT(exchange, futures_symbol, evaluated_at_bucket, engine_version)"
        in UPSERT_CANDIDATE_EVALUATION_SQL
    )
    assert "JSONB" not in UPSERT_CANDIDATE_EVALUATION_SQL
    assert "$1" in UPSERT_CANDIDATE_EVALUATION_SQL


def test_candidate_repository_batch_insert_serializes_json_and_decimal() -> None:
    connection = RecordingConnection()
    repository = CandidateRepository(RecordingDatabase(connection))  # type: ignore[arg-type]

    count = asyncio.run(repository.upsert_evaluations([evaluation("BTCUSDT")]))

    assert count == 1
    assert len(connection.executemany_args) == 1
    args = connection.executemany_args[0]
    assert args[0] == DEFAULT_EXCHANGE
    assert args[1] == "BTCUSDT"
    assert isinstance(args[6], Decimal)
    assert isinstance(args[29], str)
    assert json.loads(args[29]) == ["persistence_too_low"]
    assert json.loads(args[31])["total_score"] == "61.0000"


def test_candidate_repository_maps_latest_evaluation_rows() -> None:
    repository = CandidateRepository(RecordingDatabase(RecordingConnection()))  # type: ignore[arg-type]
    mapped = repository._row_to_evaluation(row())

    assert mapped.futures_symbol == "BTCUSDT"
    assert mapped.exchange == DEFAULT_EXCHANGE
    assert mapped.status == CandidateStatus.CANDIDATE
    assert mapped.total_score == Decimal("61.0000")
    assert mapped.rejection_reasons == (RejectionReason.PERSISTENCE_TOO_LOW,)
    assert mapped.score_components.penalties == {"late_spike_penalty": Decimal(5)}


def test_interval_summary_sql_is_idempotent() -> None:
    assert "ON CONFLICT(exchange, futures_symbol, funding_time) DO UPDATE" in (
        UPSERT_INTERVAL_SUMMARY_SQL
    )


def test_interval_summary_batch_insert_uses_realized_values() -> None:
    connection = RecordingConnection()
    repository = CandidateRepository(RecordingDatabase(connection))  # type: ignore[arg-type]
    summary = build_funding_interval_summary(
        event(),
        [snapshot(FUNDING_TIME - timedelta(minutes=5), Decimal("0.0007"))],
        min_funding_rate=Decimal("0.0003"),
        point_tolerance_seconds=90,
    )

    count = asyncio.run(repository.upsert_interval_summaries([summary]))

    assert count == 1
    args = connection.executemany_args[0]
    assert args[0] == DEFAULT_EXCHANGE
    assert args[1] == "BTCUSDT"
    assert args[5] == Decimal("0.0004")
    assert args[12] == Decimal("0.0007")
    assert args[27] == Decimal("0.0003")


def test_funding_interval_analytics_service_is_idempotent_for_existing_keys() -> None:
    repository = RecordingIntervalRepository(existing=True)
    service = FundingIntervalAnalyticsService(
        repository=repository,
        config=repository.config,
    )

    result = asyncio.run(service.build_missing_summaries())

    assert isinstance(result, FundingIntervalBuildResult)
    assert result.processed == 1
    assert result.created == 0
    assert result.updated == 1
    assert result.failed == 0
    assert repository.upserted == 1


def evaluation(symbol: str) -> CandidateEvaluation:
    score = ScoreComponents(
        funding_score=Decimal("20.0000"),
        persistence_score=Decimal("20.0000"),
        stability_score=Decimal("10.0000"),
        trend_score=Decimal("5.0000"),
        lifetime_score=Decimal("4.0000"),
        timing_score=Decimal("2.0000"),
        penalties={"late_spike_penalty": Decimal(5)},
        total_penalty=Decimal("5.0000"),
        total_score=Decimal("61.0000"),
    )
    return CandidateEvaluation(
        exchange=DEFAULT_EXCHANGE,
        futures_symbol=symbol,
        spot_symbol="BTCUSDT",
        evaluated_at=NOW,
        evaluated_at_bucket=NOW,
        next_funding_time=FUNDING_TIME,
        predicted_funding_rate=Decimal("0.0006"),
        minimum_funding_rate=Decimal("0.0003"),
        minutes_to_funding=Decimal(30),
        status=CandidateStatus.CANDIDATE,
        score_components=score,
        persistence_ratio=Decimal("0.8"),
        standard_deviation=Decimal("0.00001"),
        velocity=Decimal("0.000001"),
        acceleration=Decimal("0.000001"),
        threshold_crossings=1,
        direction_changes=0,
        signal_started_at=NOW - timedelta(minutes=20),
        signal_age_seconds=1200,
        snapshot_count=20,
        history_duration_seconds=1800,
        latest_snapshot_at=NOW,
        rejection_reasons=(RejectionReason.PERSISTENCE_TOO_LOW,),
        warning_flags=(RejectionReason.PERSISTENCE_TOO_LOW,),
        score_details=score.details(),
        metrics_details={"primary_mean_rate": "0.0005"},
        engine_version="1.0",
    )


def row():
    return {
        "futures_symbol": "BTCUSDT",
        "exchange": DEFAULT_EXCHANGE,
        "spot_symbol": "BTCUSDT",
        "evaluated_at": NOW,
        "evaluated_at_bucket": NOW,
        "next_funding_time": FUNDING_TIME,
        "predicted_funding_rate": Decimal("0.0006"),
        "minimum_funding_rate": Decimal("0.0003"),
        "minutes_to_funding": Decimal(30),
        "status": "candidate",
        "total_score": Decimal("61.0000"),
        "funding_score": Decimal("20.0000"),
        "persistence_score": Decimal("20.0000"),
        "stability_score": Decimal("10.0000"),
        "trend_score": Decimal("5.0000"),
        "lifetime_score": Decimal("4.0000"),
        "timing_score": Decimal("2.0000"),
        "total_penalty": Decimal("5.0000"),
        "persistence_ratio": Decimal("0.8"),
        "standard_deviation": Decimal("0.00001"),
        "velocity": Decimal("0.000001"),
        "acceleration": Decimal("0.000001"),
        "threshold_crossings": 1,
        "direction_changes": 0,
        "signal_started_at": NOW - timedelta(minutes=20),
        "signal_age_seconds": 1200,
        "snapshot_count": 20,
        "history_duration_seconds": 1800,
        "latest_snapshot_at": NOW,
        "rejection_reasons": '["persistence_too_low"]',
        "warning_flags": '["persistence_too_low"]',
        "score_details": (
            '{"penalties":{"late_spike_penalty":"5"},"total_score":"61.0000"}'
        ),
        "metrics_details": '{"primary_mean_rate":"0.0005"}',
        "engine_version": "1.0",
    }


def event() -> FundingEvent:
    return FundingEvent(
        symbol="BTCUSDT",
        funding_time=FUNDING_TIME,
        funding_interval_hours=8,
        actual_funding_rate=Decimal("0.0004"),
        status="confirmed",
    )


def snapshot(event_time: datetime, rate: Decimal) -> FundingSnapshot:
    return FundingSnapshot(
        symbol="BTCUSDT",
        event_time=event_time,
        received_at=event_time,
        mark_price=Decimal(100),
        index_price=Decimal(100),
        estimated_settle_price=None,
        predicted_funding_rate=rate,
        funding_rate=rate,
        interest_rate=None,
        next_funding_time=FUNDING_TIME,
        seconds_until_funding=int((FUNDING_TIME - event_time).total_seconds()),
        seconds_to_funding=max(0, int((FUNDING_TIME - event_time).total_seconds())),
        premium_rate=Decimal(0),
        funding_direction="positive",
        funding_interval_hours=8,
        capture_mode="normal",
    )


class RecordingConnection:
    def __init__(self) -> None:
        self.executemany_args = []

    async def executemany(self, _sql, args):
        self.executemany_args = args


class RecordingDatabase:
    def __init__(self, connection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection


class RecordingIntervalRepository:
    def __init__(self, *, existing: bool) -> None:
        from funding_monitor.candidate_engine import CandidateEngineConfig

        self.config = CandidateEngineConfig()
        self.existing = existing
        self.upserted = 0

    async def confirmed_events_for_interval_summaries(self, _limit):
        return [event()]

    async def existing_interval_summary_keys(self, events):
        if not self.existing:
            return set()
        return {(DEFAULT_EXCHANGE, item.symbol, item.funding_time) for item in events}

    async def snapshots_for_interval(self, _symbol, _funding_time):
        return [snapshot(FUNDING_TIME - timedelta(minutes=5), Decimal("0.0007"))]

    async def upsert_interval_summaries(self, summaries):
        rows = list(summaries)
        self.upserted = len(rows)
        return len(rows)
