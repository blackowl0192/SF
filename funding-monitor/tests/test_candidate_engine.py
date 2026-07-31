from datetime import UTC, datetime, timedelta
from decimal import Decimal

from funding_monitor.candidate_engine import (
    DEFAULT_EXCHANGE,
    CandidateEngine,
    CandidateEngineConfig,
    CandidateInput,
    CandidateStatus,
    FundingIntervalBuilder,
    FundingMetricsCollection,
    FundingSnapshotCollection,
    RejectionReason,
    build_funding_interval_summary,
    rank_evaluations,
)
from funding_monitor.history_service import calculate_funding_metrics
from funding_monitor.instrument_mapping import SpotMappingStatus
from funding_monitor.models import FundingEvent, FundingSnapshot

NOW = datetime(2024, 1, 1, 7, 30, tzinfo=UTC)
FUNDING_TIME = datetime(2024, 1, 1, 8, 30, tzinfo=UTC)


def test_stable_high_positive_funding_is_strong_candidate() -> None:
    evaluation = evaluate(rates([Decimal("0.0009")] * 16))

    assert evaluation.status == CandidateStatus.STRONG_CANDIDATE
    assert evaluation.total_score > Decimal(80)
    assert evaluation.score_components.funding_score == Decimal("30.0000")
    assert evaluation.persistence_ratio == Decimal("1.000000")
    assert evaluation.exchange == DEFAULT_EXCHANGE
    assert all(isinstance(value, Decimal) for value in score_values(evaluation))


def test_funding_equal_threshold_passes_hard_filters() -> None:
    evaluation = evaluate(rates([Decimal("0.0003")] * 16))

    assert RejectionReason.FUNDING_BELOW_THRESHOLD not in evaluation.rejection_reasons
    assert evaluation.status != CandidateStatus.REJECTED


def test_negative_zero_and_below_threshold_funding_are_rejected() -> None:
    negative = evaluate(rates([Decimal("-0.0004")] * 16))
    zero = evaluate(rates([Decimal(0)] * 16))
    below = evaluate(rates([Decimal("0.000299")] * 16))

    assert negative.status == CandidateStatus.REJECTED
    assert RejectionReason.FUNDING_NOT_POSITIVE in negative.rejection_reasons
    assert zero.status == CandidateStatus.REJECTED
    assert RejectionReason.FUNDING_NOT_POSITIVE in zero.rejection_reasons
    assert below.status == CandidateStatus.REJECTED
    assert RejectionReason.FUNDING_BELOW_THRESHOLD in below.rejection_reasons


def test_mapping_hard_filters_return_machine_readable_reasons() -> None:
    missing = evaluate(
        rates([Decimal("0.0006")] * 16),
        mapping_status=SpotMappingStatus.MISSING,
    )
    ambiguous = evaluate(
        rates([Decimal("0.0006")] * 16),
        mapping_status=SpotMappingStatus.AMBIGUOUS,
    )
    spot_disabled = evaluate(
        rates([Decimal("0.0006")] * 16),
        spot_trading_allowed=False,
    )
    strategy_unavailable = evaluate(
        rates([Decimal("0.0006")] * 16),
        positive_strategy_available=False,
    )

    assert RejectionReason.SPOT_MAPPING_MISSING in missing.rejection_reasons
    assert RejectionReason.SPOT_MAPPING_AMBIGUOUS in ambiguous.rejection_reasons
    assert RejectionReason.SPOT_TRADING_DISABLED in spot_disabled.rejection_reasons
    assert (
        RejectionReason.POSITIVE_STRATEGY_UNAVAILABLE
        in strategy_unavailable.rejection_reasons
    )


def test_stale_missing_or_expired_funding_time_have_specific_statuses() -> None:
    stale = evaluate(
        rates([Decimal("0.0006")] * 16),
        evaluated_at=NOW + timedelta(minutes=5),
    )
    missing_time = evaluate(
        rates([Decimal("0.0006")] * 16),
        next_funding_time=None,
    )
    expired = evaluate(
        rates([Decimal("0.0006")] * 16),
        next_funding_time=NOW - timedelta(seconds=1),
    )

    assert stale.status == CandidateStatus.STALE
    assert RejectionReason.STALE_SNAPSHOT in stale.rejection_reasons
    assert missing_time.status == CandidateStatus.REJECTED
    assert RejectionReason.NEXT_FUNDING_TIME_MISSING in missing_time.rejection_reasons
    assert expired.status == CandidateStatus.EXPIRED
    assert RejectionReason.FUNDING_TIME_EXPIRED in expired.rejection_reasons


def test_insufficient_history_and_snapshot_count_are_rejected_as_data_quality() -> None:
    evaluation = evaluate(rates([Decimal("0.0006")] * 5))

    assert evaluation.status == CandidateStatus.INSUFFICIENT_HISTORY
    assert RejectionReason.INSUFFICIENT_SNAPSHOT_COUNT in evaluation.rejection_reasons


def test_single_late_spike_does_not_become_strong_candidate() -> None:
    spike_rates = [
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("0.0002"),
        Decimal("0.0006"),
        Decimal("0.0010"),
    ]

    evaluation = evaluate(
        rates(spike_rates),
        next_funding_time=NOW + timedelta(minutes=10),
    )

    assert evaluation.status == CandidateStatus.LATE_SPIKE
    assert RejectionReason.LATE_SPIKE_DETECTED in evaluation.rejection_reasons
    assert evaluation.status != CandidateStatus.STRONG_CANDIDATE


def test_deteriorating_and_funding_falling_statuses_have_priority_over_score() -> None:
    deteriorating = evaluate(
        rates(
            [
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.00055"),
                Decimal("0.0005"),
                Decimal("0.00046"),
                Decimal("0.00042"),
                Decimal("0.00038"),
                Decimal("0.00034"),
            ],
            interval_seconds=60,
        )
    )
    falling = evaluate(
        rates(
            [
                Decimal("0.0009"),
                Decimal("0.00086"),
                Decimal("0.00082"),
                Decimal("0.00078"),
                Decimal("0.00074"),
                Decimal("0.00070"),
                Decimal("0.00066"),
                Decimal("0.00062"),
                Decimal("0.00058"),
                Decimal("0.00054"),
                Decimal("0.00050"),
                Decimal("0.00046"),
                Decimal("0.00042"),
                Decimal("0.00038"),
                Decimal("0.00034"),
                Decimal("0.00031"),
            ],
            interval_seconds=60,
        )
    )

    assert deteriorating.status in {
        CandidateStatus.DETERIORATING,
        CandidateStatus.FUNDING_FALLING,
    }
    assert RejectionReason.FUNDING_DETERIORATING in deteriorating.rejection_reasons
    assert falling.status == CandidateStatus.FUNDING_FALLING
    assert RejectionReason.NEGATIVE_VELOCITY in falling.warning_flags


def test_unstable_too_early_too_late_observing_and_weak_candidate_statuses() -> None:
    config = CandidateEngineConfig(min_score=Decimal(101), strong_score=Decimal(102))
    weak = evaluate(rates([Decimal("0.0006")] * 16), config=config)
    observing = evaluate(
        rates(
            [Decimal("0.0001")] * 5
            + [Decimal("0.0006")] * 11,
            interval_seconds=120,
        ),
        next_funding_time=NOW + timedelta(minutes=180),
        config=CandidateEngineConfig(max_std_dev=Decimal("0.001")),
    )
    unstable = evaluate(
        rates(
            [
                Decimal("0.0001"),
                Decimal("0.0006"),
                Decimal("0.0001"),
                Decimal("0.0006"),
                Decimal("0.0001"),
                Decimal("0.0006"),
                Decimal("0.0001"),
                Decimal("0.0006"),
                Decimal("0.0001"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
                Decimal("0.0006"),
            ]
        ),
        config=CandidateEngineConfig(max_std_dev=Decimal("0.001")),
    )
    too_early = evaluate(
        rates([Decimal("0.0006")] * 16),
        next_funding_time=NOW + timedelta(minutes=600),
    )
    too_late = evaluate(
        rates([Decimal("0.0006")] * 16),
        next_funding_time=NOW + timedelta(minutes=3),
    )

    assert weak.status == CandidateStatus.WEAK_CANDIDATE
    assert observing.status == CandidateStatus.OBSERVING
    assert RejectionReason.PERSISTENCE_TOO_LOW in observing.rejection_reasons
    assert unstable.status == CandidateStatus.UNSTABLE
    assert RejectionReason.TOO_MANY_THRESHOLD_CROSSINGS in unstable.rejection_reasons
    assert too_early.status == CandidateStatus.TOO_EARLY
    assert too_late.status == CandidateStatus.TOO_LATE


def test_score_is_deterministic_bounded_and_penalties_do_not_go_negative() -> None:
    snapshots = rates([Decimal("0.0009")] * 16)
    first = evaluate(snapshots)
    second = evaluate(snapshots)

    assert first.total_score == second.total_score
    assert Decimal(0) <= first.total_score <= Decimal(100)

    spike = evaluate(
        rates(
            [Decimal("0.00001")] * 14
            + [Decimal("0.0005"), Decimal("0.005")]
        ),
        next_funding_time=NOW + timedelta(minutes=10),
    )

    assert spike.total_score >= Decimal(0)


def test_ranking_is_deterministic() -> None:
    btc = evaluate(rates([Decimal("0.0008")] * 16), symbol="BTCUSDT")
    eth = evaluate(rates([Decimal("0.0008")] * 16), symbol="ETHUSDT")
    low = evaluate(rates([Decimal("0.0004")] * 16), symbol="ADAUSDT")

    ranked = rank_evaluations([eth, low, btc])

    assert [item.futures_symbol for item in ranked] == [
        "BTCUSDT",
        "ETHUSDT",
        "ADAUSDT",
    ]


def test_funding_interval_summary_uses_realized_rate_only_and_point_tolerance() -> None:
    event = FundingEvent(
        symbol="BTCUSDT",
        funding_time=FUNDING_TIME,
        funding_interval_hours=2,
        actual_funding_rate=Decimal("0.0004"),
        status="confirmed",
    )
    snapshots = [
        snapshot(FUNDING_TIME - timedelta(minutes=120), Decimal("0.0002")),
        snapshot(FUNDING_TIME - timedelta(minutes=60), Decimal("0.0003")),
        snapshot(FUNDING_TIME - timedelta(minutes=30), Decimal("0.0005")),
        snapshot(FUNDING_TIME - timedelta(minutes=15), Decimal("0.0006")),
        snapshot(FUNDING_TIME - timedelta(minutes=5), Decimal("0.0007")),
        snapshot(FUNDING_TIME + timedelta(seconds=1), Decimal("0.0099")),
    ]

    summary = build_funding_interval_summary(
        event,
        snapshots,
        min_funding_rate=Decimal("0.0003"),
        point_tolerance_seconds=90,
    )

    assert summary.realized_funding_rate == Decimal("0.0004")
    assert summary.exchange == DEFAULT_EXCHANGE
    assert summary.last_predicted_rate == Decimal("0.0007")
    assert summary.prediction_error == Decimal("0.0003")
    assert summary.maximum_predicted_rate == Decimal("0.0007")
    assert summary.predicted_rate_120m_before == Decimal("0.0002")
    assert summary.predicted_rate_5m_before == Decimal("0.0007")
    assert summary.snapshot_count == 5
    assert summary.peak_predicted_at == FUNDING_TIME - timedelta(minutes=5)
    assert summary.signal_started_at == FUNDING_TIME - timedelta(minutes=60)
    assert summary.longest_positive_streak_seconds == 6900


def test_funding_interval_builder_is_independent_component() -> None:
    builder = FundingIntervalBuilder(
        min_funding_rate=Decimal("0.0003"),
        point_tolerance_seconds=90,
    )
    event = FundingEvent(
        symbol="BTCUSDT",
        funding_time=FUNDING_TIME,
        funding_interval_hours=8,
        actual_funding_rate=Decimal("0.0004"),
        status="confirmed",
    )

    summary = builder.build(
        event,
        [snapshot(FUNDING_TIME - timedelta(minutes=5), Decimal("0.0005"))],
    )

    assert summary.futures_symbol == "BTCUSDT"
    assert summary.realized_funding_rate == Decimal("0.0004")


def test_funding_interval_summary_keeps_missing_point_null() -> None:
    event = FundingEvent(
        symbol="BTCUSDT",
        funding_time=FUNDING_TIME,
        funding_interval_hours=8,
        actual_funding_rate=Decimal("0.0004"),
        status="confirmed",
    )

    summary = build_funding_interval_summary(
        event,
        [snapshot(FUNDING_TIME - timedelta(minutes=10), Decimal("0.0005"))],
        min_funding_rate=Decimal("0.0003"),
        point_tolerance_seconds=90,
    )

    assert summary.predicted_rate_5m_before is None
    assert summary.summary_status.value == "partial_history"


def evaluate(
    snapshots: list[FundingSnapshot],
    *,
    symbol: str = "BTCUSDT",
    mapping_status: SpotMappingStatus | None = SpotMappingStatus.MATCHED,
    positive_strategy_available: bool = True,
    spot_trading_allowed: bool = True,
    futures_status: str | None = "TRADING",
    next_funding_time: datetime | None = FUNDING_TIME,
    evaluated_at: datetime = NOW,
    config: CandidateEngineConfig | None = None,
) -> object:
    config = config or CandidateEngineConfig()
    primary = tuple(snapshots)
    short_cutoff = snapshots[-1].event_time - timedelta(minutes=config.short_window_minutes)
    short = tuple(item for item in snapshots if item.event_time >= short_cutoff)
    candidate = CandidateInput(
        exchange=DEFAULT_EXCHANGE,
        futures_symbol=symbol,
        spot_symbol=f"{symbol[:-4]}USDT",
        mapping_status=mapping_status,
        positive_strategy_available=positive_strategy_available,
        spot_trading_allowed=spot_trading_allowed,
        futures_status=futures_status,
        current_predicted_funding_rate=snapshots[-1].predicted_funding_rate
        if snapshots
        else None,
        next_funding_time=next_funding_time,
        observed_at=snapshots[-1].event_time if snapshots else None,
        evaluated_at=evaluated_at,
        metrics=FundingMetricsCollection(
            primary=calculate_funding_metrics(
                primary,
                abs_threshold=config.min_funding_rate,
            ),
            short=calculate_funding_metrics(
                short,
                abs_threshold=config.min_funding_rate,
            ),
            long=calculate_funding_metrics(
                primary,
                abs_threshold=config.min_funding_rate,
            ),
        ),
        snapshots=FundingSnapshotCollection(
            primary=primary,
            short=short,
            long=primary,
        ),
    )
    return CandidateEngine(config=config).evaluate(candidate)


def rates(values: list[Decimal], *, interval_seconds: int = 120) -> list[FundingSnapshot]:
    start = NOW - timedelta(seconds=interval_seconds * (len(values) - 1))
    return [
        snapshot(start + timedelta(seconds=index * interval_seconds), value)
        for index, value in enumerate(values)
    ]


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
        funding_direction="positive"
        if rate > 0
        else "negative"
        if rate < 0
        else "neutral",
        funding_interval_hours=8,
        capture_mode="normal",
    )


def score_values(evaluation) -> tuple[Decimal, ...]:
    return (
        evaluation.score_components.funding_score,
        evaluation.score_components.persistence_score,
        evaluation.score_components.stability_score,
        evaluation.score_components.trend_score,
        evaluation.score_components.lifetime_score,
        evaluation.score_components.timing_score,
        evaluation.score_components.total_penalty,
        evaluation.score_components.total_score,
    )
