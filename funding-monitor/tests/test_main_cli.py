from datetime import UTC, datetime
from decimal import Decimal

from funding_monitor.candidate_engine import (
    DEFAULT_EXCHANGE,
    CandidateEvaluation,
    CandidateRejectionAggregate,
    CandidateStatus,
    FundingIntervalBuildResult,
    RejectionReason,
    ScoreComponents,
)
from funding_monitor.instrument_mapping import (
    InstrumentMapping,
    InstrumentMappingSummary,
    InstrumentMappingSyncResult,
    MappingReason,
    NegativeStrategyStatus,
    SpotMappingStatus,
)
from funding_monitor.main import (
    _build_parser,
    _filter_candidate_evaluations,
    _print_candidate_rejections,
    _print_candidates,
    _print_candidates_json,
    _print_instrument_mapping_summary,
    _print_interval_build_result,
    _print_mapping_details,
    _print_mapping_status_summary,
    _print_mapping_sync_result,
)


def test_parser_accepts_instrument_mapping_commands() -> None:
    parser = _build_parser()

    sync_args = parser.parse_args(["sync-instrument-mappings"])
    status_args = parser.parse_args(["instrument-mappings", "--status", "matched"])
    symbol_args = parser.parse_args(["instrument-mappings", "--symbol", "BTCUSDT"])

    assert sync_args.command == "sync-instrument-mappings"
    assert status_args.status == "matched"
    assert symbol_args.symbol == "BTCUSDT"


def test_parser_accepts_candidate_commands() -> None:
    parser = _build_parser()

    candidates = parser.parse_args(
        [
            "candidates",
            "--top",
            "20",
            "--min-score",
            "60",
            "--status",
            "candidate",
            "--status",
            "strong_candidate",
            "--symbol",
            "BTCUSDT",
            "--include-rejected",
            "--no-persist",
            "--json",
        ]
    )
    rejections = parser.parse_args(["candidate-rejections"])
    summaries = parser.parse_args(["build-funding-interval-summaries"])

    assert candidates.command == "candidates"
    assert candidates.top == 20
    assert candidates.min_score == Decimal(60)
    assert candidates.status == ["candidate", "strong_candidate"]
    assert candidates.symbol == "BTCUSDT"
    assert candidates.include_rejected
    assert candidates.no_persist
    assert candidates.json
    assert rejections.command == "candidate-rejections"
    assert summaries.command == "build-funding-interval-summaries"


def test_parser_accepts_pipeline_reliability_commands() -> None:
    parser = _build_parser()

    health = parser.parse_args(["collector-health", "--json"])
    pipeline_status = parser.parse_args(["pipeline-status", "--json"])
    coverage = parser.parse_args(
        ["coverage-report", "--minutes", "15", "--limit", "5", "--json"]
    )
    evaluate = parser.parse_args(
        [
            "evaluate-candidates",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--limit",
            "2",
            "--at",
            "2024-01-01T00:00:00+00:00",
            "--dry-run",
            "--json",
        ]
    )
    interval_backfill = parser.parse_args(
        [
            "backfill-funding-intervals",
            "--from",
            "2024-01-01T00:00:00+00:00",
            "--to",
            "2024-01-02T00:00:00+00:00",
            "--symbols",
            "BTCUSDT",
            "--limit",
            "10",
            "--dry-run",
            "--retry-failed",
            "--json",
        ]
    )
    confirmation_backfill = parser.parse_args(
        ["backfill-confirmations", "--limit", "3", "--retry-failed", "--json"]
    )

    assert health.command == "collector-health"
    assert health.json
    assert pipeline_status.command == "pipeline-status"
    assert coverage.minutes == 15
    assert coverage.limit == 5
    assert evaluate.command == "evaluate-candidates"
    assert evaluate.symbols == "BTCUSDT,ETHUSDT"
    assert evaluate.limit == 2
    assert evaluate.dry_run
    assert interval_backfill.command == "backfill-funding-intervals"
    assert interval_backfill.period_start == "2024-01-01T00:00:00+00:00"
    assert interval_backfill.retry_failed
    assert confirmation_backfill.command == "backfill-confirmations"
    assert confirmation_backfill.limit == 3


def test_parser_accepts_strategy_validation_commands() -> None:
    parser = _build_parser()

    validation = parser.parse_args(
        [
            "validate-strategy",
            "--from",
            "2024-01-01T00:00:00+00:00",
            "--to",
            "2024-01-02T00:00:00+00:00",
            "--symbol",
            "BTCUSDT",
            "--funding-threshold-rate",
            "0.0004",
            "--entry-mode",
            "first_qualifying_signal",
            "--validation-mode",
            "funding_only",
        ]
    )
    grid = parser.parse_args(
        [
            "validate-grid",
            "--funding-threshold-rates",
            "0.0002,0.0003",
            "--entry-minutes-grid",
            "30,60",
        ]
    )
    report = parser.parse_args(["validation-report", "--run-id", "1"])
    compare = parser.parse_args(
        ["validation-compare", "--run-id", "1", "--run-id", "2"]
    )

    assert validation.command == "validate-strategy"
    assert validation.period_start == "2024-01-01T00:00:00+00:00"
    assert validation.symbol == ["BTCUSDT"]
    assert validation.funding_threshold_rate == Decimal("0.0004")
    assert validation.entry_mode == "first_qualifying_signal"
    assert grid.command == "validate-grid"
    assert grid.funding_threshold_rates == "0.0002,0.0003"
    assert grid.entry_minutes_grid == "30,60"
    assert report.run_id == 1
    assert compare.run_id == [1, 2]


def test_strategy_validation_threshold_cli_uses_decimal_rate_units() -> None:
    parser = _build_parser()

    explicit_rate = parser.parse_args(
        ["validate-strategy", "--funding-threshold-rate", "0.0003"]
    )
    legacy_alias = parser.parse_args(
        ["validate-strategy", "--funding-threshold", "0.03"]
    )
    grid = parser.parse_args(
        ["validate-grid", "--funding-threshold-rates", "0.0002,0.0003"]
    )

    assert explicit_rate.funding_threshold_rate == Decimal("0.0003")
    assert legacy_alias.funding_threshold_rate == Decimal("0.03")
    assert grid.funding_threshold_rates == "0.0002,0.0003"


def test_sync_instrument_mappings_prints_aggregates(capsys) -> None:
    _print_mapping_sync_result(
        InstrumentMappingSyncResult(
            futures_symbols_processed=2,
            matched=1,
            missing=1,
            ambiguous=0,
            unsupported=0,
            spot_trading_disabled=0,
            positive_strategy_available=1,
            negative_strategy_available=0,
            negative_strategy_pending_borrow_implementation=1,
            synchronization_duration_seconds=1.25,
            updated_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
    )

    output = capsys.readouterr().out

    assert "futures_symbols_processed: 2" in output
    assert "matched: 1" in output
    assert "negative_strategy_available: 0" in output


def test_instrument_mappings_default_output(capsys) -> None:
    _print_instrument_mapping_summary(summary())

    output = capsys.readouterr().out

    assert "futures_symbols_processed: 2" in output
    assert "futures_with_spot: 1" in output
    assert "positive_strategy_available: 1" in output


def test_status_mapping_statistics_output(capsys) -> None:
    _print_mapping_status_summary(summary())

    output = capsys.readouterr().out

    assert "instrument_mappings: 2" in output
    assert "ambiguous_spot_mappings: 0" in output
    assert "negative_strategy_pending_borrow_check: 1" in output


def test_instrument_mappings_detail_output(capsys) -> None:
    _print_mapping_details([mapping()])

    output = capsys.readouterr().out

    assert "futures_symbol futures_base_asset" in output
    assert "BTCUSDT BTC PERPETUAL TRADING BTCUSDT matched" in output


def test_candidates_table_and_json_output(capsys) -> None:
    evaluation = candidate_evaluation("BTCUSDT", CandidateStatus.CANDIDATE)

    _print_candidates([evaluation])
    table_output = capsys.readouterr().out
    _print_candidates_json([evaluation])
    json_output = capsys.readouterr().out

    assert "rank futures_symbol spot_symbol" in table_output
    assert "BTCUSDT BTCUSDT 0.0006" in table_output
    assert '"futures_symbol": "BTCUSDT"' in json_output
    assert '"status": "candidate"' in json_output


def test_candidate_filter_hides_rejected_by_default() -> None:
    candidate = candidate_evaluation("BTCUSDT", CandidateStatus.CANDIDATE)
    rejected = candidate_evaluation("ETHUSDT", CandidateStatus.REJECTED)

    default_rows = _filter_candidate_evaluations(
        [rejected, candidate],
        statuses=[],
        symbol=None,
        min_score=None,
        include_rejected=False,
        limit=10,
    )
    rejected_rows = _filter_candidate_evaluations(
        [rejected, candidate],
        statuses=[CandidateStatus.REJECTED],
        symbol=None,
        min_score=None,
        include_rejected=False,
        limit=10,
    )

    assert default_rows == [candidate]
    assert rejected_rows == [rejected]


def test_candidate_rejections_and_interval_result_output(capsys) -> None:
    _print_candidate_rejections(
        [
            CandidateRejectionAggregate(
                reason=RejectionReason.FUNDING_BELOW_THRESHOLD,
                symbol_count=2,
                percentage=Decimal("50.000000"),
                examples=("BTCUSDT", "ETHUSDT"),
            )
        ]
    )
    rejection_output = capsys.readouterr().out
    _print_interval_build_result(
        FundingIntervalBuildResult(
            processed=3,
            created=1,
            updated=1,
            partial=1,
            skipped=0,
            failed=0,
        )
    )
    interval_output = capsys.readouterr().out

    assert "funding_below_threshold 2 50.000000 BTCUSDT,ETHUSDT" in (
        rejection_output
    )
    assert "processed: 3" in interval_output
    assert "partial: 1" in interval_output


def summary() -> InstrumentMappingSummary:
    return InstrumentMappingSummary(
        table_available=True,
        futures_symbols_processed=2,
        futures_with_spot=1,
        futures_without_spot=1,
        matched=1,
        missing=1,
        ambiguous=0,
        unsupported=0,
        spot_trading_disabled=0,
        positive_strategy_available=1,
        negative_strategy_available=0,
        negative_strategy_pending_borrow_implementation=1,
        mappings_last_updated_at=datetime(2024, 1, 1, tzinfo=UTC),
    )


def mapping() -> InstrumentMapping:
    timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    return InstrumentMapping(
        futures_symbol="BTCUSDT",
        futures_pair="BTCUSDT",
        futures_base_asset="BTC",
        futures_quote_asset="USDT",
        futures_margin_asset="USDT",
        futures_contract_type="PERPETUAL",
        futures_status="TRADING",
        spot_symbol="BTCUSDT",
        spot_base_asset="BTC",
        spot_quote_asset="USDT",
        spot_status="TRADING",
        spot_trading_allowed=True,
        spot_pair_exists=True,
        spot_mapping_status=SpotMappingStatus.MATCHED,
        mapping_reason=MappingReason.EXACT_BASE_ASSET_MATCH,
        positive_strategy_available=True,
        negative_strategy_available=False,
        negative_strategy_status=NegativeStrategyStatus.BORROW_CHECK_NOT_IMPLEMENTED,
        mapping_source="binance_exchange_info",
        mapping_updated_at=timestamp,
        created_at=timestamp,
        updated_at=timestamp,
    )


def candidate_evaluation(
    symbol: str,
    status: CandidateStatus,
) -> CandidateEvaluation:
    timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    score = ScoreComponents(
        funding_score=Decimal(20),
        persistence_score=Decimal(20),
        stability_score=Decimal(10),
        trend_score=Decimal(5),
        lifetime_score=Decimal(4),
        timing_score=Decimal(2),
        penalties={},
        total_penalty=Decimal(0),
        total_score=Decimal(61),
    )
    return CandidateEvaluation(
        exchange=DEFAULT_EXCHANGE,
        futures_symbol=symbol,
        spot_symbol=symbol,
        evaluated_at=timestamp,
        evaluated_at_bucket=timestamp,
        next_funding_time=timestamp,
        predicted_funding_rate=Decimal("0.0006"),
        minimum_funding_rate=Decimal("0.0003"),
        minutes_to_funding=Decimal(30),
        status=status,
        score_components=score,
        persistence_ratio=Decimal("0.8"),
        standard_deviation=Decimal("0.00001"),
        velocity=Decimal("0.000001"),
        acceleration=Decimal("0.000001"),
        threshold_crossings=1,
        direction_changes=0,
        signal_started_at=timestamp,
        signal_age_seconds=1200,
        snapshot_count=20,
        history_duration_seconds=1800,
        latest_snapshot_at=timestamp,
        rejection_reasons=(RejectionReason.FUNDING_BELOW_THRESHOLD,)
        if status == CandidateStatus.REJECTED
        else (),
        warning_flags=(),
        score_details=score.details(),
        metrics_details={"primary_mean_rate": "0.0005"},
        engine_version="1.0",
    )
