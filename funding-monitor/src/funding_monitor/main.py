from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from .binance_rest import BinanceRestClient, BinanceSpotRestClient
from .candidate_engine import (
    CandidateEngine,
    CandidateEngineConfig,
    CandidateEvaluation,
    CandidateRejectionAggregate,
    CandidateStatus,
    FundingIntervalAnalyticsService,
    FundingIntervalBuildResult,
    RejectionReason,
    rank_evaluations,
)
from .candidate_repository import CandidateRepository
from .collector import run_collector
from .config import Settings, load_settings
from .database import PostgresDatabase
from .diagnostics import (
    PipelineDiagnostics,
    PipelineDiagnosticsService,
    SymbolCoverageRow,
    coverage_rows_to_dict,
    diagnostics_to_dict,
)
from .history_service import FundingHistoryService, FundingMetrics, WindowCacheSummary
from .instrument_mapping import (
    InstrumentMapping,
    InstrumentMappingService,
    InstrumentMappingSummary,
    InstrumentMappingSyncError,
    InstrumentMappingSyncResult,
    SpotExchangeInfoError,
    SpotMappingStatus,
    SpotSymbolService,
)
from .instrument_repository import InstrumentMappingRepository
from .logging_config import configure_logging
from .models import (
    FundingEvent,
    datetime_to_text,
    decimal_from_text,
    decimal_to_percent_text,
    decimal_to_percentage_point_text,
    text_to_datetime,
    utc_now,
)
from .pipeline import (
    CandidateEvaluationPipeline,
    CandidateEvaluationPipelineResult,
    ConfirmationBackfillResult,
    ConfirmationBackfillService,
    FundingIntervalBackfillResult,
    FundingIntervalBackfillService,
    candidate_config_from_settings,
)
from .repository import FundingRepository
from .strategy_validation.models import (
    EntryMode,
    StrategyValidationAggregate,
    StrategyValidationConfig,
    StrategyValidationDataset,
    StrategyValidationRun,
    StrategyValidationSummary,
    ValidationMode,
)
from .strategy_validation.parameter_grid import StrategyParameterGrid
from .strategy_validation.reporting import (
    format_validation_report,
    format_validation_summary,
)
from .strategy_validation.repository import StrategyValidationRepository
from .strategy_validation.service import StrategyValidationService
from .symbol_service import SymbolService


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        settings = load_settings()
    except RuntimeError as exc:
        parser.exit(2, f"error: {exc}\n")
    configure_logging(settings.log_level)
    asyncio.run(_run(args, settings))


async def _run(args: argparse.Namespace, settings: Settings) -> None:
    database = PostgresDatabase.from_settings(settings)

    if args.command in {"init-db", "migrate"}:
        async with database:
            applied = await database.migrate()
        print(f"applied migrations: {len(applied)}")
        for version in applied:
            print(f"- {version}")
        return

    if args.command == "check-db":
        async with database:
            check_result = await database.check_connection()
        print(f"connected: {str(check_result.connected).lower()}")
        print(f"postgres_version: {check_result.postgres_version}")
        print(f"database_utc_time: {check_result.database_utc_time.isoformat()}")
        print(f"applied_migrations: {check_result.applied_migrations}")
        return

    if args.command == "sync-symbols":
        async with database:
            repository = FundingRepository(database)
            async with BinanceRestClient(
                timeout_seconds=settings.rest_timeout_seconds
            ) as rest_client:
                count = await SymbolService(
                    repository,
                    rest_client,
                    default_funding_interval_hours=settings.default_funding_interval_hours,
                ).sync_symbols()
        print(f"synced symbols: {count}")
        return

    if args.command == "status":
        async with database:
            repository = FundingRepository(database)
            summary = await repository.status_summary(settings.abs_min_funding_rate)
            mapping_summary = await InstrumentMappingRepository(database).summary()
        print(f"active_symbols: {summary['active_symbols']}")
        print(f"snapshots: {summary['snapshot_count']}")
        print(f"funding_events: {summary['event_count']}")
        print(f"last_snapshot: {summary['last_snapshot'] or ''}")
        print(f"waiting: {summary['waiting']}")
        print(f"confirmed: {summary['confirmed']}")
        print(f"confirmation_failed: {summary['confirmation_failed']}")
        print(f"positive_snapshots: {summary['positive_snapshots']}")
        print(f"negative_snapshots: {summary['negative_snapshots']}")
        print(f"neutral_snapshots: {summary['neutral_snapshots']}")
        print(
            "snapshots_above_abs_threshold: "
            f"{summary['snapshots_above_abs_threshold']}"
        )
        print(
            "snapshots_below_abs_threshold: "
            f"{summary['snapshots_below_abs_threshold']}"
        )
        print(f"next_funding_time_min: {summary['next_funding_time_min'] or ''}")
        print(f"latest_received_at: {summary['latest_received_at'] or ''}")
        _print_mapping_status_summary(mapping_summary)
        return

    if args.command == "snapshot-stats":
        async with database:
            repository = FundingRepository(database)
            stats = await repository.snapshot_stats(
                abs_threshold=settings.abs_min_funding_rate,
                minutes=args.minutes,
            )
        _print_snapshot_stats(stats)
        return

    if args.command == "collector-health":
        async with database:
            diagnostics = await PipelineDiagnosticsService(
                database=database,
                settings=settings,
            ).collect()
        if args.json:
            print(json.dumps(diagnostics_to_dict(diagnostics), sort_keys=True))
        else:
            _print_collector_health(diagnostics)
        if diagnostics.critical:
            raise SystemExit(1)
        return

    if args.command == "pipeline-status":
        async with database:
            diagnostics = await PipelineDiagnosticsService(
                database=database,
                settings=settings,
            ).collect()
        if args.json:
            print(json.dumps(diagnostics_to_dict(diagnostics), sort_keys=True))
        else:
            _print_pipeline_status(diagnostics)
        return

    if args.command == "coverage-report":
        async with database:
            coverage_rows = await PipelineDiagnosticsService(
                database=database,
                settings=settings,
            ).worst_symbol_coverage(minutes=args.minutes, limit=args.limit)
        if args.json:
            print(json.dumps(coverage_rows_to_dict(coverage_rows), sort_keys=True))
        else:
            _print_symbol_coverage(coverage_rows)
        return

    if args.command == "history":
        async with database:
            repository = FundingRepository(database)
            history = _create_history_service(repository, settings)
            await history.reload()
            history_summary = history.summary()
        _print_history_summary(history_summary)
        return

    if args.command == "metrics":
        async with database:
            repository = FundingRepository(database)
            history = _create_history_service(repository, settings)
            await history.reload()
            metrics = history.get_metrics(args.symbol, args.window_minutes)
        _print_metrics(metrics)
        return

    if args.command == "sync-instrument-mappings":
        async with database:
            mapping_repository = InstrumentMappingRepository(database)
            async with BinanceRestClient(
                timeout_seconds=settings.rest_timeout_seconds
            ) as futures_client, BinanceSpotRestClient(
                base_url=settings.binance_spot_base_url,
                timeout_seconds=settings.rest_timeout_seconds,
            ) as spot_client:
                service = InstrumentMappingService(
                    repository=mapping_repository,
                    futures_client=futures_client,
                    spot_service=SpotSymbolService(
                        spot_client,
                        supported_quote_asset=settings.supported_spot_quote_asset,
                    ),
                    supported_quote_asset=settings.supported_spot_quote_asset,
                )
                try:
                    mapping_sync_result = await service.sync_mappings()
                except (InstrumentMappingSyncError, SpotExchangeInfoError) as exc:
                    print(f"sync_instrument_mappings_error: {exc}")
                    return
        _print_mapping_sync_result(mapping_sync_result)
        return

    if args.command == "instrument-mappings":
        async with database:
            mapping_repository = InstrumentMappingRepository(database)
            if args.symbol is not None:
                mapping = await mapping_repository.get_mapping(args.symbol)
                mappings = [mapping] if mapping is not None else []
                _print_mapping_details(mappings)
                return
            if args.status is not None:
                mappings = await mapping_repository.list_mappings(
                    status=SpotMappingStatus(args.status)
                )
                _print_mapping_details(mappings)
                return
            mapping_summary = await mapping_repository.summary()
        _print_instrument_mapping_summary(mapping_summary)
        return

    if args.command == "candidates":
        config = _candidate_config_from_settings(settings)
        async with database:
            funding_repository = FundingRepository(database)
            mapping_repository = InstrumentMappingRepository(database)
            candidate_repository = CandidateRepository(database)
            history = _create_candidate_history_service(funding_repository, settings)
            await history.reload()
            mappings = await mapping_repository.list_mappings()
            engine = CandidateEngine(config=config)
            inputs = engine.inputs_from_history(mappings, history)
            evaluations = engine.evaluate_many(inputs)
            if not args.no_persist:
                await candidate_repository.upsert_evaluations(evaluations)
        filtered = _filter_candidate_evaluations(
            evaluations,
            statuses=[CandidateStatus(value) for value in args.status or []],
            symbol=args.symbol,
            min_score=args.min_score,
            include_rejected=args.include_rejected,
            limit=args.top or config.max_results,
        )
        if args.json:
            _print_candidates_json(filtered)
        else:
            _print_candidates(filtered)
        return

    if args.command == "candidate-rejections":
        async with database:
            aggregates = await CandidateRepository(database).latest_rejection_summary()
        _print_candidate_rejections(aggregates)
        return

    if args.command == "evaluate-candidates":
        config = _candidate_config_from_settings(settings)
        async with database:
            candidate_result = await CandidateEvaluationPipeline(
                funding_repository=FundingRepository(database),
                mapping_repository=InstrumentMappingRepository(database),
                candidate_repository=CandidateRepository(database),
                settings=settings,
                config=config,
            ).run(
                symbols=_symbols_arg(args.symbols),
                limit=args.limit,
                evaluated_at=text_to_datetime(args.at) if args.at is not None else None,
                dry_run=args.dry_run,
            )
        if args.json:
            print(json.dumps(_jsonable(asdict(candidate_result)), sort_keys=True))
        else:
            _print_candidate_pipeline_result(candidate_result)
        return

    if args.command == "build-funding-interval-summaries":
        config = _candidate_config_from_settings(settings)
        async with database:
            interval_result = await FundingIntervalAnalyticsService(
                repository=CandidateRepository(database),
                config=config,
            ).build_missing_summaries()
        _print_interval_build_result(interval_result)
        return

    if args.command == "backfill-funding-intervals":
        config = _candidate_config_from_settings(settings)
        async with database:
            interval_backfill_result = await FundingIntervalBackfillService(
                repository=CandidateRepository(database),
                config=config,
            ).run(
                period_start=text_to_datetime(args.period_start)
                if args.period_start is not None
                else None,
                period_end=text_to_datetime(args.period_end)
                if args.period_end is not None
                else None,
                symbols=_symbols_arg(args.symbols),
                limit=args.limit,
                dry_run=args.dry_run,
                retry_failed=args.retry_failed,
            )
        if args.json:
            print(
                json.dumps(
                    _jsonable(asdict(interval_backfill_result)),
                    sort_keys=True,
                )
            )
        else:
            _print_interval_backfill_result(interval_backfill_result)
        return

    if args.command == "backfill-confirmations":
        due_before = utc_now() - timedelta(
            minutes=settings.confirmation_overdue_grace_minutes
        )
        async with database:
            repository = FundingRepository(database)
            async with BinanceRestClient(
                timeout_seconds=settings.rest_timeout_seconds
            ) as rest_client:
                confirmation_result = await ConfirmationBackfillService(
                    repository=repository,
                    rest_client=rest_client,
                ).run(
                    due_before=due_before,
                    limit=args.limit or settings.confirmation_backfill_batch_size,
                    retry_failed=args.retry_failed,
                )
        if args.json:
            print(json.dumps(_jsonable(asdict(confirmation_result)), sort_keys=True))
        else:
            _print_confirmation_backfill_result(confirmation_result)
        return

    if args.command == "validate-strategy":
        validation_config = _strategy_validation_config_from_args(args)
        dataset = _strategy_validation_dataset_from_args(args)
        async with database:
            validation_summary = await StrategyValidationService(
                StrategyValidationRepository(database)
            ).run_validation(config=validation_config, dataset=dataset)
        print(
            format_validation_summary(
                run_id=validation_summary.run_id,
                total_events=validation_summary.total_events,
                processed_events=validation_summary.processed_events,
                successful_events=validation_summary.successful_events,
                failed_events=validation_summary.failed_events,
                aggregates=validation_summary.aggregates,
            )
        )
        return

    if args.command == "validate-grid":
        validation_config = _strategy_validation_config_from_args(args)
        dataset = _strategy_validation_dataset_from_args(args)
        grid = _strategy_parameter_grid_from_args(args)
        async with database:
            summaries = await StrategyValidationService(
                StrategyValidationRepository(database)
            ).run_grid(base_config=validation_config, grid=grid, dataset=dataset)
        _print_validation_grid_summaries(summaries)
        return

    if args.command == "validation-report":
        async with database:
            validation_repository = StrategyValidationRepository(database)
            results = await validation_repository.get_results(args.run_id)
            validation_aggregates = await validation_repository.get_aggregates(
                args.run_id
            )
            if args.export_csv is not None:
                exported = await validation_repository.export_results_csv(
                    args.run_id,
                    args.export_csv,
                )
        print(format_validation_report(results, validation_aggregates))
        if args.export_csv is not None:
            print(f"exported rows: {exported}")
            print(f"output: {args.export_csv}")
        return

    if args.command == "validation-compare":
        async with database:
            validation_repository = StrategyValidationRepository(database)
            compare_rows = [
                (
                    run_id,
                    await validation_repository.get_run(run_id),
                    await validation_repository.get_aggregates(run_id),
                )
                for run_id in args.run_id
            ]
        _print_validation_compare(compare_rows)
        return

    if args.command == "recent-events":
        async with database:
            repository = FundingRepository(database)
            events = await repository.recent_events(args.limit)
        _print_recent_events(events)
        return

    if args.command == "export-csv":
        async with database:
            repository = FundingRepository(database)
            count = await repository.export_events_csv(args.output)
        print(f"exported rows: {count}")
        print(f"output: {args.output}")
        return

    if args.command == "collect":
        count = await run_collector(
            settings,
            max_messages=args.max_messages,
            max_seconds=args.max_seconds,
        )
        print(f"collected snapshots: {count}")
        return

    raise RuntimeError(f"unknown command: {args.command}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m funding_monitor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Alias for migrate.")
    subparsers.add_parser("migrate")
    subparsers.add_parser("check-db")
    subparsers.add_parser("sync-symbols")

    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Stop after N saved snapshots. Omit for continuous collection.",
    )
    collect_parser.add_argument(
        "--max-seconds",
        type=float,
        default=None,
        help="Stop after N seconds. Omit for continuous collection.",
    )

    subparsers.add_parser("status")

    stats_parser = subparsers.add_parser("snapshot-stats")
    stats_parser.add_argument("--minutes", type=int, default=None)

    health_parser = subparsers.add_parser("collector-health")
    health_parser.add_argument("--json", action="store_true")

    pipeline_status_parser = subparsers.add_parser("pipeline-status")
    pipeline_status_parser.add_argument("--json", action="store_true")

    coverage_parser = subparsers.add_parser("coverage-report")
    coverage_parser.add_argument("--minutes", type=int, default=60)
    coverage_parser.add_argument("--limit", type=int, default=20)
    coverage_parser.add_argument("--json", action="store_true")

    subparsers.add_parser("history")

    metrics_parser = subparsers.add_parser("metrics")
    metrics_parser.add_argument("symbol")
    metrics_parser.add_argument("--window-minutes", type=int, default=None)

    subparsers.add_parser("sync-instrument-mappings")

    mappings_parser = subparsers.add_parser("instrument-mappings")
    mappings_parser.add_argument(
        "--status",
        choices=[status.value for status in SpotMappingStatus],
        default=None,
    )
    mappings_parser.add_argument("--symbol", default=None)

    candidates_parser = subparsers.add_parser("candidates")
    candidates_parser.add_argument("--top", type=int, default=None)
    candidates_parser.add_argument("--min-score", type=_decimal_arg, default=None)
    candidates_parser.add_argument(
        "--status",
        action="append",
        choices=[status.value for status in CandidateStatus],
        default=None,
    )
    candidates_parser.add_argument("--symbol", default=None)
    candidates_parser.add_argument("--include-rejected", action="store_true")
    candidates_parser.add_argument("--no-persist", action="store_true")
    candidates_parser.add_argument("--json", action="store_true")

    subparsers.add_parser("candidate-rejections")

    evaluate_parser = subparsers.add_parser("evaluate-candidates")
    evaluate_parser.add_argument("--symbols", default=None)
    evaluate_parser.add_argument("--limit", type=int, default=None)
    evaluate_parser.add_argument("--at", default=None)
    evaluate_parser.add_argument("--dry-run", action="store_true")
    evaluate_parser.add_argument("--json", action="store_true")

    subparsers.add_parser("build-funding-interval-summaries")

    backfill_intervals_parser = subparsers.add_parser("backfill-funding-intervals")
    backfill_intervals_parser.add_argument("--from", dest="period_start", default=None)
    backfill_intervals_parser.add_argument("--to", dest="period_end", default=None)
    backfill_intervals_parser.add_argument("--symbols", default=None)
    backfill_intervals_parser.add_argument("--limit", type=int, default=None)
    backfill_intervals_parser.add_argument("--dry-run", action="store_true")
    backfill_intervals_parser.add_argument("--retry-failed", action="store_true")
    backfill_intervals_parser.add_argument("--json", action="store_true")

    backfill_confirmations_parser = subparsers.add_parser("backfill-confirmations")
    backfill_confirmations_parser.add_argument("--limit", type=int, default=None)
    backfill_confirmations_parser.add_argument("--retry-failed", action="store_true")
    backfill_confirmations_parser.add_argument("--json", action="store_true")

    validation_parser = subparsers.add_parser("validate-strategy")
    _add_strategy_validation_arguments(validation_parser)

    grid_parser = subparsers.add_parser("validate-grid")
    _add_strategy_validation_arguments(grid_parser)
    grid_parser.add_argument(
        "--funding-threshold-rates",
        "--funding-thresholds",
        dest="funding_threshold_rates",
        default=None,
        help="Comma-separated decimal rates. 0.0003 means 0.03%%.",
    )
    grid_parser.add_argument(
        "--entry-modes",
        default=None,
        help="Comma-separated entry modes.",
    )
    grid_parser.add_argument(
        "--entry-minutes-grid",
        default=None,
        help="Comma-separated entry minutes before funding.",
    )
    grid_parser.add_argument(
        "--signal-confirmation-minutes-grid",
        default=None,
        help="Comma-separated signal confirmation minutes.",
    )
    grid_parser.add_argument(
        "--minimum-persistence-ratios",
        default=None,
        help="Comma-separated minimum persistence ratios.",
    )
    grid_parser.add_argument(
        "--maximum-funding-stds",
        default=None,
        help="Comma-separated maximum funding std values.",
    )
    grid_parser.add_argument(
        "--exit-minutes-grid",
        default=None,
        help="Comma-separated exit minutes after funding.",
    )
    grid_parser.add_argument("--max-combinations", type=int, default=100)

    report_parser = subparsers.add_parser("validation-report")
    report_parser.add_argument("--run-id", type=int, required=True)
    report_parser.add_argument("--export-csv", type=Path, default=None)

    compare_parser = subparsers.add_parser("validation-compare")
    compare_parser.add_argument("--run-id", type=int, action="append", required=True)

    recent_parser = subparsers.add_parser("recent-events")
    recent_parser.add_argument("--limit", type=int, default=20)

    export_parser = subparsers.add_parser("export-csv")
    export_parser.add_argument("--output", type=Path, required=True)

    return parser


def _add_strategy_validation_arguments(parser: argparse.ArgumentParser) -> None:
    defaults = StrategyValidationConfig()
    parser.add_argument("--from", dest="period_start", default=None)
    parser.add_argument("--to", dest="period_end", default=None)
    parser.add_argument("--symbol", action="append", default=None)
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated futures symbols.",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--funding-threshold-rate",
        "--funding-threshold",
        dest="funding_threshold_rate",
        type=_decimal_arg,
        default=defaults.funding_threshold,
        help="Funding threshold as a decimal rate/fraction. 0.0003 means 0.03%%.",
    )
    parser.add_argument(
        "--entry-mode",
        choices=[mode.value for mode in EntryMode],
        default=defaults.entry_mode.value,
    )
    parser.add_argument(
        "--entry-minutes-before-funding",
        type=int,
        default=defaults.entry_minutes_before_funding,
    )
    parser.add_argument(
        "--signal-confirmation-minutes",
        type=int,
        default=defaults.signal_confirmation_minutes,
    )
    parser.add_argument(
        "--minimum-persistence-ratio",
        type=_decimal_arg,
        default=defaults.minimum_persistence_ratio,
    )
    parser.add_argument(
        "--maximum-funding-std",
        type=_decimal_arg,
        default=defaults.maximum_funding_std,
    )
    parser.add_argument(
        "--maximum-prediction-drop",
        type=_decimal_arg,
        default=defaults.maximum_prediction_drop,
    )
    parser.add_argument(
        "--minimum-history-minutes",
        type=int,
        default=defaults.minimum_history_minutes,
    )
    parser.add_argument(
        "--maximum-snapshot-age-seconds",
        type=int,
        default=defaults.maximum_snapshot_age_seconds,
    )
    parser.add_argument(
        "--exit-minutes-after-funding",
        type=int,
        default=defaults.exit_minutes_after_funding,
    )
    parser.add_argument(
        "--position-notional",
        type=_decimal_arg,
        default=defaults.position_notional,
    )
    parser.add_argument(
        "--validation-mode",
        choices=[mode.value for mode in ValidationMode],
        default=defaults.validation_mode.value,
    )


def _create_history_service(
    repository: FundingRepository, settings: Settings
) -> FundingHistoryService:
    return FundingHistoryService(
        repository=repository,
        window_cache_minutes=settings.window_cache_minutes,
        default_metrics_window=settings.default_metrics_window,
        abs_threshold=settings.abs_min_funding_rate,
    )


def _create_candidate_history_service(
    repository: FundingRepository, settings: Settings
) -> FundingHistoryService:
    window_minutes = max(
        settings.window_cache_minutes,
        settings.candidate_long_window_minutes,
        settings.candidate_primary_window_minutes,
        settings.candidate_short_window_minutes,
    )
    return FundingHistoryService(
        repository=repository,
        window_cache_minutes=window_minutes,
        default_metrics_window=settings.candidate_primary_window_minutes,
        abs_threshold=settings.candidate_min_funding_rate,
    )


def _candidate_config_from_settings(settings: Settings) -> CandidateEngineConfig:
    return candidate_config_from_settings(settings)


def _strategy_validation_config_from_args(
    args: argparse.Namespace,
) -> StrategyValidationConfig:
    return StrategyValidationConfig(
        funding_threshold=args.funding_threshold_rate,
        entry_mode=EntryMode(args.entry_mode),
        entry_minutes_before_funding=args.entry_minutes_before_funding,
        signal_confirmation_minutes=args.signal_confirmation_minutes,
        minimum_persistence_ratio=args.minimum_persistence_ratio,
        maximum_funding_std=args.maximum_funding_std,
        maximum_prediction_drop=args.maximum_prediction_drop,
        minimum_history_minutes=args.minimum_history_minutes,
        maximum_snapshot_age_seconds=args.maximum_snapshot_age_seconds,
        exit_minutes_after_funding=args.exit_minutes_after_funding,
        position_notional=args.position_notional,
        validation_mode=ValidationMode(args.validation_mode),
    )


def _strategy_validation_dataset_from_args(
    args: argparse.Namespace,
) -> StrategyValidationDataset:
    return StrategyValidationDataset(
        period_start=text_to_datetime(args.period_start)
        if args.period_start is not None
        else None,
        period_end=text_to_datetime(args.period_end)
        if args.period_end is not None
        else None,
        requested_symbols=_strategy_symbols_from_args(args),
        limit=args.limit,
    )


def _strategy_parameter_grid_from_args(
    args: argparse.Namespace,
) -> StrategyParameterGrid:
    base = _strategy_validation_config_from_args(args)
    return StrategyParameterGrid(
        funding_thresholds=_decimal_grid(args.funding_threshold_rates)
        or (base.funding_threshold,),
        entry_modes=_entry_mode_grid(args.entry_modes) or (base.entry_mode,),
        entry_minutes_before_funding=_int_grid(args.entry_minutes_grid)
        or (base.entry_minutes_before_funding,),
        signal_confirmation_minutes=_int_grid(args.signal_confirmation_minutes_grid)
        or (base.signal_confirmation_minutes,),
        minimum_persistence_ratios=_decimal_grid(args.minimum_persistence_ratios)
        or (base.minimum_persistence_ratio,),
        maximum_funding_stds=_decimal_grid(args.maximum_funding_stds)
        or (base.maximum_funding_std,),
        exit_minutes_after_funding=_int_grid(args.exit_minutes_grid)
        or (base.exit_minutes_after_funding,),
        max_combinations=args.max_combinations,
    )


def _strategy_symbols_from_args(args: argparse.Namespace) -> tuple[str, ...]:
    values: list[str] = []
    if args.symbol:
        values.extend(args.symbol)
    if args.symbols:
        values.extend(args.symbols.split(","))
    return tuple(
        dict.fromkeys(symbol.strip().upper() for symbol in values if symbol.strip())
    )


def _symbols_arg(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    return tuple(
        dict.fromkeys(symbol.strip().upper() for symbol in value.split(",") if symbol.strip())
    )


def _decimal_grid(value: str | None) -> tuple[Decimal, ...]:
    if value is None:
        return ()
    return tuple(decimal_from_text(item.strip()) for item in value.split(",") if item.strip())


def _int_grid(value: str | None) -> tuple[int, ...]:
    if value is None:
        return ()
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def _entry_mode_grid(value: str | None) -> tuple[EntryMode, ...]:
    if value is None:
        return ()
    return tuple(EntryMode(item.strip()) for item in value.split(",") if item.strip())


def _print_validation_grid_summaries(
    summaries: tuple[StrategyValidationSummary, ...],
) -> None:
    print("run_id total_events processed_events successful_events failed_events")
    for summary in summaries:
        print(
            " ".join(
                [
                    str(summary.run_id or ""),
                    str(summary.total_events),
                    str(summary.processed_events),
                    str(summary.successful_events),
                    str(summary.failed_events),
                ]
            )
        )


def _print_validation_compare(
    rows: list[
        tuple[
            int,
            StrategyValidationRun | None,
            tuple[StrategyValidationAggregate, ...],
        ]
    ],
) -> None:
    print(
        "run_id status mode threshold entry_mode events signals success_rate "
        "gross_return_rate_mean"
    )
    for run_id, run, aggregates in rows:
        overall = next(
            (
                aggregate
                for aggregate in aggregates
                if aggregate.grouping_type == "overall"
                and aggregate.grouping_key == "all"
            ),
            None,
        )
        metrics = overall.metrics if overall is not None else {}
        configuration = run.configuration if run is not None else {}
        print(
            " ".join(
                [
                    str(run_id),
                    run.status.value if run is not None else "missing",
                    run.validation_mode.value if run is not None else "-",
                    _optional_text(configuration.get("funding_threshold")),
                    _optional_text(configuration.get("entry_mode")),
                    str(metrics.get("event_count", "")),
                    str(metrics.get("signal_detected_count", "")),
                    str(metrics.get("success_rate", "")),
                    str(metrics.get("gross_return_rate_mean", "")),
                ]
            )
        )


def _decimal_arg(value: str) -> Decimal:
    return decimal_from_text(value)


def _print_history_summary(summary: WindowCacheSummary) -> None:
    print(f"symbols_cached: {summary.symbols_cached}")
    print(f"snapshots_in_cache: {summary.snapshots_in_cache}")
    print(f"cache_memory_estimate_bytes: {summary.cache_memory_estimate_bytes}")
    print(f"window_size_minutes: {summary.window_size_minutes}")
    print(f"cache_oldest: {summary.cache_oldest or ''}")
    print(f"cache_newest: {summary.cache_newest or ''}")


def _print_mapping_status_summary(summary: InstrumentMappingSummary) -> None:
    if not summary.table_available:
        print("instrument_mappings: unavailable")
        print("futures_with_spot: 0")
        print("futures_without_spot: 0")
        print("ambiguous_spot_mappings: 0")
        print("unsupported_mappings: 0")
        print("spot_trading_disabled: 0")
        print("positive_strategy_available: 0")
        print("negative_strategy_available: 0")
        print("negative_strategy_pending_borrow_check: 0")
        print("mappings_last_updated_at: ")
        return
    print(f"instrument_mappings: {summary.futures_symbols_processed}")
    print(f"futures_with_spot: {summary.futures_with_spot}")
    print(f"futures_without_spot: {summary.futures_without_spot}")
    print(f"ambiguous_spot_mappings: {summary.ambiguous}")
    print(f"unsupported_mappings: {summary.unsupported}")
    print(f"spot_trading_disabled: {summary.spot_trading_disabled}")
    print(f"positive_strategy_available: {summary.positive_strategy_available}")
    print(f"negative_strategy_available: {summary.negative_strategy_available}")
    print(
        "negative_strategy_pending_borrow_check: "
        f"{summary.negative_strategy_pending_borrow_implementation}"
    )
    print(f"mappings_last_updated_at: {summary.mappings_last_updated_at or ''}")


def _print_instrument_mapping_summary(summary: InstrumentMappingSummary) -> None:
    if not summary.table_available:
        print("instrument_mappings: unavailable")
        print("migration_required: python -m funding_monitor migrate")
        return
    print(f"futures_symbols_processed: {summary.futures_symbols_processed}")
    print(f"futures_with_spot: {summary.futures_with_spot}")
    print(f"futures_without_spot: {summary.futures_without_spot}")
    print(f"matched: {summary.matched}")
    print(f"missing: {summary.missing}")
    print(f"ambiguous: {summary.ambiguous}")
    print(f"unsupported: {summary.unsupported}")
    print(f"spot_trading_disabled: {summary.spot_trading_disabled}")
    print(f"positive_strategy_available: {summary.positive_strategy_available}")
    print(f"negative_strategy_available: {summary.negative_strategy_available}")
    print(
        "negative_strategy_pending_borrow_implementation: "
        f"{summary.negative_strategy_pending_borrow_implementation}"
    )
    print(f"mappings_last_updated_at: {summary.mappings_last_updated_at or ''}")


def _print_mapping_sync_result(result: InstrumentMappingSyncResult) -> None:
    print(f"futures_symbols_processed: {result.futures_symbols_processed}")
    print(f"matched: {result.matched}")
    print(f"missing: {result.missing}")
    print(f"ambiguous: {result.ambiguous}")
    print(f"unsupported: {result.unsupported}")
    print(f"spot_trading_disabled: {result.spot_trading_disabled}")
    print(f"positive_strategy_available: {result.positive_strategy_available}")
    print(f"negative_strategy_available: {result.negative_strategy_available}")
    print(
        "negative_strategy_pending_borrow_implementation: "
        f"{result.negative_strategy_pending_borrow_implementation}"
    )
    print(
        "synchronization_duration_seconds: "
        f"{result.synchronization_duration_seconds:.3f}"
    )
    print(f"updated_at: {result.updated_at.isoformat()}")


def _print_mapping_details(mappings: list[InstrumentMapping]) -> None:
    print(
        "futures_symbol futures_base_asset futures_contract_type futures_status "
        "spot_symbol spot_mapping_status mapping_reason "
        "positive_strategy_available negative_strategy_available "
        "negative_strategy_status mapping_updated_at"
    )
    for mapping in mappings:
        print(
            " ".join(
                [
                    _optional_text(mapping.futures_symbol),
                    _optional_text(mapping.futures_base_asset),
                    _optional_text(mapping.futures_contract_type),
                    _optional_text(mapping.futures_status),
                    _optional_text(mapping.spot_symbol) or "-",
                    _optional_text(mapping.spot_mapping_status.value),
                    _optional_text(
                        mapping.mapping_reason.value
                        if mapping.mapping_reason
                        else None
                    )
                    or "-",
                    str(mapping.positive_strategy_available).lower(),
                    str(mapping.negative_strategy_available).lower(),
                    _optional_text(mapping.negative_strategy_status.value),
                    _optional_text(mapping.mapping_updated_at.isoformat()),
                ]
            )
        )


def _filter_candidate_evaluations(
    evaluations: list[CandidateEvaluation],
    *,
    statuses: list[CandidateStatus],
    symbol: str | None,
    min_score: Decimal | None,
    include_rejected: bool,
    limit: int,
) -> list[CandidateEvaluation]:
    rejected_statuses = {
        CandidateStatus.REJECTED,
        CandidateStatus.STALE,
        CandidateStatus.INSUFFICIENT_HISTORY,
        CandidateStatus.EXPIRED,
    }
    filtered = []
    for evaluation in evaluations:
        if statuses and evaluation.status not in statuses:
            continue
        if symbol is not None and evaluation.futures_symbol != symbol:
            continue
        if min_score is not None and evaluation.total_score < min_score:
            continue
        if not statuses and not include_rejected and evaluation.status in rejected_statuses:
            continue
        filtered.append(evaluation)
    return rank_evaluations(filtered)[:limit]


def _print_candidates(evaluations: list[CandidateEvaluation]) -> None:
    print(
        "rank futures_symbol spot_symbol predicted_funding score status "
        "persistence velocity std signal_age_seconds minutes_to_funding "
        "flags reasons"
    )
    for rank, evaluation in enumerate(evaluations, start=1):
        print(
            " ".join(
                [
                    str(rank),
                    _optional_text(evaluation.futures_symbol),
                    _optional_text(evaluation.spot_symbol) or "-",
                    _optional_text(evaluation.predicted_funding_rate),
                    _optional_text(evaluation.total_score),
                    _optional_text(evaluation.status.value),
                    _optional_text(evaluation.persistence_ratio),
                    _optional_text(evaluation.velocity),
                    _optional_text(evaluation.standard_deviation),
                    _optional_text(evaluation.signal_age_seconds),
                    _optional_text(evaluation.minutes_to_funding),
                    _reason_codes(evaluation.warning_flags),
                    _reason_codes(evaluation.rejection_reasons),
                ]
            )
        )


def _print_candidates_json(evaluations: list[CandidateEvaluation]) -> None:
    rows = [_candidate_json_row(evaluation) for evaluation in evaluations]
    print(json.dumps(rows, ensure_ascii=False, sort_keys=True))


def _candidate_json_row(evaluation: CandidateEvaluation) -> dict[str, object]:
    return {
        "futures_symbol": evaluation.futures_symbol,
        "spot_symbol": evaluation.spot_symbol,
        "evaluated_at": evaluation.evaluated_at.isoformat(),
        "next_funding_time": evaluation.next_funding_time.isoformat()
        if evaluation.next_funding_time is not None
        else None,
        "predicted_funding_rate": str(evaluation.predicted_funding_rate),
        "minimum_funding_rate": str(evaluation.minimum_funding_rate),
        "minutes_to_funding": str(evaluation.minutes_to_funding)
        if evaluation.minutes_to_funding is not None
        else None,
        "status": evaluation.status.value,
        "total_score": str(evaluation.total_score),
        "score_details": evaluation.score_details,
        "persistence_ratio": str(evaluation.persistence_ratio)
        if evaluation.persistence_ratio is not None
        else None,
        "standard_deviation": str(evaluation.standard_deviation)
        if evaluation.standard_deviation is not None
        else None,
        "velocity": str(evaluation.velocity)
        if evaluation.velocity is not None
        else None,
        "acceleration": str(evaluation.acceleration)
        if evaluation.acceleration is not None
        else None,
        "signal_age_seconds": evaluation.signal_age_seconds,
        "snapshot_count": evaluation.snapshot_count,
        "history_duration_seconds": evaluation.history_duration_seconds,
        "rejection_reasons": [
            reason.value for reason in evaluation.rejection_reasons
        ],
        "warning_flags": [flag.value for flag in evaluation.warning_flags],
        "metrics_details": evaluation.metrics_details,
        "engine_version": evaluation.engine_version,
    }


def _print_candidate_rejections(
    aggregates: list[CandidateRejectionAggregate],
) -> None:
    print("reason symbol_count percentage examples")
    for aggregate in aggregates:
        print(
            " ".join(
                [
                    aggregate.reason.value,
                    str(aggregate.symbol_count),
                    str(aggregate.percentage),
                    ",".join(aggregate.examples) or "-",
                ]
            )
        )


def _print_interval_build_result(result: FundingIntervalBuildResult) -> None:
    print(f"processed: {result.processed}")
    print(f"created: {result.created}")
    print(f"updated: {result.updated}")
    print(f"partial: {result.partial}")
    print(f"skipped: {result.skipped}")
    print(f"failed: {result.failed}")


def _print_collector_health(diagnostics: PipelineDiagnostics) -> None:
    print(f"latest_snapshot: {diagnostics.latest_snapshot_at or ''}")
    print(f"snapshot_age_seconds: {diagnostics.snapshot_age_seconds or ''}")
    print(f"expected_active_symbols: {diagnostics.expected_active_symbols}")
    for window in diagnostics.windows:
        print(f"snapshots_last_{window.minutes}m: {window.snapshots}")
        print(f"unique_symbols_last_{window.minutes}m: {window.unique_symbols}")
        print(f"coverage_ratio_last_{window.minutes}m: {window.coverage_ratio}")
    coverage = diagnostics.coverage
    print(f"expected_symbols: {coverage.expected_symbols}")
    print(f"observed_symbols: {coverage.observed_symbols}")
    print(f"missing_symbols: {coverage.missing_symbols}")
    print(f"coverage_ratio: {coverage.coverage_ratio}")
    print(f"snapshot_gap_seconds: {coverage.snapshot_gap_seconds or ''}")
    print(f"maximum_gap_seconds: {coverage.maximum_gap_seconds or ''}")
    print(f"median_gap_seconds: {coverage.median_gap_seconds or ''}")
    print(
        "latest_confirmed_funding_event: "
        f"{diagnostics.latest_confirmed_funding_event or ''}"
    )
    print(f"pending_confirmations: {diagnostics.pending_confirmations}")
    print(f"failed_confirmations: {diagnostics.failed_confirmations}")
    print(
        "latest_candidate_evaluation: "
        f"{diagnostics.latest_candidate_evaluation or ''}"
    )
    print(
        "latest_funding_interval_summary: "
        f"{diagnostics.latest_funding_interval_summary or ''}"
    )
    print(f"warnings: {','.join(diagnostics.warnings) or '-'}")


def _print_pipeline_status(diagnostics: PipelineDiagnostics) -> None:
    last_hour = next(
        (window for window in diagnostics.windows if window.minutes == 60),
        None,
    )
    print(f"snapshots_total: {diagnostics.snapshots_total}")
    print(f"snapshots_last_hour: {last_hour.snapshots if last_hour else 0}")
    print(
        "symbols_covered_last_hour: "
        f"{last_hour.unique_symbols if last_hour else 0}"
    )
    print("events_by_status:")
    for status, count in sorted(diagnostics.events_by_status.items()):
        print(f"{status}: {count}")
    print(f"future_confirmations: {diagnostics.future_confirmations}")
    print(f"pending_confirmations: {diagnostics.pending_confirmations}")
    print(f"failed_confirmations: {diagnostics.failed_confirmations}")
    print(f"overdue_confirmations: {diagnostics.overdue_confirmations}")
    print(f"invalid_events: {diagnostics.invalid_events}")
    print(
        "candidate_evaluations_last_hour: "
        f"{diagnostics.candidate_evaluations_last_hour}"
    )
    print(
        "interval_summaries_last_24h: "
        f"{diagnostics.interval_summaries_last_24h}"
    )
    print(f"interval_summary_backlog: {diagnostics.interval_summary_backlog}")
    print(f"latest_snapshot_at: {diagnostics.latest_snapshot_at or ''}")
    print(
        "latest_confirmed_funding_event: "
        f"{diagnostics.latest_confirmed_funding_event or ''}"
    )
    print(
        "latest_candidate_evaluation: "
        f"{diagnostics.latest_candidate_evaluation or ''}"
    )
    print(
        "latest_funding_interval_summary: "
        f"{diagnostics.latest_funding_interval_summary or ''}"
    )
    print(f"warnings: {','.join(diagnostics.warnings) or '-'}")


def _print_symbol_coverage(rows: list[SymbolCoverageRow]) -> None:
    print(
        "symbol snapshot_count latest_snapshot_at snapshot_age_seconds "
        "maximum_gap_seconds median_gap_seconds"
    )
    for row in rows:
        print(
            " ".join(
                [
                    row.symbol,
                    str(row.snapshot_count),
                    _optional_text(
                        row.latest_snapshot_at.isoformat()
                        if row.latest_snapshot_at is not None
                        else None
                    )
                    or "-",
                    _optional_text(row.snapshot_age_seconds) or "-",
                    _optional_text(row.maximum_gap_seconds) or "-",
                    _optional_text(row.median_gap_seconds) or "-",
                ]
            )
        )


def _print_candidate_pipeline_result(
    result: CandidateEvaluationPipelineResult,
) -> None:
    print(f"evaluated: {result.evaluated}")
    print(f"persisted: {result.persisted}")
    print(f"failed: {result.failed}")
    print(f"dry_run: {str(result.dry_run).lower()}")
    print(f"evaluated_at: {result.evaluated_at.isoformat()}")
    print("statuses:")
    for status, count in sorted(result.statuses.items()):
        print(f"{status}: {count}")


def _print_interval_backfill_result(result: FundingIntervalBackfillResult) -> None:
    print(f"processed: {result.processed}")
    print(f"created: {result.created}")
    print(f"updated: {result.updated}")
    print(f"persisted: {result.persisted}")
    print(f"complete: {result.complete}")
    print(f"partial: {result.partial}")
    print(f"insufficient_history: {result.insufficient_history}")
    print(f"failed: {result.failed}")
    print(f"dry_run: {str(result.dry_run).lower()}")
    print("reasons:")
    for reason, count in sorted(result.reasons.items()):
        print(f"{reason}: {count}")


def _print_confirmation_backfill_result(result: ConfirmationBackfillResult) -> None:
    print(f"checked: {result.checked}")
    print(f"confirmed: {result.confirmed}")
    print(f"not_found: {result.not_found}")
    print(f"failed: {result.failed}")
    print(f"retry_failed: {str(result.retry_failed).lower()}")


def _reason_codes(reasons: tuple[RejectionReason, ...]) -> str:
    return ",".join(reason.value for reason in reasons) or "-"


def _print_metrics(metrics: FundingMetrics) -> None:
    print(f"current: {_optional_text(metrics.current_rate)}")
    print(f"mean: {_optional_text(metrics.mean_rate)}")
    print(f"median: {_optional_text(metrics.median_rate)}")
    print(f"min: {_optional_text(metrics.min_rate)}")
    print(f"max: {_optional_text(metrics.max_rate)}")
    print(f"std: {_optional_text(metrics.std_rate)}")
    print(f"threshold_persistence: {metrics.threshold_persistence}")
    print(f"direction: {_optional_text(metrics.current_direction)}")
    print(f"direction_changes: {metrics.direction_changes}")
    print(f"velocity: {_optional_text(metrics.rate_velocity)}")
    print(f"acceleration: {_optional_text(metrics.rate_acceleration)}")
    print(f"snapshot_count: {metrics.snapshot_count}")


def _optional_text(value: object | None) -> str:
    if value is None:
        return ""
    text = str(value)
    encoding = sys.stdout.encoding or "utf-8"
    return text.encode(encoding, errors="replace").decode(encoding)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value


def _print_snapshot_stats(stats: dict[str, object]) -> None:
    print(f"total_snapshots: {stats['total_snapshots']}")
    print(f"symbols_represented: {stats['symbols_represented']}")
    print(f"positive_count: {stats['positive_count']}")
    print(f"negative_count: {stats['negative_count']}")
    print(f"neutral_count: {stats['neutral_count']}")
    print(f"above_threshold_count: {stats['above_threshold_count']}")
    print(f"below_threshold_count: {stats['below_threshold_count']}")
    print(f"min_funding_rate: {stats['min_funding_rate'] or ''}")
    print(f"max_funding_rate: {stats['max_funding_rate'] or ''}")
    print(
        "average_absolute_funding_rate: "
        f"{stats['average_absolute_funding_rate'] or ''}"
    )
    print(f"earliest_next_funding: {stats['earliest_next_funding'] or ''}")
    print(f"latest_next_funding: {stats['latest_next_funding'] or ''}")
    print(f"newest_snapshot: {stats['newest_snapshot'] or ''}")
    print(f"oldest_snapshot: {stats['oldest_snapshot'] or ''}")


def _print_recent_events(events: list[FundingEvent]) -> None:
    print(
        "symbol funding_time last_predicted_rate_pct "
        "actual_funding_rate_pct prediction_error_pp status"
    )
    for event in events:
        print(
            " ".join(
                [
                    event.symbol,
                    datetime_to_text(event.funding_time),
                    decimal_to_percent_text(event.last_predicted_rate),
                    decimal_to_percent_text(event.actual_funding_rate),
                    decimal_to_percentage_point_text(event.prediction_error),
                    event.status,
                ]
            )
        )
