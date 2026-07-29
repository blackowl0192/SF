from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from .binance_rest import BinanceRestClient, BinanceSpotRestClient
from .collector import run_collector
from .config import Settings, load_settings
from .database import PostgresDatabase
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
    decimal_to_percent_text,
    decimal_to_percentage_point_text,
)
from .repository import FundingRepository
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
            result = await database.check_connection()
        print(f"connected: {str(result.connected).lower()}")
        print(f"postgres_version: {result.postgres_version}")
        print(f"database_utc_time: {result.database_utc_time.isoformat()}")
        print(f"applied_migrations: {result.applied_migrations}")
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

    recent_parser = subparsers.add_parser("recent-events")
    recent_parser.add_argument("--limit", type=int, default=20)

    export_parser = subparsers.add_parser("export-csv")
    export_parser.add_argument("--output", type=Path, required=True)

    return parser


def _create_history_service(
    repository: FundingRepository, settings: Settings
) -> FundingHistoryService:
    return FundingHistoryService(
        repository=repository,
        window_cache_minutes=settings.window_cache_minutes,
        default_metrics_window=settings.default_metrics_window,
        abs_threshold=settings.abs_min_funding_rate,
    )


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
