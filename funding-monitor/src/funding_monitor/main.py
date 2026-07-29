from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .binance_rest import BinanceRestClient
from .collector import run_collector
from .config import Settings, load_settings
from .database import PostgresDatabase
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
                count = await SymbolService(repository, rest_client).sync_symbols()
        print(f"synced symbols: {count}")
        return

    if args.command == "status":
        async with database:
            repository = FundingRepository(database)
            summary = await repository.status_summary()
        print(f"active_symbols: {summary['active_symbols']}")
        print(f"snapshots: {summary['snapshot_count']}")
        print(f"funding_events: {summary['event_count']}")
        print(f"last_snapshot: {summary['last_snapshot'] or ''}")
        print(f"waiting: {summary['waiting']}")
        print(f"confirmed: {summary['confirmed']}")
        print(f"confirmation_failed: {summary['confirmation_failed']}")
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

    recent_parser = subparsers.add_parser("recent-events")
    recent_parser.add_argument("--limit", type=int, default=20)

    export_parser = subparsers.add_parser("export-csv")
    export_parser.add_argument("--output", type=Path, required=True)

    return parser


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
