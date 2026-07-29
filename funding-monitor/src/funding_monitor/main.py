from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .binance_rest import BinanceRestClient
from .collector import run_collector
from .config import Settings
from .database import initialize_database
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
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(_run(args, settings))


async def _run(args: argparse.Namespace, settings: Settings) -> None:
    repository = FundingRepository(settings.database_path)

    if args.command == "init-db":
        await initialize_database(settings.database_path)
        print(f"initialized database: {settings.database_path}")
        return

    if args.command == "sync-symbols":
        await initialize_database(settings.database_path)
        async with BinanceRestClient(
            timeout_seconds=settings.rest_timeout_seconds
        ) as rest_client:
            count = await SymbolService(repository, rest_client).sync_symbols()
        print(f"synced symbols: {count}")
        return

    if args.command == "collect":
        count = await run_collector(
            settings,
            max_messages=args.max_messages,
            max_seconds=args.max_seconds,
        )
        print(f"collected snapshots: {count}")
        return

    if args.command == "status":
        await initialize_database(settings.database_path)
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
        await initialize_database(settings.database_path)
        events = await repository.recent_events(args.limit)
        _print_recent_events(events)
        return

    if args.command == "export-csv":
        await initialize_database(settings.database_path)
        count = await repository.export_events_csv(args.output)
        print(f"exported rows: {count}")
        print(f"output: {args.output}")
        return

    raise RuntimeError(f"unknown command: {args.command}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m funding_monitor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db")
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
