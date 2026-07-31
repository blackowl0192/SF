from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import asyncpg

from .config import Settings
from .database import PostgresDatabase
from .models import ensure_utc

HEALTH_WINDOWS_MINUTES = (1, 5, 15, 60)
KNOWN_EVENT_STATUSES = ("waiting", "confirmed", "confirmation_failed")


@dataclass(frozen=True)
class WindowSnapshotMetrics:
    minutes: int
    snapshots: int
    unique_symbols: int
    coverage_ratio: Decimal


@dataclass(frozen=True)
class CoverageMetrics:
    expected_symbols: int
    observed_symbols: int
    missing_symbols: int
    coverage_ratio: Decimal
    snapshot_gap_seconds: int | None
    maximum_gap_seconds: int | None
    median_gap_seconds: Decimal | None


@dataclass(frozen=True)
class PipelineDiagnostics:
    snapshots_total: int
    latest_snapshot_at: datetime | None
    snapshot_age_seconds: int | None
    expected_active_symbols: int
    windows: tuple[WindowSnapshotMetrics, ...]
    coverage: CoverageMetrics
    events_by_status: dict[str, int]
    future_confirmations: int
    pending_confirmations: int
    failed_confirmations: int
    overdue_confirmations: int
    invalid_events: int
    latest_confirmed_funding_event: datetime | None
    latest_candidate_evaluation: datetime | None
    latest_funding_interval_summary: datetime | None
    candidate_evaluations_last_hour: int
    interval_summaries_last_24h: int
    interval_summary_backlog: int
    warnings: tuple[str, ...]
    critical: bool


@dataclass(frozen=True)
class SymbolCoverageRow:
    symbol: str
    snapshot_count: int
    latest_snapshot_at: datetime | None
    snapshot_age_seconds: int | None
    maximum_gap_seconds: int | None
    median_gap_seconds: Decimal | None


class PipelineDiagnosticsService:
    def __init__(
        self,
        *,
        database: PostgresDatabase,
        settings: Settings,
    ) -> None:
        self.database = database
        self.settings = settings

    async def collect(self) -> PipelineDiagnostics:
        try:
            async with self.database.acquire() as connection:
                now = ensure_utc(await connection.fetchval("SELECT NOW()"))
                expected_symbols = int(
                    await connection.fetchval(
                        "SELECT COUNT(*) FROM symbols WHERE is_active = TRUE"
                    )
                    or 0
                )
                latest_snapshot_at = await connection.fetchval(
                    "SELECT MAX(received_at) FROM funding_snapshots"
                )
                snapshots_total = int(
                    await connection.fetchval("SELECT COUNT(*) FROM funding_snapshots")
                    or 0
                )
                latest_snapshot_at = (
                    ensure_utc(latest_snapshot_at)
                    if latest_snapshot_at is not None
                    else None
                )
                window_rows: list[WindowSnapshotMetrics] = []
                for minutes in HEALTH_WINDOWS_MINUTES:
                    window_rows.append(
                        await self._window_metrics(
                            connection,
                            minutes,
                            expected_symbols,
                        )
                    )
                windows = tuple(window_rows)
                coverage = await self._coverage_metrics(
                    connection,
                    minutes=self.settings.collector_health_window_minutes,
                    expected_symbols=expected_symbols,
                    latest_snapshot_at=latest_snapshot_at,
                    now=now,
                )
                event_rows = await connection.fetch(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM funding_events
                    GROUP BY status
                    """
                )
                events_by_status = {
                    row["status"]: int(row["count"]) for row in event_rows
                }
                confirmation_row = await connection.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            WHERE status = 'waiting' AND funding_time > NOW()
                        ) AS future,
                        COUNT(*) FILTER (
                            WHERE status = 'waiting' AND funding_time <= NOW()
                        ) AS pending,
                        COUNT(*) FILTER (
                            WHERE status = 'confirmation_failed'
                        ) AS failed,
                        COUNT(*) FILTER (
                            WHERE status = 'waiting'
                              AND funding_time <= NOW() - make_interval(
                                  mins => $1::int
                              )
                        ) AS overdue,
                        COUNT(*) FILTER (
                            WHERE status <> ALL($2::text[])
                        ) AS invalid
                    FROM funding_events
                    """,
                    self.settings.confirmation_overdue_grace_minutes,
                    list(KNOWN_EVENT_STATUSES),
                )
                latest_confirmed = await connection.fetchval(
                    """
                    SELECT MAX(funding_time)
                    FROM funding_events
                    WHERE status = 'confirmed'
                    """
                )
                latest_candidate = await _optional_table_fetchval(
                    connection,
                    "SELECT MAX(evaluated_at) FROM candidate_evaluations",
                )
                latest_summary = await _optional_table_fetchval(
                    connection,
                    "SELECT MAX(updated_at) FROM funding_interval_summaries",
                )
                candidate_last_hour = int(
                    await _optional_table_fetchval(
                        connection,
                        """
                        SELECT COUNT(*)
                        FROM candidate_evaluations
                        WHERE evaluated_at >= NOW() - INTERVAL '1 hour'
                        """,
                    )
                    or 0
                )
                summaries_last_24h = int(
                    await _optional_table_fetchval(
                        connection,
                        """
                        SELECT COUNT(*)
                        FROM funding_interval_summaries
                        WHERE updated_at >= NOW() - INTERVAL '24 hours'
                        """,
                    )
                    or 0
                )
                summary_backlog = int(
                    await _optional_table_fetchval(
                        connection,
                        """
                        SELECT COUNT(*)
                        FROM funding_events fe
                        LEFT JOIN funding_interval_summaries fis
                          ON fis.exchange = 'BINANCE'
                         AND fis.futures_symbol = fe.symbol
                         AND fis.funding_time = fe.funding_time
                        WHERE fe.status = 'confirmed'
                          AND fe.actual_funding_rate IS NOT NULL
                          AND fis.id IS NULL
                        """,
                    )
                    or 0
                )
        except asyncpg.UndefinedTableError:
            return _empty_diagnostics()

        latest_candidate = (
            ensure_utc(latest_candidate) if latest_candidate is not None else None
        )
        latest_summary = ensure_utc(latest_summary) if latest_summary is not None else None
        latest_confirmed = (
            ensure_utc(latest_confirmed) if latest_confirmed is not None else None
        )
        snapshot_age = (
            int((now - latest_snapshot_at).total_seconds())
            if latest_snapshot_at is not None
            else None
        )
        warnings = _diagnostic_warnings(
            latest_snapshot_age_seconds=snapshot_age,
            coverage=coverage,
            latest_candidate_evaluation=latest_candidate,
            overdue_confirmations=int(confirmation_row["overdue"] or 0),
            interval_summary_backlog=summary_backlog,
            settings=self.settings,
            now=now,
        )
        critical = any(
            warning
            in {
                "SNAPSHOT_COLLECTION_STALE",
                "LOW_SYMBOL_COVERAGE",
                "CONFIRMATION_BACKLOG",
                "CANDIDATE_PIPELINE_NOT_RUNNING",
            }
            for warning in warnings
        )
        return PipelineDiagnostics(
            snapshots_total=snapshots_total,
            latest_snapshot_at=latest_snapshot_at,
            snapshot_age_seconds=snapshot_age,
            expected_active_symbols=expected_symbols,
            windows=windows,
            coverage=coverage,
            events_by_status=events_by_status,
            future_confirmations=int(confirmation_row["future"] or 0),
            pending_confirmations=int(confirmation_row["pending"] or 0),
            failed_confirmations=int(confirmation_row["failed"] or 0),
            overdue_confirmations=int(confirmation_row["overdue"] or 0),
            invalid_events=int(confirmation_row["invalid"] or 0),
            latest_confirmed_funding_event=latest_confirmed,
            latest_candidate_evaluation=latest_candidate,
            latest_funding_interval_summary=latest_summary,
            candidate_evaluations_last_hour=candidate_last_hour,
            interval_summaries_last_24h=summaries_last_24h,
            interval_summary_backlog=summary_backlog,
            warnings=warnings,
            critical=critical,
        )

    async def worst_symbol_coverage(
        self,
        *,
        minutes: int,
        limit: int,
    ) -> list[SymbolCoverageRow]:
        async with self.database.acquire() as connection:
            rows = await connection.fetch(
                """
                WITH active AS (
                    SELECT symbol
                    FROM symbols
                    WHERE is_active = TRUE
                ),
                recent AS (
                    SELECT
                        symbol,
                        COUNT(*) AS snapshot_count,
                        MAX(received_at) AS latest_snapshot_at
                    FROM funding_snapshots
                    WHERE received_at >= NOW() - make_interval(mins => $1::int)
                    GROUP BY symbol
                ),
                ordered AS (
                    SELECT
                        symbol,
                        received_at,
                        LAG(received_at) OVER (
                            PARTITION BY symbol ORDER BY received_at
                        ) AS previous_received_at
                    FROM funding_snapshots
                    WHERE received_at >= NOW() - make_interval(mins => $1::int)
                ),
                gaps AS (
                    SELECT
                        symbol,
                        MAX(
                            EXTRACT(EPOCH FROM received_at - previous_received_at)
                        )::integer AS maximum_gap_seconds,
                        percentile_cont(0.5) WITHIN GROUP (
                            ORDER BY EXTRACT(
                                EPOCH FROM received_at - previous_received_at
                            )
                        ) AS median_gap_seconds
                    FROM ordered
                    WHERE previous_received_at IS NOT NULL
                    GROUP BY symbol
                )
                SELECT
                    active.symbol,
                    COALESCE(recent.snapshot_count, 0) AS snapshot_count,
                    recent.latest_snapshot_at,
                    CASE
                        WHEN recent.latest_snapshot_at IS NULL THEN NULL
                        ELSE EXTRACT(
                            EPOCH FROM NOW() - recent.latest_snapshot_at
                        )::integer
                    END AS snapshot_age_seconds,
                    gaps.maximum_gap_seconds,
                    gaps.median_gap_seconds
                FROM active
                LEFT JOIN recent ON recent.symbol = active.symbol
                LEFT JOIN gaps ON gaps.symbol = active.symbol
                ORDER BY
                    COALESCE(recent.snapshot_count, 0),
                    recent.latest_snapshot_at ASC NULLS FIRST,
                    active.symbol
                LIMIT $2
                """,
                minutes,
                limit,
            )
        return [
            SymbolCoverageRow(
                symbol=row["symbol"],
                snapshot_count=int(row["snapshot_count"] or 0),
                latest_snapshot_at=ensure_utc(row["latest_snapshot_at"])
                if row["latest_snapshot_at"] is not None
                else None,
                snapshot_age_seconds=row["snapshot_age_seconds"],
                maximum_gap_seconds=row["maximum_gap_seconds"],
                median_gap_seconds=Decimal(str(row["median_gap_seconds"]))
                if row["median_gap_seconds"] is not None
                else None,
            )
            for row in rows
        ]

    async def _window_metrics(
        self,
        connection: Any,
        minutes: int,
        expected_symbols: int,
    ) -> WindowSnapshotMetrics:
        row = await connection.fetchrow(
            """
            SELECT
                COUNT(*) AS snapshots,
                COUNT(DISTINCT symbol) AS unique_symbols
            FROM funding_snapshots
            WHERE received_at >= NOW() - make_interval(mins => $1::int)
            """,
            minutes,
        )
        unique_symbols = int(row["unique_symbols"] or 0)
        return WindowSnapshotMetrics(
            minutes=minutes,
            snapshots=int(row["snapshots"] or 0),
            unique_symbols=unique_symbols,
            coverage_ratio=_ratio(unique_symbols, expected_symbols),
        )

    async def _coverage_metrics(
        self,
        connection: Any,
        *,
        minutes: int,
        expected_symbols: int,
        latest_snapshot_at: datetime | None,
        now: datetime,
    ) -> CoverageMetrics:
        row = await connection.fetchrow(
            """
            WITH recent AS (
                SELECT DISTINCT symbol
                FROM funding_snapshots
                WHERE received_at >= NOW() - make_interval(mins => $1::int)
            ),
            ordered AS (
                SELECT
                    symbol,
                    received_at,
                    LAG(received_at) OVER (
                        PARTITION BY symbol ORDER BY received_at
                    ) AS previous_received_at
                FROM funding_snapshots
                WHERE received_at >= NOW() - make_interval(mins => $1::int)
            ),
            gaps AS (
                SELECT EXTRACT(EPOCH FROM received_at - previous_received_at)
                    AS gap_seconds
                FROM ordered
                WHERE previous_received_at IS NOT NULL
            )
            SELECT
                (SELECT COUNT(*) FROM recent) AS observed_symbols,
                (SELECT MAX(gap_seconds)::integer FROM gaps)
                    AS maximum_gap_seconds,
                (SELECT percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY gap_seconds
                ) FROM gaps) AS median_gap_seconds
            """,
            minutes,
        )
        observed_symbols = int(row["observed_symbols"] or 0)
        return CoverageMetrics(
            expected_symbols=expected_symbols,
            observed_symbols=observed_symbols,
            missing_symbols=max(0, expected_symbols - observed_symbols),
            coverage_ratio=_ratio(observed_symbols, expected_symbols),
            snapshot_gap_seconds=int((now - latest_snapshot_at).total_seconds())
            if latest_snapshot_at is not None
            else None,
            maximum_gap_seconds=row["maximum_gap_seconds"],
            median_gap_seconds=Decimal(str(row["median_gap_seconds"]))
            if row["median_gap_seconds"] is not None
            else None,
        )


def diagnostics_to_dict(diagnostics: PipelineDiagnostics) -> dict[str, Any]:
    raw = asdict(diagnostics)
    return _jsonable(raw)


def coverage_rows_to_dict(rows: list[SymbolCoverageRow]) -> list[dict[str, Any]]:
    return [_jsonable(asdict(row)) for row in rows]


async def _optional_table_fetchval(connection: Any, query: str) -> Any:
    try:
        return await connection.fetchval(query)
    except asyncpg.UndefinedTableError:
        return None


def _diagnostic_warnings(
    *,
    latest_snapshot_age_seconds: int | None,
    coverage: CoverageMetrics,
    latest_candidate_evaluation: datetime | None,
    overdue_confirmations: int,
    interval_summary_backlog: int,
    settings: Settings,
    now: datetime,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if (
        latest_snapshot_age_seconds is None
        or latest_snapshot_age_seconds
        > settings.collector_health_max_snapshot_age_seconds
    ):
        warnings.append("SNAPSHOT_COLLECTION_STALE")
    if coverage.coverage_ratio < settings.collector_health_min_coverage_ratio:
        warnings.append("LOW_SYMBOL_COVERAGE")
    if overdue_confirmations > 0:
        warnings.append("CONFIRMATION_BACKLOG")
    if latest_candidate_evaluation is None:
        warnings.append("CANDIDATE_PIPELINE_NOT_RUNNING")
    else:
        age = int((now - latest_candidate_evaluation).total_seconds())
        if age > settings.candidate_evaluation_interval_seconds * 2:
            warnings.append("CANDIDATE_PIPELINE_NOT_RUNNING")
    if interval_summary_backlog > 0:
        warnings.append("INTERVAL_SUMMARY_BACKLOG")
    return tuple(warnings)


def _empty_diagnostics() -> PipelineDiagnostics:
    coverage = CoverageMetrics(
        expected_symbols=0,
        observed_symbols=0,
        missing_symbols=0,
        coverage_ratio=Decimal(0),
        snapshot_gap_seconds=None,
        maximum_gap_seconds=None,
        median_gap_seconds=None,
    )
    return PipelineDiagnostics(
        snapshots_total=0,
        latest_snapshot_at=None,
        snapshot_age_seconds=None,
        expected_active_symbols=0,
        windows=(),
        coverage=coverage,
        events_by_status={},
        future_confirmations=0,
        pending_confirmations=0,
        failed_confirmations=0,
        overdue_confirmations=0,
        invalid_events=0,
        latest_confirmed_funding_event=None,
        latest_candidate_evaluation=None,
        latest_funding_interval_summary=None,
        candidate_evaluations_last_hour=0,
        interval_summaries_last_24h=0,
        interval_summary_backlog=0,
        warnings=("SNAPSHOT_COLLECTION_STALE",),
        critical=True,
    )


def _ratio(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal(0)
    return (Decimal(numerator) / Decimal(denominator)).quantize(Decimal("0.000001"))


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value
