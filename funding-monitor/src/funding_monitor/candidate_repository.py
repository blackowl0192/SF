from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any

import asyncpg

from .candidate_engine import (
    DEFAULT_EXCHANGE,
    CandidateEvaluation,
    CandidateRejectionAggregate,
    CandidateStatus,
    FundingIntervalSummary,
    RejectionReason,
    ScoreComponents,
    rejection_aggregates,
)
from .database import PostgresDatabase
from .models import FundingEvent, FundingSnapshot, decimal_from_text, ensure_utc
from .repository import FundingRepository

UPSERT_CANDIDATE_EVALUATION_SQL = """
INSERT INTO candidate_evaluations (
    exchange,
    futures_symbol,
    spot_symbol,
    evaluated_at,
    evaluated_at_bucket,
    next_funding_time,
    predicted_funding_rate,
    minimum_funding_rate,
    minutes_to_funding,
    status,
    total_score,
    funding_score,
    persistence_score,
    stability_score,
    trend_score,
    lifetime_score,
    timing_score,
    total_penalty,
    persistence_ratio,
    standard_deviation,
    velocity,
    acceleration,
    threshold_crossings,
    direction_changes,
    signal_started_at,
    signal_age_seconds,
    snapshot_count,
    history_duration_seconds,
    latest_snapshot_at,
    rejection_reasons,
    warning_flags,
    score_details,
    metrics_details,
    engine_version
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
    $31, $32, $33, $34
)
ON CONFLICT(exchange, futures_symbol, evaluated_at_bucket, engine_version) DO UPDATE SET
    spot_symbol = excluded.spot_symbol,
    evaluated_at = excluded.evaluated_at,
    next_funding_time = excluded.next_funding_time,
    predicted_funding_rate = excluded.predicted_funding_rate,
    minimum_funding_rate = excluded.minimum_funding_rate,
    minutes_to_funding = excluded.minutes_to_funding,
    status = excluded.status,
    total_score = excluded.total_score,
    funding_score = excluded.funding_score,
    persistence_score = excluded.persistence_score,
    stability_score = excluded.stability_score,
    trend_score = excluded.trend_score,
    lifetime_score = excluded.lifetime_score,
    timing_score = excluded.timing_score,
    total_penalty = excluded.total_penalty,
    persistence_ratio = excluded.persistence_ratio,
    standard_deviation = excluded.standard_deviation,
    velocity = excluded.velocity,
    acceleration = excluded.acceleration,
    threshold_crossings = excluded.threshold_crossings,
    direction_changes = excluded.direction_changes,
    signal_started_at = excluded.signal_started_at,
    signal_age_seconds = excluded.signal_age_seconds,
    snapshot_count = excluded.snapshot_count,
    history_duration_seconds = excluded.history_duration_seconds,
    latest_snapshot_at = excluded.latest_snapshot_at,
    rejection_reasons = excluded.rejection_reasons,
    warning_flags = excluded.warning_flags,
    score_details = excluded.score_details,
    metrics_details = excluded.metrics_details
"""

SELECT_CANDIDATE_EVALUATION_COLUMNS = """
SELECT
    exchange,
    futures_symbol,
    spot_symbol,
    evaluated_at,
    evaluated_at_bucket,
    next_funding_time,
    predicted_funding_rate,
    minimum_funding_rate,
    minutes_to_funding,
    status,
    total_score,
    funding_score,
    persistence_score,
    stability_score,
    trend_score,
    lifetime_score,
    timing_score,
    total_penalty,
    persistence_ratio,
    standard_deviation,
    velocity,
    acceleration,
    threshold_crossings,
    direction_changes,
    signal_started_at,
    signal_age_seconds,
    snapshot_count,
    history_duration_seconds,
    latest_snapshot_at,
    rejection_reasons,
    warning_flags,
    score_details,
    metrics_details,
    engine_version
FROM candidate_evaluations
"""

UPSERT_INTERVAL_SUMMARY_SQL = """
INSERT INTO funding_interval_summaries (
    exchange,
    futures_symbol,
    funding_time,
    interval_started_at,
    interval_ended_at,
    realized_funding_rate,
    first_predicted_rate,
    last_predicted_rate,
    minimum_predicted_rate,
    maximum_predicted_rate,
    peak_predicted_at,
    mean_predicted_rate,
    median_predicted_rate,
    predicted_rate_120m_before,
    predicted_rate_60m_before,
    predicted_rate_30m_before,
    predicted_rate_15m_before,
    predicted_rate_5m_before,
    positive_snapshot_ratio,
    above_threshold_snapshot_ratio,
    above_threshold_duration_seconds,
    maximum_above_threshold_streak_seconds,
    signal_started_at,
    longest_positive_streak_seconds,
    threshold_crossings,
    direction_changes,
    prediction_error,
    absolute_prediction_error,
    snapshot_count,
    history_coverage_ratio,
    summary_status,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19,
    $20, $21, $22, $23, $24, $25, $26, $27, $28,
    $29, $30, $31, NOW()
)
ON CONFLICT(exchange, futures_symbol, funding_time) DO UPDATE SET
    interval_started_at = excluded.interval_started_at,
    interval_ended_at = excluded.interval_ended_at,
    realized_funding_rate = excluded.realized_funding_rate,
    first_predicted_rate = excluded.first_predicted_rate,
    last_predicted_rate = excluded.last_predicted_rate,
    minimum_predicted_rate = excluded.minimum_predicted_rate,
    maximum_predicted_rate = excluded.maximum_predicted_rate,
    peak_predicted_at = excluded.peak_predicted_at,
    mean_predicted_rate = excluded.mean_predicted_rate,
    median_predicted_rate = excluded.median_predicted_rate,
    predicted_rate_120m_before = excluded.predicted_rate_120m_before,
    predicted_rate_60m_before = excluded.predicted_rate_60m_before,
    predicted_rate_30m_before = excluded.predicted_rate_30m_before,
    predicted_rate_15m_before = excluded.predicted_rate_15m_before,
    predicted_rate_5m_before = excluded.predicted_rate_5m_before,
    positive_snapshot_ratio = excluded.positive_snapshot_ratio,
    above_threshold_snapshot_ratio = excluded.above_threshold_snapshot_ratio,
    above_threshold_duration_seconds = excluded.above_threshold_duration_seconds,
    maximum_above_threshold_streak_seconds =
        excluded.maximum_above_threshold_streak_seconds,
    signal_started_at = excluded.signal_started_at,
    longest_positive_streak_seconds = excluded.longest_positive_streak_seconds,
    threshold_crossings = excluded.threshold_crossings,
    direction_changes = excluded.direction_changes,
    prediction_error = excluded.prediction_error,
    absolute_prediction_error = excluded.absolute_prediction_error,
    snapshot_count = excluded.snapshot_count,
    history_coverage_ratio = excluded.history_coverage_ratio,
    summary_status = excluded.summary_status,
    updated_at = excluded.updated_at
"""


class CandidateRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database
        self._funding_repository = FundingRepository(database)

    async def upsert_evaluations(
        self,
        evaluations: Iterable[CandidateEvaluation],
    ) -> int:
        rows = list(evaluations)
        if not rows:
            return 0
        async with self.database.acquire() as connection:
            await connection.executemany(
                UPSERT_CANDIDATE_EVALUATION_SQL,
                [self._evaluation_args(evaluation) for evaluation in rows],
            )
        return len(rows)

    async def list_latest_evaluations(
        self,
        *,
        status: CandidateStatus | None = None,
        symbol: str | None = None,
        min_score: Decimal | None = None,
        include_rejected: bool = False,
        limit: int | None = None,
    ) -> list[CandidateEvaluation]:
        query = f"""
        WITH latest AS (
            SELECT DISTINCT ON (exchange, futures_symbol) *
            FROM candidate_evaluations
            ORDER BY exchange, futures_symbol, evaluated_at DESC
        )
        {SELECT_CANDIDATE_EVALUATION_COLUMNS.replace(
            "FROM candidate_evaluations",
            "FROM latest",
        )}
        """
        args: list[Any] = []
        conditions: list[str] = []
        if status is not None:
            args.append(status.value)
            conditions.append(f"status = ${len(args)}")
        if symbol is not None:
            args.append(symbol)
            conditions.append(f"futures_symbol = ${len(args)}")
        if min_score is not None:
            args.append(min_score)
            conditions.append(f"total_score >= ${len(args)}")
        if not include_rejected:
            conditions.append(
                "status NOT IN ('rejected', 'stale', 'insufficient_history', 'expired')"
            )
        if conditions:
            query += "\nWHERE " + " AND ".join(conditions)
        query += "\nORDER BY evaluated_at DESC, total_score DESC, futures_symbol"
        if limit is not None:
            args.append(limit)
            query += f"\nLIMIT ${len(args)}"

        try:
            async with self.database.acquire() as connection:
                rows = await connection.fetch(query, *args)
        except asyncpg.UndefinedTableError:
            return []
        return [self._row_to_evaluation(row) for row in rows]

    async def latest_rejection_summary(self) -> list[CandidateRejectionAggregate]:
        evaluations = await self.list_latest_evaluations(
            include_rejected=True,
            limit=None,
        )
        return rejection_aggregates(evaluations)

    async def confirmed_events_for_interval_summaries(
        self,
        limit: int,
    ) -> list[FundingEvent]:
        try:
            async with self.database.acquire() as connection:
                rows = await connection.fetch(
                    """
                    SELECT *
                    FROM funding_events
                    WHERE status = 'confirmed'
                      AND actual_funding_rate IS NOT NULL
                    ORDER BY funding_time DESC, symbol
                    LIMIT $1
                    """,
                    limit,
                )
        except asyncpg.UndefinedTableError:
            return []
        return [self._funding_repository._row_to_event(row) for row in rows]

    async def existing_interval_summary_keys(
        self,
        events: Iterable[FundingEvent],
    ) -> set[tuple[str, str, datetime]]:
        rows = list(events)
        if not rows:
            return set()
        symbols = sorted({event.symbol for event in rows})
        funding_times = sorted({ensure_utc(event.funding_time) for event in rows})
        try:
            async with self.database.acquire() as connection:
                existing = await connection.fetch(
                    """
                    SELECT exchange, futures_symbol, funding_time
                    FROM funding_interval_summaries
                    WHERE exchange = $1
                      AND futures_symbol = ANY($2::text[])
                      AND funding_time = ANY($3::timestamptz[])
                    """,
                    DEFAULT_EXCHANGE,
                    symbols,
                    funding_times,
                )
        except asyncpg.UndefinedTableError:
            return set()
        return {
            (
                row["exchange"],
                row["futures_symbol"],
                ensure_utc(row["funding_time"]),
            )
            for row in existing
        }

    async def snapshots_for_interval(
        self,
        symbol: str,
        funding_time: datetime,
    ) -> list[FundingSnapshot]:
        async with self.database.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT *
                FROM funding_snapshots
                WHERE symbol = $1
                  AND next_funding_time = $2
                  AND event_time <= $2
                ORDER BY event_time
                """,
                symbol,
                ensure_utc(funding_time),
            )
        return [self._funding_repository._row_to_snapshot(row) for row in rows]

    async def upsert_interval_summaries(
        self,
        summaries: Iterable[FundingIntervalSummary],
    ) -> int:
        rows = list(summaries)
        if not rows:
            return 0
        async with self.database.acquire() as connection:
            await connection.executemany(
                UPSERT_INTERVAL_SUMMARY_SQL,
                [self._interval_summary_args(summary) for summary in rows],
            )
        return len(rows)

    def _evaluation_args(self, evaluation: CandidateEvaluation) -> tuple[Any, ...]:
        score = evaluation.score_components
        return (
            evaluation.exchange,
            evaluation.futures_symbol,
            evaluation.spot_symbol,
            ensure_utc(evaluation.evaluated_at),
            ensure_utc(evaluation.evaluated_at_bucket),
            ensure_utc(evaluation.next_funding_time)
            if evaluation.next_funding_time is not None
            else None,
            evaluation.predicted_funding_rate,
            evaluation.minimum_funding_rate,
            evaluation.minutes_to_funding,
            evaluation.status.value,
            score.total_score,
            score.funding_score,
            score.persistence_score,
            score.stability_score,
            score.trend_score,
            score.lifetime_score,
            score.timing_score,
            score.total_penalty,
            evaluation.persistence_ratio,
            evaluation.standard_deviation,
            evaluation.velocity,
            evaluation.acceleration,
            evaluation.threshold_crossings,
            evaluation.direction_changes,
            ensure_utc(evaluation.signal_started_at)
            if evaluation.signal_started_at is not None
            else None,
            evaluation.signal_age_seconds,
            evaluation.snapshot_count,
            evaluation.history_duration_seconds,
            ensure_utc(evaluation.latest_snapshot_at)
            if evaluation.latest_snapshot_at is not None
            else None,
            _json_dumps([reason.value for reason in evaluation.rejection_reasons]),
            _json_dumps([flag.value for flag in evaluation.warning_flags]),
            _json_dumps(evaluation.score_details),
            _json_dumps(evaluation.metrics_details),
            evaluation.engine_version,
        )

    def _interval_summary_args(
        self,
        summary: FundingIntervalSummary,
    ) -> tuple[Any, ...]:
        return (
            summary.exchange,
            summary.futures_symbol,
            ensure_utc(summary.funding_time),
            ensure_utc(summary.interval_started_at)
            if summary.interval_started_at is not None
            else None,
            ensure_utc(summary.interval_ended_at),
            summary.realized_funding_rate,
            summary.first_predicted_rate,
            summary.last_predicted_rate,
            summary.minimum_predicted_rate,
            summary.maximum_predicted_rate,
            ensure_utc(summary.peak_predicted_at)
            if summary.peak_predicted_at is not None
            else None,
            summary.mean_predicted_rate,
            summary.median_predicted_rate,
            summary.predicted_rate_120m_before,
            summary.predicted_rate_60m_before,
            summary.predicted_rate_30m_before,
            summary.predicted_rate_15m_before,
            summary.predicted_rate_5m_before,
            summary.positive_snapshot_ratio,
            summary.above_threshold_snapshot_ratio,
            summary.above_threshold_duration_seconds,
            summary.maximum_above_threshold_streak_seconds,
            ensure_utc(summary.signal_started_at)
            if summary.signal_started_at is not None
            else None,
            summary.longest_positive_streak_seconds,
            summary.threshold_crossings,
            summary.direction_changes,
            summary.prediction_error,
            summary.absolute_prediction_error,
            summary.snapshot_count,
            summary.history_coverage_ratio,
            summary.summary_status.value,
        )

    def _row_to_evaluation(self, row: Mapping[str, Any]) -> CandidateEvaluation:
        score = ScoreComponents(
            funding_score=decimal_from_text(row["funding_score"]),
            persistence_score=decimal_from_text(row["persistence_score"]),
            stability_score=decimal_from_text(row["stability_score"]),
            trend_score=decimal_from_text(row["trend_score"]),
            lifetime_score=decimal_from_text(row["lifetime_score"]),
            timing_score=decimal_from_text(row["timing_score"]),
            penalties=_decimal_penalties(_json_loads(row["score_details"])),
            total_penalty=decimal_from_text(row["total_penalty"]),
            total_score=decimal_from_text(row["total_score"]),
        )
        return CandidateEvaluation(
            exchange=row.get("exchange", DEFAULT_EXCHANGE),
            futures_symbol=row["futures_symbol"],
            spot_symbol=row["spot_symbol"],
            evaluated_at=ensure_utc(row["evaluated_at"]),
            evaluated_at_bucket=ensure_utc(row["evaluated_at_bucket"]),
            next_funding_time=ensure_utc(row["next_funding_time"])
            if row["next_funding_time"] is not None
            else None,
            predicted_funding_rate=decimal_from_text(row["predicted_funding_rate"]),
            minimum_funding_rate=decimal_from_text(row["minimum_funding_rate"]),
            minutes_to_funding=decimal_from_text(row["minutes_to_funding"])
            if row["minutes_to_funding"] is not None
            else None,
            status=CandidateStatus(row["status"]),
            score_components=score,
            persistence_ratio=decimal_from_text(row["persistence_ratio"])
            if row["persistence_ratio"] is not None
            else None,
            standard_deviation=decimal_from_text(row["standard_deviation"])
            if row["standard_deviation"] is not None
            else None,
            velocity=decimal_from_text(row["velocity"])
            if row["velocity"] is not None
            else None,
            acceleration=decimal_from_text(row["acceleration"])
            if row["acceleration"] is not None
            else None,
            threshold_crossings=row["threshold_crossings"],
            direction_changes=row["direction_changes"],
            signal_started_at=ensure_utc(row["signal_started_at"])
            if row["signal_started_at"] is not None
            else None,
            signal_age_seconds=row["signal_age_seconds"],
            snapshot_count=int(row["snapshot_count"]),
            history_duration_seconds=row["history_duration_seconds"],
            latest_snapshot_at=ensure_utc(row["latest_snapshot_at"])
            if row["latest_snapshot_at"] is not None
            else None,
            rejection_reasons=tuple(
                RejectionReason(reason)
                for reason in _json_loads(row["rejection_reasons"])
            ),
            warning_flags=tuple(
                RejectionReason(flag) for flag in _json_loads(row["warning_flags"])
            ),
            score_details=_json_loads(row["score_details"]),
            metrics_details=_json_loads(row["metrics_details"]),
            engine_version=row["engine_version"],
        )


def _json_dumps(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _json_loads(value: object) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _decimal_penalties(score_details: object) -> dict[str, Decimal]:
    if not isinstance(score_details, dict):
        return {}
    penalties = score_details.get("penalties", {})
    if not isinstance(penalties, dict):
        return {}
    return {str(key): decimal_from_text(value) for key, value in penalties.items()}
