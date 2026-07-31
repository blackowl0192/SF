from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg

from funding_monitor.candidate_engine import DEFAULT_EXCHANGE
from funding_monitor.database import PostgresDatabase
from funding_monitor.instrument_repository import (
    SELECT_INSTRUMENT_MAPPING_COLUMNS,
    InstrumentMappingRepository,
)
from funding_monitor.models import decimal_from_text, ensure_utc
from funding_monitor.repository import FundingRepository

from .models import (
    DataQualityStatus,
    MissingMarketDataReason,
    OutcomeStatus,
    RejectionReason,
    StrategyValidationAggregate,
    StrategyValidationConfig,
    StrategyValidationDataset,
    StrategyValidationEvent,
    StrategyValidationResult,
    StrategyValidationRun,
    ValidationMode,
    ValidationRunStatus,
)

INSERT_RUN_SQL = """
INSERT INTO strategy_validation_runs (
    strategy_name,
    strategy_version,
    exchange,
    status,
    validation_mode,
    configuration,
    configuration_hash,
    dataset,
    period_start,
    period_end,
    requested_symbols
)
VALUES ($1, $2, $3, 'running', $4, $5::jsonb, $6, $7::jsonb, $8, $9, $10::jsonb)
RETURNING id
"""

COMPLETE_RUN_SQL = """
UPDATE strategy_validation_runs
SET
    status = 'completed',
    completed_at = NOW(),
    total_events = $2,
    processed_events = $3,
    successful_events = $4,
    failed_events = $5,
    error_message = NULL
WHERE id = $1
"""

FAIL_RUN_SQL = """
UPDATE strategy_validation_runs
SET
    status = 'failed',
    completed_at = NOW(),
    error_message = $2
WHERE id = $1
"""

INSERT_RESULT_SQL = """
INSERT INTO strategy_validation_results (
    run_id,
    exchange,
    symbol,
    spot_symbol,
    funding_time,
    strategy_version,
    config_hash,
    signal_detected,
    signal_started_at,
    signal_confirmed_at,
    entry_time,
    entry_minutes_before_funding,
    predicted_funding_at_entry,
    peak_predicted_funding,
    peak_predicted_at,
    last_predicted_funding,
    realized_funding_rate,
    prediction_error,
    prediction_drop_from_entry,
    prediction_drop_from_peak,
    persistence_at_entry,
    funding_std_at_entry,
    funding_velocity_at_entry,
    threshold_crossings_before_entry,
    late_spike,
    deteriorating_signal,
    spot_pair_exists,
    positive_strategy_available,
    enough_history,
    fresh_data,
    eligible,
    rejection_reason,
    validation_mode,
    market_data_complete,
    missing_data_reasons,
    position_notional,
    gross_funding_pnl,
    spot_price_pnl,
    futures_price_pnl,
    basis_pnl,
    spot_fees,
    futures_fees,
    slippage_cost,
    additional_cost,
    net_pnl,
    gross_return_rate,
    net_return_rate,
    outcome_status,
    success,
    profitable,
    data_quality_status,
    metadata
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
    $31, $32, $33, $34, $35::jsonb, $36, $37, $38, $39, $40,
    $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
    $51, $52::jsonb
)
ON CONFLICT(run_id, exchange, symbol, funding_time, config_hash) DO UPDATE SET
    spot_symbol = excluded.spot_symbol,
    signal_detected = excluded.signal_detected,
    signal_started_at = excluded.signal_started_at,
    signal_confirmed_at = excluded.signal_confirmed_at,
    entry_time = excluded.entry_time,
    entry_minutes_before_funding = excluded.entry_minutes_before_funding,
    predicted_funding_at_entry = excluded.predicted_funding_at_entry,
    peak_predicted_funding = excluded.peak_predicted_funding,
    peak_predicted_at = excluded.peak_predicted_at,
    last_predicted_funding = excluded.last_predicted_funding,
    realized_funding_rate = excluded.realized_funding_rate,
    prediction_error = excluded.prediction_error,
    prediction_drop_from_entry = excluded.prediction_drop_from_entry,
    prediction_drop_from_peak = excluded.prediction_drop_from_peak,
    persistence_at_entry = excluded.persistence_at_entry,
    funding_std_at_entry = excluded.funding_std_at_entry,
    funding_velocity_at_entry = excluded.funding_velocity_at_entry,
    threshold_crossings_before_entry = excluded.threshold_crossings_before_entry,
    late_spike = excluded.late_spike,
    deteriorating_signal = excluded.deteriorating_signal,
    spot_pair_exists = excluded.spot_pair_exists,
    positive_strategy_available = excluded.positive_strategy_available,
    enough_history = excluded.enough_history,
    fresh_data = excluded.fresh_data,
    eligible = excluded.eligible,
    rejection_reason = excluded.rejection_reason,
    validation_mode = excluded.validation_mode,
    market_data_complete = excluded.market_data_complete,
    missing_data_reasons = excluded.missing_data_reasons,
    position_notional = excluded.position_notional,
    gross_funding_pnl = excluded.gross_funding_pnl,
    spot_price_pnl = excluded.spot_price_pnl,
    futures_price_pnl = excluded.futures_price_pnl,
    basis_pnl = excluded.basis_pnl,
    spot_fees = excluded.spot_fees,
    futures_fees = excluded.futures_fees,
    slippage_cost = excluded.slippage_cost,
    additional_cost = excluded.additional_cost,
    net_pnl = excluded.net_pnl,
    gross_return_rate = excluded.gross_return_rate,
    net_return_rate = excluded.net_return_rate,
    outcome_status = excluded.outcome_status,
    success = excluded.success,
    profitable = excluded.profitable,
    data_quality_status = excluded.data_quality_status,
    metadata = excluded.metadata
"""

UPSERT_AGGREGATE_SQL = """
INSERT INTO strategy_validation_aggregates (
    run_id,
    grouping_type,
    grouping_key,
    metrics
)
VALUES ($1, $2, $3, $4::jsonb)
ON CONFLICT(run_id, grouping_type, grouping_key) DO UPDATE SET
    metrics = excluded.metrics
"""

SELECT_RESULT_COLUMNS = """
SELECT
    run_id,
    exchange,
    symbol,
    spot_symbol,
    funding_time,
    strategy_version,
    config_hash,
    signal_detected,
    signal_started_at,
    signal_confirmed_at,
    entry_time,
    entry_minutes_before_funding,
    predicted_funding_at_entry,
    peak_predicted_funding,
    peak_predicted_at,
    last_predicted_funding,
    realized_funding_rate,
    prediction_error,
    prediction_drop_from_entry,
    prediction_drop_from_peak,
    persistence_at_entry,
    funding_std_at_entry,
    funding_velocity_at_entry,
    threshold_crossings_before_entry,
    late_spike,
    deteriorating_signal,
    spot_pair_exists,
    positive_strategy_available,
    enough_history,
    fresh_data,
    eligible,
    rejection_reason,
    validation_mode,
    market_data_complete,
    missing_data_reasons,
    position_notional,
    gross_funding_pnl,
    spot_price_pnl,
    futures_price_pnl,
    basis_pnl,
    spot_fees,
    futures_fees,
    slippage_cost,
    additional_cost,
    net_pnl,
    gross_return_rate,
    net_return_rate,
    outcome_status,
    success,
    profitable,
    data_quality_status,
    metadata
FROM strategy_validation_results
"""


class StrategyValidationRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database
        self._funding_repository = FundingRepository(database)
        self._mapping_repository = InstrumentMappingRepository(database)

    async def create_run(
        self,
        config: StrategyValidationConfig,
        dataset: StrategyValidationDataset,
    ) -> int:
        async with self.database.acquire() as connection:
            run_id = await connection.fetchval(
                INSERT_RUN_SQL,
                config.strategy_name,
                config.strategy_version,
                config.exchange,
                config.validation_mode.value,
                _json_dumps(config.to_dict()),
                config.config_hash(),
                _json_dumps(dataset.to_dict()),
                ensure_utc(dataset.period_start)
                if dataset.period_start is not None
                else None,
                ensure_utc(dataset.period_end) if dataset.period_end is not None else None,
                _json_dumps(list(dataset.requested_symbols)),
            )
        return int(run_id)

    async def complete_run(
        self,
        *,
        run_id: int,
        total_events: int,
        processed_events: int,
        successful_events: int,
        failed_events: int,
    ) -> None:
        async with self.database.acquire() as connection:
            await connection.execute(
                COMPLETE_RUN_SQL,
                run_id,
                total_events,
                processed_events,
                successful_events,
                failed_events,
            )

    async def fail_run(self, *, run_id: int, error_message: str) -> None:
        async with self.database.acquire() as connection:
            await connection.execute(FAIL_RUN_SQL, run_id, error_message)

    async def fetch_events(
        self,
        *,
        exchange: str = DEFAULT_EXCHANGE,
        period_start: datetime | None = None,
        period_end: datetime | None = None,
        symbols: tuple[str, ...] = (),
        limit: int | None = None,
    ) -> list[StrategyValidationEvent]:
        event_rows = await self._fetch_event_rows(
            exchange=exchange,
            period_start=period_start,
            period_end=period_end,
            symbols=symbols,
            limit=limit,
        )
        if not event_rows:
            return []

        symbol_values = tuple(sorted({row["symbol"] for row in event_rows}))
        mappings = await self._fetch_mappings(symbol_values)
        events: list[StrategyValidationEvent] = []
        for row in event_rows:
            funding_event = self._funding_repository._row_to_event(row)
            events.append(
                StrategyValidationEvent(
                    exchange=exchange,
                    symbol=funding_event.symbol,
                    funding_event=funding_event,
                    snapshots=tuple(
                        await self._funding_repository.snapshots_for_event(
                            funding_event.symbol,
                            funding_event.funding_time,
                        )
                    ),
                    mapping=mappings.get(funding_event.symbol),
                    candidate_status=row["candidate_status"],
                    candidate_score=decimal_from_text(row["candidate_score"])
                    if row["candidate_score"] is not None
                    else None,
                    interval_summary_status=row["interval_summary_status"],
                )
            )
        return events

    async def save_results(
        self,
        results: Iterable[StrategyValidationResult],
    ) -> int:
        rows = list(results)
        if not rows:
            return 0
        async with self.database.acquire() as connection:
            await connection.executemany(
                INSERT_RESULT_SQL,
                [self._result_args(result) for result in rows],
            )
        return len(rows)

    async def save_aggregates(
        self,
        aggregates: Iterable[StrategyValidationAggregate],
    ) -> int:
        rows = list(aggregates)
        if not rows:
            return 0
        async with self.database.acquire() as connection:
            await connection.executemany(
                UPSERT_AGGREGATE_SQL,
                [
                    (
                        aggregate.run_id,
                        aggregate.grouping_type,
                        aggregate.grouping_key,
                        _json_dumps(aggregate.metrics),
                    )
                    for aggregate in rows
                ],
            )
        return len(rows)

    async def get_run(self, run_id: int) -> StrategyValidationRun | None:
        async with self.database.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT *
                FROM strategy_validation_runs
                WHERE id = $1
                """,
                run_id,
            )
        return self._row_to_run(row) if row is not None else None

    async def get_results(self, run_id: int) -> list[StrategyValidationResult]:
        async with self.database.acquire() as connection:
            rows = await connection.fetch(
                f"{SELECT_RESULT_COLUMNS}\nWHERE run_id = $1\n"
                "ORDER BY funding_time DESC, symbol",
                run_id,
            )
        return [self._row_to_result(row) for row in rows]

    async def get_aggregates(
        self,
        run_id: int,
    ) -> tuple[StrategyValidationAggregate, ...]:
        async with self.database.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT run_id, grouping_type, grouping_key, metrics
                FROM strategy_validation_aggregates
                WHERE run_id = $1
                ORDER BY grouping_type, grouping_key
                """,
                run_id,
            )
        return tuple(self._row_to_aggregate(row) for row in rows)

    async def export_results_csv(self, run_id: int, output_path: Path) -> int:
        results = await self.get_results(run_id)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=[
                    "run_id",
                    "exchange",
                    "symbol",
                    "spot_symbol",
                    "funding_time",
                    "outcome_status",
                    "signal_detected",
                    "entry_time",
                    "predicted_funding_at_entry",
                    "realized_funding_rate",
                    "gross_funding_pnl",
                    "gross_return_rate",
                    "net_pnl",
                    "net_return_rate",
                    "rejection_reason",
                    "data_quality_status",
                ],
            )
            writer.writeheader()
            for result in results:
                writer.writerow(
                    {
                        "run_id": result.run_id,
                        "exchange": result.exchange,
                        "symbol": result.symbol,
                        "spot_symbol": result.spot_symbol,
                        "funding_time": ensure_utc(result.funding_time).isoformat(),
                        "outcome_status": result.outcome_status.value,
                        "signal_detected": result.signal_detected,
                        "entry_time": ensure_utc(result.entry_time).isoformat()
                        if result.entry_time is not None
                        else "",
                        "predicted_funding_at_entry": _optional_decimal_text(
                            result.predicted_funding_at_entry
                        ),
                        "realized_funding_rate": _optional_decimal_text(
                            result.realized_funding_rate
                        ),
                        "gross_funding_pnl": _optional_decimal_text(
                            result.gross_funding_pnl
                        ),
                        "gross_return_rate": _optional_decimal_text(
                            result.gross_return_rate
                        ),
                        "net_pnl": _optional_decimal_text(result.net_pnl),
                        "net_return_rate": _optional_decimal_text(
                            result.net_return_rate
                        ),
                        "rejection_reason": result.rejection_reason.value
                        if result.rejection_reason is not None
                        else "",
                        "data_quality_status": result.data_quality_status.value,
                    }
                )
        return len(results)

    async def _fetch_event_rows(
        self,
        *,
        exchange: str,
        period_start: datetime | None,
        period_end: datetime | None,
        symbols: tuple[str, ...],
        limit: int | None,
    ) -> list[Mapping[str, Any]]:
        query = """
        SELECT
            fe.*,
            ce.status AS candidate_status,
            ce.total_score AS candidate_score,
            fis.summary_status AS interval_summary_status
        FROM funding_events fe
        LEFT JOIN LATERAL (
            SELECT status, total_score
            FROM candidate_evaluations
            WHERE exchange = $1
              AND futures_symbol = fe.symbol
              AND (
                  next_funding_time = fe.funding_time
                  OR next_funding_time IS NULL
              )
            ORDER BY evaluated_at DESC
            LIMIT 1
        ) ce ON TRUE
        LEFT JOIN funding_interval_summaries fis
          ON fis.exchange = $1
         AND fis.futures_symbol = fe.symbol
         AND fis.funding_time = fe.funding_time
        WHERE fe.status = 'confirmed'
          AND fe.actual_funding_rate IS NOT NULL
        """
        args: list[Any] = [exchange]
        if period_start is not None:
            args.append(ensure_utc(period_start))
            query += f"\nAND fe.funding_time >= ${len(args)}"
        if period_end is not None:
            args.append(ensure_utc(period_end))
            query += f"\nAND fe.funding_time < ${len(args)}"
        if symbols:
            args.append(list(symbols))
            query += f"\nAND fe.symbol = ANY(${len(args)}::text[])"
        query += "\nORDER BY fe.funding_time, fe.symbol"
        if limit is not None:
            args.append(limit)
            query += f"\nLIMIT ${len(args)}"
        try:
            async with self.database.acquire() as connection:
                rows = await connection.fetch(query, *args)
        except asyncpg.UndefinedTableError:
            return []
        return list(rows)

    async def _fetch_mappings(
        self,
        symbols: tuple[str, ...],
    ) -> dict[str, Any]:
        if not symbols:
            return {}
        query = f"""
        {SELECT_INSTRUMENT_MAPPING_COLUMNS}
        WHERE futures_symbol = ANY($1::text[])
        """
        try:
            async with self.database.acquire() as connection:
                rows = await connection.fetch(query, list(symbols))
        except asyncpg.UndefinedTableError:
            return {}
        return {
            row["futures_symbol"]: self._mapping_repository._row_to_mapping(row)
            for row in rows
        }

    def _result_args(self, result: StrategyValidationResult) -> tuple[Any, ...]:
        if result.run_id is None:
            raise ValueError("persisted validation result requires run_id")
        return (
            result.run_id,
            result.exchange,
            result.symbol,
            result.spot_symbol,
            ensure_utc(result.funding_time),
            result.strategy_version,
            result.config_hash,
            result.signal_detected,
            ensure_utc(result.signal_started_at)
            if result.signal_started_at is not None
            else None,
            ensure_utc(result.signal_confirmed_at)
            if result.signal_confirmed_at is not None
            else None,
            ensure_utc(result.entry_time) if result.entry_time is not None else None,
            result.entry_minutes_before_funding,
            result.predicted_funding_at_entry,
            result.peak_predicted_funding,
            ensure_utc(result.peak_predicted_at)
            if result.peak_predicted_at is not None
            else None,
            result.last_predicted_funding,
            result.realized_funding_rate,
            result.prediction_error,
            result.prediction_drop_from_entry,
            result.prediction_drop_from_peak,
            result.persistence_at_entry,
            result.funding_std_at_entry,
            result.funding_velocity_at_entry,
            result.threshold_crossings_before_entry,
            result.late_spike,
            result.deteriorating_signal,
            result.spot_pair_exists,
            result.positive_strategy_available,
            result.enough_history,
            result.fresh_data,
            result.eligible,
            result.rejection_reason.value if result.rejection_reason is not None else None,
            result.validation_mode.value,
            result.market_data_complete,
            _json_dumps([reason.value for reason in result.missing_data_reasons]),
            result.position_notional,
            result.gross_funding_pnl,
            result.spot_price_pnl,
            result.futures_price_pnl,
            result.basis_pnl,
            result.spot_fees,
            result.futures_fees,
            result.slippage_cost,
            result.additional_cost,
            result.net_pnl,
            result.gross_return_rate,
            result.net_return_rate,
            result.outcome_status.value,
            result.success,
            result.profitable,
            result.data_quality_status.value,
            _json_dumps(result.metadata),
        )

    def _row_to_run(self, row: Mapping[str, Any]) -> StrategyValidationRun:
        return StrategyValidationRun(
            id=int(row["id"]),
            strategy_name=row["strategy_name"],
            strategy_version=row["strategy_version"],
            exchange=row["exchange"],
            status=ValidationRunStatus(row["status"]),
            validation_mode=ValidationMode(row["validation_mode"]),
            configuration=_json_loads(row["configuration"]),
            configuration_hash=row["configuration_hash"],
            period_start=ensure_utc(row["period_start"])
            if row["period_start"] is not None
            else None,
            period_end=ensure_utc(row["period_end"])
            if row["period_end"] is not None
            else None,
            requested_symbols=tuple(_json_loads(row["requested_symbols"])),
            started_at=ensure_utc(row["started_at"]),
            completed_at=ensure_utc(row["completed_at"])
            if row["completed_at"] is not None
            else None,
            total_events=int(row["total_events"]),
            processed_events=int(row["processed_events"]),
            successful_events=int(row["successful_events"]),
            failed_events=int(row["failed_events"]),
            error_message=row["error_message"],
        )

    def _row_to_result(self, row: Mapping[str, Any]) -> StrategyValidationResult:
        return StrategyValidationResult(
            run_id=int(row["run_id"]),
            exchange=row["exchange"],
            symbol=row["symbol"],
            spot_symbol=row["spot_symbol"],
            funding_time=ensure_utc(row["funding_time"]),
            strategy_version=row["strategy_version"],
            config_hash=row["config_hash"],
            signal_detected=bool(row["signal_detected"]),
            signal_started_at=ensure_utc(row["signal_started_at"])
            if row["signal_started_at"] is not None
            else None,
            signal_confirmed_at=ensure_utc(row["signal_confirmed_at"])
            if row["signal_confirmed_at"] is not None
            else None,
            entry_time=ensure_utc(row["entry_time"]) if row["entry_time"] is not None else None,
            entry_minutes_before_funding=_optional_decimal(row, "entry_minutes_before_funding"),
            predicted_funding_at_entry=_optional_decimal(
                row,
                "predicted_funding_at_entry",
            ),
            peak_predicted_funding=_optional_decimal(row, "peak_predicted_funding"),
            peak_predicted_at=ensure_utc(row["peak_predicted_at"])
            if row["peak_predicted_at"] is not None
            else None,
            last_predicted_funding=_optional_decimal(row, "last_predicted_funding"),
            realized_funding_rate=_optional_decimal(row, "realized_funding_rate"),
            prediction_error=_optional_decimal(row, "prediction_error"),
            prediction_drop_from_entry=_optional_decimal(
                row,
                "prediction_drop_from_entry",
            ),
            prediction_drop_from_peak=_optional_decimal(
                row,
                "prediction_drop_from_peak",
            ),
            persistence_at_entry=_optional_decimal(row, "persistence_at_entry"),
            funding_std_at_entry=_optional_decimal(row, "funding_std_at_entry"),
            funding_velocity_at_entry=_optional_decimal(
                row,
                "funding_velocity_at_entry",
            ),
            threshold_crossings_before_entry=row[
                "threshold_crossings_before_entry"
            ],
            late_spike=bool(row["late_spike"]),
            deteriorating_signal=bool(row["deteriorating_signal"]),
            spot_pair_exists=bool(row["spot_pair_exists"]),
            positive_strategy_available=bool(row["positive_strategy_available"]),
            enough_history=bool(row["enough_history"]),
            fresh_data=bool(row["fresh_data"]),
            eligible=bool(row["eligible"]),
            rejection_reason=RejectionReason(row["rejection_reason"])
            if row["rejection_reason"] is not None
            else None,
            validation_mode=ValidationMode(row["validation_mode"]),
            market_data_complete=bool(row["market_data_complete"]),
            missing_data_reasons=tuple(
                MissingMarketDataReason(reason)
                for reason in _json_loads(row["missing_data_reasons"])
            ),
            position_notional=decimal_from_text(row["position_notional"]),
            gross_funding_pnl=_optional_decimal(row, "gross_funding_pnl"),
            spot_price_pnl=_optional_decimal(row, "spot_price_pnl"),
            futures_price_pnl=_optional_decimal(row, "futures_price_pnl"),
            basis_pnl=_optional_decimal(row, "basis_pnl"),
            spot_fees=_optional_decimal(row, "spot_fees"),
            futures_fees=_optional_decimal(row, "futures_fees"),
            slippage_cost=_optional_decimal(row, "slippage_cost"),
            additional_cost=_optional_decimal(row, "additional_cost"),
            net_pnl=_optional_decimal(row, "net_pnl"),
            gross_return_rate=_optional_decimal(row, "gross_return_rate"),
            net_return_rate=_optional_decimal(row, "net_return_rate"),
            outcome_status=OutcomeStatus(row["outcome_status"]),
            success=bool(row["success"]),
            profitable=bool(row["profitable"])
            if row["profitable"] is not None
            else None,
            data_quality_status=DataQualityStatus(row["data_quality_status"]),
            metadata=_json_loads(row["metadata"]),
        )

    def _row_to_aggregate(self, row: Mapping[str, Any]) -> StrategyValidationAggregate:
        return StrategyValidationAggregate(
            run_id=int(row["run_id"]),
            grouping_type=row["grouping_type"],
            grouping_key=row["grouping_key"],
            metrics=_json_loads(row["metrics"]),
        )


def _optional_decimal(row: Mapping[str, Any], key: str) -> Decimal | None:
    return decimal_from_text(row[key]) if row[key] is not None else None


def _optional_decimal_text(value: Decimal | None) -> str:
    return format(value, "f") if value is not None else ""


def _json_dumps(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _json_loads(value: object) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value
