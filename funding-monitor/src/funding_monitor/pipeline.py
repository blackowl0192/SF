from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime

import httpx

from .binance_rest import BinanceRestClient
from .candidate_engine import (
    CandidateEngine,
    CandidateEngineConfig,
    CandidateEvaluation,
    FundingIntervalBuilder,
    FundingIntervalSummary,
    FundingIntervalSummaryStatus,
)
from .candidate_repository import CandidateRepository
from .config import Settings
from .funding_event_service import ConfirmationRequest, FundingConfirmationService
from .history_service import FundingHistoryService
from .instrument_mapping import InstrumentMapping
from .instrument_repository import InstrumentMappingRepository
from .models import ensure_utc, utc_now
from .repository import FundingRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CandidateEvaluationPipelineResult:
    evaluated: int
    persisted: int
    failed: int
    dry_run: bool
    statuses: dict[str, int]
    evaluated_at: datetime


@dataclass(frozen=True)
class FundingIntervalBackfillResult:
    processed: int
    created: int
    updated: int
    persisted: int
    complete: int
    partial: int
    insufficient_history: int
    failed: int
    dry_run: bool
    reasons: dict[str, int]


@dataclass(frozen=True)
class ConfirmationBackfillResult:
    checked: int
    confirmed: int
    not_found: int
    failed: int
    retry_failed: bool


class CandidateEvaluationPipeline:
    def __init__(
        self,
        *,
        funding_repository: FundingRepository,
        mapping_repository: InstrumentMappingRepository,
        candidate_repository: CandidateRepository,
        settings: Settings,
        config: CandidateEngineConfig,
    ) -> None:
        self.funding_repository = funding_repository
        self.mapping_repository = mapping_repository
        self.candidate_repository = candidate_repository
        self.settings = settings
        self.config = config

    async def run(
        self,
        *,
        symbols: tuple[str, ...] = (),
        limit: int | None = None,
        evaluated_at: datetime | None = None,
        dry_run: bool = False,
    ) -> CandidateEvaluationPipelineResult:
        timestamp = ensure_utc(evaluated_at or utc_now())
        history = FundingHistoryService(
            repository=self.funding_repository,
            window_cache_minutes=max(
                self.settings.window_cache_minutes,
                self.config.long_window_minutes,
                self.config.primary_window_minutes,
                self.config.short_window_minutes,
            ),
            default_metrics_window=self.config.primary_window_minutes,
            abs_threshold=self.config.min_funding_rate,
        )
        await history.reload()

        mappings = await self.mapping_repository.list_mappings()
        selected_mappings = _filter_mappings(mappings, symbols)
        engine = CandidateEngine(config=self.config)
        inputs = engine.inputs_from_history(
            selected_mappings,
            history,
            evaluated_at=timestamp,
        )
        if limit is not None:
            inputs = inputs[:limit]

        evaluations = engine.evaluate_many(inputs)
        failed = 0
        persisted = 0
        if not dry_run:
            persisted, failed = await _persist_evaluations_with_isolation(
                self.candidate_repository,
                evaluations,
            )

        statuses = Counter(evaluation.status.value for evaluation in evaluations)
        return CandidateEvaluationPipelineResult(
            evaluated=len(evaluations),
            persisted=persisted,
            failed=failed,
            dry_run=dry_run,
            statuses=dict(sorted(statuses.items())),
            evaluated_at=timestamp,
        )


class FundingIntervalBackfillService:
    def __init__(
        self,
        *,
        repository: CandidateRepository,
        config: CandidateEngineConfig,
    ) -> None:
        self.repository = repository
        self.config = config
        self.builder = FundingIntervalBuilder(
            min_funding_rate=config.min_funding_rate,
            point_tolerance_seconds=config.interval_point_tolerance_seconds,
        )

    async def run(
        self,
        *,
        period_start: datetime | None = None,
        period_end: datetime | None = None,
        symbols: tuple[str, ...] = (),
        limit: int | None = None,
        dry_run: bool = False,
        retry_failed: bool = False,
    ) -> FundingIntervalBackfillResult:
        batch_limit = limit or self.config.interval_summary_batch_size
        events = await self.repository.confirmed_events_for_interval_summaries(
            batch_limit,
            period_start=period_start,
            period_end=period_end,
            symbols=symbols,
        )
        existing_keys = await self.repository.existing_interval_summary_keys(events)
        snapshots_by_event = await self.repository.snapshots_for_intervals(events)
        summaries: list[FundingIntervalSummary] = []
        failed = 0
        reasons: Counter[str] = Counter()

        for event in events:
            try:
                snapshots = snapshots_by_event.get(
                    (event.symbol, ensure_utc(event.funding_time)),
                    [],
                )
                summary = self.builder.build(event, snapshots)
                summaries.append(summary)
                reasons[summary.summary_status.value] += 1
            except Exception:
                logger.exception(
                    "funding interval summary failed for %s %s",
                    event.symbol,
                    event.funding_time.isoformat(),
                )
                failed += 1
                reasons["builder_error"] += 1

        persisted = 0
        failed_persist = 0
        if not dry_run:
            persisted, failed_persist = await _persist_summaries_with_isolation(
                self.repository,
                summaries,
            )
        failed += failed_persist

        created = sum(
            1
            for summary in summaries
            if (
                summary.exchange,
                summary.futures_symbol,
                ensure_utc(summary.funding_time),
            )
            not in existing_keys
        )
        updated = len(summaries) - created
        complete = sum(
            1
            for summary in summaries
            if summary.summary_status == FundingIntervalSummaryStatus.COMPLETE
        )
        insufficient = sum(
            1
            for summary in summaries
            if (
                summary.summary_status
                == FundingIntervalSummaryStatus.INSUFFICIENT_HISTORY
            )
        )
        partial = len(summaries) - complete - insufficient
        if retry_failed:
            reasons["retry_failed_requested"] += 0
        return FundingIntervalBackfillResult(
            processed=len(events),
            created=created,
            updated=updated,
            persisted=persisted,
            complete=complete,
            partial=partial,
            insufficient_history=insufficient,
            failed=failed,
            dry_run=dry_run,
            reasons=dict(sorted(reasons.items())),
        )


class ConfirmationBackfillService:
    def __init__(
        self,
        *,
        repository: FundingRepository,
        rest_client: BinanceRestClient,
    ) -> None:
        self.repository = repository
        self.service = FundingConfirmationService(
            repository=repository,
            rest_client=rest_client,
        )

    async def run(
        self,
        *,
        due_before: datetime,
        limit: int,
        retry_failed: bool = False,
    ) -> ConfirmationBackfillResult:
        events = await self.repository.funding_events_for_confirmation(
            due_before=due_before,
            limit=limit,
            retry_failed=retry_failed,
        )
        confirmed = 0
        not_found = 0
        failed = 0
        for event in events:
            request = ConfirmationRequest(event.symbol, event.funding_time)
            try:
                if await self.service.try_confirm(request):
                    confirmed += 1
                else:
                    not_found += 1
            except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
                logger.warning(
                    "confirmation_backfill_error symbol=%s funding_time=%s "
                    "error_type=%s",
                    event.symbol,
                    event.funding_time.isoformat(),
                    type(exc).__name__,
                )
                failed += 1
        return ConfirmationBackfillResult(
            checked=len(events),
            confirmed=confirmed,
            not_found=not_found,
            failed=failed,
            retry_failed=retry_failed,
        )


class PipelineOrchestrator:
    def __init__(
        self,
        *,
        candidate_pipeline: CandidateEvaluationPipeline,
        interval_backfill: FundingIntervalBackfillService,
        confirmation_backfill: ConfirmationBackfillService,
        settings: Settings,
    ) -> None:
        self.candidate_pipeline = candidate_pipeline
        self.interval_backfill = interval_backfill
        self.confirmation_backfill = confirmation_backfill
        self.settings = settings
        self._tasks: set[asyncio.Task[None]] = set()

    def start(self, stop_event: asyncio.Event) -> None:
        self._tasks.add(
            asyncio.create_task(
                self._run_periodic(
                    "candidate_evaluations",
                    self.settings.candidate_evaluation_interval_seconds,
                    self._run_candidate_evaluations,
                    stop_event,
                )
            )
        )
        self._tasks.add(
            asyncio.create_task(
                self._run_periodic(
                    "funding_interval_summaries",
                    self.settings.interval_summary_build_interval_seconds,
                    self._run_interval_summaries,
                    stop_event,
                )
            )
        )
        self._tasks.add(
            asyncio.create_task(
                self._run_periodic(
                    "confirmation_backfill",
                    self.settings.confirmation_backfill_interval_seconds,
                    self._run_confirmation_backfill,
                    stop_event,
                )
            )
        )

    async def close(self) -> None:
        if not self._tasks:
            return
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _run_periodic(
        self,
        stage: str,
        interval_seconds: int,
        callback: Callable[[], Awaitable[None]],
        stop_event: asyncio.Event,
    ) -> None:
        while not stop_event.is_set():
            try:
                await callback()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("collector_error stage=%s", stage)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                continue

    async def _run_candidate_evaluations(self) -> None:
        result = await self.candidate_pipeline.run()
        logger.info(
            "candidate_evaluation_batch evaluated=%s persisted=%s failed=%s",
            result.evaluated,
            result.persisted,
            result.failed,
        )

    async def _run_interval_summaries(self) -> None:
        result = await self.interval_backfill.run()
        logger.info(
            "interval_summary_batch processed=%s persisted=%s complete=%s "
            "partial=%s insufficient_history=%s failed=%s",
            result.processed,
            result.persisted,
            result.complete,
            result.partial,
            result.insufficient_history,
            result.failed,
        )

    async def _run_confirmation_backfill(self) -> None:
        due_before = utc_now()
        result = await self.confirmation_backfill.run(
            due_before=due_before,
            limit=self.settings.confirmation_backfill_batch_size,
        )
        logger.info(
            "confirmation_backfill_batch checked=%s confirmed=%s not_found=%s "
            "failed=%s",
            result.checked,
            result.confirmed,
            result.not_found,
            result.failed,
        )


async def _persist_evaluations_with_isolation(
    repository: CandidateRepository,
    evaluations: Iterable[CandidateEvaluation],
) -> tuple[int, int]:
    rows = list(evaluations)
    if not rows:
        return 0, 0
    try:
        return await repository.upsert_evaluations(rows), 0
    except Exception:
        logger.exception("candidate_evaluation_batch_persist_failed")

    persisted = 0
    failed = 0
    for evaluation in rows:
        try:
            persisted += await repository.upsert_evaluations([evaluation])
        except Exception:
            logger.exception(
                "candidate_evaluation_persist_failed symbol=%s",
                evaluation.futures_symbol,
            )
            failed += 1
    return persisted, failed


async def _persist_summaries_with_isolation(
    repository: CandidateRepository,
    summaries: Iterable[FundingIntervalSummary],
) -> tuple[int, int]:
    rows = list(summaries)
    if not rows:
        return 0, 0
    try:
        return await repository.upsert_interval_summaries(rows), 0
    except Exception:
        logger.exception("interval_summary_batch_persist_failed")

    persisted = 0
    failed = 0
    for summary in rows:
        try:
            persisted += await repository.upsert_interval_summaries([summary])
        except Exception:
            logger.exception(
                "interval_summary_persist_failed symbol=%s funding_time=%s",
                summary.futures_symbol,
                summary.funding_time.isoformat(),
            )
            failed += 1
    return persisted, failed


def _filter_mappings(
    mappings: Iterable[InstrumentMapping],
    symbols: tuple[str, ...],
) -> list[InstrumentMapping]:
    if not symbols:
        return list(mappings)
    requested = {symbol.upper() for symbol in symbols}
    return [mapping for mapping in mappings if mapping.futures_symbol in requested]


def candidate_config_from_settings(settings: Settings) -> CandidateEngineConfig:
    return CandidateEngineConfig(
        enabled=settings.candidate_engine_enabled,
        min_funding_rate=settings.candidate_min_funding_rate,
        min_history_minutes=settings.candidate_min_history_minutes,
        primary_window_minutes=settings.candidate_primary_window_minutes,
        short_window_minutes=settings.candidate_short_window_minutes,
        long_window_minutes=settings.candidate_long_window_minutes,
        min_snapshot_count=settings.candidate_min_snapshot_count,
        max_snapshot_age_seconds=settings.candidate_max_snapshot_age_seconds,
        min_persistence_ratio=settings.candidate_min_persistence_ratio,
        max_std_dev=settings.candidate_max_std_dev,
        max_threshold_crossings=settings.candidate_max_threshold_crossings,
        max_direction_changes=settings.candidate_max_direction_changes,
        late_spike_lookback_minutes=settings.candidate_late_spike_lookback_minutes,
        late_spike_min_jump_ratio=settings.candidate_late_spike_min_jump_ratio,
        deterioration_lookback_minutes=settings.candidate_deterioration_lookback_minutes,
        max_negative_velocity=settings.candidate_max_negative_velocity,
        min_minutes_to_funding=settings.candidate_min_minutes_to_funding,
        max_minutes_to_funding=settings.candidate_max_minutes_to_funding,
        strong_score=settings.candidate_strong_score,
        min_score=settings.candidate_min_score,
        persist_interval_seconds=settings.candidate_persist_interval_seconds,
        max_results=settings.candidate_max_results,
        interval_point_tolerance_seconds=(
            settings.funding_interval_point_tolerance_seconds
        ),
        interval_summary_batch_size=settings.funding_interval_summary_batch_size,
    )
