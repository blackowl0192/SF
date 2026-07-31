from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Mapping
from datetime import datetime

from .binance_rest import BinanceRestClient
from .binance_ws import BinanceWebSocketClient
from .candidate_repository import CandidateRepository
from .config import Settings
from .database import PostgresDatabase
from .funding_event_service import (
    ConfirmationRequest,
    FundingConfirmationScheduler,
    FundingEventService,
)
from .history_service import FundingHistoryService
from .instrument_repository import InstrumentMappingRepository
from .models import utc_now
from .pipeline import (
    CandidateEvaluationPipeline,
    ConfirmationBackfillService,
    FundingIntervalBackfillService,
    PipelineOrchestrator,
    candidate_config_from_settings,
)
from .repository import FundingRepository
from .snapshot_service import (
    SnapshotBatchBuffer,
    SnapshotThrottler,
    determine_capture_mode,
    snapshot_from_update,
)
from .symbol_service import SymbolService

logger = logging.getLogger(__name__)


class FundingCollector:
    def __init__(
        self,
        *,
        settings: Settings,
        repository: FundingRepository,
        rest_client: BinanceRestClient,
        ws_client: BinanceWebSocketClient,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.rest_client = rest_client
        self.ws_client = ws_client
        self.throttler = SnapshotThrottler(
            normal_interval_seconds=settings.snapshot_persist_interval_seconds,
            detailed_interval_seconds=settings.detailed_snapshot_interval_seconds,
        )
        self.snapshot_batch = SnapshotBatchBuffer(
            repository=repository,
            batch_size=settings.snapshot_batch_size,
            flush_interval_seconds=settings.snapshot_flush_interval_seconds,
        )
        self.event_service = FundingEventService(repository)
        self.confirmations = FundingConfirmationScheduler(
            repository=repository,
            rest_client=rest_client,
            initial_delay_seconds=settings.confirmation_initial_delay_seconds,
            retry_seconds=settings.confirmation_retry_seconds,
            max_attempts=settings.confirmation_max_attempts,
        )
        self.history_service = FundingHistoryService(
            repository=repository,
            window_cache_minutes=settings.window_cache_minutes,
            default_metrics_window=settings.default_metrics_window,
            abs_threshold=settings.abs_min_funding_rate,
        )
        self._last_next_funding_by_symbol: dict[str, datetime] = {}
        self._last_persist_at: datetime | None = None

    async def run(
        self,
        *,
        max_messages: int | None = None,
        max_seconds: float | None = None,
    ) -> int:
        symbol_service = SymbolService(
            self.repository,
            self.rest_client,
            default_funding_interval_hours=self.settings.default_funding_interval_hours,
        )
        await symbol_service.sync_symbols()
        active_symbols = await self.repository.active_symbols()
        await self.history_service.reload()
        logger.info(
            "collector_started active_symbol_count=%s "
            "snapshot_persist_interval_seconds=%s "
            "detailed_snapshot_interval_seconds=%s snapshot_batch_size=%s",
            len(active_symbols),
            self.settings.snapshot_persist_interval_seconds,
            self.settings.detailed_snapshot_interval_seconds,
            self.settings.snapshot_batch_size,
        )

        stop_event = asyncio.Event()
        _install_signal_handlers(stop_event)
        timer_task = _start_timer(stop_event, max_seconds)
        candidate_config = candidate_config_from_settings(self.settings)
        pipeline_orchestrator = PipelineOrchestrator(
            candidate_pipeline=CandidateEvaluationPipeline(
                funding_repository=self.repository,
                mapping_repository=InstrumentMappingRepository(self.repository.database),
                candidate_repository=CandidateRepository(self.repository.database),
                settings=self.settings,
                config=candidate_config,
            ),
            interval_backfill=FundingIntervalBackfillService(
                repository=CandidateRepository(self.repository.database),
                config=candidate_config,
            ),
            confirmation_backfill=ConfirmationBackfillService(
                repository=self.repository,
                rest_client=self.rest_client,
            ),
            settings=self.settings,
        )
        pipeline_orchestrator.start(stop_event)

        saved = 0
        created = 0
        rejected_by_reason = {
            "inactive_symbol": 0,
            "outside_capture_window": 0,
            "throttled": 0,
        }
        try:
            async for update in self.ws_client.iter_updates(stop_event):
                symbol_record = active_symbols.get(update.symbol)
                if symbol_record is None:
                    rejected_by_reason["inactive_symbol"] += 1
                    continue

                previous_next = self._last_next_funding_by_symbol.get(update.symbol)
                if previous_next is not None and previous_next != update.next_funding_time:
                    await self.event_service.store_next_predicted_rate(
                        symbol=update.symbol,
                        previous_funding_time=previous_next,
                        next_predicted_rate=update.predicted_funding_rate,
                    )
                    if update.event_time >= previous_next:
                        self.confirmations.enqueue(
                            ConfirmationRequest(update.symbol, previous_next)
                        )
                self._last_next_funding_by_symbol[update.symbol] = update.next_funding_time

                mode = determine_capture_mode(
                    update.seconds_until_funding,
                    before_seconds=self.settings.funding_window_before_seconds,
                    after_seconds=self.settings.funding_window_after_seconds,
                )
                if mode is None:
                    rejected_by_reason["outside_capture_window"] += 1
                    continue
                if not self.throttler.should_save(update.symbol, update.event_time, mode):
                    rejected_by_reason["throttled"] += 1
                    continue

                snapshot = snapshot_from_update(
                    update,
                    capture_mode=mode,
                    funding_interval_hours=symbol_record.funding_interval_hours,
                )
                created += 1
                batch_full = self.snapshot_batch.add(snapshot)
                if batch_full or self.snapshot_batch.should_flush(utc_now()):
                    saved += await self._flush_snapshots(
                        reason="batch_size" if batch_full else "interval",
                        active_symbols=active_symbols,
                    )

                if max_messages is not None and saved >= max_messages:
                    stop_event.set()
                    break
        finally:
            if timer_task is not None:
                timer_task.cancel()
            saved += await self._flush_snapshots(
                reason="shutdown",
                active_symbols=active_symbols,
            )
            await pipeline_orchestrator.close()
            await self.confirmations.close()
            logger.info(
                "collector_stopped saved_snapshots=%s created_snapshots=%s "
                "queued_snapshots=%s websocket_messages_received=%s "
                "websocket_updates_parsed=%s websocket_items_rejected=%s "
                "inactive_symbol_rejected=%s outside_capture_window_rejected=%s "
                "throttled_rejected=%s last_message_at=%s last_persist_at=%s",
                saved,
                created,
                self.snapshot_batch.pending_count,
                self.ws_client.stats.messages_received,
                self.ws_client.stats.updates_parsed,
                self.ws_client.stats.items_rejected,
                rejected_by_reason["inactive_symbol"],
                rejected_by_reason["outside_capture_window"],
                rejected_by_reason["throttled"],
                self.ws_client.stats.last_message_at.isoformat()
                if self.ws_client.stats.last_message_at is not None
                else "",
                self._last_persist_at.isoformat()
                if self._last_persist_at is not None
                else "",
            )
        return saved

    async def _flush_snapshots(
        self,
        *,
        reason: str,
        active_symbols: Mapping[str, object],
    ) -> int:
        try:
            result = await self.snapshot_batch.flush(reason=reason)
        except Exception:
            logger.exception(
                "collector_error stage=batch_flush pending_snapshot_count=%s",
                self.snapshot_batch.pending_count,
            )
            return 0
        if result.attempted == 0:
            return 0

        self._last_persist_at = utc_now()
        logger.info(
            "batch_persisted reason=%s attempted=%s inserted=%s "
            "snapshot_count=%s last_message_at=%s last_persist_at=%s "
            "active_symbol_count=%s",
            result.reason,
            result.attempted,
            result.inserted_count,
            result.inserted_count,
            self.ws_client.stats.last_message_at.isoformat()
            if self.ws_client.stats.last_message_at is not None
            else "",
            self._last_persist_at.isoformat(),
            len(active_symbols),
        )

        for snapshot in result.inserted:
            self.history_service.update(snapshot)

        try:
            observed_events = await self.event_service.observe_snapshots(
                list(result.inserted)
            )
        except Exception:
            logger.exception(
                "collector_error stage=funding_event_observation "
                "snapshot_count=%s",
                result.inserted_count,
            )
            observed_events = 0

        logger.info(
            "funding_events_observed snapshot_count=%s touched_events=%s",
            result.inserted_count,
            observed_events,
        )

        for snapshot in result.inserted:
            if snapshot.event_time >= snapshot.next_funding_time:
                self.confirmations.enqueue(
                    ConfirmationRequest(snapshot.symbol, snapshot.next_funding_time)
                )
        return result.inserted_count


async def run_collector(
    settings: Settings,
    *,
    max_messages: int | None = None,
    max_seconds: float | None = None,
) -> int:
    database = PostgresDatabase.from_settings(settings)
    async with database:
        repository = FundingRepository(database)
        async with BinanceRestClient(
            timeout_seconds=settings.rest_timeout_seconds
        ) as rest_client:
            ws_client = BinanceWebSocketClient(
                max_reconnect_delay_seconds=settings.ws_max_reconnect_delay_seconds
            )
            collector = FundingCollector(
                settings=settings,
                repository=repository,
                rest_client=rest_client,
                ws_client=ws_client,
            )
            return await collector.run(
                max_messages=max_messages, max_seconds=max_seconds
            )


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        logger.info("shutdown requested")
        stop_event.set()

    for signal_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, signal_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: request_stop())


def _start_timer(
    stop_event: asyncio.Event, max_seconds: float | None
) -> asyncio.Task[None] | None:
    if max_seconds is None:
        return None

    async def stop_after_delay() -> None:
        await asyncio.sleep(max_seconds)
        logger.info("max runtime reached")
        stop_event.set()

    return asyncio.create_task(stop_after_delay())
