from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime

from .binance_rest import BinanceRestClient
from .binance_ws import BinanceWebSocketClient
from .config import Settings
from .database import initialize_database
from .funding_event_service import (
    ConfirmationRequest,
    FundingConfirmationScheduler,
    FundingEventService,
)
from .repository import FundingRepository
from .snapshot_service import (
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
            normal_interval_seconds=settings.normal_snapshot_interval_seconds,
            detailed_interval_seconds=settings.detailed_snapshot_interval_seconds,
        )
        self.event_service = FundingEventService(repository)
        self.confirmations = FundingConfirmationScheduler(
            repository=repository,
            rest_client=rest_client,
            initial_delay_seconds=settings.confirmation_initial_delay_seconds,
            retry_seconds=settings.confirmation_retry_seconds,
            max_attempts=settings.confirmation_max_attempts,
        )
        self._last_next_funding_by_symbol: dict[str, datetime] = {}

    async def run(
        self,
        *,
        max_messages: int | None = None,
        max_seconds: float | None = None,
    ) -> int:
        await initialize_database(self.settings.database_path)
        symbol_service = SymbolService(self.repository, self.rest_client)
        await symbol_service.sync_symbols()
        active_symbols = await self.repository.active_symbols()
        logger.info("loaded %s active symbols", len(active_symbols))

        stop_event = asyncio.Event()
        _install_signal_handlers(stop_event)
        timer_task = _start_timer(stop_event, max_seconds)

        handled = 0
        try:
            async for update in self.ws_client.iter_updates(stop_event):
                symbol_record = active_symbols.get(update.symbol)
                if symbol_record is None:
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
                    continue
                if not self.throttler.should_save(update.symbol, update.event_time, mode):
                    continue

                snapshot = snapshot_from_update(update, capture_mode=mode)
                inserted = await self.repository.insert_snapshot(snapshot)
                if inserted:
                    await self.event_service.observe_snapshot(
                        snapshot,
                        funding_interval_hours=symbol_record.funding_interval_hours,
                    )

                if update.event_time >= update.next_funding_time:
                    self.confirmations.enqueue(
                        ConfirmationRequest(update.symbol, update.next_funding_time)
                    )

                handled += 1
                if max_messages is not None and handled >= max_messages:
                    stop_event.set()
                    break
        finally:
            if timer_task is not None:
                timer_task.cancel()
            await self.confirmations.close()
        return handled


async def run_collector(
    settings: Settings,
    *,
    max_messages: int | None = None,
    max_seconds: float | None = None,
) -> int:
    repository = FundingRepository(settings.database_path)
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
        return await collector.run(max_messages=max_messages, max_seconds=max_seconds)


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
