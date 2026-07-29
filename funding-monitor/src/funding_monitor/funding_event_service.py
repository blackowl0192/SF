from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import httpx

from .binance_rest import BinanceRestClient
from .models import FundingSnapshot, decimal_from_text, utc_datetime_to_millis, utc_now
from .repository import FundingRepository
from .snapshot_service import select_checkpoint_rates

logger = logging.getLogger(__name__)


class FundingEventService:
    def __init__(self, repository: FundingRepository) -> None:
        self.repository = repository

    async def observe_snapshot(
        self,
        snapshot: FundingSnapshot,
        *,
        funding_interval_hours: int,
    ) -> None:
        await self.repository.create_or_get_funding_event(
            snapshot.symbol,
            snapshot.next_funding_time,
            funding_interval_hours,
            snapshot.predicted_funding_rate,
        )
        snapshots = await self.repository.snapshots_for_event(
            snapshot.symbol, snapshot.next_funding_time
        )
        checkpoints = select_checkpoint_rates(snapshots, snapshot.next_funding_time)
        await self.repository.update_event_predictions(
            snapshot.symbol,
            snapshot.next_funding_time,
            predicted_rate_10m_before=checkpoints.predicted_rate_10m_before,
            predicted_rate_5m_before=checkpoints.predicted_rate_5m_before,
            predicted_rate_1m_before=checkpoints.predicted_rate_1m_before,
            last_predicted_rate=checkpoints.last_predicted_rate,
        )

    async def store_next_predicted_rate(
        self,
        *,
        symbol: str,
        previous_funding_time: datetime,
        next_predicted_rate: Decimal,
    ) -> None:
        await self.repository.update_next_predicted_rate(
            symbol, previous_funding_time, next_predicted_rate
        )


@dataclass(frozen=True)
class ConfirmationRequest:
    symbol: str
    funding_time: datetime


class FundingConfirmationScheduler:
    def __init__(
        self,
        *,
        repository: FundingRepository,
        rest_client: BinanceRestClient,
        initial_delay_seconds: int,
        retry_seconds: int,
        max_attempts: int,
    ) -> None:
        self.repository = repository
        self.rest_client = rest_client
        self.initial_delay_seconds = initial_delay_seconds
        self.retry_seconds = retry_seconds
        self.max_attempts = max_attempts
        self._queued: set[tuple[str, str]] = set()
        self._tasks: set[asyncio.Task[None]] = set()

    def enqueue(self, request: ConfirmationRequest) -> None:
        key = (request.symbol, request.funding_time.isoformat())
        if key in self._queued:
            return
        self._queued.add(key)
        task = asyncio.create_task(self._run_confirmation(request))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def close(self) -> None:
        if not self._tasks:
            return
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _run_confirmation(self, request: ConfirmationRequest) -> None:
        await asyncio.sleep(self.initial_delay_seconds)
        for attempt in range(1, self.max_attempts + 1):
            event = await self.repository.get_funding_event(
                request.symbol, request.funding_time
            )
            if event is not None and event.status == "confirmed":
                return

            try:
                result = await self._try_confirm(request)
            except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
                logger.warning(
                    "funding confirmation failed on attempt %s/%s for %s %s: %s",
                    attempt,
                    self.max_attempts,
                    request.symbol,
                    request.funding_time.isoformat(),
                    exc,
                )
                result = False
            if result:
                return

            if attempt < self.max_attempts:
                await asyncio.sleep(self.retry_seconds)

        await self.repository.mark_confirmation_failed(
            request.symbol, request.funding_time
        )

    async def _try_confirm(self, request: ConfirmationRequest) -> bool:
        window_start = request.funding_time - timedelta(minutes=10)
        window_end = request.funding_time + timedelta(minutes=10)
        history = await self.rest_client.get_funding_rate_history(
            request.symbol,
            start_time_ms=utc_datetime_to_millis(window_start),
            end_time_ms=utc_datetime_to_millis(window_end),
            limit=100,
        )

        funding_time_ms = utc_datetime_to_millis(request.funding_time)
        for item in history:
            if int(item.get("fundingTime", -1)) != funding_time_ms:
                continue
            actual_rate = decimal_from_text(item["fundingRate"])
            mark_price = (
                decimal_from_text(item["markPrice"])
                if item.get("markPrice") is not None
                else None
            )
            await self.repository.mark_event_confirmed(
                request.symbol,
                request.funding_time,
                actual_rate,
                mark_price,
                utc_now(),
            )
            logger.info(
                "confirmed funding event %s %s",
                request.symbol,
                request.funding_time.isoformat(),
            )
            return True
        return False
