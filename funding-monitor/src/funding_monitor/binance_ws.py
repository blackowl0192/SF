from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import websockets
from pydantic import ValidationError
from websockets.exceptions import WebSocketException

from .models import MarkPriceUpdate, parse_mark_price_payload

BINANCE_WS_URL = "wss://fstream.binance.com/ws/!markPrice@arr@1s"

logger = logging.getLogger(__name__)


class BinanceWebSocketClient:
    def __init__(
        self,
        *,
        url: str = BINANCE_WS_URL,
        max_reconnect_delay_seconds: int = 60,
    ) -> None:
        self.url = url
        self.max_reconnect_delay_seconds = max_reconnect_delay_seconds

    async def iter_updates(
        self, stop_event: asyncio.Event
    ) -> AsyncIterator[MarkPriceUpdate]:
        delay = 1
        while not stop_event.is_set():
            try:
                logger.info("connecting to Binance WebSocket: %s", self.url)
                async with websockets.connect(self.url) as websocket:
                    logger.info("connected to Binance WebSocket")
                    delay = 1
                    while not stop_event.is_set():
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=1)
                        except TimeoutError:
                            continue
                        for update in self._parse_message(message):
                            yield update
            except asyncio.CancelledError:
                raise
            except (OSError, TimeoutError, WebSocketException) as exc:
                logger.warning("Binance WebSocket error: %s", exc)

            if stop_event.is_set():
                break
            logger.info("reconnecting to Binance WebSocket in %s seconds", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.max_reconnect_delay_seconds)

        logger.info("Binance WebSocket client stopped")

    def _parse_message(self, message: str | bytes) -> list[MarkPriceUpdate]:
        try:
            raw = json.loads(message)
        except json.JSONDecodeError as exc:
            logger.warning("ignoring invalid WebSocket JSON: %s", exc)
            return []

        payload = raw.get("data", raw) if isinstance(raw, dict) else raw
        if isinstance(payload, dict):
            items: list[Any] = [payload]
        elif isinstance(payload, list):
            items = payload
        else:
            logger.warning("ignoring unexpected WebSocket message type")
            return []

        updates: list[MarkPriceUpdate] = []
        for item in items:
            if not isinstance(item, dict):
                logger.warning("ignoring non-object WebSocket item")
                continue
            try:
                updates.append(parse_mark_price_payload(item))
            except (ValidationError, TypeError, ValueError) as exc:
                logger.warning("ignoring invalid mark price payload: %s", exc)
        return updates
