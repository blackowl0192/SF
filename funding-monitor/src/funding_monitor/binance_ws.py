from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import websockets
from pydantic import ValidationError
from websockets.exceptions import WebSocketException

from .models import MarkPriceUpdate, parse_mark_price_payload, utc_now

BINANCE_WS_URL = "wss://fstream.binance.com/market/ws/!markPrice@arr@1s"

logger = logging.getLogger(__name__)


@dataclass
class WebSocketClientStats:
    messages_received: int = 0
    updates_parsed: int = 0
    items_rejected: int = 0
    connections: int = 0
    disconnects: int = 0
    last_message_at: datetime | None = None


class BinanceWebSocketClient:
    def __init__(
        self,
        *,
        url: str = BINANCE_WS_URL,
        max_reconnect_delay_seconds: int = 60,
    ) -> None:
        self.url = url
        self.max_reconnect_delay_seconds = max_reconnect_delay_seconds
        self.stats = WebSocketClientStats()

    async def iter_updates(
        self, stop_event: asyncio.Event
    ) -> AsyncIterator[MarkPriceUpdate]:
        delay = 1
        while not stop_event.is_set():
            try:
                logger.info("reconnect_attempt url=%s delay_seconds=0", self.url)
                async with websockets.connect(self.url) as websocket:
                    self.stats.connections += 1
                    logger.info("websocket_connected url=%s", self.url)
                    if self.stats.disconnects:
                        logger.info(
                            "reconnect_success connections=%s",
                            self.stats.connections,
                        )
                    delay = 1
                    try:
                        while not stop_event.is_set():
                            try:
                                message = await asyncio.wait_for(
                                    websocket.recv(), timeout=1
                                )
                            except TimeoutError:
                                continue
                            self.stats.messages_received += 1
                            self.stats.last_message_at = utc_now()
                            for update in self._parse_message(message):
                                if stop_event.is_set():
                                    break
                                yield update
                    finally:
                        self.stats.disconnects += 1
                        logger.info(
                            "websocket_disconnected messages_received=%s "
                            "updates_parsed=%s items_rejected=%s last_message_at=%s",
                            self.stats.messages_received,
                            self.stats.updates_parsed,
                            self.stats.items_rejected,
                            self.stats.last_message_at.isoformat()
                            if self.stats.last_message_at is not None
                            else "",
                        )
            except asyncio.CancelledError:
                raise
            except (OSError, TimeoutError, WebSocketException) as exc:
                logger.warning("Binance WebSocket error: %s", exc)

            if stop_event.is_set():
                break
            logger.info("reconnect_attempt url=%s delay_seconds=%s", self.url, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.max_reconnect_delay_seconds)

        logger.info("Binance WebSocket client stopped")

    def _parse_message(self, message: str | bytes) -> list[MarkPriceUpdate]:
        try:
            raw = json.loads(message)
        except json.JSONDecodeError as exc:
            logger.warning("ignoring invalid WebSocket JSON: %s", exc)
            self.stats.items_rejected += 1
            return []

        payload = raw.get("data", raw) if isinstance(raw, dict) else raw
        if isinstance(payload, dict):
            items: list[Any] = [payload]
        elif isinstance(payload, list):
            items = payload
        else:
            logger.warning("ignoring unexpected WebSocket message type")
            self.stats.items_rejected += 1
            return []

        updates: list[MarkPriceUpdate] = []
        for item in items:
            if not isinstance(item, dict):
                logger.warning("ignoring non-object WebSocket item")
                self.stats.items_rejected += 1
                continue
            try:
                updates.append(parse_mark_price_payload(item))
            except (ValidationError, TypeError, ValueError) as exc:
                logger.warning("ignoring invalid mark price payload: %s", exc)
                self.stats.items_rejected += 1
        self.stats.updates_parsed += len(updates)
        return updates
