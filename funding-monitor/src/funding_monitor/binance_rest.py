from __future__ import annotations

import asyncio
import logging
from typing import Any, Self

import httpx

BINANCE_REST_BASE_URL = "https://fapi.binance.com"

logger = logging.getLogger(__name__)


class BinanceRestClient:
    def __init__(
        self,
        *,
        base_url: str = BINANCE_REST_BASE_URL,
        timeout_seconds: float = 10,
        max_attempts: int = 3,
        initial_backoff_seconds: float = 0.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.initial_backoff_seconds = initial_backoff_seconds
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout_seconds),
            )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_server_time(self) -> dict[str, Any]:
        return await self._get("/fapi/v1/time")

    async def get_exchange_info(self) -> dict[str, Any]:
        return await self._get("/fapi/v1/exchangeInfo")

    async def get_premium_index(self, symbol: str | None = None) -> Any:
        params = {"symbol": symbol} if symbol else None
        return await self._get("/fapi/v1/premiumIndex", params=params)

    async def get_funding_rate_history(
        self,
        symbol: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        if limit is not None:
            params["limit"] = limit
        result = await self._get("/fapi/v1/fundingRate", params=params)
        if not isinstance(result, list):
            raise TypeError("unexpected fundingRate response")
        return result

    async def get_funding_info(self) -> list[dict[str, Any]]:
        result = await self._get("/fapi/v1/fundingInfo")
        if not isinstance(result, list):
            raise TypeError("unexpected fundingInfo response")
        return result

    async def _get(
        self, path: str, params: dict[str, Any] | None = None
    ) -> Any:
        if self._client is None:
            await self.start()
        assert self._client is not None

        delay = self.initial_backoff_seconds
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = await self._client.get(path, params=params)
                if response.status_code in (429, 418) or 500 <= response.status_code < 600:
                    response.raise_for_status()
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPStatusError, httpx.TransportError) as exc:
                last_error = exc
                retryable = self._is_retryable(exc)
                if attempt >= self.max_attempts or not retryable:
                    raise
                logger.warning(
                    "REST %s failed on attempt %s/%s: %s",
                    path,
                    attempt,
                    self.max_attempts,
                    exc,
                )
                await asyncio.sleep(delay)
                delay *= 2

        if last_error is not None:
            raise last_error
        raise RuntimeError("request failed without an exception")

    def _is_retryable(self, exc: Exception) -> bool:
        if isinstance(exc, httpx.TransportError):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            return status in (429, 418) or 500 <= status < 600
        return False
