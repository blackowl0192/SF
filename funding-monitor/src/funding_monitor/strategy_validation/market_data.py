from __future__ import annotations

from datetime import datetime
from typing import Protocol

from funding_monitor.models import ensure_utc

from .models import MarketPrice, MissingMarketDataReason


class HistoricalMarketDataProvider(Protocol):
    async def get_spot_futures_prices_at(
        self,
        *,
        exchange: str,
        futures_symbol: str,
        spot_symbol: str,
        timestamp: datetime,
    ) -> MarketPrice:
        ...


class UnavailableHistoricalMarketDataProvider:
    async def get_spot_futures_prices_at(
        self,
        *,
        exchange: str,
        futures_symbol: str,
        spot_symbol: str,
        timestamp: datetime,
    ) -> MarketPrice:
        return MarketPrice(
            spot_price=None,
            futures_price=None,
            timestamp=ensure_utc(timestamp),
            missing_reasons=(MissingMarketDataReason.PROVIDER_UNAVAILABLE,),
        )
