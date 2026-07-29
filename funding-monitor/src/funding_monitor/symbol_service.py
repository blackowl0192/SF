from __future__ import annotations

from .binance_rest import BinanceRestClient
from .models import SymbolRecord, utc_now
from .repository import FundingRepository

DEFAULT_FUNDING_INTERVAL_HOURS = 8


class SymbolService:
    def __init__(
        self,
        repository: FundingRepository,
        rest_client: BinanceRestClient,
    ) -> None:
        self.repository = repository
        self.rest_client = rest_client

    async def sync_symbols(self) -> int:
        exchange_info = await self.rest_client.get_exchange_info()
        funding_info = await self.rest_client.get_funding_info()
        interval_overrides = {
            item["symbol"]: int(item["fundingIntervalHours"])
            for item in funding_info
            if "symbol" in item and "fundingIntervalHours" in item
        }

        now = utc_now()
        records: list[SymbolRecord] = []
        for item in exchange_info.get("symbols", []):
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("status") != "TRADING":
                continue
            if item.get("quoteAsset") != "USDT":
                continue
            symbol = item["symbol"]
            records.append(
                SymbolRecord(
                    symbol=symbol,
                    base_asset=item["baseAsset"],
                    quote_asset=item["quoteAsset"],
                    contract_type=item["contractType"],
                    status=item["status"],
                    funding_interval_hours=interval_overrides.get(
                        symbol, DEFAULT_FUNDING_INTERVAL_HOURS
                    ),
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                )
            )

        return await self.repository.upsert_symbols(records)
