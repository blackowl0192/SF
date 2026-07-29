import asyncio

from funding_monitor.models import SymbolRecord
from funding_monitor.symbol_service import SymbolService


class FakeRepository:
    def __init__(self) -> None:
        self.symbols: dict[str, SymbolRecord] = {}

    async def upsert_symbols(self, symbols):
        rows = list(symbols)
        self.symbols.update({row.symbol: row for row in rows})
        return len(rows)

    async def active_symbols(self):
        return self.symbols


class FakeRestClient:
    async def get_exchange_info(self):
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                },
                {
                    "symbol": "ETHUSDC",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDC",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                },
                {
                    "symbol": "OLDUSDT",
                    "baseAsset": "OLD",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "BREAK",
                },
            ]
        }

    async def get_funding_info(self):
        return [{"symbol": "BTCUSDT", "fundingIntervalHours": 4}]


def test_symbol_sync_filters_and_applies_funding_interval() -> None:
    async def scenario() -> None:
        repository = FakeRepository()
        service = SymbolService(repository, FakeRestClient())  # type: ignore[arg-type]

        assert await service.sync_symbols() == 1
        assert await service.sync_symbols() == 1
        symbols = await repository.active_symbols()

        assert list(symbols) == ["BTCUSDT"]
        assert symbols["BTCUSDT"].funding_interval_hours == 4

    asyncio.run(scenario())
