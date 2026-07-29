import asyncio

from funding_monitor.database import initialize_database
from funding_monitor.repository import FundingRepository
from funding_monitor.symbol_service import SymbolService


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


def run(coro):
    return asyncio.run(coro)


def test_symbol_sync_filters_and_applies_funding_interval(tmp_path) -> None:
    async def scenario() -> None:
        database_path = tmp_path / "funding.db"
        await initialize_database(database_path)
        repository = FundingRepository(database_path)
        service = SymbolService(repository, FakeRestClient())  # type: ignore[arg-type]

        assert await service.sync_symbols() == 1
        assert await service.sync_symbols() == 1
        symbols = await repository.active_symbols()

        assert list(symbols) == ["BTCUSDT"]
        assert symbols["BTCUSDT"].funding_interval_hours == 4

    run(scenario())
