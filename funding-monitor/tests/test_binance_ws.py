import json

from funding_monitor.binance_ws import BINANCE_WS_URL, BinanceWebSocketClient


def test_mark_price_stream_uses_market_endpoint() -> None:
    assert "/market/ws/" in BINANCE_WS_URL
    assert not BINANCE_WS_URL.startswith("wss://fstream.binance.com/ws/!markPrice")


def test_websocket_parser_counts_parsed_and_rejected_items() -> None:
    client = BinanceWebSocketClient()
    payload = [
        {
            "e": "markPriceUpdate",
            "E": 1704096000000,
            "s": "BTCUSDT",
            "p": "43000.0",
            "i": "42990.0",
            "P": "43001.0",
            "r": "0.00010000",
            "T": 1704124800000,
        },
        {"bad": "payload"},
        "not an object",
    ]

    updates = client._parse_message(json.dumps(payload))

    assert len(updates) == 1
    assert client.stats.updates_parsed == 1
    assert client.stats.items_rejected == 2
