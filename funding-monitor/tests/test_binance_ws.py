from funding_monitor.binance_ws import BINANCE_WS_URL


def test_mark_price_stream_uses_market_endpoint() -> None:
    assert "/market/ws/" in BINANCE_WS_URL
    assert not BINANCE_WS_URL.startswith("wss://fstream.binance.com/ws/!markPrice")
