from datetime import UTC
from decimal import Decimal

import pytest

from funding_monitor.models import (
    decimal_from_text,
    decimal_to_text,
    millis_to_utc_datetime,
    parse_mark_price_payload,
)


def test_millis_to_utc_datetime() -> None:
    value = millis_to_utc_datetime(1704067200123)

    assert value.tzinfo is UTC
    assert value.isoformat() == "2024-01-01T00:00:00.123000+00:00"


def test_decimal_conversion_does_not_accept_float() -> None:
    value = decimal_from_text("0.0100")

    assert value == Decimal("0.0100")
    assert decimal_to_text(value) == "0.0100"
    with pytest.raises(TypeError):
        decimal_from_text(0.1)  # type: ignore[arg-type]


def test_mark_price_payload_ignores_unknown_fields() -> None:
    update = parse_mark_price_payload(
        {
            "e": "markPriceUpdate",
            "E": 1704067200123,
            "s": "BTCUSDT",
            "p": "43000.12340000",
            "i": "42990.00000000",
            "P": "43005.00000000",
            "r": "0.00010000",
            "T": 1704096000000,
            "ignored": "field",
        }
    )

    assert update.symbol == "BTCUSDT"
    assert update.mark_price == Decimal("43000.12340000")
    assert update.index_price == Decimal("42990.00000000")
    assert update.estimated_settle_price == Decimal("43005.00000000")
    assert update.predicted_funding_rate == Decimal("0.00010000")
    assert update.next_funding_time.isoformat() == "2024-01-01T08:00:00+00:00"
