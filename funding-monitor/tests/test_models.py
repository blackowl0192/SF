from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from funding_monitor.models import (
    calculate_premium_rate,
    calculate_seconds_to_funding,
    decimal_from_text,
    decimal_to_text,
    funding_direction_from_rate,
    is_above_abs_threshold,
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


def test_funding_direction_from_rate() -> None:
    assert funding_direction_from_rate(Decimal("0.0001")) == "positive"
    assert funding_direction_from_rate(Decimal("-0.0001")) == "negative"
    assert funding_direction_from_rate(Decimal(0)) == "neutral"


def test_absolute_funding_threshold() -> None:
    threshold = Decimal("0.0003")

    assert is_above_abs_threshold(Decimal("0.0003"), threshold)
    assert is_above_abs_threshold(Decimal("-0.0003"), threshold)
    assert not is_above_abs_threshold(Decimal("0.000299"), threshold)
    assert not is_above_abs_threshold(Decimal("-0.000299"), threshold)


def test_premium_rate_calculation() -> None:
    assert calculate_premium_rate(Decimal(101), Decimal(100)) == Decimal("0.01")


def test_premium_rate_returns_none_when_index_price_is_zero() -> None:
    assert calculate_premium_rate(Decimal(101), Decimal(0)) is None


def test_seconds_to_funding_is_utc_and_never_negative() -> None:
    event_time = datetime(2024, 1, 1, 7, 59, 30, tzinfo=UTC)

    assert calculate_seconds_to_funding(
        event_time, event_time + timedelta(seconds=30)
    ) == 30
    assert calculate_seconds_to_funding(
        event_time, event_time - timedelta(seconds=30)
    ) == 0
