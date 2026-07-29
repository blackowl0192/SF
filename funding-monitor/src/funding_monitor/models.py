from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

FundingDirection = Literal["positive", "negative", "neutral"]


def utc_now() -> datetime:
    return datetime.now(UTC)


def millis_to_utc_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, UTC)


def utc_datetime_to_millis(value: datetime) -> int:
    normalized = ensure_utc(value)
    return int(normalized.timestamp() * 1000)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def datetime_to_text(value: datetime) -> str:
    return ensure_utc(value).isoformat(timespec="milliseconds")


def text_to_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return ensure_utc(value)
    parsed = datetime.fromisoformat(value)
    return ensure_utc(parsed)


def decimal_from_text(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        raise TypeError("float values are not accepted for Decimal conversion")
    if not isinstance(value, str | int):
        raise TypeError(f"unsupported Decimal value type: {type(value).__name__}")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid Decimal value: {value!r}") from exc


def decimal_to_text(value: object) -> str:
    return format(decimal_from_text(value), "f")


def decimal_to_percent_text(value: str | Decimal | None) -> str:
    if value is None:
        return ""
    return format(decimal_from_text(value) * Decimal(100), "f")


def decimal_to_percentage_point_text(value: str | Decimal | None) -> str:
    return decimal_to_percent_text(value)


def funding_direction_from_rate(rate: Decimal) -> FundingDirection:
    if rate > 0:
        return "positive"
    if rate < 0:
        return "negative"
    return "neutral"


def is_above_abs_threshold(rate: Decimal, threshold: Decimal) -> bool:
    return abs(rate) >= threshold


def calculate_premium_rate(
    mark_price: Decimal, index_price: Decimal | None
) -> Decimal | None:
    if index_price is None or index_price == 0:
        return None
    return (mark_price - index_price) / index_price


def calculate_seconds_to_funding(
    event_time: datetime, next_funding_time: datetime
) -> int:
    seconds = int(
        (ensure_utc(next_funding_time) - ensure_utc(event_time)).total_seconds()
    )
    return max(0, seconds)


@dataclass(frozen=True)
class SymbolRecord:
    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str
    status: str
    funding_interval_hours: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class MarkPriceUpdate:
    symbol: str
    event_time: datetime
    mark_price: Decimal
    index_price: Decimal | None
    estimated_settle_price: Decimal | None
    predicted_funding_rate: Decimal
    interest_rate: Decimal | None
    next_funding_time: datetime

    @property
    def seconds_until_funding(self) -> int:
        return int((self.next_funding_time - self.event_time).total_seconds())


@dataclass(frozen=True)
class FundingSnapshot:
    symbol: str
    event_time: datetime
    received_at: datetime
    mark_price: Decimal
    index_price: Decimal | None
    estimated_settle_price: Decimal | None
    predicted_funding_rate: Decimal
    funding_rate: Decimal
    interest_rate: Decimal | None
    next_funding_time: datetime
    seconds_until_funding: int
    seconds_to_funding: int
    premium_rate: Decimal | None
    funding_direction: FundingDirection
    funding_interval_hours: int
    capture_mode: str


@dataclass(frozen=True)
class FundingEvent:
    symbol: str
    funding_time: datetime
    funding_interval_hours: int
    first_predicted_rate: Decimal | None = None
    predicted_rate_10m_before: Decimal | None = None
    predicted_rate_5m_before: Decimal | None = None
    predicted_rate_1m_before: Decimal | None = None
    last_predicted_rate: Decimal | None = None
    actual_funding_rate: Decimal | None = None
    prediction_error: Decimal | None = None
    mark_price_at_funding: Decimal | None = None
    next_predicted_rate: Decimal | None = None
    confirmed_at: datetime | None = None
    status: str = "waiting"


class BinanceMarkPricePayload(BaseModel):
    event_type: str = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    symbol: str = Field(alias="s")
    mark_price: str = Field(alias="p")
    index_price: str | None = Field(default=None, alias="i")
    estimated_settle_price: str | None = Field(default=None, alias="P")
    predicted_funding_rate: str = Field(alias="r")
    next_funding_time_ms: int = Field(alias="T")

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    def to_update(self) -> MarkPriceUpdate:
        return MarkPriceUpdate(
            symbol=self.symbol,
            event_time=millis_to_utc_datetime(self.event_time_ms),
            mark_price=decimal_from_text(self.mark_price),
            index_price=decimal_from_text(self.index_price)
            if self.index_price is not None
            else None,
            estimated_settle_price=decimal_from_text(self.estimated_settle_price)
            if self.estimated_settle_price is not None
            else None,
            predicted_funding_rate=decimal_from_text(self.predicted_funding_rate),
            interest_rate=None,
            next_funding_time=millis_to_utc_datetime(self.next_funding_time_ms),
        )


def parse_mark_price_payload(payload: dict[str, Any]) -> MarkPriceUpdate:
    return BinanceMarkPricePayload.model_validate(payload).to_update()
