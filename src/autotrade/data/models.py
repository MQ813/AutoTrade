from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from enum import StrEnum


def _require_non_blank_text(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_non_negative_decimal(field_name: str, value: Decimal) -> None:
    if value < Decimal("0"):
        raise ValueError(f"{field_name} must be non-negative")


def _require_non_negative_int(field_name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


class Timeframe(StrEnum):
    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    DAY = "1d"

    @property
    def interval(self) -> timedelta:
        if self is Timeframe.MINUTE_1:
            return timedelta(minutes=1)
        if self is Timeframe.MINUTE_5:
            return timedelta(minutes=5)
        if self is Timeframe.MINUTE_15:
            return timedelta(minutes=15)
        if self is Timeframe.MINUTE_30:
            return timedelta(minutes=30)
        if self is Timeframe.HOUR_1:
            return timedelta(hours=1)
        return timedelta(days=1)


@dataclass(frozen=True, slots=True)
class UniverseMember:
    symbol: str
    name: str | None = None
    active: bool = True

    def __post_init__(self) -> None:
        _require_non_blank_text("symbol", self.symbol)
        if self.name is not None:
            _require_non_blank_text("name", self.name)


@dataclass(frozen=True, slots=True)
class Bar:
    symbol: str
    timeframe: Timeframe
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = 0

    def __post_init__(self) -> None:
        _require_non_blank_text("symbol", self.symbol)
        _require_aware_datetime("timestamp", self.timestamp)
        _require_non_negative_decimal("open", self.open)
        _require_non_negative_decimal("high", self.high)
        _require_non_negative_decimal("low", self.low)
        _require_non_negative_decimal("close", self.close)
        _require_non_negative_int("volume", self.volume)

        if self.low > self.high:
            raise ValueError("low must not exceed high")

        if self.open < self.low or self.open > self.high:
            raise ValueError("open must be within low and high")

        if self.close < self.low or self.close > self.high:
            raise ValueError("close must be within low and high")
