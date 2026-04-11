from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_non_empty_symbol(symbol: str) -> None:
    if not symbol.strip():
        raise ValueError("symbol must not be blank")


def _require_non_negative_decimal(field_name: str, value: Decimal) -> None:
    if value < Decimal("0"):
        raise ValueError(f"{field_name} must be non-negative")


def _require_non_negative_int(field_name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


class SignalAction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass(frozen=True, slots=True)
class Quote:
    symbol: str
    price: Decimal
    as_of: datetime
    currency: str = "KRW"

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_non_negative_decimal("price", self.price)
        _require_aware_datetime("as_of", self.as_of)
        if not self.currency.strip():
            raise ValueError("currency must not be blank")


@dataclass(frozen=True, slots=True)
class Holding:
    symbol: str
    quantity: int
    average_price: Decimal
    current_price: Decimal | None = None

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_non_negative_int("quantity", self.quantity)
        _require_non_negative_decimal("average_price", self.average_price)
        if self.current_price is not None:
            _require_non_negative_decimal("current_price", self.current_price)


@dataclass(frozen=True, slots=True)
class OrderCapacity:
    symbol: str
    order_price: Decimal
    max_orderable_quantity: int
    cash_available: Decimal

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_non_negative_decimal("order_price", self.order_price)
        _require_non_negative_int(
            "max_orderable_quantity",
            self.max_orderable_quantity,
        )
        _require_non_negative_decimal("cash_available", self.cash_available)


@dataclass(frozen=True, slots=True)
class Signal:
    symbol: str
    action: SignalAction
    generated_at: datetime
    reason: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_aware_datetime("generated_at", self.generated_at)
        if self.reason is not None and not self.reason.strip():
            raise ValueError("reason must not be blank when provided")
