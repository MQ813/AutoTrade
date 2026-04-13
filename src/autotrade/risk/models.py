from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from autotrade.common import Holding

ZERO = Decimal("0")
ONE = Decimal("1")


def _require_non_empty_symbol(symbol: str) -> None:
    if not symbol.strip():
        raise ValueError("symbol must not be blank")


def _require_positive_decimal(field_name: str, value: Decimal) -> None:
    if value <= ZERO:
        raise ValueError(f"{field_name} must be positive")


def _require_non_negative_decimal(field_name: str, value: Decimal) -> None:
    if value < ZERO:
        raise ValueError(f"{field_name} must be non-negative")


def _require_positive_int(field_name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _require_non_negative_int(field_name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


@dataclass(frozen=True, slots=True)
class RiskSettings:
    max_position_weight: Decimal = Decimal("0.2")
    max_concurrent_holdings: int = 3
    max_loss: Decimal | None = None
    trading_halted: bool = False

    def __post_init__(self) -> None:
        if self.max_position_weight <= ZERO or self.max_position_weight > ONE:
            raise ValueError(
                "max_position_weight must be between 0 and 1 inclusive",
            )
        _require_positive_int(
            "max_concurrent_holdings",
            self.max_concurrent_holdings,
        )
        if self.max_loss is not None:
            _require_non_negative_decimal("max_loss", self.max_loss)


@dataclass(frozen=True, slots=True)
class RiskAccountSnapshot:
    holdings: tuple[Holding, ...]
    cash_available: Decimal
    total_equity: Decimal | None = None
    session_start_equity: Decimal | None = None

    def __post_init__(self) -> None:
        _require_non_negative_decimal("cash_available", self.cash_available)
        if self.total_equity is not None:
            _require_non_negative_decimal("total_equity", self.total_equity)
        if self.session_start_equity is not None:
            _require_positive_decimal(
                "session_start_equity",
                self.session_start_equity,
            )


@dataclass(frozen=True, slots=True)
class ProposedBuyOrder:
    symbol: str
    price: Decimal
    quantity: int

    def __post_init__(self) -> None:
        _require_non_empty_symbol(self.symbol)
        _require_positive_decimal("price", self.price)
        _require_positive_int("quantity", self.quantity)


class RiskViolationCode(StrEnum):
    TRADING_HALTED = "trading_halted"
    MISSING_SESSION_START_EQUITY = "missing_session_start_equity"
    LOSS_LIMIT_REACHED = "loss_limit_reached"
    MAX_CONCURRENT_HOLDINGS_EXCEEDED = "max_concurrent_holdings_exceeded"
    MAX_POSITION_WEIGHT_EXCEEDED = "max_position_weight_exceeded"


@dataclass(frozen=True, slots=True)
class RiskViolation:
    code: RiskViolationCode
    message: str

    def __post_init__(self) -> None:
        if not self.message.strip():
            raise ValueError("message must not be blank")


@dataclass(frozen=True, slots=True)
class RiskCheck:
    allowed: bool
    approved_quantity: int
    current_equity: Decimal
    projected_position_weight: Decimal | None
    violations: tuple[RiskViolation, ...]
    loss_amount: Decimal | None = None

    def __post_init__(self) -> None:
        _require_non_negative_int("approved_quantity", self.approved_quantity)
        _require_non_negative_decimal("current_equity", self.current_equity)
        if self.projected_position_weight is not None:
            _require_non_negative_decimal(
                "projected_position_weight",
                self.projected_position_weight,
            )
        if self.loss_amount is not None:
            _require_non_negative_decimal("loss_amount", self.loss_amount)
