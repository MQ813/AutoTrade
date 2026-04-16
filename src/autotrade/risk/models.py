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
    max_drawdown: Decimal | None = None
    max_orders_per_day: int | None = None
    max_operating_capital: Decimal | None = None
    trading_halted: bool = False
    emergency_stop: bool = False
    cancel_unfilled_orders_on_market_close: bool = True

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
        if self.max_drawdown is not None and (
            self.max_drawdown <= ZERO or self.max_drawdown > ONE
        ):
            raise ValueError("max_drawdown must be between 0 and 1 inclusive")
        if self.max_orders_per_day is not None:
            _require_positive_int("max_orders_per_day", self.max_orders_per_day)
        if self.max_operating_capital is not None:
            _require_positive_decimal(
                "max_operating_capital",
                self.max_operating_capital,
            )


@dataclass(frozen=True, slots=True)
class RiskAccountSnapshot:
    holdings: tuple[Holding, ...]
    cash_available: Decimal
    total_equity: Decimal | None = None
    session_start_equity: Decimal | None = None
    peak_equity: Decimal | None = None
    orders_submitted_today: int = 0
    unfilled_order_count: int = 0
    market_closing: bool = False

    def __post_init__(self) -> None:
        _require_non_negative_decimal("cash_available", self.cash_available)
        if self.total_equity is not None:
            _require_non_negative_decimal("total_equity", self.total_equity)
        if self.session_start_equity is not None:
            _require_positive_decimal(
                "session_start_equity",
                self.session_start_equity,
            )
        if self.peak_equity is not None:
            _require_positive_decimal("peak_equity", self.peak_equity)
        _require_non_negative_int(
            "orders_submitted_today",
            self.orders_submitted_today,
        )
        _require_non_negative_int(
            "unfilled_order_count",
            self.unfilled_order_count,
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
    EMERGENCY_STOP_ACTIVE = "emergency_stop_active"
    TRADING_HALTED = "trading_halted"
    MISSING_SESSION_START_EQUITY = "missing_session_start_equity"
    MISSING_PEAK_EQUITY = "missing_peak_equity"
    LOSS_LIMIT_REACHED = "loss_limit_reached"
    DRAWDOWN_LIMIT_REACHED = "drawdown_limit_reached"
    ORDER_LIMIT_REACHED = "order_limit_reached"
    OPERATING_CAPITAL_LIMIT_EXCEEDED = "operating_capital_limit_exceeded"
    MAX_CONCURRENT_HOLDINGS_EXCEEDED = "max_concurrent_holdings_exceeded"
    MAX_POSITION_WEIGHT_EXCEEDED = "max_position_weight_exceeded"
    INSUFFICIENT_CASH = "insufficient_cash"


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
    drawdown: Decimal | None = None
    should_halt_trading: bool = False

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
        if self.drawdown is not None:
            _require_non_negative_decimal("drawdown", self.drawdown)
