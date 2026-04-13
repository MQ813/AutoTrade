from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

ZERO = Decimal("0")


def _require_non_blank(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")


def _require_non_negative_decimal(field_name: str, value: Decimal) -> None:
    if value < ZERO:
        raise ValueError(f"{field_name} must be non-negative")


def _require_positive_decimal(field_name: str, value: Decimal) -> None:
    if value <= ZERO:
        raise ValueError(f"{field_name} must be positive")


def _require_non_negative_int(field_name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _require_positive_int(field_name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


@dataclass(frozen=True, slots=True)
class BacktestPortfolioState:
    initial_cash: Decimal
    cash: Decimal
    position_quantity: int = 0
    average_price: Decimal = ZERO
    position_cost_basis: Decimal = ZERO
    realized_pnl: Decimal = ZERO

    def __post_init__(self) -> None:
        _require_non_negative_decimal("initial_cash", self.initial_cash)
        _require_non_negative_decimal("cash", self.cash)
        _require_non_negative_int("position_quantity", self.position_quantity)
        _require_non_negative_decimal("average_price", self.average_price)
        _require_non_negative_decimal(
            "position_cost_basis",
            self.position_cost_basis,
        )
        if self.position_quantity == 0:
            if self.average_price != ZERO:
                raise ValueError(
                    "average_price must be zero when position_quantity is zero",
                )
            if self.position_cost_basis != ZERO:
                raise ValueError(
                    "position_cost_basis must be zero when position_quantity is zero",
                )
        else:
            _require_positive_decimal("average_price", self.average_price)
            if self.position_cost_basis <= ZERO:
                raise ValueError(
                    "position_cost_basis must be positive when position is open",
                )


@dataclass(frozen=True, slots=True)
class PortfolioSnapshot:
    symbol: str
    timestamp: datetime
    close_price: Decimal
    cash: Decimal
    position_quantity: int
    position_average_price: Decimal
    position_market_value: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_pnl: Decimal
    total_equity: Decimal

    def __post_init__(self) -> None:
        _require_non_blank("symbol", self.symbol)
        _require_non_negative_decimal("close_price", self.close_price)
        _require_non_negative_decimal("cash", self.cash)
        _require_non_negative_int("position_quantity", self.position_quantity)
        _require_non_negative_decimal(
            "position_average_price",
            self.position_average_price,
        )
        _require_non_negative_decimal(
            "position_market_value",
            self.position_market_value,
        )
        _require_non_negative_decimal("total_equity", self.total_equity)
        if self.position_quantity == 0 and self.position_average_price != ZERO:
            raise ValueError(
                "position_average_price must be zero when position_quantity is zero",
            )


def create_backtest_portfolio(initial_cash: Decimal) -> BacktestPortfolioState:
    _require_non_negative_decimal("initial_cash", initial_cash)
    return BacktestPortfolioState(
        initial_cash=initial_cash,
        cash=initial_cash,
    )


def apply_buy_fill(
    state: BacktestPortfolioState,
    *,
    price: Decimal,
    quantity: int,
    fees: Decimal = ZERO,
) -> BacktestPortfolioState:
    _require_positive_decimal("price", price)
    _require_positive_int("quantity", quantity)
    _require_non_negative_decimal("fees", fees)

    quantity_decimal = Decimal(quantity)
    notional = price * quantity_decimal
    total_cost = notional + fees
    if total_cost > state.cash:
        raise ValueError("buy cost exceeds available cash")

    total_quantity = state.position_quantity + quantity
    existing_notional = state.average_price * Decimal(state.position_quantity)
    updated_notional = existing_notional + notional

    return BacktestPortfolioState(
        initial_cash=state.initial_cash,
        cash=state.cash - total_cost,
        position_quantity=total_quantity,
        average_price=updated_notional / Decimal(total_quantity),
        position_cost_basis=state.position_cost_basis + total_cost,
        realized_pnl=state.realized_pnl,
    )


def apply_sell_fill(
    state: BacktestPortfolioState,
    *,
    price: Decimal,
    quantity: int,
    fees: Decimal = ZERO,
) -> BacktestPortfolioState:
    _require_positive_decimal("price", price)
    _require_positive_int("quantity", quantity)
    _require_non_negative_decimal("fees", fees)
    if quantity > state.position_quantity:
        raise ValueError("sell quantity exceeds open position")

    quantity_decimal = Decimal(quantity)
    current_quantity_decimal = Decimal(state.position_quantity)
    notional = price * quantity_decimal
    cost_basis_portion = state.position_cost_basis * (
        quantity_decimal / current_quantity_decimal
    )
    updated_quantity = state.position_quantity - quantity

    return BacktestPortfolioState(
        initial_cash=state.initial_cash,
        cash=state.cash + notional - fees,
        position_quantity=updated_quantity,
        average_price=ZERO if updated_quantity == 0 else state.average_price,
        position_cost_basis=ZERO
        if updated_quantity == 0
        else state.position_cost_basis - cost_basis_portion,
        realized_pnl=state.realized_pnl + notional - fees - cost_basis_portion,
    )


def build_portfolio_snapshot(
    state: BacktestPortfolioState,
    *,
    symbol: str,
    timestamp: datetime,
    close_price: Decimal,
) -> PortfolioSnapshot:
    _require_positive_decimal("close_price", close_price)
    quantity_decimal = Decimal(state.position_quantity)
    market_value = close_price * quantity_decimal
    unrealized_pnl = market_value - state.position_cost_basis
    total_pnl = state.realized_pnl + unrealized_pnl

    return PortfolioSnapshot(
        symbol=symbol,
        timestamp=timestamp,
        close_price=close_price,
        cash=state.cash,
        position_quantity=state.position_quantity,
        position_average_price=state.average_price,
        position_market_value=market_value,
        realized_pnl=state.realized_pnl,
        unrealized_pnl=unrealized_pnl,
        total_pnl=total_pnl,
        total_equity=state.cash + market_value,
    )
