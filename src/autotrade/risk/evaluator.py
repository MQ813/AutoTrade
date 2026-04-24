from __future__ import annotations

from decimal import Decimal

from autotrade.common import Holding
from autotrade.risk.models import ProposedBuyOrder
from autotrade.risk.models import RiskAccountSnapshot
from autotrade.risk.models import RiskCheck
from autotrade.risk.models import RiskSettings
from autotrade.risk.models import RiskViolation
from autotrade.risk.models import RiskViolationCode

ZERO = Decimal("0")


def evaluate_buy_order(
    settings: RiskSettings,
    snapshot: RiskAccountSnapshot,
    order: ProposedBuyOrder,
) -> RiskCheck:
    current_equity = _resolve_total_equity(snapshot)
    requested_position_weight = _calculate_projected_position_weight(
        holdings=snapshot.holdings,
        symbol=order.symbol,
        order_price=order.price,
        quantity=order.quantity,
        total_equity=current_equity,
    )
    approved_quantity = order.quantity
    violations: list[RiskViolation] = []
    should_halt_trading = False
    loss_amount = _calculate_loss_amount(
        session_start_equity=snapshot.session_start_equity,
        current_equity=current_equity,
    )
    drawdown = _calculate_drawdown(
        peak_equity=snapshot.peak_equity,
        current_equity=current_equity,
    )

    if settings.emergency_stop:
        violations.append(
            RiskViolation(
                code=RiskViolationCode.EMERGENCY_STOP_ACTIVE,
                message="emergency stop is active",
            ),
        )
        approved_quantity = 0
        should_halt_trading = True

    if settings.trading_halted:
        violations.append(
            RiskViolation(
                code=RiskViolationCode.TRADING_HALTED,
                message="trading is halted by risk settings",
            ),
        )
        approved_quantity = 0
        should_halt_trading = True

    if settings.max_loss is not None:
        if snapshot.session_start_equity is None:
            violations.append(
                RiskViolation(
                    code=RiskViolationCode.MISSING_SESSION_START_EQUITY,
                    message="session_start_equity is required when max_loss is configured",
                ),
            )
            approved_quantity = 0
            should_halt_trading = True
        elif loss_amount is not None and loss_amount >= settings.max_loss:
            violations.append(
                RiskViolation(
                    code=RiskViolationCode.LOSS_LIMIT_REACHED,
                    message=(
                        "current loss exceeds the configured max_loss "
                        f"({loss_amount} >= {settings.max_loss})"
                    ),
                ),
            )
            approved_quantity = 0
            should_halt_trading = True

    if settings.max_drawdown is not None:
        if snapshot.peak_equity is None:
            violations.append(
                RiskViolation(
                    code=RiskViolationCode.MISSING_PEAK_EQUITY,
                    message="peak_equity is required when max_drawdown is configured",
                ),
            )
            approved_quantity = 0
            should_halt_trading = True
        elif drawdown is not None and drawdown >= settings.max_drawdown:
            violations.append(
                RiskViolation(
                    code=RiskViolationCode.DRAWDOWN_LIMIT_REACHED,
                    message=(
                        "current drawdown exceeds the configured max_drawdown "
                        f"({drawdown} >= {settings.max_drawdown})"
                    ),
                ),
            )
            approved_quantity = 0
            should_halt_trading = True

    if (
        settings.max_orders_per_day is not None
        and snapshot.orders_submitted_today >= settings.max_orders_per_day
    ):
        violations.append(
            RiskViolation(
                code=RiskViolationCode.ORDER_LIMIT_REACHED,
                message=(
                    "orders submitted today reached max_orders_per_day "
                    f"({snapshot.orders_submitted_today} >= "
                    f"{settings.max_orders_per_day})"
                ),
            ),
        )
        approved_quantity = 0

    if (
        approved_quantity > 0
        and _is_new_symbol(order.symbol, snapshot.holdings)
        and _count_open_holdings(snapshot.holdings) >= settings.max_concurrent_holdings
    ):
        violations.append(
            RiskViolation(
                code=RiskViolationCode.MAX_CONCURRENT_HOLDINGS_EXCEEDED,
                message=(
                    "buying a new symbol would exceed max_concurrent_holdings "
                    f"({settings.max_concurrent_holdings})"
                ),
            ),
        )
        approved_quantity = 0

    if approved_quantity > 0:
        max_quantity_by_weight = _calculate_max_buy_quantity_by_weight(
            settings=settings,
            snapshot=snapshot,
            symbol=order.symbol,
            order_price=order.price,
        )
        max_quantity_by_entry_order_weight = (
            _calculate_max_buy_quantity_by_entry_order_weight(
                settings=settings,
                snapshot=snapshot,
                order_price=order.price,
            )
        )
        max_quantity_by_operating_capital = (
            _calculate_max_buy_quantity_by_operating_capital(
                settings=settings,
                snapshot=snapshot,
                order_price=order.price,
            )
        )
        max_quantity_by_cash = _calculate_max_buy_quantity_by_cash(
            snapshot=snapshot,
            order_price=order.price,
        )
        approved_quantity = min(
            approved_quantity,
            max_quantity_by_weight,
            max_quantity_by_entry_order_weight,
            max_quantity_by_operating_capital,
            max_quantity_by_cash,
        )

        if order.quantity > max_quantity_by_weight:
            violations.append(
                RiskViolation(
                    code=RiskViolationCode.MAX_POSITION_WEIGHT_EXCEEDED,
                    message=(
                        "requested quantity would exceed max_position_weight "
                        f"({settings.max_position_weight})"
                    ),
                ),
            )
        if order.quantity > max_quantity_by_entry_order_weight:
            violations.append(
                RiskViolation(
                    code=RiskViolationCode.ENTRY_MAX_POSITION_WEIGHT_PER_ORDER_EXCEEDED,
                    message=(
                        "requested quantity would exceed "
                        "entry_max_position_weight_per_order "
                        f"({settings.entry_max_position_weight_per_order})"
                    ),
                ),
            )
        if (
            settings.max_operating_capital is not None
            and order.quantity > max_quantity_by_operating_capital
        ):
            violations.append(
                RiskViolation(
                    code=RiskViolationCode.OPERATING_CAPITAL_LIMIT_EXCEEDED,
                    message=(
                        "requested quantity would exceed max_operating_capital "
                        f"({settings.max_operating_capital})"
                    ),
                ),
            )
        if order.quantity > max_quantity_by_cash:
            violations.append(
                RiskViolation(
                    code=RiskViolationCode.INSUFFICIENT_CASH,
                    message=(
                        "requested quantity would exceed available cash "
                        f"({snapshot.cash_available})"
                    ),
                ),
            )

    return RiskCheck(
        allowed=not violations and approved_quantity == order.quantity,
        approved_quantity=approved_quantity,
        current_equity=current_equity,
        projected_position_weight=requested_position_weight,
        violations=tuple(violations),
        loss_amount=loss_amount,
        drawdown=drawdown,
        should_halt_trading=should_halt_trading,
    )


def calculate_max_buy_quantity(
    *,
    settings: RiskSettings,
    snapshot: RiskAccountSnapshot,
    symbol: str,
    order_price: Decimal,
) -> int:
    return min(
        _calculate_max_buy_quantity_by_weight(
            settings=settings,
            snapshot=snapshot,
            symbol=symbol,
            order_price=order_price,
        ),
        _calculate_max_buy_quantity_by_entry_order_weight(
            settings=settings,
            snapshot=snapshot,
            order_price=order_price,
        ),
        _calculate_max_buy_quantity_by_operating_capital(
            settings=settings,
            snapshot=snapshot,
            order_price=order_price,
        ),
        _calculate_max_buy_quantity_by_cash(
            snapshot=snapshot,
            order_price=order_price,
        ),
    )


def should_cancel_unfilled_orders(
    settings: RiskSettings,
    snapshot: RiskAccountSnapshot,
) -> bool:
    return (
        settings.cancel_unfilled_orders_on_market_close
        and snapshot.market_closing
        and snapshot.unfilled_order_count > 0
    )


def _calculate_max_buy_quantity_by_weight(
    *,
    settings: RiskSettings,
    snapshot: RiskAccountSnapshot,
    symbol: str,
    order_price: Decimal,
) -> int:
    current_equity = _resolve_total_equity(snapshot)
    if current_equity <= ZERO:
        return 0

    current_symbol_value = _current_symbol_value(symbol, snapshot.holdings)
    max_position_value = current_equity * settings.max_position_weight
    remaining_position_value = max_position_value - current_symbol_value
    if remaining_position_value <= ZERO:
        return 0
    return int(remaining_position_value / order_price)


def _calculate_max_buy_quantity_by_cash(
    *,
    snapshot: RiskAccountSnapshot,
    order_price: Decimal,
) -> int:
    if snapshot.cash_available <= ZERO:
        return 0
    return int(snapshot.cash_available / order_price)


def _calculate_max_buy_quantity_by_entry_order_weight(
    *,
    settings: RiskSettings,
    snapshot: RiskAccountSnapshot,
    order_price: Decimal,
) -> int:
    current_equity = _resolve_total_equity(snapshot)
    if current_equity <= ZERO:
        return 0

    max_order_value = current_equity * settings.entry_max_position_weight_per_order
    if max_order_value <= ZERO:
        return 0
    return int(max_order_value / order_price)


def _calculate_max_buy_quantity_by_operating_capital(
    *,
    settings: RiskSettings,
    snapshot: RiskAccountSnapshot,
    order_price: Decimal,
) -> int:
    if settings.max_operating_capital is None:
        return _calculate_max_buy_quantity_by_cash(
            snapshot=snapshot,
            order_price=order_price,
        )

    current_position_value = sum(
        (_holding_market_value(holding) for holding in snapshot.holdings),
        start=ZERO,
    )
    remaining_operating_capital = (
        settings.max_operating_capital - current_position_value
    )
    if remaining_operating_capital <= ZERO:
        return 0

    effective_cash = min(snapshot.cash_available, remaining_operating_capital)
    if effective_cash <= ZERO:
        return 0
    return int(effective_cash / order_price)


def _resolve_total_equity(snapshot: RiskAccountSnapshot) -> Decimal:
    if snapshot.total_equity is not None:
        return snapshot.total_equity

    holdings_value = sum(
        (_holding_market_value(holding) for holding in snapshot.holdings),
        start=ZERO,
    )
    return snapshot.cash_available + holdings_value


def _holding_market_value(holding: Holding) -> Decimal:
    reference_price = (
        holding.current_price
        if holding.current_price is not None
        else holding.average_price
    )
    return reference_price * holding.quantity


def _current_symbol_value(symbol: str, holdings: tuple[Holding, ...]) -> Decimal:
    total = ZERO
    for holding in holdings:
        if holding.symbol == symbol and holding.quantity > 0:
            total += _holding_market_value(holding)
    return total


def _count_open_holdings(holdings: tuple[Holding, ...]) -> int:
    return sum(1 for holding in holdings if holding.quantity > 0)


def _is_new_symbol(symbol: str, holdings: tuple[Holding, ...]) -> bool:
    return all(
        holding.symbol != symbol or holding.quantity == 0 for holding in holdings
    )


def _calculate_projected_position_weight(
    *,
    holdings: tuple[Holding, ...],
    symbol: str,
    order_price: Decimal,
    quantity: int,
    total_equity: Decimal,
) -> Decimal | None:
    if total_equity <= ZERO:
        return None

    projected_symbol_value = _current_symbol_value(symbol, holdings) + (
        order_price * quantity
    )
    return projected_symbol_value / total_equity


def _calculate_loss_amount(
    *,
    session_start_equity: Decimal | None,
    current_equity: Decimal,
) -> Decimal | None:
    if session_start_equity is None:
        return None
    return max(ZERO, session_start_equity - current_equity)


def _calculate_drawdown(
    *,
    peak_equity: Decimal | None,
    current_equity: Decimal,
) -> Decimal | None:
    if peak_equity is None:
        return None
    if current_equity >= peak_equity:
        return ZERO
    return (peak_equity - current_equity) / peak_equity
