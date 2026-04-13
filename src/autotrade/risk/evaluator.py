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
    loss_amount = _calculate_loss_amount(
        session_start_equity=snapshot.session_start_equity,
        current_equity=current_equity,
    )

    if settings.trading_halted:
        violations.append(
            RiskViolation(
                code=RiskViolationCode.TRADING_HALTED,
                message="trading is halted by risk settings",
            ),
        )
        approved_quantity = 0

    if settings.max_loss is not None:
        if snapshot.session_start_equity is None:
            violations.append(
                RiskViolation(
                    code=RiskViolationCode.MISSING_SESSION_START_EQUITY,
                    message="session_start_equity is required when max_loss is configured",
                ),
            )
            approved_quantity = 0
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
        max_quantity_by_weight = calculate_max_buy_quantity(
            settings=settings,
            snapshot=snapshot,
            symbol=order.symbol,
            order_price=order.price,
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
            approved_quantity = max_quantity_by_weight

    return RiskCheck(
        allowed=not violations and approved_quantity == order.quantity,
        approved_quantity=approved_quantity,
        current_equity=current_equity,
        projected_position_weight=requested_position_weight,
        violations=tuple(violations),
        loss_amount=loss_amount,
    )


def calculate_max_buy_quantity(
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
