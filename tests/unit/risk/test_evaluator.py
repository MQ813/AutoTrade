from __future__ import annotations

from decimal import Decimal

from autotrade.common import Holding
from autotrade.risk import ProposedBuyOrder
from autotrade.risk import RiskAccountSnapshot
from autotrade.risk import RiskSettings
from autotrade.risk import RiskViolationCode
from autotrade.risk import calculate_max_buy_quantity
from autotrade.risk import evaluate_buy_order


def test_evaluate_buy_order_allows_order_within_limits() -> None:
    settings = RiskSettings(
        max_position_weight=Decimal("0.3"),
        max_concurrent_holdings=3,
        max_loss=Decimal("200"),
    )
    snapshot = RiskAccountSnapshot(
        holdings=(
            Holding(
                symbol="069500",
                quantity=1,
                average_price=Decimal("100"),
                current_price=Decimal("110"),
            ),
        ),
        cash_available=Decimal("890"),
        session_start_equity=Decimal("1000"),
    )
    order = ProposedBuyOrder(
        symbol="114800",
        price=Decimal("50"),
        quantity=2,
    )

    result = evaluate_buy_order(settings, snapshot, order)

    assert result.allowed is True
    assert result.approved_quantity == 2
    assert result.current_equity == Decimal("1000")
    assert result.projected_position_weight == Decimal("0.1")
    assert result.loss_amount == Decimal("0")
    assert result.violations == ()


def test_evaluate_buy_order_caps_quantity_when_position_weight_would_be_exceeded() -> (
    None
):
    settings = RiskSettings(max_position_weight=Decimal("0.2"))
    snapshot = RiskAccountSnapshot(
        holdings=(),
        cash_available=Decimal("1000"),
    )
    order = ProposedBuyOrder(
        symbol="114800",
        price=Decimal("50"),
        quantity=6,
    )

    result = evaluate_buy_order(settings, snapshot, order)

    assert result.allowed is False
    assert result.approved_quantity == 4
    assert result.projected_position_weight == Decimal("0.3")
    assert [violation.code for violation in result.violations] == [
        RiskViolationCode.MAX_POSITION_WEIGHT_EXCEEDED,
    ]


def test_evaluate_buy_order_blocks_new_symbol_when_holding_limit_is_reached() -> None:
    settings = RiskSettings(max_concurrent_holdings=2)
    snapshot = RiskAccountSnapshot(
        holdings=(
            Holding(
                symbol="069500",
                quantity=1,
                average_price=Decimal("100"),
                current_price=Decimal("100"),
            ),
            Holding(
                symbol="357870",
                quantity=1,
                average_price=Decimal("100"),
                current_price=Decimal("100"),
            ),
        ),
        cash_available=Decimal("800"),
    )
    order = ProposedBuyOrder(
        symbol="114800",
        price=Decimal("50"),
        quantity=1,
    )

    result = evaluate_buy_order(settings, snapshot, order)

    assert result.allowed is False
    assert result.approved_quantity == 0
    assert [violation.code for violation in result.violations] == [
        RiskViolationCode.MAX_CONCURRENT_HOLDINGS_EXCEEDED,
    ]


def test_evaluate_buy_order_allows_existing_symbol_even_when_holding_limit_is_reached() -> (
    None
):
    settings = RiskSettings(max_concurrent_holdings=1)
    snapshot = RiskAccountSnapshot(
        holdings=(
            Holding(
                symbol="069500",
                quantity=1,
                average_price=Decimal("100"),
                current_price=Decimal("100"),
            ),
        ),
        cash_available=Decimal("900"),
    )
    order = ProposedBuyOrder(
        symbol="069500",
        price=Decimal("50"),
        quantity=1,
    )

    result = evaluate_buy_order(settings, snapshot, order)

    assert result.allowed is True
    assert result.approved_quantity == 1


def test_evaluate_buy_order_blocks_when_loss_limit_is_reached() -> None:
    settings = RiskSettings(max_loss=Decimal("100"))
    snapshot = RiskAccountSnapshot(
        holdings=(),
        cash_available=Decimal("900"),
        session_start_equity=Decimal("1000"),
    )
    order = ProposedBuyOrder(
        symbol="114800",
        price=Decimal("50"),
        quantity=1,
    )

    result = evaluate_buy_order(settings, snapshot, order)

    assert result.allowed is False
    assert result.approved_quantity == 0
    assert result.loss_amount == Decimal("100")
    assert [violation.code for violation in result.violations] == [
        RiskViolationCode.LOSS_LIMIT_REACHED,
    ]


def test_evaluate_buy_order_blocks_when_loss_limit_has_no_baseline_equity() -> None:
    settings = RiskSettings(max_loss=Decimal("100"))
    snapshot = RiskAccountSnapshot(
        holdings=(),
        cash_available=Decimal("1000"),
    )
    order = ProposedBuyOrder(
        symbol="114800",
        price=Decimal("50"),
        quantity=1,
    )

    result = evaluate_buy_order(settings, snapshot, order)

    assert result.allowed is False
    assert result.approved_quantity == 0
    assert [violation.code for violation in result.violations] == [
        RiskViolationCode.MISSING_SESSION_START_EQUITY,
    ]


def test_evaluate_buy_order_blocks_when_trading_is_halted() -> None:
    settings = RiskSettings(trading_halted=True)
    snapshot = RiskAccountSnapshot(
        holdings=(),
        cash_available=Decimal("1000"),
    )
    order = ProposedBuyOrder(
        symbol="114800",
        price=Decimal("50"),
        quantity=1,
    )

    result = evaluate_buy_order(settings, snapshot, order)

    assert result.allowed is False
    assert result.approved_quantity == 0
    assert [violation.code for violation in result.violations] == [
        RiskViolationCode.TRADING_HALTED,
    ]


def test_calculate_max_buy_quantity_uses_existing_symbol_exposure() -> None:
    settings = RiskSettings(max_position_weight=Decimal("0.3"))
    snapshot = RiskAccountSnapshot(
        holdings=(
            Holding(
                symbol="114800",
                quantity=2,
                average_price=Decimal("95"),
                current_price=Decimal("100"),
            ),
        ),
        cash_available=Decimal("800"),
    )

    max_quantity = calculate_max_buy_quantity(
        settings=settings,
        snapshot=snapshot,
        symbol="114800",
        order_price=Decimal("50"),
    )

    assert max_quantity == 2
