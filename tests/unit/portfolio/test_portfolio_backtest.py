from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from autotrade.portfolio import apply_buy_fill
from autotrade.portfolio import apply_sell_fill
from autotrade.portfolio import build_portfolio_snapshot
from autotrade.portfolio import create_backtest_portfolio


def test_snapshot_tracks_cash_average_price_and_open_pnl() -> None:
    state = create_backtest_portfolio(Decimal("1000"))
    state = apply_buy_fill(
        state,
        price=Decimal("100"),
        quantity=2,
        fees=Decimal("1"),
    )

    snapshot = build_portfolio_snapshot(
        state,
        symbol="069500",
        timestamp=datetime.fromisoformat("2026-04-10T15:30:00+09:00"),
        close_price=Decimal("110"),
    )

    assert state.cash == Decimal("799")
    assert state.position_quantity == 2
    assert state.average_price == Decimal("100")
    assert state.position_cost_basis == Decimal("201")
    assert snapshot.position_average_price == Decimal("100")
    assert snapshot.position_market_value == Decimal("220")
    assert snapshot.realized_pnl == Decimal("0")
    assert snapshot.unrealized_pnl == Decimal("19")
    assert snapshot.total_pnl == Decimal("19")
    assert snapshot.total_equity == Decimal("1019")


def test_sell_fill_allocates_cost_basis_and_resets_flat_position() -> None:
    state = create_backtest_portfolio(Decimal("1000"))
    state = apply_buy_fill(
        state,
        price=Decimal("100"),
        quantity=1,
        fees=Decimal("1"),
    )
    state = apply_buy_fill(
        state,
        price=Decimal("120"),
        quantity=1,
        fees=Decimal("1"),
    )

    partially_closed = apply_sell_fill(
        state,
        price=Decimal("130"),
        quantity=1,
        fees=Decimal("1"),
    )
    partial_snapshot = build_portfolio_snapshot(
        partially_closed,
        symbol="069500",
        timestamp=datetime.fromisoformat("2026-04-11T15:30:00+09:00"),
        close_price=Decimal("120"),
    )

    assert partially_closed.cash == Decimal("907")
    assert partially_closed.position_quantity == 1
    assert partially_closed.average_price == Decimal("110")
    assert partially_closed.position_cost_basis == Decimal("111")
    assert partially_closed.realized_pnl == Decimal("18")
    assert partial_snapshot.unrealized_pnl == Decimal("9")
    assert partial_snapshot.total_pnl == Decimal("27")

    fully_closed = apply_sell_fill(
        partially_closed,
        price=Decimal("120"),
        quantity=1,
        fees=Decimal("1"),
    )
    final_snapshot = build_portfolio_snapshot(
        fully_closed,
        symbol="069500",
        timestamp=datetime.fromisoformat("2026-04-12T15:30:00+09:00"),
        close_price=Decimal("120"),
    )

    assert fully_closed.cash == Decimal("1026")
    assert fully_closed.position_quantity == 0
    assert fully_closed.average_price == Decimal("0")
    assert fully_closed.position_cost_basis == Decimal("0")
    assert fully_closed.realized_pnl == Decimal("26")
    assert final_snapshot.unrealized_pnl == Decimal("0")
    assert final_snapshot.total_pnl == Decimal("26")
    assert final_snapshot.total_equity == Decimal("1026")
