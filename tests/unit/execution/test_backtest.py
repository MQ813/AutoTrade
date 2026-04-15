from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

import pytest

from autotrade.common import Signal
from autotrade.common import SignalAction
from autotrade.data import Bar
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.execution import BacktestConfig
from autotrade.execution import BacktestCostModel
from autotrade.execution import BacktestEngine
from autotrade.report import build_backtest_report
from autotrade.report import render_backtest_report


class ScriptedStrategy:
    def __init__(self, actions: Sequence[SignalAction]) -> None:
        self._actions = tuple(actions)

    def generate_signal(self, bars: Sequence[Bar]) -> Signal:
        index = len(bars) - 1
        action = self._actions[index]
        bar = bars[-1]
        return Signal(
            symbol=bar.symbol,
            action=action,
            generated_at=bar.timestamp,
            reason=f"index={index}",
        )


def test_backtest_applies_costs_and_slippage_to_trade_and_equity() -> None:
    engine = BacktestEngine()
    strategy = ScriptedStrategy(
        [SignalAction.BUY, SignalAction.HOLD, SignalAction.SELL]
    )
    bars = _make_bars(
        symbol="069500",
        timeframe=Timeframe.DAY,
        start="2026-04-10T15:30:00+09:00",
        closes=[100, 100, 110],
    )
    config = BacktestConfig(
        initial_cash=Decimal("1000"),
        cost_model=BacktestCostModel(
            commission_rate=Decimal("0.01"),
            tax_rate=Decimal("0.005"),
            slippage_rate=Decimal("0.02"),
        ),
        in_sample_ratio=None,
    )

    result = engine.run(strategy, bars, config)

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.quantity == 9
    assert trade.entry_price == Decimal("102")
    assert trade.exit_price == Decimal("107.8")
    assert trade.entry_fees == Decimal("9.18")
    assert trade.exit_fees == Decimal("14.553")
    assert trade.net_pnl == Decimal("28.467")
    assert result.snapshots[0].position_average_price == Decimal("102")
    assert result.snapshots[0].realized_pnl == Decimal("0")
    assert result.snapshots[0].unrealized_pnl == Decimal("-27.18")
    assert result.snapshots[0].total_pnl == Decimal("-27.18")
    assert result.snapshots[0].total_equity == Decimal("972.82")
    assert result.snapshots[-1].position_average_price == Decimal("0")
    assert result.snapshots[-1].realized_pnl == Decimal("28.467")
    assert result.snapshots[-1].unrealized_pnl == Decimal("0")
    assert result.snapshots[-1].total_pnl == Decimal("28.467")
    assert result.snapshots[-1].total_equity == Decimal("1028.467")


def test_backtest_report_separates_in_and_out_of_sample_results() -> None:
    engine = BacktestEngine()
    strategy = ScriptedStrategy(
        [
            SignalAction.BUY,
            SignalAction.HOLD,
            SignalAction.SELL,
            SignalAction.BUY,
            SignalAction.HOLD,
            SignalAction.SELL,
        ]
    )
    bars = _make_bars(
        symbol="069500",
        timeframe=Timeframe.DAY,
        start="2026-04-10T15:30:00+09:00",
        closes=[100, 110, 120, 130, 140, 150],
    )
    result = engine.run(
        strategy,
        bars,
        BacktestConfig(
            initial_cash=Decimal("1000"),
            cost_model=BacktestCostModel(),
            in_sample_ratio=Decimal("0.5"),
        ),
    )

    report = build_backtest_report(result)

    assert report.split_timestamp == bars[3].timestamp.isoformat()
    assert report.in_sample is not None
    assert report.out_of_sample is not None
    assert report.recent is not None
    assert report.combined.trade_count == 2
    assert report.in_sample.trade_count == 1
    assert report.in_sample.final_equity == Decimal("1200")
    assert report.out_of_sample.trade_count == 1
    assert report.out_of_sample.starting_equity == Decimal("1200")
    assert report.out_of_sample.final_equity == Decimal("1380")
    assert report.out_of_sample.total_return == Decimal("0.15")
    assert report.recent_start_timestamp == bars[4].timestamp.isoformat()
    assert report.recent.final_equity == Decimal("1380")
    assert report.recent.total_return == Decimal("0.15")
    assert report.overfit_check.status == "pass"
    assert report.overfit_check.reasons == ()

    rendered = render_backtest_report(report)
    assert "section=in_sample" in rendered
    assert "section=out_of_sample" in rendered
    assert "section=recent" in rendered
    assert f"split_timestamp={bars[3].timestamp.isoformat()}" in rendered
    assert f"recent_start_timestamp={bars[4].timestamp.isoformat()}" in rendered
    assert "overfit_check_status=pass" in rendered


def test_backtest_out_of_sample_uses_split_equity_for_carry_positions() -> None:
    engine = BacktestEngine()
    strategy = ScriptedStrategy(
        [
            SignalAction.BUY,
            SignalAction.HOLD,
            SignalAction.HOLD,
            SignalAction.SELL,
        ]
    )
    bars = _make_bars(
        symbol="069500",
        timeframe=Timeframe.DAY,
        start="2026-04-10T15:30:00+09:00",
        closes=[100, 110, 120, 130],
    )
    result = engine.run(
        strategy,
        bars,
        BacktestConfig(
            initial_cash=Decimal("1000"),
            cost_model=BacktestCostModel(),
            in_sample_ratio=Decimal("0.5"),
        ),
    )

    report = build_backtest_report(result)

    assert report.in_sample is not None
    assert report.out_of_sample is not None
    assert report.in_sample.trade_count == 0
    assert report.in_sample.final_equity == Decimal("1100")
    assert report.out_of_sample.starting_equity == Decimal("1100")
    assert report.out_of_sample.trade_count == 1
    assert report.out_of_sample.final_equity == Decimal("1300")
    assert report.out_of_sample.total_return == Decimal(
        "0.1818181818181818181818181818"
    )


def test_backtest_config_rejects_invalid_in_sample_ratio() -> None:
    with pytest.raises(ValueError, match="in_sample_ratio"):
        BacktestConfig(
            initial_cash=Decimal("1000"),
            cost_model=BacktestCostModel(),
            in_sample_ratio=Decimal("1"),
        )


def test_backtest_report_flags_overfit_risk_when_out_of_sample_reverses() -> None:
    engine = BacktestEngine()
    strategy = ScriptedStrategy(
        [
            SignalAction.BUY,
            SignalAction.HOLD,
            SignalAction.SELL,
            SignalAction.BUY,
            SignalAction.HOLD,
            SignalAction.SELL,
        ]
    )
    bars = _make_bars(
        symbol="069500",
        timeframe=Timeframe.DAY,
        start="2026-04-10T15:30:00+09:00",
        closes=[100, 110, 120, 130, 120, 110],
    )

    report = build_backtest_report(
        engine.run(
            strategy,
            bars,
            BacktestConfig(
                initial_cash=Decimal("1000"),
                cost_model=BacktestCostModel(),
                in_sample_ratio=Decimal("0.5"),
            ),
        )
    )

    assert report.in_sample is not None
    assert report.out_of_sample is not None
    assert report.recent is not None
    assert report.out_of_sample.total_return == Decimal("-0.15")
    assert report.recent.total_return == Decimal("-0.15")
    assert report.overfit_check.status == "warning"
    assert report.overfit_check.reasons == (
        "out_of_sample_return_reversal",
        "recent_period_negative_return",
    )

    rendered = render_backtest_report(report)
    assert "overfit_check_status=warning" in rendered
    assert "overfit_check_reason=out_of_sample_return_reversal" in rendered
    assert "overfit_check_reason=recent_period_negative_return" in rendered


def _make_bars(
    symbol: str,
    timeframe: Timeframe,
    start: str,
    closes: list[int],
) -> tuple[Bar, ...]:
    calendar = KrxRegularSessionCalendar()
    timestamp = datetime.fromisoformat(start)
    bars: list[Bar] = []
    for close in closes:
        price = Decimal(str(close))
        bars.append(
            Bar(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=timestamp,
                open=price,
                high=price + Decimal("1"),
                low=price - Decimal("1"),
                close=price,
                volume=len(bars) + 1,
            )
        )
        timestamp = calendar.next_timestamp(timestamp, timeframe)
    return tuple(bars)
