from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import logging

from autotrade.broker import PaperBroker
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.config import AppSettings
from autotrade.config import BrokerSettings
from autotrade.data import Bar
from autotrade.data import CsvBarSource
from autotrade.data import CsvBarStore
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.execution import FileExecutionStateStore
from autotrade.report import FileNotifier
from autotrade.runtime import LiveCycleRuntime
from autotrade.runtime import strategy_timeframe_for
from autotrade.strategy import StrategyKind
from autotrade.strategy import create_strategy


def test_live_cycle_runtime_executes_buy_signal_and_emits_alerts(
    tmp_path,
    caplog,
) -> None:
    bar_root = tmp_path / "bars"
    log_dir = tmp_path / "logs"
    store = CsvBarStore(root_dir=bar_root)
    bars = _build_trend_bars("069500", Timeframe.MINUTE_30)
    store.store_bars(bars)

    settings = _settings(log_dir)
    broker = PaperBroker(initial_cash=Decimal("1000000"))
    notifier = FileNotifier(log_dir / "notifications.jsonl")
    state_store = FileExecutionStateStore(log_dir / "execution_state.json")
    runtime = LiveCycleRuntime(
        settings=settings,
        strategy=create_strategy(StrategyKind.THIRTY_MINUTE_TREND),
        timeframe=strategy_timeframe_for(StrategyKind.THIRTY_MINUTE_TREND),
        bar_source=CsvBarSource(bar_root),
        broker_reader=broker,
        broker_trader=broker,
        notifier=notifier,
        state_store=state_store,
        clock=lambda: bars[-1].timestamp,
    )

    caplog.set_level(logging.INFO, logger="autotrade.runtime.live_cycle")
    result = runtime.run()

    assert result.stored_order_snapshots == 1
    assert result.total_orders == 1
    assert result.symbol_results[0].status == "submitted"
    assert result.symbol_results[0].order is not None
    assert result.symbol_results[0].order.status in {
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.FILLED,
        OrderStatus.PARTIALLY_FILLED,
    }
    assert (log_dir / "notifications.jsonl").exists()
    assert (log_dir / "execution_state.json").exists()
    assert "운영 사이클을 시작합니다." in caplog.text
    assert "주문 제출을 진행합니다." in caplog.text
    assert "종목=069500 주기=30m 상태=매수 주문 제출" in result.render_korean_summary()


def test_live_cycle_runtime_records_no_data_without_bars(tmp_path) -> None:
    log_dir = tmp_path / "logs"
    settings = _settings(log_dir)
    broker = PaperBroker(initial_cash=Decimal("1000000"))
    runtime = LiveCycleRuntime(
        settings=settings,
        strategy=create_strategy(StrategyKind.THIRTY_MINUTE_TREND),
        timeframe=strategy_timeframe_for(StrategyKind.THIRTY_MINUTE_TREND),
        bar_source=CsvBarSource(tmp_path / "missing-bars"),
        broker_reader=broker,
        broker_trader=broker,
        notifier=FileNotifier(log_dir / "notifications.jsonl"),
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: datetime(2026, 4, 10, 15, 0, tzinfo=KST),
    )

    result = runtime.run()

    assert result.stored_order_snapshots == 0
    assert result.symbol_results[0].status == "no_data"
    assert result.symbol_results[0].order is None
    assert not (log_dir / "notifications.jsonl").exists()


def test_live_cycle_runtime_filters_out_future_bars(tmp_path) -> None:
    bar_root = tmp_path / "bars"
    log_dir = tmp_path / "logs"
    store = CsvBarStore(root_dir=bar_root)
    store.store_bars(_build_trend_bars("069500", Timeframe.MINUTE_30))
    broker = PaperBroker(initial_cash=Decimal("1000000"))
    runtime = LiveCycleRuntime(
        settings=_settings(log_dir),
        strategy=create_strategy(StrategyKind.THIRTY_MINUTE_TREND),
        timeframe=strategy_timeframe_for(StrategyKind.THIRTY_MINUTE_TREND),
        bar_source=CsvBarSource(bar_root),
        broker_reader=broker,
        broker_trader=broker,
        notifier=FileNotifier(log_dir / "notifications.jsonl"),
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: datetime(2026, 4, 10, 12, 0, tzinfo=KST),
    )

    result = runtime.run()

    assert result.stored_order_snapshots == 0
    assert result.total_orders == 0
    assert result.symbol_results[0].status == "hold"


def test_live_cycle_runtime_executes_sell_signal_for_existing_position(
    tmp_path,
) -> None:
    bar_root = tmp_path / "bars"
    log_dir = tmp_path / "logs"
    bars = _build_downtrend_bars("069500", Timeframe.MINUTE_30)
    store = CsvBarStore(root_dir=bar_root)
    store.store_bars(bars)
    broker = PaperBroker(initial_cash=Decimal("1000000"))
    broker.advance_bar(bars[0])
    broker.submit_order(
        OrderRequest(
            request_id="seed-buy",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=5,
            limit_price=bars[0].close,
            requested_at=bars[0].timestamp,
        )
    )
    runtime = LiveCycleRuntime(
        settings=_settings(log_dir),
        strategy=create_strategy(StrategyKind.THIRTY_MINUTE_TREND),
        timeframe=strategy_timeframe_for(StrategyKind.THIRTY_MINUTE_TREND),
        bar_source=CsvBarSource(bar_root),
        broker_reader=broker,
        broker_trader=broker,
        notifier=FileNotifier(log_dir / "notifications.jsonl"),
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: bars[-1].timestamp,
    )

    result = runtime.run()

    assert result.stored_order_snapshots == 1
    assert result.total_orders == 1
    assert result.symbol_results[0].status == "submitted_sell"
    assert result.symbol_results[0].order is not None
    assert result.symbol_results[0].order.side is OrderSide.SELL


def _build_trend_bars(symbol: str, timeframe: Timeframe) -> tuple[Bar, ...]:
    calendar = KrxRegularSessionCalendar()
    timestamps = []
    current = datetime(2026, 4, 10, 9, 0, tzinfo=KST)
    for _ in range(24):
        timestamps.append(current)
        current = calendar.next_timestamp(current, timeframe)
    bars: list[Bar] = []
    for index, timestamp in enumerate(timestamps, start=1):
        price = Decimal("100") + Decimal(index)
        bars.append(
            Bar(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=timestamp,
                open=price,
                high=price + Decimal("5"),
                low=price - Decimal("1"),
                close=price + Decimal("2"),
                volume=100 + index,
            )
        )
    return tuple(bars)


def _build_downtrend_bars(symbol: str, timeframe: Timeframe) -> tuple[Bar, ...]:
    calendar = KrxRegularSessionCalendar()
    timestamps = []
    current = datetime(2026, 4, 10, 9, 0, tzinfo=KST)
    for _ in range(24):
        timestamps.append(current)
        current = calendar.next_timestamp(current, timeframe)
    bars: list[Bar] = []
    for index, timestamp in enumerate(timestamps, start=1):
        price = Decimal("200") - Decimal(index)
        bars.append(
            Bar(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=timestamp,
                open=price,
                high=price + Decimal("5"),
                low=price - Decimal("1"),
                close=price - Decimal("1"),
                volume=100 + index,
            )
        )
    return tuple(bars)


def _settings(log_dir) -> AppSettings:
    return AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=log_dir,
    )
