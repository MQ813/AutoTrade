from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
import logging

from autotrade.broker import PaperBroker
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import Holding
from autotrade.common import OrderCapacity
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.common import Quote
from autotrade.common import Signal
from autotrade.common import SignalAction
from autotrade.config import AppSettings
from autotrade.config import BrokerSettings
from autotrade.data import Bar
from autotrade.data import CsvBarSource
from autotrade.data import CsvBarStore
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.execution import FileExecutionStateStore
from autotrade.execution import OrderExecutionEngine
from autotrade.report import AlertSeverity
from autotrade.report import FileNotifier
from autotrade.report import NotificationMessage
from autotrade.runtime import LiveCycleRuntime
from autotrade.runtime import strategy_timeframe_for
from autotrade.risk import RiskSettings
from autotrade.risk import RiskViolationCode
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


def test_live_cycle_runtime_recovers_from_corrupted_intraday_risk_state(
    tmp_path,
) -> None:
    bar_root = tmp_path / "bars"
    log_dir = tmp_path / "logs"
    CsvBarStore(root_dir=bar_root).store_bars(
        _build_trend_bars("069500", Timeframe.MINUTE_30)
    )
    (log_dir / "intraday_risk_state.json").parent.mkdir(parents=True, exist_ok=True)
    (log_dir / "intraday_risk_state.json").write_text("{not-json", encoding="utf-8")

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
        clock=lambda: datetime(2026, 4, 10, 15, 0, tzinfo=KST),
    )

    result = runtime.run()

    assert result.symbol_results[0].bars_loaded > 0
    backups = tuple(log_dir.glob("intraday_risk_state.json.corrupt-*"))
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == "{not-json"
    assert '"trading_day": "2026-04-10"' in (
        log_dir / "intraday_risk_state.json"
    ).read_text(encoding="utf-8")


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


def test_live_cycle_runtime_allows_top_up_for_existing_position_with_weight_headroom(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    bar = _bar("069500", "2026-04-10T10:00:00+09:00", close="100")
    broker = ScriptedLiveBroker(
        holdings=(
            Holding(
                symbol="069500",
                quantity=1,
                average_price=Decimal("100"),
                current_price=Decimal("100"),
            ),
        ),
        cash_available=Decimal("1000"),
    )
    runtime = LiveCycleRuntime(
        settings=_settings(
            log_dir,
            risk=RiskSettings(entry_max_position_weight_per_order=Decimal("0.2")),
        ),
        strategy=FixedStrategy(SignalAction.BUY),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (bar,)}),
        broker_reader=broker,
        broker_trader=broker,
        notifier=RecordingNotifier(),
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: bar.timestamp,
    )

    result = runtime.run()

    assert len(broker.submit_requests) == 1
    assert broker.submit_requests[0].quantity == 1
    assert result.symbol_results[0].status == "submitted"
    assert result.symbol_results[0].requested_quantity == 1
    assert result.symbol_results[0].approved_quantity == 1


def test_live_cycle_runtime_caps_buy_quantity_by_entry_order_weight(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    bar = _bar("069500", "2026-04-10T10:00:00+09:00", close="20")
    broker = ScriptedLiveBroker(cash_available=Decimal("1000"))
    runtime = LiveCycleRuntime(
        settings=_settings(
            log_dir,
            risk=RiskSettings(
                max_position_weight=Decimal("1"),
                entry_max_position_weight_per_order=Decimal("0.05"),
            ),
        ),
        strategy=FixedStrategy(SignalAction.BUY),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (bar,)}),
        broker_reader=broker,
        broker_trader=broker,
        notifier=RecordingNotifier(),
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: bar.timestamp,
    )

    result = runtime.run()

    assert len(broker.submit_requests) == 1
    assert broker.submit_requests[0].quantity == 2
    assert result.symbol_results[0].status == "submitted"
    assert result.symbol_results[0].requested_quantity == 2
    assert result.symbol_results[0].approved_quantity == 2


def test_live_cycle_runtime_blocks_top_up_when_existing_position_reaches_weight_limit(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    bar = _bar("069500", "2026-04-10T10:00:00+09:00", close="100")
    broker = ScriptedLiveBroker(
        holdings=(
            Holding(
                symbol="069500",
                quantity=3,
                average_price=Decimal("100"),
                current_price=Decimal("100"),
            ),
        ),
        cash_available=Decimal("1000"),
    )
    notifier = RecordingNotifier()
    runtime = LiveCycleRuntime(
        settings=_settings(
            log_dir,
            risk=RiskSettings(entry_max_position_weight_per_order=Decimal("1")),
        ),
        strategy=FixedStrategy(SignalAction.BUY),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (bar,)}),
        broker_reader=broker,
        broker_trader=broker,
        notifier=notifier,
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: bar.timestamp,
    )

    result = runtime.run()

    assert broker.submit_requests == []
    assert result.symbol_results[0].status == "risk_blocked"
    assert result.symbol_results[0].requested_quantity == 0
    assert result.symbol_results[0].approved_quantity == 0
    assert result.symbol_results[0].risk_check is not None
    assert [
        violation.code for violation in result.symbol_results[0].risk_check.violations
    ] == [RiskViolationCode.MAX_POSITION_WEIGHT_EXCEEDED]
    assert len(notifier.notifications) == 1
    assert notifier.notifications[0].subject == "AutoTrade risk block 069500"
    assert notifier.notifications[0].severity is AlertSeverity.WARNING
    assert "violation=max_position_weight_exceeded" in notifier.notifications[0].body


def test_live_cycle_runtime_syncs_pending_buy_without_duplicate_submit(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    bar = _bar("069500", "2026-04-10T10:00:00+09:00", close="101")
    broker = ScriptedLiveBroker(
        submit_outcomes=(
            _execution_order(
                order_id="order-1",
                symbol="069500",
                side=OrderSide.BUY,
                status=OrderStatus.ACKNOWLEDGED,
                requested_at=bar.timestamp,
                quantity=5,
                limit_price=bar.close,
            ),
        ),
        fill_outcomes={
            "order-1": [
                (
                    _fill(
                        "fill-1",
                        order_id="order-1",
                        symbol="069500",
                        quantity=2,
                        price=bar.close,
                        filled_at=bar.timestamp,
                    ),
                )
            ]
        },
    )
    notifier = RecordingNotifier()
    state_store = FileExecutionStateStore(log_dir / "execution_state.json")
    seed_engine = OrderExecutionEngine(broker, state_store=state_store)
    seed_engine.submit_order(
        OrderRequest(
            request_id="seed-buy",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=5,
            limit_price=bar.close,
            requested_at=bar.timestamp,
        )
    )
    runtime = LiveCycleRuntime(
        settings=_settings(log_dir),
        strategy=FixedStrategy(SignalAction.BUY),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (bar,)}),
        broker_reader=broker,
        broker_trader=broker,
        notifier=notifier,
        state_store=state_store,
        clock=lambda: bar.timestamp,
    )

    result = runtime.run()

    assert len(broker.submit_requests) == 1
    assert broker.fill_requests == ["order-1"]
    assert result.symbol_results[0].status == "buy_pending"
    assert result.symbol_results[0].order is not None
    assert result.symbol_results[0].order.status is OrderStatus.PARTIALLY_FILLED
    assert result.symbol_results[0].order.filled_quantity == 2
    assert result.symbol_results[0].fills == (
        _fill(
            "fill-1",
            order_id="order-1",
            symbol="069500",
            quantity=2,
            price=bar.close,
            filled_at=bar.timestamp,
        ),
    )
    assert len(notifier.notifications) == 1
    assert notifier.notifications[0].subject == "AutoTrade fill 069500 [2@101]"


def test_live_cycle_runtime_publishes_only_incremental_fill_alerts_for_cumulative_sync(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    first_bar = _bar("069500", "2026-04-10T10:00:00+09:00", close="101")
    second_bar = _bar("069500", "2026-04-10T10:30:00+09:00", close="102")
    broker = ScriptedLiveBroker(
        submit_outcomes=(
            _execution_order(
                order_id="order-1",
                symbol="069500",
                side=OrderSide.BUY,
                status=OrderStatus.ACKNOWLEDGED,
                requested_at=first_bar.timestamp,
                quantity=5,
                limit_price=first_bar.close,
            ),
        ),
        fill_outcomes={
            "order-1": [
                (
                    _fill(
                        "order-1:cumulative",
                        order_id="order-1",
                        symbol="069500",
                        quantity=2,
                        price=Decimal("101"),
                        filled_at=first_bar.timestamp,
                    ),
                ),
                (
                    _fill(
                        "order-1:cumulative",
                        order_id="order-1",
                        symbol="069500",
                        quantity=5,
                        price=Decimal("101.6"),
                        filled_at=second_bar.timestamp,
                    ),
                ),
            ]
        },
    )
    notifier = RecordingNotifier()
    state_store = FileExecutionStateStore(log_dir / "execution_state.json")
    seed_engine = OrderExecutionEngine(broker, state_store=state_store)
    seed_engine.submit_order(
        OrderRequest(
            request_id="seed-buy",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=5,
            limit_price=first_bar.close,
            requested_at=first_bar.timestamp,
        )
    )

    first_runtime = LiveCycleRuntime(
        settings=_settings(log_dir),
        strategy=FixedStrategy(SignalAction.HOLD),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (first_bar,)}),
        broker_reader=broker,
        broker_trader=broker,
        notifier=notifier,
        state_store=state_store,
        clock=lambda: first_bar.timestamp,
    )
    second_runtime = LiveCycleRuntime(
        settings=_settings(log_dir),
        strategy=FixedStrategy(SignalAction.HOLD),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (first_bar, second_bar)}),
        broker_reader=broker,
        broker_trader=broker,
        notifier=notifier,
        state_store=state_store,
        clock=lambda: second_bar.timestamp,
    )

    first_result = first_runtime.run()
    second_result = second_runtime.run()

    assert first_result.symbol_results[0].fills == (
        _fill(
            "order-1:cumulative:2",
            order_id="order-1",
            symbol="069500",
            quantity=2,
            price=Decimal("101"),
            filled_at=first_bar.timestamp,
        ),
    )
    assert second_result.symbol_results[0].fills == (
        _fill(
            "order-1:cumulative:5",
            order_id="order-1",
            symbol="069500",
            quantity=3,
            price=Decimal("102"),
            filled_at=second_bar.timestamp,
        ),
    )
    assert [notification.subject for notification in notifier.notifications] == [
        "AutoTrade fill 069500 [2@101]",
        "AutoTrade fill 069500 [3@102]",
    ]


def test_live_cycle_runtime_cancels_open_buy_near_market_close_and_blocks_entry(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    bar = _bar("069500", "2026-04-10T15:00:00+09:00", close="101")
    broker = ScriptedLiveBroker(
        submit_outcomes=(
            _execution_order(
                order_id="order-1",
                symbol="069500",
                side=OrderSide.BUY,
                status=OrderStatus.ACKNOWLEDGED,
                requested_at=bar.timestamp,
                quantity=5,
                limit_price=bar.close,
            ),
        ),
    )
    notifier = RecordingNotifier()
    state_store = FileExecutionStateStore(log_dir / "execution_state.json")
    seed_engine = OrderExecutionEngine(broker, state_store=state_store)
    seed_engine.submit_order(
        OrderRequest(
            request_id="seed-buy",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=5,
            limit_price=bar.close,
            requested_at=bar.timestamp,
        )
    )
    runtime = LiveCycleRuntime(
        settings=_settings(log_dir),
        strategy=FixedStrategy(SignalAction.BUY),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (bar,)}),
        broker_reader=broker,
        broker_trader=broker,
        notifier=notifier,
        state_store=state_store,
        clock=lambda: bar.timestamp,
    )

    result = runtime.run()
    canceled_snapshot = runtime.execution_engine.get_order_snapshot("order-1")

    assert result.symbol_results[0].status == "entry_restricted"
    assert [request.order_id for request in broker.cancel_requests] == ["order-1"]
    assert canceled_snapshot.order.status is OrderStatus.CANCELED
    assert len(notifier.notifications) == 2
    assert notifier.notifications[0].subject == "AutoTrade order 069500 [CANCELED]"
    assert notifier.notifications[1].subject == "AutoTrade risk block 069500"


def test_live_cycle_runtime_applies_submitted_today_order_limit(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    bar = _bar("069500", "2026-04-10T10:00:00+09:00", close="101")
    broker = ScriptedLiveBroker(
        submit_outcomes=(
            _execution_order(
                order_id="order-1",
                symbol="357870",
                side=OrderSide.BUY,
                status=OrderStatus.ACKNOWLEDGED,
                requested_at=bar.timestamp,
                quantity=1,
                limit_price=Decimal("100"),
            ),
        ),
    )
    notifier = RecordingNotifier()
    state_store = FileExecutionStateStore(log_dir / "execution_state.json")
    seed_engine = OrderExecutionEngine(broker, state_store=state_store)
    seed_engine.submit_order(
        OrderRequest(
            request_id="seed-buy",
            symbol="357870",
            side=OrderSide.BUY,
            quantity=1,
            limit_price=Decimal("100"),
            requested_at=bar.timestamp,
        )
    )
    runtime = LiveCycleRuntime(
        settings=_settings(
            log_dir,
            risk=RiskSettings(
                entry_max_position_weight_per_order=Decimal("0.2"),
                max_orders_per_day=1,
            ),
        ),
        strategy=FixedStrategy(SignalAction.BUY),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (bar,)}),
        broker_reader=broker,
        broker_trader=broker,
        notifier=notifier,
        state_store=state_store,
        clock=lambda: bar.timestamp,
    )

    result = runtime.run()

    assert len(broker.submit_requests) == 1
    assert result.symbol_results[0].status == "risk_blocked"
    assert result.symbol_results[0].risk_check is not None
    assert [
        violation.code for violation in result.symbol_results[0].risk_check.violations
    ] == [RiskViolationCode.ORDER_LIMIT_REACHED]


def test_live_cycle_runtime_persists_intraday_loss_and_drawdown_baseline(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    bar = _bar("069500", "2026-04-10T14:00:00+09:00", close="101")
    settings = _settings(
        log_dir,
        risk=RiskSettings(
            entry_max_position_weight_per_order=Decimal("0.2"),
            max_loss=Decimal("10"),
            max_drawdown=Decimal("0.01"),
        ),
    )
    state_store = FileExecutionStateStore(log_dir / "execution_state.json")
    first_broker = ScriptedLiveBroker(
        holdings=(
            Holding(
                symbol="357870",
                quantity=1,
                average_price=Decimal("100"),
                current_price=Decimal("100"),
            ),
        ),
        cash_available=Decimal("900"),
    )
    first_runtime = LiveCycleRuntime(
        settings=settings,
        strategy=FixedStrategy(SignalAction.HOLD),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (bar,)}),
        broker_reader=first_broker,
        broker_trader=first_broker,
        notifier=RecordingNotifier(),
        state_store=state_store,
        clock=lambda: bar.timestamp,
    )

    first_result = first_runtime.run()

    second_broker = ScriptedLiveBroker(
        holdings=(
            Holding(
                symbol="357870",
                quantity=1,
                average_price=Decimal("100"),
                current_price=Decimal("80"),
            ),
        ),
        cash_available=Decimal("900"),
    )
    second_runtime = LiveCycleRuntime(
        settings=settings,
        strategy=FixedStrategy(SignalAction.BUY),
        timeframe=Timeframe.MINUTE_30,
        bar_source=StaticBarSource({"069500": (bar,)}),
        broker_reader=second_broker,
        broker_trader=second_broker,
        notifier=RecordingNotifier(),
        state_store=state_store,
        clock=lambda: bar.timestamp,
    )

    second_result = second_runtime.run()
    risk_check = second_result.symbol_results[0].risk_check

    assert first_result.symbol_results[0].status == "hold"
    assert second_result.symbol_results[0].status == "risk_blocked"
    assert risk_check is not None
    assert risk_check.loss_amount == Decimal("20")
    assert risk_check.drawdown == Decimal("0.02")
    assert {violation.code for violation in risk_check.violations} == {
        RiskViolationCode.LOSS_LIMIT_REACHED,
        RiskViolationCode.DRAWDOWN_LIMIT_REACHED,
    }


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


def _bar(symbol: str, timestamp: str, *, close: str) -> Bar:
    price = Decimal(close)
    return Bar(
        symbol=symbol,
        timeframe=Timeframe.MINUTE_30,
        timestamp=datetime.fromisoformat(timestamp),
        open=price,
        high=price + Decimal("1"),
        low=price - Decimal("1"),
        close=price,
        volume=100,
    )


def _execution_order(
    *,
    order_id: str,
    symbol: str,
    side: OrderSide,
    status: OrderStatus,
    requested_at: datetime,
    quantity: int,
    limit_price: Decimal,
    filled_quantity: int = 0,
) -> ExecutionOrder:
    return ExecutionOrder(
        order_id=order_id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        limit_price=limit_price,
        status=status,
        created_at=requested_at,
        updated_at=requested_at,
        filled_quantity=filled_quantity,
    )


def _fill(
    fill_id: str,
    *,
    order_id: str,
    symbol: str,
    quantity: int,
    price: Decimal,
    filled_at: datetime,
) -> ExecutionFill:
    return ExecutionFill(
        fill_id=fill_id,
        order_id=order_id,
        symbol=symbol,
        quantity=quantity,
        price=price,
        filled_at=filled_at,
    )


def _settings(
    log_dir,
    *,
    target_symbols: tuple[str, ...] = ("069500",),
    risk: RiskSettings | None = None,
) -> AppSettings:
    return AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=target_symbols,
        log_dir=log_dir,
        risk=risk or RiskSettings(),
    )


class FixedStrategy:
    def __init__(self, action: SignalAction) -> None:
        self._action = action

    def generate_signal(self, bars: Sequence[Bar]) -> Signal:
        latest_bar = bars[-1]
        return Signal(
            symbol=latest_bar.symbol,
            action=self._action,
            generated_at=latest_bar.timestamp,
            reason="test-fixed-signal",
        )


class StaticBarSource:
    def __init__(self, bars_by_symbol: dict[str, tuple[Bar, ...]]) -> None:
        self._bars_by_symbol = bars_by_symbol

    def load_bars(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[Bar, ...]:
        bars = tuple(
            bar
            for bar in self._bars_by_symbol.get(symbol, ())
            if bar.timeframe is timeframe
            and (start is None or bar.timestamp >= start)
            and (end is None or bar.timestamp <= end)
        )
        return bars


class RecordingNotifier:
    def __init__(self) -> None:
        self.notifications: list[NotificationMessage] = []

    def send(self, notification: NotificationMessage) -> None:
        self.notifications.append(notification)


class ScriptedLiveBroker:
    def __init__(
        self,
        *,
        holdings: tuple[Holding, ...] = (),
        cash_available: Decimal = Decimal("1000000"),
        submit_outcomes: tuple[ExecutionOrder, ...] = (),
        fill_outcomes: dict[str, list[tuple[ExecutionFill, ...]]] | None = None,
    ) -> None:
        self._holdings = holdings
        self._cash_available = cash_available
        self._submit_outcomes = list(submit_outcomes)
        self._fill_outcomes = fill_outcomes or {}
        self._orders: dict[str, ExecutionOrder] = {}
        self.submit_requests: list[OrderRequest] = []
        self.cancel_requests: list[OrderCancelRequest] = []
        self.fill_requests: list[str] = []

    def get_quote(self, symbol: str) -> Quote:
        return Quote(
            symbol=symbol,
            price=Decimal("100"),
            as_of=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        )

    def get_holdings(self) -> tuple[Holding, ...]:
        return self._holdings

    def get_order_capacity(
        self,
        symbol: str,
        order_price: Decimal,
    ) -> OrderCapacity:
        max_quantity = 0
        if order_price > Decimal("0"):
            max_quantity = int(self._cash_available / order_price)
        return OrderCapacity(
            symbol=symbol,
            order_price=order_price,
            max_orderable_quantity=max_quantity,
            cash_available=self._cash_available,
        )

    def submit_order(self, request: OrderRequest) -> ExecutionOrder:
        self.submit_requests.append(request)
        if self._submit_outcomes:
            order = self._submit_outcomes.pop(0)
        else:
            order = _execution_order(
                order_id=f"order-{len(self.submit_requests)}",
                symbol=request.symbol,
                side=request.side,
                status=OrderStatus.ACKNOWLEDGED,
                requested_at=request.requested_at,
                quantity=request.quantity,
                limit_price=request.limit_price,
            )
        self._orders[order.order_id] = order
        return order

    def amend_order(self, request):  # pragma: no cover - not used in these tests
        raise NotImplementedError

    def cancel_order(self, request: OrderCancelRequest) -> ExecutionOrder:
        self.cancel_requests.append(request)
        current = self._orders[request.order_id]
        canceled = _execution_order(
            order_id=current.order_id,
            symbol=current.symbol,
            side=current.side,
            status=OrderStatus.CANCELED,
            requested_at=current.created_at,
            quantity=current.quantity,
            limit_price=current.limit_price,
            filled_quantity=current.filled_quantity,
        )
        canceled = ExecutionOrder(
            order_id=canceled.order_id,
            symbol=canceled.symbol,
            side=canceled.side,
            quantity=canceled.quantity,
            limit_price=canceled.limit_price,
            status=canceled.status,
            created_at=current.created_at,
            updated_at=request.requested_at,
            filled_quantity=current.filled_quantity,
        )
        self._orders[current.order_id] = canceled
        return canceled

    def get_fills(self, order_id: str) -> tuple[ExecutionFill, ...]:
        self.fill_requests.append(order_id)
        scripted = self._fill_outcomes.get(order_id)
        if not scripted:
            return ()
        return scripted.pop(0)
