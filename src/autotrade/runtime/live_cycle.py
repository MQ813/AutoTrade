from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
import logging

from autotrade.broker import BrokerReader
from autotrade.broker import BrokerTrader
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.common import Signal
from autotrade.common import SignalAction
from autotrade.config import AppSettings
from autotrade.data import Bar
from autotrade.data import KST
from autotrade.data import Timeframe
from autotrade.data import BarSource
from autotrade.execution import OrderExecutionEngine
from autotrade.execution import FileExecutionStateStore
from autotrade.execution import OrderExecutionSnapshot
from autotrade.risk import ProposedBuyOrder
from autotrade.risk import RiskAccountSnapshot
from autotrade.risk import RiskCheck
from autotrade.risk import calculate_max_buy_quantity
from autotrade.risk import evaluate_buy_order
from autotrade.report import AlertSeverity
from autotrade.report import NotificationMessage
from autotrade.report import Notifier
from autotrade.report import publish_fill_alert
from autotrade.report import publish_order_alert
from autotrade.scheduler import JobContext
from autotrade.scheduler import MarketSessionPhase
from autotrade.scheduler import ScheduledJob
from autotrade.strategy import Strategy
from autotrade.strategy import StrategyKind
from autotrade.strategy import create_strategy

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LiveCycleSymbolResult:
    symbol: str
    timeframe: Timeframe
    bars_loaded: int
    signal: Signal
    status: str
    requested_quantity: int = 0
    approved_quantity: int = 0
    risk_check: RiskCheck | None = None
    order: ExecutionOrder | None = None
    fills: tuple[ExecutionFill, ...] = ()
    notifications: tuple[NotificationMessage, ...] = ()


@dataclass(frozen=True, slots=True)
class LiveCycleResult:
    generated_at: datetime
    symbol_results: tuple[LiveCycleSymbolResult, ...]
    stored_order_snapshots: int

    @property
    def total_orders(self) -> int:
        return sum(1 for result in self.symbol_results if result.order is not None)

    @property
    def total_notifications(self) -> int:
        return sum(len(result.notifications) for result in self.symbol_results)

    def render_summary(self) -> str:
        lines = [
            f"generated_at={self.generated_at.isoformat()}",
            f"symbols={len(self.symbol_results)}",
            f"orders={self.total_orders}",
            f"notifications={self.total_notifications}",
            f"stored_order_snapshots={self.stored_order_snapshots}",
        ]
        for result in self.symbol_results:
            lines.append(
                "symbol="
                f"{result.symbol} "
                f"timeframe={result.timeframe.value} "
                f"status={result.status} "
                f"signal={result.signal.action.value} "
                f"bars_loaded={result.bars_loaded} "
                f"requested_quantity={result.requested_quantity} "
                f"approved_quantity={result.approved_quantity}"
            )
        return "\n".join(lines)

    def render_korean_summary(self) -> str:
        lines = [
            f"생성 시각: {self.generated_at.isoformat()}",
            f"처리 종목 수: {len(self.symbol_results)}",
            f"주문 발생 건수: {self.total_orders}",
            f"알림 발행 건수: {self.total_notifications}",
            f"저장된 주문 스냅샷 수: {self.stored_order_snapshots}",
        ]
        for result in self.symbol_results:
            lines.append(
                "종목="
                f"{result.symbol} "
                f"주기={result.timeframe.value} "
                f"상태={_status_label(result.status)} "
                f"신호={_signal_label(result.signal.action)} "
                f"바 개수={result.bars_loaded} "
                f"요청 수량={result.requested_quantity} "
                f"승인 수량={result.approved_quantity}"
            )
        return "\n".join(lines)


@dataclass(slots=True)
class LiveCycleRuntime:
    settings: AppSettings
    strategy: Strategy
    timeframe: Timeframe
    bar_source: BarSource
    broker_reader: BrokerReader
    broker_trader: BrokerTrader
    notifier: Notifier
    state_store: FileExecutionStateStore
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(KST))
    execution_engine: OrderExecutionEngine = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.execution_engine = OrderExecutionEngine(
            self.broker_trader,
            state_store=self.state_store,
        )

    def run(
        self,
        *,
        timestamp: datetime | None = None,
        symbols: Sequence[str] | None = None,
    ) -> LiveCycleResult:
        generated_at = timestamp or self.clock()
        _require_aware_datetime("generated_at", generated_at)
        resolved_symbols = tuple(symbols or self.settings.target_symbols)
        logger.info(
            "운영 사이클을 시작합니다. 시각=%s 종목수=%d 주기=%s",
            generated_at.isoformat(),
            len(resolved_symbols),
            self.timeframe.value,
        )
        symbol_results = tuple(
            self._run_symbol(symbol, generated_at=generated_at)
            for symbol in resolved_symbols
        )
        stored_order_snapshots = len(self.execution_engine.list_order_snapshots())
        logger.info(
            "운영 사이클을 마쳤습니다. 주문=%d 알림=%d 저장된 스냅샷=%d",
            sum(1 for result in symbol_results if result.order is not None),
            sum(len(result.notifications) for result in symbol_results),
            stored_order_snapshots,
        )
        return LiveCycleResult(
            generated_at=generated_at,
            symbol_results=symbol_results,
            stored_order_snapshots=stored_order_snapshots,
        )

    def build_job(
        self,
        *,
        name: str = "live_cycle",
    ) -> ScheduledJob:
        phase = _strategy_phase(self.timeframe)

        def handler(context: JobContext) -> str:
            result = self.run(timestamp=context.scheduled_at)
            return result.render_summary()

        return ScheduledJob(name=name, phase=phase, handler=handler)

    def _run_symbol(
        self,
        symbol: str,
        *,
        generated_at: datetime,
    ) -> LiveCycleSymbolResult:
        logger.info("종목 처리를 시작합니다. symbol=%s", symbol)
        # 아직 도래하지 않은 미래 바가 전략 입력에 섞이지 않도록 현재 시각까지만 읽는다.
        bars = self.bar_source.load_bars(
            symbol,
            self.timeframe,
            end=generated_at,
        )
        if not bars:
            logger.info("바 데이터가 없어 이번 사이클은 건너뜁니다. symbol=%s", symbol)
            signal = Signal(
                symbol=symbol,
                action=SignalAction.HOLD,
                generated_at=generated_at,
                reason="no bars available",
            )
            return LiveCycleSymbolResult(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=0,
                signal=signal,
                status="no_data",
            )

        _advance_market_if_supported(self.broker_trader, bars)

        logger.info("전략 신호를 계산합니다. symbol=%s bars=%d", symbol, len(bars))
        signal = self.strategy.generate_signal(bars)
        holdings = self.broker_reader.get_holdings()
        existing_snapshots = self.execution_engine.list_order_snapshots()
        if signal.action is SignalAction.HOLD:
            logger.info("전략 신호가 HOLD라 주문하지 않습니다. symbol=%s", symbol)
            return LiveCycleSymbolResult(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                signal=signal,
                status="hold",
            )
        if signal.action is SignalAction.SELL:
            open_sell_order = _find_open_order(
                existing_snapshots,
                symbol=symbol,
                side=OrderSide.SELL,
            )
            if open_sell_order is not None:
                logger.info(
                    "이미 진행 중인 매도 주문이 있어 새 주문을 건너뜁니다. symbol=%s order_id=%s",
                    symbol,
                    open_sell_order.order.order_id,
                )
                return LiveCycleSymbolResult(
                    symbol=symbol,
                    timeframe=self.timeframe,
                    bars_loaded=len(bars),
                    signal=signal,
                    status="sell_pending",
                    order=open_sell_order.order,
                )

            holding_quantity = next(
                (
                    holding.quantity
                    for holding in holdings
                    if holding.symbol == symbol and holding.quantity > 0
                ),
                0,
            )
            if holding_quantity <= 0:
                logger.info(
                    "매도 신호가 나왔지만 보유 수량이 없습니다. symbol=%s", symbol
                )
                return LiveCycleSymbolResult(
                    symbol=symbol,
                    timeframe=self.timeframe,
                    bars_loaded=len(bars),
                    signal=signal,
                    status="sell_skipped",
                )

            logger.info(
                "보유 수량 기준으로 매도 주문을 준비합니다. symbol=%s quantity=%d",
                symbol,
                holding_quantity,
            )
            synced_snapshot, new_fills, notifications = self._submit_and_sync(
                OrderRequest(
                    request_id=_build_request_id(
                        symbol,
                        signal.generated_at,
                        side=OrderSide.SELL,
                    ),
                    symbol=symbol,
                    side=OrderSide.SELL,
                    quantity=holding_quantity,
                    limit_price=bars[-1].close,
                    requested_at=generated_at,
                ),
                generated_at=generated_at,
                existing_snapshots=existing_snapshots,
            )
            return LiveCycleSymbolResult(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                signal=signal,
                status="submitted_sell",
                requested_quantity=holding_quantity,
                approved_quantity=holding_quantity,
                order=synced_snapshot.order,
                fills=new_fills,
                notifications=notifications,
            )

        latest_bar = bars[-1]
        order_price = latest_bar.close
        if any(
            holding.symbol == symbol and holding.quantity > 0 for holding in holdings
        ):
            logger.info(
                "이미 보유 중인 종목이라 신규 매수를 건너뜁니다. symbol=%s", symbol
            )
            return LiveCycleSymbolResult(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                signal=signal,
                status="already_held",
            )

        open_buy_order = _find_open_order(
            existing_snapshots,
            symbol=symbol,
            side=OrderSide.BUY,
        )
        if open_buy_order is not None:
            logger.info(
                "이미 진행 중인 매수 주문이 있어 새 주문을 건너뜁니다. symbol=%s order_id=%s",
                symbol,
                open_buy_order.order.order_id,
            )
            return LiveCycleSymbolResult(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                signal=signal,
                status="buy_pending",
                order=open_buy_order.order,
            )

        capacity = self.broker_reader.get_order_capacity(symbol, order_price)
        snapshot = RiskAccountSnapshot(
            holdings=holdings,
            cash_available=capacity.cash_available,
            orders_submitted_today=0,
        )
        logger.info(
            "매수 가능 수량을 계산합니다. symbol=%s price=%s cash=%s",
            symbol,
            order_price,
            capacity.cash_available,
        )
        requested_quantity = calculate_max_buy_quantity(
            settings=self.settings.risk,
            snapshot=snapshot,
            symbol=symbol,
            order_price=order_price,
        )
        if requested_quantity <= 0:
            logger.info("수량 계산 결과가 0이라 주문하지 않습니다. symbol=%s", symbol)
            notification = _build_risk_block_notification(
                symbol=symbol,
                signal=signal,
                created_at=generated_at,
                requested_quantity=0,
                approved_quantity=0,
                reason="risk sizing produced zero quantity",
            )
            self.notifier.send(notification)
            return LiveCycleSymbolResult(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                signal=signal,
                status="risk_blocked",
                requested_quantity=0,
                approved_quantity=0,
                notifications=(notification,),
            )

        proposal = ProposedBuyOrder(
            symbol=symbol,
            price=order_price,
            quantity=requested_quantity,
        )
        logger.info(
            "리스크 한도를 검증합니다. symbol=%s requested_quantity=%d",
            symbol,
            requested_quantity,
        )
        risk_check = evaluate_buy_order(self.settings.risk, snapshot, proposal)
        approved_quantity = risk_check.approved_quantity
        if approved_quantity <= 0:
            logger.info("리스크 검증에서 주문이 차단되었습니다. symbol=%s", symbol)
            notification = _build_risk_block_notification(
                symbol=symbol,
                signal=signal,
                created_at=generated_at,
                requested_quantity=requested_quantity,
                approved_quantity=0,
                reason="risk check rejected the order",
                risk_check=risk_check,
            )
            self.notifier.send(notification)
            return LiveCycleSymbolResult(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                signal=signal,
                status="risk_blocked",
                requested_quantity=requested_quantity,
                approved_quantity=0,
                risk_check=risk_check,
                notifications=(notification,),
            )

        logger.info(
            "주문 제출을 진행합니다. symbol=%s approved_quantity=%d",
            symbol,
            approved_quantity,
        )
        synced_snapshot, new_fills, notifications = self._submit_and_sync(
            OrderRequest(
                request_id=_build_request_id(
                    symbol,
                    signal.generated_at,
                    side=OrderSide.BUY,
                ),
                symbol=symbol,
                side=OrderSide.BUY,
                quantity=approved_quantity,
                limit_price=order_price,
                requested_at=generated_at,
            ),
            generated_at=generated_at,
            existing_snapshots=existing_snapshots,
        )

        return LiveCycleSymbolResult(
            symbol=symbol,
            timeframe=self.timeframe,
            bars_loaded=len(bars),
            signal=signal,
            status="submitted",
            requested_quantity=requested_quantity,
            approved_quantity=approved_quantity,
            risk_check=risk_check,
            order=synced_snapshot.order,
            fills=new_fills,
            notifications=notifications,
        )

    def _submit_and_sync(
        self,
        request: OrderRequest,
        *,
        generated_at: datetime,
        existing_snapshots: Sequence[OrderExecutionSnapshot],
    ) -> tuple[
        OrderExecutionSnapshot,
        tuple[ExecutionFill, ...],
        tuple[NotificationMessage, ...],
    ]:
        existing_order_ids = {
            snapshot.order.order_id for snapshot in existing_snapshots
        }
        logger.info(
            "브로커 주문을 제출합니다. symbol=%s side=%s quantity=%d",
            request.symbol,
            request.side.value,
            request.quantity,
        )
        order_snapshot = self.execution_engine.submit_order(request)
        synced_snapshot = self.execution_engine.sync_fills(
            order_snapshot.order.order_id
        )
        published_notifications: list[NotificationMessage] = []
        if order_snapshot.order.order_id not in existing_order_ids:
            published_notifications.append(
                publish_order_alert(
                    self.notifier,
                    order_snapshot.order,
                    created_at=generated_at,
                )
            )
        known_fill_ids = {fill.fill_id for fill in order_snapshot.fills}
        new_fills = tuple(
            fill for fill in synced_snapshot.fills if fill.fill_id not in known_fill_ids
        )
        for fill in new_fills:
            published_notifications.append(
                publish_fill_alert(
                    self.notifier,
                    fill,
                    created_at=generated_at,
                )
            )
        logger.info(
            "주문 제출을 마쳤습니다. order_id=%s status=%s 새 체결=%d",
            synced_snapshot.order.order_id,
            synced_snapshot.order.status.value,
            len(new_fills),
        )
        return synced_snapshot, new_fills, tuple(published_notifications)


def create_live_cycle_runtime(
    settings: AppSettings,
    *,
    strategy_kind: StrategyKind | str,
    bar_source: BarSource,
    broker_reader: BrokerReader,
    broker_trader: BrokerTrader,
    notifier: Notifier,
    state_store: FileExecutionStateStore,
    clock: Callable[[], datetime] | None = None,
) -> LiveCycleRuntime:
    resolved_strategy_kind = _normalize_strategy_kind(strategy_kind)
    return LiveCycleRuntime(
        settings=settings,
        strategy=create_strategy(resolved_strategy_kind),
        timeframe=strategy_timeframe_for(resolved_strategy_kind),
        bar_source=bar_source,
        broker_reader=broker_reader,
        broker_trader=broker_trader,
        notifier=notifier,
        state_store=state_store,
        clock=clock or (lambda: datetime.now(KST)),
    )


def run_live_cycle(
    runtime: LiveCycleRuntime,
    *,
    timestamp: datetime | None = None,
    symbols: Sequence[str] | None = None,
) -> LiveCycleResult:
    return runtime.run(timestamp=timestamp, symbols=symbols)


def build_live_cycle_job(
    runtime: LiveCycleRuntime,
    *,
    name: str = "live_cycle",
) -> ScheduledJob:
    return runtime.build_job(name=name)


def strategy_timeframe_for(kind: StrategyKind | str) -> Timeframe:
    resolved = _normalize_strategy_kind(kind)
    if resolved is StrategyKind.DAILY_TREND_FOLLOWING:
        return Timeframe.DAY
    if resolved is StrategyKind.THIRTY_MINUTE_TREND:
        return Timeframe.MINUTE_30
    raise ValueError(f"unsupported strategy kind: {kind!r}")


def _strategy_phase(timeframe: Timeframe) -> MarketSessionPhase:
    if timeframe is Timeframe.DAY:
        return MarketSessionPhase.MARKET_CLOSE
    return MarketSessionPhase.INTRADAY


def _normalize_strategy_kind(kind: StrategyKind | str) -> StrategyKind:
    if isinstance(kind, StrategyKind):
        return kind
    return StrategyKind(kind)


def _build_request_id(
    symbol: str,
    generated_at: datetime,
    *,
    side: OrderSide,
) -> str:
    return f"live-cycle:{side.value}:{symbol}:{generated_at.isoformat()}"


def _build_risk_block_notification(
    *,
    symbol: str,
    signal: Signal,
    created_at: datetime,
    requested_quantity: int,
    approved_quantity: int,
    reason: str,
    risk_check: RiskCheck | None = None,
) -> NotificationMessage:
    lines = [
        f"symbol={symbol}",
        f"action={signal.action.value}",
        f"signal_at={signal.generated_at.isoformat()}",
        f"requested_quantity={requested_quantity}",
        f"approved_quantity={approved_quantity}",
        f"reason={reason}",
    ]
    if signal.reason is not None:
        lines.append(f"signal_reason={signal.reason}")
    if risk_check is not None:
        lines.append(f"allowed={risk_check.allowed}")
        lines.append(f"violations={len(risk_check.violations)}")
        for violation in risk_check.violations:
            lines.append(
                f"violation={violation.code.value} message={violation.message}"
            )

    severity = AlertSeverity.ERROR if approved_quantity <= 0 else AlertSeverity.WARNING
    return NotificationMessage(
        created_at=created_at,
        severity=severity,
        subject=f"AutoTrade risk block {symbol}",
        body="\n".join(lines),
    )


def _advance_market_if_supported(
    trader: BrokerTrader,
    bars: Sequence[Bar],
) -> None:
    advance_bar = getattr(trader, "advance_bar", None)
    if not callable(advance_bar):
        return
    for bar in bars:
        advance_bar(bar)


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _signal_label(action: SignalAction) -> str:
    if action is SignalAction.BUY:
        return "매수"
    if action is SignalAction.SELL:
        return "매도"
    return "관망"


def _status_label(status: str) -> str:
    return {
        "no_data": "바 데이터 없음",
        "hold": "관망",
        "sell_pending": "기존 매도 주문 대기",
        "sell_skipped": "보유 수량 없음",
        "submitted_sell": "매도 주문 제출",
        "already_held": "기보유 종목",
        "buy_pending": "기존 매수 주문 대기",
        "risk_blocked": "리스크 차단",
        "submitted": "매수 주문 제출",
    }.get(status, status)


def _find_open_order(
    snapshots: Sequence[OrderExecutionSnapshot],
    *,
    symbol: str,
    side: OrderSide,
) -> OrderExecutionSnapshot | None:
    for snapshot in reversed(tuple(snapshots)):
        order = snapshot.order
        if order.symbol != symbol or order.side is not side:
            continue
        if order.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
        }:
            continue
        return snapshot
    return None
