from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
import logging
from decimal import Decimal

from autotrade.broker import BrokerReader
from autotrade.broker import BrokerTrader
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import Holding
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.common import Signal
from autotrade.common import SignalAction
from autotrade.common.price_ticks import normalize_krx_symbol_order_price
from autotrade.config import AppSettings
from autotrade.data import Bar
from autotrade.data import KST
from autotrade.data import KRX_SESSION_CLOSE
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
from autotrade.risk import should_cancel_unfilled_orders
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
from autotrade.runtime.intraday_risk_state import FileIntradayRiskStateStore
from autotrade.runtime.intraday_risk_state import IntradayRiskState

logger = logging.getLogger(__name__)
ZERO = Decimal("0")
_OPEN_ORDER_STATUSES = {
    OrderStatus.PENDING,
    OrderStatus.ACKNOWLEDGED,
    OrderStatus.PARTIALLY_FILLED,
    OrderStatus.CANCEL_PENDING,
}


@dataclass(frozen=True, slots=True)
class _PendingOrderFollowUp:
    fills: tuple[ExecutionFill, ...] = ()
    notifications: tuple[NotificationMessage, ...] = ()


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


@dataclass(frozen=True, slots=True)
class LiveCycleSyncSymbolResult:
    symbol: str
    fills: tuple[ExecutionFill, ...] = ()
    notifications: tuple[NotificationMessage, ...] = ()


@dataclass(frozen=True, slots=True)
class LiveCycleSyncResult:
    generated_at: datetime
    symbol_results: tuple[LiveCycleSyncSymbolResult, ...]
    stored_order_snapshots: int

    @property
    def total_fills(self) -> int:
        return sum(len(result.fills) for result in self.symbol_results)

    @property
    def total_notifications(self) -> int:
        return sum(len(result.notifications) for result in self.symbol_results)

    def render_summary(self) -> str:
        lines = [
            f"generated_at={self.generated_at.isoformat()}",
            f"symbols={len(self.symbol_results)}",
            f"fills={self.total_fills}",
            f"notifications={self.total_notifications}",
            f"stored_order_snapshots={self.stored_order_snapshots}",
        ]
        for result in self.symbol_results:
            lines.append(
                "symbol="
                f"{result.symbol} "
                f"fills={len(result.fills)} "
                f"notifications={len(result.notifications)}"
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
    _risk_state_store: FileIntradayRiskStateStore = field(
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        self.execution_engine = OrderExecutionEngine(
            self.broker_trader,
            state_store=self.state_store,
        )
        self._risk_state_store = FileIntradayRiskStateStore(
            self.settings.log_dir / "intraday_risk_state.json"
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

    def sync_open_orders(
        self,
        *,
        timestamp: datetime | None = None,
        symbols: Sequence[str] | None = None,
    ) -> LiveCycleSyncResult:
        generated_at = timestamp or self.clock()
        _require_aware_datetime("generated_at", generated_at)
        resolved_symbols = self._sync_symbols(symbols)
        logger.info(
            "주문/체결 동기화를 시작합니다. 시각=%s 종목수=%d",
            generated_at.isoformat(),
            len(resolved_symbols),
        )
        symbol_results: list[LiveCycleSyncSymbolResult] = []
        for symbol in resolved_symbols:
            follow_up = self._follow_up_open_orders(
                symbol=symbol,
                generated_at=generated_at,
            )
            symbol_results.append(
                LiveCycleSyncSymbolResult(
                    symbol=symbol,
                    fills=follow_up.fills,
                    notifications=follow_up.notifications,
                )
            )
        stored_order_snapshots = len(self.execution_engine.list_order_snapshots())
        logger.info(
            "주문/체결 동기화를 마쳤습니다. 체결=%d 알림=%d 저장된 스냅샷=%d",
            sum(len(result.fills) for result in symbol_results),
            sum(len(result.notifications) for result in symbol_results),
            stored_order_snapshots,
        )
        return LiveCycleSyncResult(
            generated_at=generated_at,
            symbol_results=tuple(symbol_results),
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

    def _sync_symbols(self, symbols: Sequence[str] | None) -> tuple[str, ...]:
        if symbols is not None:
            return tuple(symbols)
        resolved_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in (
            *self.settings.target_symbols,
            *(
                snapshot.order.symbol
                for snapshot in self.execution_engine.list_order_snapshots()
                if snapshot.order.status in _OPEN_ORDER_STATUSES
            ),
        ):
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            resolved_symbols.append(symbol)
        return tuple(resolved_symbols)

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
        follow_up = self._follow_up_open_orders(
            symbol=symbol,
            generated_at=generated_at,
        )
        published_fills = list(follow_up.fills)
        published_notifications = list(follow_up.notifications)
        signal = self.strategy.generate_signal(bars)
        holdings = self.broker_reader.get_holdings()
        existing_snapshots = self.execution_engine.list_order_snapshots()
        market_closing = _is_market_closing_window(generated_at, self.timeframe)
        quote = self.broker_reader.get_quote(symbol)
        order_price = _normalize_order_price_for_signal(
            symbol,
            quote.price,
            signal.action,
        )
        if order_price != quote.price:
            logger.info(
                "호가단위에 맞춰 주문 가격을 보정합니다. symbol=%s action=%s "
                "raw_price=%s normalized_price=%s",
                symbol,
                signal.action.value,
                quote.price,
                order_price,
            )
        capacity = self.broker_reader.get_order_capacity(symbol, order_price)
        risk_snapshot = self._build_risk_account_snapshot(
            generated_at=generated_at,
            holdings=holdings,
            cash_available=capacity.cash_available,
            existing_snapshots=existing_snapshots,
        )
        if signal.action is SignalAction.HOLD:
            logger.info("전략 신호가 HOLD라 주문하지 않습니다. symbol=%s", symbol)
            return LiveCycleSymbolResult(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                signal=signal,
                status="hold",
                fills=tuple(published_fills),
                notifications=tuple(published_notifications),
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
                    fills=tuple(published_fills),
                    notifications=tuple(published_notifications),
                )

            open_buy_order = _find_open_order(
                existing_snapshots,
                symbol=symbol,
                side=OrderSide.BUY,
            )
            if open_buy_order is not None:
                logger.info(
                    "매도 신호 전에 기존 매수 미체결을 취소합니다. symbol=%s order_id=%s",
                    symbol,
                    open_buy_order.order.order_id,
                )
                _, cancel_notifications = self._cancel_open_order(
                    open_buy_order.order.order_id,
                    generated_at=generated_at,
                    reason="signal_reversal_sell",
                )
                published_notifications.extend(cancel_notifications)
                holdings = self.broker_reader.get_holdings()
                existing_snapshots = self.execution_engine.list_order_snapshots()

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
                    fills=tuple(published_fills),
                    notifications=tuple(published_notifications),
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
                status="submitted_sell",
                requested_quantity=holding_quantity,
                approved_quantity=holding_quantity,
                order=synced_snapshot.order,
                fills=tuple([*published_fills, *new_fills]),
                notifications=tuple([*published_notifications, *notifications]),
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
                fills=tuple(published_fills),
                notifications=tuple(published_notifications),
            )

        if market_closing:
            logger.info(
                "장 종료 임박 구간이라 신규 매수를 제한합니다. symbol=%s",
                symbol,
            )
            notification = _build_risk_block_notification(
                symbol=symbol,
                signal=signal,
                created_at=generated_at,
                requested_quantity=0,
                approved_quantity=0,
                reason="market close entry restriction is active",
            )
            self.notifier.send(notification)
            return LiveCycleSymbolResult(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                signal=signal,
                status="entry_restricted",
                notifications=tuple([*published_notifications, notification]),
                fills=tuple(published_fills),
            )

        logger.info(
            "매수 가능 수량을 계산합니다. symbol=%s price=%s cash=%s",
            symbol,
            order_price,
            capacity.cash_available,
        )
        requested_quantity = calculate_max_buy_quantity(
            settings=self.settings.risk,
            snapshot=risk_snapshot,
            symbol=symbol,
            order_price=order_price,
        )
        if requested_quantity <= 0:
            logger.info("수량 계산 결과가 0이라 주문하지 않습니다. symbol=%s", symbol)
            risk_check = evaluate_buy_order(
                self.settings.risk,
                risk_snapshot,
                ProposedBuyOrder(
                    symbol=symbol,
                    price=order_price,
                    quantity=1,
                ),
            )
            notification = _build_risk_block_notification(
                symbol=symbol,
                signal=signal,
                created_at=generated_at,
                requested_quantity=0,
                approved_quantity=0,
                reason="risk sizing produced zero quantity",
                risk_check=risk_check,
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
                risk_check=risk_check,
                notifications=tuple([*published_notifications, notification]),
                fills=tuple(published_fills),
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
        risk_check = evaluate_buy_order(self.settings.risk, risk_snapshot, proposal)
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
                notifications=tuple([*published_notifications, notification]),
                fills=tuple(published_fills),
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
            fills=tuple([*published_fills, *new_fills]),
            notifications=tuple([*published_notifications, *notifications]),
        )

    def _follow_up_open_orders(
        self,
        *,
        symbol: str,
        generated_at: datetime,
    ) -> _PendingOrderFollowUp:
        open_snapshots = tuple(
            snapshot
            for snapshot in self.execution_engine.list_order_snapshots()
            if snapshot.order.symbol == symbol
            and snapshot.order.status in _OPEN_ORDER_STATUSES
        )
        if not open_snapshots:
            return _PendingOrderFollowUp()

        published_fills: list[ExecutionFill] = []
        published_notifications: list[NotificationMessage] = []
        for snapshot in open_snapshots:
            synced_snapshot = self.execution_engine.sync_fills(snapshot.order.order_id)
            new_fills = _new_fills(snapshot, synced_snapshot)
            for fill in new_fills:
                published_notifications.append(
                    publish_fill_alert(
                        self.notifier,
                        fill,
                        created_at=generated_at,
                    )
                )
            published_fills.extend(new_fills)

            if not self._should_cancel_open_orders(generated_at):
                continue
            if _remaining_quantity(synced_snapshot.order) <= 0:
                continue

            logger.info(
                "장 종료 정리로 미체결 주문을 취소합니다. order_id=%s symbol=%s",
                synced_snapshot.order.order_id,
                synced_snapshot.order.symbol,
            )
            _, cancel_notifications = self._cancel_open_order(
                synced_snapshot.order.order_id,
                generated_at=generated_at,
                reason="market_close_cleanup",
            )
            published_notifications.extend(cancel_notifications)

        return _PendingOrderFollowUp(
            fills=tuple(published_fills),
            notifications=tuple(published_notifications),
        )

    def _cancel_open_order(
        self,
        order_id: str,
        *,
        generated_at: datetime,
        reason: str,
    ) -> tuple[OrderExecutionSnapshot, tuple[NotificationMessage, ...]]:
        current_snapshot = self.execution_engine.get_order_snapshot(order_id)
        if current_snapshot.order.status not in _OPEN_ORDER_STATUSES:
            return current_snapshot, ()
        if _remaining_quantity(current_snapshot.order) <= 0:
            return current_snapshot, ()

        canceled_snapshot = self.execution_engine.cancel_order(
            OrderCancelRequest(
                request_id=_build_cancel_request_id(
                    order_id,
                    generated_at,
                    reason=reason,
                ),
                order_id=order_id,
                requested_at=generated_at,
            )
        )
        if canceled_snapshot.order == current_snapshot.order:
            return canceled_snapshot, ()
        notification = publish_order_alert(
            self.notifier,
            canceled_snapshot.order,
            created_at=generated_at,
        )
        return canceled_snapshot, (notification,)

    def _build_risk_account_snapshot(
        self,
        *,
        generated_at: datetime,
        holdings: tuple[Holding, ...],
        cash_available: Decimal,
        existing_snapshots: Sequence[OrderExecutionSnapshot],
    ) -> RiskAccountSnapshot:
        current_equity = _calculate_current_equity(
            holdings=holdings,
            cash_available=cash_available,
            existing_snapshots=existing_snapshots,
        )
        risk_state = self._update_intraday_risk_state(
            generated_at=generated_at,
            current_equity=current_equity,
        )
        return RiskAccountSnapshot(
            holdings=holdings,
            cash_available=cash_available,
            total_equity=current_equity,
            session_start_equity=risk_state.session_start_equity,
            peak_equity=risk_state.peak_equity,
            orders_submitted_today=_count_orders_submitted_today(
                existing_snapshots,
                generated_at,
            ),
            unfilled_order_count=_count_unfilled_orders(existing_snapshots),
            market_closing=_is_market_closing_window(generated_at, self.timeframe),
        )

    def _update_intraday_risk_state(
        self,
        *,
        generated_at: datetime,
        current_equity: Decimal,
    ) -> IntradayRiskState:
        trading_day = generated_at.astimezone(KST).date()
        current_state = self._risk_state_store.load()
        current_positive_equity = _positive_equity_or_none(current_equity)

        if current_state is None or current_state.trading_day != trading_day:
            next_state = IntradayRiskState(
                trading_day=trading_day,
                session_start_equity=current_positive_equity,
                peak_equity=current_positive_equity,
                latest_equity=current_equity,
            )
        else:
            next_state = IntradayRiskState(
                trading_day=trading_day,
                session_start_equity=(
                    current_state.session_start_equity or current_positive_equity
                ),
                peak_equity=_max_optional_decimal(
                    current_state.peak_equity,
                    current_positive_equity,
                ),
                latest_equity=current_equity,
            )

        self._risk_state_store.save(next_state)
        return next_state

    def _should_cancel_open_orders(self, generated_at: datetime) -> bool:
        return should_cancel_unfilled_orders(
            self.settings.risk,
            RiskAccountSnapshot(
                holdings=(),
                cash_available=ZERO,
                unfilled_order_count=max(
                    _count_unfilled_orders(
                        self.execution_engine.list_order_snapshots()
                    ),
                    1,
                ),
                market_closing=_is_market_closing_window(
                    generated_at,
                    self.timeframe,
                ),
            ),
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
            "브로커 주문을 제출합니다. request_id=%s symbol=%s side=%s "
            "quantity=%d limit_price=%s",
            request.request_id,
            request.symbol,
            request.side.value,
            request.quantity,
            request.limit_price,
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

    severity = AlertSeverity.WARNING
    if risk_check is not None and risk_check.should_halt_trading:
        severity = AlertSeverity.ERROR
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


def _normalize_order_price_for_signal(
    symbol: str,
    price: Decimal,
    action: SignalAction,
) -> Decimal:
    if action is SignalAction.HOLD:
        return price
    side = OrderSide.SELL if action is SignalAction.SELL else OrderSide.BUY
    return normalize_krx_symbol_order_price(symbol, price, side)


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
        "entry_restricted": "장마감 신규 진입 제한",
        "risk_blocked": "리스크 차단",
        "submitted": "매수 주문 제출",
    }.get(status, status)


def _new_fills(
    previous_snapshot: OrderExecutionSnapshot,
    current_snapshot: OrderExecutionSnapshot,
) -> tuple[ExecutionFill, ...]:
    known_fill_ids = {fill.fill_id for fill in previous_snapshot.fills}
    return tuple(
        fill for fill in current_snapshot.fills if fill.fill_id not in known_fill_ids
    )


def _remaining_quantity(order: ExecutionOrder) -> int:
    return max(0, order.quantity - order.filled_quantity)


def _is_market_closing_window(
    generated_at: datetime,
    timeframe: Timeframe,
) -> bool:
    local_time = generated_at.astimezone(KST)
    session_close = local_time.replace(
        hour=KRX_SESSION_CLOSE.hour,
        minute=KRX_SESSION_CLOSE.minute,
        second=0,
        microsecond=0,
    )
    if timeframe is Timeframe.DAY:
        return local_time >= session_close
    return local_time + timeframe.interval >= session_close


def _calculate_current_equity(
    *,
    holdings: Sequence[Holding],
    cash_available: Decimal,
    existing_snapshots: Sequence[OrderExecutionSnapshot],
) -> Decimal:
    reserved_cash = sum(
        (
            snapshot.order.limit_price * Decimal(_remaining_quantity(snapshot.order))
            for snapshot in existing_snapshots
            if snapshot.order.side is OrderSide.BUY
            and snapshot.order.status in _OPEN_ORDER_STATUSES
        ),
        start=ZERO,
    )
    holdings_value = sum(
        (
            (holding.current_price or holding.average_price) * Decimal(holding.quantity)
            for holding in holdings
        ),
        start=ZERO,
    )
    return cash_available + reserved_cash + holdings_value


def _count_orders_submitted_today(
    existing_snapshots: Sequence[OrderExecutionSnapshot],
    generated_at: datetime,
) -> int:
    trading_day = generated_at.astimezone(KST).date()
    return sum(
        1
        for snapshot in existing_snapshots
        if snapshot.order.created_at.astimezone(KST).date() == trading_day
    )


def _count_unfilled_orders(
    existing_snapshots: Sequence[OrderExecutionSnapshot],
) -> int:
    return sum(
        1
        for snapshot in existing_snapshots
        if snapshot.order.status in _OPEN_ORDER_STATUSES
        and _remaining_quantity(snapshot.order) > 0
    )


def _positive_equity_or_none(value: Decimal) -> Decimal | None:
    if value <= ZERO:
        return None
    return value


def _max_optional_decimal(
    left: Decimal | None,
    right: Decimal | None,
) -> Decimal | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _build_cancel_request_id(
    order_id: str,
    generated_at: datetime,
    *,
    reason: str,
) -> str:
    return f"live-cycle:cancel:{reason}:{order_id}:{generated_at.isoformat()}"


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
