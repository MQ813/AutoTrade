from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from decimal import Decimal
import logging
from pathlib import Path
import re
from typing import Protocol

from autotrade.broker import BrokerReader
from autotrade.broker.smoke import SmokeReport
from autotrade.broker.smoke import run_read_only_smoke
from autotrade.broker.smoke import write_smoke_report
from autotrade.common import Holding
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.common import Signal
from autotrade.common import SignalAction
from autotrade.config import AppSettings
from autotrade.data import Bar
from autotrade.data import CsvBarSource
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.execution import FileExecutionStateStore
from autotrade.execution import OrderExecutionSnapshot
from autotrade.report import AlertSeverity
from autotrade.report import DailyInspectionItem
from autotrade.report import InspectionStatus
from autotrade.report import InspectionWindow
from autotrade.report import NotificationMessage
from autotrade.report import Notifier
from autotrade.report import build_daily_inspection_report
from autotrade.report import write_daily_inspection_report
from autotrade.risk import ProposedBuyOrder
from autotrade.risk import RiskAccountSnapshot
from autotrade.risk import evaluate_buy_order
from autotrade.risk import calculate_max_buy_quantity
from autotrade.scheduler import JobContext
from autotrade.scheduler import MarketSessionPhase
from autotrade.scheduler import ScheduledJob
from autotrade.strategy import Strategy
from autotrade.strategy import create_strategy

logger = logging.getLogger(__name__)

ZERO = Decimal("0")
_OPERATIONS_LOG_PATTERN = re.compile(r"^operations_(?P<day>\d{8})_\d{6}_\d+\.log$")
_OPEN_ORDER_STATUSES = {
    OrderStatus.PENDING,
    OrderStatus.ACKNOWLEDGED,
    OrderStatus.PARTIALLY_FILLED,
    OrderStatus.CANCEL_PENDING,
}


@dataclass(frozen=True, slots=True)
class PreviousDayErrorSummary:
    trading_day: date
    log_files: int
    error_entries: int
    sources: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PreMarketDataStatus:
    symbol: str
    timeframe: Timeframe
    bars_before: int
    bars_after: int
    latest_bar_at: datetime | None
    refreshed: bool
    error: str | None = None

    def __post_init__(self) -> None:
        if self.bars_before < 0:
            raise ValueError("bars_before must be non-negative")
        if self.bars_after < 0:
            raise ValueError("bars_after must be non-negative")
        if self.latest_bar_at is not None:
            _require_aware_datetime("latest_bar_at", self.latest_bar_at)
        if self.error is not None and not self.error.strip():
            raise ValueError("error must not be blank when provided")

    @property
    def ready(self) -> bool:
        return (
            self.error is None
            and self.bars_after > 0
            and self.latest_bar_at is not None
        )

    def render_summary(self) -> str:
        fragments = [
            f"symbol={self.symbol}",
            f"timeframe={self.timeframe.value}",
            f"bars_before={self.bars_before}",
            f"bars_after={self.bars_after}",
            f"refreshed={self.refreshed}",
        ]
        if self.latest_bar_at is not None:
            fragments.append(f"latest_bar_at={self.latest_bar_at.isoformat()}")
        if self.error is not None:
            fragments.append(f"error={self.error}")
        return ":".join(fragments)


@dataclass(frozen=True, slots=True)
class StrategyPreview:
    symbol: str
    timeframe: Timeframe
    bars_loaded: int
    preview_at: datetime | None
    reference_price: Decimal | None
    signal: Signal | None
    status: str
    requested_quantity: int = 0
    approved_quantity: int = 0
    holding_quantity: int = 0
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.preview_at is not None:
            _require_aware_datetime("preview_at", self.preview_at)
        if self.reference_price is not None and self.reference_price < ZERO:
            raise ValueError("reference_price must be non-negative")
        if self.requested_quantity < 0:
            raise ValueError("requested_quantity must be non-negative")
        if self.approved_quantity < 0:
            raise ValueError("approved_quantity must be non-negative")
        if self.holding_quantity < 0:
            raise ValueError("holding_quantity must be non-negative")
        if self.reason is not None and not self.reason.strip():
            raise ValueError("reason must not be blank when provided")

    @property
    def ready(self) -> bool:
        return self.status != "preview_failed"

    def render_summary(self) -> str:
        fragments = [
            f"symbol={self.symbol}",
            f"timeframe={self.timeframe.value}",
            f"status={self.status}",
            f"bars_loaded={self.bars_loaded}",
        ]
        if self.signal is not None:
            fragments.append(f"signal={self.signal.action.value}")
        if self.preview_at is not None:
            fragments.append(f"preview_at={self.preview_at.isoformat()}")
        if self.reference_price is not None:
            fragments.append(f"reference_price={self.reference_price}")
        fragments.extend(
            (
                f"requested_quantity={self.requested_quantity}",
                f"approved_quantity={self.approved_quantity}",
                f"holding_quantity={self.holding_quantity}",
            )
        )
        if self.reason is not None:
            fragments.append(f"reason={self.reason}")
        return ":".join(fragments)

    def render_notification_line(self) -> str:
        action = "UNKNOWN" if self.signal is None else self.signal.action.value
        fragments = [
            f"{self.symbol}",
            f"status={self.status}",
            f"signal={action}",
            f"bars={self.bars_loaded}",
        ]
        if self.reference_price is not None:
            fragments.append(f"price={self.reference_price}")
        if self.approved_quantity > 0:
            fragments.append(f"approved={self.approved_quantity}")
        elif self.requested_quantity > 0:
            fragments.append(f"requested={self.requested_quantity}")
        if self.holding_quantity > 0:
            fragments.append(f"holding={self.holding_quantity}")
        detail = self.reason or (
            self.signal.reason if self.signal is not None else None
        )
        if detail is not None:
            fragments.append(f"detail={detail}")
        return " ".join(fragments)


@dataclass(frozen=True, slots=True)
class MarketOpenPreparationResult:
    generated_at: datetime
    trading_day: date
    strategy_kind: str
    timeframe: Timeframe
    target_symbols: tuple[str, ...]
    smoke_success: bool
    previous_day_errors: PreviousDayErrorSummary
    trading_halted: bool
    emergency_stop: bool
    data_statuses: tuple[PreMarketDataStatus, ...]
    strategy_previews: tuple[StrategyPreview, ...]
    smoke_report_path: Path
    inspection_report_path: Path

    def __post_init__(self) -> None:
        _require_aware_datetime("generated_at", self.generated_at)

    @property
    def success(self) -> bool:
        return not self.failure_reasons

    @property
    def attention_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if self.previous_day_errors.error_entries > 0:
            reasons.append("previous_day_errors_detected")
        return tuple(reasons)

    @property
    def failure_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if not self.smoke_success:
            reasons.append("broker_smoke_failed")
        if self.trading_halted:
            reasons.append("trading_halted")
        if self.emergency_stop:
            reasons.append("emergency_stop")
        if any(not status.ready for status in self.data_statuses):
            reasons.append("strategy_data_unavailable")
        if any(not preview.ready for preview in self.strategy_previews):
            reasons.append("strategy_preview_failed")
        return tuple(reasons)

    @property
    def status(self) -> str:
        if self.failure_reasons:
            return "failure"
        if self.attention_reasons:
            return "attention"
        return "success"

    @property
    def notification_status(self) -> str:
        if self.failure_reasons:
            return "FAILED"
        if self.attention_reasons:
            return "ATTENTION"
        return "OK"

    def render_summary(self) -> str:
        parts = [
            f"trading_day={self.trading_day.isoformat()}",
            f"status={self.status}",
            f"strategy={self.strategy_kind}",
            f"timeframe={self.timeframe.value}",
            f"targets={','.join(self.target_symbols)}",
            f"smoke_success={self.smoke_success}",
            f"previous_log_files={self.previous_day_errors.log_files}",
            f"previous_error_entries={self.previous_day_errors.error_entries}",
            f"trading_halted={self.trading_halted}",
            f"emergency_stop={self.emergency_stop}",
            f"data_ready={sum(1 for status in self.data_statuses if status.ready)}/{len(self.data_statuses)}",
            f"preview_ready={sum(1 for preview in self.strategy_previews if preview.ready)}/{len(self.strategy_previews)}",
            f"smoke_report={self.smoke_report_path.name}",
            f"inspection_report={self.inspection_report_path.name}",
            "data_statuses="
            + ",".join(status.render_summary() for status in self.data_statuses),
            "strategy_previews="
            + ",".join(preview.render_summary() for preview in self.strategy_previews),
        ]
        if self.previous_day_errors.sources:
            parts.append(
                f"previous_error_sources={','.join(self.previous_day_errors.sources)}"
            )
        if self.attention_reasons:
            parts.append(
                "attention_reasons=" + ",".join(self.attention_reasons)
            )
        if self.failure_reasons:
            parts.append(f"failure_reasons={','.join(self.failure_reasons)}")
        return " ".join(parts)

    def render_notification_body(self) -> str:
        lines = [
            f"trading_day={self.trading_day.isoformat()}",
            f"status={self.notification_status}",
            f"strategy={self.strategy_kind}",
            f"timeframe={self.timeframe.value}",
            f"targets={','.join(self.target_symbols)}",
            f"smoke_success={self.smoke_success}",
            (f"previous_day_errors={self.previous_day_errors.error_entries}"),
            f"trading_halted={self.trading_halted}",
            f"emergency_stop={self.emergency_stop}",
            f"smoke_report={self.smoke_report_path}",
            f"inspection_report={self.inspection_report_path}",
        ]
        if self.attention_reasons:
            lines.append("attention_reasons=" + ",".join(self.attention_reasons))
        if self.failure_reasons:
            lines.append("failure_reasons=" + ",".join(self.failure_reasons))
        lines.append("")
        lines.append("data_statuses:")
        lines.extend(f"- {status.render_summary()}" for status in self.data_statuses)
        lines.append("")
        lines.append("strategy_previews:")
        lines.extend(
            f"- {preview.render_notification_line()}"
            for preview in self.strategy_previews
        )
        return "\n".join(lines)


SmokeRunner = Callable[[AppSettings, datetime], SmokeReport]
SmokeReportWriter = Callable[[Path, SmokeReport], Path]
BarSourceFactory = Callable[[Path], CsvBarSource]
StrategyFactory = Callable[[str], Strategy]


class CollectStrategyBars(Protocol):
    def __call__(
        self,
        settings: AppSettings,
        *,
        bar_root: Path,
        timeframe: Timeframe,
        generated_at: datetime,
    ) -> None: ...


def _run_smoke_for_timestamp(
    settings: AppSettings,
    generated_at: datetime,
) -> SmokeReport:
    return run_read_only_smoke(
        settings,
        clock=lambda: generated_at,
    )


@dataclass(slots=True)
class MarketOpenPreparationRuntime:
    settings: AppSettings
    strategy_kind: str
    timeframe: Timeframe
    bar_root: Path
    broker_reader: BrokerReader
    notifier: Notifier
    state_store: FileExecutionStateStore
    collect_strategy_bars: CollectStrategyBars
    calendar: KrxRegularSessionCalendar = field(
        default_factory=KrxRegularSessionCalendar
    )
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(KST))
    smoke_runner: SmokeRunner = field(
        default_factory=lambda: _run_smoke_for_timestamp,
        repr=False,
    )
    smoke_report_writer: SmokeReportWriter = field(
        default=write_smoke_report,
        repr=False,
    )
    bar_source_factory: BarSourceFactory = field(
        default=CsvBarSource,
        repr=False,
    )
    strategy_factory: StrategyFactory = field(
        default=create_strategy,
        repr=False,
    )

    def run(
        self,
        *,
        timestamp: datetime | None = None,
    ) -> MarketOpenPreparationResult:
        generated_at = timestamp or self.clock()
        _require_aware_datetime("generated_at", generated_at)
        trading_day = generated_at.astimezone(KST).date()
        logger.info(
            "장 시작 전 준비를 시작합니다. 시각=%s 전략=%s 주기=%s",
            generated_at.isoformat(),
            self.strategy_kind,
            self.timeframe.value,
        )

        data_statuses = self._refresh_strategy_data(generated_at=generated_at)
        smoke_report = self.smoke_runner(self.settings, generated_at)
        smoke_report_path = self.smoke_report_writer(
            self.settings.log_dir, smoke_report
        )
        previous_day_errors = _collect_previous_day_errors(
            self.settings.log_dir,
            trading_day=trading_day,
            calendar=self.calendar,
        )
        strategy_previews = self._build_strategy_previews(
            generated_at=generated_at,
            data_statuses=data_statuses,
        )
        inspection_report = build_daily_inspection_report(
            trading_day,
            generated_at=generated_at,
            items=_build_pre_market_items(
                self.settings,
                strategy_kind=self.strategy_kind,
                timeframe=self.timeframe,
                smoke_report=smoke_report,
                smoke_report_path=smoke_report_path,
                previous_day_errors=previous_day_errors,
                data_statuses=data_statuses,
                strategy_previews=strategy_previews,
            ),
        )
        inspection_report_path = write_daily_inspection_report(
            self.settings.log_dir,
            inspection_report,
        )

        result = MarketOpenPreparationResult(
            generated_at=generated_at,
            trading_day=trading_day,
            strategy_kind=self.strategy_kind,
            timeframe=self.timeframe,
            target_symbols=self.settings.target_symbols,
            smoke_success=smoke_report.success,
            previous_day_errors=previous_day_errors,
            trading_halted=self.settings.risk.trading_halted,
            emergency_stop=self.settings.risk.emergency_stop,
            data_statuses=data_statuses,
            strategy_previews=strategy_previews,
            smoke_report_path=smoke_report_path,
            inspection_report_path=inspection_report_path,
        )
        self.notifier.send(
            _build_market_open_notification(
                result,
                created_at=generated_at,
            )
        )
        logger.info("장 시작 전 준비를 마쳤습니다. %s", result.render_summary())
        return result

    def build_job(
        self,
        *,
        name: str = "market_open_prepare",
    ) -> ScheduledJob:
        def handler(context: JobContext) -> str:
            result = self.run(timestamp=context.scheduled_at)
            summary = result.render_summary()
            if not result.success:
                raise RuntimeError(summary)
            return summary

        return ScheduledJob(
            name=name,
            phase=MarketSessionPhase.MARKET_OPEN,
            handler=handler,
        )

    def _refresh_strategy_data(
        self,
        *,
        generated_at: datetime,
    ) -> tuple[PreMarketDataStatus, ...]:
        before = self._load_cached_bars()
        collection_error: str | None = None
        try:
            self.collect_strategy_bars(
                self.settings,
                bar_root=self.bar_root,
                timeframe=self.timeframe,
                generated_at=generated_at,
            )
        except Exception as exc:
            collection_error = str(exc)
            logger.exception("장전 전략 입력 바 최신화에 실패했습니다.")
        after = self._load_cached_bars()

        statuses: list[PreMarketDataStatus] = []
        for symbol in self.settings.target_symbols:
            before_series = before.get(symbol, ())
            after_series = after.get(symbol, ())
            latest_bar = after_series[-1] if after_series else None
            latest_changed = (
                before_series[-1].timestamp if before_series else None
            ) != (latest_bar.timestamp if latest_bar is not None else None)
            statuses.append(
                PreMarketDataStatus(
                    symbol=symbol,
                    timeframe=self.timeframe,
                    bars_before=len(before_series),
                    bars_after=len(after_series),
                    latest_bar_at=None if latest_bar is None else latest_bar.timestamp,
                    refreshed=(len(before_series) != len(after_series))
                    or latest_changed,
                    error=collection_error,
                )
            )
        return tuple(statuses)

    def _build_strategy_previews(
        self,
        *,
        generated_at: datetime,
        data_statuses: tuple[PreMarketDataStatus, ...],
    ) -> tuple[StrategyPreview, ...]:
        try:
            holdings = self.broker_reader.get_holdings()
        except Exception as exc:
            logger.exception("장전 전략 프리뷰용 잔고 조회에 실패했습니다.")
            return tuple(
                StrategyPreview(
                    symbol=status.symbol,
                    timeframe=self.timeframe,
                    bars_loaded=status.bars_after,
                    preview_at=None,
                    reference_price=None,
                    signal=None,
                    status="preview_failed",
                    reason=f"holdings_lookup_failed:{exc}",
                )
                for status in data_statuses
            )

        existing_snapshots = self.state_store.list_snapshots()
        strategy = self.strategy_factory(self.strategy_kind)
        bar_source = self.bar_source_factory(self.bar_root)

        previews: list[StrategyPreview] = []
        for status in data_statuses:
            bars = bar_source.load_bars(status.symbol, self.timeframe)
            previews.append(
                self._build_strategy_preview(
                    symbol=status.symbol,
                    generated_at=generated_at,
                    strategy=strategy,
                    bars=bars,
                    holdings=holdings,
                    existing_snapshots=existing_snapshots,
                    data_status=status,
                )
            )
        return tuple(previews)

    def _build_strategy_preview(
        self,
        *,
        symbol: str,
        generated_at: datetime,
        strategy: Strategy,
        bars: tuple[Bar, ...],
        holdings: tuple[Holding, ...],
        existing_snapshots: tuple[OrderExecutionSnapshot, ...],
        data_status: PreMarketDataStatus,
    ) -> StrategyPreview:
        if not data_status.ready or not bars:
            return StrategyPreview(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                preview_at=None,
                reference_price=None,
                signal=None,
                status="preview_failed",
                reason=data_status.error or "strategy bars are unavailable",
            )

        try:
            quote = self.broker_reader.get_quote(symbol)
            preview_bar = _build_preview_bar(
                bars,
                timeframe=self.timeframe,
                quote_price=quote.price,
                calendar=self.calendar,
            )
            signal = strategy.generate_signal((*bars, preview_bar))
        except Exception as exc:
            logger.exception("장전 전략 프리뷰 계산에 실패했습니다. symbol=%s", symbol)
            return StrategyPreview(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                preview_at=None,
                reference_price=None,
                signal=None,
                status="preview_failed",
                reason=str(exc),
            )

        holding_quantity = next(
            (
                holding.quantity
                for holding in holdings
                if holding.symbol == symbol and holding.quantity > 0
            ),
            0,
        )
        if signal.action is SignalAction.HOLD:
            return StrategyPreview(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                preview_at=preview_bar.timestamp,
                reference_price=quote.price,
                signal=signal,
                status="hold",
                holding_quantity=holding_quantity,
            )

        if signal.action is SignalAction.SELL:
            if (
                _find_open_order(
                    existing_snapshots,
                    symbol=symbol,
                    side=OrderSide.SELL,
                )
                is not None
            ):
                return StrategyPreview(
                    symbol=symbol,
                    timeframe=self.timeframe,
                    bars_loaded=len(bars),
                    preview_at=preview_bar.timestamp,
                    reference_price=quote.price,
                    signal=signal,
                    status="sell_pending",
                    holding_quantity=holding_quantity,
                )
            if holding_quantity <= 0:
                return StrategyPreview(
                    symbol=symbol,
                    timeframe=self.timeframe,
                    bars_loaded=len(bars),
                    preview_at=preview_bar.timestamp,
                    reference_price=quote.price,
                    signal=signal,
                    status="sell_skipped",
                    holding_quantity=0,
                    reason="no holdings available to sell",
                )
            return StrategyPreview(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                preview_at=preview_bar.timestamp,
                reference_price=quote.price,
                signal=signal,
                status="sell_expected",
                requested_quantity=holding_quantity,
                approved_quantity=holding_quantity,
                holding_quantity=holding_quantity,
            )

        if (
            _find_open_order(
                existing_snapshots,
                symbol=symbol,
                side=OrderSide.BUY,
            )
            is not None
        ):
            return StrategyPreview(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                preview_at=preview_bar.timestamp,
                reference_price=quote.price,
                signal=signal,
                status="buy_pending",
                holding_quantity=holding_quantity,
            )
        try:
            capacity = self.broker_reader.get_order_capacity(symbol, quote.price)
        except Exception as exc:
            logger.exception(
                "장전 주문 가능 수량 조회에 실패했습니다. symbol=%s", symbol
            )
            return StrategyPreview(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                preview_at=preview_bar.timestamp,
                reference_price=quote.price,
                signal=signal,
                status="preview_failed",
                reason=f"order_capacity_lookup_failed:{exc}",
            )

        risk_snapshot = _build_preview_risk_account_snapshot(
            generated_at=generated_at,
            holdings=holdings,
            cash_available=capacity.cash_available,
            existing_snapshots=existing_snapshots,
        )
        requested_quantity = calculate_max_buy_quantity(
            settings=self.settings.risk,
            snapshot=risk_snapshot,
            symbol=symbol,
            order_price=quote.price,
        )
        if requested_quantity <= 0:
            return StrategyPreview(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                preview_at=preview_bar.timestamp,
                reference_price=quote.price,
                signal=signal,
                status="risk_blocked",
                requested_quantity=0,
                approved_quantity=0,
                holding_quantity=holding_quantity,
                reason="risk sizing produced zero quantity",
            )

        risk_check = evaluate_buy_order(
            self.settings.risk,
            risk_snapshot,
            ProposedBuyOrder(
                symbol=symbol,
                price=quote.price,
                quantity=requested_quantity,
            ),
        )
        if risk_check.approved_quantity <= 0:
            reason = (
                ",".join(violation.code.value for violation in risk_check.violations)
                or "risk_check_rejected"
            )
            return StrategyPreview(
                symbol=symbol,
                timeframe=self.timeframe,
                bars_loaded=len(bars),
                preview_at=preview_bar.timestamp,
                reference_price=quote.price,
                signal=signal,
                status="risk_blocked",
                requested_quantity=requested_quantity,
                approved_quantity=0,
                holding_quantity=holding_quantity,
                reason=reason,
            )

        return StrategyPreview(
            symbol=symbol,
            timeframe=self.timeframe,
            bars_loaded=len(bars),
            preview_at=preview_bar.timestamp,
            reference_price=quote.price,
            signal=signal,
            status="buy_expected",
            requested_quantity=requested_quantity,
            approved_quantity=risk_check.approved_quantity,
            holding_quantity=holding_quantity,
        )

    def _load_cached_bars(self) -> dict[str, tuple[Bar, ...]]:
        bar_source = self.bar_source_factory(self.bar_root)
        return {
            symbol: bar_source.load_bars(symbol, self.timeframe)
            for symbol in self.settings.target_symbols
        }


def build_market_open_preparation_job(
    runtime: MarketOpenPreparationRuntime,
    *,
    name: str = "market_open_prepare",
) -> ScheduledJob:
    return runtime.build_job(name=name)


def _build_pre_market_items(
    settings: AppSettings,
    *,
    strategy_kind: str,
    timeframe: Timeframe,
    smoke_report: SmokeReport,
    smoke_report_path: Path,
    previous_day_errors: PreviousDayErrorSummary,
    data_statuses: tuple[PreMarketDataStatus, ...],
    strategy_previews: tuple[StrategyPreview, ...],
) -> tuple[DailyInspectionItem, ...]:
    smoke_status = (
        InspectionStatus.PASSED if smoke_report.success else InspectionStatus.FAILED
    )
    holdings_status = (
        InspectionStatus.PASSED
        if smoke_report.holdings is not None
        else InspectionStatus.FAILED
    )
    capacity_status = (
        InspectionStatus.PASSED
        if smoke_report.order_capacity is not None
        else InspectionStatus.FAILED
    )
    previous_errors_status = (
        InspectionStatus.PASSED
        if previous_day_errors.error_entries == 0
        else InspectionStatus.FAILED
    )
    trading_halted_status = (
        InspectionStatus.FAILED
        if settings.risk.trading_halted
        else InspectionStatus.PASSED
    )
    emergency_stop_status = (
        InspectionStatus.FAILED
        if settings.risk.emergency_stop
        else InspectionStatus.PASSED
    )
    data_status = (
        InspectionStatus.PASSED
        if all(status.ready for status in data_statuses)
        else InspectionStatus.FAILED
    )
    preview_status = (
        InspectionStatus.PASSED
        if all(preview.ready for preview in strategy_previews)
        else InspectionStatus.FAILED
    )
    holdings_count = 0 if smoke_report.holdings is None else len(smoke_report.holdings)
    capacity_detail = "unavailable"
    if smoke_report.order_capacity is not None:
        capacity = smoke_report.order_capacity
        capacity_detail = (
            f"symbol={capacity.symbol} "
            f"max_orderable_quantity={capacity.max_orderable_quantity} "
            f"cash_available={capacity.cash_available}"
        )

    previous_error_detail = (
        f"previous_trading_day={previous_day_errors.trading_day.isoformat()} "
        f"log_files={previous_day_errors.log_files} "
        f"error_entries={previous_day_errors.error_entries}"
    )
    if previous_day_errors.sources:
        previous_error_detail += (
            f" error_sources={','.join(previous_day_errors.sources)}"
        )

    data_detail = "; ".join(status.render_summary() for status in data_statuses)
    preview_detail = "; ".join(
        preview.render_notification_line() for preview in strategy_previews
    )

    return (
        DailyInspectionItem(
            window=InspectionWindow.PRE_MARKET,
            label="API 인증 상태 확인",
            status=smoke_status,
            detail=(
                f"success={smoke_report.success} "
                f"target_symbol={smoke_report.target_symbol} "
                f"smoke_report={smoke_report_path.name}"
            ),
        ),
        DailyInspectionItem(
            window=InspectionWindow.PRE_MARKET,
            label="전일 로그 이상 여부 확인",
            status=previous_errors_status,
            detail=previous_error_detail,
        ),
        DailyInspectionItem(
            window=InspectionWindow.PRE_MARKET,
            label="장운영 플래그 확인",
            status=trading_halted_status,
            detail=f"trading_halted={settings.risk.trading_halted}",
        ),
        DailyInspectionItem(
            window=InspectionWindow.PRE_MARKET,
            label="대상 종목 목록 확인",
            status=InspectionStatus.PASSED,
            detail=(
                f"strategy={strategy_kind} "
                f"timeframe={timeframe.value} "
                f"targets={','.join(settings.target_symbols)}"
            ),
        ),
        DailyInspectionItem(
            window=InspectionWindow.PRE_MARKET,
            label="전략 입력 데이터 최신성 확인",
            status=data_status,
            detail=data_detail,
        ),
        DailyInspectionItem(
            window=InspectionWindow.PRE_MARKET,
            label="오늘 가격 기준 전략 예상 확인",
            status=preview_status,
            detail=preview_detail,
        ),
        DailyInspectionItem(
            window=InspectionWindow.PRE_MARKET,
            label="계좌/잔고 상태 확인",
            status=holdings_status,
            detail=f"holdings_count={holdings_count}",
        ),
        DailyInspectionItem(
            window=InspectionWindow.PRE_MARKET,
            label="주문가능수량 확인",
            status=capacity_status,
            detail=capacity_detail,
        ),
        DailyInspectionItem(
            window=InspectionWindow.PRE_MARKET,
            label="비상 정지 플래그 확인",
            status=emergency_stop_status,
            detail=f"emergency_stop={settings.risk.emergency_stop}",
        ),
    )


def _build_market_open_notification(
    result: MarketOpenPreparationResult,
    *,
    created_at: datetime,
) -> NotificationMessage:
    _require_aware_datetime("created_at", created_at)

    severity = AlertSeverity.INFO
    if result.failure_reasons:
        severity = AlertSeverity.ERROR
    elif result.attention_reasons:
        severity = AlertSeverity.WARNING
    status = result.notification_status
    return NotificationMessage(
        created_at=created_at,
        severity=severity,
        subject=(
            f"AutoTrade market open prep {result.trading_day.isoformat()} [{status}]"
        ),
        body=result.render_notification_body(),
    )


def _collect_previous_day_errors(
    log_dir: Path,
    *,
    trading_day: date,
    calendar: KrxRegularSessionCalendar,
) -> PreviousDayErrorSummary:
    previous_trading_day = _previous_trading_day(trading_day, calendar=calendar)
    if not log_dir.exists():
        return PreviousDayErrorSummary(
            trading_day=previous_trading_day,
            log_files=0,
            error_entries=0,
        )

    matched_paths = tuple(
        path
        for path in sorted(log_dir.glob("operations_*.log"))
        if _operations_log_day(path) == previous_trading_day
    )
    error_entries = 0
    sources: list[str] = []
    seen_sources: set[str] = set()
    for path in matched_paths:
        for line in path.read_text(encoding="utf-8").splitlines():
            if "level=error" not in line:
                continue
            error_entries += 1
            source = _extract_log_field(line, "source")
            if source is None or source in seen_sources:
                continue
            seen_sources.add(source)
            sources.append(source)

    return PreviousDayErrorSummary(
        trading_day=previous_trading_day,
        log_files=len(matched_paths),
        error_entries=error_entries,
        sources=tuple(sources),
    )


def _previous_trading_day(
    trading_day: date,
    *,
    calendar: KrxRegularSessionCalendar,
) -> date:
    current = trading_day
    while True:
        current = current.fromordinal(current.toordinal() - 1)
        if calendar.is_trading_day(current):
            return current


def _operations_log_day(path: Path) -> date | None:
    match = _OPERATIONS_LOG_PATTERN.fullmatch(path.name)
    if match is None:
        return None
    raw_day = match.group("day")
    return datetime.strptime(raw_day, "%Y%m%d").date()


def _extract_log_field(line: str, field_name: str) -> str | None:
    prefix = f"{field_name}="
    for fragment in line.split():
        if fragment.startswith(prefix):
            return fragment.removeprefix(prefix)
    return None


def _build_preview_bar(
    bars: tuple[Bar, ...],
    *,
    timeframe: Timeframe,
    quote_price: Decimal,
    calendar: KrxRegularSessionCalendar,
) -> Bar:
    last_bar = bars[-1]
    preview_at = calendar.next_timestamp(last_bar.timestamp, timeframe)
    return Bar(
        symbol=last_bar.symbol,
        timeframe=timeframe,
        timestamp=preview_at,
        open=quote_price,
        high=quote_price,
        low=quote_price,
        close=quote_price,
        volume=0,
    )


def _build_preview_risk_account_snapshot(
    *,
    generated_at: datetime,
    holdings: tuple[Holding, ...],
    cash_available: Decimal,
    existing_snapshots: tuple[OrderExecutionSnapshot, ...],
) -> RiskAccountSnapshot:
    current_equity = _calculate_current_equity(
        holdings=holdings,
        cash_available=cash_available,
    )
    session_equity = _positive_decimal_or_none(current_equity)
    return RiskAccountSnapshot(
        holdings=holdings,
        cash_available=cash_available,
        total_equity=current_equity,
        session_start_equity=session_equity,
        peak_equity=session_equity,
        orders_submitted_today=_count_orders_submitted_today(
            existing_snapshots,
            generated_at,
        ),
        unfilled_order_count=_count_unfilled_orders(existing_snapshots),
        market_closing=False,
    )


def _calculate_current_equity(
    *,
    holdings: tuple[Holding, ...],
    cash_available: Decimal,
) -> Decimal:
    holdings_value = sum(
        (
            (holding.current_price or holding.average_price) * holding.quantity
            for holding in holdings
        ),
        start=ZERO,
    )
    return cash_available + holdings_value


def _positive_decimal_or_none(value: Decimal) -> Decimal | None:
    return value if value > ZERO else None


def _count_orders_submitted_today(
    snapshots: tuple[OrderExecutionSnapshot, ...],
    generated_at: datetime,
) -> int:
    trading_day = generated_at.astimezone(KST).date()
    return sum(
        1
        for snapshot in snapshots
        if snapshot.order.created_at.astimezone(KST).date() == trading_day
    )


def _count_unfilled_orders(
    snapshots: tuple[OrderExecutionSnapshot, ...],
) -> int:
    return sum(
        1 for snapshot in snapshots if snapshot.order.status in _OPEN_ORDER_STATUSES
    )


def _find_open_order(
    snapshots: tuple[OrderExecutionSnapshot, ...],
    *,
    symbol: str,
    side: OrderSide,
) -> OrderExecutionSnapshot | None:
    for snapshot in snapshots:
        if snapshot.order.symbol != symbol:
            continue
        if snapshot.order.side is not side:
            continue
        if snapshot.order.status not in _OPEN_ORDER_STATUSES:
            continue
        return snapshot
    return None


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
