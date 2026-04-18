from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
import logging
from pathlib import Path
import re

from autotrade.broker.smoke import SmokeReport
from autotrade.broker.smoke import run_read_only_smoke
from autotrade.broker.smoke import write_smoke_report
from autotrade.config import AppSettings
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.report import DailyInspectionItem
from autotrade.report import InspectionStatus
from autotrade.report import InspectionWindow
from autotrade.report import build_daily_inspection_report
from autotrade.report import write_daily_inspection_report
from autotrade.scheduler import JobContext
from autotrade.scheduler import MarketSessionPhase
from autotrade.scheduler import ScheduledJob

logger = logging.getLogger(__name__)

_OPERATIONS_LOG_PATTERN = re.compile(
    r"^operations_(?P<day>\d{8})_\d{6}_\d+\.log$"
)


@dataclass(frozen=True, slots=True)
class PreviousDayErrorSummary:
    trading_day: date
    log_files: int
    error_entries: int
    sources: tuple[str, ...] = ()


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
    smoke_report_path: Path
    inspection_report_path: Path

    def __post_init__(self) -> None:
        _require_aware_datetime("generated_at", self.generated_at)

    @property
    def success(self) -> bool:
        return not self.failure_reasons

    @property
    def failure_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if not self.smoke_success:
            reasons.append("broker_smoke_failed")
        if self.previous_day_errors.error_entries > 0:
            reasons.append("previous_day_errors_detected")
        if self.trading_halted:
            reasons.append("trading_halted")
        if self.emergency_stop:
            reasons.append("emergency_stop")
        return tuple(reasons)

    def render_summary(self) -> str:
        status = "success" if self.success else "failure"
        parts = [
            f"trading_day={self.trading_day.isoformat()}",
            f"status={status}",
            f"strategy={self.strategy_kind}",
            f"timeframe={self.timeframe.value}",
            f"targets={','.join(self.target_symbols)}",
            f"smoke_success={self.smoke_success}",
            f"previous_log_files={self.previous_day_errors.log_files}",
            f"previous_error_entries={self.previous_day_errors.error_entries}",
            f"trading_halted={self.trading_halted}",
            f"emergency_stop={self.emergency_stop}",
            f"smoke_report={self.smoke_report_path.name}",
            f"inspection_report={self.inspection_report_path.name}",
        ]
        if self.previous_day_errors.sources:
            parts.append(
                f"previous_error_sources={','.join(self.previous_day_errors.sources)}"
            )
        if self.failure_reasons:
            parts.append(f"failure_reasons={','.join(self.failure_reasons)}")
        return " ".join(parts)


SmokeRunner = Callable[[AppSettings, datetime], SmokeReport]
SmokeReportWriter = Callable[[Path, SmokeReport], Path]


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

        smoke_report = self.smoke_runner(self.settings, generated_at)
        smoke_report_path = self.smoke_report_writer(self.settings.log_dir, smoke_report)
        previous_day_errors = _collect_previous_day_errors(
            self.settings.log_dir,
            trading_day=trading_day,
            calendar=self.calendar,
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
            smoke_report_path=smoke_report_path,
            inspection_report_path=inspection_report_path,
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


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
