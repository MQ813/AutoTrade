from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from typing import runtime_checkable

from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import OrderStatus
from autotrade.data import KST
from autotrade.scheduler import JobRunResult
from autotrade.scheduler import MarketSessionPhase

_PHASE_ORDER = (
    MarketSessionPhase.MARKET_OPEN,
    MarketSessionPhase.INTRADAY,
    MarketSessionPhase.MARKET_CLOSE,
)


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_non_blank_text(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")


class LogSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class AlertSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class InspectionWindow(StrEnum):
    PRE_MARKET = "pre_market"
    INTRADAY = "intraday"
    POST_MARKET = "post_market"


class InspectionStatus(StrEnum):
    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"


_INSPECTION_WINDOW_ORDER = (
    InspectionWindow.PRE_MARKET,
    InspectionWindow.INTRADAY,
    InspectionWindow.POST_MARKET,
)
_DEFAULT_DAILY_INSPECTION_ITEMS = (
    (InspectionWindow.PRE_MARKET, "API 인증 상태 확인"),
    (InspectionWindow.PRE_MARKET, "전일 로그 이상 여부 확인"),
    (InspectionWindow.PRE_MARKET, "장운영 플래그 확인"),
    (InspectionWindow.PRE_MARKET, "대상 종목 목록 확인"),
    (InspectionWindow.PRE_MARKET, "계좌/잔고 상태 확인"),
    (InspectionWindow.PRE_MARKET, "비상 정지 플래그 확인"),
    (InspectionWindow.INTRADAY, "스케줄러 정상 동작 확인"),
    (InspectionWindow.INTRADAY, "주문/체결 이벤트 확인"),
    (InspectionWindow.INTRADAY, "미체결 건수 확인"),
    (InspectionWindow.INTRADAY, "이상 주문 여부 확인"),
    (InspectionWindow.INTRADAY, "손실 상한 도달 여부 확인"),
    (InspectionWindow.POST_MARKET, "당일 주문 내역 저장"),
    (InspectionWindow.POST_MARKET, "체결 내역 저장"),
    (InspectionWindow.POST_MARKET, "손익 요약 리포트 생성"),
    (InspectionWindow.POST_MARKET, "오류 로그 점검"),
    (InspectionWindow.POST_MARKET, "다음 거래일 준비 상태 확인"),
)


@dataclass(frozen=True, slots=True)
class ReportLogEntry:
    timestamp: datetime
    level: LogSeverity
    source: str
    message: str

    def __post_init__(self) -> None:
        _require_aware_datetime("timestamp", self.timestamp)
        _require_non_blank_text("source", self.source)
        _require_non_blank_text("message", self.message)


@dataclass(frozen=True, slots=True)
class DailyPhaseSummary:
    phase: MarketSessionPhase
    total_jobs: int
    successful_jobs: int
    failed_jobs: int

    def __post_init__(self) -> None:
        if self.total_jobs < 0:
            raise ValueError("total_jobs must be non-negative")
        if self.successful_jobs < 0:
            raise ValueError("successful_jobs must be non-negative")
        if self.failed_jobs < 0:
            raise ValueError("failed_jobs must be non-negative")
        if self.successful_jobs + self.failed_jobs != self.total_jobs:
            raise ValueError("phase counts must add up to total_jobs")


@dataclass(frozen=True, slots=True)
class DailyRunReport:
    trading_day: date
    generated_at: datetime
    total_jobs: int
    successful_jobs: int
    failed_jobs: int
    phase_summaries: tuple[DailyPhaseSummary, ...]
    job_results: tuple[JobRunResult, ...]

    def __post_init__(self) -> None:
        _require_aware_datetime("generated_at", self.generated_at)
        if self.total_jobs < 0:
            raise ValueError("total_jobs must be non-negative")
        if self.successful_jobs < 0:
            raise ValueError("successful_jobs must be non-negative")
        if self.failed_jobs < 0:
            raise ValueError("failed_jobs must be non-negative")
        if self.successful_jobs + self.failed_jobs != self.total_jobs:
            raise ValueError("report counts must add up to total_jobs")
        if len(self.job_results) != self.total_jobs:
            raise ValueError("job_results length must match total_jobs")


@dataclass(frozen=True, slots=True)
class NotificationMessage:
    created_at: datetime
    severity: AlertSeverity
    subject: str
    body: str

    def __post_init__(self) -> None:
        _require_aware_datetime("created_at", self.created_at)
        _require_non_blank_text("subject", self.subject)
        _require_non_blank_text("body", self.body)


@runtime_checkable
class Notifier(Protocol):
    def send(self, notification: NotificationMessage) -> None: ...


@dataclass(frozen=True, slots=True)
class DailyInspectionItem:
    window: InspectionWindow
    label: str
    status: InspectionStatus = InspectionStatus.PENDING
    detail: str | None = None

    def __post_init__(self) -> None:
        _require_non_blank_text("label", self.label)
        if self.detail is not None:
            _require_non_blank_text("detail", self.detail)


@dataclass(frozen=True, slots=True)
class DailyInspectionWindowSummary:
    window: InspectionWindow
    total_items: int
    passed_items: int
    failed_items: int
    pending_items: int

    def __post_init__(self) -> None:
        if self.total_items < 0:
            raise ValueError("total_items must be non-negative")
        if self.passed_items < 0:
            raise ValueError("passed_items must be non-negative")
        if self.failed_items < 0:
            raise ValueError("failed_items must be non-negative")
        if self.pending_items < 0:
            raise ValueError("pending_items must be non-negative")
        if (
            self.passed_items + self.failed_items + self.pending_items
            != self.total_items
        ):
            raise ValueError("inspection counts must add up to total_items")


@dataclass(frozen=True, slots=True)
class DailyInspectionReport:
    trading_day: date
    generated_at: datetime
    total_items: int
    passed_items: int
    failed_items: int
    pending_items: int
    window_summaries: tuple[DailyInspectionWindowSummary, ...]
    items: tuple[DailyInspectionItem, ...]

    def __post_init__(self) -> None:
        _require_aware_datetime("generated_at", self.generated_at)
        if self.total_items < 0:
            raise ValueError("total_items must be non-negative")
        if self.passed_items < 0:
            raise ValueError("passed_items must be non-negative")
        if self.failed_items < 0:
            raise ValueError("failed_items must be non-negative")
        if self.pending_items < 0:
            raise ValueError("pending_items must be non-negative")
        if (
            self.passed_items + self.failed_items + self.pending_items
            != self.total_items
        ):
            raise ValueError("report counts must add up to total_items")
        if len(self.items) != self.total_items:
            raise ValueError("items length must match total_items")


@dataclass(frozen=True, slots=True)
class WeeklyReviewDaySummary:
    trading_day: date
    total_jobs: int
    failed_jobs: int
    passed_inspection_items: int
    failed_inspection_items: int
    pending_inspection_items: int

    def __post_init__(self) -> None:
        if self.total_jobs < 0:
            raise ValueError("total_jobs must be non-negative")
        if self.failed_jobs < 0:
            raise ValueError("failed_jobs must be non-negative")
        if self.failed_jobs > self.total_jobs:
            raise ValueError("failed_jobs must not exceed total_jobs")
        if self.passed_inspection_items < 0:
            raise ValueError("passed_inspection_items must be non-negative")
        if self.failed_inspection_items < 0:
            raise ValueError("failed_inspection_items must be non-negative")
        if self.pending_inspection_items < 0:
            raise ValueError("pending_inspection_items must be non-negative")


@dataclass(frozen=True, slots=True)
class WeeklyReviewReport:
    week_start: date
    week_end: date
    generated_at: datetime
    total_jobs: int
    failed_jobs: int
    passed_inspection_items: int
    failed_inspection_items: int
    pending_inspection_items: int
    days_with_run_failures: tuple[date, ...]
    day_summaries: tuple[WeeklyReviewDaySummary, ...]

    def __post_init__(self) -> None:
        _require_aware_datetime("generated_at", self.generated_at)
        if self.week_end < self.week_start:
            raise ValueError("week_end must be on or after week_start")
        if self.total_jobs < 0:
            raise ValueError("total_jobs must be non-negative")
        if self.failed_jobs < 0:
            raise ValueError("failed_jobs must be non-negative")
        if self.failed_jobs > self.total_jobs:
            raise ValueError("failed_jobs must not exceed total_jobs")
        if self.passed_inspection_items < 0:
            raise ValueError("passed_inspection_items must be non-negative")
        if self.failed_inspection_items < 0:
            raise ValueError("failed_inspection_items must be non-negative")
        if self.pending_inspection_items < 0:
            raise ValueError("pending_inspection_items must be non-negative")


def build_order_alert(
    order: ExecutionOrder,
    *,
    created_at: datetime,
) -> NotificationMessage:
    _require_aware_datetime("created_at", created_at)

    severity = AlertSeverity.INFO
    if order.status is OrderStatus.CANCELED:
        severity = AlertSeverity.WARNING
    if order.status is OrderStatus.REJECTED:
        severity = AlertSeverity.ERROR

    lines = [
        f"order_id={order.order_id}",
        f"symbol={order.symbol}",
        f"status={order.status}",
        f"quantity={order.quantity}",
        f"filled_quantity={order.filled_quantity}",
        f"limit_price={order.limit_price}",
        f"updated_at={order.updated_at.isoformat()}",
    ]
    return NotificationMessage(
        created_at=created_at,
        severity=severity,
        subject=f"AutoTrade order {order.symbol} [{order.status}]",
        body="\n".join(lines),
    )


def publish_order_alert(
    notifier: Notifier,
    order: ExecutionOrder,
    *,
    created_at: datetime | None = None,
) -> NotificationMessage:
    notification = build_order_alert(
        order,
        created_at=created_at or datetime.now(KST),
    )
    notifier.send(notification)
    return notification


def build_fill_alert(
    fill: ExecutionFill,
    *,
    created_at: datetime,
) -> NotificationMessage:
    _require_aware_datetime("created_at", created_at)

    return NotificationMessage(
        created_at=created_at,
        severity=AlertSeverity.INFO,
        subject=f"AutoTrade fill {fill.symbol} [{fill.quantity}@{fill.price}]",
        body="\n".join(
            [
                f"fill_id={fill.fill_id}",
                f"order_id={fill.order_id}",
                f"symbol={fill.symbol}",
                f"quantity={fill.quantity}",
                f"price={fill.price}",
                f"filled_at={fill.filled_at.isoformat()}",
            ]
        ),
    )


def publish_fill_alert(
    notifier: Notifier,
    fill: ExecutionFill,
    *,
    created_at: datetime | None = None,
) -> NotificationMessage:
    notification = build_fill_alert(
        fill,
        created_at=created_at or datetime.now(KST),
    )
    notifier.send(notification)
    return notification


def build_daily_inspection_report(
    trading_day: date,
    *,
    generated_at: datetime,
    items: Sequence[DailyInspectionItem] | None = None,
) -> DailyInspectionReport:
    _require_aware_datetime("generated_at", generated_at)

    resolved_items = (
        tuple(items) if items is not None else _default_daily_inspection_items()
    )
    passed_items = sum(
        1 for item in resolved_items if item.status is InspectionStatus.PASSED
    )
    failed_items = sum(
        1 for item in resolved_items if item.status is InspectionStatus.FAILED
    )
    pending_items = len(resolved_items) - passed_items - failed_items

    window_summaries = []
    for window in _INSPECTION_WINDOW_ORDER:
        window_items = tuple(item for item in resolved_items if item.window is window)
        passed_window_items = sum(
            1 for item in window_items if item.status is InspectionStatus.PASSED
        )
        failed_window_items = sum(
            1 for item in window_items if item.status is InspectionStatus.FAILED
        )
        window_summaries.append(
            DailyInspectionWindowSummary(
                window=window,
                total_items=len(window_items),
                passed_items=passed_window_items,
                failed_items=failed_window_items,
                pending_items=(
                    len(window_items) - passed_window_items - failed_window_items
                ),
            )
        )

    return DailyInspectionReport(
        trading_day=trading_day,
        generated_at=generated_at,
        total_items=len(resolved_items),
        passed_items=passed_items,
        failed_items=failed_items,
        pending_items=pending_items,
        window_summaries=tuple(window_summaries),
        items=resolved_items,
    )


def render_daily_inspection_report(report: DailyInspectionReport) -> str:
    lines = [
        f"trading_day={report.trading_day.isoformat()}",
        f"generated_at={report.generated_at.isoformat()}",
        f"total_items={report.total_items}",
        f"passed_items={report.passed_items}",
        f"failed_items={report.failed_items}",
        f"pending_items={report.pending_items}",
    ]
    for summary in report.window_summaries:
        lines.append(
            "window="
            f"{summary.window} "
            f"total_items={summary.total_items} "
            f"passed_items={summary.passed_items} "
            f"failed_items={summary.failed_items} "
            f"pending_items={summary.pending_items}"
        )
    for item in report.items:
        line = (
            f"item_window={item.window} "
            f"item_status={item.status} "
            f"item_label={item.label}"
        )
        if item.detail is not None:
            line += f" item_detail={item.detail}"
        lines.append(line)
    return "\n".join(lines) + "\n"


def write_daily_inspection_report(
    log_dir: Path,
    report: DailyInspectionReport,
) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = (
        log_dir
        / f"daily_inspection_{report.generated_at.strftime('%Y%m%d_%H%M%S_%f')}.txt"
    )
    report_path.write_text(render_daily_inspection_report(report), encoding="utf-8")
    return report_path


def build_weekly_review_report(
    week_start: date,
    *,
    generated_at: datetime,
    daily_run_reports: Sequence[DailyRunReport] = (),
    daily_inspection_reports: Sequence[DailyInspectionReport] = (),
) -> WeeklyReviewReport:
    _require_aware_datetime("generated_at", generated_at)

    week_end = week_start + timedelta(days=6)
    run_reports_by_day = {
        report.trading_day: report
        for report in daily_run_reports
        if week_start <= report.trading_day <= week_end
    }
    inspection_reports_by_day = {
        report.trading_day: report
        for report in daily_inspection_reports
        if week_start <= report.trading_day <= week_end
    }
    trading_days = sorted(set(run_reports_by_day) | set(inspection_reports_by_day))

    day_summaries: list[WeeklyReviewDaySummary] = []
    days_with_run_failures: list[date] = []
    total_jobs = 0
    failed_jobs = 0
    passed_inspection_items = 0
    failed_inspection_items = 0
    pending_inspection_items = 0

    for trading_day in trading_days:
        run_report = run_reports_by_day.get(trading_day)
        inspection_report = inspection_reports_by_day.get(trading_day)
        total_jobs += 0 if run_report is None else run_report.total_jobs
        failed_jobs += 0 if run_report is None else run_report.failed_jobs
        if run_report is not None and run_report.failed_jobs > 0:
            days_with_run_failures.append(trading_day)

        passed_inspection_items += (
            0 if inspection_report is None else inspection_report.passed_items
        )
        failed_inspection_items += (
            0 if inspection_report is None else inspection_report.failed_items
        )
        pending_inspection_items += (
            0 if inspection_report is None else inspection_report.pending_items
        )
        day_summaries.append(
            WeeklyReviewDaySummary(
                trading_day=trading_day,
                total_jobs=0 if run_report is None else run_report.total_jobs,
                failed_jobs=0 if run_report is None else run_report.failed_jobs,
                passed_inspection_items=(
                    0 if inspection_report is None else inspection_report.passed_items
                ),
                failed_inspection_items=(
                    0 if inspection_report is None else inspection_report.failed_items
                ),
                pending_inspection_items=(
                    0 if inspection_report is None else inspection_report.pending_items
                ),
            )
        )

    return WeeklyReviewReport(
        week_start=week_start,
        week_end=week_end,
        generated_at=generated_at,
        total_jobs=total_jobs,
        failed_jobs=failed_jobs,
        passed_inspection_items=passed_inspection_items,
        failed_inspection_items=failed_inspection_items,
        pending_inspection_items=pending_inspection_items,
        days_with_run_failures=tuple(days_with_run_failures),
        day_summaries=tuple(day_summaries),
    )


def render_weekly_review_report(report: WeeklyReviewReport) -> str:
    lines = [
        f"week_start={report.week_start.isoformat()}",
        f"week_end={report.week_end.isoformat()}",
        f"generated_at={report.generated_at.isoformat()}",
        f"total_jobs={report.total_jobs}",
        f"failed_jobs={report.failed_jobs}",
        f"passed_inspection_items={report.passed_inspection_items}",
        f"failed_inspection_items={report.failed_inspection_items}",
        f"pending_inspection_items={report.pending_inspection_items}",
    ]
    if report.days_with_run_failures:
        lines.append(
            "days_with_run_failures="
            + ",".join(day.isoformat() for day in report.days_with_run_failures)
        )
    for summary in report.day_summaries:
        lines.append(
            "day="
            f"{summary.trading_day.isoformat()} "
            f"total_jobs={summary.total_jobs} "
            f"failed_jobs={summary.failed_jobs} "
            f"passed_inspection_items={summary.passed_inspection_items} "
            f"failed_inspection_items={summary.failed_inspection_items} "
            f"pending_inspection_items={summary.pending_inspection_items}"
        )
    lines.extend(
        (
            "review_prompt=수익보다 정합성과 재현 가능성을 먼저 확인했는가?",
            "review_prompt=주문 상태와 체결 상태가 내부 기록과 일치했는가?",
            "review_prompt=실패 원인과 다음 주 조정 사항을 문서에 남겼는가?",
        )
    )
    return "\n".join(lines) + "\n"


def write_weekly_review_report(
    log_dir: Path,
    report: WeeklyReviewReport,
) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = log_dir / (
        f"weekly_review_{report.week_start.strftime('%Y%m%d')}"
        f"_{report.week_end.strftime('%Y%m%d')}.txt"
    )
    report_path.write_text(render_weekly_review_report(report), encoding="utf-8")
    return report_path


def build_run_log_entries(
    job_results: Sequence[JobRunResult],
) -> tuple[ReportLogEntry, ...]:
    entries: list[ReportLogEntry] = []
    for result in job_results:
        fragments = [
            f"phase={result.phase}",
            f"scheduled_at={result.scheduled_at.isoformat()}",
            f"status={'success' if result.success else 'failure'}",
        ]
        if result.detail is not None:
            fragments.append(f"detail={result.detail}")
        if result.error is not None:
            fragments.append(f"error={result.error}")
        entries.append(
            ReportLogEntry(
                timestamp=result.finished_at,
                level=LogSeverity.INFO if result.success else LogSeverity.ERROR,
                source=result.job_name,
                message=" ".join(fragments),
            )
        )
    return tuple(entries)


def render_run_log(entries: Sequence[ReportLogEntry]) -> str:
    lines = [
        (
            f"timestamp={entry.timestamp.isoformat()} "
            f"level={entry.level} "
            f"source={entry.source} "
            f"message={entry.message}"
        )
        for entry in entries
    ]
    return "\n".join(lines) + ("\n" if lines else "")


def write_run_log(
    log_dir: Path,
    entries: Sequence[ReportLogEntry],
    *,
    generated_at: datetime | None = None,
) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = generated_at or _resolve_generated_at(entries)
    log_path = log_dir / f"operations_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.log"
    log_path.write_text(render_run_log(entries), encoding="utf-8")
    return log_path


def build_daily_run_report(
    trading_day: date,
    job_results: Sequence[JobRunResult],
    *,
    generated_at: datetime,
) -> DailyRunReport:
    _require_aware_datetime("generated_at", generated_at)

    results = tuple(job_results)
    successful_jobs = sum(1 for result in results if result.success)
    failed_jobs = len(results) - successful_jobs

    phase_summaries = []
    for phase in _PHASE_ORDER:
        phase_results = tuple(result for result in results if result.phase == phase)
        phase_successes = sum(1 for result in phase_results if result.success)
        phase_summaries.append(
            DailyPhaseSummary(
                phase=phase,
                total_jobs=len(phase_results),
                successful_jobs=phase_successes,
                failed_jobs=len(phase_results) - phase_successes,
            )
        )

    return DailyRunReport(
        trading_day=trading_day,
        generated_at=generated_at,
        total_jobs=len(results),
        successful_jobs=successful_jobs,
        failed_jobs=failed_jobs,
        phase_summaries=tuple(phase_summaries),
        job_results=results,
    )


def render_daily_run_report(report: DailyRunReport) -> str:
    lines = [
        f"trading_day={report.trading_day.isoformat()}",
        f"generated_at={report.generated_at.isoformat()}",
        f"total_jobs={report.total_jobs}",
        f"successful_jobs={report.successful_jobs}",
        f"failed_jobs={report.failed_jobs}",
    ]
    for summary in report.phase_summaries:
        lines.append(
            "phase="
            f"{summary.phase} "
            f"total_jobs={summary.total_jobs} "
            f"successful_jobs={summary.successful_jobs} "
            f"failed_jobs={summary.failed_jobs}"
        )
    for result in report.job_results:
        status = "success" if result.success else "failure"
        fragments = [
            f"job={result.job_name}",
            f"phase={result.phase}",
            f"scheduled_at={result.scheduled_at.isoformat()}",
            f"status={status}",
        ]
        if result.detail is not None:
            fragments.append(f"detail={result.detail}")
        if result.error is not None:
            fragments.append(f"error={result.error}")
        lines.append(" ".join(fragments))
    return "\n".join(lines) + "\n"


def write_daily_run_report(log_dir: Path, report: DailyRunReport) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = (
        log_dir / f"daily_report_{report.generated_at.strftime('%Y%m%d_%H%M%S_%f')}.txt"
    )
    report_path.write_text(render_daily_run_report(report), encoding="utf-8")
    return report_path


def build_daily_run_alert(
    report: DailyRunReport,
    *,
    created_at: datetime,
) -> NotificationMessage:
    _require_aware_datetime("created_at", created_at)

    severity = AlertSeverity.INFO
    status = "OK"
    if report.failed_jobs > 0:
        severity = AlertSeverity.ERROR
        status = "FAILED"
    elif report.total_jobs == 0:
        severity = AlertSeverity.WARNING
        status = "NO_RUNS"

    lines = [
        f"trading_day={report.trading_day.isoformat()}",
        f"total_jobs={report.total_jobs}",
        f"successful_jobs={report.successful_jobs}",
        f"failed_jobs={report.failed_jobs}",
    ]
    failures = [result.job_name for result in report.job_results if not result.success]
    if failures:
        lines.append(f"failed_job_names={','.join(failures)}")

    return NotificationMessage(
        created_at=created_at,
        severity=severity,
        subject=f"AutoTrade daily report {report.trading_day.isoformat()} [{status}]",
        body="\n".join(lines),
    )


def publish_daily_run_alert(
    notifier: Notifier,
    report: DailyRunReport,
    *,
    created_at: datetime | None = None,
) -> NotificationMessage:
    notification = build_daily_run_alert(
        report,
        created_at=created_at or datetime.now(KST),
    )
    notifier.send(notification)
    return notification


def _resolve_generated_at(entries: Sequence[ReportLogEntry]) -> datetime:
    if entries:
        return entries[-1].timestamp
    return datetime.now(KST)


def _default_daily_inspection_items() -> tuple[DailyInspectionItem, ...]:
    return tuple(
        DailyInspectionItem(window=window, label=label)
        for window, label in _DEFAULT_DAILY_INSPECTION_ITEMS
    )
