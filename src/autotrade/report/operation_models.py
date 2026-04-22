from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from datetime import datetime
from enum import StrEnum
from typing import Protocol
from typing import runtime_checkable

from autotrade.scheduler import JobRunResult
from autotrade.scheduler import MarketSessionPhase


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


PHASE_ORDER = (
    MarketSessionPhase.MARKET_OPEN,
    MarketSessionPhase.INTRADAY,
    MarketSessionPhase.MARKET_CLOSE,
)
INSPECTION_WINDOW_ORDER = (
    InspectionWindow.PRE_MARKET,
    InspectionWindow.INTRADAY,
    InspectionWindow.POST_MARKET,
)
DAILY_RUN_ARCHIVE_DIR = "daily_run_reports"
DAILY_INSPECTION_ARCHIVE_DIR = "daily_inspection_reports"
WEEKLY_REVIEW_ARCHIVE_DIR = "weekly_review_reports"
JOB_HISTORY_ARCHIVE_DIR = "job_history"
DEFAULT_DAILY_INSPECTION_ITEMS = (
    (InspectionWindow.PRE_MARKET, "API 인증 상태 확인"),
    (InspectionWindow.PRE_MARKET, "전일 로그 이상 여부 확인"),
    (InspectionWindow.PRE_MARKET, "장운영 플래그 확인"),
    (InspectionWindow.PRE_MARKET, "대상 종목 목록 확인"),
    (InspectionWindow.PRE_MARKET, "전략 입력 데이터 최신성 확인"),
    (InspectionWindow.PRE_MARKET, "오늘 가격 기준 전략 예상 확인"),
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
    expected_trading_days: tuple[date, ...]
    missing_run_report_days: tuple[date, ...]
    missing_inspection_report_days: tuple[date, ...]
    repeated_failure_jobs: tuple[str, ...]
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


def default_daily_inspection_items() -> tuple[DailyInspectionItem, ...]:
    return tuple(
        DailyInspectionItem(window=window, label=label)
        for window, label in DEFAULT_DAILY_INSPECTION_ITEMS
    )
