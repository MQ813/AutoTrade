from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from typing import runtime_checkable

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
