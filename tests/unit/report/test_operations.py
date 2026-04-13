from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from datetime import datetime

from autotrade.data import KST
from autotrade.report import AlertSeverity
from autotrade.report import LogSeverity
from autotrade.report import NotificationMessage
from autotrade.report import build_daily_run_report
from autotrade.report import build_run_log_entries
from autotrade.report import publish_daily_run_alert
from autotrade.report import render_daily_run_report
from autotrade.report import render_run_log
from autotrade.report import write_daily_run_report
from autotrade.report import write_run_log
from autotrade.scheduler import JobRunResult
from autotrade.scheduler import MarketSessionPhase


def test_build_run_log_entries_and_write_run_log(tmp_path) -> None:
    results = (
        _job_result(
            job_name="prepare",
            phase=MarketSessionPhase.MARKET_OPEN,
            scheduled_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
            finished_at=datetime(2026, 4, 10, 9, 0, 5, tzinfo=KST),
            success=True,
            detail="prepared",
        ),
        _job_result(
            job_name="heartbeat",
            phase=MarketSessionPhase.INTRADAY,
            scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
            finished_at=datetime(2026, 4, 10, 9, 30, 4, tzinfo=KST),
            success=False,
            error="heartbeat failed",
        ),
    )

    entries = build_run_log_entries(results)
    rendered = render_run_log(entries)
    log_path = write_run_log(
        tmp_path,
        entries,
        generated_at=datetime(2026, 4, 10, 16, 0, tzinfo=KST),
    )

    assert entries[0].level == LogSeverity.INFO
    assert entries[1].level == LogSeverity.ERROR
    assert "source=prepare" in rendered
    assert "status=success" in rendered
    assert "error=heartbeat failed" in rendered
    assert log_path.exists()
    assert "source=heartbeat" in log_path.read_text(encoding="utf-8")


def test_build_daily_run_report_and_write_file(tmp_path) -> None:
    results = (
        _job_result(
            job_name="prepare",
            phase=MarketSessionPhase.MARKET_OPEN,
            scheduled_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
            finished_at=datetime(2026, 4, 10, 9, 0, 5, tzinfo=KST),
            success=True,
            detail="prepared",
        ),
        _job_result(
            job_name="heartbeat",
            phase=MarketSessionPhase.INTRADAY,
            scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
            finished_at=datetime(2026, 4, 10, 9, 30, 4, tzinfo=KST),
            success=False,
            error="heartbeat failed",
        ),
        _job_result(
            job_name="settle",
            phase=MarketSessionPhase.MARKET_CLOSE,
            scheduled_at=datetime(2026, 4, 10, 15, 30, tzinfo=KST),
            finished_at=datetime(2026, 4, 10, 15, 30, 2, tzinfo=KST),
            success=True,
        ),
    )

    report = build_daily_run_report(
        date(2026, 4, 10),
        results,
        generated_at=datetime(2026, 4, 10, 16, 0, tzinfo=KST),
    )
    rendered = render_daily_run_report(report)
    report_path = write_daily_run_report(tmp_path, report)

    assert report.total_jobs == 3
    assert report.successful_jobs == 2
    assert report.failed_jobs == 1
    assert [summary.total_jobs for summary in report.phase_summaries] == [1, 1, 1]
    assert "phase=intraday total_jobs=1 successful_jobs=0 failed_jobs=1" in rendered
    assert "job=heartbeat" in rendered
    assert "error=heartbeat failed" in rendered
    assert report_path.exists()
    assert "trading_day=2026-04-10" in report_path.read_text(encoding="utf-8")


def test_publish_daily_run_alert_uses_notifier() -> None:
    notifier = RecordingNotifier()
    report = build_daily_run_report(
        date(2026, 4, 10),
        (
            _job_result(
                job_name="heartbeat",
                phase=MarketSessionPhase.INTRADAY,
                scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
                finished_at=datetime(2026, 4, 10, 9, 30, 4, tzinfo=KST),
                success=False,
                error="heartbeat failed",
            ),
        ),
        generated_at=datetime(2026, 4, 10, 16, 0, tzinfo=KST),
    )

    notification = publish_daily_run_alert(
        notifier,
        report,
        created_at=datetime(2026, 4, 10, 16, 1, tzinfo=KST),
    )

    assert notification.severity == AlertSeverity.ERROR
    assert notifier.notifications == [notification]
    assert "FAILED" in notification.subject
    assert "failed_job_names=heartbeat" in notification.body


def _job_result(
    *,
    job_name: str,
    phase: MarketSessionPhase,
    scheduled_at: datetime,
    finished_at: datetime,
    success: bool,
    detail: str | None = None,
    error: str | None = None,
) -> JobRunResult:
    return JobRunResult(
        job_name=job_name,
        phase=phase,
        scheduled_at=scheduled_at,
        started_at=scheduled_at,
        finished_at=finished_at,
        success=success,
        detail=detail,
        error=error,
    )


@dataclass
class RecordingNotifier:
    notifications: list[NotificationMessage] = field(default_factory=list)

    def send(self, notification: NotificationMessage) -> None:
        self.notifications.append(notification)
