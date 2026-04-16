from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from datetime import datetime
from decimal import Decimal

from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.data import KST
from autotrade.report import AlertSeverity
from autotrade.report import InspectionStatus
from autotrade.report import InspectionWindow
from autotrade.report import LogSeverity
from autotrade.report import NotificationMessage
from autotrade.report import build_daily_inspection_report
from autotrade.report import build_daily_run_report
from autotrade.report import build_fill_alert
from autotrade.report import build_order_alert
from autotrade.report import build_run_log_entries
from autotrade.report import build_weekly_review_report
from autotrade.report import publish_fill_alert
from autotrade.report import publish_order_alert
from autotrade.report import publish_daily_run_alert
from autotrade.report import render_daily_inspection_report
from autotrade.report import render_daily_run_report
from autotrade.report import render_run_log
from autotrade.report import render_weekly_review_report
from autotrade.report import write_daily_inspection_report
from autotrade.report import write_daily_run_report
from autotrade.report import write_run_log
from autotrade.report import write_weekly_review_report
from autotrade.report import DailyInspectionItem
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


def test_build_order_and_fill_alerts_use_notifier() -> None:
    notifier = RecordingNotifier()
    order = ExecutionOrder(
        order_id="order-1",
        symbol="069500",
        side=OrderSide.BUY,
        quantity=10,
        limit_price=Decimal("10000"),
        status=OrderStatus.REJECTED,
        created_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        updated_at=datetime(2026, 4, 10, 9, 0, 1, tzinfo=KST),
    )
    fill = ExecutionFill(
        fill_id="fill-1",
        order_id="order-1",
        symbol="069500",
        quantity=3,
        price=Decimal("9990"),
        filled_at=datetime(2026, 4, 10, 9, 1, tzinfo=KST),
    )

    order_alert = build_order_alert(
        order,
        created_at=datetime(2026, 4, 10, 9, 0, 2, tzinfo=KST),
    )
    fill_alert = build_fill_alert(
        fill,
        created_at=datetime(2026, 4, 10, 9, 1, 1, tzinfo=KST),
    )
    published_order_alert = publish_order_alert(
        notifier,
        order,
        created_at=order_alert.created_at,
    )
    published_fill_alert = publish_fill_alert(
        notifier,
        fill,
        created_at=fill_alert.created_at,
    )

    assert order_alert.severity == AlertSeverity.ERROR
    assert "REJECTED" in order_alert.subject
    assert "order_id=order-1" in order_alert.body
    assert fill_alert.severity == AlertSeverity.INFO
    assert "fill_id=fill-1" in fill_alert.body
    assert notifier.notifications == [published_order_alert, published_fill_alert]


def test_build_daily_inspection_report_and_write_file(tmp_path) -> None:
    report = build_daily_inspection_report(
        date(2026, 4, 10),
        generated_at=datetime(2026, 4, 10, 8, 0, tzinfo=KST),
        items=(
            DailyInspectionItem(
                window=InspectionWindow.PRE_MARKET,
                label="API 인증 상태 확인",
                status=InspectionStatus.PASSED,
            ),
            DailyInspectionItem(
                window=InspectionWindow.INTRADAY,
                label="주문/체결 이벤트 확인",
                status=InspectionStatus.FAILED,
                detail="fill mismatch",
            ),
            DailyInspectionItem(
                window=InspectionWindow.POST_MARKET,
                label="오류 로그 점검",
            ),
        ),
    )
    rendered = render_daily_inspection_report(report)
    report_path = write_daily_inspection_report(tmp_path, report)

    assert report.total_items == 3
    assert report.passed_items == 1
    assert report.failed_items == 1
    assert report.pending_items == 1
    assert (
        "window=intraday total_items=1 passed_items=0 failed_items=1 pending_items=0"
        in rendered
    )
    assert "item_detail=fill mismatch" in rendered
    assert report_path.exists()
    assert "trading_day=2026-04-10" in report_path.read_text(encoding="utf-8")


def test_build_weekly_review_report_aggregates_daily_reports(tmp_path) -> None:
    monday_run = build_daily_run_report(
        date(2026, 4, 6),
        (
            _job_result(
                job_name="prepare",
                phase=MarketSessionPhase.MARKET_OPEN,
                scheduled_at=datetime(2026, 4, 6, 9, 0, tzinfo=KST),
                finished_at=datetime(2026, 4, 6, 9, 0, 2, tzinfo=KST),
                success=True,
            ),
            _job_result(
                job_name="heartbeat",
                phase=MarketSessionPhase.INTRADAY,
                scheduled_at=datetime(2026, 4, 6, 9, 30, tzinfo=KST),
                finished_at=datetime(2026, 4, 6, 9, 30, 2, tzinfo=KST),
                success=False,
                error="stale quote",
            ),
        ),
        generated_at=datetime(2026, 4, 6, 16, 0, tzinfo=KST),
    )
    monday_inspection = build_daily_inspection_report(
        date(2026, 4, 6),
        generated_at=datetime(2026, 4, 6, 16, 5, tzinfo=KST),
        items=(
            DailyInspectionItem(
                window=InspectionWindow.PRE_MARKET,
                label="API 인증 상태 확인",
                status=InspectionStatus.PASSED,
            ),
            DailyInspectionItem(
                window=InspectionWindow.POST_MARKET,
                label="오류 로그 점검",
                status=InspectionStatus.FAILED,
                detail="stale quote",
            ),
        ),
    )

    report = build_weekly_review_report(
        date(2026, 4, 6),
        generated_at=datetime(2026, 4, 12, 18, 0, tzinfo=KST),
        daily_run_reports=(monday_run,),
        daily_inspection_reports=(monday_inspection,),
    )
    rendered = render_weekly_review_report(report)
    report_path = write_weekly_review_report(tmp_path, report)

    assert report.total_jobs == 2
    assert report.failed_jobs == 1
    assert report.failed_inspection_items == 1
    assert report.pending_inspection_items == 0
    assert report.days_with_run_failures == (date(2026, 4, 6),)
    assert "day=2026-04-06 total_jobs=2 failed_jobs=1" in rendered
    assert "days_with_run_failures=2026-04-06" in rendered
    assert report_path.exists()


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
