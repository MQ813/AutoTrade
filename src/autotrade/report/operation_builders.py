from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from datetime import datetime
from datetime import timedelta

from autotrade.scheduler import JobRunResult
from autotrade.data import KrxRegularSessionCalendar
from autotrade.report.operation_models import DailyInspectionItem
from autotrade.report.operation_models import DailyInspectionReport
from autotrade.report.operation_models import DailyInspectionWindowSummary
from autotrade.report.operation_models import DailyPhaseSummary
from autotrade.report.operation_models import DailyRunReport
from autotrade.report.operation_models import INSPECTION_WINDOW_ORDER
from autotrade.report.operation_models import InspectionStatus
from autotrade.report.operation_models import LogSeverity
from autotrade.report.operation_models import PHASE_ORDER
from autotrade.report.operation_models import ReportLogEntry
from autotrade.report.operation_models import WeeklyReviewDaySummary
from autotrade.report.operation_models import WeeklyReviewReport
from autotrade.report.operation_models import _require_aware_datetime
from autotrade.report.operation_models import default_daily_inspection_items


def build_daily_inspection_report(
    trading_day: date,
    *,
    generated_at: datetime,
    items: Sequence[DailyInspectionItem] | None = None,
) -> DailyInspectionReport:
    _require_aware_datetime("generated_at", generated_at)

    resolved_items = (
        tuple(items) if items is not None else default_daily_inspection_items()
    )
    passed_items = sum(
        1 for item in resolved_items if item.status is InspectionStatus.PASSED
    )
    failed_items = sum(
        1 for item in resolved_items if item.status is InspectionStatus.FAILED
    )
    pending_items = len(resolved_items) - passed_items - failed_items

    window_summaries = []
    for window in INSPECTION_WINDOW_ORDER:
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


def build_weekly_review_report(
    week_start: date,
    *,
    generated_at: datetime,
    daily_run_reports: Sequence[DailyRunReport] = (),
    daily_inspection_reports: Sequence[DailyInspectionReport] = (),
    calendar: KrxRegularSessionCalendar | None = None,
) -> WeeklyReviewReport:
    _require_aware_datetime("generated_at", generated_at)

    week_end = week_start + timedelta(days=6)
    resolved_calendar = calendar or KrxRegularSessionCalendar()
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
    expected_trading_days = tuple(
        current_day
        for offset in range(7)
        if resolved_calendar.is_trading_day(
            current_day := week_start + timedelta(days=offset)
        )
    )
    trading_days = sorted(set(run_reports_by_day) | set(inspection_reports_by_day))

    day_summaries: list[WeeklyReviewDaySummary] = []
    days_with_run_failures: list[date] = []
    repeated_failure_job_counts: dict[str, int] = {}
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
            for result in run_report.job_results:
                if result.success:
                    continue
                repeated_failure_job_counts[result.job_name] = (
                    repeated_failure_job_counts.get(result.job_name, 0) + 1
                )

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

    missing_run_report_days = tuple(
        trading_day
        for trading_day in expected_trading_days
        if trading_day not in run_reports_by_day
    )
    missing_inspection_report_days = tuple(
        trading_day
        for trading_day in expected_trading_days
        if trading_day not in inspection_reports_by_day
    )
    repeated_failure_jobs = tuple(
        sorted(
            job_name
            for job_name, count in repeated_failure_job_counts.items()
            if count >= 2
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
        expected_trading_days=expected_trading_days,
        missing_run_report_days=missing_run_report_days,
        missing_inspection_report_days=missing_inspection_report_days,
        repeated_failure_jobs=repeated_failure_jobs,
        day_summaries=tuple(day_summaries),
    )


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
    for phase in PHASE_ORDER:
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
