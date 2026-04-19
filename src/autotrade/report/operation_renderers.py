from __future__ import annotations

from collections.abc import Sequence

from autotrade.report.operation_models import DailyInspectionReport
from autotrade.report.operation_models import DailyRunReport
from autotrade.report.operation_models import ReportLogEntry
from autotrade.report.operation_models import WeeklyReviewReport


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
    if report.expected_trading_days:
        lines.append(
            "expected_trading_days="
            + ",".join(day.isoformat() for day in report.expected_trading_days)
        )
    if report.days_with_run_failures:
        lines.append(
            "days_with_run_failures="
            + ",".join(day.isoformat() for day in report.days_with_run_failures)
        )
    if report.missing_run_report_days:
        lines.append(
            "missing_run_report_days="
            + ",".join(day.isoformat() for day in report.missing_run_report_days)
        )
    if report.missing_inspection_report_days:
        lines.append(
            "missing_inspection_report_days="
            + ",".join(day.isoformat() for day in report.missing_inspection_report_days)
        )
    if report.repeated_failure_jobs:
        lines.append("repeated_failure_jobs=" + ",".join(report.repeated_failure_jobs))
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
            (
                "review_prompt="
                f"주간 총 job={report.total_jobs} 실패 job={report.failed_jobs} "
                f"실패 일수={len(report.days_with_run_failures)}일이다. "
                "실패 원인을 하나의 운영 이슈 목록으로 정리했는가?"
            ),
            (
                "review_prompt="
                f"점검 실패={report.failed_inspection_items} "
                f"점검 대기={report.pending_inspection_items} "
                f"점검 리포트 누락 일수={len(report.missing_inspection_report_days)}일이다. "
                "누락과 반복 수동 작업을 제거할 계획이 있는가?"
            ),
            (
                "review_prompt="
                f"반복 오류 job={','.join(report.repeated_failure_jobs) or 'none'} "
                f"리포트 누락 일수={len(report.missing_run_report_days)}일이다. "
                "다음 주 조정 사항과 재현 절차를 문서에 남겼는가?"
            ),
        )
    )
    return "\n".join(lines) + "\n"


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
