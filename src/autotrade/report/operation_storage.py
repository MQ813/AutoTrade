from __future__ import annotations

import json
from collections.abc import Callable
from collections.abc import Sequence
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import TypeVar

from autotrade.data import KST
from autotrade.report.operation_models import DAILY_INSPECTION_ARCHIVE_DIR
from autotrade.report.operation_models import DAILY_RUN_ARCHIVE_DIR
from autotrade.report.operation_models import JOB_HISTORY_ARCHIVE_DIR
from autotrade.report.operation_models import WEEKLY_REVIEW_ARCHIVE_DIR
from autotrade.report.operation_models import DailyInspectionItem
from autotrade.report.operation_models import DailyInspectionReport
from autotrade.report.operation_models import DailyInspectionWindowSummary
from autotrade.report.operation_models import DailyPhaseSummary
from autotrade.report.operation_models import DailyRunReport
from autotrade.report.operation_models import InspectionStatus
from autotrade.report.operation_models import InspectionWindow
from autotrade.report.operation_models import ReportLogEntry
from autotrade.report.operation_models import WeeklyReviewDaySummary
from autotrade.report.operation_models import WeeklyReviewReport
from autotrade.report.operation_renderers import render_daily_inspection_report
from autotrade.report.operation_renderers import render_daily_run_report
from autotrade.report.operation_renderers import render_run_log
from autotrade.report.operation_renderers import render_weekly_review_report
from autotrade.scheduler import JobRunResult
from autotrade.scheduler import MarketSessionPhase

_T = TypeVar("_T")


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
    _write_json_file(
        _daily_inspection_archive_path(log_dir, report.trading_day),
        _serialize_daily_inspection_report(report),
    )
    return report_path


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
    _write_json_file(
        _weekly_review_archive_path(log_dir, report.week_start, report.week_end),
        _serialize_weekly_review_report(report),
    )
    return report_path


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


def write_daily_run_report(log_dir: Path, report: DailyRunReport) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = (
        log_dir / f"daily_report_{report.generated_at.strftime('%Y%m%d_%H%M%S_%f')}.txt"
    )
    report_path.write_text(render_daily_run_report(report), encoding="utf-8")
    _write_json_file(
        _daily_run_archive_path(log_dir, report.trading_day),
        _serialize_daily_run_report(report),
    )
    return report_path


def append_job_run_result(log_dir: Path, result: JobRunResult) -> Path:
    history_path = _job_history_path(
        log_dir,
        result.scheduled_at.astimezone(KST).date(),
    )
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                _serialize_job_run_result(result),
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        handle.write("\n")
    return history_path


def load_job_run_results(log_dir: Path, trading_day: date) -> tuple[JobRunResult, ...]:
    history_path = _job_history_path(log_dir, trading_day)
    if not history_path.exists():
        return ()
    deduplicated: dict[tuple[str, MarketSessionPhase, datetime], JobRunResult] = {}
    for line in history_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        result = _deserialize_job_run_result(payload)
        key = (result.job_name, result.phase, result.scheduled_at)
        deduplicated[key] = result
    return tuple(
        sorted(
            deduplicated.values(),
            key=lambda result: (result.scheduled_at, result.job_name),
        )
    )


def load_daily_run_report(log_dir: Path, trading_day: date) -> DailyRunReport | None:
    return _load_json_file(
        _daily_run_archive_path(log_dir, trading_day),
        _deserialize_daily_run_report,
    )


def load_daily_run_reports(
    log_dir: Path,
    *,
    start: date | None = None,
    end: date | None = None,
) -> tuple[DailyRunReport, ...]:
    return _load_archived_reports(
        log_dir / DAILY_RUN_ARCHIVE_DIR,
        _deserialize_daily_run_report,
        start=start,
        end=end,
    )


def load_daily_inspection_report(
    log_dir: Path,
    trading_day: date,
) -> DailyInspectionReport | None:
    return _load_json_file(
        _daily_inspection_archive_path(log_dir, trading_day),
        _deserialize_daily_inspection_report,
    )


def load_daily_inspection_reports(
    log_dir: Path,
    *,
    start: date | None = None,
    end: date | None = None,
) -> tuple[DailyInspectionReport, ...]:
    return _load_archived_reports(
        log_dir / DAILY_INSPECTION_ARCHIVE_DIR,
        _deserialize_daily_inspection_report,
        start=start,
        end=end,
    )


def _resolve_generated_at(entries: Sequence[ReportLogEntry]) -> datetime:
    if entries:
        return entries[-1].timestamp
    return datetime.now(KST)


def _write_json_file(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _load_json_file(
    path: Path,
    deserialize: Callable[[object], _T],
) -> _T | None:
    if not path.exists():
        return None
    return deserialize(json.loads(path.read_text(encoding="utf-8")))


def _load_archived_reports(
    archive_dir: Path,
    deserialize: Callable[[object], _T],
    *,
    start: date | None,
    end: date | None,
) -> tuple[_T, ...]:
    if not archive_dir.exists():
        return ()
    reports: list[_T] = []
    for path in sorted(archive_dir.glob("*.json")):
        report = deserialize(json.loads(path.read_text(encoding="utf-8")))
        trading_day = getattr(report, "trading_day", None)
        if isinstance(trading_day, date):
            if start is not None and trading_day < start:
                continue
            if end is not None and trading_day > end:
                continue
        reports.append(report)
    return tuple(reports)


def _serialize_daily_phase_summary(summary: DailyPhaseSummary) -> dict[str, object]:
    return {
        "phase": summary.phase.value,
        "total_jobs": summary.total_jobs,
        "successful_jobs": summary.successful_jobs,
        "failed_jobs": summary.failed_jobs,
    }


def _deserialize_daily_phase_summary(payload: object) -> DailyPhaseSummary:
    summary = _require_mapping(payload, "serialized daily phase summary")
    return DailyPhaseSummary(
        phase=MarketSessionPhase(_require_text(summary.get("phase"), "phase")),
        total_jobs=_require_int(summary.get("total_jobs"), "total_jobs"),
        successful_jobs=_require_int(
            summary.get("successful_jobs"),
            "successful_jobs",
        ),
        failed_jobs=_require_int(summary.get("failed_jobs"), "failed_jobs"),
    )


def _serialize_job_run_result(result: JobRunResult) -> dict[str, object]:
    return {
        "job_name": result.job_name,
        "phase": result.phase.value,
        "scheduled_at": result.scheduled_at.isoformat(),
        "started_at": result.started_at.isoformat(),
        "finished_at": result.finished_at.isoformat(),
        "success": result.success,
        "detail": result.detail,
        "error": result.error,
    }


def _deserialize_job_run_result(payload: object) -> JobRunResult:
    result = _require_mapping(payload, "serialized job run result")
    return JobRunResult(
        job_name=_require_text(result.get("job_name"), "job_name"),
        phase=MarketSessionPhase(_require_text(result.get("phase"), "phase")),
        scheduled_at=_require_datetime(result.get("scheduled_at"), "scheduled_at"),
        started_at=_require_datetime(result.get("started_at"), "started_at"),
        finished_at=_require_datetime(result.get("finished_at"), "finished_at"),
        success=_require_bool(result.get("success"), "success"),
        detail=_require_optional_text(result.get("detail"), "detail"),
        error=_require_optional_text(result.get("error"), "error"),
    )


def _serialize_daily_run_report(report: DailyRunReport) -> dict[str, object]:
    return {
        "trading_day": report.trading_day.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "total_jobs": report.total_jobs,
        "successful_jobs": report.successful_jobs,
        "failed_jobs": report.failed_jobs,
        "phase_summaries": [
            _serialize_daily_phase_summary(summary)
            for summary in report.phase_summaries
        ],
        "job_results": [
            _serialize_job_run_result(result) for result in report.job_results
        ],
    }


def _deserialize_daily_run_report(payload: object) -> DailyRunReport:
    report = _require_mapping(payload, "serialized daily run report")
    return DailyRunReport(
        trading_day=_require_date(report.get("trading_day"), "trading_day"),
        generated_at=_require_datetime(report.get("generated_at"), "generated_at"),
        total_jobs=_require_int(report.get("total_jobs"), "total_jobs"),
        successful_jobs=_require_int(
            report.get("successful_jobs"),
            "successful_jobs",
        ),
        failed_jobs=_require_int(report.get("failed_jobs"), "failed_jobs"),
        phase_summaries=tuple(
            _deserialize_daily_phase_summary(item)
            for item in _require_list(report.get("phase_summaries"), "phase_summaries")
        ),
        job_results=tuple(
            _deserialize_job_run_result(item)
            for item in _require_list(report.get("job_results"), "job_results")
        ),
    )


def _serialize_daily_inspection_item(
    item: DailyInspectionItem,
) -> dict[str, object]:
    return {
        "window": item.window.value,
        "label": item.label,
        "status": item.status.value,
        "detail": item.detail,
    }


def _deserialize_daily_inspection_item(payload: object) -> DailyInspectionItem:
    item = _require_mapping(payload, "serialized daily inspection item")
    return DailyInspectionItem(
        window=InspectionWindow(_require_text(item.get("window"), "window")),
        label=_require_text(item.get("label"), "label"),
        status=InspectionStatus(_require_text(item.get("status"), "status")),
        detail=_require_optional_text(item.get("detail"), "detail"),
    )


def _serialize_daily_inspection_window_summary(
    summary: DailyInspectionWindowSummary,
) -> dict[str, object]:
    return {
        "window": summary.window.value,
        "total_items": summary.total_items,
        "passed_items": summary.passed_items,
        "failed_items": summary.failed_items,
        "pending_items": summary.pending_items,
    }


def _deserialize_daily_inspection_window_summary(
    payload: object,
) -> DailyInspectionWindowSummary:
    summary = _require_mapping(payload, "serialized inspection window summary")
    return DailyInspectionWindowSummary(
        window=InspectionWindow(_require_text(summary.get("window"), "window")),
        total_items=_require_int(summary.get("total_items"), "total_items"),
        passed_items=_require_int(summary.get("passed_items"), "passed_items"),
        failed_items=_require_int(summary.get("failed_items"), "failed_items"),
        pending_items=_require_int(summary.get("pending_items"), "pending_items"),
    )


def _serialize_daily_inspection_report(
    report: DailyInspectionReport,
) -> dict[str, object]:
    return {
        "trading_day": report.trading_day.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "total_items": report.total_items,
        "passed_items": report.passed_items,
        "failed_items": report.failed_items,
        "pending_items": report.pending_items,
        "window_summaries": [
            _serialize_daily_inspection_window_summary(summary)
            for summary in report.window_summaries
        ],
        "items": [_serialize_daily_inspection_item(item) for item in report.items],
    }


def _deserialize_daily_inspection_report(payload: object) -> DailyInspectionReport:
    report = _require_mapping(payload, "serialized daily inspection report")
    return DailyInspectionReport(
        trading_day=_require_date(report.get("trading_day"), "trading_day"),
        generated_at=_require_datetime(report.get("generated_at"), "generated_at"),
        total_items=_require_int(report.get("total_items"), "total_items"),
        passed_items=_require_int(report.get("passed_items"), "passed_items"),
        failed_items=_require_int(report.get("failed_items"), "failed_items"),
        pending_items=_require_int(report.get("pending_items"), "pending_items"),
        window_summaries=tuple(
            _deserialize_daily_inspection_window_summary(item)
            for item in _require_list(
                report.get("window_summaries"), "window_summaries"
            )
        ),
        items=tuple(
            _deserialize_daily_inspection_item(item)
            for item in _require_list(report.get("items"), "items")
        ),
    )


def _serialize_weekly_review_day_summary(
    summary: WeeklyReviewDaySummary,
) -> dict[str, object]:
    return {
        "trading_day": summary.trading_day.isoformat(),
        "total_jobs": summary.total_jobs,
        "failed_jobs": summary.failed_jobs,
        "passed_inspection_items": summary.passed_inspection_items,
        "failed_inspection_items": summary.failed_inspection_items,
        "pending_inspection_items": summary.pending_inspection_items,
    }


def _serialize_weekly_review_report(report: WeeklyReviewReport) -> dict[str, object]:
    return {
        "week_start": report.week_start.isoformat(),
        "week_end": report.week_end.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "total_jobs": report.total_jobs,
        "failed_jobs": report.failed_jobs,
        "passed_inspection_items": report.passed_inspection_items,
        "failed_inspection_items": report.failed_inspection_items,
        "pending_inspection_items": report.pending_inspection_items,
        "days_with_run_failures": [
            trading_day.isoformat() for trading_day in report.days_with_run_failures
        ],
        "expected_trading_days": [
            trading_day.isoformat() for trading_day in report.expected_trading_days
        ],
        "missing_run_report_days": [
            trading_day.isoformat() for trading_day in report.missing_run_report_days
        ],
        "missing_inspection_report_days": [
            trading_day.isoformat()
            for trading_day in report.missing_inspection_report_days
        ],
        "repeated_failure_jobs": list(report.repeated_failure_jobs),
        "day_summaries": [
            _serialize_weekly_review_day_summary(summary)
            for summary in report.day_summaries
        ],
    }


def _daily_run_archive_path(log_dir: Path, trading_day: date) -> Path:
    return log_dir / DAILY_RUN_ARCHIVE_DIR / f"{trading_day.isoformat()}.json"


def _daily_inspection_archive_path(log_dir: Path, trading_day: date) -> Path:
    return log_dir / DAILY_INSPECTION_ARCHIVE_DIR / f"{trading_day.isoformat()}.json"


def _weekly_review_archive_path(
    log_dir: Path,
    week_start: date,
    week_end: date,
) -> Path:
    return (
        log_dir
        / WEEKLY_REVIEW_ARCHIVE_DIR
        / f"{week_start.isoformat()}_{week_end.isoformat()}.json"
    )


def _job_history_path(log_dir: Path, trading_day: date) -> Path:
    return log_dir / JOB_HISTORY_ARCHIVE_DIR / f"{trading_day.isoformat()}.jsonl"


def _require_mapping(payload: object, field_name: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return payload


def _require_list(payload: object, field_name: str) -> list[object]:
    if not isinstance(payload, list):
        raise ValueError(f"{field_name} must be a list")
    return payload


def _require_text(payload: object, field_name: str) -> str:
    if not isinstance(payload, str) or not payload.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    return payload


def _require_optional_text(payload: object, field_name: str) -> str | None:
    if payload is None:
        return None
    return _require_text(payload, field_name)


def _require_bool(payload: object, field_name: str) -> bool:
    if not isinstance(payload, bool):
        raise ValueError(f"{field_name} must be a bool")
    return payload


def _require_int(payload: object, field_name: str) -> int:
    if not isinstance(payload, int) or isinstance(payload, bool):
        raise ValueError(f"{field_name} must be an int")
    return payload


def _require_date(payload: object, field_name: str) -> date:
    return date.fromisoformat(_require_text(payload, field_name))


def _require_datetime(payload: object, field_name: str) -> datetime:
    return datetime.fromisoformat(_require_text(payload, field_name))
