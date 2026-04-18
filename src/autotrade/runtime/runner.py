from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from enum import StrEnum
import logging
from pathlib import Path
import time

from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.report import AlertSeverity
from autotrade.report import NotificationMessage
from autotrade.report import Notifier
from autotrade.report import append_job_run_result
from autotrade.report import build_run_log_entries
from autotrade.report import write_run_log
from autotrade.scheduler import JobRunResult
from autotrade.scheduler import ScheduledJob
from autotrade.scheduler import SchedulerConfig
from autotrade.scheduler import SchedulerRetryPolicy
from autotrade.scheduler import SchedulerRun
from autotrade.scheduler import SchedulerState
from autotrade.scheduler import SchedulerStateStore
from autotrade.scheduler import run_scheduled_jobs

logger = logging.getLogger(__name__)


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_non_blank_text(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")


class RunnerStatus(StrEnum):
    COMPLETED = "completed"
    STOPPED = "stopped"
    SAFE_STOP = "safe_stop"


@dataclass(frozen=True, slots=True)
class RunnerResult:
    status: RunnerStatus
    runs: tuple[SchedulerRun, ...]
    stop_reason: str | None = None


@dataclass(frozen=True, slots=True)
class SafeStopContext:
    triggered_at: datetime
    trading_day: date
    reason: str
    detail: str
    runs: tuple[SchedulerRun, ...]
    last_run: SchedulerRun | None = None
    failures: tuple[JobRunResult, ...] = ()

    def __post_init__(self) -> None:
        _require_aware_datetime("triggered_at", self.triggered_at)
        _require_non_blank_text("reason", self.reason)
        _require_non_blank_text("detail", self.detail)


SafeStopHandler = Callable[[SafeStopContext], str | None]


@dataclass(slots=True)
class ScheduledRunner:
    jobs: Sequence[ScheduledJob]
    state_store: SchedulerStateStore
    notifier: Notifier
    scheduler_config: SchedulerConfig | None = None
    scheduler_retry_policy: SchedulerRetryPolicy | None = None
    calendar: KrxRegularSessionCalendar | None = None
    log_dir: Path | None = None
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(KST))
    sleep: Callable[[float], None] = field(default=time.sleep, repr=False)
    stop_on_job_failure: bool = True
    safe_stop_handler: SafeStopHandler | None = None

    def run_once(self, *, timestamp: datetime | None = None) -> SchedulerRun:
        evaluated_at = timestamp or self.clock()
        state = self._load_state_for_day(evaluated_at)
        logger.info(
            "운영 runner가 scheduler를 평가합니다. 시각=%s 보관된실행수=%d",
            evaluated_at.isoformat(),
            len(state.executed_runs),
        )
        run = run_scheduled_jobs(
            self.jobs,
            timestamp=evaluated_at,
            state=state,
            config=self.scheduler_config,
            calendar=self.calendar,
            clock=self.clock,
            retry_policy=self.scheduler_retry_policy,
        )
        persisted_state = run.state.retain_from(_trading_day(evaluated_at))
        self.state_store.save(persisted_state)
        if self.log_dir is not None and run.executed_jobs:
            for result in run.executed_jobs:
                append_job_run_result(self.log_dir, result)
            write_run_log(
                self.log_dir,
                build_run_log_entries(run.executed_jobs),
            )
        logger.info(
            "운영 runner가 scheduler 평가를 마쳤습니다. 실행job수=%d 다음실행=%s",
            len(run.executed_jobs),
            run.next_run_at.isoformat(),
        )
        return SchedulerRun(
            evaluated_at=run.evaluated_at,
            executed_jobs=run.executed_jobs,
            next_run_at=run.next_run_at,
            state=persisted_state,
        )

    def run_forever(
        self,
        *,
        max_iterations: int | None = None,
    ) -> RunnerResult:
        if max_iterations is not None and max_iterations <= 0:
            raise ValueError("max_iterations must be positive")

        runs: list[SchedulerRun] = []
        while max_iterations is None or len(runs) < max_iterations:
            try:
                run = self.run_once()
                runs.append(run)

                failures = tuple(
                    result for result in run.executed_jobs if not result.success
                )
                if failures and self.stop_on_job_failure:
                    stop_reason = _failed_jobs_reason(failures)
                    safe_stop_at = self.clock()
                    cleanup_detail = self._run_safe_stop_handler(
                        triggered_at=safe_stop_at,
                        reason="job_failure",
                        detail=stop_reason,
                        runs=tuple(runs),
                        failures=failures,
                    )
                    self.notifier.send(
                        build_safe_stop_notification(
                            created_at=safe_stop_at,
                            reason="job_failure",
                            detail=_compose_safe_stop_detail(
                                stop_reason,
                                cleanup_detail=cleanup_detail,
                            ),
                        )
                    )
                    logger.error(
                        "실패한 job이 있어 runner를 안전 정지합니다. %s",
                        stop_reason,
                    )
                    return RunnerResult(
                        status=RunnerStatus.SAFE_STOP,
                        runs=tuple(runs),
                        stop_reason=stop_reason,
                    )

                if max_iterations is not None and len(runs) >= max_iterations:
                    break

                self._sleep_until(run.next_run_at)
            except KeyboardInterrupt:
                logger.info("사용자 요청으로 운영 runner를 종료합니다.")
                return RunnerResult(
                    status=RunnerStatus.STOPPED,
                    runs=tuple(runs),
                    stop_reason="keyboard interrupt",
                )
            except Exception as exc:  # pragma: no cover - exercised in unit tests
                stop_reason = str(exc)
                safe_stop_at = self.clock()
                logger.exception("운영 runner에서 예외가 발생해 안전 정지합니다.")
                cleanup_detail = self._run_safe_stop_handler(
                    triggered_at=safe_stop_at,
                    reason="runner_exception",
                    detail=stop_reason,
                    runs=tuple(runs),
                )
                self.notifier.send(
                    build_safe_stop_notification(
                        created_at=safe_stop_at,
                        reason="runner_exception",
                        detail=_compose_safe_stop_detail(
                            stop_reason,
                            cleanup_detail=cleanup_detail,
                        ),
                    )
                )
                return RunnerResult(
                    status=RunnerStatus.SAFE_STOP,
                    runs=tuple(runs),
                    stop_reason=stop_reason,
                )

        return RunnerResult(status=RunnerStatus.COMPLETED, runs=tuple(runs))

    def _load_state_for_day(self, evaluated_at: datetime) -> SchedulerState:
        return self.state_store.load().retain_from(_trading_day(evaluated_at))

    def _sleep_until(self, wake_at: datetime) -> None:
        current = self.clock()
        wait_seconds = max(0.0, (wake_at - current).total_seconds())
        logger.info(
            "다음 실행까지 대기합니다. 현재=%s 다음=%s 대기초=%.3f",
            current.isoformat(),
            wake_at.isoformat(),
            wait_seconds,
        )
        self.sleep(wait_seconds)

    def _run_safe_stop_handler(
        self,
        *,
        triggered_at: datetime,
        reason: str,
        detail: str,
        runs: tuple[SchedulerRun, ...],
        failures: tuple[JobRunResult, ...] = (),
    ) -> str | None:
        if self.safe_stop_handler is None:
            return None
        context = SafeStopContext(
            triggered_at=triggered_at,
            trading_day=_trading_day(triggered_at),
            reason=reason,
            detail=detail,
            runs=runs,
            last_run=runs[-1] if runs else None,
            failures=failures,
        )
        try:
            cleanup_detail = self.safe_stop_handler(context)
        except Exception as exc:  # pragma: no cover - exercised in unit tests
            logger.exception("safe stop 종료 정리 훅 실행에 실패했습니다.")
            return f"cleanup_hook_failed:{exc}"
        if cleanup_detail is not None:
            logger.info("safe stop 종료 정리 훅을 완료했습니다. %s", cleanup_detail)
        return cleanup_detail


def build_safe_stop_notification(
    *,
    created_at: datetime,
    reason: str,
    detail: str,
) -> NotificationMessage:
    return NotificationMessage(
        created_at=created_at,
        severity=AlertSeverity.ERROR,
        subject=f"AutoTrade runner safe stop [{reason}]",
        body="\n".join(
            [
                f"reason={reason}",
                f"detail={detail}",
            ]
        ),
    )


def _compose_safe_stop_detail(
    detail: str,
    *,
    cleanup_detail: str | None,
) -> str:
    if cleanup_detail is None:
        return detail
    return f"{detail}\ncleanup_detail={cleanup_detail}"


def _failed_jobs_reason(failures: Sequence[JobRunResult]) -> str:
    return ",".join(
        (
            f"{result.job_name}:{result.phase.value}:"
            f"{result.scheduled_at.astimezone(KST).isoformat()}"
        )
        for result in failures
    )


def _trading_day(timestamp: datetime) -> date:
    return timestamp.astimezone(KST).date()
