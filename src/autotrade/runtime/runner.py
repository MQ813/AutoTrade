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
from autotrade.runtime.control import RunnerControlMode
from autotrade.runtime.control import RunnerControlState
from autotrade.runtime.control import RunnerControlStore
from autotrade.scheduler import build_session_slots
from autotrade.scheduler import JobRunResult
from autotrade.scheduler import ScheduledJob
from autotrade.scheduler import SchedulerConfig
from autotrade.scheduler import SchedulerRetryPolicy
from autotrade.scheduler import SchedulerRun
from autotrade.scheduler import SchedulerState
from autotrade.scheduler import SchedulerStateStore
from autotrade.scheduler import run_scheduled_jobs

logger = logging.getLogger(__name__)
_CONTROL_POLLER_FAILURE_LOG_INTERVAL_SECONDS = 300.0


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
ControlPoller = Callable[[], None]


@dataclass(frozen=True, slots=True)
class ResumeContext:
    paused_at: datetime
    resumed_at: datetime
    source: str | None
    state: RunnerControlState
    runs: tuple[SchedulerRun, ...]

    def __post_init__(self) -> None:
        _require_aware_datetime("paused_at", self.paused_at)
        _require_aware_datetime("resumed_at", self.resumed_at)
        if self.source is not None:
            _require_non_blank_text("source", self.source)


ResumeHandler = Callable[[ResumeContext], str | None]


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
    control_store: RunnerControlStore | None = None
    control_poller: ControlPoller | None = None
    resume_handler: ResumeHandler | None = None
    control_poll_interval_seconds: float = 5.0
    _last_handled_resume_at: datetime | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _skip_scheduled_at_through: datetime | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _last_control_poller_error: str | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _last_control_poller_error_logged_at: datetime | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def run_once(self, *, timestamp: datetime | None = None) -> SchedulerRun:
        evaluated_at = timestamp or self.clock()
        state = self._state_with_skipped_controlled_slots(
            self._load_state_for_day(evaluated_at),
            evaluated_at=evaluated_at,
        )
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
        if self.control_poll_interval_seconds <= 0:
            raise ValueError("control_poll_interval_seconds must be positive")

        runs: list[SchedulerRun] = []
        self._initialize_handled_resume_marker()
        while max_iterations is None or len(runs) < max_iterations:
            try:
                control_result = self._handle_control_if_needed(runs=tuple(runs))
                if control_result is not None:
                    return control_result

                run = self.run_once()
                runs.append(run)

                failures = tuple(
                    result for result in run.executed_jobs if not result.success
                )
                if failures and self.stop_on_job_failure:
                    stop_reason = _failed_jobs_reason(failures)
                    safe_stop_at = self.clock()
                    logger.error(
                        "실패한 job이 있어 runner를 안전 정지합니다. %s",
                        stop_reason,
                    )
                    return self._build_safe_stop_result(
                        triggered_at=safe_stop_at,
                        reason="job_failure",
                        detail=stop_reason,
                        runs=tuple(runs),
                        failures=failures,
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
                return self._build_safe_stop_result(
                    triggered_at=safe_stop_at,
                    reason="runner_exception",
                    detail=stop_reason,
                    runs=tuple(runs),
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
        if self.control_store is None and self.control_poller is None:
            self.sleep(wait_seconds)
            return

        while wait_seconds > 0:
            state = self._poll_and_load_control_state()
            if state is not None and state.mode is RunnerControlMode.PAUSED:
                logger.info(
                    "runner control pause 상태를 감지해 다음 scheduler 실행 대기를 중단합니다."
                )
                return
            self.sleep(min(wait_seconds, self.control_poll_interval_seconds))
            current = self.clock()
            wait_seconds = max(0.0, (wake_at - current).total_seconds())

    def _initialize_handled_resume_marker(self) -> None:
        if self._last_handled_resume_at is not None or self.control_store is None:
            return
        state = self.control_store.load()
        if state.mode is RunnerControlMode.RUNNING:
            self._last_handled_resume_at = state.resumed_at

    def _handle_control_if_needed(
        self,
        *,
        runs: tuple[SchedulerRun, ...],
    ) -> RunnerResult | None:
        state = self._poll_and_load_control_state()
        if state is None:
            return None
        if state.mode is RunnerControlMode.PAUSED:
            return self._wait_until_resumed(paused_state=state, runs=runs)
        return self._run_resume_handler_if_needed(state=state, runs=runs)

    def _wait_until_resumed(
        self,
        *,
        paused_state: RunnerControlState,
        runs: tuple[SchedulerRun, ...],
    ) -> RunnerResult | None:
        assert paused_state.paused_at is not None
        logger.info(
            "runner가 pause 상태로 대기합니다. paused_at=%s source=%s",
            paused_state.paused_at.isoformat(),
            paused_state.paused_by,
        )
        state = paused_state
        while state.mode is RunnerControlMode.PAUSED:
            self.sleep(self.control_poll_interval_seconds)
            loaded_state = self._poll_and_load_control_state()
            if loaded_state is None:
                return None
            state = loaded_state
        return self._run_resume_handler_if_needed(state=state, runs=runs)

    def _run_resume_handler_if_needed(
        self,
        *,
        state: RunnerControlState,
        runs: tuple[SchedulerRun, ...],
    ) -> RunnerResult | None:
        resumed_at = state.resumed_at
        if resumed_at is None:
            return None
        if resumed_at == self._last_handled_resume_at:
            return None

        paused_at = state.paused_at or resumed_at
        self._skip_scheduled_at_through = resumed_at
        try:
            if self.resume_handler is not None:
                detail = self.resume_handler(
                    ResumeContext(
                        paused_at=paused_at,
                        resumed_at=resumed_at,
                        source=state.resumed_by,
                        state=state,
                        runs=runs,
                    )
                )
                if detail is not None:
                    logger.info("resume maintenance를 완료했습니다. %s", detail)
        except Exception as exc:  # pragma: no cover - exercised in unit tests
            stop_reason = str(exc)
            safe_stop_at = self.clock()
            logger.exception("resume maintenance 중 예외가 발생해 안전 정지합니다.")
            return self._build_safe_stop_result(
                triggered_at=safe_stop_at,
                reason="resume_maintenance_failure",
                detail=stop_reason,
                runs=runs,
            )

        self._last_handled_resume_at = resumed_at
        return None

    def _poll_and_load_control_state(self) -> RunnerControlState | None:
        if self.control_poller is not None:
            try:
                self.control_poller()
            except Exception as exc:
                self._log_control_poller_failure(exc)
        if self.control_store is None:
            return None
        return self.control_store.load()

    def _log_control_poller_failure(self, error: Exception) -> None:
        error_message = str(error) or type(error).__name__
        logged_at = self._last_control_poller_error_logged_at
        now = self.clock()
        if (
            error_message == self._last_control_poller_error
            and logged_at is not None
            and (now - logged_at).total_seconds()
            < _CONTROL_POLLER_FAILURE_LOG_INTERVAL_SECONDS
        ):
            return
        self._last_control_poller_error = error_message
        self._last_control_poller_error_logged_at = now
        logger.warning(
            "runner control poller 실행에 실패했습니다. error=%s", error_message
        )

    def _state_with_skipped_controlled_slots(
        self,
        state: SchedulerState,
        *,
        evaluated_at: datetime,
    ) -> SchedulerState:
        skip_through = self._skip_scheduled_at_through
        if skip_through is None:
            return state
        local_day = evaluated_at.astimezone(KST).date()
        updated_state = state
        for slot in build_session_slots(
            local_day,
            config=self.scheduler_config,
            calendar=self.calendar,
        ):
            if slot.scheduled_at > skip_through:
                break
            for job in self.jobs:
                if job.phase is not slot.phase:
                    continue
                if updated_state.is_executed(
                    job_name=job.name,
                    phase=job.phase,
                    scheduled_at=slot.scheduled_at,
                ):
                    continue
                updated_state = updated_state.mark_executed(
                    job_name=job.name,
                    phase=job.phase,
                    scheduled_at=slot.scheduled_at,
                )
                logger.info(
                    "pause window에 포함된 scheduler 슬롯을 건너뜁니다. job=%s phase=%s scheduled_at=%s",
                    job.name,
                    job.phase.value,
                    slot.scheduled_at.isoformat(),
                )
        return updated_state

    def _build_safe_stop_result(
        self,
        *,
        triggered_at: datetime,
        reason: str,
        detail: str,
        runs: tuple[SchedulerRun, ...],
        failures: tuple[JobRunResult, ...] = (),
    ) -> RunnerResult:
        cleanup_detail = self._run_safe_stop_handler(
            triggered_at=triggered_at,
            reason=reason,
            detail=detail,
            runs=runs,
            failures=failures,
        )
        self.notifier.send(
            build_safe_stop_notification(
                created_at=triggered_at,
                reason=reason,
                detail=_compose_safe_stop_detail(
                    detail,
                    cleanup_detail=cleanup_detail,
                ),
            )
        )
        return RunnerResult(
            status=RunnerStatus.SAFE_STOP,
            runs=runs,
            stop_reason=detail,
        )

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
