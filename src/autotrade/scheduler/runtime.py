from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from enum import StrEnum
from typing import Protocol
from typing import runtime_checkable

from autotrade.data import KST
from autotrade.data import KRX_SESSION_CLOSE
from autotrade.data import KRX_SESSION_OPEN
from autotrade.data import KrxRegularSessionCalendar

_SESSION_DURATION = datetime.combine(
    date(2000, 1, 1),
    KRX_SESSION_CLOSE,
) - datetime.combine(date(2000, 1, 1), KRX_SESSION_OPEN)


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_non_blank_text(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")


class MarketSessionPhase(StrEnum):
    MARKET_OPEN = "market_open"
    INTRADAY = "intraday"
    MARKET_CLOSE = "market_close"


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    intraday_interval: timedelta = timedelta(minutes=30)

    def __post_init__(self) -> None:
        if self.intraday_interval <= timedelta(0):
            raise ValueError("intraday_interval must be positive")
        if self.intraday_interval >= _SESSION_DURATION:
            raise ValueError(
                "intraday_interval must be shorter than the regular session"
            )


@dataclass(frozen=True, slots=True)
class SchedulerRetryPolicy:
    max_attempts: int = 1
    retryable_exceptions: tuple[type[BaseException], ...] = (Exception,)

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if not self.retryable_exceptions:
            raise ValueError("retryable_exceptions must not be empty")


@dataclass(frozen=True, slots=True)
class SessionSlot:
    phase: MarketSessionPhase
    scheduled_at: datetime

    def __post_init__(self) -> None:
        _require_aware_datetime("scheduled_at", self.scheduled_at)


@dataclass(frozen=True, slots=True)
class JobContext:
    phase: MarketSessionPhase
    trading_day: date
    scheduled_at: datetime
    triggered_at: datetime

    def __post_init__(self) -> None:
        _require_aware_datetime("scheduled_at", self.scheduled_at)
        _require_aware_datetime("triggered_at", self.triggered_at)


@runtime_checkable
class ScheduledJobHandler(Protocol):
    def __call__(self, context: JobContext) -> str | None: ...


@dataclass(frozen=True, slots=True)
class ScheduledJob:
    name: str
    phase: MarketSessionPhase
    handler: ScheduledJobHandler

    def __post_init__(self) -> None:
        _require_non_blank_text("name", self.name)


@dataclass(frozen=True, slots=True)
class PendingJob:
    job: ScheduledJob
    phase: MarketSessionPhase
    scheduled_at: datetime

    def __post_init__(self) -> None:
        _require_aware_datetime("scheduled_at", self.scheduled_at)


@dataclass(frozen=True, slots=True)
class ExecutedJobKey:
    job_name: str
    phase: MarketSessionPhase
    scheduled_at: datetime

    def __post_init__(self) -> None:
        _require_non_blank_text("job_name", self.job_name)
        _require_aware_datetime("scheduled_at", self.scheduled_at)


@dataclass(frozen=True, slots=True)
class JobRunResult:
    job_name: str
    phase: MarketSessionPhase
    scheduled_at: datetime
    started_at: datetime
    finished_at: datetime
    success: bool
    detail: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        _require_non_blank_text("job_name", self.job_name)
        _require_aware_datetime("scheduled_at", self.scheduled_at)
        _require_aware_datetime("started_at", self.started_at)
        _require_aware_datetime("finished_at", self.finished_at)
        if self.finished_at < self.started_at:
            raise ValueError("finished_at must not be earlier than started_at")
        if self.success and self.error is not None:
            raise ValueError("successful jobs must not carry an error")
        if not self.success and self.error is None:
            raise ValueError("failed jobs must carry an error")
        if self.detail is not None and not self.detail.strip():
            raise ValueError("detail must not be blank when provided")
        if self.error is not None and not self.error.strip():
            raise ValueError("error must not be blank when provided")


@dataclass(frozen=True, slots=True)
class SchedulerState:
    executed_runs: frozenset[ExecutedJobKey] = frozenset()

    def retain_from(self, trading_day: date) -> SchedulerState:
        return SchedulerState(
            executed_runs=frozenset(
                executed_run
                for executed_run in self.executed_runs
                if executed_run.scheduled_at.astimezone(KST).date() >= trading_day
            )
        )

    def is_executed(
        self,
        *,
        job_name: str,
        phase: MarketSessionPhase,
        scheduled_at: datetime,
    ) -> bool:
        return (
            ExecutedJobKey(
                job_name=job_name,
                phase=phase,
                scheduled_at=scheduled_at,
            )
            in self.executed_runs
        )

    def mark_executed(
        self,
        *,
        job_name: str,
        phase: MarketSessionPhase,
        scheduled_at: datetime,
    ) -> SchedulerState:
        key = ExecutedJobKey(
            job_name=job_name,
            phase=phase,
            scheduled_at=scheduled_at,
        )
        return SchedulerState(executed_runs=self.executed_runs | frozenset({key}))


@dataclass(frozen=True, slots=True)
class SchedulerRun:
    evaluated_at: datetime
    executed_jobs: tuple[JobRunResult, ...]
    next_run_at: datetime
    state: SchedulerState

    def __post_init__(self) -> None:
        _require_aware_datetime("evaluated_at", self.evaluated_at)
        _require_aware_datetime("next_run_at", self.next_run_at)


def build_session_slots(
    trading_day: date,
    *,
    config: SchedulerConfig | None = None,
    calendar: KrxRegularSessionCalendar | None = None,
) -> tuple[SessionSlot, ...]:
    resolved_config = config or SchedulerConfig()
    resolved_calendar = calendar or KrxRegularSessionCalendar()

    if not resolved_calendar.is_trading_day(trading_day):
        return ()

    open_at = datetime.combine(trading_day, KRX_SESSION_OPEN, tzinfo=KST)
    close_at = datetime.combine(trading_day, KRX_SESSION_CLOSE, tzinfo=KST)
    slots = [SessionSlot(phase=MarketSessionPhase.MARKET_OPEN, scheduled_at=open_at)]

    current = open_at + resolved_config.intraday_interval
    while current < close_at:
        slots.append(
            SessionSlot(
                phase=MarketSessionPhase.INTRADAY,
                scheduled_at=current,
            )
        )
        current += resolved_config.intraday_interval

    slots.append(
        SessionSlot(
            phase=MarketSessionPhase.MARKET_CLOSE,
            scheduled_at=close_at,
        )
    )
    return tuple(slots)


def collect_due_jobs(
    jobs: Sequence[ScheduledJob],
    *,
    timestamp: datetime,
    state: SchedulerState | None = None,
    config: SchedulerConfig | None = None,
    calendar: KrxRegularSessionCalendar | None = None,
) -> tuple[PendingJob, ...]:
    _require_aware_datetime("timestamp", timestamp)

    local_timestamp = timestamp.astimezone(KST)
    resolved_state = state or SchedulerState()
    resolved_calendar = calendar or KrxRegularSessionCalendar()

    if not resolved_calendar.is_trading_day(local_timestamp.date()):
        return ()

    slots = build_session_slots(
        local_timestamp.date(),
        config=config,
        calendar=resolved_calendar,
    )
    due_jobs: list[PendingJob] = []
    for slot in slots:
        if slot.scheduled_at > local_timestamp:
            break
        for job in jobs:
            if job.phase != slot.phase:
                continue
            if resolved_state.is_executed(
                job_name=job.name,
                phase=job.phase,
                scheduled_at=slot.scheduled_at,
            ):
                continue
            due_jobs.append(
                PendingJob(
                    job=job,
                    phase=slot.phase,
                    scheduled_at=slot.scheduled_at,
                )
            )
    return tuple(due_jobs)


def next_scheduled_run_at(
    timestamp: datetime,
    *,
    config: SchedulerConfig | None = None,
    calendar: KrxRegularSessionCalendar | None = None,
) -> datetime:
    _require_aware_datetime("timestamp", timestamp)

    local_timestamp = timestamp.astimezone(KST)
    resolved_calendar = calendar or KrxRegularSessionCalendar()
    current_day_slots = build_session_slots(
        local_timestamp.date(),
        config=config,
        calendar=resolved_calendar,
    )
    for slot in current_day_slots:
        if slot.scheduled_at > local_timestamp:
            return slot.scheduled_at

    next_day = local_timestamp.date()
    while True:
        next_day = next_day.fromordinal(next_day.toordinal() + 1)
        if resolved_calendar.is_trading_day(next_day):
            return datetime.combine(next_day, KRX_SESSION_OPEN, tzinfo=KST)


def run_scheduled_jobs(
    jobs: Sequence[ScheduledJob],
    *,
    timestamp: datetime,
    state: SchedulerState | None = None,
    config: SchedulerConfig | None = None,
    calendar: KrxRegularSessionCalendar | None = None,
    clock: Callable[[], datetime] | None = None,
    retry_policy: SchedulerRetryPolicy | None = None,
) -> SchedulerRun:
    _require_aware_datetime("timestamp", timestamp)

    resolved_clock = clock or (lambda: datetime.now(KST))
    resolved_retry_policy = retry_policy or SchedulerRetryPolicy()
    resolved_state = state or SchedulerState()
    due_jobs = collect_due_jobs(
        jobs,
        timestamp=timestamp,
        state=resolved_state,
        config=config,
        calendar=calendar,
    )

    executed_jobs: list[JobRunResult] = []
    updated_state = resolved_state
    next_run_reference = timestamp
    for pending_job in due_jobs:
        started_at = resolved_clock()
        _require_aware_datetime("started_at", started_at)

        detail: str | None = None
        error: str | None = None
        success = False
        attempt_started_at = started_at
        for attempt_index in range(resolved_retry_policy.max_attempts):
            try:
                detail = pending_job.job.handler(
                    JobContext(
                        phase=pending_job.phase,
                        trading_day=pending_job.scheduled_at.astimezone(KST).date(),
                        scheduled_at=pending_job.scheduled_at,
                        triggered_at=attempt_started_at,
                    )
                )
                success = True
                error = None
                break
            except Exception as exc:  # pragma: no cover - exercised in unit tests
                error = str(exc)
                if (
                    attempt_index + 1 < resolved_retry_policy.max_attempts
                    and isinstance(exc, resolved_retry_policy.retryable_exceptions)
                ):
                    attempt_started_at = resolved_clock()
                    _require_aware_datetime("attempt_started_at", attempt_started_at)
                    continue
                break

        finished_at = resolved_clock()
        _require_aware_datetime("finished_at", finished_at)

        result = JobRunResult(
            job_name=pending_job.job.name,
            phase=pending_job.phase,
            scheduled_at=pending_job.scheduled_at,
            started_at=started_at,
            finished_at=finished_at,
            success=success,
            detail=detail,
            error=error,
        )
        executed_jobs.append(result)
        next_run_reference = max(next_run_reference, finished_at)
        # Failed jobs are also marked executed to avoid implicit retries of
        # side-effectful operations. Retry policy belongs above the scheduler.
        updated_state = updated_state.mark_executed(
            job_name=pending_job.job.name,
            phase=pending_job.phase,
            scheduled_at=pending_job.scheduled_at,
        )

    return SchedulerRun(
        evaluated_at=timestamp,
        executed_jobs=tuple(executed_jobs),
        next_run_at=next_scheduled_run_at(
            next_run_reference,
            config=config,
            calendar=calendar,
        ),
        state=updated_state,
    )
