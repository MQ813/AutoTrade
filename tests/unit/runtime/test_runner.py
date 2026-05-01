from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from datetime import timedelta

from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.report import NotificationMessage
from autotrade.runtime import RunnerStatus
from autotrade.runtime import ResumeContext
from autotrade.runtime.runner import SafeStopContext
from autotrade.runtime import ScheduledRunner
from autotrade.runtime.control import FileRunnerControlStore
from autotrade.scheduler import FileSchedulerStateStore
from autotrade.scheduler import MarketSessionPhase
from autotrade.scheduler import ScheduledJob
from autotrade.scheduler import SchedulerState


def test_scheduled_runner_safe_stops_and_notifies_on_job_failure(tmp_path) -> None:
    state_store = FileSchedulerStateStore(tmp_path / "scheduler_state.json")
    notifier = RecordingNotifier()
    runner = ScheduledRunner(
        jobs=(
            ScheduledJob(
                name="live_cycle",
                phase=MarketSessionPhase.MARKET_OPEN,
                handler=lambda context: (_ for _ in ()).throw(RuntimeError("boom")),
            ),
        ),
        state_store=state_store,
        notifier=notifier,
        clock=AdjustableClock(datetime(2026, 4, 10, 9, 0, tzinfo=KST)),
    )

    result = runner.run_forever(max_iterations=3)

    assert result.status is RunnerStatus.SAFE_STOP
    assert result.stop_reason == "live_cycle:market_open:2026-04-10T08:00:00+09:00"
    assert len(notifier.notifications) == 1
    assert notifier.notifications[0].subject == (
        "AutoTrade runner safe stop [job_failure]"
    )
    assert state_store.load().is_executed(
        job_name="live_cycle",
        phase=MarketSessionPhase.MARKET_OPEN,
        scheduled_at=datetime(2026, 4, 10, 8, 0, tzinfo=KST),
    )


def test_scheduled_runner_skips_same_day_slot_already_persisted(tmp_path) -> None:
    state_store = FileSchedulerStateStore(tmp_path / "scheduler_state.json")
    state_store.save(
        SchedulerState().mark_executed(
            job_name="live_cycle",
            phase=MarketSessionPhase.INTRADAY,
            scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
        )
    )
    calls: list[datetime] = []
    runner = ScheduledRunner(
        jobs=(
            ScheduledJob(
                name="live_cycle",
                phase=MarketSessionPhase.INTRADAY,
                handler=lambda context: calls.append(context.scheduled_at) or "ok",
            ),
        ),
        state_store=state_store,
        notifier=RecordingNotifier(),
        clock=AdjustableClock(datetime(2026, 4, 10, 9, 45, tzinfo=KST)),
    )

    run = runner.run_once()

    assert calls == []
    assert run.executed_jobs == ()
    assert run.next_run_at == datetime(2026, 4, 10, 10, 0, tzinfo=KST)


def test_scheduled_runner_sleeps_to_next_trading_day_after_holiday(tmp_path) -> None:
    state_store = FileSchedulerStateStore(tmp_path / "scheduler_state.json")
    state_store.save(
        SchedulerState().mark_executed(
            job_name="live_cycle",
            phase=MarketSessionPhase.MARKET_OPEN,
            scheduled_at=datetime(2026, 4, 10, 8, 0, tzinfo=KST),
        )
    )
    clock = AdjustableClock(datetime(2026, 4, 10, 15, 30, tzinfo=KST))
    sleep_calls: list[float] = []
    calls: list[datetime] = []
    calendar = KrxRegularSessionCalendar(holiday_dates=frozenset({date(2026, 4, 13)}))

    def sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock.current = clock.current + timedelta(seconds=seconds)

    runner = ScheduledRunner(
        jobs=(
            ScheduledJob(
                name="live_cycle",
                phase=MarketSessionPhase.MARKET_OPEN,
                handler=lambda context: calls.append(context.scheduled_at) or "ok",
            ),
        ),
        state_store=state_store,
        notifier=RecordingNotifier(),
        calendar=calendar,
        clock=clock,
        sleep=sleep,
    )

    result = runner.run_forever(max_iterations=2)

    assert result.status is RunnerStatus.COMPLETED
    assert calls == [datetime(2026, 4, 14, 8, 0, tzinfo=KST)]
    assert sleep_calls == [318600.0]


def test_scheduled_runner_writes_operations_log_for_executed_jobs(tmp_path) -> None:
    state_store = FileSchedulerStateStore(tmp_path / "scheduler_state.json")
    runner = ScheduledRunner(
        jobs=(
            ScheduledJob(
                name="prepare",
                phase=MarketSessionPhase.MARKET_OPEN,
                handler=lambda context: "prepared",
            ),
        ),
        state_store=state_store,
        notifier=RecordingNotifier(),
        log_dir=tmp_path / "logs",
        clock=AdjustableClock(datetime(2026, 4, 10, 9, 0, tzinfo=KST)),
    )

    run = runner.run_once()

    assert len(run.executed_jobs) == 1
    log_paths = tuple(sorted((tmp_path / "logs").glob("operations_*.log")))
    history_paths = tuple(sorted((tmp_path / "logs" / "job_history").glob("*.jsonl")))
    assert len(log_paths) == 1
    assert len(history_paths) == 1
    assert "source=prepare" in log_paths[0].read_text(encoding="utf-8")
    assert '"job_name": "prepare"' in history_paths[0].read_text(encoding="utf-8")


def test_scheduled_runner_runs_safe_stop_handler_on_job_failure(tmp_path) -> None:
    state_store = FileSchedulerStateStore(tmp_path / "scheduler_state.json")
    notifier = RecordingNotifier()
    contexts: list[SafeStopContext] = []
    runner = ScheduledRunner(
        jobs=(
            ScheduledJob(
                name="live_cycle",
                phase=MarketSessionPhase.INTRADAY,
                handler=lambda context: (_ for _ in ()).throw(RuntimeError("boom")),
            ),
        ),
        state_store=state_store,
        notifier=notifier,
        clock=AdjustableClock(datetime(2026, 4, 10, 9, 30, tzinfo=KST)),
        safe_stop_handler=lambda context: contexts.append(context) or "cleanup_done",
    )

    result = runner.run_forever(max_iterations=1)

    assert result.status is RunnerStatus.SAFE_STOP
    assert len(contexts) == 1
    assert contexts[0].reason == "job_failure"
    assert contexts[0].detail == "live_cycle:intraday:2026-04-10T09:30:00+09:00"
    assert contexts[0].trading_day == date(2026, 4, 10)
    assert len(contexts[0].runs) == 1
    assert len(contexts[0].failures) == 1
    assert "cleanup_detail=cleanup_done" in notifier.notifications[0].body


def test_scheduled_runner_runs_safe_stop_handler_on_runner_exception(
    tmp_path,
) -> None:
    state_store = FileSchedulerStateStore(tmp_path / "scheduler_state.json")
    notifier = RecordingNotifier()
    contexts: list[SafeStopContext] = []
    runner = ScheduledRunner(
        jobs=(
            ScheduledJob(
                name="prepare",
                phase=MarketSessionPhase.MARKET_OPEN,
                handler=lambda context: "prepared",
            ),
        ),
        state_store=state_store,
        notifier=notifier,
        clock=AdjustableClock(datetime(2026, 4, 10, 9, 0, tzinfo=KST)),
        sleep=lambda seconds: (_ for _ in ()).throw(RuntimeError("runner blew up")),
        safe_stop_handler=lambda context: contexts.append(context) or "cleanup_done",
    )

    result = runner.run_forever(max_iterations=2)

    assert result.status is RunnerStatus.SAFE_STOP
    assert result.stop_reason == "runner blew up"
    assert len(contexts) == 1
    assert contexts[0].reason == "runner_exception"
    assert contexts[0].detail == "runner blew up"
    assert contexts[0].failures == ()
    assert len(contexts[0].runs) == 1
    assert "cleanup_detail=cleanup_done" in notifier.notifications[0].body


def test_scheduled_runner_pauses_resumes_and_skips_paused_slots(tmp_path) -> None:
    state_store = FileSchedulerStateStore(tmp_path / "scheduler_state.json")
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    clock = AdjustableClock(datetime(2026, 4, 10, 9, 0, tzinfo=KST))
    calls: list[datetime] = []
    resume_contexts: list[ResumeContext] = []
    sleep_calls = 0

    def sleep(seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            control_store.pause(
                timestamp=datetime(2026, 4, 10, 9, 5, tzinfo=KST),
                source="cli",
            )
            clock.current = datetime(2026, 4, 10, 9, 5, tzinfo=KST)
            return
        if sleep_calls == 2:
            control_store.resume(
                timestamp=datetime(2026, 4, 10, 10, 15, tzinfo=KST),
                source="cli",
            )
            clock.current = datetime(2026, 4, 10, 10, 15, tzinfo=KST)
            return
        clock.current = clock.current + timedelta(seconds=seconds)

    runner = ScheduledRunner(
        jobs=(
            ScheduledJob(
                name="live_cycle",
                phase=MarketSessionPhase.INTRADAY,
                handler=lambda context: calls.append(context.scheduled_at) or "ok",
            ),
        ),
        state_store=state_store,
        notifier=RecordingNotifier(),
        clock=clock,
        sleep=sleep,
        control_store=control_store,
        control_poll_interval_seconds=60,
        resume_handler=lambda context: resume_contexts.append(context) or "resumed",
    )

    result = runner.run_forever(max_iterations=2)

    assert result.status is RunnerStatus.COMPLETED
    assert calls == []
    assert len(resume_contexts) == 1
    assert resume_contexts[0].paused_at == datetime(2026, 4, 10, 9, 5, tzinfo=KST)
    assert resume_contexts[0].resumed_at == datetime(2026, 4, 10, 10, 15, tzinfo=KST)
    assert state_store.load().is_executed(
        job_name="live_cycle",
        phase=MarketSessionPhase.INTRADAY,
        scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
    )
    assert state_store.load().is_executed(
        job_name="live_cycle",
        phase=MarketSessionPhase.INTRADAY,
        scheduled_at=datetime(2026, 4, 10, 10, 0, tzinfo=KST),
    )


def test_scheduled_runner_safe_stops_when_resume_maintenance_fails(
    tmp_path,
) -> None:
    state_store = FileSchedulerStateStore(tmp_path / "scheduler_state.json")
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    control_store.pause(timestamp=datetime(2026, 4, 10, 9, 0, tzinfo=KST), source="cli")
    clock = AdjustableClock(datetime(2026, 4, 10, 9, 5, tzinfo=KST))
    notifier = RecordingNotifier()

    def sleep(seconds: float) -> None:
        control_store.resume(
            timestamp=datetime(2026, 4, 10, 9, 10, tzinfo=KST),
            source="cli",
        )
        clock.current = datetime(2026, 4, 10, 9, 10, tzinfo=KST)

    runner = ScheduledRunner(
        jobs=(),
        state_store=state_store,
        notifier=notifier,
        clock=clock,
        sleep=sleep,
        control_store=control_store,
        control_poll_interval_seconds=60,
        resume_handler=lambda context: (_ for _ in ()).throw(
            RuntimeError("maintenance failed")
        ),
    )

    result = runner.run_forever(max_iterations=1)

    assert result.status is RunnerStatus.SAFE_STOP
    assert result.stop_reason == "maintenance failed"
    assert notifier.notifications[0].subject == (
        "AutoTrade runner safe stop [resume_maintenance_failure]"
    )


@dataclass(slots=True)
class RecordingNotifier:
    notifications: list[NotificationMessage] = field(default_factory=list)

    def send(self, notification: NotificationMessage) -> None:
        self.notifications.append(notification)


@dataclass(slots=True)
class AdjustableClock:
    current: datetime

    def __call__(self) -> datetime:
        return self.current
