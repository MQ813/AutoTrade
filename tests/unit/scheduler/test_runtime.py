from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import timedelta

from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.scheduler import MarketSessionPhase
from autotrade.scheduler import ScheduledJob
from autotrade.scheduler import SchedulerConfig
from autotrade.scheduler import SchedulerRetryPolicy
from autotrade.scheduler import SchedulerState
from autotrade.scheduler import build_session_slots
from autotrade.scheduler import collect_due_jobs
from autotrade.scheduler import next_scheduled_run_at
from autotrade.scheduler import run_scheduled_jobs


def test_build_session_slots_for_trading_day() -> None:
    slots = build_session_slots(date(2026, 4, 10))

    assert len(slots) == 14
    assert slots[0].phase == MarketSessionPhase.MARKET_OPEN
    assert slots[0].scheduled_at == datetime(2026, 4, 10, 8, 0, tzinfo=KST)
    assert slots[1].phase == MarketSessionPhase.INTRADAY
    assert slots[1].scheduled_at == datetime(2026, 4, 10, 9, 30, tzinfo=KST)
    assert slots[-2].scheduled_at == datetime(2026, 4, 10, 15, 0, tzinfo=KST)
    assert slots[-1].phase == MarketSessionPhase.MARKET_CLOSE
    assert slots[-1].scheduled_at == datetime(2026, 4, 10, 15, 30, tzinfo=KST)


def test_collect_due_jobs_skips_completed_runs() -> None:
    jobs = (
        ScheduledJob(
            name="prepare",
            phase=MarketSessionPhase.MARKET_OPEN,
            handler=lambda context: "prepared",
        ),
        ScheduledJob(
            name="heartbeat",
            phase=MarketSessionPhase.INTRADAY,
            handler=lambda context: None,
        ),
    )
    state = SchedulerState().mark_executed(
        job_name="prepare",
        phase=MarketSessionPhase.MARKET_OPEN,
        scheduled_at=datetime(2026, 4, 10, 8, 0, tzinfo=KST),
    )

    due_jobs = collect_due_jobs(
        jobs,
        timestamp=datetime(2026, 4, 10, 10, 0, tzinfo=KST),
        state=state,
    )

    assert [
        (pending.job.name, pending.phase, pending.scheduled_at) for pending in due_jobs
    ] == [
        (
            "heartbeat",
            MarketSessionPhase.INTRADAY,
            datetime(2026, 4, 10, 9, 30, tzinfo=KST),
        ),
        (
            "heartbeat",
            MarketSessionPhase.INTRADAY,
            datetime(2026, 4, 10, 10, 0, tzinfo=KST),
        ),
    ]


def test_collect_due_jobs_skips_stale_intraday_backlog() -> None:
    jobs = (
        ScheduledJob(
            name="heartbeat",
            phase=MarketSessionPhase.INTRADAY,
            handler=lambda context: None,
        ),
    )

    due_jobs = collect_due_jobs(
        jobs,
        timestamp=datetime(2026, 4, 10, 10, 5, tzinfo=KST),
    )

    assert [
        (pending.job.name, pending.phase, pending.scheduled_at) for pending in due_jobs
    ] == [
        (
            "heartbeat",
            MarketSessionPhase.INTRADAY,
            datetime(2026, 4, 10, 10, 0, tzinfo=KST),
        ),
    ]


def test_collect_due_jobs_skips_stale_after_hours_backlog() -> None:
    jobs = (
        ScheduledJob(
            name="heartbeat",
            phase=MarketSessionPhase.INTRADAY,
            handler=lambda context: None,
        ),
        ScheduledJob(
            name="cleanup",
            phase=MarketSessionPhase.MARKET_CLOSE,
            handler=lambda context: None,
        ),
    )

    due_jobs = collect_due_jobs(
        jobs,
        timestamp=datetime(2026, 4, 10, 21, 20, tzinfo=KST),
    )

    assert due_jobs == ()


def test_run_scheduled_jobs_executes_due_handlers_once_and_captures_failures() -> None:
    calls: list[tuple[str, MarketSessionPhase, datetime]] = []

    def record_open(context) -> str:
        calls.append(("prepare", context.phase, context.scheduled_at))
        return "prepared"

    def fail_intraday(context) -> str:
        calls.append(("heartbeat", context.phase, context.scheduled_at))
        raise RuntimeError("heartbeat failed")

    jobs = (
        ScheduledJob(
            name="prepare",
            phase=MarketSessionPhase.MARKET_OPEN,
            handler=record_open,
        ),
        ScheduledJob(
            name="heartbeat",
            phase=MarketSessionPhase.INTRADAY,
            handler=fail_intraday,
        ),
    )
    clock_ticks = iter(
        (
            datetime(2026, 4, 10, 15, 0, 1, tzinfo=KST),
            datetime(2026, 4, 10, 15, 0, 2, tzinfo=KST),
            datetime(2026, 4, 10, 15, 0, 3, tzinfo=KST),
            datetime(2026, 4, 10, 15, 0, 4, tzinfo=KST),
        )
    )

    first_run = run_scheduled_jobs(
        jobs,
        timestamp=datetime(2026, 4, 10, 15, 0, tzinfo=KST),
        config=SchedulerConfig(intraday_interval=timedelta(hours=6)),
        clock=lambda: next(clock_ticks),
    )

    assert calls == [
        (
            "prepare",
            MarketSessionPhase.MARKET_OPEN,
            datetime(2026, 4, 10, 8, 0, tzinfo=KST),
        ),
        (
            "heartbeat",
            MarketSessionPhase.INTRADAY,
            datetime(2026, 4, 10, 15, 0, tzinfo=KST),
        ),
    ]
    assert len(first_run.executed_jobs) == 2
    assert first_run.executed_jobs[0].success is True
    assert first_run.executed_jobs[0].detail == "prepared"
    assert first_run.executed_jobs[1].success is False
    assert first_run.executed_jobs[1].error == "heartbeat failed"
    assert first_run.next_run_at == datetime(2026, 4, 10, 15, 30, tzinfo=KST)

    second_run = run_scheduled_jobs(
        jobs,
        timestamp=datetime(2026, 4, 10, 15, 0, tzinfo=KST),
        state=first_run.state,
        config=SchedulerConfig(intraday_interval=timedelta(hours=6)),
    )

    assert second_run.executed_jobs == ()


def test_run_scheduled_jobs_retries_retryable_failures_within_same_slot() -> None:
    attempts: list[datetime] = []

    def flaky_job(context) -> str:
        attempts.append(context.triggered_at)
        if len(attempts) == 1:
            raise RuntimeError("temporary failure")
        return "recovered"

    run = run_scheduled_jobs(
        (
            ScheduledJob(
                name="heartbeat",
                phase=MarketSessionPhase.INTRADAY,
                handler=flaky_job,
            ),
        ),
        timestamp=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
        clock=iter(
            (
                datetime(2026, 4, 10, 9, 30, 1, tzinfo=KST),
                datetime(2026, 4, 10, 9, 30, 2, tzinfo=KST),
                datetime(2026, 4, 10, 9, 30, 3, tzinfo=KST),
            )
        ).__next__,
        retry_policy=SchedulerRetryPolicy(max_attempts=2),
    )

    assert attempts == [
        datetime(2026, 4, 10, 9, 30, 1, tzinfo=KST),
        datetime(2026, 4, 10, 9, 30, 2, tzinfo=KST),
    ]
    assert run.executed_jobs[0].success is True
    assert run.executed_jobs[0].detail == "recovered"
    assert run.executed_jobs[0].error is None


def test_next_scheduled_run_at_skips_holiday_after_close() -> None:
    calendar = KrxRegularSessionCalendar(holiday_dates=frozenset({date(2026, 4, 13)}))

    next_run = next_scheduled_run_at(
        datetime(2026, 4, 10, 15, 30, tzinfo=KST),
        calendar=calendar,
    )

    assert next_run == datetime(2026, 4, 14, 8, 0, tzinfo=KST)


def test_run_scheduled_jobs_uses_last_completion_time_for_next_run() -> None:
    job = ScheduledJob(
        name="prepare",
        phase=MarketSessionPhase.MARKET_OPEN,
        handler=lambda context: "prepared",
    )
    clock_ticks = iter(
        (
            datetime(2026, 4, 10, 9, 0, 1, tzinfo=KST),
            datetime(2026, 4, 10, 9, 31, 0, tzinfo=KST),
        )
    )

    run = run_scheduled_jobs(
        (job,),
        timestamp=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        clock=lambda: next(clock_ticks),
    )

    assert run.next_run_at == datetime(2026, 4, 10, 10, 0, tzinfo=KST)
