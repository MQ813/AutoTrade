from __future__ import annotations

from datetime import date
from datetime import datetime

from autotrade.data import KST
from autotrade.scheduler import ExecutedJobKey
from autotrade.scheduler import FileSchedulerStateStore
from autotrade.scheduler import MarketSessionPhase
from autotrade.scheduler import SchedulerState


def test_file_scheduler_state_store_persists_executed_runs_across_restart(
    tmp_path,
) -> None:
    path = tmp_path / "scheduler_state.json"
    state = SchedulerState().mark_executed(
        job_name="live_cycle",
        phase=MarketSessionPhase.INTRADAY,
        scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
    )

    FileSchedulerStateStore(path).save(state)

    restored = FileSchedulerStateStore(path).load()

    assert restored == state


def test_scheduler_state_retain_from_discards_previous_trading_days() -> None:
    state = SchedulerState(
        executed_runs=frozenset(
            {
                _executed_run(
                    job_name="old",
                    scheduled_at=datetime(2026, 4, 9, 15, 30, tzinfo=KST),
                ),
                _executed_run(
                    job_name="current",
                    scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
                ),
            }
        )
    )

    retained = state.retain_from(date(2026, 4, 10))

    assert {run.job_name for run in retained.executed_runs} == {"current"}


def _executed_run(
    *,
    job_name: str,
    scheduled_at: datetime,
):
    return ExecutedJobKey(
        job_name=job_name,
        phase=MarketSessionPhase.INTRADAY,
        scheduled_at=scheduled_at,
    )
