from __future__ import annotations

from datetime import datetime

from autotrade.data import KST
from autotrade.runtime.control import FileRunnerControlStore
from autotrade.runtime.control import RunnerControlMode
from autotrade.runtime.control import RunnerControlState


def test_runner_control_state_pause_resume_are_idempotent() -> None:
    paused_at = datetime(2026, 4, 10, 9, 0, tzinfo=KST)
    resumed_at = datetime(2026, 4, 10, 10, 0, tzinfo=KST)

    paused = RunnerControlState().pause(timestamp=paused_at, source="cli")
    paused_again = paused.pause(
        timestamp=datetime(2026, 4, 10, 9, 5, tzinfo=KST),
        source="telegram",
    )
    resumed = paused.resume(timestamp=resumed_at, source="telegram")
    resumed_again = resumed.resume(
        timestamp=datetime(2026, 4, 10, 10, 5, tzinfo=KST),
        source="cli",
    )

    assert paused.mode is RunnerControlMode.PAUSED
    assert paused.paused_at == paused_at
    assert paused.paused_by == "cli"
    assert paused_again == paused
    assert resumed.mode is RunnerControlMode.RUNNING
    assert resumed.paused_at == paused_at
    assert resumed.resumed_at == resumed_at
    assert resumed.resumed_by == "telegram"
    assert resumed_again == resumed


def test_file_runner_control_store_persists_state(tmp_path) -> None:
    store = FileRunnerControlStore(tmp_path / "runner_control.json")
    paused_at = datetime(2026, 4, 10, 9, 0, tzinfo=KST)
    resumed_at = datetime(2026, 4, 10, 10, 0, tzinfo=KST)

    store.pause(timestamp=paused_at, source="cli")
    store.resume(timestamp=resumed_at, source="telegram")
    store.save_telegram_update_offset(42)

    loaded = FileRunnerControlStore(tmp_path / "runner_control.json").load()
    assert loaded.mode is RunnerControlMode.RUNNING
    assert loaded.paused_at == paused_at
    assert loaded.resumed_at == resumed_at
    assert loaded.telegram_update_offset == 42


def test_file_runner_control_store_recovers_from_corrupted_file(tmp_path) -> None:
    path = tmp_path / "runner_control.json"
    path.write_text("{not-json", encoding="utf-8")
    store = FileRunnerControlStore(path)

    state = store.load()

    assert state == RunnerControlState()
    assert not path.exists()
    assert tuple(tmp_path.glob("runner_control.json.corrupt-*"))
