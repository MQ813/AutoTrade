from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from json import JSONDecodeError
import logging
from pathlib import Path
from typing import Protocol

from autotrade.common.persistence import move_corrupt_file
from autotrade.common.persistence import write_text_atomically
from autotrade.scheduler.runtime import ExecutedJobKey
from autotrade.scheduler.runtime import MarketSessionPhase
from autotrade.scheduler.runtime import SchedulerState

logger = logging.getLogger(__name__)


class SchedulerStateStore(Protocol):
    def load(self) -> SchedulerState: ...

    def save(self, state: SchedulerState) -> None: ...


@dataclass(slots=True)
class InMemorySchedulerStateStore:
    _state: SchedulerState = field(default_factory=SchedulerState, repr=False)

    def load(self) -> SchedulerState:
        return self._state

    def save(self, state: SchedulerState) -> None:
        self._state = state


@dataclass(slots=True)
class FileSchedulerStateStore:
    path: Path

    def __post_init__(self) -> None:
        if self.path.exists() and self.path.is_dir():
            raise ValueError("path must point to a file")

    def load(self) -> SchedulerState:
        if not self.path.exists():
            return SchedulerState()
        try:
            raw_payload = json.loads(self.path.read_text(encoding="utf-8"))
            payload = _require_mapping(raw_payload, "serialized scheduler state")
            executed_runs = frozenset(
                _deserialize_executed_run(item)
                for item in _require_list(payload.get("executed_runs"), "executed_runs")
            )
            return SchedulerState(executed_runs=executed_runs)
        except (JSONDecodeError, ValueError) as error:
            backup_path = move_corrupt_file(self.path)
            logger.warning(
                "손상된 scheduler 상태 파일을 백업하고 초기화합니다. path=%s backup=%s reason=%s",
                self.path,
                backup_path,
                error,
            )
            return SchedulerState()

    def save(self, state: SchedulerState) -> None:
        payload = {
            "executed_runs": [
                _serialize_executed_run(executed_run)
                for executed_run in sorted(
                    state.executed_runs,
                    key=lambda executed_run: (
                        executed_run.scheduled_at,
                        executed_run.job_name,
                        executed_run.phase.value,
                    ),
                )
            ]
        }
        write_text_atomically(
            self.path,
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        )


def _serialize_executed_run(executed_run: ExecutedJobKey) -> dict[str, str]:
    return {
        "job_name": executed_run.job_name,
        "phase": executed_run.phase.value,
        "scheduled_at": executed_run.scheduled_at.isoformat(),
    }


def _deserialize_executed_run(raw_value: object) -> ExecutedJobKey:
    payload = _require_mapping(raw_value, "executed run")
    job_name = _require_string(payload.get("job_name"), "job_name")
    phase = MarketSessionPhase(_require_string(payload.get("phase"), "phase"))
    scheduled_at = datetime.fromisoformat(
        _require_string(payload.get("scheduled_at"), "scheduled_at")
    )
    return ExecutedJobKey(
        job_name=job_name,
        phase=phase,
        scheduled_at=scheduled_at,
    )


def _require_mapping(raw_value: object, field_name: str) -> dict[str, object]:
    if not isinstance(raw_value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    if not all(isinstance(key, str) for key in raw_value):
        raise ValueError(f"{field_name} must use string keys")
    return raw_value


def _require_list(raw_value: object, field_name: str) -> list[object]:
    if raw_value is None:
        return []
    if not isinstance(raw_value, list):
        raise ValueError(f"{field_name} must be a list")
    return raw_value


def _require_string(raw_value: object, field_name: str) -> str:
    if not isinstance(raw_value, str):
        raise ValueError(f"{field_name} must be a string")
    return raw_value
