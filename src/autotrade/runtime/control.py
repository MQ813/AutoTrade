from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
import fcntl
import json
import logging
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from enum import StrEnum
from json import JSONDecodeError
from pathlib import Path
from threading import RLock
from typing import Protocol

from autotrade.common.persistence import move_corrupt_file
from autotrade.common.persistence import write_text_atomically

logger = logging.getLogger(__name__)
_LOCK_REGISTRY_GUARD = RLock()
_LOCK_REGISTRY: dict[Path, RLock] = {}


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_non_blank_text(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")


class RunnerControlMode(StrEnum):
    RUNNING = "running"
    PAUSED = "paused"


@dataclass(frozen=True, slots=True)
class RunnerControlState:
    mode: RunnerControlMode = RunnerControlMode.RUNNING
    updated_at: datetime | None = None
    updated_by: str | None = None
    paused_at: datetime | None = None
    paused_by: str | None = None
    resumed_at: datetime | None = None
    resumed_by: str | None = None
    telegram_update_offset: int | None = None

    def __post_init__(self) -> None:
        for field_name in ("updated_at", "paused_at", "resumed_at"):
            value = getattr(self, field_name)
            if value is not None:
                _require_aware_datetime(field_name, value)
        for field_name in ("updated_by", "paused_by", "resumed_by"):
            value = getattr(self, field_name)
            if value is not None:
                _require_non_blank_text(field_name, value)
        if self.telegram_update_offset is not None and self.telegram_update_offset < 0:
            raise ValueError("telegram_update_offset must be non-negative")
        if self.mode is RunnerControlMode.PAUSED and self.paused_at is None:
            raise ValueError("paused_at is required when runner is paused")

    def pause(self, *, timestamp: datetime, source: str) -> RunnerControlState:
        _require_aware_datetime("timestamp", timestamp)
        _require_non_blank_text("source", source)
        if self.mode is RunnerControlMode.PAUSED:
            return self
        return RunnerControlState(
            mode=RunnerControlMode.PAUSED,
            updated_at=timestamp,
            updated_by=source,
            paused_at=timestamp,
            paused_by=source,
            resumed_at=None,
            resumed_by=None,
            telegram_update_offset=self.telegram_update_offset,
        )

    def resume(self, *, timestamp: datetime, source: str) -> RunnerControlState:
        _require_aware_datetime("timestamp", timestamp)
        _require_non_blank_text("source", source)
        if self.mode is RunnerControlMode.RUNNING:
            return self
        return RunnerControlState(
            mode=RunnerControlMode.RUNNING,
            updated_at=timestamp,
            updated_by=source,
            paused_at=self.paused_at,
            paused_by=self.paused_by,
            resumed_at=timestamp,
            resumed_by=source,
            telegram_update_offset=self.telegram_update_offset,
        )

    def with_telegram_update_offset(self, offset: int) -> RunnerControlState:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        return replace(self, telegram_update_offset=offset)


class RunnerControlStore(Protocol):
    def load(self) -> RunnerControlState: ...

    def save(self, state: RunnerControlState) -> None: ...


@dataclass(slots=True)
class FileRunnerControlStore:
    path: Path

    def __post_init__(self) -> None:
        if self.path.exists() and self.path.is_dir():
            raise ValueError("path must point to a file")

    def load(self) -> RunnerControlState:
        with _locked_control_file(self.path):
            return self._load_unlocked()

    def save(self, state: RunnerControlState) -> None:
        with _locked_control_file(self.path):
            self._save_unlocked(state)

    def pause(self, *, timestamp: datetime, source: str) -> RunnerControlState:
        return self._update(
            lambda state: state.pause(timestamp=timestamp, source=source)
        )

    def resume(self, *, timestamp: datetime, source: str) -> RunnerControlState:
        return self._update(
            lambda state: state.resume(timestamp=timestamp, source=source)
        )

    def save_telegram_update_offset(self, offset: int) -> RunnerControlState:
        return self._update(lambda state: state.with_telegram_update_offset(offset))

    def _update(
        self,
        transform: Callable[[RunnerControlState], RunnerControlState],
    ) -> RunnerControlState:
        with _locked_control_file(self.path):
            state = transform(self._load_unlocked())
            self._save_unlocked(state)
            return state

    def _load_unlocked(self) -> RunnerControlState:
        if not self.path.exists():
            return RunnerControlState()
        try:
            raw_payload = json.loads(self.path.read_text(encoding="utf-8"))
            payload = _require_mapping(raw_payload, "serialized runner control state")
            return RunnerControlState(
                mode=RunnerControlMode(_require_string(payload.get("mode"), "mode")),
                updated_at=_optional_datetime(payload.get("updated_at"), "updated_at"),
                updated_by=_optional_string(payload.get("updated_by"), "updated_by"),
                paused_at=_optional_datetime(payload.get("paused_at"), "paused_at"),
                paused_by=_optional_string(payload.get("paused_by"), "paused_by"),
                resumed_at=_optional_datetime(payload.get("resumed_at"), "resumed_at"),
                resumed_by=_optional_string(payload.get("resumed_by"), "resumed_by"),
                telegram_update_offset=_optional_int(
                    payload.get("telegram_update_offset"),
                    "telegram_update_offset",
                ),
            )
        except (JSONDecodeError, ValueError) as error:
            backup_path = move_corrupt_file(self.path)
            logger.warning(
                "손상된 runner control 상태 파일을 백업하고 초기화합니다. path=%s backup=%s reason=%s",
                self.path,
                backup_path,
                error,
            )
            return RunnerControlState()

    def _save_unlocked(self, state: RunnerControlState) -> None:
        write_text_atomically(
            self.path,
            json.dumps(
                _serialize_state(state),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
        )


def _serialize_state(state: RunnerControlState) -> dict[str, object]:
    return {
        "mode": state.mode.value,
        "updated_at": _serialize_datetime(state.updated_at),
        "updated_by": state.updated_by,
        "paused_at": _serialize_datetime(state.paused_at),
        "paused_by": state.paused_by,
        "resumed_at": _serialize_datetime(state.resumed_at),
        "resumed_by": state.resumed_by,
        "telegram_update_offset": state.telegram_update_offset,
    }


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _require_mapping(raw_value: object, field_name: str) -> dict[str, object]:
    if not isinstance(raw_value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    if not all(isinstance(key, str) for key in raw_value):
        raise ValueError(f"{field_name} must use string keys")
    return raw_value


def _require_string(raw_value: object, field_name: str) -> str:
    if not isinstance(raw_value, str):
        raise ValueError(f"{field_name} must be a string")
    if not raw_value.strip():
        raise ValueError(f"{field_name} must not be blank")
    return raw_value


def _optional_string(raw_value: object, field_name: str) -> str | None:
    if raw_value is None:
        return None
    return _require_string(raw_value, field_name)


def _optional_datetime(raw_value: object, field_name: str) -> datetime | None:
    if raw_value is None:
        return None
    value = datetime.fromisoformat(_require_string(raw_value, field_name))
    _require_aware_datetime(field_name, value)
    return value


def _optional_int(raw_value: object, field_name: str) -> int | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, int):
        raise ValueError(f"{field_name} must be an integer")
    if raw_value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return raw_value


@contextmanager
def _locked_control_file(path: Path) -> Iterator[None]:
    lock = _thread_lock_for(path)
    with lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = path.with_name(f".{path.name}.lock")
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def _thread_lock_for(path: Path) -> RLock:
    key = path.expanduser().resolve(strict=False)
    with _LOCK_REGISTRY_GUARD:
        lock = _LOCK_REGISTRY.get(key)
        if lock is None:
            lock = RLock()
            _LOCK_REGISTRY[key] = lock
        return lock
