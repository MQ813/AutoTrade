from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from json import JSONDecodeError
from pathlib import Path

from autotrade.common.persistence import move_corrupt_file
from autotrade.common.persistence import write_text_atomically

logger = logging.getLogger(__name__)
ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class IntradayRiskState:
    trading_day: date
    session_start_equity: Decimal | None
    peak_equity: Decimal | None
    latest_equity: Decimal | None = None

    def __post_init__(self) -> None:
        _require_optional_positive_decimal(
            "session_start_equity",
            self.session_start_equity,
        )
        _require_optional_positive_decimal("peak_equity", self.peak_equity)
        _require_optional_non_negative_decimal("latest_equity", self.latest_equity)


@dataclass(slots=True)
class FileIntradayRiskStateStore:
    path: Path

    def __post_init__(self) -> None:
        if self.path.exists() and self.path.is_dir():
            raise ValueError("path must point to a file")

    def load(self) -> IntradayRiskState | None:
        if not self.path.exists():
            return None
        try:
            raw_payload = json.loads(self.path.read_text(encoding="utf-8"))
            payload = _require_mapping(raw_payload, "serialized intraday risk state")
            return IntradayRiskState(
                trading_day=date.fromisoformat(
                    _require_text(payload.get("trading_day"), "trading_day")
                ),
                session_start_equity=_require_optional_decimal(
                    payload.get("session_start_equity"),
                    "session_start_equity",
                ),
                peak_equity=_require_optional_decimal(
                    payload.get("peak_equity"),
                    "peak_equity",
                ),
                latest_equity=_require_optional_decimal(
                    payload.get("latest_equity"),
                    "latest_equity",
                ),
            )
        except (JSONDecodeError, ValueError) as error:
            backup_path = move_corrupt_file(self.path)
            logger.warning(
                "손상된 intraday risk 상태 파일을 백업하고 초기화합니다. path=%s backup=%s reason=%s",
                self.path,
                backup_path,
                error,
            )
            return None

    def save(self, state: IntradayRiskState) -> None:
        payload = {
            "trading_day": state.trading_day.isoformat(),
            "session_start_equity": _serialize_optional_decimal(
                state.session_start_equity
            ),
            "peak_equity": _serialize_optional_decimal(state.peak_equity),
            "latest_equity": _serialize_optional_decimal(state.latest_equity),
        }
        write_text_atomically(
            self.path,
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        )


def _require_optional_positive_decimal(
    field_name: str,
    value: Decimal | None,
) -> None:
    if value is not None and value <= ZERO:
        raise ValueError(f"{field_name} must be positive when provided")


def _require_optional_non_negative_decimal(
    field_name: str,
    value: Decimal | None,
) -> None:
    if value is not None and value < ZERO:
        raise ValueError(f"{field_name} must be non-negative when provided")


def _serialize_optional_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _require_mapping(raw_value: object, field_name: str) -> dict[str, object]:
    if not isinstance(raw_value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    if not all(isinstance(key, str) for key in raw_value):
        raise ValueError(f"{field_name} must use string keys")
    return raw_value


def _require_text(raw_value: object, field_name: str) -> str:
    if not isinstance(raw_value, str):
        raise ValueError(f"{field_name} must be a string")
    return raw_value


def _require_optional_decimal(
    raw_value: object,
    field_name: str,
) -> Decimal | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, str):
        raise ValueError(f"{field_name} must be a decimal string")
    return Decimal(raw_value)
