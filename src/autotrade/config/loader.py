from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from autotrade.config.models import AppSettings
from autotrade.config.models import BrokerSettings
from autotrade.config.models import BrokerEnvironment

ETF_CODE_PATTERN = re.compile(r"^\d{6}$")
DEFAULT_BROKER_PROVIDER = "koreainvestment"
DEFAULT_BROKER_ENVIRONMENT = "paper"


class ConfigError(ValueError):
    """Raised when required settings are missing or invalid."""


def load_settings(env: Mapping[str, str] | None = None) -> AppSettings:
    environment = os.environ if env is None else env

    provider = _read_optional_value(
        environment,
        "AUTOTRADE_BROKER_PROVIDER",
        default=DEFAULT_BROKER_PROVIDER,
    )
    broker_environment = _parse_broker_environment(
        _read_optional_value(
            environment,
            "AUTOTRADE_BROKER_ENV",
            default=DEFAULT_BROKER_ENVIRONMENT,
        ),
    )
    api_key = _read_required_value(environment, "AUTOTRADE_BROKER_API_KEY")
    api_secret = _read_required_value(environment, "AUTOTRADE_BROKER_API_SECRET")
    account = _read_required_value(environment, "AUTOTRADE_BROKER_ACCOUNT")
    target_etfs = _parse_target_etfs(
        _read_required_value(environment, "AUTOTRADE_TARGET_ETFS"),
    )
    log_dir = _parse_log_dir(_read_required_value(environment, "AUTOTRADE_LOG_DIR"))

    return AppSettings(
        broker=BrokerSettings(
            provider=provider,
            api_key=api_key,
            api_secret=api_secret,
            account=account,
            environment=broker_environment,
        ),
        target_etfs=target_etfs,
        log_dir=log_dir,
    )


def _read_required_value(environment: Mapping[str, str], key: str) -> str:
    raw_value = environment.get(key)
    if raw_value is None:
        raise ConfigError(f"Missing required setting: {key}")

    value = raw_value.strip()
    if not value:
        raise ConfigError(f"Setting must not be blank: {key}")
    return value


def _read_optional_value(
    environment: Mapping[str, str],
    key: str,
    *,
    default: str,
) -> str:
    raw_value = environment.get(key)
    if raw_value is None:
        return default

    value = raw_value.strip()
    return value or default


def _parse_broker_environment(raw_value: str) -> BrokerEnvironment:
    normalized = raw_value.strip().lower()
    if normalized not in {"paper", "live"}:
        raise ConfigError(
            "AUTOTRADE_BROKER_ENV must be one of: paper, live",
        )
    return cast(BrokerEnvironment, normalized)


def _parse_target_etfs(raw_value: str) -> tuple[str, ...]:
    parsed_codes: list[str] = []
    seen_codes: set[str] = set()

    for raw_code in raw_value.split(","):
        code = raw_code.strip()
        if not code:
            raise ConfigError("AUTOTRADE_TARGET_ETFS must not contain empty entries")
        if not ETF_CODE_PATTERN.fullmatch(code):
            raise ConfigError(
                f"AUTOTRADE_TARGET_ETFS contains invalid ETF code: {code}",
            )
        if code in seen_codes:
            raise ConfigError(
                f"AUTOTRADE_TARGET_ETFS contains duplicate ETF code: {code}"
            )
        seen_codes.add(code)
        parsed_codes.append(code)

    return tuple(parsed_codes)


def _parse_log_dir(raw_value: str) -> Path:
    log_dir = Path(raw_value).expanduser()
    if log_dir.exists() and not log_dir.is_dir():
        raise ConfigError(f"AUTOTRADE_LOG_DIR must point to a directory: {log_dir}")
    return log_dir
