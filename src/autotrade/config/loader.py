from __future__ import annotations

import os
import re
from collections.abc import Mapping
from decimal import Decimal
from decimal import InvalidOperation
from pathlib import Path
from typing import cast

from autotrade.config.models import AppSettings
from autotrade.config.models import BrokerSettings
from autotrade.config.models import BrokerEnvironment
from autotrade.risk import RiskSettings

SYMBOL_CODE_PATTERN = re.compile(r"^\d{6}$")
DEFAULT_BROKER_PROVIDER = "koreainvestment"
DEFAULT_BROKER_ENVIRONMENT = "paper"
TARGET_SYMBOLS_ENV_KEY = "AUTOTRADE_TARGET_SYMBOLS"
DEFAULT_RISK_MAX_POSITION_WEIGHT = "0.2"
DEFAULT_RISK_MAX_CONCURRENT_HOLDINGS = "3"
DEFAULT_RISK_TRADING_HALTED = "false"
DEFAULT_RISK_EMERGENCY_STOP = "false"
DEFAULT_RISK_CANCEL_UNFILLED_ON_MARKET_CLOSE = "true"


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
    target_symbols = _parse_target_symbols(
        _read_required_value(environment, TARGET_SYMBOLS_ENV_KEY),
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
        target_symbols=target_symbols,
        log_dir=log_dir,
        risk=_load_risk_settings(environment),
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


def _parse_target_symbols(raw_value: str) -> tuple[str, ...]:
    parsed_codes: list[str] = []
    seen_codes: set[str] = set()

    for raw_code in raw_value.split(","):
        code = raw_code.strip()
        if not code:
            raise ConfigError(
                f"{TARGET_SYMBOLS_ENV_KEY} must not contain empty entries",
            )
        if not SYMBOL_CODE_PATTERN.fullmatch(code):
            raise ConfigError(
                f"{TARGET_SYMBOLS_ENV_KEY} contains invalid symbol code: {code}",
            )
        if code in seen_codes:
            raise ConfigError(
                f"{TARGET_SYMBOLS_ENV_KEY} contains duplicate symbol code: {code}"
            )
        seen_codes.add(code)
        parsed_codes.append(code)

    return tuple(parsed_codes)


def _parse_log_dir(raw_value: str) -> Path:
    log_dir = Path(raw_value).expanduser()
    if log_dir.exists() and not log_dir.is_dir():
        raise ConfigError(f"AUTOTRADE_LOG_DIR must point to a directory: {log_dir}")
    return log_dir


def _load_risk_settings(environment: Mapping[str, str]) -> RiskSettings:
    return RiskSettings(
        max_position_weight=_parse_decimal_setting(
            _read_optional_value(
                environment,
                "AUTOTRADE_RISK_MAX_POSITION_WEIGHT",
                default=DEFAULT_RISK_MAX_POSITION_WEIGHT,
            ),
            key="AUTOTRADE_RISK_MAX_POSITION_WEIGHT",
        ),
        max_concurrent_holdings=_parse_int_setting(
            _read_optional_value(
                environment,
                "AUTOTRADE_RISK_MAX_CONCURRENT_HOLDINGS",
                default=DEFAULT_RISK_MAX_CONCURRENT_HOLDINGS,
            ),
            key="AUTOTRADE_RISK_MAX_CONCURRENT_HOLDINGS",
        ),
        max_loss=_parse_optional_decimal_setting(
            environment,
            "AUTOTRADE_RISK_MAX_LOSS",
        ),
        max_drawdown=_parse_optional_decimal_setting(
            environment,
            "AUTOTRADE_RISK_MAX_DRAWDOWN",
        ),
        max_orders_per_day=_parse_optional_int_setting(
            environment,
            "AUTOTRADE_RISK_MAX_ORDERS_PER_DAY",
        ),
        trading_halted=_parse_bool_setting(
            _read_optional_value(
                environment,
                "AUTOTRADE_RISK_TRADING_HALTED",
                default=DEFAULT_RISK_TRADING_HALTED,
            ),
            key="AUTOTRADE_RISK_TRADING_HALTED",
        ),
        emergency_stop=_parse_bool_setting(
            _read_optional_value(
                environment,
                "AUTOTRADE_RISK_EMERGENCY_STOP",
                default=DEFAULT_RISK_EMERGENCY_STOP,
            ),
            key="AUTOTRADE_RISK_EMERGENCY_STOP",
        ),
        cancel_unfilled_orders_on_market_close=_parse_bool_setting(
            _read_optional_value(
                environment,
                "AUTOTRADE_RISK_CANCEL_UNFILLED_ON_MARKET_CLOSE",
                default=DEFAULT_RISK_CANCEL_UNFILLED_ON_MARKET_CLOSE,
            ),
            key="AUTOTRADE_RISK_CANCEL_UNFILLED_ON_MARKET_CLOSE",
        ),
    )


def _parse_decimal_setting(raw_value: str, *, key: str) -> Decimal:
    normalized = raw_value.strip()
    try:
        return Decimal(normalized)
    except InvalidOperation as error:
        raise ConfigError(f"{key} must be a decimal-compatible value") from error


def _parse_optional_decimal_setting(
    environment: Mapping[str, str],
    key: str,
) -> Decimal | None:
    raw_value = environment.get(key)
    if raw_value is None:
        return None

    normalized = raw_value.strip()
    if not normalized:
        return None
    return _parse_decimal_setting(normalized, key=key)


def _parse_optional_int_setting(
    environment: Mapping[str, str],
    key: str,
) -> int | None:
    raw_value = environment.get(key)
    if raw_value is None:
        return None

    normalized = raw_value.strip()
    if not normalized:
        return None
    return _parse_int_setting(normalized, key=key)


def _parse_int_setting(raw_value: str, *, key: str) -> int:
    normalized = raw_value.strip()
    try:
        return int(normalized)
    except ValueError as error:
        raise ConfigError(f"{key} must be an integer") from error


def _parse_bool_setting(raw_value: str, *, key: str) -> bool:
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"{key} must be a boolean value")
