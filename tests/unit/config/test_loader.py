from __future__ import annotations

from pathlib import Path
from decimal import Decimal

import pytest

from autotrade.config import AppSettings
from autotrade.config import ConfigError
from autotrade.config import load_settings


def test_load_settings_returns_app_settings_with_default_provider(
    tmp_path: Path,
) -> None:
    settings = load_settings(
        {
            "AUTOTRADE_BROKER_API_KEY": "demo-key",
            "AUTOTRADE_BROKER_API_SECRET": "demo-secret",
            "AUTOTRADE_BROKER_ACCOUNT": "12345678-01",
            "AUTOTRADE_TARGET_SYMBOLS": "069500, 357870, 114800",
            "AUTOTRADE_LOG_DIR": str(tmp_path / "logs"),
        },
    )

    assert isinstance(settings, AppSettings)
    assert settings.broker.provider == "koreainvestment"
    assert settings.broker.environment == "paper"
    assert settings.target_symbols == ("069500", "357870", "114800")
    assert settings.log_dir == tmp_path / "logs"


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("AUTOTRADE_BROKER_API_KEY", None),
        ("AUTOTRADE_BROKER_API_SECRET", " "),
        ("AUTOTRADE_BROKER_ACCOUNT", None),
        ("AUTOTRADE_TARGET_SYMBOLS", ""),
        ("AUTOTRADE_LOG_DIR", " "),
    ],
)
def test_load_settings_rejects_missing_or_blank_required_values(
    tmp_path: Path,
    key: str,
    value: str | None,
) -> None:
    env = _make_env(tmp_path)
    if value is None:
        env.pop(key)
    else:
        env[key] = value

    with pytest.raises(ConfigError):
        load_settings(env)


def test_load_settings_rejects_log_dir_that_points_to_existing_file(
    tmp_path: Path,
) -> None:
    log_file = tmp_path / "autotrade.log"
    log_file.write_text("existing log file", encoding="utf-8")

    with pytest.raises(ConfigError):
        load_settings(
            _make_env(
                tmp_path,
                AUTOTRADE_LOG_DIR=str(log_file),
            ),
        )


@pytest.mark.parametrize(
    "target_symbols",
    [
        "069500,,357870",
        "069500,ABC123",
        "069500,35787",
        "069500,069500",
    ],
)
def test_load_settings_rejects_invalid_target_symbols(
    tmp_path: Path,
    target_symbols: str,
) -> None:
    with pytest.raises(ConfigError):
        load_settings(
            _make_env(
                tmp_path,
                AUTOTRADE_TARGET_SYMBOLS=target_symbols,
            ),
        )


def test_load_settings_preserves_target_symbol_order(tmp_path: Path) -> None:
    settings = load_settings(
        _make_env(
            tmp_path,
            AUTOTRADE_TARGET_SYMBOLS="360750,069500,114800",
        ),
    )

    assert settings.target_symbols == ("360750", "069500", "114800")


def test_load_settings_accepts_live_broker_environment(tmp_path: Path) -> None:
    settings = load_settings(
        _make_env(
            tmp_path,
            AUTOTRADE_BROKER_ENV="live",
        ),
    )

    assert settings.broker.environment == "live"


def test_load_settings_reads_risk_settings(tmp_path: Path) -> None:
    settings = load_settings(
        _make_env(
            tmp_path,
            AUTOTRADE_RISK_MAX_POSITION_WEIGHT="0.35",
            AUTOTRADE_RISK_MAX_CONCURRENT_HOLDINGS="2",
            AUTOTRADE_RISK_MAX_LOSS="150000",
            AUTOTRADE_RISK_MAX_DRAWDOWN="0.12",
            AUTOTRADE_RISK_MAX_ORDERS_PER_DAY="5",
            AUTOTRADE_RISK_TRADING_HALTED="true",
            AUTOTRADE_RISK_EMERGENCY_STOP="true",
            AUTOTRADE_RISK_CANCEL_UNFILLED_ON_MARKET_CLOSE="false",
        ),
    )

    assert settings.risk.max_position_weight == Decimal("0.35")
    assert settings.risk.max_concurrent_holdings == 2
    assert settings.risk.max_loss == Decimal("150000")
    assert settings.risk.max_drawdown == Decimal("0.12")
    assert settings.risk.max_orders_per_day == 5
    assert settings.risk.trading_halted is True
    assert settings.risk.emergency_stop is True
    assert settings.risk.cancel_unfilled_orders_on_market_close is False


def test_load_settings_rejects_invalid_broker_environment(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_settings(
            _make_env(
                tmp_path,
                AUTOTRADE_BROKER_ENV="staging",
            ),
        )


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("AUTOTRADE_RISK_MAX_POSITION_WEIGHT", "abc"),
        ("AUTOTRADE_RISK_MAX_CONCURRENT_HOLDINGS", "1.5"),
        ("AUTOTRADE_RISK_MAX_DRAWDOWN", "abc"),
        ("AUTOTRADE_RISK_MAX_ORDERS_PER_DAY", "1.5"),
        ("AUTOTRADE_RISK_TRADING_HALTED", "maybe"),
        ("AUTOTRADE_RISK_EMERGENCY_STOP", "maybe"),
        ("AUTOTRADE_RISK_CANCEL_UNFILLED_ON_MARKET_CLOSE", "maybe"),
    ],
)
def test_load_settings_rejects_invalid_risk_values(
    tmp_path: Path,
    key: str,
    value: str,
) -> None:
    with pytest.raises((ConfigError, ValueError)):
        load_settings(
            _make_env(
                tmp_path,
                **{key: value},
            ),
        )


def _make_env(tmp_path: Path, **overrides: str) -> dict[str, str]:
    env = {
        "AUTOTRADE_BROKER_PROVIDER": "koreainvestment",
        "AUTOTRADE_BROKER_ENV": "paper",
        "AUTOTRADE_BROKER_API_KEY": "demo-key",
        "AUTOTRADE_BROKER_API_SECRET": "demo-secret",
        "AUTOTRADE_BROKER_ACCOUNT": "12345678-01",
        "AUTOTRADE_TARGET_SYMBOLS": "069500,357870",
        "AUTOTRADE_LOG_DIR": str(tmp_path / "logs"),
    }
    env.update(overrides)
    return env
