from __future__ import annotations

from pathlib import Path

import pytest
from decimal import Decimal

from autotrade.config import AppSettings
from autotrade.config import BrokerSettings
from autotrade.config import RiskSettings
from autotrade.config import TelegramSettings


def test_app_settings_accepts_target_symbols() -> None:
    settings = AppSettings(
        broker=_make_broker_settings(),
        target_symbols=("069500", "005930"),
        log_dir=Path("logs"),
    )

    assert settings.target_symbols == ("069500", "005930")
    assert settings.risk == RiskSettings()
    assert settings.telegram == TelegramSettings()


def test_app_settings_rejects_empty_target_symbols() -> None:
    with pytest.raises(ValueError):
        AppSettings(
            broker=_make_broker_settings(),
            target_symbols=(),
            log_dir=Path("logs"),
        )


def test_risk_settings_accepts_operating_capital_limit() -> None:
    settings = RiskSettings(max_operating_capital=Decimal("500000"))

    assert settings.max_operating_capital == Decimal("500000")


def test_risk_settings_rejects_non_positive_operating_capital_limit() -> None:
    with pytest.raises(ValueError):
        RiskSettings(max_operating_capital=Decimal("0"))


def test_telegram_settings_require_bot_token_and_chat_id_when_enabled() -> None:
    with pytest.raises(ValueError):
        TelegramSettings(enabled=True)


def test_broker_settings_reject_blank_hts_id() -> None:
    with pytest.raises(ValueError):
        BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
            hts_id=" ",
        )


def _make_broker_settings() -> BrokerSettings:
    return BrokerSettings(
        provider="koreainvestment",
        api_key="demo-key",
        api_secret="demo-secret",
        account="12345678-01",
        environment="paper",
    )
