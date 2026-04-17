from __future__ import annotations

import importlib.util
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from autotrade.config import AppSettings
from autotrade.config import BrokerSettings
from autotrade.data import Bar
from autotrade.data import Timeframe


def test_load_env_file_parses_simple_entries(tmp_path) -> None:
    module = _load_live_cycle_module()
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "AUTOTRADE_BROKER_ENV=paper",
                "export AUTOTRADE_LOG_DIR=./logs",
                'AUTOTRADE_TARGET_SYMBOLS="069500,357870"',
            ]
        ),
        encoding="utf-8",
    )

    parsed = module._load_env_file(env_file)

    assert parsed == {
        "AUTOTRADE_BROKER_ENV": "paper",
        "AUTOTRADE_LOG_DIR": "./logs",
        "AUTOTRADE_TARGET_SYMBOLS": "069500,357870",
    }


def test_resolve_environment_prefers_shell_values_over_env_file(tmp_path) -> None:
    module = _load_live_cycle_module()
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "AUTOTRADE_BROKER_ENV=paper",
                "AUTOTRADE_LOG_DIR=./logs-from-file",
            ]
        ),
        encoding="utf-8",
    )

    resolved = module._resolve_environment(
        {
            "AUTOTRADE_LOG_DIR": "./logs-from-shell",
            "EXTRA": "1",
        },
        env_file=env_file,
    )

    assert resolved["AUTOTRADE_BROKER_ENV"] == "paper"
    assert resolved["AUTOTRADE_LOG_DIR"] == "./logs-from-shell"
    assert resolved["EXTRA"] == "1"


def test_collect_strategy_bars_fetches_and_writes_csv(tmp_path, monkeypatch) -> None:
    module = _load_live_cycle_module()
    calls: list[tuple[str, Timeframe, datetime, datetime]] = []

    class FakeBarSource:
        def __init__(self, settings: BrokerSettings) -> None:
            assert settings.provider == "koreainvestment"

        def load_bars(
            self,
            symbol: str,
            timeframe: Timeframe,
            start: datetime | None = None,
            end: datetime | None = None,
        ) -> tuple[Bar, ...]:
            assert start is not None
            assert end is not None
            calls.append((symbol, timeframe, start, end))
            return (
                Bar(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=datetime(
                        2026,
                        4,
                        10,
                        9,
                        0,
                        tzinfo=ZoneInfo("Asia/Seoul"),
                    ),
                    open=Decimal("100"),
                    high=Decimal("105"),
                    low=Decimal("99"),
                    close=Decimal("104"),
                    volume=10,
                ),
            )

    monkeypatch.setattr(module, "KoreaInvestmentBarSource", FakeBarSource)
    settings = AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=tmp_path / "logs",
        )
    generated_at = datetime(2026, 4, 10, 21, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    bar_root = tmp_path / "logs" / "bars"

    module._collect_strategy_bars(
        settings,
        bar_root=bar_root,
        timeframe=Timeframe.MINUTE_30,
        generated_at=generated_at,
    )

    assert calls == [
        (
            "069500",
            Timeframe.MINUTE_30,
            generated_at - module.timedelta(days=21),
            generated_at,
        )
    ]
    assert (bar_root / "30m" / "069500.csv").read_text(encoding="utf-8") == (
        "symbol,timeframe,timestamp,open,high,low,close,volume\n"
        "069500,30m,2026-04-10T09:00:00+09:00,100,105,99,104,10\n"
    )


def _load_live_cycle_module():
    root = Path(__file__).resolve().parents[3]
    module_path = root / "tools" / "live_cycle.py"
    spec = importlib.util.spec_from_file_location("live_cycle_tool", module_path)
    if spec is None or spec.loader is None:
        raise AssertionError("failed to create module spec for live_cycle.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
