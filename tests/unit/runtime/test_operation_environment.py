from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from autotrade.data import KST
from autotrade.recommendation import ApprovedSymbolsRecord
from autotrade.recommendation import write_approved_symbols_bundle
from autotrade.runtime import operation_environment


def test_load_runtime_settings_applies_latest_approved_symbols(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "logs"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "AUTOTRADE_BROKER_API_KEY=demo-key",
                "AUTOTRADE_BROKER_API_SECRET=demo-secret",
                "AUTOTRADE_BROKER_ACCOUNT=12345678-01",
                "AUTOTRADE_TARGET_SYMBOLS=069500,357870",
                f"AUTOTRADE_LOG_DIR={log_dir}",
            ]
        ),
        encoding="utf-8",
    )
    write_approved_symbols_bundle(
        log_dir,
        ApprovedSymbolsRecord(
            as_of=datetime(2026, 4, 11, 10, 0, tzinfo=KST).date(),
            approved_at=datetime(2026, 4, 11, 10, 0, tzinfo=KST),
            symbols=("005930", "000660", "035420"),
        ),
    )

    settings = operation_environment.load_runtime_settings(
        env_file,
        env_template_file=tmp_path / "autotrade.env.example",
        logger=logging.getLogger("test"),
    )

    assert settings is not None
    assert settings.target_symbols == ("005930", "000660", "035420")


def test_load_runtime_settings_uses_env_targets_without_approved_file(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "logs"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "AUTOTRADE_BROKER_API_KEY=demo-key",
                "AUTOTRADE_BROKER_API_SECRET=demo-secret",
                "AUTOTRADE_BROKER_ACCOUNT=12345678-01",
                "AUTOTRADE_TARGET_SYMBOLS=069500,357870",
                f"AUTOTRADE_LOG_DIR={log_dir}",
            ]
        ),
        encoding="utf-8",
    )

    settings = operation_environment.load_runtime_settings(
        env_file,
        env_template_file=tmp_path / "autotrade.env.example",
        logger=logging.getLogger("test"),
    )

    assert settings is not None
    assert settings.target_symbols == ("069500", "357870")
