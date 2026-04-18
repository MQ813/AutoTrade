from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def test_load_env_file_parses_simple_entries(tmp_path) -> None:
    module = _load_weekly_review_module()
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "AUTOTRADE_LOG_DIR=./logs",
                "export AUTOTRADE_TELEGRAM_ENABLED=true",
                'AUTOTRADE_TELEGRAM_CHAT_ID="-10012345"',
            ]
        ),
        encoding="utf-8",
    )

    parsed = module._load_env_file(env_file)

    assert parsed == {
        "AUTOTRADE_LOG_DIR": "./logs",
        "AUTOTRADE_TELEGRAM_ENABLED": "true",
        "AUTOTRADE_TELEGRAM_CHAT_ID": "-10012345",
    }


def test_main_publishes_weekly_alert_when_telegram_enabled(
    tmp_path,
    monkeypatch,
) -> None:
    module = _load_weekly_review_module()
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"AUTOTRADE_LOG_DIR={tmp_path / 'logs'}",
                "AUTOTRADE_TELEGRAM_ENABLED=true",
                "AUTOTRADE_TELEGRAM_BOT_TOKEN=bot-token",
                "AUTOTRADE_TELEGRAM_CHAT_ID=-10012345",
            ]
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(module, "load_daily_run_reports", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        module,
        "load_daily_inspection_reports",
        lambda *args, **kwargs: (),
    )

    report = object()
    monkeypatch.setattr(
        module,
        "build_weekly_review_report",
        lambda *args, **kwargs: report,
    )
    monkeypatch.setattr(
        module,
        "write_weekly_review_report",
        lambda log_dir, weekly_report: Path(log_dir) / "weekly_review.txt",
    )

    def fake_publish_weekly_review_alert(notifier, weekly_report, *, created_at):
        captured["notifier"] = notifier
        captured["report"] = weekly_report
        captured["created_at"] = created_at

    monkeypatch.setattr(
        module,
        "publish_weekly_review_alert",
        fake_publish_weekly_review_alert,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["weekly_review.py", "--env-file", str(env_file)],
    )

    assert module.main() == 0
    assert captured["report"] is report
    notifier = captured["notifier"]
    assert isinstance(notifier, module.CompositeNotifier)
    assert isinstance(notifier.notifiers[0], module.FileNotifier)
    assert isinstance(notifier.notifiers[1], module.TelegramNotifier)


def _load_weekly_review_module():
    root = Path(__file__).resolve().parents[3]
    module_path = root / "tools" / "weekly_review.py"
    spec = importlib.util.spec_from_file_location("weekly_review_tool", module_path)
    if spec is None or spec.loader is None:
        raise AssertionError("failed to create module spec for weekly_review.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
