from __future__ import annotations

from datetime import datetime

from autotrade.data import KST
from autotrade.report import AlertSeverity
from autotrade.report import FileNotifier
from autotrade.report import NotificationMessage


def test_file_notifier_appends_jsonl_records(tmp_path) -> None:
    notifier = FileNotifier(tmp_path / "notifications.jsonl")
    first = NotificationMessage(
        created_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        severity=AlertSeverity.INFO,
        subject="first",
        body="hello",
    )
    second = NotificationMessage(
        created_at=datetime(2026, 4, 10, 9, 1, tzinfo=KST),
        severity=AlertSeverity.ERROR,
        subject="second",
        body="world",
    )

    notifier.send(first)
    notifier.send(second)

    assert (tmp_path / "notifications.jsonl").read_text(
        encoding="utf-8"
    ).splitlines() == [
        '{"body": "hello", "created_at": "2026-04-10T09:00:00+09:00", "severity": "info", "subject": "first"}',
        '{"body": "world", "created_at": "2026-04-10T09:01:00+09:00", "severity": "error", "subject": "second"}',
    ]
