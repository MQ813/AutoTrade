from __future__ import annotations

import json
from datetime import datetime

import pytest

from autotrade.data import KST
from autotrade.report import AlertSeverity
from autotrade.report import CompositeNotifier
from autotrade.report import FileNotifier
from autotrade.report import NotificationDeliveryError
from autotrade.report import NotificationMessage
from autotrade.report import TelegramNotifier
from autotrade.report.notifiers import TelegramHttpResponse
from autotrade.config import TelegramSettings


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


def test_composite_notifier_continues_when_one_notifier_fails() -> None:
    recorded: list[NotificationMessage] = []

    class RecordingNotifier:
        def send(self, notification: NotificationMessage) -> None:
            recorded.append(notification)

    class BrokenNotifier:
        def send(self, notification: NotificationMessage) -> None:
            raise RuntimeError("telegram down")

    notifier = CompositeNotifier((RecordingNotifier(), BrokenNotifier()))
    notification = NotificationMessage(
        created_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        severity=AlertSeverity.WARNING,
        subject="warning",
        body="retry later",
    )

    notifier.send(notification)

    assert recorded == [notification]


def test_composite_notifier_raises_when_all_notifiers_fail() -> None:
    class BrokenNotifier:
        def send(self, notification: NotificationMessage) -> None:
            raise RuntimeError("unreachable")

    notifier = CompositeNotifier((BrokenNotifier(), BrokenNotifier()))

    with pytest.raises(NotificationDeliveryError):
        notifier.send(
            NotificationMessage(
                created_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
                severity=AlertSeverity.ERROR,
                subject="broken",
                body="still broken",
            )
        )


def test_telegram_notifier_retries_rate_limit_and_uses_error_chat_id() -> None:
    requests = []
    sleeps = []
    responses = [
        TelegramHttpResponse(
            status=429,
            body=json.dumps(
                {
                    "ok": False,
                    "description": "Too Many Requests",
                    "parameters": {"retry_after": 2},
                }
            ).encode("utf-8"),
            headers={},
        ),
        TelegramHttpResponse(
            status=200,
            body=json.dumps({"ok": True, "result": {"message_id": 1}}).encode("utf-8"),
            headers={},
        ),
    ]

    def transport(request):
        requests.append(request)
        return responses.pop(0)

    notifier = TelegramNotifier(
        TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
            error_chat_id="-100error",
            max_retries=1,
        ),
        transport=transport,
        sleep=sleeps.append,
    )

    notifier.send(
        NotificationMessage(
            created_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
            severity=AlertSeverity.ERROR,
            subject="daily failed",
            body="failed_jobs=market_close_cleanup",
        )
    )

    assert len(requests) == 2
    assert sleeps == [2.0]
    first_payload = json.loads(requests[0].body.decode("utf-8"))
    assert first_payload["chat_id"] == "-100error"
    assert "daily failed" in first_payload["text"]


def test_telegram_notifier_splits_long_messages() -> None:
    requests = []

    def transport(request):
        requests.append(request)
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(
                {"ok": True, "result": {"message_id": len(requests)}}
            ).encode("utf-8"),
            headers={},
        )

    notifier = TelegramNotifier(
        TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
        ),
        transport=transport,
    )

    notifier.send(
        NotificationMessage(
            created_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
            severity=AlertSeverity.INFO,
            subject="weekly review",
            body="x" * 9000,
        )
    )

    assert len(requests) >= 3
    decoded_payloads = [
        json.loads(request.body.decode("utf-8")) for request in requests
    ]
    assert all(payload["chat_id"] == "-100base" for payload in decoded_payloads)
    assert all(len(payload["text"]) <= 4096 for payload in decoded_payloads)
