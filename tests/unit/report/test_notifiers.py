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
import autotrade.report.notifiers as report_notifiers
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


def test_telegram_notifier_sets_force_ipv4_on_http_request() -> None:
    requests = []

    def transport(request):
        requests.append(request)
        return TelegramHttpResponse(
            status=200,
            body=json.dumps({"ok": True, "result": {"message_id": 1}}).encode("utf-8"),
            headers={},
        )

    notifier = TelegramNotifier(
        TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
            force_ipv4=True,
        ),
        transport=transport,
    )

    notifier.send(
        NotificationMessage(
            created_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
            severity=AlertSeverity.INFO,
            subject="ipv4",
            body="force IPv4",
        )
    )

    assert requests[0].force_ipv4 is True


def test_telegram_ipv4_connection_resolves_only_ipv4(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    getaddrinfo_calls = []

    class FakeSocket:
        def __init__(self, family, socktype, proto) -> None:
            self.family = family
            self.socktype = socktype
            self.proto = proto
            self.timeout = None
            self.connected_to = None

        def settimeout(self, timeout) -> None:
            self.timeout = timeout

        def bind(self, source_address) -> None:
            self.source_address = source_address

        def connect(self, sockaddr) -> None:
            self.connected_to = sockaddr

        def close(self) -> None:
            self.closed = True

    def fake_getaddrinfo(host, port, family, socktype):
        getaddrinfo_calls.append((host, port, family, socktype))
        return [(family, socktype, 6, "", ("203.0.113.10", port))]

    monkeypatch.setattr(report_notifiers.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(report_notifiers.socket, "socket", FakeSocket)

    connection = report_notifiers._create_ipv4_connection(
        ("api.telegram.org", 443),
        timeout=3.0,
    )

    assert getaddrinfo_calls == [
        (
            "api.telegram.org",
            443,
            report_notifiers.socket.AF_INET,
            report_notifiers.socket.SOCK_STREAM,
        )
    ]
    assert connection.family == report_notifiers.socket.AF_INET
    assert connection.timeout == 3.0
    assert connection.connected_to == ("203.0.113.10", 443)


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


def test_telegram_notifier_localizes_message_and_formats_symbol_names(
    monkeypatch,
) -> None:
    report_notifiers._symbol_name_map.cache_clear()
    monkeypatch.setattr(
        report_notifiers,
        "_load_symbol_name_map",
        lambda: {"005930": "삼성전자", "069500": "KODEX 200"},
    )

    requests = []

    def transport(request):
        requests.append(request)
        return TelegramHttpResponse(
            status=200,
            body=json.dumps({"ok": True, "result": {"message_id": 1}}).encode("utf-8"),
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
            severity=AlertSeverity.WARNING,
            subject="AutoTrade order 069500 [CANCELED]",
            body="\n".join(
                (
                    "symbol=069500",
                    "status=CANCELED",
                    "targets=069500,005930",
                    "reason=market close entry restriction is active",
                )
            ),
        )
    )

    payload = json.loads(requests[0].body.decode("utf-8"))
    assert "[경고] 주문 알림 KODEX 200(069500) [취소됨]" in payload["text"]
    assert "생성 시각: 2026-04-10T09:00:00+09:00" in payload["text"]
    assert "종목: KODEX 200(069500)" in payload["text"]
    assert "상태: 취소됨" in payload["text"]
    assert "대상 종목: KODEX 200(069500), 삼성전자(005930)" in payload["text"]
    assert "사유: 장 마감 신규 진입 제한 활성화" in payload["text"]

    report_notifiers._symbol_name_map.cache_clear()
