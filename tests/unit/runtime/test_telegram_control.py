from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime

from autotrade.config import TelegramSettings
from autotrade.data import KST
from autotrade.report import NotificationMessage
from autotrade.report import TelegramHttpResponse
from autotrade.runtime.control import FileRunnerControlStore
from autotrade.runtime.control import RunnerControlMode
from autotrade.runtime.telegram_control import TelegramControlPoller


def test_telegram_control_poller_accepts_primary_chat_commands(tmp_path) -> None:
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    notifier = RecordingNotifier()
    timestamps = [
        datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        datetime(2026, 4, 10, 9, 5, tzinfo=KST),
    ]
    requests = []

    def transport(request):
        requests.append(request)
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 10,
                            "message": {"chat": {"id": "-100other"}, "text": "/pause"},
                        },
                        {
                            "update_id": 11,
                            "message": {"chat": {"id": "-100base"}, "text": "/pause"},
                        },
                        {
                            "update_id": 12,
                            "message": {
                                "chat": {"id": "-100base"},
                                "text": "/resume@AutoTradeBot",
                            },
                        },
                    ],
                }
            ).encode("utf-8"),
            headers={},
        )

    poller = TelegramControlPoller(
        settings=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
        ),
        control_store=control_store,
        notifier=notifier,
        clock=lambda: timestamps.pop(0),
        transport=transport,
    )

    poller.poll()

    state = control_store.load()
    assert state.mode is RunnerControlMode.RUNNING
    assert state.paused_by == "telegram"
    assert state.resumed_by == "telegram"
    assert state.telegram_update_offset == 13
    assert len(notifier.notifications) == 2
    assert notifier.notifications[0].subject == "AutoTrade runner control [PAUSE]"
    assert notifier.notifications[1].subject == "AutoTrade runner control [RESUME]"
    request_payload = json.loads(requests[0].body.decode("utf-8"))
    assert request_payload["allowed_updates"] == ["message"]


def test_telegram_control_poller_ignores_other_chats_and_advances_offset(
    tmp_path,
) -> None:
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    notifier = RecordingNotifier()

    def transport(request):
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 20,
                            "message": {"chat": {"id": "-100other"}, "text": "/pause"},
                        }
                    ],
                }
            ).encode("utf-8"),
            headers={},
        )

    poller = TelegramControlPoller(
        settings=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
        ),
        control_store=control_store,
        notifier=notifier,
        clock=lambda: datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        transport=transport,
    )

    poller.poll()

    state = control_store.load()
    assert state.mode is RunnerControlMode.RUNNING
    assert state.telegram_update_offset == 21
    assert notifier.notifications == []


@dataclass(slots=True)
class RecordingNotifier:
    notifications: list[NotificationMessage] = field(default_factory=list)

    def send(self, notification: NotificationMessage) -> None:
        self.notifications.append(notification)
