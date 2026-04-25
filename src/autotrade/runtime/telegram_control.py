from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from autotrade.config import TelegramSettings
from autotrade.report import AlertSeverity
from autotrade.report import NotificationMessage
from autotrade.report import Notifier
from autotrade.report import TelegramHttpRequest
from autotrade.report import TelegramHttpResponse
from autotrade.report import telegram_http_transport
from autotrade.runtime.control import FileRunnerControlStore

logger = logging.getLogger(__name__)
_TELEGRAM_API_BASE_URL = "https://api.telegram.org"


class TelegramControlCommand(StrEnum):
    PAUSE = "pause"
    RESUME = "resume"


@dataclass(slots=True)
class TelegramControlPoller:
    settings: TelegramSettings
    control_store: FileRunnerControlStore
    notifier: Notifier
    clock: Callable[[], datetime]
    transport: Callable[[TelegramHttpRequest], TelegramHttpResponse] = (
        telegram_http_transport
    )

    def poll(self) -> None:
        if not self.settings.enabled:
            return
        assert self.settings.bot_token is not None
        assert self.settings.chat_id is not None

        state = self.control_store.load()
        payload: dict[str, object] = {
            "allowed_updates": ["message"],
            "timeout": 0,
        }
        if state.telegram_update_offset is not None:
            payload["offset"] = state.telegram_update_offset

        request = TelegramHttpRequest(
            url=f"{_TELEGRAM_API_BASE_URL}/bot{self.settings.bot_token}/getUpdates",
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=self.settings.timeout_seconds,
        )
        response = self.transport(request)
        decoded_payload = _decode_payload(response.body)
        if response.status != 200 or decoded_payload.get("ok") is not True:
            description = decoded_payload.get(
                "description", "telegram getUpdates failed"
            )
            raise RuntimeError(
                f"telegram getUpdates failed: status={response.status} description={description}"
            )

        next_offset = state.telegram_update_offset
        notifications: list[NotificationMessage] = []
        for update in _require_update_list(decoded_payload.get("result")):
            update_id = update.get("update_id")
            if isinstance(update_id, int):
                next_offset = (
                    update_id + 1
                    if next_offset is None
                    else max(next_offset, update_id + 1)
                )
            command = _extract_control_command(
                update,
                allowed_chat_id=self.settings.chat_id,
            )
            if command is None:
                continue
            notifications.append(self._apply_command(command))

        if next_offset is not None:
            self.control_store.save_telegram_update_offset(next_offset)

        for notification in notifications:
            self._send_notification(notification)

    def _apply_command(
        self,
        command: TelegramControlCommand,
    ) -> NotificationMessage:
        timestamp = self.clock()
        if command is TelegramControlCommand.PAUSE:
            state = self.control_store.pause(timestamp=timestamp, source="telegram")
        else:
            state = self.control_store.resume(timestamp=timestamp, source="telegram")
        return NotificationMessage(
            created_at=timestamp,
            severity=AlertSeverity.INFO,
            subject=f"AutoTrade runner control [{command.value.upper()}]",
            body="\n".join(
                (
                    f"mode={state.mode.value}",
                    "source=telegram",
                )
            ),
        )

    def _send_notification(self, notification: NotificationMessage) -> None:
        try:
            self.notifier.send(notification)
        except Exception as error:
            logger.warning(
                "telegram control 확인 알림 전송에 실패했습니다. subject=%s error=%s",
                notification.subject,
                error,
            )


def _decode_payload(body: bytes) -> dict[str, object]:
    if not body:
        return {}
    try:
        decoded = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _require_update_list(raw_value: object) -> tuple[dict[str, object], ...]:
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise RuntimeError("telegram getUpdates result must be a list")
    updates: list[dict[str, object]] = []
    for item in raw_value:
        if isinstance(item, dict) and all(isinstance(key, str) for key in item):
            updates.append(item)
    return tuple(updates)


def _extract_control_command(
    update: dict[str, object],
    *,
    allowed_chat_id: str,
) -> TelegramControlCommand | None:
    message = update.get("message")
    if not isinstance(message, dict):
        return None
    chat = message.get("chat")
    if not isinstance(chat, dict):
        return None
    if str(chat.get("id")) != allowed_chat_id:
        return None
    text = message.get("text")
    if not isinstance(text, str):
        return None
    token = text.strip().split(maxsplit=1)[0] if text.strip() else ""
    command_name = token.split("@", maxsplit=1)[0].casefold()
    if command_name == "/pause":
        return TelegramControlCommand.PAUSE
    if command_name == "/resume":
        return TelegramControlCommand.RESUME
    return None
