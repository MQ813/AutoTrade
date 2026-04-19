from __future__ import annotations

import json
import logging
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from time import sleep as default_sleep
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.request import Request
from urllib.request import urlopen

from autotrade.config import TelegramSettings
from autotrade.report.operations import AlertSeverity
from autotrade.report.operations import NotificationMessage
from autotrade.report.operations import Notifier

logger = logging.getLogger(__name__)
_TELEGRAM_MAX_TEXT_LENGTH = 4096
_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
_TELEGRAM_PART_SUFFIX_BUDGET = 12


@dataclass(slots=True)
class FileNotifier:
    path: Path

    def __post_init__(self) -> None:
        if self.path.exists() and self.path.is_dir():
            raise ValueError("path must point to a file")

    def send(self, notification: NotificationMessage) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "created_at": notification.created_at.isoformat(),
            "severity": notification.severity.value,
            "subject": notification.subject,
            "body": notification.body,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")


class NotificationDeliveryError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class TelegramHttpRequest:
    url: str
    body: bytes
    headers: Mapping[str, str]
    timeout: float


@dataclass(frozen=True, slots=True)
class TelegramHttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]


def _urllib_transport(request: TelegramHttpRequest) -> TelegramHttpResponse:
    urllib_request = Request(
        request.url,
        data=request.body,
        headers=dict(request.headers),
        method="POST",
    )
    try:
        with urlopen(urllib_request, timeout=request.timeout) as response:
            return TelegramHttpResponse(
                status=response.status,
                body=response.read(),
                headers=dict(response.headers.items()),
            )
    except HTTPError as error:
        return TelegramHttpResponse(
            status=error.code,
            body=error.read(),
            headers=dict(error.headers.items()),
        )
    except URLError as error:  # pragma: no cover - exercised through retry path
        raise NotificationDeliveryError(str(error.reason)) from error


@dataclass(slots=True)
class CompositeNotifier:
    notifiers: tuple[Notifier, ...]

    def __post_init__(self) -> None:
        if not self.notifiers:
            raise ValueError("notifiers must not be empty")

    def send(self, notification: NotificationMessage) -> None:
        failures: list[Exception] = []
        successful_deliveries = 0
        for notifier in self.notifiers:
            try:
                notifier.send(notification)
                successful_deliveries += 1
            except Exception as error:  # pragma: no cover - defensive aggregation
                failures.append(error)
                logger.exception(
                    "알림 전송에 실패했습니다: notifier=%s", type(notifier).__name__
                )
        if successful_deliveries == 0 and failures:
            raise NotificationDeliveryError(
                "; ".join(str(error) for error in failures),
            ) from failures[-1]


@dataclass(slots=True)
class TelegramNotifier:
    settings: TelegramSettings
    transport: Callable[[TelegramHttpRequest], TelegramHttpResponse] = _urllib_transport
    sleep: Callable[[float], None] = field(default=default_sleep)

    def send(self, notification: NotificationMessage) -> None:
        if not self.settings.enabled:
            return

        chat_id = self._chat_id_for(notification.severity)
        for message_text in _format_telegram_messages(notification):
            self._send_message(
                chat_id=chat_id,
                text=message_text,
            )

    def _chat_id_for(self, severity: AlertSeverity) -> str:
        if severity is AlertSeverity.ERROR and self.settings.error_chat_id is not None:
            return self.settings.error_chat_id
        if (
            severity is AlertSeverity.WARNING
            and self.settings.warning_chat_id is not None
        ):
            return self.settings.warning_chat_id
        assert self.settings.chat_id is not None
        return self.settings.chat_id

    def _send_message(self, *, chat_id: str, text: str) -> None:
        url = f"{_TELEGRAM_API_BASE_URL}/bot{self.settings.bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "disable_web_page_preview": True,
            "text": text,
        }
        request = TelegramHttpRequest(
            url=url,
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=self.settings.timeout_seconds,
        )

        for attempt in range(self.settings.max_retries + 1):
            try:
                response = self.transport(request)
            except NotificationDeliveryError as error:
                if attempt >= self.settings.max_retries:
                    raise
                delay_seconds = _network_retry_delay_seconds(attempt)
                logger.warning(
                    "텔레그램 알림 전송이 실패해 재시도합니다. attempt=%s delay=%.1f error=%s",
                    attempt + 1,
                    delay_seconds,
                    error,
                )
                self.sleep(delay_seconds)
                continue

            payload = _decode_telegram_payload(response.body)
            if response.status == 200 and payload.get("ok") is True:
                return

            retry_delay_seconds = _retry_delay_seconds(
                response=response,
                payload=payload,
                attempt=attempt,
            )
            description = str(payload.get("description", "telegram send failed"))
            if attempt < self.settings.max_retries and retry_delay_seconds is not None:
                logger.warning(
                    "텔레그램 알림 전송이 실패해 재시도합니다. attempt=%s status=%s delay=%.1f description=%s",
                    attempt + 1,
                    response.status,
                    retry_delay_seconds,
                    description,
                )
                self.sleep(retry_delay_seconds)
                continue
            raise NotificationDeliveryError(
                f"telegram send failed: status={response.status} description={description}"
            )


def _format_telegram_messages(
    notification: NotificationMessage,
) -> tuple[str, ...]:
    header = f"[{notification.severity.value.upper()}] {notification.subject}"
    body = "\n".join(
        (
            f"created_at={notification.created_at.isoformat()}",
            notification.body,
        )
    )
    available_body_length = max(
        1,
        _TELEGRAM_MAX_TEXT_LENGTH - len(header) - _TELEGRAM_PART_SUFFIX_BUDGET - 2,
    )
    chunks = _split_text(body, max_length=available_body_length)
    total_chunks = len(chunks)
    rendered_messages = []
    for index, chunk in enumerate(chunks, start=1):
        current_header = header
        if total_chunks > 1:
            current_header = f"{header} ({index}/{total_chunks})"
        rendered_messages.append(
            current_header if not chunk else f"{current_header}\n\n{chunk}"
        )
    return tuple(rendered_messages)


def _split_text(text: str, *, max_length: int) -> tuple[str, ...]:
    if len(text) <= max_length:
        return (text,)

    chunks: list[str] = []
    current_lines: list[str] = []
    current_length = 0
    for line in text.splitlines():
        line_chunks = _split_long_line(line, max_length=max_length)
        for line_chunk in line_chunks:
            additional_length = (
                len(line_chunk) if not current_lines else len(line_chunk) + 1
            )
            if current_lines and current_length + additional_length > max_length:
                chunks.append("\n".join(current_lines))
                current_lines = [line_chunk]
                current_length = len(line_chunk)
                continue
            current_lines.append(line_chunk)
            current_length += additional_length
    if current_lines:
        chunks.append("\n".join(current_lines))
    return tuple(chunks)


def _split_long_line(line: str, *, max_length: int) -> tuple[str, ...]:
    if len(line) <= max_length:
        return (line,)
    return tuple(
        line[index : index + max_length] for index in range(0, len(line), max_length)
    )


def _decode_telegram_payload(body: bytes) -> dict[str, object]:
    if not body:
        return {}
    try:
        decoded = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _retry_delay_seconds(
    *,
    response: TelegramHttpResponse,
    payload: Mapping[str, object],
    attempt: int,
) -> float | None:
    if response.status == 429:
        retry_after = _extract_retry_after_seconds(response=response, payload=payload)
        return (
            retry_after
            if retry_after is not None
            else _network_retry_delay_seconds(attempt)
        )
    if 500 <= response.status < 600:
        return _network_retry_delay_seconds(attempt)
    return None


def _extract_retry_after_seconds(
    *,
    response: TelegramHttpResponse,
    payload: Mapping[str, object],
) -> float | None:
    parameters = payload.get("parameters")
    if isinstance(parameters, dict):
        retry_after = parameters.get("retry_after")
        if isinstance(retry_after, int | float):
            return float(retry_after)

    header_value = response.headers.get("Retry-After") or response.headers.get(
        "retry-after"
    )
    if header_value is None:
        return None
    try:
        return float(header_value)
    except ValueError:
        return None


def _network_retry_delay_seconds(attempt: int) -> float:
    return float(min(2**attempt, 30))
