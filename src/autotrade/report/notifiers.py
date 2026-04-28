from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from functools import lru_cache
from http.client import HTTPConnection
from http.client import HTTPSConnection
import json
import logging
from pathlib import Path
import re
import socket
from time import sleep as default_sleep
from typing import Any
from typing import cast
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.request import HTTPHandler
from urllib.request import HTTPSHandler
from urllib.request import Request
from urllib.request import build_opener
from urllib.request import urlopen

from autotrade.config import TelegramSettings
from autotrade.recommendation.kis_seed_universe import DEFAULT_KIS_RAW_DIR
from autotrade.recommendation.kis_seed_universe import load_konex_master_records
from autotrade.recommendation.kis_seed_universe import load_kosdaq_master_records
from autotrade.recommendation.kis_seed_universe import load_kospi_master_records
from autotrade.report.operations import AlertSeverity
from autotrade.report.operations import NotificationMessage
from autotrade.report.operations import Notifier

logger = logging.getLogger(__name__)
_TELEGRAM_MAX_TEXT_LENGTH = 4096
_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
_TELEGRAM_PART_SUFFIX_BUDGET = 12
_SYMBOL_PATTERN = re.compile(r"\b\d{6}\b")
_ORDER_SUBJECT_PATTERN = re.compile(
    r"^AutoTrade order (?P<symbol>\d{6}) \[(?P<status>[A-Z_]+)\]$"
)
_FILL_SUBJECT_PATTERN = re.compile(
    r"^AutoTrade fill (?P<symbol>\d{6}) \[(?P<quantity>\d+)@(?P<price>.+)\]$"
)
_RISK_BLOCK_SUBJECT_PATTERN = re.compile(r"^AutoTrade risk block (?P<symbol>\d{6})$")
_DAILY_REPORT_SUBJECT_PATTERN = re.compile(
    r"^AutoTrade daily report (?P<trading_day>\d{4}-\d{2}-\d{2}) \[(?P<status>[A-Z_]+)\]$"
)
_WEEKLY_REVIEW_SUBJECT_PATTERN = re.compile(
    r"^AutoTrade weekly review "
    r"(?P<week_start>\d{4}-\d{2}-\d{2})~(?P<week_end>\d{4}-\d{2}-\d{2}) "
    r"\[(?P<status>[A-Z_]+)\]$"
)
_MARKET_OPEN_SUBJECT_PATTERN = re.compile(
    r"^AutoTrade market open prep "
    r"(?P<trading_day>\d{4}-\d{2}-\d{2}) "
    r"\[(?P<status>[A-Z_]+)\]$"
)
_SAFE_STOP_SUBJECT_PATTERN = re.compile(
    r"^AutoTrade runner safe stop \[(?P<reason>.+)\]$"
)
_TELEGRAM_SEVERITY_LABELS = {
    AlertSeverity.INFO: "정보",
    AlertSeverity.WARNING: "경고",
    AlertSeverity.ERROR: "오류",
}
_TELEGRAM_FIELD_LABELS = {
    "action": "신호",
    "allowed": "허용 여부",
    "approved": "승인 수량",
    "approved_quantity": "승인 수량",
    "attention_reasons": "주의 사유",
    "bars": "바",
    "bars_after": "바 개수(후)",
    "bars_before": "바 개수(전)",
    "created_at": "생성 시각",
    "detail": "상세",
    "emergency_stop": "비상 정지",
    "error": "오류",
    "failed_inspection_items": "점검 실패 수",
    "failed_job_names": "실패 작업",
    "failed_jobs": "실패 작업 수",
    "failure_reasons": "실패 사유",
    "filled_at": "체결 시각",
    "filled_quantity": "체결 수량",
    "fill_id": "체결 ID",
    "holding": "보유 수량",
    "holding_quantity": "보유 수량",
    "inspection_report": "점검 리포트",
    "latest_bar_at": "최신 바 시각",
    "limit_price": "지정가",
    "message": "메시지",
    "missing_inspection_report_days": "점검 리포트 누락일",
    "missing_run_report_days": "실행 리포트 누락일",
    "order_id": "주문 ID",
    "pending_inspection_items": "점검 대기 수",
    "previous_day_errors": "전일 오류 수",
    "price": "가격",
    "reason": "사유",
    "refreshed": "재수집 여부",
    "repeated_failure_jobs": "반복 실패 작업",
    "requested": "요청 수량",
    "requested_quantity": "요청 수량",
    "signal": "신호",
    "signal_at": "신호 시각",
    "signal_reason": "신호 근거",
    "smoke_report": "스모크 리포트",
    "smoke_success": "스모크 점검 성공",
    "status": "상태",
    "symbol": "종목",
    "targets": "대상 종목",
    "timeframe": "주기",
    "total_jobs": "전체 작업 수",
    "trading_day": "거래일",
    "trading_halted": "거래 중지",
    "updated_at": "업데이트 시각",
    "violation": "리스크 위반",
    "violations": "위반 수",
    "week_end": "주 종료일",
    "week_start": "주 시작일",
}
_TELEGRAM_VALUE_LABELS = {
    "ACKNOWLEDGED": "접수됨",
    "ATTENTION": "주의",
    "BUY": "매수",
    "CANCELED": "취소됨",
    "CANCEL_PENDING": "취소 대기",
    "FAILED": "실패",
    "FILLED": "체결 완료",
    "HOLD": "관망",
    "NO_RUNS": "실행 없음",
    "OK": "정상",
    "PARTIALLY_FILLED": "부분 체결",
    "PENDING": "대기",
    "REJECTED": "거부됨",
    "SELL": "매도",
    "already_held": "기보유 종목",
    "buy_pending": "기존 매수 주문 대기",
    "entry_restricted": "장마감 신규 진입 제한",
    "failure": "실패",
    "hold": "관망",
    "no_data": "바 데이터 없음",
    "preview_failed": "미리보기 실패",
    "risk_blocked": "리스크 차단",
    "sell_pending": "기존 매도 주문 대기",
    "sell_skipped": "보유 수량 없음",
    "submitted": "매수 주문 제출",
    "submitted_sell": "매도 주문 제출",
    "success": "정상",
}
_TELEGRAM_REASON_LABELS = {
    "broker_smoke_failed": "브로커 스모크 점검 실패",
    "emergency_stop": "비상 정지 활성화",
    "market close entry restriction is active": "장 마감 신규 진입 제한 활성화",
    "previous_day_errors_detected": "전일 오류 감지",
    "risk check rejected the order": "리스크 점검에서 주문이 거부됨",
    "risk sizing produced zero quantity": "리스크 수량 계산 결과가 0",
    "strategy_data_unavailable": "전략 데이터 준비 실패",
    "strategy_preview_failed": "전략 미리보기 실패",
    "trading_halted": "거래 중지 활성화",
}
_TELEGRAM_HEADER_LABELS = {
    "data_statuses:": "데이터 상태:",
    "strategy_previews:": "전략 미리보기:",
}


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
    force_ipv4: bool = False


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
        with _open_telegram_url(urllib_request, request) as response:
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


telegram_http_transport = _urllib_transport


def _open_telegram_url(urllib_request: Request, request: TelegramHttpRequest) -> Any:
    if not request.force_ipv4:
        return urlopen(urllib_request, timeout=request.timeout)
    opener = build_opener(_IPv4HTTPHandler, _IPv4HTTPSHandler)
    return opener.open(urllib_request, timeout=request.timeout)


def _create_ipv4_connection(
    address: tuple[str, int],
    timeout: float | None | object = None,
    source_address: tuple[str, int] | None = None,
) -> socket.socket:
    host, port = address
    last_error: OSError | None = None
    for result in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM):
        family, socktype, proto, _, sockaddr = result
        connection = socket.socket(family, socktype, proto)
        try:
            if timeout is None or isinstance(timeout, int | float):
                connection.settimeout(cast(float | None, timeout))
            if source_address is not None:
                connection.bind(source_address)
            connection.connect(sockaddr)
            return connection
        except OSError as error:
            last_error = error
            connection.close()
    if last_error is not None:
        raise last_error
    raise OSError(f"no IPv4 address found for {host}")


class _IPv4HTTPConnection(HTTPConnection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._create_connection = _create_ipv4_connection


class _IPv4HTTPSConnection(HTTPSConnection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._create_connection = _create_ipv4_connection


class _IPv4HTTPHandler(HTTPHandler):
    def http_open(self, request: Request) -> Any:
        return self.do_open(_IPv4HTTPConnection, request)


class _IPv4HTTPSHandler(HTTPSHandler):
    def https_open(self, request: Request) -> Any:
        http_connection_args: dict[str, Any] = {}
        if (context := getattr(self, "_context", None)) is not None:
            http_connection_args["context"] = context
        if (check_hostname := getattr(self, "_check_hostname", None)) is not None:
            http_connection_args["check_hostname"] = check_hostname
        return self.do_open(
            _IPv4HTTPSConnection,
            request,
            **http_connection_args,
        )


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
            force_ipv4=self.settings.force_ipv4,
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
    header = _format_telegram_header(notification)
    body = _format_telegram_body(notification)
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


def _format_telegram_header(notification: NotificationMessage) -> str:
    severity = _TELEGRAM_SEVERITY_LABELS.get(
        notification.severity,
        notification.severity.value.upper(),
    )
    subject = _localize_telegram_subject(notification.subject)
    return f"[{severity}] {subject}"


def _format_telegram_body(notification: NotificationMessage) -> str:
    lines = [
        _localize_telegram_fragment(
            f"created_at={notification.created_at.isoformat()}",
            value_separator=": ",
        )
    ]
    lines.extend(
        _localize_telegram_line(line) for line in notification.body.splitlines()
    )
    return "\n".join(lines)


def _localize_telegram_subject(subject: str) -> str:
    if match := _ORDER_SUBJECT_PATTERN.match(subject):
        return (
            f"주문 알림 {_display_symbol(match.group('symbol'))} "
            f"[{_localize_value('status', match.group('status'))}]"
        )
    if match := _FILL_SUBJECT_PATTERN.match(subject):
        return (
            f"체결 알림 {_display_symbol(match.group('symbol'))} "
            f"[{match.group('quantity')}주 @ {match.group('price')}]"
        )
    if match := _RISK_BLOCK_SUBJECT_PATTERN.match(subject):
        return f"리스크 차단 {_display_symbol(match.group('symbol'))}"
    if match := _DAILY_REPORT_SUBJECT_PATTERN.match(subject):
        return (
            f"일일 리포트 {match.group('trading_day')} "
            f"[{_localize_value('status', match.group('status'))}]"
        )
    if match := _WEEKLY_REVIEW_SUBJECT_PATTERN.match(subject):
        return (
            "주간 리뷰 "
            f"{match.group('week_start')}~{match.group('week_end')} "
            f"[{_localize_value('status', match.group('status'))}]"
        )
    if match := _MARKET_OPEN_SUBJECT_PATTERN.match(subject):
        return (
            f"장 시작 준비 {match.group('trading_day')} "
            f"[{_localize_value('status', match.group('status'))}]"
        )
    if match := _SAFE_STOP_SUBJECT_PATTERN.match(subject):
        return f"러너 안전 정지 [{_localize_reason_text(match.group('reason'))}]"
    return _replace_symbols(subject)


def _localize_telegram_line(line: str) -> str:
    if not line:
        return ""
    if header := _TELEGRAM_HEADER_LABELS.get(line):
        return header
    if line.startswith("- "):
        return f"- {_localize_telegram_line(line[2:])}"
    if line.startswith("violation=") and " message=" in line:
        violation, message = line.split(" message=", maxsplit=1)
        return " / ".join(
            (
                _localize_telegram_fragment(violation, value_separator=": "),
                f"{_TELEGRAM_FIELD_LABELS['message']}: {_localize_reason_text(message)}",
            )
        )
    if ":" in line and "=" in line and " " not in line:
        return ":".join(
            _localize_telegram_fragment(fragment) for fragment in line.split(":")
        )
    if "=" in line and line.count("=") == 1:
        return _localize_telegram_fragment(line, value_separator=": ")
    return _localize_telegram_fragment_sequence(line)


def _localize_telegram_fragment_sequence(text: str) -> str:
    tokens = text.split()
    localized_tokens: list[str] = []
    for token in tokens:
        if "=" in token:
            localized_tokens.append(_localize_telegram_fragment(token))
            continue
        if _looks_like_symbol(token):
            localized_tokens.append(_display_symbol(token))
            continue
        localized_tokens.append(token)
    return " ".join(localized_tokens)


def _localize_telegram_fragment(
    fragment: str,
    *,
    value_separator: str = "=",
) -> str:
    if "=" not in fragment:
        return _replace_symbols(fragment)
    key, value = fragment.split("=", maxsplit=1)
    label = _TELEGRAM_FIELD_LABELS.get(key, key)
    return f"{label}{value_separator}{_localize_value(key, value)}"


def _localize_value(key: str, value: str) -> str:
    normalized = value.strip()
    if key in {"symbol"}:
        return _display_symbol(normalized)
    if key in {"targets"}:
        return ", ".join(
            _display_symbol(item) for item in normalized.split(",") if item
        )
    if key in {"attention_reasons", "failure_reasons"}:
        return ", ".join(
            _localize_reason_text(item) for item in normalized.split(",") if item
        )
    if key in {"reason", "detail", "signal_reason", "message"}:
        return _localize_reason_text(normalized)
    if key in {
        "allowed",
        "emergency_stop",
        "refreshed",
        "smoke_success",
        "trading_halted",
    }:
        lowered = normalized.casefold()
        if lowered == "true":
            return "예"
        if lowered == "false":
            return "아니오"
        return normalized
    if key in {"action", "signal", "status"}:
        return _TELEGRAM_VALUE_LABELS.get(normalized, normalized)
    return _replace_symbols(_TELEGRAM_VALUE_LABELS.get(normalized, normalized))


def _localize_reason_text(value: str) -> str:
    if "," in value and " " not in value:
        return ", ".join(
            _TELEGRAM_REASON_LABELS.get(item, _replace_symbols(item))
            for item in value.split(",")
        )
    return _TELEGRAM_REASON_LABELS.get(value, _replace_symbols(value))


def _display_symbol(symbol: str) -> str:
    normalized = symbol.strip()
    if not _looks_like_symbol(normalized):
        return normalized
    name = _symbol_name_map().get(normalized)
    if name is None:
        return normalized
    return f"{name}({normalized})"


def _looks_like_symbol(value: str) -> bool:
    return bool(_SYMBOL_PATTERN.fullmatch(value))


def _replace_symbols(text: str) -> str:
    return _SYMBOL_PATTERN.sub(lambda match: _display_symbol(match.group(0)), text)


@lru_cache(maxsize=1)
def _symbol_name_map() -> dict[str, str]:
    return _load_symbol_name_map()


def _load_symbol_name_map() -> dict[str, str]:
    records_by_symbol: dict[str, str] = {}
    sources = (
        (DEFAULT_KIS_RAW_DIR / "kospi_code.mst", load_kospi_master_records),
        (DEFAULT_KIS_RAW_DIR / "kosdaq_code.mst", load_kosdaq_master_records),
        (DEFAULT_KIS_RAW_DIR / "konex_code.mst", load_konex_master_records),
    )
    for path, loader in sources:
        if not path.is_file():
            continue
        try:
            for record in loader(path):
                records_by_symbol.setdefault(record.symbol, record.name)
        except Exception:
            logger.debug("심볼명 메타데이터 로딩에 실패했습니다. path=%s", path)
    return records_by_symbol


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
