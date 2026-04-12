from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from decimal import InvalidOperation
from json import JSONDecodeError
from pathlib import Path
from typing import Final
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from autotrade.broker.normalization import normalize_holding
from autotrade.broker.normalization import normalize_order_capacity
from autotrade.broker.normalization import normalize_quote
from autotrade.broker.readers import BrokerReader
from autotrade.common import Holding
from autotrade.common import OrderCapacity
from autotrade.common import Quote
from autotrade.config.models import BrokerSettings

PAPER_BASE_URL: Final[str] = "https://openapivts.koreainvestment.com:29443"
LIVE_BASE_URL: Final[str] = "https://openapi.koreainvestment.com:9443"
KST: Final[ZoneInfo] = ZoneInfo("Asia/Seoul")
LIVE_ACCOUNT_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\d{8}-\d{2}$|^\d{10}$")
PAPER_ACCOUNT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\d{8}$|^\d{8}-\d{2}$|^\d{10}$"
)
PAPER_PRODUCT_CODE: Final[str] = "01"
TOKEN_CACHE_REFRESH_BUFFER: Final[timedelta] = timedelta(minutes=1)


@dataclass(frozen=True, slots=True)
class HttpRequest:
    method: str
    url: str
    headers: Mapping[str, str]
    body: bytes | None = None
    timeout: float = 30.0


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


class KoreaInvestmentBrokerError(RuntimeError):
    """Raised when the Korea Investment HTTP API cannot be used safely."""


HttpTransport = Callable[[HttpRequest], HttpResponse]


@dataclass(frozen=True, slots=True)
class CachedAccessToken:
    access_token: str
    expires_at: datetime | None


class KoreaInvestmentBrokerReader(BrokerReader):
    def __init__(
        self,
        settings: BrokerSettings,
        *,
        transport: HttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        token_cache_path: Path | None = None,
    ) -> None:
        self._settings = settings
        self._transport = transport or _urllib_transport
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._access_token: str | None = None
        self._access_token_expires_at: datetime | None = None
        self._base_url = (
            PAPER_BASE_URL if settings.environment == "paper" else LIVE_BASE_URL
        )
        self._token_cache_path = token_cache_path or _default_token_cache_path(settings)
        self._quote_tr_id = "FHKST01010100"
        self._balance_tr_id = (
            "VTTC8434R" if settings.environment == "paper" else "TTTC8434R"
        )
        self._order_capacity_tr_id = (
            "VTTC8908R" if settings.environment == "paper" else "TTTC8908R"
        )

    def get_quote(self, symbol: str) -> Quote:
        payload = self._request_json(
            "GET",
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": symbol,
            },
            tr_id=self._quote_tr_id,
        )
        output = _require_mapping(payload, "output")
        normalized_payload = {
            "symbol": symbol,
            "price": output.get("stck_prpr"),
            "as_of": _resolve_as_of(output, self._clock()),
            "currency": "KRW",
        }
        return normalize_quote(normalized_payload)

    def get_holdings(self) -> tuple[Holding, ...]:
        cano, acnt_prdt_cd = _split_account(
            self._settings.account,
            environment=self._settings.environment,
        )
        payload = self._request_json(
            "GET",
            "/uapi/domestic-stock/v1/trading/inquire-balance",
            params={
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "00",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
            tr_id=self._balance_tr_id,
        )
        rows = _require_sequence(payload, "output1")
        holdings: list[Holding] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise KoreaInvestmentBrokerError("output1 entries must be mappings")

            quantity = _coerce_int(row.get("hldg_qty"), field_name="hldg_qty")
            if quantity <= 0:
                continue

            holdings.append(
                normalize_holding(
                    {
                        "symbol": _coerce_string(row.get("pdno"), field_name="pdno"),
                        "quantity": quantity,
                        "average_price": row.get("pchs_avg_pric"),
                        "current_price": row.get("prpr"),
                    },
                ),
            )
        return tuple(sorted(holdings, key=lambda holding: holding.symbol))

    def get_order_capacity(
        self,
        symbol: str,
        order_price: Decimal,
    ) -> OrderCapacity:
        cano, acnt_prdt_cd = _split_account(
            self._settings.account,
            environment=self._settings.environment,
        )
        payload = self._request_json(
            "GET",
            "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            params={
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "PDNO": symbol,
                "ORD_UNPR": str(order_price),
                "ORD_DVSN": "01",
                "CMA_EVLU_AMT_ICLD_YN": "N",
                "OVRS_ICLD_YN": "N",
            },
            tr_id=self._order_capacity_tr_id,
        )
        output = _require_mapping(payload, "output")
        cash_available_raw = output.get("ord_psbl_cash")
        if cash_available_raw is None:
            cash_available_raw = output.get("nrcvb_buy_amt")
        if cash_available_raw is None:
            cash_available_raw = output.get("cash_available")
        cash_available = _coerce_decimal(
            cash_available_raw,
            field_name="ord_psbl_cash",
        )
        max_orderable_quantity_raw = output.get("nrcvb_buy_qty")
        if max_orderable_quantity_raw is None:
            max_orderable_quantity_raw = output.get("max_buy_qty")
        if max_orderable_quantity_raw is None:
            max_orderable_quantity_raw = output.get("max_orderable_quantity")
        if max_orderable_quantity_raw is None:
            max_orderable_quantity_raw = output.get("ord_psbl_qty")
        max_orderable_quantity = _coerce_optional_int(max_orderable_quantity_raw)
        if max_orderable_quantity is None:
            max_orderable_quantity = (
                0 if order_price <= 0 else int(cash_available // order_price)
            )

        return normalize_order_capacity(
            {
                "symbol": symbol,
                "order_price": order_price,
                "max_orderable_quantity": max_orderable_quantity,
                "cash_available": cash_available,
            },
        )

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        body: Mapping[str, object] | None = None,
        tr_id: str | None = None,
    ) -> Mapping[str, object]:
        headers: dict[str, str] = {
            "content-type": "application/json; charset=utf-8",
            "appkey": self._settings.api_key,
            "appsecret": self._settings.api_secret,
        }
        if tr_id is not None:
            headers["tr_id"] = tr_id
        if method.upper() != "POST":
            headers["custtype"] = "P"
        if path != "/oauth2/tokenP":
            headers["authorization"] = (
                f"Bearer {self._access_token or self._get_access_token()}"
            )

        request = HttpRequest(
            method=method.upper(),
            url=_build_url(self._base_url, path, params),
            headers=headers,
            body=(
                json.dumps(body, separators=(",", ":")).encode("utf-8")
                if body is not None
                else None
            ),
        )
        response = self._transport(request)
        if response.status >= 400:
            raise KoreaInvestmentBrokerError(
                _format_http_error(
                    status=response.status,
                    method=request.method,
                    url=request.url,
                    body=response.body,
                )
            )

        payload = _decode_json(response.body)
        _raise_for_api_error(payload)
        return payload

    def _get_access_token(self) -> str:
        if self._has_usable_access_token():
            assert self._access_token is not None
            return self._access_token

        cached_token = self._load_access_token_from_cache()
        if cached_token is not None:
            self._access_token = cached_token.access_token
            self._access_token_expires_at = cached_token.expires_at
            return cached_token.access_token

        payload = self._request_json(
            "POST",
            "/oauth2/tokenP",
            body={
                "grant_type": "client_credentials",
                "appkey": self._settings.api_key,
                "appsecret": self._settings.api_secret,
            },
        )
        access_token = payload.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            raise KoreaInvestmentBrokerError("token response missing access_token")

        self._access_token = access_token.strip()
        self._access_token_expires_at = _resolve_access_token_expiration(
            payload,
            now=self._clock(),
        )
        self._write_access_token_cache(
            CachedAccessToken(
                access_token=self._access_token,
                expires_at=self._access_token_expires_at,
            )
        )
        return self._access_token

    def _has_usable_access_token(self) -> bool:
        if self._access_token is None:
            return False
        if self._access_token_expires_at is None:
            return True
        return not _is_token_expired(
            self._access_token_expires_at,
            now=self._clock(),
        )

    def _load_access_token_from_cache(self) -> CachedAccessToken | None:
        try:
            raw_cache = self._token_cache_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError:
            return None

        try:
            decoded = json.loads(raw_cache)
        except JSONDecodeError:
            return None

        if not isinstance(decoded, Mapping):
            return None

        access_token = decoded.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            return None

        expires_at = _parse_cached_datetime(decoded.get("expires_at"))
        if expires_at is None or _is_token_expired(expires_at, now=self._clock()):
            return None

        return CachedAccessToken(
            access_token=access_token.strip(),
            expires_at=expires_at,
        )

    def _write_access_token_cache(self, cached_token: CachedAccessToken) -> None:
        if cached_token.expires_at is None:
            return

        payload = {
            "access_token": cached_token.access_token,
            "expires_at": cached_token.expires_at.isoformat(),
        }
        cache_directory = self._token_cache_path.parent
        temporary_path = self._token_cache_path.with_suffix(".tmp")

        try:
            cache_directory.mkdir(parents=True, exist_ok=True)
            temporary_path.write_text(
                json.dumps(payload, separators=(",", ":")),
                encoding="utf-8",
            )
            temporary_path.replace(self._token_cache_path)
        except OSError:
            return


def _urllib_transport(request: HttpRequest) -> HttpResponse:
    try:
        urllib_request = Request(
            request.url,
            data=request.body,
            headers=dict(request.headers),
            method=request.method,
        )
        with urlopen(urllib_request, timeout=request.timeout) as response:
            return HttpResponse(
                status=response.getcode(),
                headers=dict(response.headers.items()),
                body=response.read(),
            )
    except HTTPError as error:
        return HttpResponse(
            status=error.code,
            headers=dict(error.headers.items()) if error.headers is not None else {},
            body=error.read(),
        )


def _build_url(
    base_url: str,
    path: str,
    params: Mapping[str, str] | None,
) -> str:
    if not params:
        return f"{base_url}{path}"
    return f"{base_url}{path}?{urlencode(params)}"


def _decode_json(raw_body: bytes) -> Mapping[str, object]:
    try:
        decoded = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, JSONDecodeError) as error:
        raise KoreaInvestmentBrokerError("response body is not valid JSON") from error

    if not isinstance(decoded, Mapping):
        raise KoreaInvestmentBrokerError("response JSON must be an object")
    return decoded


def _raise_for_api_error(payload: Mapping[str, object]) -> None:
    rt_cd = payload.get("rt_cd")
    if isinstance(rt_cd, str) and rt_cd.strip() and rt_cd != "0":
        raise KoreaInvestmentBrokerError(_format_kis_payload_error(payload))


def _format_http_error(
    *,
    status: int,
    method: str,
    url: str,
    body: bytes,
) -> str:
    details = _format_http_error_body(body)
    if details is None:
        return f"HTTP {status} from {method} {url}"
    return f"HTTP {status} from {method} {url}: {details}"


def _format_http_error_body(body: bytes) -> str | None:
    payload = _try_decode_json(body)
    if payload is not None:
        return _format_kis_payload_error(payload)

    raw_body = body.decode("utf-8", errors="replace").strip()
    if not raw_body:
        return None
    return raw_body


def _try_decode_json(raw_body: bytes) -> Mapping[str, object] | None:
    try:
        decoded = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, JSONDecodeError):
        return None

    if not isinstance(decoded, Mapping):
        return None
    return decoded


def _format_kis_payload_error(payload: Mapping[str, object]) -> str:
    msg_cd = payload.get("msg_cd")
    msg1 = payload.get("msg1")
    error_description = payload.get("error_description")

    details: list[str] = []
    if isinstance(msg_cd, str) and msg_cd.strip():
        details.append(msg_cd.strip())
    if isinstance(msg1, str) and msg1.strip():
        details.append(msg1.strip())
    if isinstance(error_description, str) and error_description.strip():
        details.append(error_description.strip())

    if details:
        return " - ".join(details)
    return "KIS request failed"


def _resolve_access_token_expiration(
    payload: Mapping[str, object],
    *,
    now: datetime,
) -> datetime | None:
    expiration_value = payload.get("access_token_token_expired")
    if isinstance(expiration_value, str) and expiration_value.strip():
        try:
            return datetime.strptime(
                expiration_value.strip(),
                "%Y-%m-%d %H:%M:%S",
            ).replace(tzinfo=KST)
        except ValueError:
            return None

    expires_in_seconds = _coerce_optional_int(payload.get("expires_in"))
    if expires_in_seconds is None:
        return None
    return now + timedelta(seconds=expires_in_seconds)


def _parse_cached_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _is_token_expired(expires_at: datetime, *, now: datetime) -> bool:
    return expires_at <= now + TOKEN_CACHE_REFRESH_BUFFER


def _default_token_cache_path(settings: BrokerSettings) -> Path:
    cache_root = os.environ.get("XDG_CACHE_HOME")
    base_directory = (
        Path(cache_root).expanduser() if cache_root else Path.home() / ".cache"
    )
    cache_key = hashlib.sha256(
        f"{settings.environment}:{settings.api_key}:{settings.account}".encode("utf-8")
    ).hexdigest()
    return (
        base_directory
        / "autotrade"
        / "korea_investment"
        / f"access-token-{cache_key[:16]}.json"
    )


def _resolve_as_of(output: Mapping[str, object], fallback: datetime) -> datetime:
    base_date = output.get("stck_bsop_date")
    current_time = output.get("stck_cntg_hour")
    if isinstance(base_date, str) and isinstance(current_time, str):
        normalized_date = base_date.strip()
        normalized_time = current_time.strip()
        if len(normalized_date) == 8 and len(normalized_time) == 6:
            try:
                parsed = datetime.strptime(
                    normalized_date + normalized_time,
                    "%Y%m%d%H%M%S",
                )
            except ValueError:
                return fallback
            return parsed.replace(tzinfo=KST)
    return fallback


def _require_mapping(
    payload: Mapping[str, object],
    key: str,
) -> Mapping[str, object]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise KoreaInvestmentBrokerError(f"{key} must be an object")
    return value


def _require_sequence(
    payload: Mapping[str, object],
    key: str,
) -> Sequence[object]:
    value = payload.get(key)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise KoreaInvestmentBrokerError(f"{key} must be an array")
    return value


def _coerce_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise KoreaInvestmentBrokerError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise KoreaInvestmentBrokerError(f"{field_name} must not be blank")
    return normalized


def _coerce_decimal(value: object, *, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return Decimal(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise KoreaInvestmentBrokerError(f"{field_name} must not be blank")
        try:
            return Decimal(normalized)
        except InvalidOperation as error:
            raise KoreaInvestmentBrokerError(
                f"{field_name} must be decimal-compatible"
            ) from error
    raise KoreaInvestmentBrokerError(f"{field_name} must be decimal-compatible")


def _coerce_int(value: object, *, field_name: str) -> int:
    normalized = _coerce_optional_int(value)
    if normalized is None:
        raise KoreaInvestmentBrokerError(f"{field_name} must be an integer")
    return normalized


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            return None
        return int(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            return int(normalized)
        except ValueError:
            return None
    return None


def _split_account(account: str, *, environment: str) -> tuple[str, str]:
    normalized = account.strip()
    if not normalized:
        raise KoreaInvestmentBrokerError("account must not be blank")

    account_pattern = (
        PAPER_ACCOUNT_PATTERN if environment == "paper" else LIVE_ACCOUNT_PATTERN
    )
    if account_pattern.fullmatch(normalized) is None:
        if environment == "paper":
            raise KoreaInvestmentBrokerError(
                "paper account must be 8 digits, or 8 digits + 2 digits with optional hyphen",
            )
        raise KoreaInvestmentBrokerError(
            "live account must be 8 digits + 2 digits, with optional hyphen",
        )

    if "-" in normalized:
        cano, acnt_prdt_cd = normalized.split("-", 1)
    elif len(normalized) == 8:
        cano, acnt_prdt_cd = normalized, PAPER_PRODUCT_CODE
    else:
        cano, acnt_prdt_cd = normalized[:8], normalized[8:]
    return cano, acnt_prdt_cd
