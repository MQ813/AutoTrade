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
from autotrade.broker.trading import BrokerTrader
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import Holding
from autotrade.common import OrderAmendRequest
from autotrade.common import OrderCapacity
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.common import OrderType
from autotrade.common import Quote
from autotrade.config.models import BrokerSettings

PAPER_BASE_URL: Final[str] = "https://openapivts.koreainvestment.com:29443"
LIVE_BASE_URL: Final[str] = "https://openapi.koreainvestment.com:9443"
KST: Final[ZoneInfo] = ZoneInfo("Asia/Seoul")
LIVE_ACCOUNT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\d{8}$|^\d{8}-\d{2}$|^\d{10}$"
)
PAPER_ACCOUNT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\d{8}$|^\d{8}-\d{2}$|^\d{10}$"
)
PAPER_PRODUCT_CODE: Final[str] = "01"
TOKEN_CACHE_REFRESH_BUFFER: Final[timedelta] = timedelta(minutes=1)
KIS_TOKEN_PATH: Final[str] = "/oauth2/tokenP"
KIS_HASHKEY_PATH: Final[str] = "/uapi/hashkey"
KIS_QUOTE_PATH: Final[str] = "/uapi/domestic-stock/v1/quotations/inquire-price"
KIS_BALANCE_PATH: Final[str] = "/uapi/domestic-stock/v1/trading/inquire-balance"
KIS_ORDER_CAPACITY_PATH: Final[str] = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
KIS_ORDER_PATH: Final[str] = "/uapi/domestic-stock/v1/trading/order-cash"
KIS_ORDER_AMEND_CANCEL_PATH: Final[str] = (
    "/uapi/domestic-stock/v1/trading/order-rvsecncl"
)
KIS_ORDER_HISTORY_PATH: Final[str] = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
KIS_LIMIT_ORDER_DIVISION: Final[str] = "00"


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


class _KoreaInvestmentApiClient:
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
        self._cano, self._account_product_code = _split_account(
            settings.account,
            environment=settings.environment,
        )

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        body: Mapping[str, object] | None = None,
        tr_id: str | None = None,
        require_hashkey: bool = False,
    ) -> Mapping[str, object]:
        method_name = method.upper()
        request = HttpRequest(
            method=method_name,
            url=_build_url(self._base_url, path, params),
            headers=self._build_headers(
                method=method_name,
                path=path,
                tr_id=tr_id,
                hashkey_body=body if require_hashkey else None,
            ),
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

    def _build_headers(
        self,
        *,
        method: str,
        path: str,
        tr_id: str | None,
        hashkey_body: Mapping[str, object] | None,
    ) -> dict[str, str]:
        headers: dict[str, str] = {
            "content-type": "application/json; charset=utf-8",
            "appkey": self._settings.api_key,
            "appsecret": self._settings.api_secret,
        }
        if tr_id is not None:
            headers["tr_id"] = tr_id
        if path != KIS_TOKEN_PATH:
            headers["authorization"] = (
                f"Bearer {self._access_token or self._get_access_token()}"
            )
            headers["custtype"] = "P"
        if hashkey_body is not None:
            headers["hashkey"] = self._request_hashkey(hashkey_body)
        if method != "POST":
            headers["custtype"] = "P"
        return headers

    def _request_hashkey(self, body: Mapping[str, object]) -> str:
        payload = self._request_json("POST", KIS_HASHKEY_PATH, body=body)
        hashkey = payload.get("HASH")
        if not isinstance(hashkey, str) or not hashkey.strip():
            raise KoreaInvestmentBrokerError("hashkey response missing HASH")
        return hashkey.strip()

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
            KIS_TOKEN_PATH,
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


class KoreaInvestmentBrokerReader(_KoreaInvestmentApiClient, BrokerReader):
    def __init__(
        self,
        settings: BrokerSettings,
        *,
        transport: HttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        token_cache_path: Path | None = None,
    ) -> None:
        super().__init__(
            settings,
            transport=transport,
            clock=clock,
            token_cache_path=token_cache_path,
        )
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
            KIS_QUOTE_PATH,
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
        payload = self._request_json(
            "GET",
            KIS_BALANCE_PATH,
            params={
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._account_product_code,
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
        payload = self._request_json(
            "GET",
            KIS_ORDER_CAPACITY_PATH,
            params={
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._account_product_code,
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


class KoreaInvestmentBrokerTrader(_KoreaInvestmentApiClient, BrokerTrader):
    def __init__(
        self,
        settings: BrokerSettings,
        *,
        transport: HttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        token_cache_path: Path | None = None,
    ) -> None:
        super().__init__(
            settings,
            transport=transport,
            clock=clock,
            token_cache_path=token_cache_path,
        )
        self._buy_order_tr_id = (
            "VTTC0802U" if settings.environment == "paper" else "TTTC0802U"
        )
        self._sell_order_tr_id = (
            "VTTC0801U" if settings.environment == "paper" else "TTTC0801U"
        )
        self._amend_cancel_tr_id = (
            "VTTC0803U" if settings.environment == "paper" else "TTTC0803U"
        )
        self._order_history_tr_id = (
            "VTTC8001R" if settings.environment == "paper" else "TTTC8001R"
        )

    def submit_order(self, request: OrderRequest) -> ExecutionOrder:
        if request.order_type is not OrderType.LIMIT:
            raise KoreaInvestmentBrokerError(
                f"unsupported order_type={request.order_type}"
            )

        payload = self._request_json(
            "POST",
            KIS_ORDER_PATH,
            body={
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._account_product_code,
                "PDNO": request.symbol,
                "ORD_DVSN": KIS_LIMIT_ORDER_DIVISION,
                "ORD_QTY": str(request.quantity),
                "ORD_UNPR": _format_order_price(request.limit_price),
            },
            tr_id=(
                self._buy_order_tr_id
                if request.side is OrderSide.BUY
                else self._sell_order_tr_id
            ),
            require_hashkey=True,
        )
        output = _require_mapping(payload, "output")
        order_id = _coerce_string(output.get("ODNO"), field_name="ODNO")
        return ExecutionOrder(
            order_id=order_id,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            limit_price=request.limit_price,
            status=OrderStatus.ACKNOWLEDGED,
            created_at=request.requested_at,
            updated_at=request.requested_at,
        )

    def amend_order(self, request: OrderAmendRequest) -> ExecutionOrder:
        current_record = self._require_order_record(
            request.order_id,
            reference_time=request.requested_at,
        )
        current_order = _record_to_execution_order(
            current_record,
            fallback_order_id=request.order_id,
            fallback_time=request.requested_at,
        )
        next_quantity = request.quantity or current_order.quantity
        next_limit_price = request.limit_price or current_order.limit_price
        amend_all_remaining = request.quantity is None
        payload = self._request_json(
            "POST",
            KIS_ORDER_AMEND_CANCEL_PATH,
            body={
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._account_product_code,
                "KRX_FWDG_ORD_ORGNO": _resolve_order_branch(current_record),
                "ORGN_ODNO": request.order_id,
                "ORD_DVSN": _resolve_order_division(current_record),
                "RVSE_CNCL_DVSN_CD": "01",
                "ORD_QTY": "0" if amend_all_remaining else str(next_quantity),
                "ORD_UNPR": _format_order_price(next_limit_price),
                "QTY_ALL_ORD_YN": "Y" if amend_all_remaining else "N",
            },
            tr_id=self._amend_cancel_tr_id,
            require_hashkey=True,
        )
        output = _require_mapping(payload, "output")
        amended_order_id = _optional_output_string(output.get("ODNO")) or request.order_id
        return ExecutionOrder(
            order_id=amended_order_id,
            symbol=current_order.symbol,
            side=current_order.side,
            quantity=next_quantity,
            limit_price=next_limit_price,
            status=OrderStatus.ACKNOWLEDGED,
            created_at=current_order.created_at,
            updated_at=request.requested_at,
            filled_quantity=current_order.filled_quantity,
        )

    def cancel_order(self, request: OrderCancelRequest) -> ExecutionOrder:
        current_record = self._require_order_record(
            request.order_id,
            reference_time=request.requested_at,
        )
        current_order = _record_to_execution_order(
            current_record,
            fallback_order_id=request.order_id,
            fallback_time=request.requested_at,
        )
        payload = self._request_json(
            "POST",
            KIS_ORDER_AMEND_CANCEL_PATH,
            body={
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._account_product_code,
                "KRX_FWDG_ORD_ORGNO": _resolve_order_branch(current_record),
                "ORGN_ODNO": request.order_id,
                "ORD_DVSN": _resolve_order_division(current_record),
                "RVSE_CNCL_DVSN_CD": "02",
                "ORD_QTY": "0",
                "ORD_UNPR": "0",
                "QTY_ALL_ORD_YN": "Y",
            },
            tr_id=self._amend_cancel_tr_id,
            require_hashkey=True,
        )
        output = _require_mapping(payload, "output")
        canceled_order_id = (
            _optional_output_string(output.get("ODNO")) or request.order_id
        )
        return ExecutionOrder(
            order_id=canceled_order_id,
            symbol=current_order.symbol,
            side=current_order.side,
            quantity=current_order.quantity,
            limit_price=current_order.limit_price,
            status=OrderStatus.CANCELED,
            created_at=current_order.created_at,
            updated_at=request.requested_at,
            filled_quantity=current_order.filled_quantity,
        )

    def get_fills(self, order_id: str) -> tuple[ExecutionFill, ...]:
        record = self._require_order_record(order_id, reference_time=self._clock())
        filled_quantity = _coerce_optional_int(record.get("tot_ccld_qty")) or 0
        if filled_quantity <= 0:
            return ()

        average_price_raw = record.get("avg_prvs")
        if _is_blank_value(average_price_raw):
            average_price_raw = record.get("ord_unpr")
        return (
            ExecutionFill(
                fill_id=f"{order_id}:aggregate",
                order_id=order_id,
                symbol=_coerce_string(record.get("pdno"), field_name="pdno"),
                quantity=filled_quantity,
                price=_coerce_decimal(average_price_raw, field_name="avg_prvs"),
                filled_at=_parse_order_timestamp(record, fallback=self._clock()),
            ),
        )

    def _require_order_record(
        self,
        order_id: str,
        *,
        reference_time: datetime,
    ) -> Mapping[str, object]:
        records = self._load_order_records(reference_time)
        normalized_order_id = order_id.strip()
        exact_match = _find_order_record_by_field(
            records,
            field_name="odno",
            value=normalized_order_id,
        )
        if exact_match is not None:
            return exact_match
        origin_match = _find_order_record_by_field(
            records,
            field_name="orgn_odno",
            value=normalized_order_id,
        )
        if origin_match is not None:
            return origin_match
        raise KoreaInvestmentBrokerError(f"order history missing order_id={order_id}")

    def _load_order_records(
        self,
        reference_time: datetime,
    ) -> tuple[Mapping[str, object], ...]:
        inquiry_date = reference_time.astimezone(KST).strftime("%Y%m%d")
        payload = self._request_json(
            "GET",
            KIS_ORDER_HISTORY_PATH,
            params={
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._account_product_code,
                "INQR_STRT_DT": inquiry_date,
                "INQR_END_DT": inquiry_date,
                "SLL_BUY_DVSN_CD": "00",
                "INQR_DVSN": "00",
                "PDNO": "",
                "CCLD_DVSN": "00",
                "ORD_GNO_BRNO": "",
                "ODNO": "",
                "INQR_DVSN_3": "00",
                "INQR_DVSN_1": "",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
            tr_id=self._order_history_tr_id,
        )
        rows = _require_sequence(payload, "output1")
        records = tuple(
            row
            for row in rows
            if isinstance(row, Mapping)
            and not _is_blank_value(row.get("odno"))
        )
        return tuple(
            sorted(
                records,
                key=lambda record: _parse_order_timestamp(
                    record,
                    fallback=reference_time,
                ),
                reverse=True,
            )
        )


def _is_blank_value(value: object) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _optional_output_string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _find_order_record_by_field(
    records: Sequence[Mapping[str, object]],
    *,
    field_name: str,
    value: str,
) -> Mapping[str, object] | None:
    for record in records:
        if _optional_output_string(record.get(field_name)) == value:
            return record
    return None


def _format_order_price(value: Decimal) -> str:
    if value != value.to_integral_value():
        raise KoreaInvestmentBrokerError("order price must be an integer KRW amount")
    return str(int(value))


def _parse_order_timestamp(
    record: Mapping[str, object],
    *,
    fallback: datetime,
) -> datetime:
    order_date = _optional_output_string(record.get("ord_dt"))
    order_time = _optional_output_string(record.get("ord_tmd"))
    if order_date is None or order_time is None:
        return fallback

    normalized_time = order_time.zfill(6)
    if len(order_date) != 8 or len(normalized_time) != 6:
        return fallback

    try:
        parsed = datetime.strptime(order_date + normalized_time, "%Y%m%d%H%M%S")
    except ValueError:
        return fallback
    return parsed.replace(tzinfo=KST)


def _resolve_order_side(record: Mapping[str, object]) -> OrderSide:
    side_code = _optional_output_string(record.get("sll_buy_dvsn_cd"))
    if side_code == "02":
        return OrderSide.BUY
    if side_code == "01":
        return OrderSide.SELL

    side_name = _optional_output_string(record.get("sll_buy_dvsn_cd_name"))
    if side_name == "매수":
        return OrderSide.BUY
    if side_name == "매도":
        return OrderSide.SELL
    raise KoreaInvestmentBrokerError("order history missing side")


def _resolve_order_status(
    record: Mapping[str, object],
    *,
    quantity: int,
) -> OrderStatus:
    filled_quantity = _coerce_optional_int(record.get("tot_ccld_qty")) or 0
    rejected_quantity = _coerce_optional_int(record.get("rjct_qty")) or 0
    canceled = _optional_output_string(record.get("cncl_yn")) == "Y"

    if rejected_quantity >= quantity and filled_quantity == 0:
        return OrderStatus.REJECTED
    if canceled:
        return OrderStatus.CANCELED
    if filled_quantity >= quantity:
        return OrderStatus.FILLED
    if filled_quantity > 0:
        return OrderStatus.PARTIALLY_FILLED
    return OrderStatus.ACKNOWLEDGED


def _resolve_order_branch(record: Mapping[str, object]) -> str:
    return (
        _optional_output_string(record.get("ord_gno_brno"))
        or _optional_output_string(record.get("ord_orgno"))
        or ""
    )


def _resolve_order_division(record: Mapping[str, object]) -> str:
    return _optional_output_string(record.get("ord_dvsn_cd")) or KIS_LIMIT_ORDER_DIVISION


def _record_to_execution_order(
    record: Mapping[str, object],
    *,
    fallback_order_id: str,
    fallback_time: datetime,
) -> ExecutionOrder:
    quantity = _coerce_int(record.get("ord_qty"), field_name="ord_qty")
    created_at = _parse_order_timestamp(record, fallback=fallback_time)
    return ExecutionOrder(
        order_id=_optional_output_string(record.get("odno")) or fallback_order_id,
        symbol=_coerce_string(record.get("pdno"), field_name="pdno"),
        side=_resolve_order_side(record),
        quantity=quantity,
        limit_price=_coerce_decimal(record.get("ord_unpr"), field_name="ord_unpr"),
        status=_resolve_order_status(record, quantity=quantity),
        created_at=created_at,
        updated_at=created_at,
        filled_quantity=min(
            _coerce_optional_int(record.get("tot_ccld_qty")) or 0,
            quantity,
        ),
    )


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
            "live account must be 8 digits, or 8 digits + 2 digits with optional hyphen",
        )

    if "-" in normalized:
        cano, acnt_prdt_cd = normalized.split("-", 1)
    elif len(normalized) == 8:
        cano, acnt_prdt_cd = normalized, PAPER_PRODUCT_CODE
    else:
        cano, acnt_prdt_cd = normalized[:8], normalized[8:]
    return cano, acnt_prdt_cd
