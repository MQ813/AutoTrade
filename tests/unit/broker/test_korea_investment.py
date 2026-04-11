from __future__ import annotations

import json
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import pytest

from autotrade.broker import BrokerReader
from autotrade.broker.korea_investment import HttpRequest
from autotrade.broker.korea_investment import HttpResponse
from autotrade.broker.korea_investment import KoreaInvestmentBrokerError
from autotrade.broker.korea_investment import KoreaInvestmentBrokerReader
from autotrade.broker.korea_investment import _split_account
from autotrade.common import Holding
from autotrade.common import OrderCapacity
from autotrade.common import Quote
from autotrade.config.models import BrokerEnvironment
from autotrade.config import BrokerSettings


def test_korea_investment_broker_reader_returns_standard_models() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"access_token": "token-123"},
            ),
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345.67",
                    },
                },
            ),
            ("GET", "/uapi/domestic-stock/v1/trading/inquire-balance"): json_response(
                {
                    "rt_cd": "0",
                    "output1": [
                        {
                            "pdno": "357870",
                            "hldg_qty": "2",
                            "pchs_avg_pric": "10000",
                            "prpr": "10100",
                        },
                        {
                            "pdno": "069500",
                            "hldg_qty": "1",
                            "pchs_avg_pric": "9000",
                            "prpr": "9500",
                        },
                    ],
                },
            ),
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            ): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "ord_psbl_cash": "133250",
                        "nrcvb_buy_qty": "13",
                    },
                },
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert isinstance(reader, BrokerReader)

    quote = reader.get_quote("069500")
    holdings = reader.get_holdings()
    capacity = reader.get_order_capacity("114800", Decimal("10250"))

    assert quote == Quote(
        symbol="069500",
        price=Decimal("12345.67"),
        as_of=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    assert holdings == (
        Holding(
            symbol="069500",
            quantity=1,
            average_price=Decimal("9000"),
            current_price=Decimal("9500"),
        ),
        Holding(
            symbol="357870",
            quantity=2,
            average_price=Decimal("10000"),
            current_price=Decimal("10100"),
        ),
    )
    assert capacity == OrderCapacity(
        symbol="114800",
        order_price=Decimal("10250"),
        max_orderable_quantity=13,
        cash_available=Decimal("133250"),
    )

    assert [request.method for request in transport.requests] == [
        "POST",
        "GET",
        "GET",
        "GET",
    ]
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
    ]
    assert json.loads(transport.requests[0].body.decode("utf-8")) == {
        "grant_type": "client_credentials",
        "appkey": "demo-key",
        "appsecret": "demo-secret",
    }
    assert transport.requests[1].headers["authorization"] == "Bearer token-123"
    assert parse_qs(urlsplit(transport.requests[1].url).query) == {
        "FID_COND_MRKT_DIV_CODE": ["J"],
        "FID_INPUT_ISCD": ["069500"],
    }
    assert parse_qs(urlsplit(transport.requests[2].url).query)["PRCS_DVSN"] == ["00"]
    assert parse_qs(urlsplit(transport.requests[3].url).query) == {
        "CANO": ["12345678"],
        "ACNT_PRDT_CD": ["01"],
        "PDNO": ["114800"],
        "ORD_UNPR": ["10250"],
        "ORD_DVSN": ["01"],
        "CMA_EVLU_AMT_ICLD_YN": ["N"],
        "OVRS_ICLD_YN": ["N"],
    }


def test_korea_investment_broker_reader_raises_when_token_is_missing() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {"rt_cd": "0"},
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(_make_settings(), transport=transport)

    with pytest.raises(KoreaInvestmentBrokerError, match="access_token"):
        reader.get_quote("069500")


def test_korea_investment_broker_reader_includes_http_error_body_details() -> None:
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {
                    "msg_cd": "EGW00123",
                    "msg1": "token blocked",
                },
                status=403,
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(_make_settings(), transport=transport)

    with pytest.raises(
        KoreaInvestmentBrokerError,
        match=r"HTTP 403.*EGW00123 - token blocked",
    ):
        reader.get_quote("069500")


def test_korea_investment_broker_reader_reuses_cached_token_file(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "token-cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "access_token": "cached-token",
                "expires_at": "2026-04-12T09:00:00+09:00",
            }
        ),
        encoding="utf-8",
    )
    transport = RecordingTransport(
        {
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345.67",
                    },
                },
            ),
        },
    )
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        token_cache_path=cache_path,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    quote = reader.get_quote("069500")

    assert quote.symbol == "069500"
    assert [request.method for request in transport.requests] == ["GET"]
    assert transport.requests[0].headers["authorization"] == "Bearer cached-token"


def test_korea_investment_broker_reader_writes_token_cache_after_fetch(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "token-cache.json"
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=RecordingTransport(
            {
                ("POST", "/oauth2/tokenP"): json_response(
                    {
                        "access_token": "token-123",
                        "access_token_token_expired": "2026-04-12 09:00:00",
                    },
                ),
                (
                    "GET",
                    "/uapi/domestic-stock/v1/quotations/inquire-price",
                ): json_response(
                    {
                        "rt_cd": "0",
                        "output": {
                            "stck_bsop_date": "20260411",
                            "stck_cntg_hour": "090000",
                            "stck_prpr": "12345.67",
                        },
                    },
                ),
            },
        ),
        token_cache_path=cache_path,
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    reader.get_quote("069500")

    cached_payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cached_payload == {
        "access_token": "token-123",
        "expires_at": "2026-04-12T09:00:00+09:00",
    }


def test_korea_investment_broker_reader_refreshes_expired_cached_token(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "token-cache.json"
    expired_at = datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    cache_path.write_text(
        json.dumps(
            {
                "access_token": "expired-token",
                "expires_at": expired_at.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    transport = RecordingTransport(
        {
            ("POST", "/oauth2/tokenP"): json_response(
                {
                    "access_token": "fresh-token",
                    "expires_in": 7200,
                },
            ),
            ("GET", "/uapi/domestic-stock/v1/quotations/inquire-price"): json_response(
                {
                    "rt_cd": "0",
                    "output": {
                        "stck_bsop_date": "20260411",
                        "stck_cntg_hour": "090000",
                        "stck_prpr": "12345.67",
                    },
                },
            ),
        },
    )
    now = expired_at + timedelta(minutes=2)
    reader = KoreaInvestmentBrokerReader(
        _make_settings(),
        transport=transport,
        token_cache_path=cache_path,
        clock=lambda: now,
    )

    reader.get_quote("069500")

    assert [request.method for request in transport.requests] == ["POST", "GET"]
    assert transport.requests[1].headers["authorization"] == "Bearer fresh-token"


@pytest.mark.parametrize(
    ("account", "environment", "expected"),
    [
        ("12345678", "paper", ("12345678", "01")),
        ("12345678-01", "paper", ("12345678", "01")),
        ("1234567801", "paper", ("12345678", "01")),
        ("12345678-01", "live", ("12345678", "01")),
        ("1234567801", "live", ("12345678", "01")),
    ],
)
def test_split_account_accepts_expected_formats(
    account: str,
    environment: BrokerEnvironment,
    expected: tuple[str, str],
) -> None:
    assert _split_account(account, environment=environment) == expected


def test_split_account_rejects_invalid_format() -> None:
    with pytest.raises(KoreaInvestmentBrokerError):
        _split_account("12345", environment="paper")


@pytest.mark.parametrize(
    ("account", "environment", "message"),
    [
        ("1234567-01", "paper", "paper account"),
        ("12345678-1", "paper", "paper account"),
        ("12345678-AB", "paper", "paper account"),
        ("12345678901", "paper", "paper account"),
        ("12345678--01", "paper", "paper account"),
        ("12345678", "live", "live account"),
    ],
)
def test_split_account_rejects_invalid_digit_structure(
    account: str,
    environment: BrokerEnvironment,
    message: str,
) -> None:
    with pytest.raises(KoreaInvestmentBrokerError, match=message):
        _split_account(account, environment=environment)


def _make_settings(environment: BrokerEnvironment = "paper") -> BrokerSettings:
    return BrokerSettings(
        provider="koreainvestment",
        api_key="demo-key",
        api_secret="demo-secret",
        account="12345678-01",
        environment=environment,
    )


def json_response(payload: dict[str, object], status: int = 200) -> HttpResponse:
    return HttpResponse(
        status=status,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode("utf-8"),
    )


class RecordingTransport:
    def __init__(self, responses: dict[tuple[str, str], HttpResponse]) -> None:
        self._responses = responses
        self.requests: list[HttpRequest] = []

    def __call__(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        key = (request.method, urlsplit(request.url).path)
        if key not in self._responses:
            raise AssertionError(f"unexpected request: {key}")
        return self._responses[key]
