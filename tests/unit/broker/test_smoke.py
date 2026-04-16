from __future__ import annotations

import json
from datetime import datetime
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from autotrade.broker.korea_investment import HttpRequest
from autotrade.broker.korea_investment import HttpResponse
from autotrade.broker.smoke import render_smoke_report
from autotrade.broker.smoke import run_read_only_smoke
from autotrade.broker.smoke import write_smoke_report
from autotrade.config import AppSettings
from autotrade.config import BrokerSettings


def test_run_read_only_smoke_logs_success_and_writes_file(tmp_path) -> None:
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
    settings = AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=tmp_path / "logs",
    )
    fixed_now = datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    report = run_read_only_smoke(
        settings,
        transport=transport,
        clock=lambda: fixed_now,
    )
    log_path = write_smoke_report(settings.log_dir, report)

    assert report.success is True
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
    assert log_path.exists()

    log_text = log_path.read_text(encoding="utf-8")
    assert "success=True" in log_text
    assert "step=get_quote status=start detail=069500" in log_text
    assert "step=get_quote status=success detail=069500:12345.67" in log_text
    assert "step=get_holdings status=start" in log_text
    assert "step=get_holdings status=success detail=1" in log_text
    assert "step=get_order_capacity status=start detail=069500:12345.67" in log_text
    assert "step=get_order_capacity status=success detail=069500:13" in log_text
    assert "step=smoke status=success" in log_text

    rendered = render_smoke_report(report)
    assert "quote=069500:12345.67" in rendered


def test_run_read_only_smoke_records_failure_step(tmp_path) -> None:
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
                    "rt_cd": "9",
                    "msg1": "balance lookup failed",
                },
            ),
        },
    )
    settings = AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=tmp_path / "logs",
    )
    fixed_now = datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    report = run_read_only_smoke(
        settings,
        transport=transport,
        clock=lambda: fixed_now,
    )

    assert report.success is False
    rendered = render_smoke_report(report)
    assert "step=get_holdings status=start" in rendered
    assert "step=get_holdings status=failure detail=balance lookup failed" in rendered
    assert "step=smoke status=failure detail=balance lookup failed" in rendered


def test_run_read_only_smoke_records_http_error_details(tmp_path) -> None:
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
    settings = AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=tmp_path / "logs",
    )
    fixed_now = datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    report = run_read_only_smoke(
        settings,
        transport=transport,
        clock=lambda: fixed_now,
    )

    assert report.success is False
    rendered = render_smoke_report(report)
    assert "HTTP 403" in rendered
    assert "EGW00123 - token blocked" in rendered


def test_run_read_only_smoke_retries_kis_rate_limit_errors(tmp_path) -> None:
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
                        "stck_prpr": "12345",
                    },
                },
            ),
            ("GET", "/uapi/domestic-stock/v1/trading/inquire-balance"): [
                json_response(
                    {
                        "msg_cd": "EGW00201",
                        "msg1": "초당 거래건수를 초과하였습니다.",
                    },
                    status=500,
                ),
                json_response(
                    {
                        "rt_cd": "0",
                        "output1": [],
                    },
                ),
            ],
            (
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            ): [
                json_response(
                    {
                        "msg_cd": "EGW00201",
                        "msg1": "초당 거래건수를 초과하였습니다.",
                    },
                    status=500,
                ),
                json_response(
                    {
                        "rt_cd": "0",
                        "output": {
                            "ord_psbl_cash": "133250",
                            "nrcvb_buy_qty": "13",
                        },
                    },
                ),
            ],
        },
    )
    settings = AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=tmp_path / "logs",
    )
    fixed_now = datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    sleeps: list[float] = []

    report = run_read_only_smoke(
        settings,
        transport=transport,
        clock=lambda: fixed_now,
        sleep=sleeps.append,
    )

    assert report.success is True
    assert sleeps == [1.1, 1.1]
    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
    ]
    rendered = render_smoke_report(report)
    assert (
        "step=get_holdings status=retry "
        "detail=attempt=2 delay_seconds=1.1 error=HTTP 500"
    ) in rendered
    assert (
        "step=get_order_capacity status=retry "
        "detail=attempt=2 delay_seconds=1.1 error=HTTP 500"
    ) in rendered


def json_response(payload: dict[str, object], status: int = 200) -> HttpResponse:
    return HttpResponse(
        status=status,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode("utf-8"),
    )


class RecordingTransport:
    def __init__(
        self,
        responses: dict[
            tuple[str, str],
            HttpResponse | list[HttpResponse],
        ],
    ) -> None:
        self._responses = {
            key: value if isinstance(value, list) else [value]
            for key, value in responses.items()
        }
        self.requests: list[HttpRequest] = []

    def __call__(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        key = (request.method, urlsplit(request.url).path)
        if key not in self._responses:
            raise AssertionError(f"unexpected request: {key}")
        responses = self._responses[key]
        if not responses:
            raise AssertionError(f"no remaining response for request: {key}")
        return responses.pop(0)
