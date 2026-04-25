from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs
from urllib.parse import urlsplit

from autotrade.broker.korea_investment import HttpRequest
from autotrade.broker.korea_investment import HttpResponse
from autotrade.broker.korea_investment import KoreaInvestmentBrokerReader
from autotrade.broker.korea_investment import KoreaInvestmentBrokerTrader
from autotrade.common import ExecutionFill
from autotrade.common import Holding
from autotrade.common import OrderCapacity
from autotrade.common import Quote
from autotrade.config import BrokerSettings

FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "kis_endpoint_contracts"
    / "read_only_order_history.json"
)


def test_recorded_kis_endpoint_contracts_cover_read_only_and_order_history() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    transport = RecordedFixtureTransport(fixture)
    settings = BrokerSettings(**fixture["settings"])

    def clock() -> datetime:
        return datetime.fromisoformat(fixture["clock"])

    reader = KoreaInvestmentBrokerReader(
        settings,
        transport=transport,
        clock=clock,
    )
    trader = KoreaInvestmentBrokerTrader(
        settings,
        transport=transport,
        clock=clock,
    )

    quote = reader.get_quote(fixture["symbol"])
    holdings = reader.get_holdings()
    capacity = reader.get_order_capacity(
        fixture["symbol"],
        Decimal(fixture["order_price"]),
    )
    fills = trader.get_fills(fixture["order_id"])

    assert quote == Quote(
        symbol=fixture["expected"]["quote"]["symbol"],
        price=Decimal(fixture["expected"]["quote"]["price"]),
        as_of=datetime.fromisoformat(fixture["expected"]["quote"]["as_of"]),
    )
    assert holdings == tuple(
        Holding(
            symbol=row["symbol"],
            quantity=row["quantity"],
            average_price=Decimal(row["average_price"]),
            current_price=Decimal(row["current_price"]),
        )
        for row in fixture["expected"]["holdings"]
    )
    assert capacity == OrderCapacity(
        symbol=fixture["expected"]["order_capacity"]["symbol"],
        order_price=Decimal(fixture["expected"]["order_capacity"]["order_price"]),
        max_orderable_quantity=fixture["expected"]["order_capacity"][
            "max_orderable_quantity"
        ],
        cash_available=Decimal(fixture["expected"]["order_capacity"]["cash_available"]),
    )
    assert fills == tuple(
        ExecutionFill(
            fill_id=row["fill_id"],
            order_id=row["order_id"],
            symbol=row["symbol"],
            quantity=row["quantity"],
            price=Decimal(row["price"]),
            filled_at=datetime.fromisoformat(row["filled_at"]),
        )
        for row in fixture["expected"]["fills"]
    )

    assert [urlsplit(request.url).path for request in transport.requests] == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
    ]
    assert [request.headers.get("tr_id") for request in transport.requests] == [
        None,
        "FHKST01010100",
        "VTTC8434R",
        "VTTC8908R",
        None,
        "VTTC8001R",
    ]
    assert _query(transport.requests[1]) == {
        "FID_COND_MRKT_DIV_CODE": ["J"],
        "FID_INPUT_ISCD": ["069500"],
    }
    assert _query(transport.requests[2]) == {
        "CANO": ["12345678"],
        "ACNT_PRDT_CD": ["01"],
        "AFHR_FLPR_YN": ["N"],
        "OFL_YN": [""],
        "INQR_DVSN": ["02"],
        "UNPR_DVSN": ["01"],
        "FUND_STTL_ICLD_YN": ["N"],
        "FNCG_AMT_AUTO_RDPT_YN": ["N"],
        "PRCS_DVSN": ["00"],
        "CTX_AREA_FK100": [""],
        "CTX_AREA_NK100": [""],
    }
    assert _query(transport.requests[3]) == {
        "CANO": ["12345678"],
        "ACNT_PRDT_CD": ["01"],
        "PDNO": ["069500"],
        "ORD_UNPR": ["95000"],
        "ORD_DVSN": ["01"],
        "CMA_EVLU_AMT_ICLD_YN": ["N"],
        "OVRS_ICLD_YN": ["N"],
    }
    assert _query(transport.requests[5]) == {
        "CANO": ["12345678"],
        "ACNT_PRDT_CD": ["01"],
        "INQR_STRT_DT": ["20260424"],
        "INQR_END_DT": ["20260424"],
        "SLL_BUY_DVSN_CD": ["00"],
        "INQR_DVSN": ["00"],
        "PDNO": [""],
        "CCLD_DVSN": ["00"],
        "ORD_GNO_BRNO": [""],
        "ODNO": ["11960"],
        "INQR_DVSN_3": ["00"],
        "INQR_DVSN_1": [""],
        "CTX_AREA_FK100": [""],
        "CTX_AREA_NK100": [""],
    }


def _query(request: HttpRequest) -> dict[str, list[str]]:
    return parse_qs(urlsplit(request.url).query, keep_blank_values=True)


class RecordedFixtureTransport:
    def __init__(self, fixture: dict[str, object]) -> None:
        self._entries = list(fixture["responses"])
        self.requests: list[HttpRequest] = []

    def __call__(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        if not self._entries:
            raise AssertionError(f"unexpected request: {request.method} {request.url}")

        entry = self._entries.pop(0)
        expected_request = entry["request"]
        assert request.method == expected_request["method"]
        assert urlsplit(request.url).path == expected_request["path"]
        expected_tr_id = expected_request.get("tr_id")
        if expected_tr_id is not None:
            assert request.headers["tr_id"] == expected_tr_id

        return HttpResponse(
            status=200,
            headers={"content-type": "application/json"},
            body=json.dumps(entry["response"]).encode("utf-8"),
        )
