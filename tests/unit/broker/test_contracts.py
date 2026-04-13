from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from autotrade.broker import BrokerNormalizationError
from autotrade.broker import BrokerReader
from autotrade.broker import BrokerTrader
from autotrade.broker.korea_investment import HttpRequest
from autotrade.broker.korea_investment import HttpResponse
from autotrade.broker.korea_investment import KoreaInvestmentBrokerReader
from autotrade.broker import normalize_holding
from autotrade.broker import normalize_order_capacity
from autotrade.broker import normalize_quote
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import Holding
from autotrade.common import OrderAmendRequest
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderCapacity
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.common import Quote
from autotrade.config import BrokerSettings


def test_normalize_quote_converts_decimal_and_aware_timestamp() -> None:
    quote = normalize_quote(
        {
            "symbol": "069500",
            "price": "12345.67",
            "as_of": "2026-04-11T09:00:00+09:00",
        },
    )

    assert quote == Quote(
        symbol="069500",
        price=Decimal("12345.67"),
        as_of=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )


def test_normalize_quote_rejects_naive_timestamp() -> None:
    with pytest.raises(BrokerNormalizationError, match="timezone-aware"):
        normalize_quote(
            {
                "symbol": "069500",
                "price": "12345.67",
                "as_of": "2026-04-11T09:00:00",
            },
        )


def test_normalize_holding_accepts_optional_current_price() -> None:
    holding = normalize_holding(
        {
            "symbol": "357870",
            "quantity": "7",
            "average_price": "10100",
        },
    )

    assert holding == Holding(
        symbol="357870",
        quantity=7,
        average_price=Decimal("10100"),
        current_price=None,
    )


def test_normalize_order_capacity_converts_numeric_fields() -> None:
    capacity = normalize_order_capacity(
        {
            "symbol": "114800",
            "order_price": "10250",
            "max_orderable_quantity": "13",
            "cash_available": "133250",
        },
    )

    assert capacity == OrderCapacity(
        symbol="114800",
        order_price=Decimal("10250"),
        max_orderable_quantity=13,
        cash_available=Decimal("133250"),
    )


@pytest.mark.parametrize(
    "payload",
    [
        {
            "symbol": "069500",
            "price": "not-a-number",
            "as_of": "2026-04-11T09:00:00+09:00",
        },
        {
            "symbol": "069500",
            "price": "12345.67",
        },
        {
            "symbol": "114800",
            "order_price": "10250",
            "max_orderable_quantity": "1.5",
            "cash_available": "133250",
        },
    ],
)
def test_normalizers_raise_broker_normalization_error_for_invalid_payloads(
    payload: dict[str, str],
) -> None:
    normalizer = normalize_quote if "price" in payload else normalize_order_capacity

    with pytest.raises(BrokerNormalizationError):
        normalizer(payload)


def test_broker_reader_contract_returns_standard_models() -> None:
    reader = DummyBrokerReader()

    assert isinstance(reader, BrokerReader)

    quote = reader.get_quote("069500")
    holdings = reader.get_holdings()
    capacity = reader.get_order_capacity("069500", Decimal("10250"))

    assert isinstance(quote, Quote)
    assert isinstance(holdings, tuple)
    assert holdings == tuple(sorted(holdings, key=lambda holding: holding.symbol))
    assert all(isinstance(holding, Holding) for holding in holdings)
    assert isinstance(capacity, OrderCapacity)
    assert capacity.order_price == Decimal("10250")


def test_korea_investment_broker_reader_conforms_to_contract() -> None:
    reader = KoreaInvestmentBrokerReader(
        BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        transport=_ContractTransport(),
        clock=lambda: datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert isinstance(reader, BrokerReader)
    assert isinstance(reader.get_quote("069500"), Quote)
    assert isinstance(reader.get_holdings(), tuple)
    assert isinstance(
        reader.get_order_capacity("069500", Decimal("10250")),
        OrderCapacity,
    )


def test_broker_trader_contract_returns_standard_models() -> None:
    trader = DummyBrokerTrader()

    assert isinstance(trader, BrokerTrader)

    order = trader.submit_order(
        OrderRequest(
            request_id="submit-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=3,
            limit_price=Decimal("10000"),
            requested_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )
    amended = trader.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id=order.order_id,
            limit_price=Decimal("10100"),
            requested_at=datetime(2026, 4, 11, 9, 1, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )
    canceled = trader.cancel_order(
        OrderCancelRequest(
            request_id="cancel-1",
            order_id=order.order_id,
            requested_at=datetime(2026, 4, 11, 9, 2, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    )
    fills = trader.get_fills(order.order_id)

    assert isinstance(order, ExecutionOrder)
    assert isinstance(amended, ExecutionOrder)
    assert isinstance(canceled, ExecutionOrder)
    assert isinstance(fills, tuple)
    assert all(isinstance(fill, ExecutionFill) for fill in fills)


class DummyBrokerReader:
    def get_quote(self, symbol: str) -> Quote:
        return normalize_quote(
            {
                "symbol": symbol,
                "price": "12345.67",
                "as_of": "2026-04-11T09:00:00+09:00",
            },
        )

    def get_holdings(self) -> tuple[Holding, ...]:
        normalized = (
            normalize_holding(
                {
                    "symbol": "357870",
                    "quantity": "2",
                    "average_price": "10000",
                    "current_price": "10100",
                },
            ),
            normalize_holding(
                {
                    "symbol": "069500",
                    "quantity": "1",
                    "average_price": "9000",
                    "current_price": "9500",
                },
            ),
        )
        return tuple(sorted(normalized, key=lambda holding: holding.symbol))

    def get_order_capacity(
        self,
        symbol: str,
        order_price: Decimal,
    ) -> OrderCapacity:
        return normalize_order_capacity(
            {
                "symbol": symbol,
                "order_price": str(order_price),
                "max_orderable_quantity": "13",
                "cash_available": "133250",
            },
        )


class DummyBrokerTrader:
    def submit_order(self, request: OrderRequest) -> ExecutionOrder:
        return ExecutionOrder(
            order_id="order-1",
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            limit_price=request.limit_price,
            status=OrderStatus.ACKNOWLEDGED,
            created_at=request.requested_at,
            updated_at=request.requested_at,
        )

    def amend_order(self, request: OrderAmendRequest) -> ExecutionOrder:
        return ExecutionOrder(
            order_id=request.order_id,
            symbol="069500",
            side=OrderSide.BUY,
            quantity=request.quantity or 3,
            limit_price=request.limit_price or Decimal("10000"),
            status=OrderStatus.ACKNOWLEDGED,
            created_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
            updated_at=request.requested_at,
        )

    def cancel_order(self, request: OrderCancelRequest) -> ExecutionOrder:
        return ExecutionOrder(
            order_id=request.order_id,
            symbol="069500",
            side=OrderSide.BUY,
            quantity=3,
            limit_price=Decimal("10100"),
            status=OrderStatus.CANCELED,
            created_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
            updated_at=request.requested_at,
        )

    def get_fills(self, order_id: str) -> tuple[ExecutionFill, ...]:
        return (
            ExecutionFill(
                fill_id="fill-1",
                order_id=order_id,
                symbol="069500",
                quantity=1,
                price=Decimal("10050"),
                filled_at=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
            ),
        )


class _ContractTransport:
    def __init__(self) -> None:
        self.requests: list[HttpRequest] = []

    def __call__(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        path = request.url.split("?")[0].split(
            "https://openapivts.koreainvestment.com:29443",
        )[-1]
        if path == "/oauth2/tokenP":
            return HttpResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps({"access_token": "token-123"}).encode("utf-8"),
            )
        if path == "/uapi/domestic-stock/v1/quotations/inquire-price":
            return HttpResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "rt_cd": "0",
                        "output": {
                            "stck_bsop_date": "20260411",
                            "stck_cntg_hour": "090000",
                            "stck_prpr": "12345.67",
                        },
                    },
                ).encode("utf-8"),
            )
        if path == "/uapi/domestic-stock/v1/trading/inquire-balance":
            return HttpResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "rt_cd": "0",
                        "output1": [
                            {
                                "pdno": "069500",
                                "hldg_qty": "1",
                                "pchs_avg_pric": "9000",
                                "prpr": "9500",
                            }
                        ],
                    },
                ).encode("utf-8"),
            )
        if path == "/uapi/domestic-stock/v1/trading/inquire-psbl-order":
            return HttpResponse(
                status=200,
                headers={"content-type": "application/json"},
                body=json.dumps(
                    {
                        "rt_cd": "0",
                        "output": {
                            "ord_psbl_cash": "133250",
                            "max_buy_qty": "13",
                        },
                    },
                ).encode("utf-8"),
            )
        raise AssertionError(f"unexpected request: {path}")
