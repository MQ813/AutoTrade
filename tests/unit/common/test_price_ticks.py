from __future__ import annotations

from decimal import Decimal

from autotrade.common import OrderSide
from autotrade.common.price_ticks import KrxInstrumentType
from autotrade.common.price_ticks import is_valid_krx_order_price
from autotrade.common.price_ticks import is_valid_krx_stock_order_price
from autotrade.common.price_ticks import krx_stock_tick_size
from autotrade.common.price_ticks import normalize_krx_order_price
from autotrade.common.price_ticks import normalize_krx_stock_order_price
from autotrade.common.price_ticks import resolve_krx_instrument_type


def test_krx_stock_tick_size_uses_current_price_bands() -> None:
    assert krx_stock_tick_size(Decimal("1999")) == Decimal("1")
    assert krx_stock_tick_size(Decimal("2000")) == Decimal("5")
    assert krx_stock_tick_size(Decimal("5000")) == Decimal("10")
    assert krx_stock_tick_size(Decimal("20000")) == Decimal("50")
    assert krx_stock_tick_size(Decimal("50000")) == Decimal("100")
    assert krx_stock_tick_size(Decimal("200000")) == Decimal("500")
    assert krx_stock_tick_size(Decimal("500000")) == Decimal("1000")


def test_krx_stock_order_price_validation_requires_integer_tick() -> None:
    assert is_valid_krx_stock_order_price(Decimal("222500"))
    assert is_valid_krx_stock_order_price(Decimal("223000"))
    assert not is_valid_krx_stock_order_price(Decimal("222750"))
    assert not is_valid_krx_stock_order_price(Decimal("100.5"))


def test_krx_etf_order_price_validation_uses_five_won_tick_above_2000() -> None:
    assert is_valid_krx_order_price(Decimal("97265"), KrxInstrumentType.ETF)
    assert not is_valid_krx_order_price(Decimal("97263"), KrxInstrumentType.ETF)


def test_resolve_krx_instrument_type_uses_local_etf_universe() -> None:
    assert resolve_krx_instrument_type("069500") is KrxInstrumentType.ETF
    assert resolve_krx_instrument_type("005930") is KrxInstrumentType.STOCK


def test_normalize_krx_stock_order_price_rounds_by_order_side() -> None:
    assert normalize_krx_stock_order_price(
        Decimal("224250"),
        OrderSide.BUY,
    ) == Decimal("224500")
    assert normalize_krx_stock_order_price(
        Decimal("222750"),
        OrderSide.SELL,
    ) == Decimal("222500")


def test_normalize_krx_etf_order_price_preserves_valid_five_won_tick() -> None:
    assert normalize_krx_order_price(
        Decimal("97265"),
        OrderSide.BUY,
        KrxInstrumentType.ETF,
    ) == Decimal("97265")
