from __future__ import annotations

from csv import DictReader
from decimal import Decimal
from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from autotrade.common.models import OrderSide


class KrxInstrumentType(StrEnum):
    STOCK = "stock"
    ETF = "etf"


KRX_STOCK_TICK_TABLE: tuple[tuple[Decimal, Decimal], ...] = (
    (Decimal("2000"), Decimal("1")),
    (Decimal("5000"), Decimal("5")),
    (Decimal("20000"), Decimal("10")),
    (Decimal("50000"), Decimal("50")),
    (Decimal("200000"), Decimal("100")),
    (Decimal("500000"), Decimal("500")),
)
KRX_STOCK_MAX_TICK_SIZE = Decimal("1000")
KRX_ETF_TICK_TABLE: tuple[tuple[Decimal, Decimal], ...] = (
    (Decimal("2000"), Decimal("1")),
)
KRX_ETF_MAX_TICK_SIZE = Decimal("5")
DEFAULT_KRX_ETF_UNIVERSE_PATH = Path("universe/universe_etf.csv")
ZERO = Decimal("0")


def krx_stock_tick_size(price: Decimal) -> Decimal:
    return krx_order_tick_size(price, KrxInstrumentType.STOCK)


def krx_order_tick_size(
    price: Decimal,
    instrument_type: KrxInstrumentType,
) -> Decimal:
    if price <= ZERO:
        raise ValueError("price must be positive")
    if instrument_type is KrxInstrumentType.ETF:
        return _krx_tick_size(price, KRX_ETF_TICK_TABLE, KRX_ETF_MAX_TICK_SIZE)
    return _krx_tick_size(price, KRX_STOCK_TICK_TABLE, KRX_STOCK_MAX_TICK_SIZE)


def resolve_krx_instrument_type(symbol: str) -> KrxInstrumentType:
    normalized_symbol = symbol.strip()
    if normalized_symbol in _known_krx_etf_symbols():
        return KrxInstrumentType.ETF
    return KrxInstrumentType.STOCK


def is_valid_krx_symbol_order_price(symbol: str, price: Decimal) -> bool:
    return is_valid_krx_order_price(price, resolve_krx_instrument_type(symbol))


def is_valid_krx_stock_order_price(price: Decimal) -> bool:
    return is_valid_krx_order_price(price, KrxInstrumentType.STOCK)


def is_valid_krx_order_price(
    price: Decimal,
    instrument_type: KrxInstrumentType,
) -> bool:
    if price <= ZERO:
        return False
    if price != price.to_integral_value():
        return False
    tick_size = krx_order_tick_size(price, instrument_type)
    return price % tick_size == ZERO


def normalize_krx_symbol_order_price(
    symbol: str,
    price: Decimal,
    side: OrderSide,
) -> Decimal:
    return normalize_krx_order_price(
        price,
        side,
        resolve_krx_instrument_type(symbol),
    )


def normalize_krx_stock_order_price(price: Decimal, side: OrderSide) -> Decimal:
    return normalize_krx_order_price(price, side, KrxInstrumentType.STOCK)


def normalize_krx_order_price(
    price: Decimal,
    side: OrderSide,
    instrument_type: KrxInstrumentType,
) -> Decimal:
    tick_size = krx_order_tick_size(price, instrument_type)
    rounded_down = (price // tick_size) * tick_size
    if side is OrderSide.SELL:
        candidate = rounded_down
    elif price == rounded_down:
        candidate = price
    else:
        candidate = rounded_down + tick_size
    if candidate <= ZERO:
        candidate = tick_size

    while not is_valid_krx_order_price(candidate, instrument_type):
        next_tick_size = krx_order_tick_size(candidate, instrument_type)
        rounded_down = (candidate // next_tick_size) * next_tick_size
        if side is OrderSide.SELL:
            candidate = rounded_down
        elif candidate == rounded_down:
            break
        else:
            candidate = rounded_down + next_tick_size
    return candidate


def _krx_tick_size(
    price: Decimal,
    tick_table: tuple[tuple[Decimal, Decimal], ...],
    max_tick_size: Decimal,
) -> Decimal:
    for upper_bound, tick_size in tick_table:
        if price < upper_bound:
            return tick_size
    return max_tick_size


@lru_cache(maxsize=1)
def _known_krx_etf_symbols() -> frozenset[str]:
    if not DEFAULT_KRX_ETF_UNIVERSE_PATH.is_file():
        return frozenset()
    symbols: set[str] = set()
    with DEFAULT_KRX_ETF_UNIVERSE_PATH.open(encoding="utf-8", newline="") as handle:
        for row in DictReader(handle):
            symbol = (row.get("symbol") or "").strip()
            asset_type = (row.get("asset_type") or "").strip().upper()
            is_etf = (row.get("is_etf") or "").strip()
            if symbol and (asset_type == "ETF" or is_etf == "1"):
                symbols.add(symbol)
    return frozenset(symbols)
