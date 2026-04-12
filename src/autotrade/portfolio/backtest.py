from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class PortfolioSnapshot:
    symbol: str
    timestamp: datetime
    close_price: Decimal
    cash: Decimal
    position_quantity: int
    position_market_value: Decimal
    total_equity: Decimal
