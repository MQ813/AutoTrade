from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from autotrade.data import Bar
from autotrade.data import BarIntegrityChecker
from autotrade.data import BarSource
from autotrade.data import BarStore
from autotrade.data import Timeframe
from autotrade.data import UniverseMember
from autotrade.data import UniverseSource


def test_data_contracts_accept_protocol_implementations() -> None:
    source = DummyUniverseSource()
    bar_source = DummyBarSource()
    store = DummyBarStore()
    checker = DummyBarChecker()

    assert isinstance(source, UniverseSource)
    assert isinstance(bar_source, BarSource)
    assert isinstance(store, BarStore)
    assert isinstance(checker, BarIntegrityChecker)

    universe = source.load_universe()
    bars = bar_source.load_bars("069500", Timeframe.MINUTE_5)

    checker.validate_bars(bars)
    store.store_bars(bars)

    assert universe == (UniverseMember(symbol="069500"),)
    assert store.received == bars


class DummyUniverseSource:
    def load_universe(self) -> tuple[UniverseMember, ...]:
        return (UniverseMember(symbol="069500"),)


class DummyBarSource:
    def load_bars(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[Bar, ...]:
        del start, end
        return (
            Bar(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
                open=Decimal("100"),
                high=Decimal("105"),
                low=Decimal("99"),
                close=Decimal("104"),
                volume=10,
            ),
        )


class DummyBarStore:
    def __init__(self) -> None:
        self.received: tuple[Bar, ...] = ()

    def store_bars(self, bars: Sequence[Bar]) -> None:
        self.received = tuple(bars)


class DummyBarChecker:
    def validate_bars(self, bars: Sequence[Bar]) -> None:
        assert bars
