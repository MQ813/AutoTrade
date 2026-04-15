from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from autotrade.data import Bar
from autotrade.data import BarCollectionService
from autotrade.data import Timeframe
from autotrade.data import UniverseMember


def test_collection_service_collects_daily_bars() -> None:
    universe_source = RecordingUniverseSource()
    bar_source = RecordingBarSource()
    bar_store = RecordingBarStore()
    bar_checker = RecordingBarChecker()
    service = BarCollectionService(
        universe_source=universe_source,
        bar_source=bar_source,
        bar_store=bar_store,
        bar_checker=bar_checker,
    )
    start = datetime(2026, 4, 10, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    end = datetime(2026, 4, 11, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    service.collect_daily_bars(start=start, end=end)

    assert bar_source.calls == [
        ("069500", Timeframe.DAY, start, end),
        ("357870", Timeframe.DAY, start, end),
    ]
    assert bar_checker.received == [
        tuple(bar_source.daily_bars["069500"]),
        tuple(bar_source.daily_bars["357870"]),
    ]
    assert bar_store.received == bar_checker.received


def test_collection_service_collects_intraday_bars() -> None:
    universe_source = RecordingUniverseSource()
    bar_source = RecordingBarSource()
    bar_store = RecordingBarStore()
    bar_checker = RecordingBarChecker()
    service = BarCollectionService(
        universe_source=universe_source,
        bar_source=bar_source,
        bar_store=bar_store,
        bar_checker=bar_checker,
    )

    service.collect_intraday_bars()

    assert bar_source.calls == [
        ("069500", Timeframe.MINUTE_30, None, None),
        ("357870", Timeframe.MINUTE_30, None, None),
    ]
    assert bar_checker.received == [
        tuple(bar_source.intraday_bars["069500"]),
        tuple(bar_source.intraday_bars["357870"]),
    ]
    assert bar_store.received == bar_checker.received


class RecordingUniverseSource:
    def load_universe(self) -> tuple[UniverseMember, ...]:
        return (
            UniverseMember(symbol="069500"),
            UniverseMember(symbol="357870"),
        )


class RecordingBarSource:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Timeframe, datetime | None, datetime | None]] = []
        self.daily_bars = {
            "069500": (
                _make_bar("069500", Timeframe.DAY, "2026-04-10T15:30:00+09:00"),
            ),
            "357870": (
                _make_bar("357870", Timeframe.DAY, "2026-04-10T15:30:00+09:00"),
            ),
        }
        self.intraday_bars = {
            "069500": (
                _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T09:00:00+09:00"),
                _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T09:30:00+09:00"),
            ),
            "357870": (
                _make_bar("357870", Timeframe.MINUTE_30, "2026-04-10T09:00:00+09:00"),
                _make_bar("357870", Timeframe.MINUTE_30, "2026-04-10T09:30:00+09:00"),
            ),
        }

    def load_bars(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[Bar, ...]:
        self.calls.append((symbol, timeframe, start, end))
        if timeframe is Timeframe.DAY:
            return self.daily_bars[symbol]
        return self.intraday_bars[symbol]


class RecordingBarChecker:
    def __init__(self) -> None:
        self.received: list[tuple[Bar, ...]] = []

    def validate_bars(self, bars: Sequence[Bar]) -> None:
        self.received.append(tuple(bars))


class RecordingBarStore:
    def __init__(self) -> None:
        self.received: list[tuple[Bar, ...]] = []

    def store_bars(self, bars: Sequence[Bar]) -> None:
        self.received.append(tuple(bars))


def _make_bar(symbol: str, timeframe: Timeframe, timestamp: str) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=datetime.fromisoformat(timestamp),
        open=Decimal("100"),
        high=Decimal("105"),
        low=Decimal("99"),
        close=Decimal("104"),
        volume=10,
    )
