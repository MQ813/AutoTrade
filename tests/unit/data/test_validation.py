from __future__ import annotations

from datetime import datetime
from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from autotrade.data import Bar
from autotrade.data import Timeframe
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import find_missing_bar_timestamps
from autotrade.data import normalize_symbol
from autotrade.data import normalize_symbols
from autotrade.data import validate_bar_series


def test_normalize_symbol_strips_and_uppercases() -> None:
    assert normalize_symbol("  aapl ") == "AAPL"


@pytest.mark.parametrize("symbol", ["", " ", "\t"])
def test_normalize_symbol_rejects_blank_values(symbol: str) -> None:
    with pytest.raises(ValueError, match="symbol must not be blank"):
        normalize_symbol(symbol)


def test_normalize_symbols_preserves_order() -> None:
    assert normalize_symbols([" 069500 ", "357870"]) == ("069500", "357870")


def test_normalize_symbols_rejects_duplicates_after_normalization() -> None:
    with pytest.raises(ValueError, match="duplicate symbol"):
        normalize_symbols(["069500", " 069500 "])


def test_validate_bar_series_accepts_contiguous_series() -> None:
    bars = (
        _make_bar("069500", "2026-04-10T09:00:00+09:00"),
        _make_bar("069500", "2026-04-10T09:05:00+09:00"),
        _make_bar("069500", "2026-04-10T09:10:00+09:00"),
    )

    validate_bar_series(bars)
    assert find_missing_bar_timestamps(bars) == ()


def test_validate_bar_series_rejects_unsorted_series() -> None:
    bars = (
        _make_bar("069500", "2026-04-10T09:05:00+09:00"),
        _make_bar("069500", "2026-04-10T09:00:00+09:00"),
    )

    with pytest.raises(ValueError, match="sorted by timestamp"):
        validate_bar_series(bars)


def test_validate_bar_series_rejects_duplicates() -> None:
    bars = (
        _make_bar("069500", "2026-04-10T09:00:00+09:00"),
        _make_bar("069500", "2026-04-10T09:00:00+09:00"),
    )

    with pytest.raises(ValueError, match="duplicate timestamps"):
        validate_bar_series(bars)


def test_validate_bar_series_rejects_missing_intervals() -> None:
    bars = (
        _make_bar("069500", "2026-04-10T09:00:00+09:00"),
        _make_bar("069500", "2026-04-10T09:10:00+09:00"),
    )

    with pytest.raises(ValueError, match="missing timestamps"):
        validate_bar_series(bars)
    assert find_missing_bar_timestamps(bars) == (
        datetime(2026, 4, 10, 9, 5, tzinfo=ZoneInfo("Asia/Seoul")),
    )


def test_validate_bar_series_rejects_mixed_symbols() -> None:
    bars = (
        _make_bar("069500", "2026-04-10T09:00:00+09:00"),
        _make_bar("357870", "2026-04-10T09:05:00+09:00"),
    )

    with pytest.raises(ValueError, match="one symbol"):
        validate_bar_series(bars)


def test_validate_bar_series_allows_weekend_boundary_gap() -> None:
    bars = (
        _make_bar("069500", "2026-04-10T15:30:00+09:00", Timeframe.DAY),
        _make_bar("069500", "2026-04-13T15:30:00+09:00", Timeframe.DAY),
    )

    validate_bar_series(bars)
    assert find_missing_bar_timestamps(bars) == ()


def test_validate_bar_series_allows_session_boundary_gap() -> None:
    bars = (
        _make_bar("069500", "2026-04-10T15:30:00+09:00", Timeframe.MINUTE_30),
        _make_bar("069500", "2026-04-13T09:00:00+09:00", Timeframe.MINUTE_30),
    )

    validate_bar_series(bars)
    assert find_missing_bar_timestamps(bars) == ()


def test_validate_bar_series_rejects_intra_session_gap() -> None:
    bars = (
        _make_bar("069500", "2026-04-10T09:00:00+09:00"),
        _make_bar("069500", "2026-04-10T10:00:00+09:00"),
    )

    with pytest.raises(ValueError, match="missing timestamps"):
        validate_bar_series(bars)


def test_validate_bar_series_rejects_non_session_timestamp() -> None:
    bars = (
        _make_bar("069500", "2026-04-10T09:00:00+09:00"),
        _make_bar("069500", "2026-04-10T09:07:00+09:00"),
    )

    with pytest.raises(ValueError, match="valid KRX session timestamps"):
        validate_bar_series(bars)


def test_validate_bar_series_applies_holiday_injection() -> None:
    calendar = KrxRegularSessionCalendar(holiday_dates=frozenset({date(2026, 4, 13)}))
    bars = (
        _make_bar("069500", "2026-04-10T15:30:00+09:00", Timeframe.DAY),
        _make_bar("069500", "2026-04-14T15:30:00+09:00", Timeframe.DAY),
    )

    validate_bar_series(bars, calendar=calendar)
    assert find_missing_bar_timestamps(bars, calendar=calendar) == ()


def _make_bar(
    symbol: str,
    timestamp: str,
    timeframe: Timeframe = Timeframe.MINUTE_5,
) -> Bar:
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
