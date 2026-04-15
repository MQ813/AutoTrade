from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from autotrade.data import Bar
from autotrade.data import CsvBarStore
from autotrade.data import Timeframe


def test_csv_bar_store_writes_deterministic_series_files(tmp_path: Path) -> None:
    store = CsvBarStore(root_dir=tmp_path / "bars")
    bars = (
        _make_bar("357870", Timeframe.MINUTE_30, "2026-04-10T09:30:00+09:00"),
        _make_bar("069500", Timeframe.DAY, "2026-04-10T15:30:00+09:00"),
        _make_bar("357870", Timeframe.MINUTE_30, "2026-04-10T09:00:00+09:00"),
    )

    store.store_bars(bars)

    assert (tmp_path / "bars" / "1d" / "069500.csv").read_text(encoding="utf-8") == (
        "symbol,timeframe,timestamp,open,high,low,close,volume\n"
        "069500,1d,2026-04-10T15:30:00+09:00,100,105,99,104,10\n"
    )
    assert (tmp_path / "bars" / "30m" / "357870.csv").read_text(encoding="utf-8") == (
        "symbol,timeframe,timestamp,open,high,low,close,volume\n"
        "357870,30m,2026-04-10T09:00:00+09:00,100,105,99,104,10\n"
        "357870,30m,2026-04-10T09:30:00+09:00,100,105,99,104,10\n"
    )


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
