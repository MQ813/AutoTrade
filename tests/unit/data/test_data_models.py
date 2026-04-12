from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from autotrade.data import Bar
from autotrade.data import Timeframe
from autotrade.data import UniverseMember


def test_universe_member_accepts_non_blank_symbol() -> None:
    member = UniverseMember(symbol=" 069500 ", name="KODEX 200")

    assert member.symbol == " 069500 "
    assert member.name == "KODEX 200"
    assert member.active is True


@pytest.mark.parametrize(
    ("_field", "kwargs"),
    [
        ("symbol", {"symbol": " "}),
        ("name", {"symbol": "069500", "name": " "}),
    ],
)
def test_universe_member_rejects_blank_text(
    _field: str,
    kwargs: dict[str, str],
) -> None:
    with pytest.raises(ValueError):
        UniverseMember(**kwargs)


def test_timeframe_exposes_expected_interval() -> None:
    assert Timeframe.MINUTE_5.interval.total_seconds() == 300
    assert Timeframe.DAY.interval.total_seconds() == 86_400


def test_bar_accepts_valid_ohlc_payload() -> None:
    bar = Bar(
        symbol="069500",
        timeframe=Timeframe.MINUTE_5,
        timestamp=datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        open=Decimal("100"),
        high=Decimal("105"),
        low=Decimal("99"),
        close=Decimal("104"),
        volume=10,
    )

    assert bar.volume == 10
    assert bar.high == Decimal("105")


@pytest.mark.parametrize(
    ("_field", "kwargs", "match"),
    [
        ("timestamp", {"timestamp": datetime(2026, 4, 11, 9, 0)}, "timezone-aware"),
        ("open", {"open": Decimal("-1")}, "non-negative"),
        (
            "high",
            {
                "high": Decimal("101"),
                "close": Decimal("102"),
            },
            "within low and high",
        ),
        ("low", {"low": Decimal("106")}, "low must not exceed high"),
        ("volume", {"volume": -1}, "non-negative"),
    ],
)
def test_bar_rejects_invalid_values(
    _field: str,
    kwargs: dict[str, object],
    match: str,
) -> None:
    base_kwargs = {
        "symbol": "069500",
        "timeframe": Timeframe.MINUTE_5,
        "timestamp": datetime(2026, 4, 11, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        "open": Decimal("100"),
        "high": Decimal("105"),
        "low": Decimal("99"),
        "close": Decimal("104"),
        "volume": 10,
    }
    base_kwargs.update(kwargs)

    with pytest.raises(ValueError, match=match):
        Bar(**base_kwargs)
