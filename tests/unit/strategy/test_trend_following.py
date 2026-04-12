from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from autotrade.common import SignalAction
from autotrade.data import Bar
from autotrade.data import Timeframe
from autotrade.data import KrxRegularSessionCalendar
from autotrade.strategy import DailyTrendFollowingStrategy
from autotrade.strategy import ThirtyMinuteTrendStrategy


def test_daily_trend_following_generates_buy_signal_with_reason() -> None:
    strategy = DailyTrendFollowingStrategy()
    bars = _make_bars(
        symbol="069500",
        timeframe=Timeframe.DAY,
        start="2026-04-10T15:30:00+09:00",
        closes=[Decimal(str(value)) for value in range(100, 120)],
    )

    signal = strategy.generate_signal(bars)

    assert signal.action is SignalAction.BUY
    assert signal.symbol == "069500"
    assert signal.generated_at == bars[-1].timestamp
    assert signal.reason is not None
    assert "strategy=daily_trend_following" in signal.reason
    assert "action=BUY" in signal.reason
    assert "trigger=trend_alignment" in signal.reason
    assert "fast_sma=" in signal.reason
    assert "slow_sma=" in signal.reason


def test_thirty_minute_trend_generates_sell_signal_with_reason() -> None:
    strategy = ThirtyMinuteTrendStrategy()
    bars = _make_bars(
        symbol="069500",
        timeframe=Timeframe.MINUTE_30,
        start="2026-04-10T09:00:00+09:00",
        closes=[Decimal(str(value)) for value in range(200, 176, -1)],
    )

    signal = strategy.generate_signal(bars)

    assert signal.action is SignalAction.SELL
    assert signal.reason is not None
    assert "strategy=30m_low_frequency_trend" in signal.reason
    assert "action=SELL" in signal.reason
    assert "trigger=trend_breakdown" in signal.reason


def test_strategy_returns_hold_for_insufficient_history() -> None:
    strategy = DailyTrendFollowingStrategy()
    bars = _make_bars(
        symbol="069500",
        timeframe=Timeframe.DAY,
        start="2026-04-10T15:30:00+09:00",
        closes=[Decimal("100"), Decimal("101"), Decimal("102")],
    )

    signal = strategy.generate_signal(bars)

    assert signal.action is SignalAction.HOLD
    assert signal.reason is not None
    assert "trigger=insufficient_history" in signal.reason
    assert "required_bars=20" in signal.reason
    assert "actual_bars=3" in signal.reason


@pytest.mark.parametrize(
    ("bars", "match"),
    [
        (
            (
                Bar(
                    symbol="069500",
                    timeframe=Timeframe.DAY,
                    timestamp=datetime(
                        2026,
                        4,
                        10,
                        15,
                        30,
                        tzinfo=ZoneInfo("Asia/Seoul"),
                    ),
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100"),
                    volume=1,
                ),
                Bar(
                    symbol="069500",
                    timeframe=Timeframe.DAY,
                    timestamp=datetime(
                        2026,
                        4,
                        9,
                        15,
                        30,
                        tzinfo=ZoneInfo("Asia/Seoul"),
                    ),
                    open=Decimal("101"),
                    high=Decimal("102"),
                    low=Decimal("100"),
                    close=Decimal("101"),
                    volume=1,
                ),
            ),
            "sorted by timestamp",
        ),
        (
            (
                Bar(
                    symbol="069500",
                    timeframe=Timeframe.DAY,
                    timestamp=datetime(
                        2026,
                        4,
                        10,
                        15,
                        30,
                        tzinfo=ZoneInfo("Asia/Seoul"),
                    ),
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100"),
                    volume=1,
                ),
                Bar(
                    symbol="357870",
                    timeframe=Timeframe.DAY,
                    timestamp=datetime(
                        2026,
                        4,
                        10,
                        15,
                        30,
                        tzinfo=ZoneInfo("Asia/Seoul"),
                    ),
                    open=Decimal("101"),
                    high=Decimal("102"),
                    low=Decimal("100"),
                    close=Decimal("101"),
                    volume=1,
                ),
            ),
            "one symbol",
        ),
    ],
)
def test_strategy_rejects_invalid_series_structure(
    bars: tuple[Bar, ...],
    match: str,
) -> None:
    strategy = DailyTrendFollowingStrategy()

    with pytest.raises(ValueError, match=match):
        strategy.generate_signal(bars)


def test_strategy_rejects_gapped_bar_series() -> None:
    strategy = ThirtyMinuteTrendStrategy()
    bars = (
        _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T09:00:00+09:00", 100),
        _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T09:30:00+09:00", 101),
        _make_bar("069500", Timeframe.MINUTE_30, "2026-04-10T10:30:00+09:00", 103),
    )

    with pytest.raises(ValueError, match="missing timestamps"):
        strategy.generate_signal(bars)


def _make_bars(
    symbol: str,
    timeframe: Timeframe,
    start: str,
    closes: list[Decimal],
) -> tuple[Bar, ...]:
    calendar = KrxRegularSessionCalendar()
    timestamp = datetime.fromisoformat(start)
    bars: list[Bar] = []
    for close in closes:
        bars.append(
            Bar(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=timestamp,
                open=close,
                high=close + Decimal("1"),
                low=close - Decimal("1"),
                close=close,
                volume=len(bars) + 1,
            ),
        )
        timestamp = calendar.next_timestamp(timestamp, timeframe)
    return tuple(bars)


def _make_bar(
    symbol: str,
    timeframe: Timeframe,
    timestamp: str,
    close: int | Decimal,
) -> Bar:
    value = Decimal(str(close))
    return Bar(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=datetime.fromisoformat(timestamp),
        open=value,
        high=value + Decimal("1"),
        low=value - Decimal("1"),
        close=value,
        volume=1,
    )
