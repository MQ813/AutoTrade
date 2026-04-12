from __future__ import annotations

import pytest

from autotrade.strategy import DailyTrendFollowingStrategy
from autotrade.strategy import Strategy
from autotrade.strategy import StrategyKind
from autotrade.strategy import ThirtyMinuteTrendStrategy
from autotrade.strategy import create_strategy


@pytest.mark.parametrize(
    ("kind", "expected_type"),
    [
        (StrategyKind.DAILY_TREND_FOLLOWING, DailyTrendFollowingStrategy),
        ("daily_trend_following", DailyTrendFollowingStrategy),
        ("daily_trend", DailyTrendFollowingStrategy),
        (StrategyKind.THIRTY_MINUTE_TREND, ThirtyMinuteTrendStrategy),
        ("30m_low_frequency_trend", ThirtyMinuteTrendStrategy),
        ("30m_trend", ThirtyMinuteTrendStrategy),
    ],
)
def test_create_strategy_returns_expected_strategy(
    kind: StrategyKind | str,
    expected_type: type[object],
) -> None:
    strategy = create_strategy(kind)

    assert isinstance(strategy, expected_type)
    assert isinstance(strategy, Strategy)


def test_create_strategy_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unsupported strategy kind"):
        create_strategy("momentum")
