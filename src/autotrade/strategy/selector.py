from __future__ import annotations

from enum import StrEnum

from autotrade.strategy.interfaces import Strategy
from autotrade.strategy.trend_following import DailyTrendFollowingStrategy
from autotrade.strategy.trend_following import ThirtyMinuteTrendStrategy


class StrategyKind(StrEnum):
    DAILY_TREND_FOLLOWING = "daily_trend_following"
    THIRTY_MINUTE_TREND = "30m_low_frequency_trend"


_STRATEGY_KIND_ALIASES: dict[str, StrategyKind] = {
    "daily_trend": StrategyKind.DAILY_TREND_FOLLOWING,
    "30m_trend": StrategyKind.THIRTY_MINUTE_TREND,
}


def create_strategy(kind: StrategyKind | str) -> Strategy:
    normalized_kind = _normalize_strategy_kind(kind)
    if normalized_kind is StrategyKind.DAILY_TREND_FOLLOWING:
        return DailyTrendFollowingStrategy()
    if normalized_kind is StrategyKind.THIRTY_MINUTE_TREND:
        return ThirtyMinuteTrendStrategy()
    raise ValueError(f"unsupported strategy kind: {kind!r}")


def _normalize_strategy_kind(kind: StrategyKind | str) -> StrategyKind:
    if isinstance(kind, StrategyKind):
        return kind

    try:
        return StrategyKind(kind)
    except ValueError as error:
        alias = _STRATEGY_KIND_ALIASES.get(kind)
        if alias is None:
            raise ValueError(f"unsupported strategy kind: {kind!r}") from error
        return alias
