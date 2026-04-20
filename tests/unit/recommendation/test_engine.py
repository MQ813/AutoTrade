from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from autotrade.data import Bar
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.recommendation import RecommendationPolicy
from autotrade.recommendation import RecommendationWeights
from autotrade.recommendation import SeedUniverseEntry
from autotrade.recommendation import build_recommendation_report
from autotrade.recommendation import summarize_filter_reasons
from autotrade.recommendation import summarize_selection_reasons


def test_build_recommendation_report_filters_ranks_and_applies_sector_cap() -> None:
    universe = (
        SeedUniverseEntry(
            symbol="AAA1",
            name="Alpha Tech",
            asset_type="Stock",
            sector="Technology",
        ),
        SeedUniverseEntry(
            symbol="BBB1",
            name="Beta Tech",
            asset_type="Stock",
            sector="Technology",
        ),
        SeedUniverseEntry(
            symbol="CCC1",
            name="Care Health",
            asset_type="Stock",
            sector="Healthcare",
        ),
        SeedUniverseEntry(
            symbol="DDD1",
            name="Delta Leveraged",
            asset_type="ETF",
            sector="Index",
            is_etf=True,
            is_leveraged=True,
        ),
        SeedUniverseEntry(
            symbol="EEE1",
            name="",
            asset_type="Stock",
            sector="Consumer",
        ),
        SeedUniverseEntry(
            symbol="FFF1",
            name="Flat Liquidity",
            asset_type="Stock",
            sector="Industrial",
        ),
    )
    policy = RecommendationPolicy(
        min_history_days=130,
        min_average_traded_value=Decimal("500000"),
        top_n=2,
        max_per_sector=1,
        weights=RecommendationWeights(
            liquidity=0.4,
            momentum=0.4,
            volatility=0.1,
            trend=0.1,
        ),
    )
    bars_by_symbol = {
        "AAA1": _make_daily_series("AAA1", base_price=100, daily_step=1.4, volume=8000),
        "BBB1": _make_daily_series("BBB1", base_price=90, daily_step=1.2, volume=7600),
        "CCC1": _make_daily_series("CCC1", base_price=120, daily_step=0.5, volume=7200),
        "DDD1": _make_daily_series("DDD1", base_price=70, daily_step=1.8, volume=9000),
        "FFF1": _make_daily_series("FFF1", base_price=30, daily_step=0.02, volume=200),
    }
    as_of = bars_by_symbol["AAA1"][-1].timestamp.astimezone(KST).date()

    report = build_recommendation_report(
        universe,
        bars_by_symbol,
        policy,
        as_of=as_of,
        generated_at=datetime(2026, 7, 20, 18, 0, tzinfo=KST),
    )

    assert [candidate.member.symbol for candidate in report.ranked] == [
        "AAA1",
        "BBB1",
        "CCC1",
    ]
    assert [candidate.member.symbol for candidate in report.selected] == [
        "AAA1",
        "CCC1",
    ]
    assert [
        exclusion.recommendation.member.symbol
        for exclusion in report.selection_exclusions
    ] == ["BBB1"]
    assert summarize_selection_reasons(report) == {"sector_cap_reached": 1}
    assert summarize_filter_reasons(report) == {
        "insufficient_liquidity": 1,
        "leveraged_etf": 1,
        "missing_name": 1,
        "missing_daily_bars": 1,
    }
    assert report.selected[0].scores.total > report.selected[1].scores.total


def test_build_recommendation_report_uses_symbol_tiebreak_for_equal_scores() -> None:
    universe = (
        SeedUniverseEntry(
            symbol="AAA2",
            name="Alpha",
            asset_type="Stock",
            sector="Technology",
        ),
        SeedUniverseEntry(
            symbol="BBB2",
            name="Beta",
            asset_type="Stock",
            sector="Healthcare",
        ),
    )
    identical_series = _make_daily_series(
        "AAA2",
        base_price=80,
        daily_step=0.6,
        volume=5000,
    )
    bars_by_symbol = {
        "AAA2": identical_series,
        "BBB2": tuple(
            Bar(
                symbol="BBB2",
                timeframe=bar.timeframe,
                timestamp=bar.timestamp,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
            )
            for bar in identical_series
        ),
    }
    as_of = bars_by_symbol["AAA2"][-1].timestamp.astimezone(KST).date()

    report = build_recommendation_report(
        universe,
        bars_by_symbol,
        RecommendationPolicy(
            min_history_days=130,
            min_average_traded_value=Decimal("100000"),
            top_n=2,
            max_per_sector=1,
        ),
        as_of=as_of,
        generated_at=datetime(2026, 7, 20, 18, 0, tzinfo=KST),
    )

    assert [candidate.member.symbol for candidate in report.ranked] == ["AAA2", "BBB2"]


def _make_daily_series(
    symbol: str,
    *,
    base_price: int,
    daily_step: float,
    volume: int,
    length: int = 140,
) -> tuple[Bar, ...]:
    calendar = KrxRegularSessionCalendar()
    timestamp = datetime(2026, 1, 2, 15, 30, tzinfo=KST)
    close = Decimal(str(base_price))
    bars: list[Bar] = []
    for _ in range(length):
        close += Decimal(str(daily_step))
        bars.append(
            Bar(
                symbol=symbol,
                timeframe=Timeframe.DAY,
                timestamp=timestamp,
                open=close - Decimal("1"),
                high=close + Decimal("1"),
                low=close - Decimal("2"),
                close=close,
                volume=volume,
            )
        )
        timestamp = calendar.next_timestamp(timestamp, Timeframe.DAY)
    return tuple(bars)
