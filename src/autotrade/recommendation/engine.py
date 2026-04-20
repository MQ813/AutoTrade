from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import date
from datetime import datetime
from decimal import Decimal
import math
import statistics

from autotrade.data import Bar
from autotrade.data import KST
from autotrade.data import Timeframe
from autotrade.data import validate_bar_series
from autotrade.data.validation import normalize_symbol
from autotrade.recommendation.models import FilteredSymbol
from autotrade.recommendation.models import RankedRecommendation
from autotrade.recommendation.models import RecommendationMetrics
from autotrade.recommendation.models import RecommendationPolicy
from autotrade.recommendation.models import RecommendationReport
from autotrade.recommendation.models import RecommendationScores
from autotrade.recommendation.models import SeedUniverseEntry
from autotrade.recommendation.models import SelectionExclusion


def build_recommendation_report(
    universe: Sequence[SeedUniverseEntry],
    bars_by_symbol: Mapping[str, Sequence[Bar]],
    policy: RecommendationPolicy | None = None,
    *,
    as_of: date | None = None,
    generated_at: datetime | None = None,
) -> RecommendationReport:
    if not universe:
        raise ValueError("universe must not be empty")

    resolved_policy = policy or RecommendationPolicy()
    normalized_series = {
        normalize_symbol(symbol): tuple(series)
        for symbol, series in bars_by_symbol.items()
    }
    resolved_as_of = as_of or _resolve_as_of(normalized_series)
    resolved_generated_at = generated_at or datetime.now(KST)
    filtered_out: list[FilteredSymbol] = []
    eligible: list[tuple[SeedUniverseEntry, RecommendationMetrics]] = []
    excluded_symbols = set(resolved_policy.excluded_symbols)
    excluded_sectors = {
        sector.casefold() for sector in resolved_policy.excluded_sectors
    }

    for member in universe:
        reasons: list[str] = []

        if not member.active:
            reasons.append("inactive")
        if member.name is None:
            reasons.append("missing_name")
        if member.asset_type is None:
            reasons.append("missing_asset_type")
        if member.sector is None:
            reasons.append("missing_sector")
        if member.symbol in excluded_symbols:
            reasons.append("symbol_excluded")
        if member.sector is not None and member.sector.casefold() in excluded_sectors:
            reasons.append("sector_excluded")
        if not resolved_policy.allow_etfs and member.is_etf:
            reasons.append("etf_excluded")
        if member.is_inverse:
            reasons.append("inverse_etf")
        if member.is_leveraged:
            reasons.append("leveraged_etf")

        series = normalized_series.get(member.symbol, ())
        if not series:
            reasons.append("missing_daily_bars")
        else:
            _validate_daily_bars(member.symbol, series)
            if len(series) < resolved_policy.min_history_days:
                reasons.append("insufficient_history")
            if series[-1].timestamp.astimezone(KST).date() != resolved_as_of:
                reasons.append("stale_daily_bars")

        metrics: RecommendationMetrics | None = None
        if not reasons and series:
            metrics = _build_metrics(series, resolved_policy)
            if metrics.average_traded_value < resolved_policy.min_average_traded_value:
                reasons.append("insufficient_liquidity")

        if reasons:
            filtered_out.append(
                FilteredSymbol(
                    symbol=member.symbol,
                    name=member.name,
                    reasons=tuple(dict.fromkeys(reasons)),
                )
            )
            continue

        assert metrics is not None
        eligible.append((member, metrics))

    ranked = _rank_candidates(eligible, resolved_policy)
    selected, selection_exclusions = _select_candidates(ranked, resolved_policy)
    return RecommendationReport(
        as_of=resolved_as_of,
        generated_at=resolved_generated_at,
        universe_size=len(universe),
        filtered_out=tuple(filtered_out),
        ranked=tuple(ranked),
        selected=tuple(selected),
        selection_exclusions=tuple(selection_exclusions),
        policy=resolved_policy,
    )


def summarize_filter_reasons(report: RecommendationReport) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for filtered in report.filtered_out:
        counts.update(filtered.reasons)
    return dict(sorted(counts.items()))


def summarize_selection_reasons(report: RecommendationReport) -> dict[str, int]:
    counts = Counter(exclusion.reason for exclusion in report.selection_exclusions)
    return dict(sorted(counts.items()))


def _resolve_as_of(bars_by_symbol: Mapping[str, Sequence[Bar]]) -> date:
    latest_dates = [
        series[-1].timestamp.astimezone(KST).date()
        for series in bars_by_symbol.values()
        if series
    ]
    if not latest_dates:
        raise ValueError("as_of is required when bars_by_symbol is empty")
    return max(latest_dates)


def _validate_daily_bars(symbol: str, bars: Sequence[Bar]) -> None:
    if not bars:
        raise ValueError(f"daily bars must not be empty for {symbol}")
    if any(bar.timeframe is not Timeframe.DAY for bar in bars):
        raise ValueError(f"bars must use timeframe 1d for {symbol}")
    validate_bar_series(bars)


def _build_metrics(
    bars: Sequence[Bar],
    policy: RecommendationPolicy,
) -> RecommendationMetrics:
    closes = tuple(bar.close for bar in bars)
    latest_close = closes[-1]
    sma_fast = _sma(closes[-policy.trend_fast_window :])
    sma_slow = _sma(closes[-policy.trend_slow_window :])
    previous_fast = _sma(closes[-(policy.trend_fast_window + 5) : -5])
    average_traded_value = _average_traded_value(bars[-policy.liquidity_window :])
    average_traded_value_5d = _average_traded_value(bars[-5:])
    return_20 = _calculate_return(closes, 20)
    return_60 = _calculate_return(closes, 60)
    return_120 = _calculate_return(closes, 120)
    momentum_raw = round((return_20 * 0.5) + (return_60 * 0.3) + (return_120 * 0.2), 6)
    volatility_20 = _realized_volatility(closes[-(policy.volatility_window + 1) :])
    trend_conditions = (
        latest_close >= sma_fast,
        latest_close >= sma_slow,
        sma_fast >= sma_slow,
        sma_fast >= previous_fast,
    )
    trend_raw = round(sum(1.0 for matched in trend_conditions if matched) / 4.0, 6)
    return RecommendationMetrics(
        average_traded_value=average_traded_value,
        average_traded_value_5d=average_traded_value_5d,
        close=latest_close,
        sma_fast=sma_fast,
        sma_slow=sma_slow,
        return_20=round(return_20, 6),
        return_60=round(return_60, 6),
        return_120=round(return_120, 6),
        momentum_raw=momentum_raw,
        volatility_20=volatility_20,
        trend_raw=trend_raw,
    )


def _average_traded_value(bars: Sequence[Bar]) -> Decimal:
    traded_values = [bar.close * Decimal(bar.volume) for bar in bars]
    return sum(traded_values, start=Decimal("0")) / Decimal(len(traded_values))


def _calculate_return(closes: Sequence[Decimal], window: int) -> float:
    if len(closes) < 2:
        return 0.0
    reference_index = max(0, len(closes) - window - 1)
    reference_close = closes[reference_index]
    latest_close = closes[-1]
    if reference_close == 0:
        return 0.0
    return float((latest_close / reference_close) - Decimal("1"))


def _realized_volatility(closes: Sequence[Decimal]) -> float:
    if len(closes) < 2:
        return 0.0
    returns = [
        float((current / previous) - Decimal("1"))
        for previous, current in zip(closes, closes[1:], strict=False)
        if previous != 0
    ]
    if len(returns) < 2:
        return 0.0
    return round(statistics.pstdev(returns) * math.sqrt(252), 6)


def _sma(values: Sequence[Decimal]) -> Decimal:
    return sum(values, start=Decimal("0")) / Decimal(len(values))


def _rank_candidates(
    eligible: Sequence[tuple[SeedUniverseEntry, RecommendationMetrics]],
    policy: RecommendationPolicy,
) -> list[RankedRecommendation]:
    if not eligible:
        return []

    liquidity_values = [metrics.average_traded_value for _, metrics in eligible]
    momentum_values = [metrics.momentum_raw for _, metrics in eligible]
    volatility_values = [metrics.volatility_20 for _, metrics in eligible]
    trend_values = [metrics.trend_raw for _, metrics in eligible]
    ranked: list[RankedRecommendation] = []
    for member, metrics in eligible:
        scores = RecommendationScores(
            liquidity=_percentile_score(
                liquidity_values,
                metrics.average_traded_value,
                higher_is_better=True,
            ),
            momentum=_percentile_score(
                momentum_values,
                metrics.momentum_raw,
                higher_is_better=True,
            ),
            volatility=_percentile_score(
                volatility_values,
                metrics.volatility_20,
                higher_is_better=False,
            ),
            trend=_percentile_score(
                trend_values,
                metrics.trend_raw,
                higher_is_better=True,
            ),
            total=0.0,
        )
        total = round(
            (
                (scores.liquidity * policy.weights.liquidity)
                + (scores.momentum * policy.weights.momentum)
                + (scores.volatility * policy.weights.volatility)
                + (scores.trend * policy.weights.trend)
            )
            / policy.weights.total,
            4,
        )
        scores = RecommendationScores(
            liquidity=scores.liquidity,
            momentum=scores.momentum,
            volatility=scores.volatility,
            trend=scores.trend,
            total=total,
        )
        ranked.append(
            RankedRecommendation(
                member=member,
                rank=1,
                metrics=metrics,
                scores=scores,
            )
        )

    ordered = sorted(
        ranked,
        key=lambda candidate: (
            -candidate.scores.total,
            -candidate.scores.liquidity,
            -candidate.scores.momentum,
            candidate.member.symbol,
        ),
    )
    return [
        RankedRecommendation(
            member=candidate.member,
            rank=index,
            metrics=candidate.metrics,
            scores=candidate.scores,
        )
        for index, candidate in enumerate(ordered, start=1)
    ]


def _select_candidates(
    ranked: Sequence[RankedRecommendation],
    policy: RecommendationPolicy,
) -> tuple[list[RankedRecommendation], list[SelectionExclusion]]:
    if not ranked:
        return [], []

    selected: list[RankedRecommendation] = []
    exclusions: list[SelectionExclusion] = []
    sector_counts: Counter[str] = Counter()
    for candidate in ranked:
        if len(selected) >= policy.top_n:
            break
        sector_key = (candidate.member.sector or "").casefold()
        if sector_counts[sector_key] >= policy.max_per_sector:
            exclusions.append(
                SelectionExclusion(
                    recommendation=candidate,
                    reason="sector_cap_reached",
                )
            )
            continue
        selected.append(candidate)
        sector_counts[sector_key] += 1
    return selected, exclusions


def _percentile_score(
    values: Sequence[Decimal | float],
    value: Decimal | float,
    *,
    higher_is_better: bool,
) -> float:
    if len(values) == 1:
        return 100.0

    lower = sum(1 for candidate in values if candidate < value)
    equal = sum(1 for candidate in values if candidate == value)
    rank = lower + ((equal - 1) / 2)
    percentile = (rank / (len(values) - 1)) * 100
    score = percentile if higher_is_better else (100 - percentile)
    return round(score, 4)
