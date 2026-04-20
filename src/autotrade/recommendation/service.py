from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from autotrade.data import BarSource
from autotrade.data import Timeframe
from autotrade.recommendation.engine import build_recommendation_report
from autotrade.recommendation.models import RankedRecommendation
from autotrade.recommendation.models import RecommendationExclusion
from autotrade.recommendation.models import RecommendationExclusionStage
from autotrade.recommendation.models import RecommendationParameters
from autotrade.recommendation.models import RecommendationReport
from autotrade.recommendation.models import SeedUniverseEntry
from autotrade.recommendation.models import SelectionExclusion
from autotrade.recommendation.universe import load_seed_universe_csv


def load_seed_universe(path: Path) -> tuple[SeedUniverseEntry, ...]:
    return load_seed_universe_csv(path)


def generate_weekly_recommendation(
    universe: tuple[SeedUniverseEntry, ...],
    *,
    bar_source: BarSource,
    generated_at: datetime,
    parameters: RecommendationParameters | None = None,
) -> RecommendationReport:
    resolved_parameters = parameters or RecommendationParameters()
    bars_by_symbol = {
        member.symbol: bar_source.load_bars(
            member.symbol,
            Timeframe.DAY,
            end=generated_at,
        )
        for member in universe
    }
    base_report = build_recommendation_report(
        universe,
        bars_by_symbol,
        resolved_parameters.to_policy(
            top_n=max(1, len(universe)),
            max_per_sector=max(1, len(universe)),
        ),
        generated_at=generated_at,
    )
    selected, selection_exclusions = _apply_selection_rules(
        base_report.ranked,
        resolved_parameters,
    )
    compatibility_exclusions = _build_compatibility_exclusions(
        base_report.filtered_out,
        selection_exclusions,
    )
    return RecommendationReport(
        as_of=base_report.as_of,
        generated_at=base_report.generated_at,
        universe_size=base_report.universe_size,
        filtered_out=base_report.filtered_out,
        ranked=base_report.ranked,
        selected=tuple(selected),
        selection_exclusions=tuple(selection_exclusions),
        policy=base_report.policy,
        parameters=resolved_parameters,
        compatibility_exclusions=tuple(compatibility_exclusions),
    )


def build_exclusion_reason_summary(
    exclusions: tuple[RecommendationExclusion, ...],
) -> tuple[tuple[str, int], ...]:
    counts = Counter(exclusion.reason for exclusion in exclusions)
    return tuple(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _apply_selection_rules(
    ranked: Sequence[RankedRecommendation],
    parameters: RecommendationParameters,
) -> tuple[list[RankedRecommendation], list[SelectionExclusion]]:
    selected: list[RankedRecommendation] = []
    exclusions: list[SelectionExclusion] = []
    sector_counts: Counter[str] = Counter()
    for candidate in ranked:
        if len(selected) >= parameters.candidate_count:
            break
        reason = _resolve_selection_reason(
            candidate,
            sector_count=sector_counts[(candidate.sector or "").casefold()],
            parameters=parameters,
        )
        if reason is not None:
            exclusions.append(
                SelectionExclusion(
                    recommendation=candidate,
                    reason=reason,
                )
            )
            continue
        selected.append(candidate)
        sector_counts[(candidate.sector or "").casefold()] += 1
    return selected, exclusions


def _resolve_selection_reason(
    candidate: RankedRecommendation,
    *,
    sector_count: int,
    parameters: RecommendationParameters,
) -> str | None:
    if candidate.volatility_20d > parameters.maximum_volatility_20d:
        return "volatility_above_threshold"
    if candidate.average_trading_value_20d > 0 and (
        candidate.average_trading_value_5d
        < candidate.average_trading_value_20d
        * Decimal(str(parameters.minimum_recent_trading_value_ratio))
    ):
        return "recent_liquidity_drop"
    if sector_count >= parameters.max_candidates_per_sector:
        return "sector_cap_reached"
    return None


def _build_compatibility_exclusions(
    filtered_out,
    selection_exclusions: Sequence[SelectionExclusion],
) -> list[RecommendationExclusion]:
    exclusions: list[RecommendationExclusion] = []
    for filtered in filtered_out:
        exclusions.append(
            RecommendationExclusion(
                symbol=filtered.symbol,
                name=filtered.name,
                stage=RecommendationExclusionStage.FILTER,
                reason=filtered.reasons[0],
            )
        )
    exclusions.extend(
        RecommendationExclusion(
            symbol=exclusion.recommendation.symbol,
            name=exclusion.recommendation.name,
            stage=RecommendationExclusionStage.SELECTION,
            reason=exclusion.reason,
        )
        for exclusion in selection_exclusions
    )
    return exclusions
