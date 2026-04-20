from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from autotrade.data.validation import normalize_symbol
from autotrade.data.validation import normalize_symbols


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def _normalize_sectors(sectors: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(normalized for sector in sectors if (normalized := sector.strip()))


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_positive_int(field_name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


def _require_non_negative_decimal(field_name: str, value: Decimal) -> None:
    if value < Decimal("0"):
        raise ValueError(f"{field_name} must be non-negative")


def _require_non_negative_float(field_name: str, value: float) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _require_score(field_name: str, value: float) -> None:
    if value < 0 or value > 100:
        raise ValueError(f"{field_name} must be between 0 and 100")


@dataclass(frozen=True, slots=True)
class SeedUniverseEntry:
    symbol: str
    name: str | None = None
    asset_type: str | None = None
    sector: str | None = None
    is_etf: bool = False
    is_inverse: bool = False
    is_leveraged: bool = False
    active: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _normalize_optional_text(self.name))
        object.__setattr__(
            self,
            "asset_type",
            _normalize_optional_text(self.asset_type),
        )
        object.__setattr__(self, "sector", _normalize_optional_text(self.sector))


RecommendationUniverseMember = SeedUniverseEntry


@dataclass(frozen=True, slots=True)
class RecommendationWeights:
    liquidity: float = 0.35
    momentum: float = 0.35
    volatility: float = 0.15
    trend: float = 0.15

    def __post_init__(self) -> None:
        for field_name in ("liquidity", "momentum", "volatility", "trend"):
            _require_non_negative_float(field_name, getattr(self, field_name))
        if self.total <= 0:
            raise ValueError("weights must sum to a positive value")

    @property
    def total(self) -> float:
        return self.liquidity + self.momentum + self.volatility + self.trend


@dataclass(frozen=True, slots=True)
class RecommendationPolicy:
    min_history_days: int = 120
    liquidity_window: int = 20
    volatility_window: int = 20
    trend_fast_window: int = 20
    trend_slow_window: int = 60
    min_average_traded_value: Decimal = Decimal("100000000")
    top_n: int = 20
    max_per_sector: int = 3
    allow_etfs: bool = True
    excluded_symbols: tuple[str, ...] = ()
    excluded_sectors: tuple[str, ...] = ()
    weights: RecommendationWeights = field(default_factory=RecommendationWeights)

    def __post_init__(self) -> None:
        for field_name in (
            "min_history_days",
            "liquidity_window",
            "volatility_window",
            "trend_fast_window",
            "trend_slow_window",
            "top_n",
            "max_per_sector",
        ):
            _require_positive_int(field_name, getattr(self, field_name))
        _require_non_negative_decimal(
            "min_average_traded_value",
            self.min_average_traded_value,
        )
        object.__setattr__(
            self,
            "excluded_symbols",
            normalize_symbols(self.excluded_symbols) if self.excluded_symbols else (),
        )
        object.__setattr__(
            self,
            "excluded_sectors",
            _normalize_sectors(self.excluded_sectors),
        )


@dataclass(frozen=True, slots=True)
class RecommendationParameters:
    candidate_count: int = 20
    minimum_history_days: int = 120
    minimum_average_trading_value: Decimal = Decimal("100000000")
    maximum_volatility_20d: float = 0.05
    minimum_recent_trading_value_ratio: float = 0.5
    max_candidates_per_sector: int = 3
    excluded_symbols: tuple[str, ...] = ()
    excluded_sectors: tuple[str, ...] = ()
    liquidity_weight: float = 0.35
    momentum_weight: float = 0.35
    volatility_weight: float = 0.15
    trend_weight: float = 0.15
    allow_etfs: bool = True

    def __post_init__(self) -> None:
        for field_name in (
            "candidate_count",
            "minimum_history_days",
            "max_candidates_per_sector",
        ):
            _require_positive_int(field_name, getattr(self, field_name))
        _require_non_negative_decimal(
            "minimum_average_trading_value",
            self.minimum_average_trading_value,
        )
        for field_name in (
            "maximum_volatility_20d",
            "minimum_recent_trading_value_ratio",
            "liquidity_weight",
            "momentum_weight",
            "volatility_weight",
            "trend_weight",
        ):
            _require_non_negative_float(field_name, getattr(self, field_name))
        object.__setattr__(
            self,
            "excluded_symbols",
            normalize_symbols(self.excluded_symbols) if self.excluded_symbols else (),
        )
        object.__setattr__(
            self,
            "excluded_sectors",
            _normalize_sectors(self.excluded_sectors),
        )
        if self.weights.total <= 0:
            raise ValueError("parameter weights must sum to a positive value")

    @property
    def weights(self) -> RecommendationWeights:
        return RecommendationWeights(
            liquidity=self.liquidity_weight,
            momentum=self.momentum_weight,
            volatility=self.volatility_weight,
            trend=self.trend_weight,
        )

    def to_policy(
        self,
        *,
        top_n: int | None = None,
        max_per_sector: int | None = None,
    ) -> RecommendationPolicy:
        return RecommendationPolicy(
            min_history_days=self.minimum_history_days,
            min_average_traded_value=self.minimum_average_trading_value,
            top_n=top_n or self.candidate_count,
            max_per_sector=max_per_sector or self.max_candidates_per_sector,
            allow_etfs=self.allow_etfs,
            excluded_symbols=self.excluded_symbols,
            excluded_sectors=self.excluded_sectors,
            weights=self.weights,
        )


class RecommendationExclusionStage(StrEnum):
    FILTER = "filter"
    SELECTION = "selection"


@dataclass(frozen=True, slots=True)
class RecommendationExclusion:
    symbol: str
    name: str | None
    stage: RecommendationExclusionStage
    reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _normalize_optional_text(self.name))
        if not self.reason.strip():
            raise ValueError("reason must not be blank")


@dataclass(frozen=True, slots=True)
class RecommendationMetrics:
    average_traded_value: Decimal
    average_traded_value_5d: Decimal
    close: Decimal
    sma_fast: Decimal
    sma_slow: Decimal
    return_20: float
    return_60: float
    return_120: float
    momentum_raw: float
    volatility_20: float
    trend_raw: float

    def __post_init__(self) -> None:
        for field_name in (
            "average_traded_value",
            "average_traded_value_5d",
            "close",
            "sma_fast",
            "sma_slow",
        ):
            _require_non_negative_decimal(field_name, getattr(self, field_name))
        _require_non_negative_float("volatility_20", self.volatility_20)
        if self.trend_raw < 0 or self.trend_raw > 1:
            raise ValueError("trend_raw must be between 0 and 1")

    @property
    def average_trading_value_20d(self) -> Decimal:
        return self.average_traded_value

    @property
    def average_trading_value_5d(self) -> Decimal:
        return self.average_traded_value_5d

    @property
    def volatility_20d(self) -> float:
        return self.volatility_20


@dataclass(frozen=True, slots=True)
class RecommendationScores:
    liquidity: float
    momentum: float
    volatility: float
    trend: float
    total: float

    def __post_init__(self) -> None:
        _require_score("liquidity", self.liquidity)
        _require_score("momentum", self.momentum)
        _require_score("volatility", self.volatility)
        _require_score("trend", self.trend)
        _require_score("total", self.total)


@dataclass(frozen=True, slots=True)
class FilteredSymbol:
    symbol: str
    name: str | None
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _normalize_optional_text(self.name))
        if not self.reasons:
            raise ValueError("reasons must not be empty")


@dataclass(frozen=True, slots=True)
class RankedRecommendation:
    member: SeedUniverseEntry
    rank: int
    metrics: RecommendationMetrics
    scores: RecommendationScores

    def __post_init__(self) -> None:
        _require_positive_int("rank", self.rank)

    @property
    def symbol(self) -> str:
        return self.member.symbol

    @property
    def name(self) -> str | None:
        return self.member.name

    @property
    def asset_type(self) -> str | None:
        return self.member.asset_type

    @property
    def sector(self) -> str | None:
        return self.member.sector

    @property
    def is_etf(self) -> bool:
        return self.member.is_etf

    @property
    def total_score(self) -> float:
        return self.scores.total

    @property
    def liquidity_score(self) -> float:
        return self.scores.liquidity

    @property
    def momentum_score(self) -> float:
        return self.scores.momentum

    @property
    def volatility_score(self) -> float:
        return self.scores.volatility

    @property
    def trend_score(self) -> float:
        return self.scores.trend

    @property
    def average_trading_value_20d(self) -> Decimal:
        return self.metrics.average_traded_value

    @property
    def average_trading_value_5d(self) -> Decimal:
        return self.metrics.average_traded_value_5d

    @property
    def return_20d(self) -> float:
        return self.metrics.return_20

    @property
    def return_60d(self) -> float:
        return self.metrics.return_60

    @property
    def return_120d(self) -> float:
        return self.metrics.return_120

    @property
    def volatility_20d(self) -> float:
        return self.metrics.volatility_20


RecommendationCandidate = RankedRecommendation


@dataclass(frozen=True, slots=True)
class SelectionExclusion:
    recommendation: RankedRecommendation
    reason: str

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("reason must not be blank")


@dataclass(frozen=True, slots=True)
class RecommendationReport:
    as_of: date
    generated_at: datetime
    universe_size: int
    filtered_out: tuple[FilteredSymbol, ...]
    ranked: tuple[RankedRecommendation, ...]
    selected: tuple[RankedRecommendation, ...]
    selection_exclusions: tuple[SelectionExclusion, ...]
    policy: RecommendationPolicy
    parameters: RecommendationParameters | None = None
    compatibility_exclusions: tuple[RecommendationExclusion, ...] = ()

    def __post_init__(self) -> None:
        _require_aware_datetime("generated_at", self.generated_at)
        if self.universe_size < 0:
            raise ValueError("universe_size must be non-negative")
        if len(self.selected) > self.policy.top_n:
            raise ValueError("selected recommendations exceed top_n")

    @property
    def trading_day(self) -> date:
        return self.as_of

    @property
    def filtered_symbol_count(self) -> int:
        return len(self.ranked)

    @property
    def ranked_symbol_count(self) -> int:
        return len(self.ranked)

    @property
    def candidates(self) -> tuple[RankedRecommendation, ...]:
        return self.selected

    @property
    def exclusions(self) -> tuple[RecommendationExclusion, ...]:
        return self.compatibility_exclusions


@dataclass(frozen=True, slots=True, init=False)
class ApprovedSymbolsRecord:
    created_at: datetime
    symbols: tuple[str, ...]
    source_report_path: str | None
    notes: str | None
    as_of: date

    def __init__(
        self,
        *,
        created_at: datetime | None = None,
        approved_at: datetime | None = None,
        symbols: tuple[str, ...],
        source_report_path: str | None = None,
        source_report: str | None = None,
        notes: str | None = None,
        as_of: date | None = None,
    ) -> None:
        resolved_created_at = created_at or approved_at
        if resolved_created_at is None:
            raise TypeError("created_at or approved_at is required")
        if (
            created_at is not None
            and approved_at is not None
            and created_at != approved_at
        ):
            raise ValueError("created_at and approved_at must match when both provided")

        _require_aware_datetime("created_at", resolved_created_at)
        object.__setattr__(self, "created_at", resolved_created_at)
        object.__setattr__(self, "symbols", normalize_symbols(symbols))
        object.__setattr__(
            self,
            "source_report_path",
            _normalize_optional_text(source_report_path or source_report),
        )
        object.__setattr__(self, "notes", _normalize_optional_text(notes))
        object.__setattr__(self, "as_of", as_of or resolved_created_at.date())

    @property
    def approved_at(self) -> datetime:
        return self.created_at

    @property
    def source_report(self) -> str | None:
        return self.source_report_path
