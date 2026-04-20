from __future__ import annotations

from csv import writer
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import json
from io import StringIO
from json import JSONDecodeError
from pathlib import Path

from autotrade.common.persistence import move_corrupt_file
from autotrade.common.persistence import write_text_atomically
from autotrade.recommendation.approvals import RECOMMENDATION_DIR
from autotrade.recommendation.engine import summarize_filter_reasons
from autotrade.recommendation.engine import summarize_selection_reasons
from autotrade.recommendation.models import FilteredSymbol
from autotrade.recommendation.models import RankedRecommendation
from autotrade.recommendation.models import RecommendationExclusion
from autotrade.recommendation.models import RecommendationExclusionStage
from autotrade.recommendation.models import RecommendationMetrics
from autotrade.recommendation.models import RecommendationParameters
from autotrade.recommendation.models import RecommendationPolicy
from autotrade.recommendation.models import RecommendationReport
from autotrade.recommendation.models import RecommendationScores
from autotrade.recommendation.models import RecommendationWeights
from autotrade.recommendation.models import SeedUniverseEntry
from autotrade.recommendation.models import SelectionExclusion

WEEKLY_CANDIDATE_REPORT_DIR = RECOMMENDATION_DIR


@dataclass(frozen=True, slots=True)
class RecommendationArtifacts:
    markdown_path: Path
    csv_path: Path
    json_path: Path


def render_recommendation_markdown(report: RecommendationReport) -> str:
    lines = [
        "# Weekly Stock Recommendations",
        "",
        f"- as_of: {report.as_of.isoformat()}",
        f"- generated_at: {report.generated_at.isoformat()}",
        f"- universe_size: {report.universe_size}",
        f"- filtered_out: {len(report.filtered_out)}",
        f"- ranked: {len(report.ranked)}",
        f"- selected: {len(report.selected)}",
        "",
        "## Filter Summary",
    ]
    filter_counts = summarize_filter_reasons(report)
    if filter_counts:
        lines.extend(_render_count_table(filter_counts))
    else:
        lines.append("No filtered symbols.")

    lines.extend(
        (
            "",
            "## Selected Candidates",
            "",
            "| rank | symbol | name | sector | total | liquidity | momentum | volatility | trend | avg_traded_value | ret20 | ret60 | ret120 | vol20 |",
            "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        )
    )
    for candidate in report.selected:
        lines.append(_render_candidate_markdown_row(candidate))
    if not report.selected:
        lines.append("| - | - | - | - | - | - | - | - | - | - | - | - | - | - |")

    lines.extend(("", "## Hard Exclusions"))
    selection_counts = summarize_selection_reasons(report)
    if selection_counts:
        lines.extend(_render_count_table(selection_counts))
    else:
        lines.append("No hard exclusions.")

    if report.selection_exclusions:
        lines.extend(
            (
                "",
                "| symbol | reason | rank | total |",
                "| --- | --- | ---: | ---: |",
            )
        )
        for exclusion in report.selection_exclusions:
            lines.append(
                "| "
                f"{exclusion.recommendation.symbol} | "
                f"{exclusion.reason} | "
                f"{exclusion.recommendation.rank} | "
                f"{exclusion.recommendation.total_score:.2f} |"
            )

    lines.extend(
        (
            "",
            "## Codex Review Prompt",
            "",
            "Review the selected candidates below and recommend the best 3-4 symbols.",
            "Focus on concentration risk, momentum durability, liquidity, and reasons to exclude weaker names.",
        )
    )
    return "\n".join(lines) + "\n"


def render_recommendation_csv(report: RecommendationReport) -> str:
    buffer = StringIO()
    csv_writer = writer(buffer, lineterminator="\n")
    csv_writer.writerow(
        [
            "rank",
            "symbol",
            "name",
            "asset_type",
            "sector",
            "total_score",
            "liquidity_score",
            "momentum_score",
            "volatility_score",
            "trend_score",
            "average_traded_value",
            "return_20",
            "return_60",
            "return_120",
            "volatility_20",
            "close",
            "sma_fast",
            "sma_slow",
        ]
    )
    for candidate in report.selected:
        csv_writer.writerow(
            [
                candidate.rank,
                candidate.symbol,
                candidate.name or "",
                candidate.asset_type or "",
                candidate.sector or "",
                f"{candidate.total_score:.4f}",
                f"{candidate.liquidity_score:.4f}",
                f"{candidate.momentum_score:.4f}",
                f"{candidate.volatility_score:.4f}",
                f"{candidate.trend_score:.4f}",
                str(candidate.metrics.average_traded_value),
                f"{candidate.return_20d:.6f}",
                f"{candidate.return_60d:.6f}",
                f"{candidate.return_120d:.6f}",
                f"{candidate.volatility_20d:.6f}",
                str(candidate.metrics.close),
                str(candidate.metrics.sma_fast),
                str(candidate.metrics.sma_slow),
            ]
        )
    return buffer.getvalue()


def write_recommendation_markdown(path: Path, report: RecommendationReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomically(path, render_recommendation_markdown(report))
    return path


def write_recommendation_csv(path: Path, report: RecommendationReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomically(path, render_recommendation_csv(report))
    return path


def write_recommendation_json(path: Path, report: RecommendationReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomically(
        path,
        json.dumps(
            serialize_recommendation_report(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
    )
    return path


def write_recommendation_bundle(
    log_dir: Path,
    report: RecommendationReport,
) -> RecommendationArtifacts:
    output_dir = log_dir / RECOMMENDATION_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"weekly_candidates_{report.as_of.strftime('%Y%m%d')}"
    markdown_path = write_recommendation_markdown(output_dir / f"{stem}.md", report)
    csv_path = write_recommendation_csv(output_dir / f"{stem}.csv", report)
    json_path = write_recommendation_json(output_dir / f"{stem}.json", report)
    write_recommendation_markdown(output_dir / "weekly_candidates_latest.md", report)
    write_recommendation_csv(output_dir / "weekly_candidates_latest.csv", report)
    write_recommendation_json(output_dir / "weekly_candidates_latest.json", report)
    write_recommendation_markdown(output_dir / "latest.md", report)
    write_recommendation_csv(output_dir / "latest.csv", report)
    write_recommendation_json(output_dir / "latest.json", report)
    return RecommendationArtifacts(
        markdown_path=markdown_path,
        csv_path=csv_path,
        json_path=json_path,
    )


def write_weekly_recommendation_outputs(
    log_dir: Path,
    report: RecommendationReport,
) -> RecommendationArtifacts:
    return write_recommendation_bundle(log_dir, report)


def serialize_recommendation_report(report: RecommendationReport) -> dict[str, object]:
    return {
        "as_of": report.as_of.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "policy": _serialize_policy(report.policy),
        "parameters": _serialize_parameters(report.parameters)
        if report.parameters is not None
        else None,
        "summary": {
            "universe_size": report.universe_size,
            "filtered_out": len(report.filtered_out),
            "ranked": len(report.ranked),
            "selected": len(report.selected),
            "selection_exclusions": len(report.selection_exclusions),
        },
        "filtered_symbol_count": report.filtered_symbol_count,
        "ranked_symbol_count": report.ranked_symbol_count,
        "filter_reason_counts": summarize_filter_reasons(report),
        "selection_reason_counts": summarize_selection_reasons(report),
        "filtered_out": [
            {
                "symbol": filtered.symbol,
                "name": filtered.name,
                "reasons": list(filtered.reasons),
            }
            for filtered in report.filtered_out
        ],
        "ranked": [_serialize_candidate(candidate) for candidate in report.ranked],
        "selected": [_serialize_candidate(candidate) for candidate in report.selected],
        "exclusions": [
            {
                "symbol": exclusion.symbol,
                "name": exclusion.name,
                "stage": exclusion.stage.value,
                "reason": exclusion.reason,
            }
            for exclusion in report.exclusions
        ],
        "selection_exclusions": [
            {
                "symbol": exclusion.recommendation.symbol,
                "reason": exclusion.reason,
                "rank": exclusion.recommendation.rank,
                "total_score": exclusion.recommendation.total_score,
            }
            for exclusion in report.selection_exclusions
        ],
    }


def load_weekly_recommendation_report(path: Path) -> RecommendationReport:
    return deserialize_recommendation_report(
        json.loads(path.read_text(encoding="utf-8"))
    )


def load_latest_weekly_recommendation_report(
    log_dir: Path,
) -> RecommendationReport | None:
    output_dir = log_dir / WEEKLY_CANDIDATE_REPORT_DIR
    for candidate_path in (
        output_dir / "latest.json",
        output_dir / "weekly_candidates_latest.json",
    ):
        if not candidate_path.exists():
            continue
        try:
            return load_weekly_recommendation_report(candidate_path)
        except (JSONDecodeError, ValueError):
            move_corrupt_file(candidate_path)
    return None


def deserialize_recommendation_report(payload: object) -> RecommendationReport:
    mapping = _require_mapping(payload, "recommendation report")
    filtered_payload = _require_list(mapping.get("filtered_out"), "filtered_out")
    ranked_payload = _require_list(mapping.get("ranked"), "ranked")
    selected_payload = _require_list(mapping.get("selected"), "selected")
    selection_payload = _require_list(
        mapping.get("selection_exclusions"),
        "selection_exclusions",
    )
    exclusions_payload = mapping.get("exclusions")
    summary_payload = _require_mapping(mapping.get("summary"), "summary")
    return RecommendationReport(
        as_of=_parse_date(mapping.get("as_of"), "as_of"),
        generated_at=_parse_datetime(mapping.get("generated_at"), "generated_at"),
        universe_size=_require_int(
            summary_payload.get("universe_size"), "summary.universe_size"
        ),
        filtered_out=tuple(
            FilteredSymbol(
                symbol=_require_text(item.get("symbol"), "filtered_out.symbol"),
                name=_optional_text(item.get("name")),
                reasons=tuple(
                    _require_str_list(item.get("reasons"), "filtered_out.reasons")
                ),
            )
            for item in (
                _require_mapping(candidate, "filtered_out item")
                for candidate in filtered_payload
            )
        ),
        ranked=tuple(_deserialize_candidate(item) for item in ranked_payload),
        selected=tuple(_deserialize_candidate(item) for item in selected_payload),
        selection_exclusions=tuple(
            _deserialize_selection_exclusion(item) for item in selection_payload
        ),
        policy=_deserialize_policy(_require_mapping(mapping.get("policy"), "policy")),
        parameters=_deserialize_parameters(mapping.get("parameters")),
        compatibility_exclusions=tuple(
            _deserialize_exclusion(item)
            for item in _require_list(exclusions_payload, "exclusions")
        )
        if exclusions_payload is not None
        else (),
    )


def _serialize_candidate(candidate: RankedRecommendation) -> dict[str, object]:
    return {
        "rank": candidate.rank,
        "symbol": candidate.symbol,
        "name": candidate.name,
        "asset_type": candidate.asset_type,
        "sector": candidate.sector,
        "scores": {
            "liquidity": candidate.liquidity_score,
            "momentum": candidate.momentum_score,
            "volatility": candidate.volatility_score,
            "trend": candidate.trend_score,
            "total": candidate.total_score,
        },
        "metrics": {
            "average_traded_value": str(candidate.metrics.average_traded_value),
            "average_traded_value_5d": str(candidate.metrics.average_traded_value_5d),
            "close": str(candidate.metrics.close),
            "sma_fast": str(candidate.metrics.sma_fast),
            "sma_slow": str(candidate.metrics.sma_slow),
            "return_20": candidate.metrics.return_20,
            "return_60": candidate.metrics.return_60,
            "return_120": candidate.metrics.return_120,
            "momentum_raw": candidate.metrics.momentum_raw,
            "volatility_20": candidate.metrics.volatility_20,
            "trend_raw": candidate.metrics.trend_raw,
        },
    }


def _serialize_policy(policy: RecommendationPolicy) -> dict[str, object]:
    return {
        "min_history_days": policy.min_history_days,
        "liquidity_window": policy.liquidity_window,
        "volatility_window": policy.volatility_window,
        "trend_fast_window": policy.trend_fast_window,
        "trend_slow_window": policy.trend_slow_window,
        "min_average_traded_value": str(policy.min_average_traded_value),
        "top_n": policy.top_n,
        "max_per_sector": policy.max_per_sector,
        "allow_etfs": policy.allow_etfs,
        "excluded_symbols": list(policy.excluded_symbols),
        "excluded_sectors": list(policy.excluded_sectors),
        "weights": {
            "liquidity": policy.weights.liquidity,
            "momentum": policy.weights.momentum,
            "volatility": policy.weights.volatility,
            "trend": policy.weights.trend,
        },
    }


def _serialize_parameters(
    parameters: RecommendationParameters,
) -> dict[str, object]:
    return {
        "candidate_count": parameters.candidate_count,
        "minimum_history_days": parameters.minimum_history_days,
        "minimum_average_trading_value": str(parameters.minimum_average_trading_value),
        "maximum_volatility_20d": parameters.maximum_volatility_20d,
        "minimum_recent_trading_value_ratio": parameters.minimum_recent_trading_value_ratio,
        "max_candidates_per_sector": parameters.max_candidates_per_sector,
        "excluded_symbols": list(parameters.excluded_symbols),
        "excluded_sectors": list(parameters.excluded_sectors),
        "liquidity_weight": parameters.liquidity_weight,
        "momentum_weight": parameters.momentum_weight,
        "volatility_weight": parameters.volatility_weight,
        "trend_weight": parameters.trend_weight,
        "allow_etfs": parameters.allow_etfs,
    }


def _deserialize_candidate(payload: object) -> RankedRecommendation:
    item = _require_mapping(payload, "candidate")
    metrics_payload = _require_mapping(item.get("metrics"), "candidate.metrics")
    scores_payload = _require_mapping(item.get("scores"), "candidate.scores")
    return RankedRecommendation(
        member=SeedUniverseEntry(
            symbol=_require_text(item.get("symbol"), "candidate.symbol"),
            name=_optional_text(item.get("name")),
            asset_type=_optional_text(item.get("asset_type")),
            sector=_optional_text(item.get("sector")),
        ),
        rank=_require_int(item.get("rank"), "candidate.rank"),
        metrics=RecommendationMetrics(
            average_traded_value=Decimal(
                _require_text(
                    metrics_payload.get("average_traded_value"),
                    "candidate.metrics.average_traded_value",
                )
            ),
            average_traded_value_5d=Decimal(
                _require_text(
                    metrics_payload.get(
                        "average_traded_value_5d",
                        metrics_payload.get("average_traded_value"),
                    ),
                    "candidate.metrics.average_traded_value_5d",
                )
            ),
            close=Decimal(
                _require_text(metrics_payload.get("close"), "candidate.metrics.close")
            ),
            sma_fast=Decimal(
                _require_text(
                    metrics_payload.get("sma_fast"),
                    "candidate.metrics.sma_fast",
                )
            ),
            sma_slow=Decimal(
                _require_text(
                    metrics_payload.get("sma_slow"),
                    "candidate.metrics.sma_slow",
                )
            ),
            return_20=_require_float(
                metrics_payload.get("return_20"),
                "candidate.metrics.return_20",
            ),
            return_60=_require_float(
                metrics_payload.get("return_60"),
                "candidate.metrics.return_60",
            ),
            return_120=_require_float(
                metrics_payload.get("return_120"),
                "candidate.metrics.return_120",
            ),
            momentum_raw=_require_float(
                metrics_payload.get("momentum_raw"),
                "candidate.metrics.momentum_raw",
            ),
            volatility_20=_require_float(
                metrics_payload.get("volatility_20"),
                "candidate.metrics.volatility_20",
            ),
            trend_raw=_require_float(
                metrics_payload.get("trend_raw"),
                "candidate.metrics.trend_raw",
            ),
        ),
        scores=RecommendationScores(
            liquidity=_require_float(
                scores_payload.get("liquidity"),
                "candidate.scores.liquidity",
            ),
            momentum=_require_float(
                scores_payload.get("momentum"),
                "candidate.scores.momentum",
            ),
            volatility=_require_float(
                scores_payload.get("volatility"),
                "candidate.scores.volatility",
            ),
            trend=_require_float(scores_payload.get("trend"), "candidate.scores.trend"),
            total=_require_float(scores_payload.get("total"), "candidate.scores.total"),
        ),
    )


def _deserialize_selection_exclusion(payload: object) -> SelectionExclusion:
    item = _require_mapping(payload, "selection exclusion")
    return SelectionExclusion(
        recommendation=RankedRecommendation(
            member=SeedUniverseEntry(
                symbol=_require_text(item.get("symbol"), "selection_exclusion.symbol"),
            ),
            rank=_require_int(item.get("rank"), "selection_exclusion.rank"),
            metrics=RecommendationMetrics(
                average_traded_value=Decimal("0"),
                average_traded_value_5d=Decimal("0"),
                close=Decimal("0"),
                sma_fast=Decimal("0"),
                sma_slow=Decimal("0"),
                return_20=0.0,
                return_60=0.0,
                return_120=0.0,
                momentum_raw=0.0,
                volatility_20=0.0,
                trend_raw=0.0,
            ),
            scores=RecommendationScores(
                liquidity=0.0,
                momentum=0.0,
                volatility=0.0,
                trend=0.0,
                total=_require_float(
                    item.get("total_score"),
                    "selection_exclusion.total_score",
                ),
            ),
        ),
        reason=_require_text(item.get("reason"), "selection_exclusion.reason"),
    )


def _deserialize_exclusion(payload: object) -> RecommendationExclusion:
    item = _require_mapping(payload, "exclusion")
    return RecommendationExclusion(
        symbol=_require_text(item.get("symbol"), "exclusion.symbol"),
        name=_optional_text(item.get("name")),
        stage=RecommendationExclusionStage(
            _require_text(item.get("stage"), "exclusion.stage")
        ),
        reason=_require_text(item.get("reason"), "exclusion.reason"),
    )


def _deserialize_policy(payload: dict[str, object]) -> RecommendationPolicy:
    weights_payload = _require_mapping(payload.get("weights"), "policy.weights")
    return RecommendationPolicy(
        min_history_days=_require_int(
            payload.get("min_history_days"),
            "policy.min_history_days",
        ),
        liquidity_window=_require_int(
            payload.get("liquidity_window"),
            "policy.liquidity_window",
        ),
        volatility_window=_require_int(
            payload.get("volatility_window"),
            "policy.volatility_window",
        ),
        trend_fast_window=_require_int(
            payload.get("trend_fast_window"),
            "policy.trend_fast_window",
        ),
        trend_slow_window=_require_int(
            payload.get("trend_slow_window"),
            "policy.trend_slow_window",
        ),
        min_average_traded_value=Decimal(
            _require_text(
                payload.get("min_average_traded_value"),
                "policy.min_average_traded_value",
            )
        ),
        top_n=_require_int(payload.get("top_n"), "policy.top_n"),
        max_per_sector=_require_int(
            payload.get("max_per_sector"),
            "policy.max_per_sector",
        ),
        allow_etfs=_require_bool(payload.get("allow_etfs"), "policy.allow_etfs"),
        excluded_symbols=tuple(
            _require_str_list(
                payload.get("excluded_symbols"), "policy.excluded_symbols"
            )
        ),
        excluded_sectors=tuple(
            _require_str_list(
                payload.get("excluded_sectors"), "policy.excluded_sectors"
            )
        ),
        weights=RecommendationWeights(
            liquidity=_require_float(
                weights_payload.get("liquidity"),
                "policy.weights.liquidity",
            ),
            momentum=_require_float(
                weights_payload.get("momentum"),
                "policy.weights.momentum",
            ),
            volatility=_require_float(
                weights_payload.get("volatility"),
                "policy.weights.volatility",
            ),
            trend=_require_float(weights_payload.get("trend"), "policy.weights.trend"),
        ),
    )


def _deserialize_parameters(payload: object) -> RecommendationParameters | None:
    if payload is None:
        return None
    item = _require_mapping(payload, "parameters")
    return RecommendationParameters(
        candidate_count=_require_int(
            item.get("candidate_count"),
            "parameters.candidate_count",
        ),
        minimum_history_days=_require_int(
            item.get("minimum_history_days"),
            "parameters.minimum_history_days",
        ),
        minimum_average_trading_value=Decimal(
            _require_text(
                item.get("minimum_average_trading_value"),
                "parameters.minimum_average_trading_value",
            )
        ),
        maximum_volatility_20d=_require_float(
            item.get("maximum_volatility_20d"),
            "parameters.maximum_volatility_20d",
        ),
        minimum_recent_trading_value_ratio=_require_float(
            item.get("minimum_recent_trading_value_ratio"),
            "parameters.minimum_recent_trading_value_ratio",
        ),
        max_candidates_per_sector=_require_int(
            item.get("max_candidates_per_sector"),
            "parameters.max_candidates_per_sector",
        ),
        excluded_symbols=tuple(
            _require_str_list(
                item.get("excluded_symbols"),
                "parameters.excluded_symbols",
            )
        ),
        excluded_sectors=tuple(
            _require_str_list(
                item.get("excluded_sectors"),
                "parameters.excluded_sectors",
            )
        ),
        liquidity_weight=_require_float(
            item.get("liquidity_weight"),
            "parameters.liquidity_weight",
        ),
        momentum_weight=_require_float(
            item.get("momentum_weight"),
            "parameters.momentum_weight",
        ),
        volatility_weight=_require_float(
            item.get("volatility_weight"),
            "parameters.volatility_weight",
        ),
        trend_weight=_require_float(
            item.get("trend_weight"),
            "parameters.trend_weight",
        ),
        allow_etfs=_require_bool(item.get("allow_etfs"), "parameters.allow_etfs"),
    )


def _render_count_table(counts: dict[str, int]) -> list[str]:
    lines = [
        "",
        "| reason | count |",
        "| --- | ---: |",
    ]
    for reason, count in counts.items():
        lines.append(f"| {reason} | {count} |")
    return lines


def _render_candidate_markdown_row(candidate: RankedRecommendation) -> str:
    return (
        "| "
        f"{candidate.rank} | "
        f"{candidate.symbol} | "
        f"{candidate.name or ''} | "
        f"{candidate.sector or ''} | "
        f"{candidate.total_score:.2f} | "
        f"{candidate.liquidity_score:.2f} | "
        f"{candidate.momentum_score:.2f} | "
        f"{candidate.volatility_score:.2f} | "
        f"{candidate.trend_score:.2f} | "
        f"{candidate.metrics.average_traded_value} | "
        f"{candidate.return_20d:.4f} | "
        f"{candidate.return_60d:.4f} | "
        f"{candidate.return_120d:.4f} | "
        f"{candidate.volatility_20d:.4f} |"
    )


def _require_mapping(payload: object, label: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be an object")
    if not all(isinstance(key, str) for key in payload):
        raise ValueError(f"{label} keys must be strings")
    return payload


def _require_list(payload: object, label: str) -> list[object]:
    if not isinstance(payload, list):
        raise ValueError(f"{label} must be a list")
    return payload


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be text")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional text value must be a string")
    normalized = value.strip()
    return normalized or None


def _require_float(value: object, field_name: str) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    raise ValueError(f"{field_name} must be numeric")


def _require_int(value: object, field_name: str) -> int:
    if not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _require_str_list(value: object, field_name: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"{field_name} must be a list of strings")
    return value


def _parse_date(value: object, field_name: str):
    return datetime.fromisoformat(
        _require_text(value, field_name) + "T00:00:00+00:00"
    ).date()


def _parse_datetime(value: object, field_name: str) -> datetime:
    return datetime.fromisoformat(_require_text(value, field_name))
