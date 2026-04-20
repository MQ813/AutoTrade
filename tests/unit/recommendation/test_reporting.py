from __future__ import annotations

import json
from datetime import date
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from autotrade.data import KST
from autotrade.recommendation import FilteredSymbol
from autotrade.recommendation import RankedRecommendation
from autotrade.recommendation import RecommendationMetrics
from autotrade.recommendation import RecommendationPolicy
from autotrade.recommendation import RecommendationReport
from autotrade.recommendation import RecommendationScores
from autotrade.recommendation import SeedUniverseEntry
from autotrade.recommendation import SelectionExclusion
from autotrade.recommendation import write_recommendation_bundle
from autotrade.recommendation import render_recommendation_csv
from autotrade.recommendation import render_recommendation_markdown
from autotrade.recommendation import write_recommendation_csv
from autotrade.recommendation import write_recommendation_json
from autotrade.recommendation import write_recommendation_markdown


def test_recommendation_report_renderers_and_writers_emit_stable_outputs(
    tmp_path: Path,
) -> None:
    report = _make_report()

    markdown = render_recommendation_markdown(report)
    csv_output = render_recommendation_csv(report)
    markdown_path = write_recommendation_markdown(tmp_path / "weekly.md", report)
    csv_path = write_recommendation_csv(tmp_path / "weekly.csv", report)
    json_path = write_recommendation_json(tmp_path / "weekly.json", report)
    payload = json.loads(json_path.read_text(encoding="utf-8"))

    assert "## Selected Candidates" in markdown
    assert "| 1 | AAA1 | Alpha Tech | Technology | 92.50 |" in markdown
    assert "| insufficient_liquidity | 1 |" in markdown
    assert "| sector_cap_reached | 1 |" in markdown
    assert csv_output == (
        "rank,symbol,name,asset_type,sector,total_score,liquidity_score,"
        "momentum_score,volatility_score,trend_score,average_traded_value,"
        "return_20,return_60,return_120,volatility_20,close,sma_fast,sma_slow\n"
        "1,AAA1,Alpha Tech,Stock,Technology,92.5000,100.0000,90.0000,80.0000,"
        "100.0000,1250000,0.110000,0.240000,0.400000,0.120000,150,146,138\n"
    )
    assert markdown_path.read_text(encoding="utf-8") == markdown
    assert csv_path.read_text(encoding="utf-8") == csv_output
    assert payload["summary"]["selected"] == 1
    assert payload["selected"][0]["symbol"] == "AAA1"


def test_write_recommendation_bundle_writes_latest_aliases(tmp_path: Path) -> None:
    artifacts = write_recommendation_bundle(tmp_path, _make_report())

    assert artifacts.markdown_path.exists()
    assert artifacts.csv_path.exists()
    assert artifacts.json_path.exists()
    assert (tmp_path / "recommendations" / "weekly_candidates_latest.md").exists()
    assert (tmp_path / "recommendations" / "weekly_candidates_latest.csv").exists()
    assert (tmp_path / "recommendations" / "weekly_candidates_latest.json").exists()


def _make_report() -> RecommendationReport:
    candidate = RankedRecommendation(
        member=SeedUniverseEntry(
            symbol="AAA1",
            name="Alpha Tech",
            asset_type="Stock",
            sector="Technology",
        ),
        rank=1,
        metrics=RecommendationMetrics(
            average_traded_value=Decimal("1250000"),
            average_traded_value_5d=Decimal("1200000"),
            close=Decimal("150"),
            sma_fast=Decimal("146"),
            sma_slow=Decimal("138"),
            return_20=0.11,
            return_60=0.24,
            return_120=0.4,
            momentum_raw=0.199,
            volatility_20=0.12,
            trend_raw=1.0,
        ),
        scores=RecommendationScores(
            liquidity=100.0,
            momentum=90.0,
            volatility=80.0,
            trend=100.0,
            total=92.5,
        ),
    )
    excluded = RankedRecommendation(
        member=SeedUniverseEntry(
            symbol="BBB1",
            name="Beta Tech",
            asset_type="Stock",
            sector="Technology",
        ),
        rank=2,
        metrics=candidate.metrics,
        scores=RecommendationScores(
            liquidity=90.0,
            momentum=80.0,
            volatility=70.0,
            trend=90.0,
            total=83.5,
        ),
    )
    return RecommendationReport(
        as_of=date(2026, 7, 20),
        generated_at=datetime(2026, 7, 20, 18, 0, tzinfo=KST),
        universe_size=3,
        filtered_out=(
            FilteredSymbol(
                symbol="CCC1",
                name="Cash Thin",
                reasons=("insufficient_liquidity",),
            ),
        ),
        ranked=(candidate, excluded),
        selected=(candidate,),
        selection_exclusions=(
            SelectionExclusion(
                recommendation=excluded,
                reason="sector_cap_reached",
            ),
        ),
        policy=RecommendationPolicy(
            min_history_days=120,
            min_average_traded_value=Decimal("100000"),
            top_n=1,
            max_per_sector=1,
        ),
    )
