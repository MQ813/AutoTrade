from __future__ import annotations

from pathlib import Path

import pytest

from autotrade.recommendation import CsvSeedUniverseSource
from autotrade.recommendation import SeedUniverseEntry


def test_csv_seed_universe_source_loads_rows_and_normalizes_values(
    tmp_path: Path,
) -> None:
    path = tmp_path / "seed_universe.csv"
    path.write_text(
        "\n".join(
            (
                "symbol,name,asset_type,sector,is_etf,is_inverse,is_leveraged,active",
                " 069500 ,KODEX 200,ETF,Index,1,0,0,1",
                "005930, Samsung Electronics ,Stock,Technology,0,0,0,yes",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    loaded = CsvSeedUniverseSource(path).load_universe()

    assert loaded == (
        SeedUniverseEntry(
            symbol="069500",
            name="KODEX 200",
            asset_type="ETF",
            sector="Index",
            is_etf=True,
        ),
        SeedUniverseEntry(
            symbol="005930",
            name="Samsung Electronics",
            asset_type="Stock",
            sector="Technology",
        ),
    )


def test_csv_seed_universe_source_rejects_missing_required_columns(
    tmp_path: Path,
) -> None:
    path = tmp_path / "seed_universe.csv"
    path.write_text(
        "symbol,name,asset_type,is_etf,is_inverse,is_leveraged,active\n"
        "069500,KODEX 200,ETF,1,0,0,1\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing required columns: sector"):
        CsvSeedUniverseSource(path).load_universe()
