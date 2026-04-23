from __future__ import annotations

from pathlib import Path

from autotrade.recommendation.kis_seed_universe import KisStocksInfoFiles
from autotrade.recommendation.kis_seed_universe import SeedUniverseAssetScope
from autotrade.recommendation.kis_seed_universe import build_seed_universe_from_kis_files
from autotrade.recommendation.kis_seed_universe import diff_seed_universe
from autotrade.recommendation.kis_seed_universe import summarize_seed_universe_diff
from autotrade.recommendation.kis_seed_universe import write_seed_universe_csv
from autotrade.recommendation.models import SeedUniverseEntry
from autotrade.recommendation.universe import load_seed_universe_csv
import autotrade.recommendation.kis_seed_universe as kis_seed_universe


def test_build_seed_universe_from_kis_files_maps_metadata_and_filters_scope(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    files = KisStocksInfoFiles(
        kospi_path=raw_dir / "kospi_code.mst",
        kosdaq_path=raw_dir / "kosdaq_code.mst",
        konex_path=raw_dir / "konex_code.mst",
        sector_path=raw_dir / "idxcode.mst",
    )
    files.sector_path.write_text(
        "".join(
            (
                _sector_line("1000", "Technology"),
                _sector_line("2000", "Financials"),
            )
        ),
        encoding="cp949",
    )
    files.kospi_path.write_text(
        "\n".join(
            (
                _kospi_row(
                    symbol="005930",
                    standard_code="KR7005930003",
                    name="Samsung Electronics",
                    security_group_code="ST",
                    sector_medium_code="1000",
                ),
                _kospi_row(
                    symbol="114800",
                    standard_code="KR7114800006",
                    name="KODEX Inverse 2X",
                    security_group_code="EF",
                    etp_product_code="1",
                ),
                _kospi_row(
                    symbol="088980",
                    standard_code="KR7088980004",
                    name="Macquarie Infra",
                    security_group_code="RT",
                    sector_medium_code="2000",
                ),
            )
        )
        + "\n",
        encoding="cp949",
    )
    files.kosdaq_path.write_text(
        _kosdaq_row(
            symbol="035420",
            standard_code="KR7035420009",
            name="NAVER",
            security_group_code="ST",
            sector_medium_code="1000",
        )
        + "\n",
        encoding="cp949",
    )
    files.konex_path.write_text(
        _konex_row(
            symbol="330000",
            standard_code="KR7330000000",
            name="Konex Example",
            management_issue=True,
        )
        + "\n",
        encoding="cp949",
    )

    all_entries = build_seed_universe_from_kis_files(files)
    stock_entries = build_seed_universe_from_kis_files(
        files,
        asset_scope=SeedUniverseAssetScope.STOCK,
    )
    etf_entries = build_seed_universe_from_kis_files(
        files,
        asset_scope=SeedUniverseAssetScope.ETF,
    )

    assert all_entries == (
        SeedUniverseEntry(
            symbol="005930",
            name="Samsung Electronics",
            asset_type="Stock",
            sector="Technology",
        ),
        SeedUniverseEntry(
            symbol="035420",
            name="NAVER",
            asset_type="Stock",
            sector="Technology",
        ),
        SeedUniverseEntry(
            symbol="114800",
            name="KODEX Inverse 2X",
            asset_type="ETF",
            sector="ETF",
            is_etf=True,
            is_inverse=True,
            is_leveraged=True,
        ),
        SeedUniverseEntry(
            symbol="330000",
            name="Konex Example",
            asset_type="Stock",
            sector="KONEX",
            active=False,
        ),
    )
    assert stock_entries == (
        SeedUniverseEntry(
            symbol="005930",
            name="Samsung Electronics",
            asset_type="Stock",
            sector="Technology",
        ),
        SeedUniverseEntry(
            symbol="035420",
            name="NAVER",
            asset_type="Stock",
            sector="Technology",
        ),
        SeedUniverseEntry(
            symbol="330000",
            name="Konex Example",
            asset_type="Stock",
            sector="KONEX",
            active=False,
        ),
    )
    assert etf_entries == (
        SeedUniverseEntry(
            symbol="114800",
            name="KODEX Inverse 2X",
            asset_type="ETF",
            sector="ETF",
            is_etf=True,
            is_inverse=True,
            is_leveraged=True,
        ),
    )


def test_write_seed_universe_csv_round_trips_through_existing_loader(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "universe_stock.csv"
    universe = (
        SeedUniverseEntry(
            symbol="114800",
            name="KODEX Inverse 2X",
            asset_type="ETF",
            sector="ETF",
            is_etf=True,
            is_inverse=True,
            is_leveraged=True,
        ),
        SeedUniverseEntry(
            symbol="005930",
            name="Samsung Electronics",
            asset_type="Stock",
            sector="Technology",
        ),
    )

    write_seed_universe_csv(output_path, universe)

    assert output_path.read_text(encoding="utf-8") == (
        "symbol,name,asset_type,sector,is_etf,is_inverse,is_leveraged,active\n"
        "005930,Samsung Electronics,Stock,Technology,0,0,0,1\n"
        "114800,KODEX Inverse 2X,ETF,ETF,1,1,1,1\n"
    )
    assert load_seed_universe_csv(output_path) == (
        SeedUniverseEntry(
            symbol="005930",
            name="Samsung Electronics",
            asset_type="Stock",
            sector="Technology",
        ),
        SeedUniverseEntry(
            symbol="114800",
            name="KODEX Inverse 2X",
            asset_type="ETF",
            sector="ETF",
            is_etf=True,
            is_inverse=True,
            is_leveraged=True,
        ),
    )


def test_diff_seed_universe_reports_added_removed_and_changed_symbols() -> None:
    before = (
        SeedUniverseEntry(
            symbol="005930",
            name="Samsung Electronics",
            asset_type="Stock",
            sector="Technology",
        ),
        SeedUniverseEntry(
            symbol="114800",
            name="KODEX Inverse 2X",
            asset_type="ETF",
            sector="ETF",
            is_etf=True,
            is_inverse=True,
            is_leveraged=True,
        ),
    )
    after = (
        SeedUniverseEntry(
            symbol="005930",
            name="Samsung Electronics",
            asset_type="Stock",
            sector="Mega Cap Technology",
        ),
        SeedUniverseEntry(
            symbol="330000",
            name="Konex Example",
            asset_type="Stock",
            sector="KONEX",
        ),
    )

    diff = diff_seed_universe(before, after)

    assert diff.added_symbols == ("330000",)
    assert diff.removed_symbols == ("114800",)
    assert diff.changed_symbols == ("005930",)
    assert summarize_seed_universe_diff(diff) == (
        "seed universe diff summary\n"
        "- added: 1\n"
        "- removed: 1\n"
        "- changed: 1\n"
        "- added_symbols: 330000\n"
        "- removed_symbols: 114800\n"
        "- changed_symbols: 005930"
    )


def _sector_line(code: str, name: str) -> str:
    return f"0{code}{name.ljust(40)[:40]}"


def _kospi_row(
    *,
    symbol: str,
    standard_code: str,
    name: str,
    security_group_code: str,
    sector_medium_code: str | None = None,
    etp_product_code: str | None = None,
) -> str:
    fields = [""] * len(kis_seed_universe._KOSPI_TAIL_WIDTHS)
    fields[0] = security_group_code
    fields[3] = sector_medium_code or ""
    fields[12] = etp_product_code or ""
    return _build_kospi_like_row(
        symbol=symbol,
        standard_code=standard_code,
        name=name,
        tail_length=kis_seed_universe._KOSPI_TAIL_LENGTH,
        widths=kis_seed_universe._KOSPI_TAIL_WIDTHS,
        fields=fields,
    )


def _kosdaq_row(
    *,
    symbol: str,
    standard_code: str,
    name: str,
    security_group_code: str,
    sector_medium_code: str | None = None,
    etp_product_code: str | None = None,
) -> str:
    fields = [""] * len(kis_seed_universe._KOSDAQ_TAIL_WIDTHS)
    fields[0] = security_group_code
    fields[3] = sector_medium_code or ""
    fields[8] = etp_product_code or ""
    return _build_kospi_like_row(
        symbol=symbol,
        standard_code=standard_code,
        name=name,
        tail_length=kis_seed_universe._KOSDAQ_TAIL_LENGTH,
        widths=kis_seed_universe._KOSDAQ_TAIL_WIDTHS,
        fields=fields,
    )


def _build_kospi_like_row(
    *,
    symbol: str,
    standard_code: str,
    name: str,
    tail_length: int,
    widths: tuple[int, ...],
    fields: list[str],
) -> str:
    prefix = symbol.ljust(9)[:9] + standard_code.ljust(12)[:12] + name
    tail = "".join(
        value.ljust(width)[:width]
        for value, width in zip(fields, widths, strict=True)
    )
    return prefix + tail.ljust(tail_length)


def _konex_row(
    *,
    symbol: str,
    standard_code: str,
    name: str,
    management_issue: bool = False,
) -> str:
    tail = [" "] * 184
    tail[0:2] = list("ST")
    tail[23] = "Y" if management_issue else " "
    return symbol.ljust(9)[:9] + standard_code.ljust(12)[:12] + name + "".join(tail)
