from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autotrade.recommendation.kis_seed_universe import DEFAULT_KIS_RAW_DIR  # noqa: E402
from autotrade.recommendation.kis_seed_universe import SeedUniverseAssetScope  # noqa: E402
from autotrade.recommendation.kis_seed_universe import (  # noqa: E402
    build_seed_universe_from_kis_files,
)
from autotrade.recommendation.kis_seed_universe import diff_seed_universe  # noqa: E402
from autotrade.recommendation.kis_seed_universe import download_kis_stocks_info_files  # noqa: E402
from autotrade.recommendation.kis_seed_universe import load_kis_stocks_info_files  # noqa: E402
from autotrade.recommendation.kis_seed_universe import summarize_seed_universe_diff  # noqa: E402
from autotrade.recommendation.kis_seed_universe import write_seed_universe_csv  # noqa: E402
from autotrade.recommendation.universe import load_seed_universe_csv  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a recommendation seed universe CSV from official KIS stocks_info "
            "master files."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="output seed universe CSV path",
    )
    parser.add_argument(
        "--asset-scope",
        type=SeedUniverseAssetScope,
        choices=tuple(SeedUniverseAssetScope),
        default=SeedUniverseAssetScope.ALL,
        help="which assets to emit: all, stock, or etf",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_KIS_RAW_DIR,
        help=f"directory for downloaded KIS raw master files (default: {DEFAULT_KIS_RAW_DIR})",
    )
    parser.add_argument(
        "--compare-to",
        type=Path,
        default=None,
        help="optional existing seed CSV to diff against before overwrite",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="reuse raw files already present in --raw-dir instead of downloading",
    )
    args = parser.parse_args()

    before = (
        load_seed_universe_csv(args.compare_to)
        if args.compare_to is not None and args.compare_to.exists()
        else ()
    )
    files = (
        load_kis_stocks_info_files(args.raw_dir)
        if args.skip_download
        else download_kis_stocks_info_files(args.raw_dir)
    )
    universe = build_seed_universe_from_kis_files(
        files,
        asset_scope=args.asset_scope,
    )
    output_path = write_seed_universe_csv(args.output, universe)
    print(output_path)
    print(f"rows={len(universe)} asset_scope={args.asset_scope.value}")
    print(f"raw_dir={args.raw_dir}")

    if args.compare_to is not None:
        diff = diff_seed_universe(before, universe)
        print(summarize_seed_universe_diff(diff))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
