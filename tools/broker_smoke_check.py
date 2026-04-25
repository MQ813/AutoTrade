from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autotrade.broker.smoke import run_read_only_smoke  # noqa: E402
from autotrade.broker.smoke import write_smoke_report  # noqa: E402
from autotrade.config import load_settings  # noqa: E402


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run KIS broker smoke checks.")
    parser.add_argument(
        "--order-history-order-id",
        help="optional order id used to smoke-check KIS order history parsing",
    )
    args = parser.parse_args(argv)

    settings = load_settings()
    report = run_read_only_smoke(
        settings,
        order_history_order_id=args.order_history_order_id,
    )
    log_path = write_smoke_report(settings.log_dir, report)
    print(log_path)
    return 0 if report.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
