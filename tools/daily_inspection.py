from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autotrade.data import KST  # noqa: E402
from autotrade.report import build_daily_inspection_report  # noqa: E402
from autotrade.report import write_daily_inspection_report  # noqa: E402


def main() -> int:
    raw_log_dir = os.environ.get("AUTOTRADE_LOG_DIR")
    if raw_log_dir is None or not raw_log_dir.strip():
        raise SystemExit("Missing required setting: AUTOTRADE_LOG_DIR")

    generated_at = datetime.now(KST)
    report = build_daily_inspection_report(
        generated_at.date(),
        generated_at=generated_at,
    )
    report_path = write_daily_inspection_report(Path(raw_log_dir).expanduser(), report)
    print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
