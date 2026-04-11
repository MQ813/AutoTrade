from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autotrade.broker.smoke import run_read_only_smoke  # noqa: E402
from autotrade.broker.smoke import write_smoke_report  # noqa: E402
from autotrade.config import load_settings  # noqa: E402


def main() -> int:
    settings = load_settings()
    report = run_read_only_smoke(settings)
    log_path = write_smoke_report(settings.log_dir, report)
    print(log_path)
    return 0 if report.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
