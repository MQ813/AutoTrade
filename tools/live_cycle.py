from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autotrade.cli import main_live_cycle_compat  # noqa: E402


def main() -> int:
    return main_live_cycle_compat()


if __name__ == "__main__":
    raise SystemExit(main())
