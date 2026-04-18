from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autotrade.cli import main as cli_main  # noqa: E402


def main() -> int:
    return cli_main()


if __name__ == "__main__":
    raise SystemExit(main())
