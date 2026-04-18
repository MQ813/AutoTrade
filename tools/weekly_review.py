from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping
from datetime import datetime
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autotrade.data import KST  # noqa: E402
from autotrade.config import load_telegram_settings  # noqa: E402
from autotrade.report import CompositeNotifier  # noqa: E402
from autotrade.report import FileNotifier  # noqa: E402
from autotrade.report import TelegramNotifier  # noqa: E402
from autotrade.report import publish_weekly_review_alert  # noqa: E402
from autotrade.report import build_weekly_review_report  # noqa: E402
from autotrade.report import load_daily_inspection_reports  # noqa: E402
from autotrade.report import load_daily_run_reports  # noqa: E402
from autotrade.report import write_weekly_review_report  # noqa: E402

DEFAULT_ENV_FILE = ROOT / ".env"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="AutoTrade 주간 리뷰 파일을 생성하고 필요하면 텔레그램으로 발행합니다."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    args = parser.parse_args()

    environment = _resolve_environment(os.environ, env_file=args.env_file)
    raw_log_dir = environment.get("AUTOTRADE_LOG_DIR")
    if raw_log_dir is None or not raw_log_dir.strip():
        raise SystemExit("Missing required setting: AUTOTRADE_LOG_DIR")

    generated_at = datetime.now(KST)
    week_start = generated_at.date() - timedelta(days=generated_at.weekday())
    log_dir = Path(raw_log_dir).expanduser()
    report = build_weekly_review_report(
        week_start,
        generated_at=generated_at,
        daily_run_reports=load_daily_run_reports(
            log_dir,
            start=week_start,
            end=week_start + timedelta(days=6),
        ),
        daily_inspection_reports=load_daily_inspection_reports(
            log_dir,
            start=week_start,
            end=week_start + timedelta(days=6),
        ),
    )
    report_path = write_weekly_review_report(log_dir, report)
    telegram_settings = load_telegram_settings(environment)
    if telegram_settings.enabled:
        notifier = CompositeNotifier(
            (
                FileNotifier(log_dir / "notifications.jsonl"),
                TelegramNotifier(telegram_settings),
            )
        )
        publish_weekly_review_alert(
            notifier,
            report,
            created_at=generated_at,
        )
    print(report_path)
    return 0


def _resolve_environment(
    base_environment: Mapping[str, str],
    *,
    env_file: Path,
) -> dict[str, str]:
    merged = _load_env_file(env_file)
    merged.update(base_environment)
    return merged


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    if not path.is_file():
        raise ValueError(f".env 경로가 파일이 아닙니다: {path}")

    parsed: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped.removeprefix("export ").strip()
        if "=" not in stripped:
            raise ValueError(f".env 형식이 잘못되었습니다: line {line_number}")
        key, value = stripped.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            raise ValueError(f".env 키가 비어 있습니다: line {line_number}")
        parsed[normalized_key] = _strip_optional_quotes(value.strip())
    return parsed


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
