from __future__ import annotations

import argparse
import re
from datetime import date
from pathlib import Path


DATE_PATTERN = re.compile(
    rb"(?P<value>(?:[0-9]\x00){4}-\x00(?:[0-9]\x00){2}-\x00(?:[0-9]\x00){2})"
)
DEFAULT_RAW_DIR = Path("tools/krx_holidays/raw")
DEFAULT_OUTPUT_PATH = Path("src/autotrade/data/krx_holidays.py")


def extract_holiday_dates_from_xls_bytes(data: bytes) -> tuple[date, ...]:
    dates = {
        date.fromisoformat(match.group("value").decode("utf-16le"))
        for match in DATE_PATTERN.finditer(data)
    }
    return tuple(sorted(dates))


def extract_holiday_dates_from_xls(path: Path) -> tuple[date, ...]:
    return extract_holiday_dates_from_xls_bytes(path.read_bytes())


def collect_holiday_dates(raw_dir: Path) -> tuple[date, ...]:
    dates: set[date] = set()
    for path in sorted(raw_dir.glob("*.xls")):
        dates.update(extract_holiday_dates_from_xls(path))
    return tuple(sorted(dates))


def render_holiday_module(
    holiday_dates: tuple[date, ...],
    source_paths: tuple[Path, ...],
) -> str:
    source_lines = "\n".join(f"# - {path.as_posix()}" for path in source_paths)
    date_block = ""
    if holiday_dates:
        date_lines = ",\n".join(
            f"        date({day.year}, {day.month}, {day.day})" for day in holiday_dates
        )
        date_block = f"{date_lines},\n"
    return (
        '"""Generated KRX regular-session holiday dates.\n\n'
        "Regenerate with:\n"
        "`python tools/krx_holidays/convert_holidays.py`\n\n"
        "Sources:\n"
        f"{source_lines}\n"
        '"""\n'
        "\n"
        "from __future__ import annotations\n"
        "\n"
        "from datetime import date\n"
        "\n"
        "KRX_HOLIDAY_DATES = frozenset(\n"
        "    {\n"
        f"{date_block}"
        "    }\n"
        ")\n"
    )


def write_holiday_module(
    *,
    raw_dir: Path = DEFAULT_RAW_DIR,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    source_paths = tuple(sorted(raw_dir.glob("*.xls")))
    if not source_paths:
        raise ValueError(f"no .xls files found in {raw_dir}")

    module_text = render_holiday_module(
        holiday_dates=collect_holiday_dates(raw_dir),
        source_paths=source_paths,
    )
    output_path.write_text(module_text, encoding="utf-8")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate runtime KRX holiday dates from raw KRX .xls files.",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help=f"directory containing raw KRX .xls files (default: {DEFAULT_RAW_DIR})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"generated Python module path (default: {DEFAULT_OUTPUT_PATH})",
    )
    args = parser.parse_args()
    output_path = write_holiday_module(raw_dir=args.raw_dir, output_path=args.output)
    print(output_path)


if __name__ == "__main__":
    main()
