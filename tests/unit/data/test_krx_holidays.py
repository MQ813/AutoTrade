from __future__ import annotations

import importlib.util
from datetime import date
from datetime import datetime
from pathlib import Path
from types import ModuleType
from zoneinfo import ZoneInfo

from autotrade.data import KRX_HOLIDAY_DATES
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe


def test_generated_holiday_dates_match_raw_krx_xls_files() -> None:
    converter = _load_converter_module()
    raw_dir = _repo_root() / "tools/krx_holidays/raw"

    assert frozenset(converter.collect_holiday_dates(raw_dir)) == KRX_HOLIDAY_DATES


def test_converter_extracts_expected_2026_holidays() -> None:
    converter = _load_converter_module()
    raw_file = _repo_root() / "tools/krx_holidays/raw/krx_holidays_2026.xls"

    assert converter.extract_holiday_dates_from_xls(raw_file) == (
        date(2026, 1, 1),
        date(2026, 2, 16),
        date(2026, 2, 17),
        date(2026, 2, 18),
        date(2026, 3, 2),
        date(2026, 5, 1),
        date(2026, 5, 5),
        date(2026, 5, 25),
        date(2026, 8, 17),
        date(2026, 9, 24),
        date(2026, 9, 25),
        date(2026, 10, 5),
        date(2026, 10, 9),
        date(2026, 12, 25),
        date(2026, 12, 31),
    )


def test_calendar_uses_generated_holidays_by_default() -> None:
    calendar = KrxRegularSessionCalendar()

    assert calendar.is_trading_day(date(2026, 1, 1)) is False
    assert calendar.is_trading_day(date(2026, 1, 2)) is True
    assert calendar.next_timestamp(
        datetime(2025, 12, 30, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        Timeframe.DAY,
    ) == datetime(2026, 1, 2, 15, 30, tzinfo=ZoneInfo("Asia/Seoul"))


def _load_converter_module() -> ModuleType:
    module_path = _repo_root() / "tools/krx_holidays/convert_holidays.py"
    spec = importlib.util.spec_from_file_location("krx_holiday_converter", module_path)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]
