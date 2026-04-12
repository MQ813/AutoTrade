from __future__ import annotations

from datetime import date
from datetime import datetime
from zoneinfo import ZoneInfo

from autotrade.data import KRX_SESSION_CLOSE
from autotrade.data import KRX_SESSION_OPEN
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe


def test_calendar_accepts_weekday_session_timestamp() -> None:
    calendar = KrxRegularSessionCalendar()

    assert calendar.is_trading_day(date(2026, 4, 10)) is True
    assert calendar.is_session_timestamp(
        datetime(2026, 4, 10, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        Timeframe.MINUTE_30,
    )
    assert calendar.is_session_timestamp(
        datetime(2026, 4, 10, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        Timeframe.DAY,
    )


def test_calendar_skips_weekend_when_stepping_timestamps() -> None:
    calendar = KrxRegularSessionCalendar()

    assert calendar.next_timestamp(
        datetime(2026, 4, 10, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        Timeframe.DAY,
    ) == datetime(2026, 4, 13, 15, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    assert calendar.next_timestamp(
        datetime(2026, 4, 10, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        Timeframe.MINUTE_30,
    ) == datetime(2026, 4, 13, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))


def test_calendar_applies_holiday_injection() -> None:
    calendar = KrxRegularSessionCalendar(holiday_dates=frozenset({date(2026, 4, 13)}))

    assert calendar.is_trading_day(date(2026, 4, 13)) is False
    assert calendar.next_timestamp(
        datetime(2026, 4, 10, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
        Timeframe.DAY,
    ) == datetime(2026, 4, 14, 15, 30, tzinfo=ZoneInfo("Asia/Seoul"))


def test_calendar_exports_regular_session_bounds() -> None:
    assert KRX_SESSION_OPEN.hour == 9
    assert KRX_SESSION_CLOSE.hour == 15
    assert KRX_SESSION_CLOSE.minute == 30
