from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from datetime import datetime
from datetime import time
from zoneinfo import ZoneInfo

from autotrade.data.models import Timeframe

KST = ZoneInfo("Asia/Seoul")
KRX_SESSION_OPEN = time(9, 0)
KRX_SESSION_CLOSE = time(15, 30)


@dataclass(frozen=True, slots=True)
class KrxRegularSessionCalendar:
    holiday_dates: frozenset[date] = field(default_factory=frozenset)

    def is_trading_day(self, day: date) -> bool:
        return day.weekday() < 5 and day not in self.holiday_dates

    def is_session_timestamp(self, timestamp: datetime, timeframe: Timeframe) -> bool:
        local_timestamp = timestamp.astimezone(KST)
        if not self.is_trading_day(local_timestamp.date()):
            return False

        if local_timestamp.second != 0 or local_timestamp.microsecond != 0:
            return False

        if timeframe is Timeframe.DAY:
            return local_timestamp.time() == KRX_SESSION_CLOSE

        if local_timestamp.time() < KRX_SESSION_OPEN:
            return False
        if local_timestamp.time() > KRX_SESSION_CLOSE:
            return False

        elapsed = local_timestamp - local_timestamp.replace(
            hour=KRX_SESSION_OPEN.hour,
            minute=KRX_SESSION_OPEN.minute,
            second=0,
            microsecond=0,
        )
        interval_seconds = int(timeframe.interval.total_seconds())
        return elapsed.total_seconds() % interval_seconds == 0

    def next_timestamp(self, timestamp: datetime, timeframe: Timeframe) -> datetime:
        local_timestamp = timestamp.astimezone(KST)

        if timeframe is Timeframe.DAY:
            return datetime.combine(
                self._next_trading_day(local_timestamp.date()),
                KRX_SESSION_CLOSE,
                tzinfo=KST,
            )

        candidate = local_timestamp + timeframe.interval
        if candidate.date() == local_timestamp.date() and self.is_session_timestamp(
            candidate, timeframe
        ):
            return candidate.astimezone(KST)

        return datetime.combine(
            self._next_trading_day(local_timestamp.date()),
            KRX_SESSION_OPEN,
            tzinfo=KST,
        )

    def _next_trading_day(self, day: date) -> date:
        next_day = day
        while True:
            next_day = next_day.fromordinal(next_day.toordinal() + 1)
            if self.is_trading_day(next_day):
                return next_day
