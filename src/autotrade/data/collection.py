from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from autotrade.data.contracts import BarIntegrityChecker
from autotrade.data.contracts import BarSource
from autotrade.data.contracts import BarStore
from autotrade.data.contracts import UniverseSource
from autotrade.data.models import Timeframe


@dataclass(frozen=True, slots=True)
class BarCollectionService:
    universe_source: UniverseSource
    bar_source: BarSource
    bar_store: BarStore
    bar_checker: BarIntegrityChecker

    def collect_daily_bars(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> None:
        self.collect_bars(Timeframe.DAY, start=start, end=end)

    def collect_intraday_bars(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        timeframe: Timeframe = Timeframe.MINUTE_30,
    ) -> None:
        self.collect_bars(timeframe, start=start, end=end)

    def collect_bars(
        self,
        timeframe: Timeframe,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> None:
        for member in self.universe_source.load_universe():
            bars = self.bar_source.load_bars(
                member.symbol,
                timeframe,
                start=start,
                end=end,
            )
            self.bar_checker.validate_bars(bars)
            self.bar_store.store_bars(bars)
