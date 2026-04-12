from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol
from typing import runtime_checkable

from autotrade.data.models import Bar
from autotrade.data.models import Timeframe
from autotrade.data.models import UniverseMember


@runtime_checkable
class UniverseSource(Protocol):
    """Read-only contract for loading a target universe."""

    def load_universe(self) -> tuple[UniverseMember, ...]: ...


@runtime_checkable
class BarSource(Protocol):
    """Read-only contract for loading OHLC bars."""

    def load_bars(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[Bar, ...]: ...


@runtime_checkable
class BarStore(Protocol):
    """Write-only contract for persisting OHLC bars."""

    def store_bars(self, bars: Sequence[Bar]) -> None: ...


@runtime_checkable
class BarIntegrityChecker(Protocol):
    """Contract for validating a bar series before persistence."""

    def validate_bars(self, bars: Sequence[Bar]) -> None: ...
