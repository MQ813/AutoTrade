from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Sequence
from datetime import datetime

from autotrade.data.calendar import KrxRegularSessionCalendar
from autotrade.data.models import Bar


def normalize_symbol(symbol: str) -> str:
    if not isinstance(symbol, str):
        raise ValueError("symbol must be a string")

    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("symbol must not be blank")
    return normalized


def normalize_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()

    for symbol in symbols:
        candidate = normalize_symbol(symbol)
        if candidate in seen:
            raise ValueError(f"duplicate symbol: {candidate}")
        seen.add(candidate)
        normalized.append(candidate)

    if not normalized:
        raise ValueError("symbols must not be empty")

    return tuple(normalized)


def find_missing_bar_timestamps(
    bars: Sequence[Bar],
    calendar: KrxRegularSessionCalendar | None = None,
) -> tuple[datetime, ...]:
    if not bars:
        return ()

    _validate_series_membership(bars)
    _validate_series_ordering(bars)
    calendar = calendar or KrxRegularSessionCalendar()
    _validate_series_calendar(bars, calendar)

    if len(bars) < 2:
        return ()

    missing: list[datetime] = []
    previous = bars[0]
    expected = calendar.next_timestamp(previous.timestamp, previous.timeframe)

    for bar in bars[1:]:
        current = bar.timestamp

        while expected < current:
            missing.append(expected)
            expected = calendar.next_timestamp(expected, bar.timeframe)

        previous = bar
        expected = calendar.next_timestamp(previous.timestamp, previous.timeframe)

    return tuple(missing)


def validate_bar_series(
    bars: Sequence[Bar],
    calendar: KrxRegularSessionCalendar | None = None,
) -> None:
    if not bars:
        return

    missing = find_missing_bar_timestamps(bars, calendar=calendar)
    if missing:
        first_missing = missing[0].isoformat()
        raise ValueError(f"bars contain missing timestamps starting at {first_missing}")


def _validate_series_membership(bars: Sequence[Bar]) -> None:
    reference_symbol = bars[0].symbol
    reference_timeframe = bars[0].timeframe

    for bar in bars:
        if bar.symbol != reference_symbol:
            raise ValueError("bars must belong to one symbol")
        if bar.timeframe != reference_timeframe:
            raise ValueError("bars must share one timeframe")


def _validate_series_ordering(bars: Sequence[Bar]) -> None:
    previous_timestamp = bars[0].timestamp

    for bar in bars[1:]:
        current_timestamp = bar.timestamp
        if current_timestamp < previous_timestamp:
            raise ValueError("bars must be sorted by timestamp")
        if current_timestamp == previous_timestamp:
            raise ValueError("bars must not contain duplicate timestamps")
        previous_timestamp = current_timestamp


def _validate_series_calendar(
    bars: Sequence[Bar],
    calendar: KrxRegularSessionCalendar,
) -> None:
    for bar in bars:
        if not calendar.is_session_timestamp(bar.timestamp, bar.timeframe):
            raise ValueError(
                "bars must use valid KRX session timestamps: "
                f"{bar.timestamp.isoformat()}",
            )
