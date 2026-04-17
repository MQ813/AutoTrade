from __future__ import annotations

from csv import DictReader
from collections.abc import Sequence
from csv import writer
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from autotrade.data.models import Bar
from autotrade.data.models import Timeframe
from autotrade.data.validation import normalize_symbol

_CSV_HEADER = (
    "symbol",
    "timeframe",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


@dataclass(frozen=True, slots=True)
class CsvBarStore:
    root_dir: Path

    def __post_init__(self) -> None:
        if self.root_dir.exists() and not self.root_dir.is_dir():
            raise ValueError("root_dir must point to a directory")

    def store_bars(self, bars: Sequence[Bar]) -> None:
        if not bars:
            return

        grouped_bars: dict[tuple[str, str], list[Bar]] = {}
        for bar in bars:
            key = (normalize_symbol(bar.symbol), bar.timeframe.value)
            grouped_bars.setdefault(key, []).append(bar)

        for (symbol, timeframe_value), series in sorted(
            grouped_bars.items(),
            key=lambda item: (item[0][1], item[0][0]),
        ):
            series.sort(key=lambda bar: bar.timestamp)
            path = self.root_dir / timeframe_value / f"{symbol}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8", newline="") as handle:
                csv_writer = writer(handle)
                csv_writer.writerow(_CSV_HEADER)
                for bar in series:
                    csv_writer.writerow(
                        [
                            normalize_symbol(bar.symbol),
                            bar.timeframe.value,
                            bar.timestamp.isoformat(),
                            str(bar.open),
                            str(bar.high),
                            str(bar.low),
                            str(bar.close),
                            str(bar.volume),
                        ]
                    )


@dataclass(frozen=True, slots=True)
class CsvBarSource:
    root_dir: Path

    def __post_init__(self) -> None:
        if self.root_dir.exists() and not self.root_dir.is_dir():
            raise ValueError("root_dir must point to a directory")

    def load_bars(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[Bar, ...]:
        normalized_symbol = normalize_symbol(symbol)
        path = self.root_dir / timeframe.value / f"{normalized_symbol}.csv"
        if not path.exists():
            return ()

        bars: list[Bar] = []
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = DictReader(handle)
            for row in reader:
                bar = _parse_csv_bar(
                    row, expected_symbol=normalized_symbol, timeframe=timeframe
                )
                if start is not None and bar.timestamp < start:
                    continue
                if end is not None and bar.timestamp > end:
                    continue
                bars.append(bar)
        bars.sort(key=lambda bar: bar.timestamp)
        return tuple(bars)


def _parse_csv_bar(
    row: dict[str, str | None],
    *,
    expected_symbol: str,
    timeframe: Timeframe,
) -> Bar:
    symbol = normalize_symbol(_require_csv_field(row, "symbol"))
    if symbol != expected_symbol:
        raise ValueError(
            f"csv row symbol mismatch: expected {expected_symbol}, got {symbol}"
        )

    timeframe_value = _require_csv_field(row, "timeframe")
    if timeframe_value != timeframe.value:
        raise ValueError(
            f"csv row timeframe mismatch: expected {timeframe.value}, got {timeframe_value}"
        )

    return Bar(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=datetime.fromisoformat(_require_csv_field(row, "timestamp")),
        open=Decimal(_require_csv_field(row, "open")),
        high=Decimal(_require_csv_field(row, "high")),
        low=Decimal(_require_csv_field(row, "low")),
        close=Decimal(_require_csv_field(row, "close")),
        volume=int(_require_csv_field(row, "volume")),
    )


def _require_csv_field(row: dict[str, str | None], field_name: str) -> str:
    value = row.get(field_name)
    if value is None or not value.strip():
        raise ValueError(f"csv row is missing required field: {field_name}")
    return value
