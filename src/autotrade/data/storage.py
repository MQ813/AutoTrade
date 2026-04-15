from __future__ import annotations

from collections.abc import Sequence
from csv import writer
from dataclasses import dataclass
from pathlib import Path

from autotrade.data.models import Bar
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
