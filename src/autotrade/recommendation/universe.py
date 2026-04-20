from __future__ import annotations

from collections.abc import Sequence
from csv import DictReader
from dataclasses import dataclass
from pathlib import Path

from autotrade.data.validation import normalize_symbol
from autotrade.recommendation.models import SeedUniverseEntry

_REQUIRED_COLUMNS = (
    "symbol",
    "name",
    "asset_type",
    "sector",
    "is_etf",
    "is_inverse",
    "is_leveraged",
    "active",
)


@dataclass(frozen=True, slots=True)
class CsvSeedUniverseSource:
    path: Path

    def __post_init__(self) -> None:
        if self.path.exists() and not self.path.is_file():
            raise ValueError("path must point to a file")

    def load_universe(self) -> tuple[SeedUniverseEntry, ...]:
        if not self.path.exists():
            raise FileNotFoundError(self.path)

        with self.path.open("r", encoding="utf-8", newline="") as handle:
            reader = DictReader(handle)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                raise ValueError("seed universe csv must include a header row")
            _validate_header(fieldnames)

            members: list[SeedUniverseEntry] = []
            seen: set[str] = set()
            for row_number, row in enumerate(reader, start=2):
                symbol = normalize_symbol(_require_csv_field(row, "symbol", row_number))
                if symbol in seen:
                    raise ValueError(f"duplicate symbol: {symbol}")
                seen.add(symbol)
                members.append(
                    SeedUniverseEntry(
                        symbol=symbol,
                        name=_optional_csv_field(row, "name"),
                        asset_type=_optional_csv_field(row, "asset_type"),
                        sector=_optional_csv_field(row, "sector"),
                        is_etf=_parse_bool(
                            row.get("is_etf"),
                            field_name="is_etf",
                            row_number=row_number,
                            default=False,
                        ),
                        is_inverse=_parse_bool(
                            row.get("is_inverse"),
                            field_name="is_inverse",
                            row_number=row_number,
                            default=False,
                        ),
                        is_leveraged=_parse_bool(
                            row.get("is_leveraged"),
                            field_name="is_leveraged",
                            row_number=row_number,
                            default=False,
                        ),
                        active=_parse_bool(
                            row.get("active"),
                            field_name="active",
                            row_number=row_number,
                            default=True,
                        ),
                    )
                )

        if not members:
            raise ValueError("seed universe csv must not be empty")

        return tuple(members)


def load_seed_universe_csv(path: Path) -> tuple[SeedUniverseEntry, ...]:
    return CsvSeedUniverseSource(path=path).load_universe()


def _validate_header(fieldnames: Sequence[str]) -> None:
    missing = [column for column in _REQUIRED_COLUMNS if column not in fieldnames]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"seed universe csv is missing required columns: {joined}")


def _require_csv_field(
    row: dict[str, str | None],
    field_name: str,
    row_number: int,
) -> str:
    value = row.get(field_name)
    if value is None or not value.strip():
        raise ValueError(f"row {row_number} is missing required field: {field_name}")
    return value


def _optional_csv_field(row: dict[str, str | None], field_name: str) -> str | None:
    value = row.get(field_name)
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def _parse_bool(
    value: str | None,
    *,
    field_name: str,
    row_number: int,
    default: bool,
) -> bool:
    if value is None:
        return default

    normalized = value.strip().lower()
    if not normalized:
        return default
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"row {row_number} has invalid boolean for {field_name}: {value}")
