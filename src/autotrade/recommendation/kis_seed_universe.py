from __future__ import annotations

from collections.abc import Sequence
from csv import writer
from dataclasses import dataclass
from enum import StrEnum
from io import BytesIO
from io import StringIO
from pathlib import Path
import ssl
import urllib.request
from zipfile import ZipFile

from autotrade.common.persistence import write_text_atomically
from autotrade.recommendation.models import SeedUniverseEntry

DEFAULT_KIS_RAW_DIR = Path("tools/kis_stocks_info/raw")


class SeedUniverseAssetScope(StrEnum):
    ALL = "all"
    STOCK = "stock"
    ETF = "etf"


@dataclass(frozen=True, slots=True)
class KisStocksInfoFiles:
    kospi_path: Path
    kosdaq_path: Path
    konex_path: Path
    sector_path: Path


@dataclass(frozen=True, slots=True)
class KisMasterRecord:
    symbol: str
    name: str
    market: str
    security_group_code: str | None = None
    sector_large_code: str | None = None
    sector_medium_code: str | None = None
    sector_small_code: str | None = None
    etp_product_code: str | None = None
    trading_halt: bool = False
    cleanup_sale: bool = False
    management_issue: bool = False


@dataclass(frozen=True, slots=True)
class SeedUniverseDiff:
    added_symbols: tuple[str, ...]
    removed_symbols: tuple[str, ...]
    changed_symbols: tuple[str, ...]

    @property
    def has_changes(self) -> bool:
        return bool(
            self.added_symbols or self.removed_symbols or self.changed_symbols
        )


@dataclass(frozen=True, slots=True)
class _KisDownloadSource:
    url: str
    extracted_name: str


_KIS_DOWNLOAD_SOURCES = {
    "kospi": _KisDownloadSource(
        url="https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
        extracted_name="kospi_code.mst",
    ),
    "kosdaq": _KisDownloadSource(
        url="https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
        extracted_name="kosdaq_code.mst",
    ),
    "konex": _KisDownloadSource(
        url="https://new.real.download.dws.co.kr/common/master/konex_code.mst.zip",
        extracted_name="konex_code.mst",
    ),
    "sector": _KisDownloadSource(
        url="https://new.real.download.dws.co.kr/common/master/idxcode.mst.zip",
        extracted_name="idxcode.mst",
    ),
}

_KOSPI_TAIL_WIDTHS: tuple[int, ...] = (
    2,
    1,
    4,
    4,
    4,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    9,
    5,
    5,
    1,
    1,
    1,
    2,
    1,
    1,
    1,
    2,
    2,
    2,
    3,
    1,
    3,
    12,
    12,
    8,
    15,
    21,
    2,
    7,
    1,
    1,
    1,
    1,
    9,
    9,
    9,
    5,
    9,
    8,
    9,
    3,
    1,
    1,
    1,
)
_KOSDAQ_TAIL_WIDTHS: tuple[int, ...] = (
    2,
    1,
    4,
    4,
    4,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    9,
    5,
    5,
    1,
    1,
    1,
    2,
    1,
    1,
    1,
    2,
    2,
    2,
    3,
    1,
    3,
    12,
    12,
    8,
    15,
    21,
    2,
    7,
    1,
    1,
    1,
    1,
    9,
    9,
    9,
    5,
    9,
    8,
    9,
    3,
    1,
    1,
    1,
)
_KOSPI_TAIL_LENGTH = sum(_KOSPI_TAIL_WIDTHS) + 1
_KOSDAQ_TAIL_LENGTH = sum(_KOSDAQ_TAIL_WIDTHS)
_ETF_GROUP_CODES = frozenset({"EF", "FE"})
_STOCK_GROUP_CODES = frozenset({"ST", "FS"})
_ETF_PRODUCT_CODES = frozenset({"1", "2", "5"})
_TRUE_VALUES = frozenset({"Y", "1"})


def download_kis_stocks_info_files(
    raw_dir: Path = DEFAULT_KIS_RAW_DIR,
) -> KisStocksInfoFiles:
    raw_dir.mkdir(parents=True, exist_ok=True)
    resolved_paths: dict[str, Path] = {}
    for name, source in _KIS_DOWNLOAD_SOURCES.items():
        payload = _download_and_extract_zip_member(
            url=source.url,
            expected_name=source.extracted_name,
        )
        output_path = raw_dir / source.extracted_name
        output_path.write_bytes(payload)
        resolved_paths[name] = output_path
    return KisStocksInfoFiles(
        kospi_path=resolved_paths["kospi"],
        kosdaq_path=resolved_paths["kosdaq"],
        konex_path=resolved_paths["konex"],
        sector_path=resolved_paths["sector"],
    )


def load_kis_stocks_info_files(
    raw_dir: Path = DEFAULT_KIS_RAW_DIR,
) -> KisStocksInfoFiles:
    files = KisStocksInfoFiles(
        kospi_path=raw_dir / _KIS_DOWNLOAD_SOURCES["kospi"].extracted_name,
        kosdaq_path=raw_dir / _KIS_DOWNLOAD_SOURCES["kosdaq"].extracted_name,
        konex_path=raw_dir / _KIS_DOWNLOAD_SOURCES["konex"].extracted_name,
        sector_path=raw_dir / _KIS_DOWNLOAD_SOURCES["sector"].extracted_name,
    )
    missing = [
        path.as_posix()
        for path in (
            files.kospi_path,
            files.kosdaq_path,
            files.konex_path,
            files.sector_path,
        )
        if not path.is_file()
    ]
    if missing:
        joined = ", ".join(missing)
        raise FileNotFoundError(f"missing KIS raw metadata files: {joined}")
    return files


def build_seed_universe_from_kis_files(
    files: KisStocksInfoFiles,
    *,
    asset_scope: SeedUniverseAssetScope = SeedUniverseAssetScope.ALL,
) -> tuple[SeedUniverseEntry, ...]:
    sector_names = load_sector_name_map(files.sector_path)
    records = (
        *load_kospi_master_records(files.kospi_path),
        *load_kosdaq_master_records(files.kosdaq_path),
        *load_konex_master_records(files.konex_path),
    )
    entries_by_symbol: dict[str, SeedUniverseEntry] = {}
    for record in records:
        entry = _record_to_seed_entry(record, sector_names=sector_names)
        if entry is None:
            continue
        if not _matches_asset_scope(entry, asset_scope):
            continue
        if entry.symbol in entries_by_symbol:
            raise ValueError(f"duplicate symbol in KIS metadata: {entry.symbol}")
        entries_by_symbol[entry.symbol] = entry
    if not entries_by_symbol:
        raise ValueError(f"no supported seed universe entries for asset_scope={asset_scope}")
    return tuple(sorted(entries_by_symbol.values(), key=lambda entry: entry.symbol))


def render_seed_universe_csv(universe: Sequence[SeedUniverseEntry]) -> str:
    buffer = StringIO()
    csv_writer = writer(buffer, lineterminator="\n")
    csv_writer.writerow(
        (
            "symbol",
            "name",
            "asset_type",
            "sector",
            "is_etf",
            "is_inverse",
            "is_leveraged",
            "active",
        )
    )
    for entry in sorted(universe, key=lambda candidate: candidate.symbol):
        csv_writer.writerow(
            (
                entry.symbol,
                entry.name or "",
                entry.asset_type or "",
                entry.sector or "",
                "1" if entry.is_etf else "0",
                "1" if entry.is_inverse else "0",
                "1" if entry.is_leveraged else "0",
                "1" if entry.active else "0",
            )
        )
    return buffer.getvalue()


def write_seed_universe_csv(
    output_path: Path,
    universe: Sequence[SeedUniverseEntry],
) -> Path:
    write_text_atomically(output_path, render_seed_universe_csv(universe))
    return output_path


def diff_seed_universe(
    before: Sequence[SeedUniverseEntry],
    after: Sequence[SeedUniverseEntry],
) -> SeedUniverseDiff:
    before_by_symbol = {entry.symbol: entry for entry in before}
    after_by_symbol = {entry.symbol: entry for entry in after}
    before_symbols = set(before_by_symbol)
    after_symbols = set(after_by_symbol)
    added_symbols = tuple(sorted(after_symbols - before_symbols))
    removed_symbols = tuple(sorted(before_symbols - after_symbols))
    changed_symbols = tuple(
        sorted(
            symbol
            for symbol in before_symbols & after_symbols
            if before_by_symbol[symbol] != after_by_symbol[symbol]
        )
    )
    return SeedUniverseDiff(
        added_symbols=added_symbols,
        removed_symbols=removed_symbols,
        changed_symbols=changed_symbols,
    )


def summarize_seed_universe_diff(diff: SeedUniverseDiff) -> str:
    lines = [
        "seed universe diff summary",
        f"- added: {len(diff.added_symbols)}",
        f"- removed: {len(diff.removed_symbols)}",
        f"- changed: {len(diff.changed_symbols)}",
    ]
    if diff.added_symbols:
        lines.append(f"- added_symbols: {', '.join(diff.added_symbols[:10])}")
    if diff.removed_symbols:
        lines.append(f"- removed_symbols: {', '.join(diff.removed_symbols[:10])}")
    if diff.changed_symbols:
        lines.append(f"- changed_symbols: {', '.join(diff.changed_symbols[:10])}")
    return "\n".join(lines)


def load_sector_name_map(path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for raw_line in _read_cp949_lines(path):
        code = raw_line[1:5].strip()
        name = raw_line[5:45].strip()
        if not code or not name:
            continue
        mapping[code] = name
    if not mapping:
        raise ValueError(f"sector master file is empty: {path}")
    return mapping


def load_kospi_master_records(path: Path) -> tuple[KisMasterRecord, ...]:
    return _load_kospi_like_records(
        path=path,
        market="KOSPI",
        tail_widths=_KOSPI_TAIL_WIDTHS,
        tail_length=_KOSPI_TAIL_LENGTH,
        field_indexes={
            "security_group_code": 0,
            "sector_large_code": 2,
            "sector_medium_code": 3,
            "sector_small_code": 4,
            "etp_product_code": 12,
            "trading_halt": 34,
            "cleanup_sale": 35,
            "management_issue": 36,
        },
    )


def load_kosdaq_master_records(path: Path) -> tuple[KisMasterRecord, ...]:
    return _load_kospi_like_records(
        path=path,
        market="KOSDAQ",
        tail_widths=_KOSDAQ_TAIL_WIDTHS,
        tail_length=_KOSDAQ_TAIL_LENGTH,
        field_indexes={
            "security_group_code": 0,
            "sector_large_code": 2,
            "sector_medium_code": 3,
            "sector_small_code": 4,
            "etp_product_code": 8,
            "trading_halt": 29,
            "cleanup_sale": 30,
            "management_issue": 31,
        },
    )


def load_konex_master_records(path: Path) -> tuple[KisMasterRecord, ...]:
    records: list[KisMasterRecord] = []
    for raw_line in _read_cp949_lines(path):
        if not raw_line.strip():
            continue
        symbol = raw_line[0:9].strip()
        name = raw_line[21:-184].strip()
        if not symbol or not name:
            continue
        records.append(
            KisMasterRecord(
                symbol=symbol,
                name=name,
                market="KONEX",
                security_group_code=_optional_fixed_field(raw_line[-184:-182]),
                trading_halt=_is_true_flag(raw_line[-163:-162]),
                cleanup_sale=_is_true_flag(raw_line[-162:-161]),
                management_issue=_is_true_flag(raw_line[-161:-160]),
            )
        )
    if not records:
        raise ValueError(f"konex master file is empty: {path}")
    return tuple(records)


def _load_kospi_like_records(
    *,
    path: Path,
    market: str,
    tail_widths: Sequence[int],
    tail_length: int,
    field_indexes: dict[str, int],
) -> tuple[KisMasterRecord, ...]:
    records: list[KisMasterRecord] = []
    for raw_line in _read_cp949_lines(path):
        if not raw_line.strip():
            continue
        front = raw_line[:-tail_length]
        tail = raw_line[-tail_length:]
        symbol = front[0:9].strip()
        name = front[21:].strip()
        if not symbol or not name:
            continue
        tail_fields = _split_fixed_width_fields(tail, tail_widths)
        records.append(
            KisMasterRecord(
                symbol=symbol,
                name=name,
                market=market,
                security_group_code=_optional_fixed_field(
                    tail_fields[field_indexes["security_group_code"]]
                ),
                sector_large_code=_optional_fixed_field(
                    tail_fields[field_indexes["sector_large_code"]]
                ),
                sector_medium_code=_optional_fixed_field(
                    tail_fields[field_indexes["sector_medium_code"]]
                ),
                sector_small_code=_optional_fixed_field(
                    tail_fields[field_indexes["sector_small_code"]]
                ),
                etp_product_code=_optional_fixed_field(
                    tail_fields[field_indexes["etp_product_code"]]
                ),
                trading_halt=_is_true_flag(
                    tail_fields[field_indexes["trading_halt"]]
                ),
                cleanup_sale=_is_true_flag(
                    tail_fields[field_indexes["cleanup_sale"]]
                ),
                management_issue=_is_true_flag(
                    tail_fields[field_indexes["management_issue"]]
                ),
            )
        )
    if not records:
        raise ValueError(f"{market.lower()} master file is empty: {path}")
    return tuple(records)


def _record_to_seed_entry(
    record: KisMasterRecord,
    *,
    sector_names: dict[str, str],
) -> SeedUniverseEntry | None:
    asset_type = _resolve_asset_type(record)
    if asset_type is None:
        return None
    is_etf = asset_type == "ETF"
    return SeedUniverseEntry(
        symbol=record.symbol,
        name=record.name,
        asset_type=asset_type,
        sector=_resolve_sector_name(record, sector_names, is_etf=is_etf),
        is_etf=is_etf,
        is_inverse=_is_inverse_name(record.name),
        is_leveraged=_is_leveraged_name(record.name),
        active=not (
            record.trading_halt or record.cleanup_sale or record.management_issue
        ),
    )


def _resolve_asset_type(record: KisMasterRecord) -> str | None:
    security_group_code = (record.security_group_code or "").strip().upper()
    etp_product_code = (record.etp_product_code or "").strip()
    if record.market == "KONEX":
        return "Stock"
    if security_group_code in _ETF_GROUP_CODES or etp_product_code in _ETF_PRODUCT_CODES:
        return "ETF"
    if security_group_code in _STOCK_GROUP_CODES:
        return "Stock"
    return None


def _resolve_sector_name(
    record: KisMasterRecord,
    sector_names: dict[str, str],
    *,
    is_etf: bool,
) -> str | None:
    for code in (
        _normalize_sector_code(record.sector_medium_code),
        _normalize_sector_code(record.sector_large_code),
        _normalize_sector_code(record.sector_small_code),
    ):
        if code is None:
            continue
        name = sector_names.get(code)
        if name is not None:
            return name
    if is_etf:
        return "ETF"
    if record.market == "KONEX":
        return "KONEX"
    return None


def _normalize_sector_code(value: str | None) -> str | None:
    if value is None:
        return None
    code = value.strip()
    if not code or set(code) == {"0"}:
        return None
    return code


def _matches_asset_scope(
    entry: SeedUniverseEntry,
    asset_scope: SeedUniverseAssetScope,
) -> bool:
    if asset_scope is SeedUniverseAssetScope.ALL:
        return True
    if asset_scope is SeedUniverseAssetScope.STOCK:
        return not entry.is_etf
    return entry.is_etf


def _split_fixed_width_fields(text: str, widths: Sequence[int]) -> tuple[str, ...]:
    fields: list[str] = []
    offset = 0
    for width in widths:
        next_offset = offset + width
        fields.append(text[offset:next_offset])
        offset = next_offset
    return tuple(fields)


def _optional_fixed_field(value: str) -> str | None:
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def _is_true_flag(value: str) -> bool:
    return value.strip().upper() in _TRUE_VALUES


def _is_inverse_name(name: str) -> bool:
    normalized = name.upper()
    return "인버스" in name or "곱버스" in name or "INVERSE" in normalized


def _is_leveraged_name(name: str) -> bool:
    normalized = name.upper()
    return (
        "레버리지" in name
        or "2X" in normalized
        or "곱버스" in name
        or "LEVERAGE" in normalized
    )


def _read_cp949_lines(path: Path) -> tuple[str, ...]:
    with path.open("r", encoding="cp949") as handle:
        return tuple(line.rstrip("\r\n") for line in handle)


def _download_and_extract_zip_member(*, url: str, expected_name: str) -> bytes:
    context = ssl._create_unverified_context()
    with urllib.request.urlopen(url, context=context) as response:
        archive_bytes = response.read()
    with ZipFile(BytesIO(archive_bytes)) as archive:
        member_name = _resolve_zip_member_name(archive, expected_name)
        with archive.open(member_name) as handle:
            return handle.read()


def _resolve_zip_member_name(archive: ZipFile, expected_name: str) -> str:
    names = archive.namelist()
    if expected_name in names:
        return expected_name
    for name in names:
        if Path(name).name == expected_name:
            return name
    raise ValueError(f"zip archive does not contain expected member: {expected_name}")
