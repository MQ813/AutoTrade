from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path

from autotrade.common.persistence import move_corrupt_file
from autotrade.common.persistence import write_text_atomically
from autotrade.recommendation.models import ApprovedSymbolsRecord

RECOMMENDATION_DIR = "recommendations"


@dataclass(frozen=True, slots=True)
class ApprovedSymbolsArtifacts:
    archive_path: Path
    latest_path: Path


def write_approved_symbols_record(path: Path, record: ApprovedSymbolsRecord) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomically(
        path,
        json.dumps(
            serialize_approved_symbols_record(record),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
    )
    return path


def load_approved_symbols_record(path: Path) -> ApprovedSymbolsRecord | None:
    if not path.exists():
        return None
    return deserialize_approved_symbols_record(
        json.loads(path.read_text(encoding="utf-8"))
    )


def write_approved_symbols_bundle(
    log_dir: Path,
    record: ApprovedSymbolsRecord,
) -> ApprovedSymbolsArtifacts:
    output_dir = log_dir / RECOMMENDATION_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / (
        f"approved_symbols_{record.as_of.strftime('%Y%m%d')}"
        f"_{record.created_at.strftime('%H%M%S_%f')}.json"
    )
    latest_path = output_dir / "approved_symbols_latest.json"
    write_approved_symbols_record(archive_path, record)
    write_approved_symbols_record(latest_path, record)
    return ApprovedSymbolsArtifacts(
        archive_path=archive_path,
        latest_path=latest_path,
    )


def load_latest_approved_symbols(log_dir: Path) -> ApprovedSymbolsRecord | None:
    latest_path = log_dir / RECOMMENDATION_DIR / "approved_symbols_latest.json"
    if not latest_path.exists():
        return None
    try:
        return load_approved_symbols_record(latest_path)
    except (JSONDecodeError, ValueError):
        move_corrupt_file(latest_path)
        return None


def serialize_approved_symbols_record(
    record: ApprovedSymbolsRecord,
) -> dict[str, object]:
    return {
        "as_of": record.as_of.isoformat(),
        "created_at": record.created_at.isoformat(),
        "symbols": list(record.symbols),
        "source_report_path": record.source_report_path,
        "notes": record.notes,
    }


def deserialize_approved_symbols_record(payload: object) -> ApprovedSymbolsRecord:
    mapping = _require_mapping(payload, "approved symbols record")
    symbols = mapping.get("symbols")
    if not isinstance(symbols, list) or any(
        not isinstance(symbol, str) for symbol in symbols
    ):
        raise ValueError(
            "approved symbols record field symbols must be a list of strings"
        )
    source_report_path = mapping.get("source_report_path", mapping.get("source_report"))
    notes = mapping.get("notes")
    if source_report_path is not None and not isinstance(source_report_path, str):
        raise ValueError(
            "approved symbols record field source_report_path must be a string"
        )
    if notes is not None and not isinstance(notes, str):
        raise ValueError("approved symbols record field notes must be a string")
    as_of_value = mapping.get("as_of")
    created_at_value = mapping.get("created_at", mapping.get("approved_at"))
    created_at = datetime.fromisoformat(_require_text(created_at_value, "created_at"))
    return ApprovedSymbolsRecord(
        as_of=(
            date.fromisoformat(_require_text(as_of_value, "as_of"))
            if as_of_value is not None
            else created_at.date()
        ),
        created_at=created_at,
        symbols=tuple(symbols),
        source_report_path=source_report_path,
        notes=notes,
    )


def _require_mapping(payload: object, label: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be an object")
    if not all(isinstance(key, str) for key in payload):
        raise ValueError(f"{label} keys must be strings")
    return payload


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"approved symbols record field {field_name} must be text")
    return value


def write_approved_symbols(
    log_dir: Path,
    record: ApprovedSymbolsRecord,
) -> ApprovedSymbolsArtifacts:
    return write_approved_symbols_bundle(log_dir, record)
