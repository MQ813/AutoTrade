from __future__ import annotations

from datetime import date
from datetime import datetime
from pathlib import Path

import pytest

from autotrade.data import KST
from autotrade.recommendation import ApprovedSymbolsRecord
from autotrade.recommendation import load_latest_approved_symbols
from autotrade.recommendation import load_approved_symbols_record
from autotrade.recommendation import write_approved_symbols_bundle
from autotrade.recommendation import write_approved_symbols_record


def test_approved_symbols_record_round_trips_to_json(tmp_path: Path) -> None:
    path = tmp_path / "approved_symbols.json"
    record = ApprovedSymbolsRecord(
        created_at=datetime(2026, 7, 20, 19, 0, tzinfo=KST),
        symbols=(" 069500 ", "005930"),
        source_report_path="weekly_candidates_20260720.json",
        notes="manual review complete",
        as_of=date(2026, 7, 20),
    )

    write_approved_symbols_record(path, record)
    loaded = load_approved_symbols_record(path)

    assert loaded == ApprovedSymbolsRecord(
        created_at=datetime(2026, 7, 20, 19, 0, tzinfo=KST),
        symbols=("069500", "005930"),
        source_report_path="weekly_candidates_20260720.json",
        notes="manual review complete",
        as_of=date(2026, 7, 20),
    )


def test_load_approved_symbols_record_rejects_invalid_payload(
    tmp_path: Path,
) -> None:
    path = tmp_path / "approved_symbols.json"
    path.write_text(
        '{"as_of":"2026-07-20","created_at":"bad","symbols":["069500"]}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid isoformat string"):
        load_approved_symbols_record(path)


def test_write_approved_symbols_bundle_writes_latest_alias(tmp_path: Path) -> None:
    record = ApprovedSymbolsRecord(
        created_at=datetime(2026, 7, 20, 19, 0, tzinfo=KST),
        symbols=("069500", "005930", "000660"),
        as_of=date(2026, 7, 20),
    )

    artifacts = write_approved_symbols_bundle(tmp_path, record)
    latest = load_latest_approved_symbols(tmp_path)

    assert artifacts.archive_path.exists()
    assert artifacts.latest_path.exists()
    assert latest == record
