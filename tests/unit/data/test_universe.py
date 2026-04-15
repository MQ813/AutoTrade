from __future__ import annotations

import pytest

from autotrade.data import StaticUniverseSource
from autotrade.data import UniverseMember


def test_static_universe_source_loads_configured_symbols() -> None:
    source = StaticUniverseSource(target_symbols=(" 069500 ", "357870"))

    assert source.load_universe() == (
        UniverseMember(symbol="069500"),
        UniverseMember(symbol="357870"),
    )


def test_static_universe_source_rejects_duplicate_symbols() -> None:
    source = StaticUniverseSource(target_symbols=("069500", " 069500 "))

    with pytest.raises(ValueError, match="duplicate symbol"):
        source.load_universe()
