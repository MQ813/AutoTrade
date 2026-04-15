from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from autotrade.data.models import UniverseMember
from autotrade.data.validation import normalize_symbols


@dataclass(frozen=True, slots=True)
class StaticUniverseSource:
    target_symbols: Sequence[str]

    def load_universe(self) -> tuple[UniverseMember, ...]:
        return tuple(
            UniverseMember(symbol=symbol)
            for symbol in normalize_symbols(self.target_symbols)
        )
