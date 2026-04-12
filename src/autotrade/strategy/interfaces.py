from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol
from typing import runtime_checkable

from autotrade.common import Signal
from autotrade.data import Bar


@runtime_checkable
class Strategy(Protocol):
    """Deterministic, read-only signal generation contract."""

    def generate_signal(self, bars: Sequence[Bar]) -> Signal: ...
