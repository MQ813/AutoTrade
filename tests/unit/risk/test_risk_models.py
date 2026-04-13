from __future__ import annotations

from decimal import Decimal

import pytest

from autotrade.risk import RiskSettings


def test_risk_settings_accepts_defaults() -> None:
    settings = RiskSettings()

    assert settings.max_position_weight == Decimal("0.2")
    assert settings.max_concurrent_holdings == 3
    assert settings.max_loss is None
    assert settings.trading_halted is False


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"max_position_weight": Decimal("0")},
            "max_position_weight",
        ),
        (
            {"max_position_weight": Decimal("1.1")},
            "max_position_weight",
        ),
        (
            {"max_concurrent_holdings": 0},
            "max_concurrent_holdings",
        ),
        (
            {"max_loss": Decimal("-1")},
            "max_loss",
        ),
    ],
)
def test_risk_settings_rejects_invalid_values(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        RiskSettings(**kwargs)
