from __future__ import annotations

from decimal import Decimal

import pytest

from autotrade.risk import RiskSettings


def test_risk_settings_accepts_defaults() -> None:
    settings = RiskSettings()

    assert settings.max_position_weight == Decimal("0.2")
    assert settings.entry_max_position_weight_per_order == Decimal("0.05")
    assert settings.max_concurrent_holdings == 3
    assert settings.max_loss is None
    assert settings.max_drawdown is None
    assert settings.max_orders_per_day is None
    assert settings.trading_halted is False
    assert settings.emergency_stop is False
    assert settings.cancel_unfilled_orders_on_market_close is True


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
            {"entry_max_position_weight_per_order": Decimal("0")},
            "entry_max_position_weight_per_order",
        ),
        (
            {"entry_max_position_weight_per_order": Decimal("1.1")},
            "entry_max_position_weight_per_order",
        ),
        (
            {"max_concurrent_holdings": 0},
            "max_concurrent_holdings",
        ),
        (
            {"max_loss": Decimal("-1")},
            "max_loss",
        ),
        (
            {"max_drawdown": Decimal("0")},
            "max_drawdown",
        ),
        (
            {"max_drawdown": Decimal("1.1")},
            "max_drawdown",
        ),
        (
            {"max_orders_per_day": 0},
            "max_orders_per_day",
        ),
    ],
)
def test_risk_settings_rejects_invalid_values(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        RiskSettings(**kwargs)


def test_entry_max_position_weight_per_order_is_independent_of_total_cap() -> None:
    settings = RiskSettings(
        max_position_weight=Decimal("0.2"),
        entry_max_position_weight_per_order=Decimal("0.8"),
    )

    assert settings.entry_max_position_weight_per_order == Decimal("0.8")
