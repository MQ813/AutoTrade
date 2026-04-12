from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from autotrade.common import Signal
from autotrade.common import SignalAction
from autotrade.data import Bar
from autotrade.data import Timeframe
from autotrade.data import validate_bar_series


@dataclass(frozen=True, slots=True)
class _TrendFollowingConfig:
    strategy_name: str
    timeframe: Timeframe
    fast_window: int
    slow_window: int


class DailyTrendFollowingStrategy:
    _config = _TrendFollowingConfig(
        strategy_name="daily_trend_following",
        timeframe=Timeframe.DAY,
        fast_window=5,
        slow_window=20,
    )

    def generate_signal(self, bars: Sequence[Bar]) -> Signal:
        return _generate_trend_signal(self._config, bars)


class ThirtyMinuteTrendStrategy:
    _config = _TrendFollowingConfig(
        strategy_name="30m_low_frequency_trend",
        timeframe=Timeframe.MINUTE_30,
        fast_window=8,
        slow_window=24,
    )

    def generate_signal(self, bars: Sequence[Bar]) -> Signal:
        return _generate_trend_signal(self._config, bars)


def _generate_trend_signal(
    config: _TrendFollowingConfig,
    bars: Sequence[Bar],
) -> Signal:
    series = _validate_bars(bars, config.timeframe)
    latest_bar = series[-1]

    if len(series) < config.slow_window:
        return _build_signal(
            config=config,
            bar=latest_bar,
            action=SignalAction.HOLD,
            fast_sma=None,
            slow_sma=None,
            trigger="insufficient_history",
            extra=f"required_bars={config.slow_window} actual_bars={len(series)}",
        )

    closes = tuple(bar.close for bar in series)
    fast_sma = _sma(closes[-config.fast_window :])
    slow_sma = _sma(closes[-config.slow_window :])
    latest_close = closes[-1]

    if fast_sma > slow_sma and latest_close >= fast_sma:
        return _build_signal(
            config=config,
            bar=latest_bar,
            action=SignalAction.BUY,
            fast_sma=fast_sma,
            slow_sma=slow_sma,
            trigger="trend_alignment",
        )

    if fast_sma < slow_sma and latest_close <= fast_sma:
        return _build_signal(
            config=config,
            bar=latest_bar,
            action=SignalAction.SELL,
            fast_sma=fast_sma,
            slow_sma=slow_sma,
            trigger="trend_breakdown",
        )

    return _build_signal(
        config=config,
        bar=latest_bar,
        action=SignalAction.HOLD,
        fast_sma=fast_sma,
        slow_sma=slow_sma,
        trigger="no_trade",
    )


def _validate_bars(
    bars: Sequence[Bar],
    expected_timeframe: Timeframe,
) -> tuple[Bar, ...]:
    series = tuple(bars)
    if not series:
        raise ValueError("bars must not be empty")

    first = series[0]
    if any(bar.symbol != first.symbol for bar in series):
        raise ValueError("bars must contain one symbol")
    if any(bar.timeframe is not first.timeframe for bar in series):
        raise ValueError("bars must contain one timeframe")
    if first.timeframe is not expected_timeframe:
        raise ValueError(
            f"bars must use timeframe {expected_timeframe.value}",
        )

    validate_bar_series(series)

    return series


def _sma(values: Sequence[Decimal]) -> Decimal:
    return sum(values, start=Decimal("0")) / Decimal(len(values))


def _build_signal(
    config: _TrendFollowingConfig,
    bar: Bar,
    action: SignalAction,
    fast_sma: Decimal | None,
    slow_sma: Decimal | None,
    trigger: str,
    extra: str | None = None,
) -> Signal:
    reason_parts = [
        f"strategy={config.strategy_name}",
        f"action={action.value}",
        f"symbol={bar.symbol}",
        f"timeframe={bar.timeframe.value}",
        f"timestamp={bar.timestamp.isoformat()}",
        f"close={bar.close}",
        f"trigger={trigger}",
    ]
    if fast_sma is not None:
        reason_parts.append(f"fast_sma={fast_sma}")
    if slow_sma is not None:
        reason_parts.append(f"slow_sma={slow_sma}")
    if extra is not None:
        reason_parts.append(extra)

    return Signal(
        symbol=bar.symbol,
        action=action,
        generated_at=bar.timestamp,
        reason=" ".join(reason_parts),
    )
