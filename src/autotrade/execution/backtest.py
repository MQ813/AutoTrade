from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from autotrade.common import SignalAction
from autotrade.data import Bar
from autotrade.data import Timeframe
from autotrade.data import validate_bar_series
from autotrade.portfolio import BacktestPortfolioState
from autotrade.portfolio import PortfolioSnapshot
from autotrade.portfolio import apply_buy_fill
from autotrade.portfolio import apply_sell_fill
from autotrade.portfolio import build_portfolio_snapshot
from autotrade.portfolio import create_backtest_portfolio
from autotrade.strategy import Strategy


def _require_positive_decimal(field_name: str, value: Decimal) -> None:
    if value <= Decimal("0"):
        raise ValueError(f"{field_name} must be positive")


def _require_rate(field_name: str, value: Decimal) -> None:
    if value < Decimal("0") or value >= Decimal("1"):
        raise ValueError(f"{field_name} must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class BacktestCostModel:
    commission_rate: Decimal = Decimal("0")
    tax_rate: Decimal = Decimal("0")
    slippage_rate: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        _require_rate("commission_rate", self.commission_rate)
        _require_rate("tax_rate", self.tax_rate)
        _require_rate("slippage_rate", self.slippage_rate)


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    initial_cash: Decimal
    cost_model: BacktestCostModel = field(default_factory=BacktestCostModel)
    in_sample_ratio: Decimal | None = Decimal("0.7")
    close_open_position_on_finish: bool = True

    def __post_init__(self) -> None:
        _require_positive_decimal("initial_cash", self.initial_cash)
        if self.in_sample_ratio is not None:
            _require_rate("in_sample_ratio", self.in_sample_ratio)
            if self.in_sample_ratio == Decimal("0"):
                raise ValueError("in_sample_ratio must be greater than 0")


@dataclass(frozen=True, slots=True)
class BacktestTrade:
    symbol: str
    entered_at: datetime
    exited_at: datetime
    quantity: int
    entry_price: Decimal
    exit_price: Decimal
    entry_fees: Decimal
    exit_fees: Decimal
    gross_pnl: Decimal
    net_pnl: Decimal
    holding_period_bars: int
    exit_reason: str


@dataclass(frozen=True, slots=True)
class BacktestResult:
    symbol: str
    timeframe: Timeframe
    initial_cash: Decimal
    cost_model: BacktestCostModel
    started_at: datetime
    finished_at: datetime
    split_timestamp: datetime | None
    trades: tuple[BacktestTrade, ...]
    snapshots: tuple[PortfolioSnapshot, ...]


@dataclass(slots=True)
class _OpenPosition:
    entry_fees: Decimal
    entered_at: datetime
    entry_index: int


class BacktestEngine:
    def run(
        self,
        strategy: Strategy,
        bars: Sequence[Bar],
        config: BacktestConfig,
    ) -> BacktestResult:
        series = _validate_bars(bars)
        split_timestamp = _resolve_split_timestamp(series, config.in_sample_ratio)
        symbol = series[0].symbol
        timeframe = series[0].timeframe
        cost_model = config.cost_model

        portfolio = create_backtest_portfolio(config.initial_cash)
        position: _OpenPosition | None = None
        trades: list[BacktestTrade] = []
        snapshots: list[PortfolioSnapshot] = []
        last_index = len(series) - 1

        for index, bar in enumerate(series):
            signal = strategy.generate_signal(series[: index + 1])
            _validate_signal(
                signal_symbol=signal.symbol, signal_time=signal.generated_at, bar=bar
            )

            if (
                signal.action is SignalAction.BUY
                and position is None
                and not _should_skip_final_bar_entry(index, last_index, config)
            ):
                quantity = _calculate_buy_quantity(
                    cash=portfolio.cash,
                    price=bar.close,
                    cost_model=cost_model,
                )
                if quantity > 0:
                    execution_price = _buy_execution_price(bar.close, cost_model)
                    notional = execution_price * Decimal(quantity)
                    entry_fees = notional * cost_model.commission_rate
                    portfolio = apply_buy_fill(
                        portfolio,
                        price=execution_price,
                        quantity=quantity,
                        fees=entry_fees,
                    )
                    position = _OpenPosition(
                        entry_fees=entry_fees,
                        entered_at=bar.timestamp,
                        entry_index=index,
                    )

            if signal.action is SignalAction.SELL and position is not None:
                portfolio, trade = _close_position(
                    position=position,
                    portfolio=portfolio,
                    price=bar.close,
                    timestamp=bar.timestamp,
                    bar_index=index,
                    cost_model=cost_model,
                    symbol=symbol,
                    exit_reason="signal",
                )
                trades.append(trade)
                position = None

            if (
                index == last_index
                and config.close_open_position_on_finish
                and position is not None
            ):
                portfolio, trade = _close_position(
                    position=position,
                    portfolio=portfolio,
                    price=bar.close,
                    timestamp=bar.timestamp,
                    bar_index=index,
                    cost_model=cost_model,
                    symbol=symbol,
                    exit_reason="end_of_data",
                )
                trades.append(trade)
                position = None

            snapshots.append(
                build_portfolio_snapshot(
                    portfolio,
                    symbol=symbol,
                    timestamp=bar.timestamp,
                    close_price=bar.close,
                )
            )

        return BacktestResult(
            symbol=symbol,
            timeframe=timeframe,
            initial_cash=config.initial_cash,
            cost_model=cost_model,
            started_at=series[0].timestamp,
            finished_at=series[-1].timestamp,
            split_timestamp=split_timestamp,
            trades=tuple(trades),
            snapshots=tuple(snapshots),
        )


def _validate_bars(bars: Sequence[Bar]) -> tuple[Bar, ...]:
    series = tuple(bars)
    if not series:
        raise ValueError("bars must not be empty")

    first = series[0]
    if any(bar.symbol != first.symbol for bar in series):
        raise ValueError("bars must contain one symbol")
    if any(bar.timeframe is not first.timeframe for bar in series):
        raise ValueError("bars must contain one timeframe")

    validate_bar_series(series)
    return series


def _validate_signal(
    *,
    signal_symbol: str,
    signal_time: datetime,
    bar: Bar,
) -> None:
    if signal_symbol != bar.symbol:
        raise ValueError("strategy signal symbol must match backtest symbol")
    if signal_time != bar.timestamp:
        raise ValueError("strategy signal timestamp must match current bar timestamp")


def _resolve_split_timestamp(
    bars: Sequence[Bar],
    in_sample_ratio: Decimal | None,
) -> datetime | None:
    if in_sample_ratio is None or len(bars) < 2:
        return None

    split_index = int(Decimal(len(bars)) * in_sample_ratio)
    split_index = max(1, min(len(bars) - 1, split_index))
    return bars[split_index].timestamp


def _should_skip_final_bar_entry(
    index: int,
    last_index: int,
    config: BacktestConfig,
) -> bool:
    return config.close_open_position_on_finish and index == last_index


def _calculate_buy_quantity(
    *,
    cash: Decimal,
    price: Decimal,
    cost_model: BacktestCostModel,
) -> int:
    execution_price = _buy_execution_price(price, cost_model)
    per_share_cost = execution_price * (Decimal("1") + cost_model.commission_rate)
    return int(cash / per_share_cost)


def _buy_execution_price(
    price: Decimal,
    cost_model: BacktestCostModel,
) -> Decimal:
    return price * (Decimal("1") + cost_model.slippage_rate)


def _sell_execution_price(
    price: Decimal,
    cost_model: BacktestCostModel,
) -> Decimal:
    return price * (Decimal("1") - cost_model.slippage_rate)


def _close_position(
    *,
    position: _OpenPosition,
    portfolio: BacktestPortfolioState,
    price: Decimal,
    timestamp: datetime,
    bar_index: int,
    cost_model: BacktestCostModel,
    symbol: str,
    exit_reason: str,
) -> tuple[BacktestPortfolioState, BacktestTrade]:
    execution_price = _sell_execution_price(price, cost_model)
    quantity = portfolio.position_quantity
    quantity_decimal = Decimal(quantity)
    notional = execution_price * quantity_decimal
    exit_fees = notional * (cost_model.commission_rate + cost_model.tax_rate)
    gross_pnl = (execution_price - portfolio.average_price) * quantity_decimal
    updated_portfolio = apply_sell_fill(
        portfolio,
        price=execution_price,
        quantity=quantity,
        fees=exit_fees,
    )
    net_pnl = updated_portfolio.realized_pnl - portfolio.realized_pnl

    trade = BacktestTrade(
        symbol=symbol,
        entered_at=position.entered_at,
        exited_at=timestamp,
        quantity=quantity,
        entry_price=portfolio.average_price,
        exit_price=execution_price,
        entry_fees=position.entry_fees,
        exit_fees=exit_fees,
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        holding_period_bars=bar_index - position.entry_index,
        exit_reason=exit_reason,
    )
    return updated_portfolio, trade
