from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from autotrade.execution import BacktestCostModel
from autotrade.execution import BacktestResult
from autotrade.execution import BacktestTrade
from autotrade.portfolio import PortfolioSnapshot


@dataclass(frozen=True, slots=True)
class BacktestPerformanceSummary:
    label: str
    starting_equity: Decimal
    final_equity: Decimal
    net_profit: Decimal
    total_return: Decimal
    cagr: Decimal | None
    max_drawdown: Decimal
    trade_count: int
    win_rate: Decimal | None
    profit_factor: Decimal | None
    average_holding_bars: Decimal | None
    total_fees: Decimal
    bar_count: int


@dataclass(frozen=True, slots=True)
class BacktestOverfitCheck:
    status: str
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BacktestReport:
    symbol: str
    timeframe: str
    initial_cash: Decimal
    cost_model: BacktestCostModel
    split_timestamp: str | None
    recent_start_timestamp: str | None
    combined: BacktestPerformanceSummary
    in_sample: BacktestPerformanceSummary | None
    out_of_sample: BacktestPerformanceSummary | None
    recent: BacktestPerformanceSummary | None
    overfit_check: BacktestOverfitCheck


def build_backtest_report(result: BacktestResult) -> BacktestReport:
    combined = _build_summary(
        label="combined",
        start_equity=result.initial_cash,
        snapshots=result.snapshots,
        trades=result.trades,
        start_timestamp=result.started_at,
        end_timestamp=result.finished_at,
    )

    in_sample: BacktestPerformanceSummary | None = None
    out_of_sample: BacktestPerformanceSummary | None = None
    split_timestamp = result.split_timestamp
    recent_start_timestamp: str | None = None
    recent = _build_recent_summary(result)
    if recent is not None:
        recent_start_timestamp = recent.start_timestamp.isoformat()

    if split_timestamp is not None:
        in_sample_snapshots = tuple(
            snapshot
            for snapshot in result.snapshots
            if snapshot.timestamp < split_timestamp
        )
        out_of_sample_snapshots = tuple(
            snapshot
            for snapshot in result.snapshots
            if snapshot.timestamp >= split_timestamp
        )
        in_sample_trades = tuple(
            trade for trade in result.trades if trade.exited_at < split_timestamp
        )
        out_of_sample_trades = tuple(
            trade for trade in result.trades if trade.exited_at >= split_timestamp
        )

        in_sample_end_equity = (
            result.initial_cash
            if not in_sample_snapshots
            else in_sample_snapshots[-1].total_equity
        )
        in_sample_end_timestamp = (
            split_timestamp
            if not in_sample_snapshots
            else in_sample_snapshots[-1].timestamp
        )

        in_sample = _build_summary(
            label="in_sample",
            start_equity=result.initial_cash,
            snapshots=in_sample_snapshots,
            trades=in_sample_trades,
            start_timestamp=result.started_at,
            end_timestamp=in_sample_end_timestamp,
        )
        out_of_sample = _build_summary(
            label="out_of_sample",
            start_equity=in_sample_end_equity,
            snapshots=out_of_sample_snapshots,
            trades=out_of_sample_trades,
            start_timestamp=split_timestamp,
            end_timestamp=result.finished_at,
        )

    overfit_check = _build_overfit_check(
        in_sample=in_sample,
        out_of_sample=out_of_sample,
        recent=recent.summary if recent is not None else None,
    )

    return BacktestReport(
        symbol=result.symbol,
        timeframe=result.timeframe.value,
        initial_cash=result.initial_cash,
        cost_model=result.cost_model,
        split_timestamp=None
        if split_timestamp is None
        else split_timestamp.isoformat(),
        recent_start_timestamp=recent_start_timestamp,
        combined=combined,
        in_sample=in_sample,
        out_of_sample=out_of_sample,
        recent=None if recent is None else recent.summary,
        overfit_check=overfit_check,
    )


def render_backtest_report(report: BacktestReport) -> str:
    lines = [
        f"symbol={report.symbol}",
        f"timeframe={report.timeframe}",
        f"initial_cash={report.initial_cash}",
        f"commission_rate={report.cost_model.commission_rate}",
        f"tax_rate={report.cost_model.tax_rate}",
        f"slippage_rate={report.cost_model.slippage_rate}",
    ]
    if report.split_timestamp is not None:
        lines.append(f"split_timestamp={report.split_timestamp}")
    if report.recent_start_timestamp is not None:
        lines.append(f"recent_start_timestamp={report.recent_start_timestamp}")

    lines.extend(_render_summary(report.combined))
    if report.in_sample is not None:
        lines.extend(_render_summary(report.in_sample))
    if report.out_of_sample is not None:
        lines.extend(_render_summary(report.out_of_sample))
    if report.recent is not None:
        lines.extend(_render_summary(report.recent))
    lines.append(f"overfit_check_status={report.overfit_check.status}")
    for reason in report.overfit_check.reasons:
        lines.append(f"overfit_check_reason={reason}")

    return "\n".join(lines) + "\n"


@dataclass(frozen=True, slots=True)
class _RecentSummaryWindow:
    start_timestamp: datetime
    summary: BacktestPerformanceSummary


def _build_summary(
    *,
    label: str,
    start_equity: Decimal,
    snapshots: tuple[PortfolioSnapshot, ...],
    trades: tuple[BacktestTrade, ...],
    start_timestamp,
    end_timestamp,
    ) -> BacktestPerformanceSummary:
    final_equity = start_equity if not snapshots else snapshots[-1].total_equity
    net_profit = final_equity - start_equity
    total_return = net_profit / start_equity
    total_fees = sum(
        (trade.entry_fees + trade.exit_fees for trade in trades),
        start=Decimal("0"),
    )
    cagr = _calculate_cagr(
        start_equity=start_equity,
        final_equity=final_equity,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )

    return BacktestPerformanceSummary(
        label=label,
        starting_equity=start_equity,
        final_equity=final_equity,
        net_profit=net_profit,
        total_return=total_return,
        cagr=cagr,
        max_drawdown=_calculate_max_drawdown(start_equity, snapshots),
        trade_count=len(trades),
        win_rate=_calculate_win_rate(trades),
        profit_factor=_calculate_profit_factor(trades),
        average_holding_bars=_calculate_average_holding_bars(trades),
        total_fees=total_fees,
        bar_count=len(snapshots),
    )


def _build_recent_summary(result: BacktestResult) -> _RecentSummaryWindow | None:
    if not result.snapshots:
        return None

    recent_bar_count = max(1, math.ceil(len(result.snapshots) * 0.2))
    start_index = len(result.snapshots) - recent_bar_count
    recent_snapshots = result.snapshots[start_index:]
    recent_start_timestamp = recent_snapshots[0].timestamp
    recent_start_equity = (
        result.initial_cash
        if start_index == 0
        else result.snapshots[start_index - 1].total_equity
    )
    recent_trades = tuple(
        trade for trade in result.trades if trade.exited_at >= recent_start_timestamp
    )

    return _RecentSummaryWindow(
        start_timestamp=recent_start_timestamp,
        summary=_build_summary(
            label="recent",
            start_equity=recent_start_equity,
            snapshots=recent_snapshots,
            trades=recent_trades,
            start_timestamp=recent_start_timestamp,
            end_timestamp=result.finished_at,
        ),
    )


def _build_overfit_check(
    *,
    in_sample: BacktestPerformanceSummary | None,
    out_of_sample: BacktestPerformanceSummary | None,
    recent: BacktestPerformanceSummary | None,
) -> BacktestOverfitCheck:
    if in_sample is None or out_of_sample is None:
        return BacktestOverfitCheck(
            status="unavailable",
            reasons=("out_of_sample_check_unavailable",),
        )

    reasons: list[str] = []
    if (
        in_sample.total_return > Decimal("0")
        and out_of_sample.total_return <= Decimal("0")
    ):
        reasons.append("out_of_sample_return_reversal")
    elif (
        in_sample.total_return > Decimal("0")
        and out_of_sample.total_return < in_sample.total_return * Decimal("0.5")
    ):
        reasons.append("out_of_sample_return_degradation")

    if recent is not None and recent.total_return < Decimal("0"):
        reasons.append("recent_period_negative_return")

    return BacktestOverfitCheck(
        status="warning" if reasons else "pass",
        reasons=tuple(reasons),
    )


def _calculate_cagr(
    *,
    start_equity: Decimal,
    final_equity: Decimal,
    start_timestamp,
    end_timestamp,
) -> Decimal | None:
    duration_seconds = (end_timestamp - start_timestamp).total_seconds()
    if duration_seconds <= 0:
        return None

    years = duration_seconds / (60 * 60 * 24 * 365.25)
    annualized_return = math.pow(float(final_equity / start_equity), 1 / years) - 1
    return Decimal(str(annualized_return))


def _calculate_max_drawdown(
    start_equity: Decimal,
    snapshots: tuple[PortfolioSnapshot, ...],
) -> Decimal:
    peak = start_equity
    max_drawdown = Decimal("0")

    for equity in (start_equity, *(snapshot.total_equity for snapshot in snapshots)):
        if equity > peak:
            peak = equity
        drawdown = (peak - equity) / peak
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    return max_drawdown


def _calculate_win_rate(trades: tuple[BacktestTrade, ...]) -> Decimal | None:
    if not trades:
        return None

    winners = sum(1 for trade in trades if trade.net_pnl > Decimal("0"))
    return Decimal(winners) / Decimal(len(trades))


def _calculate_profit_factor(trades: tuple[BacktestTrade, ...]) -> Decimal | None:
    if not trades:
        return None

    gross_profit = sum(
        (trade.net_pnl for trade in trades if trade.net_pnl > Decimal("0")),
        start=Decimal("0"),
    )
    gross_loss = sum(
        (-trade.net_pnl for trade in trades if trade.net_pnl < Decimal("0")),
        start=Decimal("0"),
    )
    if gross_loss == Decimal("0"):
        return None if gross_profit == Decimal("0") else Decimal("Infinity")
    return gross_profit / gross_loss


def _calculate_average_holding_bars(
    trades: tuple[BacktestTrade, ...],
) -> Decimal | None:
    if not trades:
        return None

    total_bars = sum(trade.holding_period_bars for trade in trades)
    return Decimal(total_bars) / Decimal(len(trades))


def _render_summary(summary: BacktestPerformanceSummary) -> list[str]:
    return [
        f"section={summary.label}",
        f"bars={summary.bar_count}",
        f"starting_equity={summary.starting_equity}",
        f"final_equity={summary.final_equity}",
        f"net_profit={summary.net_profit}",
        f"total_return={_format_ratio(summary.total_return)}",
        f"cagr={_format_optional_ratio(summary.cagr)}",
        f"max_drawdown={_format_ratio(summary.max_drawdown)}",
        f"trade_count={summary.trade_count}",
        f"win_rate={_format_optional_ratio(summary.win_rate)}",
        f"profit_factor={_format_optional_decimal(summary.profit_factor)}",
        f"average_holding_bars={_format_optional_decimal(summary.average_holding_bars)}",
        f"total_fees={summary.total_fees}",
    ]


def _format_ratio(value: Decimal) -> str:
    return _format_optional_ratio(value)


def _format_optional_ratio(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    if value.is_infinite():
        return "inf"
    return f"{(value * Decimal('100')).quantize(Decimal('0.01'))}%"


def _format_optional_decimal(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    if value.is_infinite():
        return "inf"
    return str(value.normalize())
