from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from decimal import Decimal

from autotrade.broker import PaperBroker
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.data import KST
from autotrade.data import Bar
from autotrade.data import Timeframe
from autotrade.execution import OrderExecutionEngine
from autotrade.execution import ReplaySession
from autotrade.execution import render_replay_log
from autotrade.execution import restore_replay_session_from_log
from autotrade.execution import write_replay_log
from autotrade.scheduler import MarketSessionPhase
from autotrade.scheduler import ScheduledJob
from autotrade.scheduler import SchedulerConfig


def test_replay_session_replays_historical_bars_across_market_phases(tmp_path) -> None:
    session = ReplaySession(
        PaperBroker(Decimal("1000")),
        scheduler_config=SchedulerConfig(intraday_interval=timedelta(hours=6)),
    )
    jobs, calls = _build_jobs(session.broker)

    runs = session.run(_bars(), jobs)
    log_path = write_replay_log(tmp_path, session.log_entries)
    rendered = render_replay_log(session.log_entries)

    assert [len(run.executed_jobs) for run in runs] == [2, 1, 1]
    assert calls == [
        "prepare:market_open",
        "buy:market_open",
        "heartbeat:intraday",
        "sell:market_close",
    ]
    assert session.snapshot().broker_snapshot.cash == Decimal("1050")
    assert "jobs=prepare,buy" in rendered
    assert "cash=1050" in rendered
    assert log_path.exists()


def test_replay_session_resumes_from_snapshot_without_replaying_completed_work() -> None:
    config = SchedulerConfig(intraday_interval=timedelta(hours=6))
    full_session = ReplaySession(PaperBroker(Decimal("1000")), scheduler_config=config)
    full_jobs, _ = _build_jobs(full_session.broker)
    full_session.run(_bars(), full_jobs)

    partial_session = ReplaySession(
        PaperBroker(Decimal("1000")),
        scheduler_config=config,
    )
    partial_jobs, _ = _build_jobs(partial_session.broker)
    partial_session.run(_bars()[:2], partial_jobs)
    restored = ReplaySession.from_snapshot(partial_session.snapshot())
    restored_jobs, restored_calls = _build_jobs(restored.broker)

    runs = restored.run(_bars()[2:], restored_jobs)

    assert [result.job_name for result in runs[0].executed_jobs] == ["sell"]
    assert restored_calls == ["sell:market_close"]
    assert restored.snapshot() == full_session.snapshot()


def test_replay_log_can_restore_session_state() -> None:
    session = ReplaySession(
        PaperBroker(Decimal("1000")),
        scheduler_config=SchedulerConfig(intraday_interval=timedelta(hours=6)),
    )
    jobs, _ = _build_jobs(session.broker)

    session.run(_bars(), jobs)
    restored = restore_replay_session_from_log(session.log_entries)

    assert restored.snapshot() == session.snapshot()


def _build_jobs(
    broker: PaperBroker,
) -> tuple[tuple[ScheduledJob, ...], list[str]]:
    engine = OrderExecutionEngine(broker)
    calls: list[str] = []

    def prepare(context) -> str:
        calls.append(f"prepare:{context.phase}")
        return "ready"

    def buy(context) -> str:
        calls.append(f"buy:{context.phase}")
        quote = broker.get_quote("069500")
        snapshot = engine.submit_order(
            OrderRequest(
                request_id="market-open-buy",
                symbol="069500",
                side=OrderSide.BUY,
                quantity=5,
                limit_price=quote.price,
                requested_at=context.triggered_at,
            )
        )
        synced = engine.sync_fills(snapshot.order.order_id)
        return f"buy_status={synced.order.status}"

    def heartbeat(context) -> str:
        calls.append(f"heartbeat:{context.phase}")
        holdings = broker.get_holdings()
        return f"holdings={len(holdings)}"

    def sell(context) -> str:
        calls.append(f"sell:{context.phase}")
        holdings = broker.get_holdings()
        if not holdings:
            return "no_position"
        quote = broker.get_quote("069500")
        snapshot = engine.submit_order(
            OrderRequest(
                request_id="market-close-sell",
                symbol="069500",
                side=OrderSide.SELL,
                quantity=holdings[0].quantity,
                limit_price=quote.price,
                requested_at=context.triggered_at,
            )
        )
        synced = engine.sync_fills(snapshot.order.order_id)
        return f"sell_status={synced.order.status}"

    return (
        (
            ScheduledJob(
                name="prepare",
                phase=MarketSessionPhase.MARKET_OPEN,
                handler=prepare,
            ),
            ScheduledJob(
                name="buy",
                phase=MarketSessionPhase.MARKET_OPEN,
                handler=buy,
            ),
            ScheduledJob(
                name="heartbeat",
                phase=MarketSessionPhase.INTRADAY,
                handler=heartbeat,
            ),
            ScheduledJob(
                name="sell",
                phase=MarketSessionPhase.MARKET_CLOSE,
                handler=sell,
            ),
        ),
        calls,
    )


def _bars() -> tuple[Bar, ...]:
    return (
        _bar("2026-04-13T09:00:00+09:00", close="100", low="99"),
        _bar("2026-04-13T15:00:00+09:00", close="105", low="100"),
        _bar("2026-04-13T15:30:00+09:00", close="110", low="110"),
    )


def _bar(
    timestamp: str,
    *,
    close: str,
    low: str,
) -> Bar:
    price = Decimal(close)
    return Bar(
        symbol="069500",
        timeframe=Timeframe.MINUTE_30,
        timestamp=_dt(timestamp),
        open=price,
        high=price,
        low=Decimal(low),
        close=price,
        volume=1,
    )


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(KST)
