from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import pytest

from autotrade.broker.smoke import SmokeReport
from autotrade.broker.smoke import SmokeStep
from autotrade.common import Holding
from autotrade.common import OrderCapacity
from autotrade.common import Quote
from autotrade.config import AppSettings
from autotrade.config import BrokerSettings
from autotrade.data import Bar
from autotrade.data import CsvBarStore
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.execution import FileExecutionStateStore
from autotrade.report import NotificationMessage
from autotrade.risk import RiskSettings
from autotrade.runtime.market_open import MarketOpenPreparationRuntime
from autotrade.scheduler import JobContext
from autotrade.scheduler import MarketSessionPhase


def test_market_open_preparation_runtime_writes_reports_and_sends_summary(
    tmp_path,
) -> None:
    generated_at = datetime(2026, 4, 14, 8, 0, tzinfo=KST)
    log_dir = tmp_path / "logs"
    settings = _settings(log_dir)
    notifier = RecordingNotifier()
    runtime = MarketOpenPreparationRuntime(
        settings=settings,
        strategy_kind="30m_low_frequency_trend",
        timeframe=Timeframe.MINUTE_30,
        bar_root=log_dir / "bars",
        broker_reader=StubBrokerReader(),
        notifier=notifier,
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        collect_strategy_bars=_write_strategy_bars,
        smoke_runner=lambda resolved_settings, timestamp: _successful_smoke_report(
            resolved_settings,
            timestamp=timestamp,
        ),
    )

    result = runtime.run(timestamp=generated_at)

    assert result.success is True
    assert result.smoke_report_path.exists()
    assert result.inspection_report_path.exists()
    assert result.data_statuses[0].ready is True
    assert result.strategy_previews[0].status == "buy_expected"
    assert result.strategy_previews[0].approved_quantity > 0
    inspection_text = result.inspection_report_path.read_text(encoding="utf-8")
    assert "item_label=전략 입력 데이터 최신성 확인" in inspection_text
    assert "item_label=오늘 가격 기준 전략 예상 확인" in inspection_text
    assert "item_status=passed item_label=전략 입력 데이터 최신성 확인" in (
        inspection_text
    )
    assert "item_status=passed item_label=오늘 가격 기준 전략 예상 확인" in (
        inspection_text
    )
    assert len(notifier.notifications) == 1
    assert notifier.notifications[0].subject == (
        "AutoTrade market open prep 2026-04-14 [OK]"
    )
    assert "strategy_previews:" in notifier.notifications[0].body


def test_market_open_preparation_job_raises_after_writing_reports_on_failed_checks(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "operations_20260413_153000_000000.log").write_text(
        (
            "timestamp=2026-04-13T15:30:00+09:00 level=error "
            "source=live_cycle message=boom\n"
        ),
        encoding="utf-8",
    )
    settings = _settings(
        log_dir,
        risk=RiskSettings(
            trading_halted=True,
            emergency_stop=True,
        ),
    )
    notifier = RecordingNotifier()
    runtime = MarketOpenPreparationRuntime(
        settings=settings,
        strategy_kind="30m_low_frequency_trend",
        timeframe=Timeframe.MINUTE_30,
        bar_root=log_dir / "bars",
        broker_reader=StubBrokerReader(),
        notifier=notifier,
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        collect_strategy_bars=_skip_strategy_bars,
        smoke_runner=lambda resolved_settings, timestamp: _successful_smoke_report(
            resolved_settings,
            timestamp=timestamp,
        ),
    )
    job = runtime.build_job()

    with pytest.raises(RuntimeError, match="failure_reasons="):
        job.handler(
            JobContext(
                phase=MarketSessionPhase.MARKET_OPEN,
                trading_day=datetime(2026, 4, 14, 0, 0, tzinfo=KST).date(),
                scheduled_at=datetime(2026, 4, 14, 8, 0, tzinfo=KST),
                triggered_at=datetime(2026, 4, 14, 8, 0, tzinfo=KST),
            )
        )

    inspection_paths = tuple(sorted(log_dir.glob("daily_inspection_*.txt")))
    assert len(inspection_paths) == 1
    inspection_text = inspection_paths[0].read_text(encoding="utf-8")
    assert "item_status=failed item_label=전일 로그 이상 여부 확인" in inspection_text
    assert "item_status=failed item_label=장운영 플래그 확인" in inspection_text
    assert "item_status=failed item_label=비상 정지 플래그 확인" in inspection_text
    assert "item_status=failed item_label=전략 입력 데이터 최신성 확인" in (
        inspection_text
    )
    assert "item_status=failed item_label=오늘 가격 기준 전략 예상 확인" in (
        inspection_text
    )
    assert len(notifier.notifications) == 1
    assert notifier.notifications[0].subject == (
        "AutoTrade market open prep 2026-04-14 [FAILED]"
    )


@dataclass(slots=True)
class RecordingNotifier:
    notifications: list[NotificationMessage]

    def __init__(self) -> None:
        self.notifications = []

    def send(self, notification: NotificationMessage) -> None:
        self.notifications.append(notification)


class StubBrokerReader:
    def get_quote(self, symbol: str) -> Quote:
        return Quote(
            symbol=symbol,
            price=Decimal("130"),
            as_of=datetime(2026, 4, 14, 8, 0, tzinfo=KST),
        )

    def get_holdings(self) -> tuple[Holding, ...]:
        return ()

    def get_order_capacity(
        self,
        symbol: str,
        order_price: Decimal,
    ) -> OrderCapacity:
        return OrderCapacity(
            symbol=symbol,
            order_price=order_price,
            max_orderable_quantity=999,
            cash_available=Decimal("86415"),
        )


def _write_strategy_bars(
    settings: AppSettings,
    *,
    bar_root,
    timeframe: Timeframe,
    generated_at: datetime,
) -> None:
    del settings
    del generated_at
    CsvBarStore(bar_root).store_bars(_build_trend_bars("069500", timeframe, count=24))


def _skip_strategy_bars(
    settings: AppSettings,
    *,
    bar_root,
    timeframe: Timeframe,
    generated_at: datetime,
) -> None:
    del settings
    del bar_root
    del timeframe
    del generated_at


def _build_trend_bars(
    symbol: str,
    timeframe: Timeframe,
    *,
    count: int,
) -> tuple[Bar, ...]:
    calendar = KrxRegularSessionCalendar()
    last_timestamp = datetime(2026, 4, 13, 15, 30, tzinfo=KST)
    timestamps: list[datetime] = []
    current = datetime(2026, 4, 8, 9, 0, tzinfo=KST)
    while current <= last_timestamp:
        timestamps.append(current)
        current = calendar.next_timestamp(current, timeframe)
    selected = timestamps[-count:]
    return tuple(
        Bar(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=timestamp,
            open=Decimal(str(100 + index)),
            high=Decimal(str(101 + index)),
            low=Decimal(str(99 + index)),
            close=Decimal(str(100 + index)),
            volume=100 + index,
        )
        for index, timestamp in enumerate(selected)
    )


def _successful_smoke_report(
    settings: AppSettings,
    *,
    timestamp: datetime,
) -> SmokeReport:
    target_symbol = settings.target_symbols[0]
    return SmokeReport(
        started_at=timestamp,
        finished_at=timestamp,
        target_symbol=target_symbol,
        steps=(
            SmokeStep(name="smoke", status="start", detail=target_symbol),
            SmokeStep(name="smoke", status="success"),
        ),
        quote=Quote(
            symbol=target_symbol,
            price=Decimal("12345"),
            as_of=timestamp,
        ),
        holdings=(
            Holding(
                symbol=target_symbol,
                quantity=1,
                average_price=Decimal("12000"),
                current_price=Decimal("12345"),
            ),
        ),
        order_capacity=OrderCapacity(
            symbol=target_symbol,
            order_price=Decimal("12345"),
            max_orderable_quantity=7,
            cash_available=Decimal("86415"),
        ),
        success=True,
    )


def _settings(
    log_dir,
    *,
    risk: RiskSettings | None = None,
) -> AppSettings:
    return AppSettings(
        broker=BrokerSettings(
            provider="koreainvestment",
            api_key="demo-key",
            api_secret="demo-secret",
            account="12345678-01",
            environment="paper",
        ),
        target_symbols=("069500",),
        log_dir=log_dir,
        risk=risk or RiskSettings(),
    )
