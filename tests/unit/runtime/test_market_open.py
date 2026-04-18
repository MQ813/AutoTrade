from __future__ import annotations

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
from autotrade.data import KST
from autotrade.data import Timeframe
from autotrade.risk import RiskSettings
from autotrade.runtime.market_open import MarketOpenPreparationRuntime
from autotrade.scheduler import JobContext
from autotrade.scheduler import MarketSessionPhase


def test_market_open_preparation_runtime_writes_reports_when_checks_pass(
    tmp_path,
) -> None:
    generated_at = datetime(2026, 4, 14, 9, 0, tzinfo=KST)
    settings = _settings(tmp_path / "logs")
    runtime = MarketOpenPreparationRuntime(
        settings=settings,
        strategy_kind="thirty_minute_trend",
        timeframe=Timeframe.MINUTE_30,
        smoke_runner=lambda resolved_settings, timestamp: _successful_smoke_report(
            resolved_settings,
            timestamp=timestamp,
        ),
    )

    result = runtime.run(timestamp=generated_at)

    assert result.success is True
    assert result.smoke_report_path.exists()
    assert result.inspection_report_path.exists()
    inspection_text = result.inspection_report_path.read_text(encoding="utf-8")
    assert "item_label=API 인증 상태 확인" in inspection_text
    assert "item_status=passed" in inspection_text
    assert "item_label=대상 종목 목록 확인" in inspection_text
    assert "strategy=thirty_minute_trend timeframe=30m targets=069500" in (
        inspection_text
    )
    assert "item_label=전일 로그 이상 여부 확인" in inspection_text
    assert "error_entries=0" in inspection_text


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
    runtime = MarketOpenPreparationRuntime(
        settings=settings,
        strategy_kind="thirty_minute_trend",
        timeframe=Timeframe.MINUTE_30,
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
                scheduled_at=datetime(2026, 4, 14, 9, 0, tzinfo=KST),
                triggered_at=datetime(2026, 4, 14, 9, 0, tzinfo=KST),
            )
        )

    inspection_paths = tuple(sorted(log_dir.glob("daily_inspection_*.txt")))
    assert len(inspection_paths) == 1
    inspection_text = inspection_paths[0].read_text(encoding="utf-8")
    assert "item_status=failed item_label=전일 로그 이상 여부 확인" in inspection_text
    assert "item_status=failed item_label=장운영 플래그 확인" in inspection_text
    assert "item_status=failed item_label=비상 정지 플래그 확인" in inspection_text


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
