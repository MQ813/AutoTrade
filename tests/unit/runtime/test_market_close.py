from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path

from autotrade.broker import PaperBroker
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.config import AppSettings
from autotrade.config import BrokerSettings
from autotrade.data import Bar
from autotrade.data import KST
from autotrade.data import Timeframe
from autotrade.execution import FileExecutionStateStore
from autotrade.execution import OrderExecutionEngine
from autotrade.report import DailyInspectionItem
from autotrade.report import InspectionStatus
from autotrade.report import InspectionWindow
from autotrade.report import NotificationMessage
from autotrade.report import append_job_run_result
from autotrade.report import build_daily_inspection_report
from autotrade.report import load_daily_inspection_report
from autotrade.report import load_daily_run_report
from autotrade.report import write_daily_inspection_report
from autotrade.runtime.market_close import MarketCloseRuntime
from autotrade.risk import RiskSettings
from autotrade.scheduler import JobRunResult
from autotrade.scheduler import MarketSessionPhase


def test_market_close_runtime_writes_daily_outputs_and_updates_inspection(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    trading_day = date(2026, 4, 10)
    generated_at = datetime(2026, 4, 10, 15, 30, tzinfo=KST)
    settings = _settings(log_dir)
    broker = PaperBroker(initial_cash=Decimal("1000000"))
    broker.advance_bar(
        Bar(
            symbol="069500",
            timeframe=Timeframe.MINUTE_30,
            timestamp=datetime(2026, 4, 10, 10, 0, tzinfo=KST),
            open=Decimal("10000"),
            high=Decimal("10000"),
            low=Decimal("10000"),
            close=Decimal("10000"),
            volume=100,
        )
    )
    state_store = FileExecutionStateStore(log_dir / "execution_state.json")
    engine = OrderExecutionEngine(broker, state_store=state_store)
    snapshot = engine.submit_order(
        OrderRequest(
            request_id="buy-1",
            symbol="069500",
            side=OrderSide.BUY,
            quantity=3,
            limit_price=Decimal("10000"),
            requested_at=datetime(2026, 4, 10, 10, 0, tzinfo=KST),
        )
    )
    engine.sync_fills(snapshot.order.order_id)
    append_job_run_result(
        log_dir,
        _job_result(
            job_name="market_open_prepare",
            phase=MarketSessionPhase.MARKET_OPEN,
            scheduled_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
            finished_at=datetime(2026, 4, 10, 9, 0, 5, tzinfo=KST),
            success=True,
            detail="prepared",
        ),
    )
    append_job_run_result(
        log_dir,
        _job_result(
            job_name="live_cycle",
            phase=MarketSessionPhase.INTRADAY,
            scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
            finished_at=datetime(2026, 4, 10, 9, 30, 4, tzinfo=KST),
            success=True,
            detail="submitted",
        ),
    )
    write_daily_inspection_report(
        log_dir,
        build_daily_inspection_report(
            trading_day,
            generated_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
            items=(
                DailyInspectionItem(
                    window=InspectionWindow.PRE_MARKET,
                    label="API 인증 상태 확인",
                    status=InspectionStatus.PASSED,
                ),
            ),
        ),
    )
    notifier = RecordingNotifier()
    runtime = MarketCloseRuntime(
        settings=settings,
        broker_reader=broker,
        notifier=notifier,
        state_store=state_store,
        clock=lambda: generated_at,
    )

    result = runtime.run(timestamp=generated_at, triggered_at=generated_at)
    daily_report = load_daily_run_report(log_dir, trading_day)
    inspection_report = load_daily_inspection_report(log_dir, trading_day)

    assert result.total_jobs == 3
    assert result.failed_jobs == 0
    assert result.order_snapshots == 1
    assert result.daily_fills == 1
    assert result.holdings == 1
    assert result.daily_run_report_path.exists()
    assert result.inspection_report_path.exists()
    assert result.next_day_preparation_path.exists()
    assert daily_report is not None
    assert daily_report.total_jobs == 3
    assert daily_report.failed_jobs == 0
    assert inspection_report is not None
    assert any(
        item.label == "손익 요약 리포트 생성" and item.status is InspectionStatus.PASSED
        for item in inspection_report.items
    )
    assert any(
        item.label == "다음 거래일 준비 상태 확인"
        and item.status is InspectionStatus.PASSED
        for item in inspection_report.items
    )
    assert any(
        item.label == "손실 상한 도달 여부 확인"
        and item.status is InspectionStatus.PASSED
        and "max_loss=not_configured" in (item.detail or "")
        for item in inspection_report.items
    )
    assert "next_trading_day=2026-04-13" in result.next_day_preparation_path.read_text(
        encoding="utf-8"
    )
    assert len(notifier.notifications) == 1
    assert "[OK]" in notifier.notifications[0].subject


def test_market_close_runtime_marks_loss_limit_passed_from_runtime_state(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    trading_day = date(2026, 4, 10)
    generated_at = datetime(2026, 4, 10, 15, 30, tzinfo=KST)
    _write_intraday_risk_state(
        log_dir,
        trading_day=trading_day,
        session_start_equity="1000",
        peak_equity="1010",
        latest_equity="960",
    )
    runtime = MarketCloseRuntime(
        settings=_settings(log_dir, risk=RiskSettings(max_loss=Decimal("50"))),
        broker_reader=PaperBroker(initial_cash=Decimal("1000000")),
        notifier=RecordingNotifier(),
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: generated_at,
    )

    runtime.run(timestamp=generated_at, triggered_at=generated_at)

    inspection_report = load_daily_inspection_report(log_dir, trading_day)
    assert inspection_report is not None
    assert any(
        item.label == "손실 상한 도달 여부 확인"
        and item.status is InspectionStatus.PASSED
        and "loss_amount=40" in (item.detail or "")
        and "latest_equity=960" in (item.detail or "")
        for item in inspection_report.items
    )


def test_market_close_runtime_marks_loss_limit_failed_when_reached(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    trading_day = date(2026, 4, 10)
    generated_at = datetime(2026, 4, 10, 15, 30, tzinfo=KST)
    _write_intraday_risk_state(
        log_dir,
        trading_day=trading_day,
        session_start_equity="1000",
        peak_equity="1000",
        latest_equity="950",
    )
    runtime = MarketCloseRuntime(
        settings=_settings(log_dir, risk=RiskSettings(max_loss=Decimal("50"))),
        broker_reader=PaperBroker(initial_cash=Decimal("1000000")),
        notifier=RecordingNotifier(),
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: generated_at,
    )

    runtime.run(timestamp=generated_at, triggered_at=generated_at)

    inspection_report = load_daily_inspection_report(log_dir, trading_day)
    assert inspection_report is not None
    assert any(
        item.label == "손실 상한 도달 여부 확인"
        and item.status is InspectionStatus.FAILED
        and "loss_amount=50" in (item.detail or "")
        for item in inspection_report.items
    )


def test_market_close_runtime_keeps_loss_limit_pending_for_legacy_state(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    trading_day = date(2026, 4, 10)
    generated_at = datetime(2026, 4, 10, 15, 30, tzinfo=KST)
    _write_intraday_risk_state(
        log_dir,
        trading_day=trading_day,
        session_start_equity="1000",
        peak_equity="1000",
    )
    runtime = MarketCloseRuntime(
        settings=_settings(log_dir, risk=RiskSettings(max_loss=Decimal("50"))),
        broker_reader=PaperBroker(initial_cash=Decimal("1000000")),
        notifier=RecordingNotifier(),
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: generated_at,
    )

    runtime.run(timestamp=generated_at, triggered_at=generated_at)

    inspection_report = load_daily_inspection_report(log_dir, trading_day)
    assert inspection_report is not None
    assert any(
        item.label == "손실 상한 도달 여부 확인"
        and item.status is InspectionStatus.PENDING
        and "state=incomplete" in (item.detail or "")
        and "missing_fields=latest_equity" in (item.detail or "")
        for item in inspection_report.items
    )


def test_market_close_runtime_degrades_to_failed_report_when_holdings_lookup_breaks(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    trading_day = date(2026, 4, 10)
    generated_at = datetime(2026, 4, 10, 15, 30, tzinfo=KST)
    settings = _settings(log_dir)
    state_store = FileExecutionStateStore(log_dir / "execution_state.json")
    append_job_run_result(
        log_dir,
        _job_result(
            job_name="live_cycle",
            phase=MarketSessionPhase.INTRADAY,
            scheduled_at=datetime(2026, 4, 10, 9, 30, tzinfo=KST),
            finished_at=datetime(2026, 4, 10, 9, 30, 4, tzinfo=KST),
            success=True,
            detail="submitted",
        ),
    )
    notifier = RecordingNotifier()
    runtime = MarketCloseRuntime(
        settings=settings,
        broker_reader=BrokenBrokerReader(),
        notifier=notifier,
        state_store=state_store,
        clock=lambda: generated_at,
    )

    result = runtime.run(timestamp=generated_at, triggered_at=generated_at)

    daily_report = load_daily_run_report(log_dir, trading_day)
    inspection_report = load_daily_inspection_report(log_dir, trading_day)

    assert result.total_jobs == 2
    assert result.failed_jobs == 1
    assert result.holdings == 0
    assert daily_report is not None
    assert daily_report.total_jobs == 2
    assert daily_report.failed_jobs == 1
    assert any(
        result.job_name == "market_close_cleanup"
        and result.error == "broker unavailable"
        for result in daily_report.job_results
    )
    assert inspection_report is not None
    assert any(
        item.label == "오류 로그 점검"
        and item.status is InspectionStatus.FAILED
        and "market_close_cleanup:broker unavailable" in (item.detail or "")
        for item in inspection_report.items
    )
    assert len(notifier.notifications) == 1
    assert "[FAILED]" in notifier.notifications[0].subject


def test_market_close_runtime_safe_stop_cleanup_records_runner_exception(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    trading_day = date(2026, 4, 10)
    generated_at = datetime(2026, 4, 10, 11, 0, tzinfo=KST)
    settings = _settings(log_dir)
    broker = PaperBroker(initial_cash=Decimal("1000000"))
    notifier = RecordingNotifier()
    runtime = MarketCloseRuntime(
        settings=settings,
        broker_reader=broker,
        notifier=notifier,
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: generated_at,
    )

    result = runtime.run_safe_stop_cleanup(
        timestamp=generated_at,
        reason="runner_exception",
        detail="scheduler loop crashed",
    )
    daily_report = load_daily_run_report(log_dir, trading_day)
    inspection_report = load_daily_inspection_report(log_dir, trading_day)

    assert result.total_jobs == 2
    assert result.failed_jobs == 1
    assert daily_report is not None
    assert any(
        report_result.job_name == "runner_safe_stop"
        and report_result.error == "runner_exception:scheduler loop crashed"
        for report_result in daily_report.job_results
    )
    assert inspection_report is not None
    assert any(
        item.label == "오류 로그 점검"
        and item.status is InspectionStatus.FAILED
        and "safe_stop_reason=runner_exception" in (item.detail or "")
        for item in inspection_report.items
    )
    assert (
        "safe stop 원인 확인: reason=runner_exception detail=scheduler loop crashed"
        in (result.next_day_preparation_path.read_text(encoding="utf-8"))
    )
    assert len(notifier.notifications) == 1
    assert "[FAILED]" in notifier.notifications[0].subject


def test_market_close_runtime_safe_stop_cleanup_is_best_effort_on_broker_failure(
    tmp_path,
) -> None:
    log_dir = tmp_path / "logs"
    trading_day = date(2026, 4, 10)
    generated_at = datetime(2026, 4, 10, 11, 0, tzinfo=KST)
    settings = _settings(log_dir)
    notifier = RecordingNotifier()
    runtime = MarketCloseRuntime(
        settings=settings,
        broker_reader=BrokenBrokerReader(),
        notifier=notifier,
        state_store=FileExecutionStateStore(log_dir / "execution_state.json"),
        clock=lambda: generated_at,
    )

    result = runtime.run_safe_stop_cleanup(
        timestamp=generated_at,
        reason="runner_exception",
        detail="scheduler loop crashed",
    )
    daily_report = load_daily_run_report(log_dir, trading_day)
    inspection_report = load_daily_inspection_report(log_dir, trading_day)

    assert result.total_jobs == 2
    assert result.failed_jobs == 2
    assert result.holdings == 0
    assert daily_report is not None
    assert any(
        report_result.job_name == "runner_safe_stop"
        and report_result.error == "runner_exception:scheduler loop crashed"
        for report_result in daily_report.job_results
    )
    assert any(
        report_result.job_name == "market_close_cleanup"
        and report_result.error == "broker unavailable"
        for report_result in daily_report.job_results
    )
    assert inspection_report is not None
    assert any(
        item.label == "오류 로그 점검"
        and item.status is InspectionStatus.FAILED
        and "market_close_cleanup:broker unavailable" in (item.detail or "")
        for item in inspection_report.items
    )
    assert len(notifier.notifications) == 1
    assert "[FAILED]" in notifier.notifications[0].subject


def _job_result(
    *,
    job_name: str,
    phase: MarketSessionPhase,
    scheduled_at: datetime,
    finished_at: datetime,
    success: bool,
    detail: str | None = None,
    error: str | None = None,
) -> JobRunResult:
    return JobRunResult(
        job_name=job_name,
        phase=phase,
        scheduled_at=scheduled_at,
        started_at=scheduled_at,
        finished_at=finished_at,
        success=success,
        detail=detail,
        error=error,
    )


def _settings(log_dir: Path, *, risk: RiskSettings | None = None) -> AppSettings:
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


def _write_intraday_risk_state(
    log_dir: Path,
    *,
    trading_day: date,
    session_start_equity: str | None,
    peak_equity: str | None,
    latest_equity: str | None = None,
) -> None:
    payload = {
        "trading_day": trading_day.isoformat(),
        "session_start_equity": session_start_equity,
        "peak_equity": peak_equity,
    }
    if latest_equity is not None:
        payload["latest_equity"] = latest_equity
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "intraday_risk_state.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )


@dataclass(slots=True)
class RecordingNotifier:
    notifications: list[NotificationMessage] = field(default_factory=list)

    def send(self, notification: NotificationMessage) -> None:
        self.notifications.append(notification)


class BrokenBrokerReader:
    def get_holdings(self):
        raise RuntimeError("broker unavailable")
