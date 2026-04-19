from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from autotrade.broker import BrokerReader
from autotrade.broker import BrokerTrader
from autotrade.broker import KoreaInvestmentBarSource
from autotrade.broker import KoreaInvestmentBrokerReader
from autotrade.broker import KoreaInvestmentBrokerTrader
from autotrade.broker import PaperBroker
from autotrade.config import AppSettings
from autotrade.config import ConfigError
from autotrade.config import TelegramSettings
from autotrade.config import load_settings
from autotrade.config import load_telegram_settings
from autotrade.data import Bar
from autotrade.data import CsvBarSource
from autotrade.data import CsvBarStore
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.data import validate_bar_series
from autotrade.execution import FileExecutionStateStore
from autotrade.report import CompositeNotifier
from autotrade.report import FileNotifier
from autotrade.report import Notifier
from autotrade.report import TelegramNotifier
from autotrade.report import WeeklyReviewReport
from autotrade.report import build_weekly_review_report
from autotrade.report import load_daily_inspection_reports
from autotrade.report import load_daily_run_reports
from autotrade.report import publish_weekly_review_alert
from autotrade.report import write_weekly_review_report
from autotrade.runtime.live_cycle import LiveCycleRuntime
from autotrade.runtime.live_cycle import strategy_timeframe_for
from autotrade.runtime.market_close import MarketCloseResult
from autotrade.runtime.market_close import MarketCloseRuntime
from autotrade.runtime.market_open import MarketOpenPreparationRuntime
from autotrade.runtime.runner import RunnerStatus
from autotrade.runtime.runner import SafeStopContext
from autotrade.runtime.runner import ScheduledRunner
from autotrade.scheduler import FileSchedulerStateStore
from autotrade.scheduler import JobContext
from autotrade.scheduler import ScheduledJob
from autotrade.strategy import StrategyKind
from autotrade.strategy import create_strategy

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ENV_FILE = ROOT / ".env"
ENV_TEMPLATE_FILE = ROOT / "docs" / "autotrade.env.example"
logger = logging.getLogger(__name__)
EXIT_CODE_SUCCESS = 0
EXIT_CODE_OPERATION_FAILED = 1
EXIT_CODE_CONFIGURATION_ERROR = 2


@dataclass(frozen=True, slots=True)
class WeeklyReviewExecution:
    report: WeeklyReviewReport
    report_path: Path


@dataclass(frozen=True, slots=True)
class OperationServices:
    settings: AppSettings
    strategy_kind: StrategyKind
    bar_root: Path
    notification_log_path: Path
    notifier: Notifier
    state_store: FileExecutionStateStore
    scheduler_state_store: FileSchedulerStateStore
    broker_reader: BrokerReader
    broker_trader: BrokerTrader
    runtime: LiveCycleRuntime


def _log_operation_failure(command_name: str, error: Exception) -> None:
    logger.error("%s 실행에 실패했습니다: %s", command_name, error)


def _handle_run_once(args: argparse.Namespace) -> int:
    logger.info("AutoTrade 운영 사이클 실행을 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    try:
        services = _build_operation_services(
            settings,
            strategy_kind=StrategyKind(args.strategy),
            bar_root=args.bar_root,
            paper_cash_override=args.paper_cash,
        )
        generated_at = datetime.now(KST)
        logger.info("운영 사이클을 실행합니다.")
        result = _execute_live_cycle(
            services.runtime,
            settings=services.settings,
            bar_root=services.bar_root,
            generated_at=generated_at,
        )
    except Exception as exc:
        _log_operation_failure("run-once", exc)
        return EXIT_CODE_OPERATION_FAILED
    logger.info("운영 사이클 실행이 끝났습니다.")

    print(result.render_korean_summary())
    print(f"알림 파일: {services.notification_log_path}")
    print(f"주문 상태 파일: {services.state_store.path}")
    return EXIT_CODE_SUCCESS


def _handle_run_continuous(args: argparse.Namespace) -> int:
    logger.info("AutoTrade 연속 운영 실행을 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    try:
        services = _build_operation_services(
            settings,
            strategy_kind=StrategyKind(args.strategy),
            bar_root=args.bar_root,
            paper_cash_override=args.paper_cash,
        )
        preparation_runtime = MarketOpenPreparationRuntime(
            settings=settings,
            strategy_kind=services.strategy_kind.value,
            timeframe=services.runtime.timeframe,
        )
        market_close_runtime = MarketCloseRuntime(
            settings=settings,
            broker_reader=services.broker_reader,
            notifier=services.notifier,
            state_store=services.state_store,
        )
        runner = ScheduledRunner(
            jobs=(
                preparation_runtime.build_job(),
                _build_scheduled_cycle_job(
                    services.runtime,
                    settings=settings,
                    bar_root=services.bar_root,
                ),
                _build_market_close_job(
                    market_close_runtime,
                    notifier=services.notifier,
                    telegram_settings=settings.telegram,
                ),
            ),
            state_store=services.scheduler_state_store,
            notifier=services.notifier,
            log_dir=settings.log_dir,
            safe_stop_handler=_build_safe_stop_cleanup_handler(
                market_close_runtime,
                notifier=services.notifier,
                telegram_settings=settings.telegram,
            ),
        )
        logger.info("연속 운영 runner를 시작합니다.")
        runner_result = runner.run_forever(max_iterations=args.max_iterations)
    except Exception as exc:
        _log_operation_failure("run-continuous", exc)
        return EXIT_CODE_OPERATION_FAILED
    logger.info(
        "연속 운영 runner가 종료되었습니다. 상태=%s",
        runner_result.status.value,
    )
    print(f"runner 상태: {runner_result.status.value}")
    if runner_result.stop_reason is not None:
        print(f"중지 사유: {runner_result.stop_reason}")
    print(f"알림 파일: {services.notification_log_path}")
    print(f"주문 상태 파일: {services.state_store.path}")
    print(f"scheduler 상태 파일: {services.scheduler_state_store.path}")
    if runner_result.status is RunnerStatus.SAFE_STOP:
        return EXIT_CODE_OPERATION_FAILED
    return EXIT_CODE_SUCCESS


def _handle_market_open(args: argparse.Namespace) -> int:
    logger.info("장전 준비 실행을 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    strategy_kind = StrategyKind(args.strategy)
    runtime = MarketOpenPreparationRuntime(
        settings=settings,
        strategy_kind=strategy_kind.value,
        timeframe=strategy_timeframe_for(strategy_kind),
    )
    result = runtime.run()
    print(result.render_summary())
    print(f"스모크 리포트 파일: {result.smoke_report_path}")
    print(f"점검 리포트 파일: {result.inspection_report_path}")
    if not result.success:
        return EXIT_CODE_OPERATION_FAILED
    return EXIT_CODE_SUCCESS


def _handle_market_close(args: argparse.Namespace) -> int:
    logger.info("장종료 정리 실행을 준비합니다.")
    settings = _load_runtime_settings(args.env_file)
    if settings is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    try:
        broker_reader, _ = _build_broker_clients(
            settings,
            paper_cash_override=args.paper_cash,
        )
        notifier = _build_notifier(settings)
        state_store = FileExecutionStateStore(settings.log_dir / "execution_state.json")
        runtime = MarketCloseRuntime(
            settings=settings,
            broker_reader=broker_reader,
            notifier=notifier,
            state_store=state_store,
        )
        generated_at = datetime.now(KST)
        result, weekly_review = _run_market_close_flow(
            runtime,
            notifier=notifier,
            telegram_settings=settings.telegram,
            timestamp=generated_at,
            triggered_at=generated_at,
        )
    except Exception as exc:
        _log_operation_failure("market-close", exc)
        return EXIT_CODE_OPERATION_FAILED

    print(result.render_summary())
    print(f"일일 실행 리포트 파일: {result.daily_run_report_path}")
    print(f"점검 리포트 파일: {result.inspection_report_path}")
    print(f"다음 거래일 준비 파일: {result.next_day_preparation_path}")
    if weekly_review is not None:
        print(f"주간 리뷰 파일: {weekly_review.report_path}")
    return EXIT_CODE_SUCCESS


def _handle_weekly_review(args: argparse.Namespace) -> int:
    environment = _load_environment(args.env_file)
    if environment is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    raw_log_dir = environment.get("AUTOTRADE_LOG_DIR")
    if raw_log_dir is None or not raw_log_dir.strip():
        logger.error(
            "설정 로딩에 실패했습니다: Missing required setting AUTOTRADE_LOG_DIR"
        )
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
    log_dir = Path(raw_log_dir).expanduser()
    try:
        telegram_settings = load_telegram_settings(environment)
    except ConfigError as exc:
        logger.error("설정 로딩에 실패했습니다: %s", exc)
        return EXIT_CODE_CONFIGURATION_ERROR

    try:
        weekly_review = _build_and_write_weekly_review(
            log_dir=log_dir,
            generated_at=generated_at,
        )
        if telegram_settings.enabled:
            publish_weekly_review_alert(
                _build_weekly_review_notifier(log_dir, telegram_settings),
                weekly_review.report,
                created_at=generated_at,
            )
    except Exception as exc:
        _log_operation_failure("weekly-review", exc)
        return EXIT_CODE_OPERATION_FAILED
    print(weekly_review.report_path)
    return EXIT_CODE_SUCCESS


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def _load_runtime_settings(env_file: Path) -> AppSettings | None:
    environment = _load_environment(env_file)
    if environment is None:
        return None
    try:
        settings = load_settings(env=environment)
    except ConfigError as exc:
        logger.error("설정 로딩에 실패했습니다: %s", exc)
        logger.error(
            "템플릿을 참고해 .env 파일을 준비하세요: %s",
            ENV_TEMPLATE_FILE,
        )
        return None
    return settings


def _load_environment(env_file: Path) -> dict[str, str] | None:
    try:
        return _resolve_environment(os.environ, env_file=env_file)
    except ValueError as exc:
        logger.error(".env 파일을 읽는 중 문제가 발생했습니다: %s", exc)
        logger.error(
            "템플릿을 참고해 .env 형식을 확인하세요: %s",
            ENV_TEMPLATE_FILE,
        )
        return None


def _build_operation_services(
    settings: AppSettings,
    *,
    strategy_kind: StrategyKind,
    bar_root: Path | None,
    paper_cash_override: Decimal | None,
) -> OperationServices:
    resolved_bar_root = bar_root or (settings.log_dir / "bars")
    notification_log_path = settings.log_dir / "notifications.jsonl"
    notifier = _build_notifier(settings)
    state_store = FileExecutionStateStore(settings.log_dir / "execution_state.json")
    scheduler_state_store = FileSchedulerStateStore(
        settings.log_dir / "scheduler_state.json"
    )
    logger.info(
        "설정을 불러왔습니다. 환경=%s 전략=%s 대상종목=%s",
        settings.broker.environment,
        strategy_kind.value,
        ",".join(settings.target_symbols),
    )
    logger.info("바 데이터 경로: %s", resolved_bar_root)
    logger.info("알림 파일 경로: %s", notification_log_path)
    logger.info("주문 상태 파일 경로: %s", state_store.path)
    logger.info("scheduler 상태 파일 경로: %s", scheduler_state_store.path)

    broker_reader, broker_trader = _build_broker_clients(
        settings,
        paper_cash_override=paper_cash_override,
    )
    runtime = LiveCycleRuntime(
        settings=settings,
        strategy=create_strategy(strategy_kind),
        timeframe=strategy_timeframe_for(strategy_kind),
        bar_source=CsvBarSource(resolved_bar_root),
        broker_reader=broker_reader,
        broker_trader=broker_trader,
        notifier=notifier,
        state_store=state_store,
    )
    return OperationServices(
        settings=settings,
        strategy_kind=strategy_kind,
        bar_root=resolved_bar_root,
        notification_log_path=notification_log_path,
        notifier=notifier,
        state_store=state_store,
        scheduler_state_store=scheduler_state_store,
        broker_reader=broker_reader,
        broker_trader=broker_trader,
        runtime=runtime,
    )


def _build_broker_clients(
    settings: AppSettings,
    *,
    paper_cash_override: Decimal | None,
) -> tuple[BrokerReader, BrokerTrader]:
    if settings.broker.environment == "paper":
        broker, paper_cash = _build_paper_broker(
            settings,
            paper_cash_override=paper_cash_override,
        )
        if paper_cash_override is None:
            logger.info(
                "KIS paper 주문가능현금으로 내부 PaperBroker를 초기화합니다. 초기 현금=%s",
                paper_cash,
            )
        else:
            logger.info(
                "사용자 지정 초기 현금으로 내부 PaperBroker를 실행합니다. 초기 현금=%s",
                paper_cash,
            )
        return broker, broker

    logger.info("실브로커(KIS) 연동 객체를 초기화합니다.")
    return (
        KoreaInvestmentBrokerReader(settings.broker),
        KoreaInvestmentBrokerTrader(settings.broker),
    )


def _build_notifier(settings: AppSettings) -> Notifier:
    file_notifier = FileNotifier(settings.log_dir / "notifications.jsonl")
    if not settings.telegram.enabled:
        return file_notifier
    return CompositeNotifier(
        (
            file_notifier,
            TelegramNotifier(settings.telegram),
        )
    )


def _build_weekly_review_notifier(
    log_dir: Path,
    telegram_settings: TelegramSettings,
) -> Notifier:
    return CompositeNotifier(
        (
            FileNotifier(log_dir / "notifications.jsonl"),
            TelegramNotifier(telegram_settings),
        )
    )


def _resolve_environment(
    base_environment: Mapping[str, str],
    *,
    env_file: Path,
) -> dict[str, str]:
    env_values = _load_env_file(env_file)
    if env_file.exists():
        logger.info(
            ".env 파일을 읽었습니다. 경로=%s 항목수=%d",
            env_file,
            len(env_values),
        )
    else:
        logger.info(
            ".env 파일이 없어 현재 셸 환경만 사용합니다. 기본 경로=%s 템플릿=%s",
            env_file,
            ENV_TEMPLATE_FILE,
        )
    merged = dict(env_values)
    merged.update(base_environment)
    return merged


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    if not path.is_file():
        raise ValueError(f".env 경로가 파일이 아닙니다: {path}")

    parsed: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped.removeprefix("export ").strip()
        if "=" not in stripped:
            raise ValueError(f".env 형식이 잘못되었습니다: line {line_number}")
        key, value = stripped.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            raise ValueError(f".env 키가 비어 있습니다: line {line_number}")
        parsed[normalized_key] = _strip_optional_quotes(value.strip())
    return parsed


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _collect_strategy_bars(
    settings: AppSettings,
    *,
    bar_root: Path,
    timeframe: Timeframe,
    generated_at: datetime,
) -> None:
    if settings.broker.provider != "koreainvestment":
        raise ValueError("현재 자동 바 수집은 koreainvestment provider만 지원합니다.")

    window_start = _collection_window_start(timeframe, generated_at)
    bar_source = KoreaInvestmentBarSource(settings.broker)
    bar_store = CsvBarStore(bar_root)
    cached_bar_source = CsvBarSource(bar_root)
    calendar = KrxRegularSessionCalendar()
    logger.info(
        "전략 입력 바 수집을 시작합니다. 시작=%s 종료=%s 주기=%s",
        window_start.isoformat(),
        generated_at.isoformat(),
        timeframe.value,
    )

    total_fetched_bars = 0
    for symbol in settings.target_symbols:
        cached_bars = cached_bar_source.load_bars(
            symbol,
            timeframe,
            start=window_start,
            end=generated_at,
        )
        request_start = _resolve_incremental_collection_start(
            cached_bars,
            timeframe=timeframe,
            window_start=window_start,
            generated_at=generated_at,
            calendar=calendar,
            symbol=symbol,
        )
        if request_start is None:
            logger.info(
                "이미 최신 바가 있어 수집 요청을 건너뜁니다. symbol=%s cached_bars=%d",
                symbol,
                len(cached_bars),
            )
            continue

        logger.info(
            "바 수집을 요청합니다. symbol=%s start=%s end=%s",
            symbol,
            request_start.isoformat(),
            generated_at.isoformat(),
        )
        fetched_bars = bar_source.load_bars(
            symbol,
            timeframe,
            start=request_start,
            end=generated_at,
        )
        validate_bar_series(fetched_bars)
        merged_bars = _merge_bar_series(cached_bars, fetched_bars)
        validate_bar_series(merged_bars)
        bar_store.store_bars(merged_bars)
        total_fetched_bars += len(fetched_bars)
        logger.info(
            "바 수집을 마쳤습니다. symbol=%s fetched_bars=%d stored_bars=%d path=%s",
            symbol,
            len(fetched_bars),
            len(merged_bars),
            bar_root / timeframe.value / f"{symbol}.csv",
        )

    logger.info(
        "전략 입력 바 수집을 완료했습니다. 총 신규 바 수=%d",
        total_fetched_bars,
    )


def _execute_live_cycle(
    runtime: LiveCycleRuntime,
    *,
    settings: AppSettings,
    bar_root: Path,
    generated_at: datetime,
):
    _collect_strategy_bars(
        settings,
        bar_root=bar_root,
        timeframe=runtime.timeframe,
        generated_at=generated_at,
    )
    return runtime.run(timestamp=generated_at)


def _resolve_incremental_collection_start(
    cached_bars: tuple[Bar, ...],
    *,
    timeframe: Timeframe,
    window_start: datetime,
    generated_at: datetime,
    calendar: KrxRegularSessionCalendar,
    symbol: str,
) -> datetime | None:
    if not cached_bars:
        return window_start

    try:
        validate_bar_series(cached_bars, calendar=calendar)
    except ValueError as exc:
        logger.warning(
            "기존 캐시 바가 완전하지 않아 전체 윈도우를 다시 수집합니다. symbol=%s reason=%s",
            symbol,
            exc,
        )
        return window_start

    next_timestamp = calendar.next_timestamp(cached_bars[-1].timestamp, timeframe)
    if next_timestamp > generated_at:
        return None
    return max(window_start, next_timestamp)


def _merge_bar_series(
    cached_bars: tuple[Bar, ...],
    fetched_bars: tuple[Bar, ...],
) -> tuple[Bar, ...]:
    merged_by_timestamp = {bar.timestamp: bar for bar in cached_bars}
    for bar in fetched_bars:
        merged_by_timestamp[bar.timestamp] = bar
    return tuple(
        sorted(
            merged_by_timestamp.values(),
            key=lambda bar: bar.timestamp,
        )
    )


def _build_scheduled_cycle_job(
    runtime: LiveCycleRuntime,
    *,
    settings: AppSettings,
    bar_root: Path,
) -> ScheduledJob:
    base_job = runtime.build_job()

    def handler(context: JobContext) -> str:
        result = _execute_live_cycle(
            runtime,
            settings=settings,
            bar_root=bar_root,
            generated_at=context.scheduled_at,
        )
        return result.render_summary()

    return ScheduledJob(
        name=base_job.name,
        phase=base_job.phase,
        handler=handler,
    )


def _build_market_close_job(
    runtime: MarketCloseRuntime,
    *,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
) -> ScheduledJob:
    base_job = runtime.build_job()

    def handler(context: JobContext) -> str:
        result, weekly_review = _run_market_close_flow(
            runtime,
            notifier=notifier,
            telegram_settings=telegram_settings,
            timestamp=context.scheduled_at,
            triggered_at=context.triggered_at,
        )
        return _render_market_close_summary(result, weekly_review)

    return ScheduledJob(
        name=base_job.name,
        phase=base_job.phase,
        handler=handler,
    )


def _build_safe_stop_cleanup_handler(
    runtime: MarketCloseRuntime,
    *,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
):
    def handler(context: SafeStopContext) -> str:
        result, weekly_review = _run_market_close_flow(
            runtime,
            notifier=notifier,
            telegram_settings=telegram_settings,
            timestamp=context.triggered_at,
            safe_stop_reason=context.reason,
            safe_stop_detail=context.detail,
        )
        return _render_market_close_summary(result, weekly_review)

    return handler


def _run_market_close_flow(
    runtime: MarketCloseRuntime,
    *,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
    timestamp: datetime,
    triggered_at: datetime | None = None,
    safe_stop_reason: str | None = None,
    safe_stop_detail: str | None = None,
) -> tuple[MarketCloseResult, WeeklyReviewExecution | None]:
    if safe_stop_reason is None and safe_stop_detail is None:
        result = runtime.run(
            timestamp=timestamp,
            triggered_at=triggered_at,
        )
    else:
        result = runtime.run_safe_stop_cleanup(
            timestamp=timestamp,
            reason=safe_stop_reason or "safe_stop",
            detail=safe_stop_detail or "safe stop cleanup",
        )

    weekly_review = _maybe_create_weekly_review(
        log_dir=runtime.settings.log_dir,
        trading_day=result.trading_day,
        generated_at=result.generated_at,
        notifier=notifier,
        telegram_settings=telegram_settings,
        calendar=runtime.calendar,
    )
    return result, weekly_review


def _maybe_create_weekly_review(
    *,
    log_dir: Path,
    trading_day: date,
    generated_at: datetime,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
    calendar: KrxRegularSessionCalendar,
) -> WeeklyReviewExecution | None:
    if not _is_last_trading_day_of_week(trading_day, calendar):
        logger.info(
            "이번 거래일은 주 종료일이 아니므로 주간 리뷰를 건너뜁니다. trading_day=%s",
            trading_day.isoformat(),
        )
        return None

    logger.info(
        "주 종료일이라 주간 리뷰를 생성합니다. trading_day=%s",
        trading_day.isoformat(),
    )
    weekly_review = _build_and_write_weekly_review(
        log_dir=log_dir,
        generated_at=generated_at,
    )
    if telegram_settings.enabled:
        publish_weekly_review_alert(
            notifier,
            weekly_review.report,
            created_at=generated_at,
        )
    return weekly_review


def _build_and_write_weekly_review(
    *,
    log_dir: Path,
    generated_at: datetime,
) -> WeeklyReviewExecution:
    week_start = generated_at.date() - timedelta(days=generated_at.weekday())
    report = build_weekly_review_report(
        week_start,
        generated_at=generated_at,
        daily_run_reports=load_daily_run_reports(
            log_dir,
            start=week_start,
            end=week_start + timedelta(days=6),
        ),
        daily_inspection_reports=load_daily_inspection_reports(
            log_dir,
            start=week_start,
            end=week_start + timedelta(days=6),
        ),
    )
    report_path = write_weekly_review_report(log_dir, report)
    return WeeklyReviewExecution(report=report, report_path=report_path)


def _is_last_trading_day_of_week(
    trading_day: date,
    calendar: KrxRegularSessionCalendar,
) -> bool:
    week_end = trading_day + timedelta(days=6 - trading_day.weekday())
    candidate = trading_day + timedelta(days=1)
    while candidate <= week_end:
        if calendar.is_trading_day(candidate):
            return False
        candidate += timedelta(days=1)
    return True


def _render_market_close_summary(
    result: MarketCloseResult,
    weekly_review: WeeklyReviewExecution | None,
) -> str:
    summary = result.render_summary()
    if weekly_review is None:
        return f"{summary} weekly_review=skipped"
    return f"{summary} weekly_review={weekly_review.report_path.name}"


def _collection_window_start(timeframe: Timeframe, generated_at: datetime) -> datetime:
    if timeframe is Timeframe.DAY:
        return generated_at - timedelta(days=180)
    return generated_at - timedelta(days=21)


def _build_paper_broker(
    settings: AppSettings,
    *,
    paper_cash_override: Decimal | None,
) -> tuple[PaperBroker, Decimal]:
    if paper_cash_override is not None:
        return PaperBroker(initial_cash=paper_cash_override), paper_cash_override

    reader = KoreaInvestmentBrokerReader(settings.broker)
    target_symbol = settings.target_symbols[0]
    quote = reader.get_quote(target_symbol)
    capacity = reader.get_order_capacity(target_symbol, quote.price)
    return PaperBroker(initial_cash=capacity.cash_available), capacity.cash_available


__all__ = [
    "DEFAULT_ENV_FILE",
    "ENV_TEMPLATE_FILE",
    "OperationServices",
    "WeeklyReviewExecution",
    "_build_notifier",
    "_build_paper_broker",
    "_build_safe_stop_cleanup_handler",
    "_build_scheduled_cycle_job",
    "_build_weekly_review_notifier",
    "_configure_logging",
    "_collect_strategy_bars",
    "_execute_live_cycle",
    "_handle_market_close",
    "_handle_market_open",
    "_handle_run_continuous",
    "_handle_run_once",
    "_handle_weekly_review",
    "_is_last_trading_day_of_week",
    "_load_env_file",
    "_maybe_create_weekly_review",
    "_resolve_environment",
]
