from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Mapping
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from autotrade.broker import KoreaInvestmentBarSource
from autotrade.broker import KoreaInvestmentBrokerReader
from autotrade.broker import KoreaInvestmentBrokerTrader
from autotrade.broker import PaperBroker
from autotrade.config import AppSettings
from autotrade.config import ConfigError
from autotrade.config import TelegramSettings
from autotrade.config import load_telegram_settings
from autotrade.data import Bar
from autotrade.data import CsvBarSource
from autotrade.data import CsvBarStore
from autotrade.data import KST
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.data import validate_bar_series
from autotrade.data.validation import normalize_symbols
from autotrade.execution import FileExecutionStateStore
from autotrade.recommendation import ApprovedSymbolsRecord
from autotrade.recommendation import RecommendationArtifacts
from autotrade.recommendation import RecommendationPolicy
from autotrade.recommendation import RECOMMENDATION_DIR
from autotrade.recommendation import build_recommendation_report
from autotrade.recommendation import load_seed_universe_csv
from autotrade.recommendation import write_approved_symbols_bundle
from autotrade.recommendation import write_recommendation_bundle
from autotrade.report import build_daily_inspection_report
from autotrade.report import build_weekly_review_report
from autotrade.report import CompositeNotifier
from autotrade.report import FileNotifier
from autotrade.report import Notifier
from autotrade.report import publish_weekly_review_alert
from autotrade.report import TelegramNotifier
from autotrade.report import write_daily_inspection_report
from autotrade.report import write_weekly_review_report
from autotrade.report import load_daily_inspection_reports
from autotrade.report import load_daily_run_reports
from autotrade.runtime.live_cycle import LiveCycleRuntime
from autotrade.runtime.live_cycle import strategy_timeframe_for
from autotrade.runtime.market_close import MarketCloseResult
from autotrade.runtime.market_close import MarketCloseRuntime
from autotrade.runtime.market_open import MarketOpenPreparationRuntime
from autotrade.runtime.operation_environment import load_env_file as _load_env_file_impl
from autotrade.runtime.operation_environment import (
    load_environment as _load_environment_impl,
)
from autotrade.runtime.operation_environment import (
    load_runtime_settings as _load_runtime_settings_impl,
)
from autotrade.runtime.operation_environment import (
    resolve_environment as _resolve_environment_impl,
)
from autotrade.runtime.operation_flows import WeeklyReviewExecution
from autotrade.runtime.operation_flows import (
    build_and_write_weekly_review as _build_and_write_weekly_review_impl,
)
from autotrade.runtime.operation_flows import (
    build_market_close_job as _build_market_close_job_impl,
)
from autotrade.runtime.operation_flows import (
    build_safe_stop_cleanup_handler as _build_safe_stop_cleanup_handler_impl,
)
from autotrade.runtime.operation_flows import (
    build_scheduled_cycle_job as _build_scheduled_cycle_job_impl,
)
from autotrade.runtime.operation_flows import (
    collect_strategy_bars as _collect_strategy_bars_impl,
)
from autotrade.runtime.operation_flows import (
    collection_window_start as _collection_window_start_impl,
)
from autotrade.runtime.operation_flows import (
    execute_live_cycle as _execute_live_cycle_impl,
)
from autotrade.runtime.operation_flows import (
    is_last_trading_day_of_week as _is_last_trading_day_of_week_impl,
)
from autotrade.runtime.operation_flows import (
    maybe_create_weekly_review as _maybe_create_weekly_review_impl,
)
from autotrade.runtime.operation_flows import merge_bar_series as _merge_bar_series_impl
from autotrade.runtime.operation_flows import (
    render_market_close_summary as _render_market_close_summary_impl,
)
from autotrade.runtime.operation_flows import (
    resolve_incremental_collection_start as _resolve_incremental_collection_start_impl,
)
from autotrade.runtime.operation_flows import (
    run_market_close_flow as _run_market_close_flow_impl,
)
from autotrade.runtime.operation_services import OperationServices
from autotrade.runtime.operation_services import (
    build_broker_clients as _build_broker_clients_impl,
)
from autotrade.runtime.operation_services import build_notifier as _build_notifier_impl
from autotrade.runtime.operation_services import (
    build_operation_services as _build_operation_services_impl,
)
from autotrade.runtime.operation_services import (
    build_paper_broker as _build_paper_broker_impl,
)
from autotrade.runtime.operation_services import (
    build_weekly_review_notifier as _build_weekly_review_notifier_impl,
)
from autotrade.runtime.runner import RunnerStatus
from autotrade.runtime.runner import ScheduledRunner
from autotrade.strategy import StrategyKind

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ENV_FILE = ROOT / ".env"
ENV_TEMPLATE_FILE = ROOT / "docs" / "autotrade.env.example"
logger = logging.getLogger(__name__)
EXIT_CODE_SUCCESS = 0
EXIT_CODE_OPERATION_FAILED = 1
EXIT_CODE_CONFIGURATION_ERROR = 2


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
            bar_root=services.bar_root,
            broker_reader=services.broker_reader,
            notifier=services.notifier,
            state_store=services.state_store,
            collect_strategy_bars=_collect_strategy_bars,
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
    try:
        services = _build_operation_services(
            settings,
            strategy_kind=strategy_kind,
            bar_root=None,
            paper_cash_override=None,
        )
        runtime = MarketOpenPreparationRuntime(
            settings=settings,
            strategy_kind=strategy_kind.value,
            timeframe=strategy_timeframe_for(strategy_kind),
            bar_root=services.bar_root,
            broker_reader=services.broker_reader,
            notifier=services.notifier,
            state_store=services.state_store,
            collect_strategy_bars=_collect_strategy_bars,
        )
        result = runtime.run()
    except Exception as exc:
        _log_operation_failure("market-open", exc)
        return EXIT_CODE_OPERATION_FAILED
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
    log_dir = _require_log_dir(environment)
    if log_dir is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
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


def _handle_weekly_recommendation(args: argparse.Namespace) -> int:
    environment = _load_environment(args.env_file)
    if environment is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    log_dir = _require_log_dir(environment)
    if log_dir is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
    resolved_bar_root = args.bar_root or (log_dir / "bars")
    policy = RecommendationPolicy(
        min_history_days=args.minimum_history_days,
        min_average_traded_value=args.minimum_average_trading_value,
        top_n=args.candidate_count,
        max_per_sector=args.max_candidates_per_sector,
        excluded_symbols=tuple(args.exclude_symbol),
        excluded_sectors=tuple(args.exclude_sector),
    )
    try:
        artifacts = _build_and_write_weekly_recommendation(
            log_dir=log_dir,
            universe_file=args.universe_file,
            bar_root=resolved_bar_root,
            generated_at=generated_at,
            policy=policy,
        )
    except Exception as exc:
        _log_operation_failure("weekly-recommendation", exc)
        return EXIT_CODE_OPERATION_FAILED
    print(artifacts.markdown_path)
    print(artifacts.csv_path)
    print(artifacts.json_path)
    return EXIT_CODE_SUCCESS


def _handle_approve_symbols(args: argparse.Namespace) -> int:
    environment = _load_environment(args.env_file)
    if environment is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    log_dir = _require_log_dir(environment)
    if log_dir is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
    try:
        candidate_report, candidate_report_path = _load_candidate_report_for_approval(
            log_dir=log_dir,
            candidate_json=args.candidate_json,
        )
        approved_symbols = tuple(
            symbol.strip() for symbol in _strip_optional_quotes(args.symbols).split(",")
        )
        approved_record = _approve_symbols(
            log_dir=log_dir,
            approved_symbols=approved_symbols,
            candidate_payload=candidate_report,
            candidate_report_path=candidate_report_path,
            created_at=generated_at,
        )
    except Exception as exc:
        _log_operation_failure("approve-symbols", exc)
        return EXIT_CODE_OPERATION_FAILED
    print(approved_record.latest_path)
    return EXIT_CODE_SUCCESS


def _handle_daily_inspection(args: argparse.Namespace) -> int:
    environment = _load_environment(args.env_file)
    if environment is None:
        return EXIT_CODE_CONFIGURATION_ERROR
    log_dir = _require_log_dir(environment)
    if log_dir is None:
        return EXIT_CODE_CONFIGURATION_ERROR

    generated_at = datetime.now(KST)
    report = build_daily_inspection_report(
        generated_at.date(),
        generated_at=generated_at,
    )
    report_path = write_daily_inspection_report(log_dir, report)
    print(report_path)
    return EXIT_CODE_SUCCESS


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def _load_runtime_settings(env_file: Path) -> AppSettings | None:
    return _load_runtime_settings_impl(
        env_file,
        env_template_file=ENV_TEMPLATE_FILE,
        logger=logger,
    )


def _load_environment(env_file: Path) -> dict[str, str] | None:
    return _load_environment_impl(
        env_file,
        env_template_file=ENV_TEMPLATE_FILE,
        logger=logger,
    )


def _build_operation_services(
    settings: AppSettings,
    *,
    strategy_kind: StrategyKind,
    bar_root: Path | None,
    paper_cash_override: Decimal | None,
) -> OperationServices:
    return _build_operation_services_impl(
        settings,
        strategy_kind=strategy_kind,
        bar_root=bar_root,
        paper_cash_override=paper_cash_override,
        logger=logger,
        build_notifier=_build_notifier,
        build_broker_clients=_build_broker_clients,
    )


def _build_broker_clients(
    settings: AppSettings,
    *,
    paper_cash_override: Decimal | None,
):
    return _build_broker_clients_impl(
        settings,
        paper_cash_override=paper_cash_override,
        logger=logger,
        build_paper_broker=_build_paper_broker,
        reader_cls=KoreaInvestmentBrokerReader,
        trader_cls=KoreaInvestmentBrokerTrader,
    )


def _build_notifier(settings: AppSettings) -> Notifier:
    return _build_notifier_impl(
        settings,
        file_notifier_cls=FileNotifier,
        composite_notifier_cls=CompositeNotifier,
        telegram_notifier_cls=TelegramNotifier,
    )


def _build_weekly_review_notifier(
    log_dir: Path,
    telegram_settings: TelegramSettings,
) -> Notifier:
    return _build_weekly_review_notifier_impl(
        log_dir,
        telegram_settings,
        file_notifier_cls=FileNotifier,
        composite_notifier_cls=CompositeNotifier,
        telegram_notifier_cls=TelegramNotifier,
    )


def _resolve_environment(
    base_environment: Mapping[str, str],
    *,
    env_file: Path,
) -> dict[str, str]:
    return _resolve_environment_impl(
        base_environment,
        env_file=env_file,
        env_template_file=ENV_TEMPLATE_FILE,
        logger=logger,
    )


def _load_env_file(path: Path) -> dict[str, str]:
    return _load_env_file_impl(path)


def _strip_optional_quotes(value: str) -> str:
    return (
        value[1:-1]
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}
        else value
    )


def _collect_strategy_bars(
    settings: AppSettings,
    *,
    bar_root: Path,
    timeframe: Timeframe,
    generated_at: datetime,
) -> None:
    return _collect_strategy_bars_impl(
        settings,
        bar_root=bar_root,
        timeframe=timeframe,
        generated_at=generated_at,
        logger=logger,
        bar_source_factory=KoreaInvestmentBarSource,
        bar_store_factory=CsvBarStore,
        cached_bar_source_factory=CsvBarSource,
        calendar_factory=KrxRegularSessionCalendar,
        validate_bars=validate_bar_series,
    )


def _execute_live_cycle(
    runtime: LiveCycleRuntime,
    *,
    settings: AppSettings,
    bar_root: Path,
    generated_at: datetime,
):
    return _execute_live_cycle_impl(
        runtime,
        settings=settings,
        bar_root=bar_root,
        generated_at=generated_at,
        collect_strategy_bars=_collect_strategy_bars,
    )


def _resolve_incremental_collection_start(
    cached_bars: tuple[Bar, ...],
    *,
    timeframe: Timeframe,
    window_start: datetime,
    generated_at: datetime,
    calendar: KrxRegularSessionCalendar,
    symbol: str,
) -> datetime | None:
    return _resolve_incremental_collection_start_impl(
        cached_bars,
        timeframe=timeframe,
        window_start=window_start,
        generated_at=generated_at,
        calendar=calendar,
        symbol=symbol,
        logger=logger,
        validate_bars=validate_bar_series,
    )


def _merge_bar_series(
    cached_bars: tuple[Bar, ...],
    fetched_bars: tuple[Bar, ...],
) -> tuple[Bar, ...]:
    return _merge_bar_series_impl(cached_bars, fetched_bars)


def _build_scheduled_cycle_job(
    runtime: LiveCycleRuntime,
    *,
    settings: AppSettings,
    bar_root: Path,
):
    return _build_scheduled_cycle_job_impl(
        runtime,
        settings=settings,
        bar_root=bar_root,
        execute_live_cycle=_execute_live_cycle,
    )


def _build_market_close_job(
    runtime: MarketCloseRuntime,
    *,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
):
    return _build_market_close_job_impl(
        runtime,
        notifier=notifier,
        telegram_settings=telegram_settings,
        run_market_close_flow=_run_market_close_flow,
        render_market_close_summary=_render_market_close_summary,
    )


def _build_safe_stop_cleanup_handler(
    runtime: MarketCloseRuntime,
    *,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
):
    return _build_safe_stop_cleanup_handler_impl(
        runtime,
        notifier=notifier,
        telegram_settings=telegram_settings,
        run_market_close_flow=_run_market_close_flow,
        render_market_close_summary=_render_market_close_summary,
    )


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
    return _run_market_close_flow_impl(
        runtime,
        notifier=notifier,
        telegram_settings=telegram_settings,
        timestamp=timestamp,
        maybe_create_weekly_review=_maybe_create_weekly_review,
        triggered_at=triggered_at,
        safe_stop_reason=safe_stop_reason,
        safe_stop_detail=safe_stop_detail,
    )


def _maybe_create_weekly_review(
    *,
    log_dir: Path,
    trading_day: date,
    generated_at: datetime,
    notifier: Notifier,
    telegram_settings: TelegramSettings,
    calendar: KrxRegularSessionCalendar,
) -> WeeklyReviewExecution | None:
    return _maybe_create_weekly_review_impl(
        log_dir=log_dir,
        trading_day=trading_day,
        generated_at=generated_at,
        notifier=notifier,
        telegram_settings=telegram_settings,
        calendar=calendar,
        logger=logger,
        is_last_trading_day_of_week=_is_last_trading_day_of_week,
        build_and_write_weekly_review=_build_and_write_weekly_review,
        publish_weekly_review_alert=publish_weekly_review_alert,
    )


def _build_and_write_weekly_review(
    *,
    log_dir: Path,
    generated_at: datetime,
) -> WeeklyReviewExecution:
    return _build_and_write_weekly_review_impl(
        log_dir=log_dir,
        generated_at=generated_at,
        build_weekly_review_report=build_weekly_review_report,
        load_daily_run_reports=load_daily_run_reports,
        load_daily_inspection_reports=load_daily_inspection_reports,
        write_weekly_review_report=write_weekly_review_report,
    )


def _build_and_write_weekly_recommendation(
    *,
    log_dir: Path,
    universe_file: Path,
    bar_root: Path,
    generated_at: datetime,
    policy: RecommendationPolicy,
) -> RecommendationArtifacts:
    universe = load_seed_universe_csv(universe_file)
    bar_source = CsvBarSource(bar_root)
    bars_by_symbol = {
        member.symbol: bar_source.load_bars(
            member.symbol,
            Timeframe.DAY,
            end=generated_at,
        )
        for member in universe
    }
    report = build_recommendation_report(
        universe,
        bars_by_symbol,
        policy,
        as_of=generated_at.date(),
        generated_at=generated_at,
    )
    return write_recommendation_bundle(log_dir, report)


def _load_candidate_report_for_approval(
    *,
    log_dir: Path,
    candidate_json: Path | None,
):
    if candidate_json is not None:
        return _load_candidate_payload(candidate_json), candidate_json
    latest_path = log_dir / RECOMMENDATION_DIR / "weekly_candidates_latest.json"
    payload = _load_candidate_payload(latest_path)
    if payload is None:
        raise ValueError("latest weekly recommendation report was not found")
    return payload, latest_path


def _approve_symbols(
    *,
    log_dir: Path,
    approved_symbols: tuple[str, ...],
    candidate_payload: dict[str, object],
    candidate_report_path: Path,
    created_at: datetime,
):
    selected = candidate_payload.get("selected")
    if not isinstance(selected, list):
        raise ValueError("candidate report payload must contain a selected list")
    candidate_symbols = {_extract_candidate_symbol(candidate) for candidate in selected}
    resolved_symbols = normalize_symbols(approved_symbols)
    unknown_symbols = sorted(
        symbol for symbol in resolved_symbols if symbol not in candidate_symbols
    )
    if unknown_symbols:
        raise ValueError(
            "approved symbols must exist in the candidate report: "
            + ",".join(unknown_symbols)
        )
    return write_approved_symbols_bundle(
        log_dir,
        ApprovedSymbolsRecord(
            as_of=created_at.date(),
            approved_at=created_at,
            symbols=resolved_symbols,
            source_report=str(candidate_report_path),
        ),
    )


def _is_last_trading_day_of_week(
    trading_day: date,
    calendar: KrxRegularSessionCalendar,
) -> bool:
    return _is_last_trading_day_of_week_impl(trading_day, calendar)


def _render_market_close_summary(
    result: MarketCloseResult,
    weekly_review: WeeklyReviewExecution | None,
) -> str:
    return _render_market_close_summary_impl(result, weekly_review)


def _collection_window_start(timeframe: Timeframe, generated_at: datetime) -> datetime:
    return _collection_window_start_impl(timeframe, generated_at)


def _build_paper_broker(
    settings: AppSettings,
    *,
    paper_cash_override: Decimal | None,
) -> tuple[PaperBroker, Decimal]:
    return _build_paper_broker_impl(
        settings,
        paper_cash_override=paper_cash_override,
        reader_cls=KoreaInvestmentBrokerReader,
        broker_cls=PaperBroker,
    )


def _require_log_dir(environment: Mapping[str, str]) -> Path | None:
    raw_log_dir = environment.get("AUTOTRADE_LOG_DIR")
    if raw_log_dir is None or not raw_log_dir.strip():
        logger.error(
            "설정 로딩에 실패했습니다: Missing required setting AUTOTRADE_LOG_DIR"
        )
        return None
    return Path(raw_log_dir).expanduser()


def _load_candidate_payload(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("candidate report payload must be an object")
    return payload


def _extract_candidate_symbol(payload: object) -> str:
    if not isinstance(payload, dict):
        raise ValueError("candidate entry must be an object")
    symbol = payload.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("candidate entry must include a symbol")
    return symbol


__all__ = [
    "DEFAULT_ENV_FILE",
    "ENV_TEMPLATE_FILE",
    "EXIT_CODE_CONFIGURATION_ERROR",
    "EXIT_CODE_OPERATION_FAILED",
    "EXIT_CODE_SUCCESS",
    "OperationServices",
    "WeeklyReviewExecution",
    "_build_notifier",
    "_build_paper_broker",
    "_build_safe_stop_cleanup_handler",
    "_build_scheduled_cycle_job",
    "_build_and_write_weekly_recommendation",
    "_build_weekly_review_notifier",
    "_collection_window_start",
    "_configure_logging",
    "_collect_strategy_bars",
    "_execute_live_cycle",
    "_handle_approve_symbols",
    "_handle_daily_inspection",
    "_handle_market_close",
    "_handle_market_open",
    "_handle_run_continuous",
    "_handle_run_once",
    "_handle_weekly_recommendation",
    "_handle_weekly_review",
    "_is_last_trading_day_of_week",
    "_load_env_file",
    "_load_environment",
    "_load_runtime_settings",
    "_maybe_create_weekly_review",
    "_merge_bar_series",
    "_render_market_close_summary",
    "_resolve_environment",
    "_resolve_incremental_collection_start",
    "_run_market_close_flow",
    "timedelta",
]
