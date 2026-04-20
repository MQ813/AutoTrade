from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Protocol

from autotrade.config import AppSettings
from autotrade.config import TelegramSettings
from autotrade.data import Bar
from autotrade.data import KrxRegularSessionCalendar
from autotrade.data import Timeframe
from autotrade.report import WeeklyReviewReport
from autotrade.runtime.live_cycle import LiveCycleResult
from autotrade.runtime.live_cycle import LiveCycleRuntime
from autotrade.runtime.market_close import MarketCloseResult
from autotrade.runtime.market_close import MarketCloseRuntime
from autotrade.runtime.runner import SafeStopContext
from autotrade.scheduler import JobContext
from autotrade.scheduler import ScheduledJob


@dataclass(frozen=True, slots=True)
class WeeklyReviewExecution:
    report: WeeklyReviewReport
    report_path: Path


class _BarSource(Protocol):
    def load_bars(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[Bar, ...]: ...


class _BarStore(Protocol):
    def store_bars(self, bars: tuple[Bar, ...]) -> None: ...


def collect_strategy_bars(
    settings: AppSettings,
    *,
    bar_root: Path,
    timeframe: Timeframe,
    generated_at: datetime,
    logger: logging.Logger,
    bar_source_factory: Callable[..., _BarSource],
    bar_store_factory: Callable[..., _BarStore],
    cached_bar_source_factory: Callable[..., _BarSource],
    calendar_factory: Callable[[], KrxRegularSessionCalendar],
    validate_bars: Callable[..., None],
) -> None:
    if settings.broker.provider != "koreainvestment":
        raise ValueError("현재 자동 바 수집은 koreainvestment provider만 지원합니다.")

    window_start = collection_window_start(timeframe, generated_at)
    bar_source = bar_source_factory(settings.broker)
    bar_store = bar_store_factory(bar_root)
    cached_bar_source = cached_bar_source_factory(bar_root)
    calendar = calendar_factory()
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
        request_start = resolve_incremental_collection_start(
            cached_bars,
            timeframe=timeframe,
            window_start=window_start,
            generated_at=generated_at,
            calendar=calendar,
            symbol=symbol,
            logger=logger,
            validate_bars=validate_bars,
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
        validate_bars(fetched_bars)
        merged_bars = merge_bar_series(cached_bars, fetched_bars)
        validate_bars(merged_bars)
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


def execute_live_cycle(
    runtime: LiveCycleRuntime,
    *,
    settings: AppSettings,
    bar_root: Path,
    generated_at: datetime,
    collect_strategy_bars: Callable[..., None],
) -> LiveCycleResult:
    collect_strategy_bars(
        settings,
        bar_root=bar_root,
        timeframe=runtime.timeframe,
        generated_at=generated_at,
    )
    return runtime.run(timestamp=generated_at)


def resolve_incremental_collection_start(
    cached_bars: tuple[Bar, ...],
    *,
    timeframe: Timeframe,
    window_start: datetime,
    generated_at: datetime,
    calendar: KrxRegularSessionCalendar,
    symbol: str,
    logger: logging.Logger,
    validate_bars: Callable[..., None],
) -> datetime | None:
    if not cached_bars:
        return window_start

    try:
        validate_bars(cached_bars, calendar=calendar)
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


def merge_bar_series(
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


def build_scheduled_cycle_job(
    runtime: LiveCycleRuntime,
    *,
    settings: AppSettings,
    bar_root: Path,
    execute_live_cycle: Callable[..., LiveCycleResult],
) -> ScheduledJob:
    base_job = runtime.build_job()

    def handler(context: JobContext) -> str:
        result = execute_live_cycle(
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


def build_market_close_job(
    runtime: MarketCloseRuntime,
    *,
    notifier,
    telegram_settings: TelegramSettings,
    run_market_close_flow: Callable[
        ..., tuple[MarketCloseResult, WeeklyReviewExecution | None]
    ],
    render_market_close_summary: Callable[
        [MarketCloseResult, WeeklyReviewExecution | None], str
    ],
) -> ScheduledJob:
    base_job = runtime.build_job()

    def handler(context: JobContext) -> str:
        result, weekly_review = run_market_close_flow(
            runtime,
            notifier=notifier,
            telegram_settings=telegram_settings,
            timestamp=context.scheduled_at,
            triggered_at=context.triggered_at,
        )
        return render_market_close_summary(result, weekly_review)

    return ScheduledJob(
        name=base_job.name,
        phase=base_job.phase,
        handler=handler,
    )


def build_safe_stop_cleanup_handler(
    runtime: MarketCloseRuntime,
    *,
    notifier,
    telegram_settings: TelegramSettings,
    run_market_close_flow: Callable[
        ..., tuple[MarketCloseResult, WeeklyReviewExecution | None]
    ],
    render_market_close_summary: Callable[
        [MarketCloseResult, WeeklyReviewExecution | None], str
    ],
):
    def handler(context: SafeStopContext) -> str:
        result, weekly_review = run_market_close_flow(
            runtime,
            notifier=notifier,
            telegram_settings=telegram_settings,
            timestamp=context.triggered_at,
            safe_stop_reason=context.reason,
            safe_stop_detail=context.detail,
        )
        return render_market_close_summary(result, weekly_review)

    return handler


def run_market_close_flow(
    runtime: MarketCloseRuntime,
    *,
    notifier,
    telegram_settings: TelegramSettings,
    timestamp: datetime,
    maybe_create_weekly_review: Callable[..., WeeklyReviewExecution | None],
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

    weekly_review = maybe_create_weekly_review(
        log_dir=runtime.settings.log_dir,
        trading_day=result.trading_day,
        generated_at=result.generated_at,
        notifier=notifier,
        telegram_settings=telegram_settings,
        calendar=runtime.calendar,
    )
    return result, weekly_review


def maybe_create_weekly_review(
    *,
    log_dir: Path,
    trading_day: date,
    generated_at: datetime,
    notifier,
    telegram_settings: TelegramSettings,
    calendar: KrxRegularSessionCalendar,
    logger: logging.Logger,
    is_last_trading_day_of_week: Callable[[date, KrxRegularSessionCalendar], bool],
    build_and_write_weekly_review: Callable[..., WeeklyReviewExecution],
    publish_weekly_review_alert: Callable[..., object],
) -> WeeklyReviewExecution | None:
    if not is_last_trading_day_of_week(trading_day, calendar):
        logger.info(
            "이번 거래일은 주 종료일이 아니므로 주간 리뷰를 건너뜁니다. trading_day=%s",
            trading_day.isoformat(),
        )
        return None

    logger.info(
        "주 종료일이라 주간 리뷰를 생성합니다. trading_day=%s",
        trading_day.isoformat(),
    )
    weekly_review = build_and_write_weekly_review(
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


def build_and_write_weekly_review(
    *,
    log_dir: Path,
    generated_at: datetime,
    build_weekly_review_report: Callable[..., WeeklyReviewReport],
    load_daily_run_reports: Callable[..., tuple[object, ...]],
    load_daily_inspection_reports: Callable[..., tuple[object, ...]],
    write_weekly_review_report: Callable[[Path, WeeklyReviewReport], Path],
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


def is_last_trading_day_of_week(
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


def render_market_close_summary(
    result: MarketCloseResult,
    weekly_review: WeeklyReviewExecution | None,
) -> str:
    summary = result.render_summary()
    if weekly_review is None:
        return f"{summary} weekly_review=skipped"
    return f"{summary} weekly_review={weekly_review.report_path.name}"


def collection_window_start(timeframe: Timeframe, generated_at: datetime) -> datetime:
    if timeframe is Timeframe.DAY:
        return generated_at - timedelta(days=180)
    return generated_at - timedelta(days=21)
