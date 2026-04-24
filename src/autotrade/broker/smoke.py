from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import TypeVar

from autotrade.broker.korea_investment import HttpTransport
from autotrade.broker.korea_investment import KoreaInvestmentBrokerReader
from autotrade.common import Holding
from autotrade.common import OrderCapacity
from autotrade.common import Quote
from autotrade.config.models import AppSettings

_T = TypeVar("_T")
_KIS_RATE_LIMIT_ERROR_CODE = "EGW00201"
_SMOKE_RETRY_DELAYS_SECONDS = (1.1, 1.3)


@dataclass(frozen=True, slots=True)
class SmokeStep:
    name: str
    status: str
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class SmokeReport:
    started_at: datetime
    finished_at: datetime
    target_symbol: str
    steps: tuple[SmokeStep, ...]
    quote: Quote | None
    holdings: tuple[Holding, ...] | None
    order_capacity: OrderCapacity | None
    success: bool
    failure: str | None = None


def run_read_only_smoke(
    settings: AppSettings,
    *,
    transport: HttpTransport | None = None,
    clock: Callable[[], datetime] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> SmokeReport:
    now = clock or (lambda: datetime.now(timezone.utc))
    resolved_sleep = sleep or time.sleep
    started_at = now()
    target_symbol = settings.target_symbols[0]
    broker = KoreaInvestmentBrokerReader(
        settings.broker,
        transport=transport,
        clock=now,
        sleep=resolved_sleep,
    )
    steps: list[SmokeStep] = [
        SmokeStep(name="smoke", status="start", detail=target_symbol)
    ]

    quote: Quote | None = None
    holdings: tuple[Holding, ...] | None = None
    order_capacity: OrderCapacity | None = None

    try:
        quote = _run_smoke_step(
            "get_quote",
            detail=target_symbol,
            operation=lambda: broker.get_quote(target_symbol),
            steps=steps,
            sleep=resolved_sleep,
        )
        steps.append(
            SmokeStep(
                name="get_quote",
                status="success",
                detail=f"{quote.symbol}:{quote.price}",
            ),
        )

        holdings = _run_smoke_step(
            "get_holdings",
            operation=broker.get_holdings,
            steps=steps,
            sleep=resolved_sleep,
        )
        steps.append(
            SmokeStep(
                name="get_holdings",
                status="success",
                detail=str(len(holdings)),
            ),
        )

        order_capacity = _run_smoke_step(
            "get_order_capacity",
            detail=f"{quote.symbol}:{quote.price}",
            operation=lambda: broker.get_order_capacity(quote.symbol, quote.price),
            steps=steps,
            sleep=resolved_sleep,
        )
        steps.append(
            SmokeStep(
                name="get_order_capacity",
                status="success",
                detail=f"{order_capacity.symbol}:{order_capacity.max_orderable_quantity}",
            ),
        )
        steps.append(SmokeStep(name="smoke", status="success"))
        success = True
        failure = None
    except Exception as error:  # pragma: no cover - exercised in failure tests
        failed_step = "get_quote"
        if quote is not None and holdings is None:
            failed_step = "get_holdings"
        if quote is not None and holdings is not None:
            failed_step = "get_order_capacity"
        steps.append(
            SmokeStep(
                name=failed_step,
                status="failure",
                detail=str(error),
            ),
        )
        steps.append(
            SmokeStep(
                name="smoke",
                status="failure",
                detail=str(error),
            ),
        )
        success = False
        failure = str(error)

    finished_at = now()
    return SmokeReport(
        started_at=started_at,
        finished_at=finished_at,
        target_symbol=target_symbol,
        steps=tuple(steps),
        quote=quote,
        holdings=holdings,
        order_capacity=order_capacity,
        success=success,
        failure=failure,
    )


def _run_smoke_step(
    step_name: str,
    *,
    operation: Callable[[], _T],
    steps: list[SmokeStep],
    sleep: Callable[[float], None],
    detail: str | None = None,
) -> _T:
    steps.append(SmokeStep(name=step_name, status="start", detail=detail))
    for attempt, delay_seconds in enumerate(
        _SMOKE_RETRY_DELAYS_SECONDS,
        start=1,
    ):
        try:
            return operation()
        except Exception as error:
            if not _is_retryable_rate_limit_error(error):
                raise
            steps.append(
                SmokeStep(
                    name=step_name,
                    status="retry",
                    detail=(
                        f"attempt={attempt + 1} "
                        f"delay_seconds={delay_seconds:.1f} "
                        f"error={error}"
                    ),
                )
            )
            sleep(delay_seconds)
    return operation()


def _is_retryable_rate_limit_error(error: Exception) -> bool:
    return _KIS_RATE_LIMIT_ERROR_CODE in str(error)


def render_smoke_report(report: SmokeReport) -> str:
    lines = [
        f"started_at={report.started_at.isoformat()}",
        f"finished_at={report.finished_at.isoformat()}",
        f"target_symbol={report.target_symbol}",
        f"success={report.success}",
    ]
    if report.quote is not None:
        lines.append(f"quote={report.quote.symbol}:{report.quote.price}")
    if report.holdings is not None:
        symbols = ",".join(holding.symbol for holding in report.holdings)
        lines.append(f"holdings={len(report.holdings)}[{symbols}]")
    if report.order_capacity is not None:
        lines.append(
            "order_capacity="
            f"{report.order_capacity.symbol}:{report.order_capacity.max_orderable_quantity}"
        )
    if report.failure is not None:
        lines.append(f"failure={report.failure}")
    for step in report.steps:
        detail = f" detail={step.detail}" if step.detail is not None else ""
        lines.append(f"step={step.name} status={step.status}{detail}")
    return "\n".join(lines) + "\n"


def write_smoke_report(log_dir: Path, report: SmokeReport) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = (
        log_dir / f"broker_smoke_{report.started_at.strftime('%Y%m%d_%H%M%S_%f')}.log"
    )
    log_path.write_text(render_smoke_report(report), encoding="utf-8")
    return log_path
