from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from collections.abc import Callable
from pathlib import Path

from autotrade.broker.korea_investment import HttpTransport
from autotrade.broker.korea_investment import KoreaInvestmentBrokerReader
from autotrade.common import Holding
from autotrade.common import OrderCapacity
from autotrade.common import Quote
from autotrade.config.models import AppSettings


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
) -> SmokeReport:
    now = clock or (lambda: datetime.now(timezone.utc))
    started_at = now()
    target_symbol = settings.target_etfs[0]
    broker = KoreaInvestmentBrokerReader(
        settings.broker,
        transport=transport,
        clock=now,
    )
    steps: list[SmokeStep] = [
        SmokeStep(name="smoke", status="start", detail=target_symbol)
    ]

    quote: Quote | None = None
    holdings: tuple[Holding, ...] | None = None
    order_capacity: OrderCapacity | None = None

    try:
        steps.append(
            SmokeStep(name="get_quote", status="start", detail=target_symbol),
        )
        quote = broker.get_quote(target_symbol)
        steps.append(
            SmokeStep(
                name="get_quote",
                status="success",
                detail=f"{quote.symbol}:{quote.price}",
            ),
        )

        steps.append(
            SmokeStep(name="get_holdings", status="start"),
        )
        holdings = broker.get_holdings()
        steps.append(
            SmokeStep(
                name="get_holdings",
                status="success",
                detail=str(len(holdings)),
            ),
        )

        steps.append(
            SmokeStep(
                name="get_order_capacity",
                status="start",
                detail=f"{quote.symbol}:{quote.price}",
            ),
        )
        order_capacity = broker.get_order_capacity(quote.symbol, quote.price)
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
