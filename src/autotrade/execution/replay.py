from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from autotrade.broker.paper import PaperBroker
from autotrade.broker.paper import PaperBrokerSnapshot
from autotrade.data import Bar
from autotrade.scheduler import JobRunResult
from autotrade.scheduler import ScheduledJob
from autotrade.scheduler import SchedulerConfig
from autotrade.scheduler import SchedulerRun
from autotrade.scheduler import SchedulerState
from autotrade.scheduler import run_scheduled_jobs


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_positive_decimal(field_name: str, value: Decimal) -> None:
    if value <= Decimal("0"):
        raise ValueError(f"{field_name} must be positive")


@dataclass(frozen=True, slots=True)
class ReplaySessionSnapshot:
    broker_snapshot: PaperBrokerSnapshot
    scheduler_state: SchedulerState
    scheduler_config: SchedulerConfig | None = None
    last_timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.last_timestamp is not None:
            _require_aware_datetime("last_timestamp", self.last_timestamp)


@dataclass(frozen=True, slots=True)
class ReplayLogEntry:
    timestamp: datetime
    symbol: str
    close_price: Decimal
    executed_jobs: tuple[JobRunResult, ...]
    snapshot: ReplaySessionSnapshot

    def __post_init__(self) -> None:
        _require_aware_datetime("timestamp", self.timestamp)
        if not self.symbol.strip():
            raise ValueError("symbol must not be blank")
        _require_positive_decimal("close_price", self.close_price)


class ReplaySession:
    def __init__(
        self,
        broker: PaperBroker,
        *,
        scheduler_state: SchedulerState | None = None,
        scheduler_config: SchedulerConfig | None = None,
    ) -> None:
        self._broker = broker
        self._scheduler_state = scheduler_state or SchedulerState()
        self._scheduler_config = scheduler_config
        self._last_timestamp: datetime | None = None
        self._log_entries: list[ReplayLogEntry] = []

    @classmethod
    def from_snapshot(
        cls,
        snapshot: ReplaySessionSnapshot,
        *,
        scheduler_config: SchedulerConfig | None = None,
    ) -> ReplaySession:
        session = cls(
            PaperBroker.from_snapshot(snapshot.broker_snapshot),
            scheduler_state=snapshot.scheduler_state,
            scheduler_config=scheduler_config or snapshot.scheduler_config,
        )
        session._last_timestamp = snapshot.last_timestamp
        return session

    @property
    def broker(self) -> PaperBroker:
        return self._broker

    @property
    def log_entries(self) -> tuple[ReplayLogEntry, ...]:
        return tuple(self._log_entries)

    def snapshot(self) -> ReplaySessionSnapshot:
        return ReplaySessionSnapshot(
            broker_snapshot=self._broker.snapshot(),
            scheduler_state=self._scheduler_state,
            scheduler_config=self._scheduler_config,
            last_timestamp=self._last_timestamp,
        )

    def advance(
        self,
        bar: Bar,
        jobs: Sequence[ScheduledJob],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> SchedulerRun:
        self._broker.advance_bar(bar)
        resolved_clock = clock or (lambda: bar.timestamp)
        run = run_scheduled_jobs(
            jobs,
            timestamp=bar.timestamp,
            state=self._scheduler_state,
            config=self._scheduler_config,
            clock=resolved_clock,
        )
        self._scheduler_state = run.state
        self._last_timestamp = bar.timestamp
        self._log_entries.append(
            ReplayLogEntry(
                timestamp=bar.timestamp,
                symbol=bar.symbol,
                close_price=bar.close,
                executed_jobs=run.executed_jobs,
                snapshot=self.snapshot(),
            )
        )
        return run

    def run(
        self,
        bars: Sequence[Bar],
        jobs: Sequence[ScheduledJob],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> tuple[SchedulerRun, ...]:
        return tuple(self.advance(bar, jobs, clock=clock) for bar in bars)


def restore_replay_session_from_log(
    log_entries: Sequence[ReplayLogEntry],
    *,
    scheduler_config: SchedulerConfig | None = None,
) -> ReplaySession:
    if not log_entries:
        raise ValueError("log_entries must not be empty")
    return ReplaySession.from_snapshot(
        log_entries[-1].snapshot,
        scheduler_config=scheduler_config,
    )


def render_replay_log(log_entries: Sequence[ReplayLogEntry]) -> str:
    lines = []
    for entry in log_entries:
        job_names = ",".join(result.job_name for result in entry.executed_jobs) or "-"
        holdings = ",".join(
            f"{holding.symbol}:{holding.quantity}"
            for holding in entry.snapshot.broker_snapshot.holdings
        )
        lines.append(
            " ".join(
                (
                    f"timestamp={entry.timestamp.isoformat()}",
                    f"symbol={entry.symbol}",
                    f"close_price={entry.close_price}",
                    f"jobs={job_names}",
                    f"cash={entry.snapshot.broker_snapshot.cash}",
                    f"holdings={holdings or '-'}",
                )
            )
        )
    return "\n".join(lines) + ("\n" if lines else "")


def write_replay_log(
    log_dir: Path,
    log_entries: Sequence[ReplayLogEntry],
) -> Path:
    if not log_entries:
        raise ValueError("log_entries must not be empty")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = (
        log_dir
        / f"replay_{log_entries[-1].timestamp.strftime('%Y%m%d_%H%M%S_%f')}.log"
    )
    log_path.write_text(render_replay_log(log_entries), encoding="utf-8")
    return log_path
