# Architecture Mapping

`docs/roadmap.md` root modules map to `src/autotrade/` packages:

- `config/` -> `src/autotrade/config/`
- `data/` -> `src/autotrade/data/`
- `strategy/` -> `src/autotrade/strategy/`
- `risk/` -> `src/autotrade/risk/`
- `broker/` -> `src/autotrade/broker/`
- `execution/` -> `src/autotrade/execution/`
- `portfolio/` -> `src/autotrade/portfolio/`
- `scheduler/` -> `src/autotrade/scheduler/`
- `report/` -> `src/autotrade/report/`
- common utilities -> `src/autotrade/common/`

Not every roadmap root module is complete, but the current code defines the main operating boundaries for `scheduler` and `report`.

## Portfolio

`src/autotrade/portfolio/backtest.py`

- `BacktestPortfolioState` stores initial/current cash, quantity, average price, position cost, and realized PnL.
- Snapshots compute cash, average price, market value, realized/unrealized/total PnL, and total equity from close price.
- `execution.backtest` controls signals and fill timing; portfolio math stays in `portfolio`.

## Broker

`src/autotrade/broker/paper.py`

- `PaperBroker` implements `BrokerReader` and `BrokerTrader` as a deterministic simulated broker.
- It uses the current `Bar` for price, holdings, orderable quantity, and limit-order submit/modify/cancel/fill behavior.
- `PaperBrokerSnapshot` restores holdings, orders, and current market bar.

## Execution Replay

`src/autotrade/execution/replay.py`

- `ReplaySession` reruns scheduler jobs over historical bars.
- Snapshots store simulated broker and scheduler state for restart tests.
- Log entries include close price, executed jobs, and session snapshot so the last log can validate recovery.

## Scheduler

`src/autotrade/scheduler/runtime.py`

- Computes KRX regular-session open, intraday, and close slots.
- Jobs use `ScheduledJob` and `MarketSessionPhase`.
- `JobRunResult` normalizes results; `SchedulerState` prevents duplicate runs.
- Next run time is the next session slot after the latest completed job.

## Report

- `operation_models.py`: operation report models, `Notifier` protocol, validation.
- `operation_builders.py`: daily inspection/run and weekly review aggregation.
- `operation_renderers.py`: text rendering only.
- `operation_storage.py`: JSON archive, job history, text output.
- `operation_alerts.py`: order/fill/daily/weekly alert messages and notifier publishing.
- `operations.py`: compatibility facade re-exporting public helpers.
- `backtest.py`: kept separate from operation reports.

## Runtime Operations

- `runtime/operations.py`: CLI handler and compatibility private exports.
- `operation_environment.py`: `.env` parsing, shell environment merge, config-load errors.
- `operation_services.py`: notifier, broker, live runtime assembly.
- `operation_flows.py`: bar collection, live-cycle orchestration, market-close weekly review flow.

The structure separates parsing, scheduling, execution, output, and file storage so live executors and notification adapters can be attached safely.
