# `cli run-continuous` Flow

Mermaid overview for `python -m autotrade.cli run-continuous`.

## 1. Top-Level Flow

```mermaid
flowchart TD
    A[python -m autotrade.cli run-continuous] --> B[Set logging]
    B --> C[Parse args<br/>strategy env-file bar-root paper-cash max-iterations]
    C --> D[Merge .env + shell environment]
    D --> E{Config loaded?}
    E -->|No| E1[Log error<br/>exit 2]
    E -->|Yes| F[Build notifier, states, broker, runtime]
    F --> G{paper env?}
    G -->|Yes| H[Build PaperBroker<br/>query KIS cash if paper_cash missing]
    G -->|No| I[Build KIS reader/trader]
    H --> J[Build MarketOpenPreparationRuntime]
    I --> J
    J --> K[Build LiveCycleRuntime]
    K --> L[Build MarketCloseRuntime]
    L --> M[Build ScheduledRunner<br/>with runner_control + Telegram control]
    M --> N[Register jobs<br/>market_open_prepare<br/>live_cycle<br/>market_close_cleanup]
    N --> O[runner.run_forever]
    O --> P{Runner status}
    P -->|completed/stopped| Q[Print state/artifact paths<br/>exit 0]
    P -->|safe_stop| R[Print stop reason<br/>exit 1]
```

## 2. Scheduler Loop

```mermaid
flowchart TD
    A[run_forever loop] --> B[run_once]
    B --> B1{runner_control paused?}
    B1 -->|Yes| B2[Wait and poll CLI/Telegram control]
    B2 --> B3{resumed?}
    B3 -->|No| B2
    B3 -->|Yes| B4[Run resume maintenance<br/>data catch-up + sync + missed close cleanup]
    B4 --> C
    B1 -->|No| C[Load scheduler_state for trading day]
    C --> D[Find due jobs up to now]
    D --> E[Run jobs for unrun slots<br/>skip slots <= resumed_at after pause]
    E --> F[Save scheduler_state]
    F --> G[Write job_history/run_log]
    G --> H{Any failed job?}
    H -->|No| I{max_iterations reached?}
    I -->|Yes| J[RunnerStatus.COMPLETED]
    I -->|No| K[Sleep until next_run_at] --> A
    H -->|Yes| L[Run safe-stop cleanup]
    L --> M[Publish safe-stop alert]
    M --> N[RunnerStatus.SAFE_STOP]
```

## 2.1 Control Commands

```mermaid
flowchart TD
    A[CLI control pause/resume<br/>or Telegram /pause /resume] --> B[Update runner_control.json]
    B --> C[run-continuous polls during sleep]
    C --> D{paused?}
    D -->|Yes| E[Do not start later scheduler jobs]
    E --> F{resume observed?}
    F -->|Yes| G[Refresh bars]
    G --> H[Sync open orders/fills only]
    H --> I{pause crossed market close?}
    I -->|Yes| J[Run market_close_cleanup once<br/>timestamp=missed close]
    I -->|No| K[Continue scheduler]
    J --> K
    K --> L[Skip delayed trading slots<br/>scheduled_at <= resumed_at]
```

## 3. Phases and Artifacts

```mermaid
flowchart LR
    A[Pre-open<br/>market_open_prepare] --> B[Intraday<br/>live_cycle]
    B --> C[Close<br/>market_close_cleanup]
    A --> A1[Daily inspection<br/>pre-open smoke report]
    B --> B1[Bar CSV<br/>order state<br/>order/fill alerts]
    C --> C1[Daily run report<br/>daily inspection<br/>next-day prep]
    C --> C2{Last trading day of week?}
    C2 -->|Yes| C3[Weekly review]
    C2 -->|No| C4[No weekly review]
```

## 4. Pre-Open Detail

```mermaid
flowchart TD
    A[08:00 MARKET_OPEN slot] --> B[MarketOpenPreparationRuntime.run]
    B --> C[Check/refresh strategy input bars]
    C --> D[Run read-only broker smoke]
    D --> E[Summarize previous operations errors]
    E --> F[Generate current-price strategy preview]
    F --> G[Build inspection items]
    G --> H[Save daily inspection report]
    H --> I[Publish pre-open summary<br/>file + Telegram]
    I --> J{Success?}
    J -->|Yes| K[Wait for next INTRADAY slot]
    J -->|No| L[Record failed job] --> M[Enter safe stop]
```

## 5. Intraday Detail

```mermaid
flowchart TD
    A[INTRADAY slot] --> B[Run live_cycle handler]
    B --> C[_collect_strategy_bars]
    C --> D[Refresh strategy-period bar CSV]
    D --> E[LiveCycleRuntime.run]
    E --> F[Per-symbol loop]
    F --> G[Sync/cancel pending orders if needed]
    G --> H[Calculate signal]
    H --> I[Run risk checks]
    I --> J{Order allowed?}
    J -->|No| K[Risk-block alert]
    J -->|Yes| L[Submit order / sync fills]
    L --> M[Order/fill alerts]
    K --> N[Return summary]
    M --> N
```

## 6. Close and Reports

```mermaid
flowchart TD
    A[MARKET_CLOSE slot] --> B[MarketCloseRuntime.run]
    B --> C[Aggregate job results, orders, holdings]
    C --> D[Build/save daily run report]
    D --> E[Publish daily run report alert]
    E --> F[Save next-day prep file]
    F --> G[Build close inspection items]
    G --> H[Save daily inspection report]
    H --> I{Last trading day of week?}
    I -->|No| J[Skip weekly review]
    I -->|Yes| K[Build/save weekly review]
    K --> L{Telegram enabled?}
    L -->|Yes| M[Publish weekly review]
    L -->|No| N[File artifacts only]
    J --> O[Return summary]
    M --> O
    N --> O
```

## 7. Safe Stop Cleanup

```mermaid
flowchart TD
    A[Job failure or runner exception] --> B[Build safe-stop context]
    B --> C[MarketCloseRuntime.run_safe_stop_cleanup]
    C --> D[Save daily run report]
    D --> E[Save daily inspection report]
    E --> F[Save next-day prep file]
    F --> G{Last trading day of week?}
    G -->|No| H[Skip weekly review]
    G -->|Yes| I[Build weekly review and optional alert]
    H --> J[Publish safe-stop alert]
    I --> J
```
