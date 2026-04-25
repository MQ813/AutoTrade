# Development Workflow

Use this flow for AutoTrade changes. Each stage has a validation gate; on failure, record the reason and return to the nearest safe stage.

```mermaid
flowchart TD
    A[Start<br/>request received] --> B[Clarify goal, scope, done criteria]
    B --> C{Clear enough?}
    C -->|No| B1[Record gap<br/>missing requirement/scope/done criteria] --> B
    C -->|Yes| D[Read AGENTS.md, docs, affected code]
    D --> E{Files, boundaries, constraints known?}
    E -->|No| D1[Record gap<br/>files/boundaries/validation missing] --> D
    E -->|Yes| F{Large change?}
    F -->|Yes| G[planner/manager plan<br/>summary/files/steps/risks/validation]
    G --> H{Plan minimal and testable?}
    H -->|No| G1[Record issue<br/>scope too broad/steps unclear/checks weak] --> G
    H -->|Yes| I[coder implements approved scope]
    F -->|No| I
    I --> J{Scope and boundaries preserved?}
    J -->|No| D2[Record issue<br/>scope creep/boundary violation/refactor drift] --> D
    J -->|Yes| K[Run nearest validation]
    K --> L{Pass?}
    L -->|No| I1[Record failure<br/>test/doc/assumption mismatch] --> I
    L -->|Yes| M{Code changed?}
    M -->|Yes| N[Run ruff check .<br/>mypy src/]
    N --> O{Pass?}
    O -->|No| I2[Record lint/type failure] --> I
    O -->|Yes| P[Run pytest tests/unit -q]
    P --> Q{Pass?}
    Q -->|No| I3[Record regression/assumption/coverage gap] --> I
    Q -->|Yes| R{Large change?}
    M -->|No| S[Manual doc validation<br/>flow, commands, project rules]
    S --> T{Pass?}
    T -->|No| I4[Record doc mismatch/command error/weak basis] --> I
    T -->|Yes| R
    R -->|Yes| U[review/tester verifies plan, regressions, checks]
    U --> V{Review passes?}
    V -->|No| G2[Record plan drift/missing checks/unrelated change] --> G
    V -->|Yes| W[Summarize changes, validation, risks]
    R -->|No| W
    W --> X[Done]
```

## Validation Rules

- Validate the nearest relevant scope first.
- Python changes: `ruff check .` -> `mypy src/` -> `pytest tests/unit -q`.
- Docs-only changes may lack automatic checks; manually verify flow accuracy, command accuracy, and project-rule consistency, then state that in the summary.
- Record why a failure happened and which stage to revisit.

## Large-Change Gate

Use `planner/manager -> coder -> review/tester` when a change touches multiple files/modules, shared/core logic, existing behavior, or likely needs multiple implementation/validation cycles.
