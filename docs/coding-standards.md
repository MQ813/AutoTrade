# Coding Standards

## Purpose

Define AutoTrade code rules for correctness, reproducibility, testability, and safe refactoring. Style consistency is secondary.

## Principles

- Solve with the smallest change.
- Keep each function/module focused.
- Prefer pure functions, explicit inputs, and return values over hidden state.
- Optimize for validation and operational safety.
- Do not mix refactoring and behavior changes in one patch.

## Module Boundaries

- `config`: settings, environment, operating parameters
- `data`: market data collection, normalization, storage, validation
- `strategy`: signal and strategy rules
- `risk`: position/loss limits and orderable ranges
- `broker`: broker API wrappers and response normalization
- `execution`: order creation, state transitions, retries
- `portfolio`: holdings and internal position models
- `scheduler`: execution ordering and scheduling
- `report`: reports and operational output
- `common`: small stable utilities shared by modules

Rules:
- Do not mix parsing, transformation, and I/O in one function.
- Keep business logic out of CLI/entrypoints.
- Put shared utilities in `src/autotrade/common/`.
- Connect layers through public interfaces, not internal imports.

## Functions and Classes

- Prefer functions that take inputs and return values.
- Split long functions by step.
- Use intent-revealing names.
- Avoid Boolean flags that switch multiple behaviors.
- Do not mix abstraction levels in one function.
- Introduce classes only when state and responsibility are clear.
- Prefer `dataclass`-style structures for simple data bundles.

## Types and Data

- Add type hints to public and boundary functions.
- Use explicit data structures where possible.
- Limit broad types like `dict[str, Any]` to boundaries.
- Normalize broker responses, config, and report data early.
- If `None` is meaningful, document it or make it clear in the name.

## State and Time

- Inject time, randomness, and external responses.
- Avoid global state and implicit caches.
- Prefer deterministic results for identical inputs.
- Keep operational state mutations behind clear boundaries.

## Exceptions and Logging

- Do not ignore exceptions.
- Separate external-system failures from domain-rule violations.
- Prefer contextual logs at higher boundaries over noisy low-level logs.
- Pure logic should communicate through returns and exceptions.
- Retry only in `execution` or external I/O boundaries.

## Style

- Follow `ruff format .` and pass `ruff check .`.
- Use readable names over unnecessary abbreviations.
- Name constants, especially trading rules and operating limits.
- Comment only to explain non-obvious reasons.
- Preserve standard-library, third-party, local import ordering.
- Avoid cycles and new dependencies unless necessary.
- Depend on interfaces in tests where possible.

## I/O and Settings

- Centralize file, network, and environment access in boundary modules.
- Internal logic should use parsed values and standardized objects.
- Treat user data and operational artifacts conservatively before overwriting.
- Prefer deterministic scripts for bulk transforms.

## Tests

- Isolate new logic into testable units first.
- Cover strategy, risk, and order transitions with pure unit tests.
- Keep tests deterministic; avoid direct dependence on current time, randomness, or external APIs.
- Keep fixtures minimal and test names descriptive.
- Add a reproduction test for bug fixes when practical.

## Change Units

- One patch/commit should have one intent.
- Separate renames/structure cleanup from behavior changes.
- For large changes, fix boundaries and data models first.
- Avoid leaving the program broken between steps.

## Validation

For Python changes:

1. nearest relevant test
2. `ruff check .`
3. `mypy src/`
4. `pytest tests/unit -q`

For docs-only changes, automatic validation may not apply; summarize the scope and validation performed.
