# AGENTS.md

## Project summary
This repository contains the automation and validation tooling for AutoTrade.
Primary goals are correctness, reproducibility, and safe refactoring.

## Scope
- You may modify files under `src/`, `tools/`, `tests/`, and `docs/`.
- Do not edit vendored, generated, or third-party files directly.
- Do not change CI, dependency versions, or public interfaces unless the task requires it.

## Required workflow
1. Read the relevant files before editing.
2. Make the smallest change that solves the task.
3. Run focused validation first.
4. Run broader validation if the focused checks pass.
5. Summarize what changed, what was validated, and any remaining risk.
6. Refer docs/workflow.md for detailed flow.

## Multi-agent workflow for large changes

For any large-scope coding task, do not proceed with a single-agent flow.
You must use the following 3-agent group workflow before implementation begins:

1. planner/manager
2. coder
3. review/tester

A task is considered large-scope if it includes one or more of the following:
- changes across multiple files or modules
- refactoring of shared/core logic
- new feature implementation affecting existing behavior
- architecture, interface, or dependency changes
- work expected to require multiple edit/test cycles

### Role responsibilities

#### planner/manager
- Understand the request and relevant codebase context first.
- Break the task into smaller steps with clear scope boundaries.
- Identify affected files, risks, dependencies, and validation strategy.
- Produce an implementation plan before coding starts.
- Prevent unnecessary scope expansion.

#### coder
- Implement only the approved plan.
- Keep changes minimal and scoped to the task.
- Do not silently change architecture, interfaces, or unrelated logic.
- If the plan becomes invalid during implementation, stop and send the issue back to planner/manager.

#### review/tester
- Review correctness, regressions, and scope discipline.
- Check whether the implementation matches the original plan.
- Run required lint, type, build, and test commands.
- Report failures, risks, and missing coverage clearly.
- Reject completion if validation is incomplete or if unrelated changes were introduced.

### Required execution order
1. planner/manager creates the plan
2. coder implements the planned change
3. review/tester reviews and validates the result

Do not skip planner/manager for large-scope tasks.
Do not let coder self-approve completion without review/tester validation.
Do not mark the task complete until review/tester passes the required checks.

### Output requirements for large tasks
Before coding, planner/manager must provide:
- task summary
- affected files/modules
- implementation steps
- risk points
- validation plan

After coding, review/tester must provide:
- review summary
- validation commands run
- pass/fail status
- remaining risks or follow-ups

## Validation commands
- Lint: `ruff check .`
- Format: `ruff format .`
- Type check: `mypy src/`
- Unit tests: `pytest tests/unit -q`

## Validation order
- For small edits, run the nearest relevant test first.
- For Python code changes, run lint -> type check -> tests in that order.
- Do not claim success without reporting the exact commands run.

## Architecture constraints
- Keep parsing, transformation, and I/O layers separated.
- Do not add business logic to CLI entrypoints.
- Shared utilities belong in `src/common/`.
- Avoid cross-module imports that bypass public interfaces.

## Code conventions
- Prefer small pure functions over large stateful functions.
- Keep diffs minimal.
- Preserve existing naming unless there is a strong reason to change it.
- Add comments only when the reasoning is not obvious from the code.

## Safety rules
- Never overwrite user data without an explicit backup path or dry-run mode.
- For file transforms, prefer deterministic scripts over manual bulk edits.
- For large JSON/YAML/generated artifacts, modify the generator, not the output.

## Common pitfalls
- Do not edit generated files directly.
- Do not skip validation because a change “looks small”.
- Do not mix refactoring with behavior changes in one patch.
- Do not introduce new dependencies without necessity.

## Task-specific references
- Architecture: `docs/architecture.md`
- Coding standards: `docs/coding-standards.md`
- Testing guide: `docs/testing.md`

## When blocked
- Report the exact blocker.
- Propose the smallest safe next step.
- Do not guess hidden requirements.