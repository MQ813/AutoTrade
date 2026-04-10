# AGENTS.md

## Project summary
This repository contains the automation and validation tooling for <project>.
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