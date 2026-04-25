# AGENTS.md

## Project
AutoTrade automation/validation tooling. Prioritize correctness, reproducibility, and safe refactoring.

## Scope
- Editable: `src/`, `tools/`, `tests/`, `docs/`.
- Do not edit vendored/generated/third-party files.
- Do not change CI, dependency versions, or public interfaces unless required.

## Workflow
1. Read relevant files before editing.
2. Make the smallest sufficient change.
3. Run focused validation first, then broader validation if it passes.
4. Summarize changes, validation, and remaining risk.
5. See `docs/workflow.md` for the detailed flow.
6. If roadmap-related, update `docs/roadmap.md`.

## Large-Scope Coding Tasks
Use a 3-role flow before implementation when a coding task spans multiple files/modules, changes shared/core logic, adds behavior, changes architecture/interfaces/dependencies, or likely needs multiple edit/test cycles.

1. planner/manager: understand context; define scope, affected files, risks, dependencies, implementation steps, and validation.
2. coder: implement only the approved plan; keep scope minimal; stop if the plan becomes invalid.
3. review/tester: verify plan match, regressions, scope discipline, and required checks; reject incomplete validation or unrelated changes.

Order: planner/manager -> coder -> review/tester. Do not skip planning, self-approve coder work, or finish before review/tester passes.

Large-task outputs:
- Before coding: task summary, affected files/modules, implementation steps, risks, validation plan.
- After coding: review summary, validation commands, pass/fail status, remaining risks/follow-ups.

## Validation
- Lint: `ruff check .`
- Format: `ruff format .`
- Type check: `mypy src/`
- Unit tests: `pytest tests/unit -q`

Rules:
- Small edits: run the nearest relevant test first.
- Python changes: lint -> type check -> tests.
- Report exact commands run; do not claim success without them.

## Architecture
- Keep parsing, transformation, and I/O separated.
- Keep business logic out of CLI entrypoints.
- Shared utilities belong in `src/common/`.
- Do not bypass public interfaces with cross-module internal imports.

## Code Conventions
- Prefer small pure functions.
- Keep diffs minimal and names stable unless change is justified.
- Add comments only for non-obvious reasoning.

## Safety
- Never overwrite user data without explicit backup path or dry-run mode.
- Prefer deterministic scripts for file transforms.
- Modify generators, not large JSON/YAML/generated artifacts.
- Do not read `.env`.

## Pitfalls
- Do not edit generated files directly.
- Do not skip validation because a change looks small.
- Do not mix refactoring with behavior changes.
- Do not add dependencies unless necessary.

## References
- Architecture: `docs/architecture.md`
- Coding standards: `docs/coding-standards.md`
- Testing: `docs/testing.md`

## When Blocked
- State the exact blocker.
- Propose the smallest safe next step.
- Do not guess hidden requirements.

## Token Saving
- Be concise.
- Do not print full files/diffs unless asked.
- Prefer `rg`.
- Summarize only relevant test errors.

## graphify
Knowledge graph output lives in `graphify-out/`.

- Before architecture/codebase answers, read `graphify-out/GRAPH_REPORT.md` for god nodes and community structure.
- If `graphify-out/wiki/index.md` exists, navigate it before raw files.
- After modifying code files, run `.venv/bin/python -m graphify update .` to refresh the AST-only graph.
