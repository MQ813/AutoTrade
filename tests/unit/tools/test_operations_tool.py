from __future__ import annotations

import importlib.util
from pathlib import Path


def test_main_delegates_to_package_cli(monkeypatch) -> None:
    module = _load_operations_module()
    captured: list[bool] = []

    monkeypatch.setattr(
        module,
        "cli_main",
        lambda: captured.append(True) or 0,
    )

    assert module.main() == 0
    assert captured == [True]


def _load_operations_module():
    root = Path(__file__).resolve().parents[3]
    module_path = root / "tools" / "operations.py"
    spec = importlib.util.spec_from_file_location("operations_tool", module_path)
    if spec is None or spec.loader is None:
        raise AssertionError("failed to create module spec for operations.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
