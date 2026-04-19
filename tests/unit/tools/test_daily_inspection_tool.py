from __future__ import annotations

import importlib.util
from pathlib import Path


def test_main_delegates_to_daily_inspection_compat(monkeypatch) -> None:
    module = _load_daily_inspection_module()
    captured: list[bool] = []

    monkeypatch.setattr(
        module,
        "main_daily_inspection_compat",
        lambda: captured.append(True) or 0,
    )

    assert module.main() == 0
    assert captured == [True]


def _load_daily_inspection_module():
    root = Path(__file__).resolve().parents[3]
    module_path = root / "tools" / "daily_inspection.py"
    spec = importlib.util.spec_from_file_location("daily_inspection_tool", module_path)
    if spec is None or spec.loader is None:
        raise AssertionError("failed to create module spec for daily_inspection.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
