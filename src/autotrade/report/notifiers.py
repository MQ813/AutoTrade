from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from autotrade.report.operations import NotificationMessage


@dataclass(slots=True)
class FileNotifier:
    path: Path

    def __post_init__(self) -> None:
        if self.path.exists() and self.path.is_dir():
            raise ValueError("path must point to a file")

    def send(self, notification: NotificationMessage) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "created_at": notification.created_at.isoformat(),
            "severity": notification.severity.value,
            "subject": notification.subject,
            "body": notification.body,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
