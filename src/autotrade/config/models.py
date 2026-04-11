from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

BrokerEnvironment = Literal["paper", "live"]


@dataclass(frozen=True, slots=True)
class BrokerSettings:
    provider: str
    api_key: str
    api_secret: str
    account: str
    environment: BrokerEnvironment = "paper"

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("provider must not be blank")
        if not self.api_key.strip():
            raise ValueError("api_key must not be blank")
        if not self.api_secret.strip():
            raise ValueError("api_secret must not be blank")
        if not self.account.strip():
            raise ValueError("account must not be blank")
        if self.environment not in {"paper", "live"}:
            raise ValueError("environment must be 'paper' or 'live'")


@dataclass(frozen=True, slots=True)
class AppSettings:
    broker: BrokerSettings
    target_etfs: tuple[str, ...]
    log_dir: Path

    def __post_init__(self) -> None:
        if not self.target_etfs:
            raise ValueError("target_etfs must not be empty")
