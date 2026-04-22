from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Literal

from autotrade.risk import RiskSettings

BrokerEnvironment = Literal["paper", "live"]
PaperTradingMode = Literal["simulate", "broker"]


@dataclass(frozen=True, slots=True)
class BrokerSettings:
    provider: str
    api_key: str
    api_secret: str
    account: str
    environment: BrokerEnvironment = "paper"
    paper_trading_mode: PaperTradingMode = "simulate"
    hts_id: str | None = None

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
        if self.paper_trading_mode not in {"simulate", "broker"}:
            raise ValueError("paper_trading_mode must be 'simulate' or 'broker'")
        if self.hts_id is not None and not self.hts_id.strip():
            raise ValueError("hts_id must not be blank when provided")


@dataclass(frozen=True, slots=True)
class TelegramSettings:
    enabled: bool = False
    bot_token: str | None = None
    chat_id: str | None = None
    warning_chat_id: str | None = None
    error_chat_id: str | None = None
    max_retries: int = 3
    timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        for field_name in (
            "bot_token",
            "chat_id",
            "warning_chat_id",
            "error_chat_id",
        ):
            value = getattr(self, field_name)
            if value is not None and not value.strip():
                raise ValueError(f"{field_name} must not be blank")
        if self.enabled:
            if self.bot_token is None:
                raise ValueError("bot_token is required when telegram is enabled")
            if self.chat_id is None:
                raise ValueError("chat_id is required when telegram is enabled")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


@dataclass(frozen=True, slots=True)
class AppSettings:
    broker: BrokerSettings
    target_symbols: tuple[str, ...]
    log_dir: Path
    risk: RiskSettings = field(default_factory=RiskSettings)
    telegram: TelegramSettings = field(default_factory=TelegramSettings)

    def __post_init__(self) -> None:
        if not self.target_symbols:
            raise ValueError("target_symbols must not be empty")
