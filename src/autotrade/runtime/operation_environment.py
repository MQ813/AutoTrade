from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from pathlib import Path

from autotrade.config import AppSettings
from autotrade.config import ConfigError
from autotrade.config import load_settings


def load_runtime_settings(
    env_file: Path,
    *,
    env_template_file: Path,
    logger: logging.Logger,
) -> AppSettings | None:
    environment = load_environment(
        env_file,
        env_template_file=env_template_file,
        logger=logger,
    )
    if environment is None:
        return None
    try:
        settings = load_settings(env=environment)
    except ConfigError as exc:
        logger.error("설정 로딩에 실패했습니다: %s", exc)
        logger.error(
            "템플릿을 참고해 .env 파일을 준비하세요: %s",
            env_template_file,
        )
        return None
    return settings


def load_environment(
    env_file: Path,
    *,
    env_template_file: Path,
    logger: logging.Logger,
    base_environment: Mapping[str, str] | None = None,
) -> dict[str, str] | None:
    try:
        return resolve_environment(
            base_environment or os.environ,
            env_file=env_file,
            env_template_file=env_template_file,
            logger=logger,
        )
    except ValueError as exc:
        logger.error(".env 파일을 읽는 중 문제가 발생했습니다: %s", exc)
        logger.error(
            "템플릿을 참고해 .env 형식을 확인하세요: %s",
            env_template_file,
        )
        return None


def resolve_environment(
    base_environment: Mapping[str, str],
    *,
    env_file: Path,
    env_template_file: Path,
    logger: logging.Logger,
) -> dict[str, str]:
    env_values = load_env_file(env_file)
    if env_file.exists():
        logger.info(
            ".env 파일을 읽었습니다. 경로=%s 항목수=%d",
            env_file,
            len(env_values),
        )
    else:
        logger.info(
            ".env 파일이 없어 현재 셸 환경만 사용합니다. 기본 경로=%s 템플릿=%s",
            env_file,
            env_template_file,
        )
    merged = dict(env_values)
    merged.update(base_environment)
    return merged


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    if not path.is_file():
        raise ValueError(f".env 경로가 파일이 아닙니다: {path}")

    parsed: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped.removeprefix("export ").strip()
        if "=" not in stripped:
            raise ValueError(f".env 형식이 잘못되었습니다: line {line_number}")
        key, value = stripped.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            raise ValueError(f".env 키가 비어 있습니다: line {line_number}")
        parsed[normalized_key] = strip_optional_quotes(value.strip())
    return parsed


def strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value
