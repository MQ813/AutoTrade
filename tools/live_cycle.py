from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Mapping
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autotrade.broker import KoreaInvestmentBrokerReader  # noqa: E402
from autotrade.broker import KoreaInvestmentBrokerTrader  # noqa: E402
from autotrade.broker import KoreaInvestmentBarSource  # noqa: E402
from autotrade.broker.korea_investment import KoreaInvestmentBrokerError  # noqa: E402
from autotrade.broker import PaperBroker  # noqa: E402
from autotrade.config import ConfigError  # noqa: E402
from autotrade.config import load_settings  # noqa: E402
from autotrade.data import CsvBarStore  # noqa: E402
from autotrade.data import CsvBarSource  # noqa: E402
from autotrade.data import KST  # noqa: E402
from autotrade.data import Timeframe  # noqa: E402
from autotrade.data import validate_bar_series  # noqa: E402
from autotrade.execution import FileExecutionStateStore  # noqa: E402
from autotrade.report import FileNotifier  # noqa: E402
from autotrade.runtime import LiveCycleRuntime  # noqa: E402
from autotrade.runtime import strategy_timeframe_for  # noqa: E402
from autotrade.strategy import StrategyKind  # noqa: E402
from autotrade.strategy import create_strategy  # noqa: E402

DEFAULT_ENV_FILE = ROOT / ".env"
ENV_TEMPLATE_FILE = ROOT / "docs" / "live_cycle.env.example"
logger = logging.getLogger(__name__)


def main() -> int:
    _configure_logging()
    parser = argparse.ArgumentParser(
        description="AutoTrade 운영 사이클을 한 번 실행합니다."
    )
    parser.add_argument(
        "--strategy",
        default=StrategyKind.THIRTY_MINUTE_TREND.value,
        choices=[kind.value for kind in StrategyKind],
        help="실행할 전략 종류입니다.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    parser.add_argument(
        "--bar-root",
        type=Path,
        default=None,
        help="CSV 바 데이터 루트 경로입니다. 기본값은 AUTOTRADE_LOG_DIR/bars 입니다.",
    )
    parser.add_argument(
        "--paper-cash",
        type=Decimal,
        default=Decimal("100000000"),
        help="AUTOTRADE_BROKER_ENV=paper 일 때 사용할 초기 현금입니다.",
    )
    args = parser.parse_args()

    logger.info("AutoTrade 운영 사이클 실행을 준비합니다.")
    try:
        environment = _resolve_environment(os.environ, env_file=args.env_file)
    except ValueError as exc:
        logger.error(".env 파일을 읽는 중 문제가 발생했습니다: %s", exc)
        logger.error(
            "템플릿을 참고해 .env 형식을 확인하세요: %s",
            ENV_TEMPLATE_FILE,
        )
        return 2
    try:
        settings = load_settings(env=environment)
    except ConfigError as exc:
        logger.error("설정 로딩에 실패했습니다: %s", exc)
        logger.error(
            "템플릿을 참고해 .env 파일을 준비하세요: %s",
            ENV_TEMPLATE_FILE,
        )
        return 2

    bar_root = args.bar_root or (settings.log_dir / "bars")
    strategy_kind = StrategyKind(args.strategy)
    bar_source = CsvBarSource(bar_root)
    notifier = FileNotifier(settings.log_dir / "notifications.jsonl")
    state_store = FileExecutionStateStore(settings.log_dir / "execution_state.json")
    logger.info(
        "설정을 불러왔습니다. 환경=%s 전략=%s 대상종목=%s",
        settings.broker.environment,
        strategy_kind.value,
        ",".join(settings.target_symbols),
    )
    logger.info("바 데이터 경로: %s", bar_root)
    logger.info("알림 파일 경로: %s", notifier.path)
    logger.info("주문 상태 파일 경로: %s", state_store.path)
    generated_at = datetime.now(KST)

    try:
        _collect_strategy_bars(
            settings,
            bar_root=bar_root,
            timeframe=strategy_timeframe_for(strategy_kind),
            generated_at=generated_at,
        )
    except (KoreaInvestmentBrokerError, ValueError) as exc:
        logger.error("바 데이터 수집에 실패했습니다: %s", exc)
        return 2

    if settings.broker.environment == "paper":
        logger.info(
            "내부 PaperBroker로 실행합니다. 초기 현금=%s",
            args.paper_cash,
        )
        broker = PaperBroker(initial_cash=args.paper_cash)
        broker_reader = broker
        broker_trader = broker
    else:
        logger.info("실브로커(KIS) 연동 객체를 초기화합니다.")
        broker_reader = KoreaInvestmentBrokerReader(settings.broker)
        broker_trader = KoreaInvestmentBrokerTrader(settings.broker)

    # CLI에서는 전략/브로커/알림/상태 저장소를 명시적으로 조립해
    # 한 번의 운영 사이클이 어떤 입력과 출력으로 실행되는지 드러낸다.
    runtime = LiveCycleRuntime(
        settings=settings,
        strategy=create_strategy(strategy_kind),
        timeframe=strategy_timeframe_for(strategy_kind),
        bar_source=bar_source,
        broker_reader=broker_reader,
        broker_trader=broker_trader,
        notifier=notifier,
        state_store=state_store,
    )
    logger.info("운영 사이클을 실행합니다.")
    result = runtime.run(timestamp=generated_at)
    logger.info("운영 사이클 실행이 끝났습니다.")

    print(result.render_korean_summary())
    print(f"알림 파일: {notifier.path}")
    print(f"주문 상태 파일: {state_store.path}")
    return 0


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def _resolve_environment(
    base_environment: Mapping[str, str],
    *,
    env_file: Path,
) -> dict[str, str]:
    env_values = _load_env_file(env_file)
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
            ENV_TEMPLATE_FILE,
        )
    # `.env`는 기본값을 제공하고, 이미 export된 셸 환경변수가 있으면 그 값이 우선한다.
    merged = dict(env_values)
    merged.update(base_environment)
    return merged


def _load_env_file(path: Path) -> dict[str, str]:
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
        parsed[normalized_key] = _strip_optional_quotes(value.strip())
    return parsed


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _collect_strategy_bars(
    settings,
    *,
    bar_root: Path,
    timeframe: Timeframe,
    generated_at: datetime,
) -> None:
    if settings.broker.provider != "koreainvestment":
        raise ValueError(
            "현재 자동 바 수집은 koreainvestment provider만 지원합니다."
        )

    window_start = _collection_window_start(timeframe, generated_at)
    bar_source = KoreaInvestmentBarSource(settings.broker)
    bar_store = CsvBarStore(bar_root)
    logger.info(
        "전략 입력 바 수집을 시작합니다. 시작=%s 종료=%s 주기=%s",
        window_start.isoformat(),
        generated_at.isoformat(),
        timeframe.value,
    )

    total_bars = 0
    for symbol in settings.target_symbols:
        logger.info("바 수집을 요청합니다. symbol=%s", symbol)
        bars = bar_source.load_bars(
            symbol,
            timeframe,
            start=window_start,
            end=generated_at,
        )
        validate_bar_series(bars)
        bar_store.store_bars(bars)
        total_bars += len(bars)
        logger.info(
            "바 수집을 마쳤습니다. symbol=%s bars=%d path=%s",
            symbol,
            len(bars),
            bar_root / timeframe.value / f"{symbol}.csv",
        )

    logger.info("전략 입력 바 수집을 완료했습니다. 총 바 수=%d", total_bars)


def _collection_window_start(timeframe: Timeframe, generated_at: datetime) -> datetime:
    if timeframe is Timeframe.DAY:
        return generated_at - timedelta(days=180)
    return generated_at - timedelta(days=21)


if __name__ == "__main__":
    raise SystemExit(main())
