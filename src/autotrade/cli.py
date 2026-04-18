from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

import autotrade.runtime.operations as operations
from autotrade.strategy import StrategyKind


def main(argv: Sequence[str] | None = None) -> int:
    operations._configure_logging()
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 2
    return handler(args)


def main_live_cycle_compat(argv: Sequence[str] | None = None) -> int:
    resolved_argv = list(sys.argv[1:] if argv is None else argv)
    if "--continuous" in resolved_argv:
        forwarded = [arg for arg in resolved_argv if arg != "--continuous"]
        return main(["run-continuous", *forwarded])
    return main(["run-once", *resolved_argv])


def main_weekly_review_compat(argv: Sequence[str] | None = None) -> int:
    resolved_argv = list(sys.argv[1:] if argv is None else argv)
    return main(["weekly-review", *resolved_argv])


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AutoTrade 운영 작업을 실행합니다."
    )
    subparsers = parser.add_subparsers(dest="command")

    run_once_parser = subparsers.add_parser(
        "run-once",
        help="장중 운영 사이클을 한 번 실행합니다.",
    )
    _add_runtime_arguments(run_once_parser)
    run_once_parser.set_defaults(handler=operations._handle_run_once)

    run_continuous_parser = subparsers.add_parser(
        "run-continuous",
        help="장전 준비, 장중 매매, 장종료 정리를 scheduler로 연속 실행합니다.",
    )
    _add_runtime_arguments(run_continuous_parser)
    run_continuous_parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="continuous 모드에서 scheduler 평가 횟수를 제한합니다.",
    )
    run_continuous_parser.set_defaults(handler=operations._handle_run_continuous)

    market_open_parser = subparsers.add_parser(
        "market-open",
        help="장전 준비 점검만 실행합니다.",
    )
    market_open_parser.add_argument(
        "--strategy",
        default=StrategyKind.THIRTY_MINUTE_TREND.value,
        choices=[kind.value for kind in StrategyKind],
        help="장전 준비에서 사용할 전략 종류입니다.",
    )
    market_open_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    market_open_parser.set_defaults(handler=operations._handle_market_open)

    market_close_parser = subparsers.add_parser(
        "market-close",
        help="장종료 정리와 주간 리뷰 후처리를 실행합니다.",
    )
    market_close_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    market_close_parser.add_argument(
        "--paper-cash",
        type=Decimal,
        default=None,
        help=(
            "AUTOTRADE_BROKER_ENV=paper 일 때 내부 PaperBroker 초기 현금을 "
            "수동 지정합니다. 지정하지 않으면 KIS paper 주문가능현금을 사용합니다."
        ),
    )
    market_close_parser.set_defaults(handler=operations._handle_market_close)

    weekly_review_parser = subparsers.add_parser(
        "weekly-review",
        help="주간 리뷰 파일을 생성하고 필요하면 알림을 발행합니다.",
    )
    weekly_review_parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
        help="설정에 사용할 .env 파일 경로입니다. 기본값은 저장소 루트의 .env입니다.",
    )
    weekly_review_parser.set_defaults(handler=operations._handle_weekly_review)

    return parser


def _add_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--strategy",
        default=StrategyKind.THIRTY_MINUTE_TREND.value,
        choices=[kind.value for kind in StrategyKind],
        help="실행할 전략 종류입니다.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=operations.DEFAULT_ENV_FILE,
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
        default=None,
        help=(
            "AUTOTRADE_BROKER_ENV=paper 일 때 내부 PaperBroker 초기 현금을 "
            "수동 지정합니다. 지정하지 않으면 KIS paper 주문가능현금을 사용합니다."
        ),
    )


__all__ = [
    "main",
    "main_live_cycle_compat",
    "main_weekly_review_compat",
]


if __name__ == "__main__":
    raise SystemExit(main())
