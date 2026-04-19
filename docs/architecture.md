# Architecture Mapping

`docs/roadmap.md`의 루트 모듈을 `src/autotrade/` 하위 패키지로 매핑합니다.

- `config/` -> `src/autotrade/config/`
- `data/` -> `src/autotrade/data/`
- `strategy/` -> `src/autotrade/strategy/`
- `risk/` -> `src/autotrade/risk/`
- `broker/` -> `src/autotrade/broker/`
- `execution/` -> `src/autotrade/execution/`
- `portfolio/` -> `src/autotrade/portfolio/`
- `scheduler/` -> `src/autotrade/scheduler/`
- `report/` -> `src/autotrade/report/`
- 공통 유틸리티 -> `src/autotrade/common/`

현재 구현은 모든 루트 모듈을 완성한 상태는 아니지만, `scheduler`와 `report`의 운영 기본 경계를 포함합니다.

## Portfolio

`src/autotrade/portfolio/backtest.py`

- 백테스트 포트폴리오 상태는 `BacktestPortfolioState`로 관리합니다.
- 상태에는 초기 현금, 현재 현금, 보유 수량, 평균단가, 포지션 원가, 실현 손익을 저장합니다.
- 스냅샷은 종가 기준으로 현금, 평균단가, 평가금액, 실현 손익, 평가 손익, 총 손익, 총 자산을 계산합니다.
- `execution.backtest`는 주문 신호와 체결 시점 제어만 담당하고, 잔고/평단/손익 계산은 `portfolio` 모듈로 위임합니다.

## Broker

`src/autotrade/broker/paper.py`

- `PaperBroker`는 `BrokerReader`와 `BrokerTrader`를 함께 구현하는 결정적 모의 브로커입니다.
- 현재 바(`Bar`)를 기준으로 현재가, 보유 수량, 주문 가능 수량을 계산하고 지정가 주문의 접수/정정/취소/체결을 재현합니다.
- 스냅샷(`PaperBrokerSnapshot`)을 통해 보유 상태, 주문 상태, 현재 시장 바를 복구할 수 있습니다.

## Execution Replay

`src/autotrade/execution/replay.py`

- `ReplaySession`은 과거 바 시퀀스를 따라 `scheduler` 작업을 재실행하는 운영 리플레이 경계입니다.
- 세션 스냅샷은 모의 브로커 상태와 스케줄러 실행 상태를 함께 저장해 재시작 테스트에 사용합니다.
- 로그 엔트리는 각 리플레이 시점의 종가, 실행된 작업, 세션 스냅샷을 담아 마지막 로그만으로 상태 복구 가능 여부를 검증할 수 있게 합니다.

## Scheduler

`src/autotrade/scheduler/runtime.py`

- KRX 정규장 기준으로 장 시작, 장중, 장마감 실행 슬롯을 계산합니다.
- 작업은 `ScheduledJob`과 `MarketSessionPhase`로 선언합니다.
- 실행 결과는 `JobRunResult`로 정규화하고, 중복 실행 방지는 `SchedulerState`가 담당합니다.
- 다음 실행 시각은 마지막 작업 완료 시각 이후의 다음 세션 슬롯으로 계산합니다.

## Report

`src/autotrade/report/operation_models.py`

- 운영 리포트 모델과 `Notifier` protocol은 모델 모듈에 두고, 데이터 유효성 검증을 여기서 고정합니다.

`src/autotrade/report/operation_builders.py`

- 일일 점검, 일일 실행, 주간 리뷰 집계를 만드는 계산 로직만 담당합니다.

`src/autotrade/report/operation_renderers.py`

- 텍스트 리포트 렌더링만 담당해 모델 계산과 파일 기록을 분리합니다.

`src/autotrade/report/operation_storage.py`

- JSON archive, job history, 텍스트 파일 기록을 담당합니다.

`src/autotrade/report/operation_alerts.py`

- 주문/체결/일일/주간 알림 message 조립과 notifier 발행만 담당합니다.

`src/autotrade/report/operations.py`

- 기존 import 호환용 facade이며, 위 모듈의 public helper를 재수출합니다.
- 백테스트 리포트는 기존 `src/autotrade/report/backtest.py`를 그대로 유지하고, 운영 리포트와 분리합니다.

## Runtime Operations

`src/autotrade/runtime/operations.py`

- CLI handler와 호환용 private export만 유지합니다.

`src/autotrade/runtime/operation_environment.py`

- `.env` 파싱과 셸 환경 병합, 설정 로딩 오류 메시지를 담당합니다.

`src/autotrade/runtime/operation_services.py`

- notifier, broker, live runtime 조립을 담당합니다.

`src/autotrade/runtime/operation_flows.py`

- 바 수집, live-cycle orchestration, market-close 후 주간 리뷰 생성 흐름을 담당합니다.

이 구조는 파싱, 스케줄 판단, 실행, 출력, 파일 기록을 분리해 이후 실제 실행기와 알림 전송 어댑터를 안전하게 연결하는 것을 목표로 합니다.
