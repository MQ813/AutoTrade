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

## Scheduler

`src/autotrade/scheduler/runtime.py`

- KRX 정규장 기준으로 장 시작, 장중, 장마감 실행 슬롯을 계산합니다.
- 작업은 `ScheduledJob`과 `MarketSessionPhase`로 선언합니다.
- 실행 결과는 `JobRunResult`로 정규화하고, 중복 실행 방지는 `SchedulerState`가 담당합니다.
- 다음 실행 시각은 마지막 작업 완료 시각 이후의 다음 세션 슬롯으로 계산합니다.

## Report

`src/autotrade/report/operations.py`

- 스케줄러 실행 결과를 운영 로그 항목으로 변환합니다.
- 일일 실행 결과를 phase별 요약이 포함된 텍스트 리포트로 렌더링합니다.
- 알림은 `Notifier` protocol로 분리해 전송 채널 의존성을 모듈 밖으로 유지합니다.
- 백테스트 리포트는 기존 `src/autotrade/report/backtest.py`를 그대로 유지하고, 운영 리포트와 분리합니다.

이 구조는 파싱, 스케줄 판단, 실행, 출력, 파일 기록을 분리해 이후 실제 실행기와 알림 전송 어댑터를 안전하게 연결하는 것을 목표로 합니다.
