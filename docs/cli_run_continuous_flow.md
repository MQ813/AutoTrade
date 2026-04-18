# `cli run-continuous` Flow

이 문서는 `python -m autotrade.cli run-continuous` 실행 흐름을 Mermaid로 정리합니다.

## 1. 상위 실행 흐름

```mermaid
flowchart TD
    A[python -m autotrade.cli run-continuous] --> B[로깅 설정]
    B --> C[CLI 인자 파싱<br/>strategy env-file bar-root paper-cash max-iterations]
    C --> D[.env + 셸 환경 병합]
    D --> E{설정 로드 성공?}
    E -->|아니오| E1[오류 로그 출력 후 종료 code 2]
    E -->|예| F[notifier execution_state scheduler_state broker runtime 구성]
    F --> G{paper 환경인가?}
    G -->|예| H[PaperBroker 구성<br/>paper_cash 미지정 시 KIS 주문가능현금 조회]
    G -->|아니오| I[KIS reader / trader 구성]
    H --> J[MarketOpenPreparationRuntime 구성]
    I --> J
    J --> K[LiveCycleRuntime 구성]
    K --> L[MarketCloseRuntime 구성]
    L --> M[ScheduledRunner 구성]
    M --> N[jobs 등록<br/>market_open_prepare<br/>live_cycle<br/>market_close_cleanup]
    N --> O[runner.run_forever 실행]
    O --> P{runner 상태}
    P -->|completed| Q[상태 및 산출물 경로 출력<br/>종료 code 0]
    P -->|stopped| Q
    P -->|safe_stop| R[중지 사유 포함 출력<br/>종료 code 1]
```

## 2. Scheduler 반복 흐름

```mermaid
flowchart TD
    A[run_forever 루프 시작] --> B[run_once 호출]
    B --> C[오늘 trading_day 기준 scheduler_state 로드]
    C --> D[현재 시각까지 due job 계산]
    D --> E[미실행 slot의 job 실행]
    E --> F[scheduler_state 저장]
    F --> G[job_history / run_log 기록]
    G --> H{실패한 job 존재?}
    H -->|아니오| I{max_iterations 도달?}
    I -->|예| J[RunnerStatus.COMPLETED]
    I -->|아니오| K[next_run_at까지 sleep]
    K --> A
    H -->|예| L[safe stop cleanup handler 실행]
    L --> M[safe stop 알림 발행]
    M --> N[RunnerStatus.SAFE_STOP]
```

## 3. 운영 단계와 산출물

```mermaid
flowchart LR
    A[장전 준비<br/>market_open_prepare] --> B[장중 매매<br/>live_cycle]
    B --> C[장종료 정리<br/>market_close_cleanup]

    A --> A1[산출물<br/>일일 점검 리포트<br/>장전 smoke 리포트]
    B --> B1[산출물<br/>바 CSV 갱신<br/>주문 상태 파일<br/>주문/체결 알림]
    C --> C1[산출물<br/>일일 실행 리포트<br/>일일 점검 리포트<br/>다음 거래일 준비 파일]
    C --> C2{주의 마지막 거래일인가?}
    C2 -->|예| C3[산출물<br/>주간 리뷰 리포트]
    C2 -->|아니오| C4[주간 리뷰 없음]
```

## 4. 장전 준비 상세

```mermaid
flowchart TD
    A[MARKET_OPEN slot 도달] --> B[MarketOpenPreparationRuntime.run]
    B --> C[read-only broker smoke 실행]
    C --> D[전일 operations 로그 오류 요약]
    D --> E[장전 점검 항목 구성]
    E --> F[일일 점검 리포트 저장]
    F --> G{장전 준비 성공?}
    G -->|예| H[다음 INTRADAY slot 대기]
    G -->|아니오| I[job 실패로 기록]
    I --> J[runner safe stop 경로 진입]
```

## 5. 장중 매매 상세

```mermaid
flowchart TD
    A[INTRADAY slot 도달] --> B[live_cycle handler 실행]
    B --> C[_collect_strategy_bars]
    C --> D[전략 주기 기준 바 CSV 갱신]
    D --> E[LiveCycleRuntime.run]
    E --> F[심볼별 처리]
    F --> G[기존 미체결 주문 sync / 필요 시 취소]
    G --> H[전략 신호 계산]
    H --> I[리스크 검증]
    I --> J{주문 가능?}
    J -->|아니오| K[리스크 차단 알림]
    J -->|예| L[주문 제출 / 체결 동기화]
    L --> M[주문 알림 / 체결 알림]
    K --> N[summary 반환]
    M --> N
```

## 6. 장종료와 일간/주간 리포트 상세

```mermaid
flowchart TD
    A[MARKET_CLOSE slot 도달] --> B[MarketCloseRuntime.run]
    B --> C[당일 job 결과 / 주문 snapshot / holdings 집계]
    C --> D[일일 실행 리포트 build + 저장]
    D --> E[일일 실행 리포트 알림 발행]
    E --> F[다음 거래일 준비 파일 저장]
    F --> G[장마감 점검 항목 구성]
    G --> H[일일 점검 리포트 저장]
    H --> I{해당 거래일이 주의 마지막 거래일인가?}
    I -->|아니오| J[주간 리뷰 생략]
    I -->|예| K[주간 리뷰 build + 저장]
    K --> L{텔레그램 사용?}
    L -->|예| M[주간 리뷰 알림 발행]
    L -->|아니오| N[파일 산출물만 남김]
    J --> O[summary 반환]
    M --> O
    N --> O
```

## 7. Safe Stop 후처리

```mermaid
flowchart TD
    A[job 실패 또는 runner 예외] --> B[safe stop context 생성]
    B --> C[MarketCloseRuntime.run_safe_stop_cleanup 실행]
    C --> D[일일 실행 리포트 저장]
    D --> E[일일 점검 리포트 저장]
    E --> F[다음 거래일 준비 파일 저장]
    F --> G{해당 거래일이 주의 마지막 거래일인가?}
    G -->|아니오| H[주간 리뷰 생략]
    G -->|예| I[주간 리뷰 생성 및 필요 시 알림]
    H --> J[safe stop 알림 발행]
    I --> J
```
