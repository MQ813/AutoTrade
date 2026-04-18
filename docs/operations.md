# Operations

현재 저장소에는 정규장 운영을 위한 기본 `scheduler/report` 구현이 포함됩니다.

- `scheduler`: 장 시작, 장중, 장마감 작업을 KRX 정규장 캘린더 기준으로 실행합니다.
- `report`: 실행 결과를 운영 로그, 일일 리포트, 알림 메시지로 변환합니다.

## 소액 실전 운영 기준

- 실전 환경에서는 `AUTOTRADE_BROKER_ENV=live`와 함께 `AUTOTRADE_RISK_MAX_OPERATING_CAPITAL`을 명시해 자동매매가 사용할 최대 운영 자금을 제한합니다.
- 주문/체결 알림은 `report` 모듈의 `build_order_alert`, `build_fill_alert`, `publish_order_alert`, `publish_fill_alert`로 생성합니다.
- `AUTOTRADE_TELEGRAM_ENABLED=true`이면 공식 CLI인 `python -m autotrade.cli ...`가 파일 notifier와 Telegram notifier를 함께 연결해 주문/체결/일일 리포트를 발행합니다.
- `python -m autotrade.cli weekly-review`는 기본적으로 저장소 루트의 `.env`를 읽고, 텔레그램이 켜져 있으면 주간 리뷰를 파일로 저장한 뒤 Telegram으로 발행합니다.
- 텔레그램 채널은 `AUTOTRADE_TELEGRAM_CHAT_ID`를 기본값으로 쓰고, `AUTOTRADE_TELEGRAM_WARNING_CHAT_ID`, `AUTOTRADE_TELEGRAM_ERROR_CHAT_ID`로 심각도별 분리를 선택할 수 있습니다.
- `python -m autotrade.cli run-once`는 실행 시작 시 KIS에서 전략 주기에 맞는 바를 수집해 `AUTOTRADE_LOG_DIR/bars`에 저장한 뒤, 전략 신호, 리스크 검증, 주문 제출, 주문/체결 알림 발행을 한 번 실행합니다.
- `python -m autotrade.cli run-continuous`는 `scheduler`를 함께 구동해 `next_run_at` 기준으로 대기/재개하며, `AUTOTRADE_LOG_DIR/scheduler_state.json`을 사용해 재시작 뒤에도 같은 슬롯을 중복 실행하지 않습니다.
- `run-continuous`의 `market_close` 단계는 일일 실행 리포트와 일일 점검 리포트를 남기고, 해당 거래일이 그 주의 마지막 거래일이면 주간 리뷰도 같은 흐름에서 생성합니다.
- 공식 CLI는 `src/autotrade/cli.py`에 있고, 저장소 로컬 실행 호환 경로로 `tools/operations.py`도 유지합니다.
- 공식 CLI는 기본적으로 저장소 루트의 `.env`를 읽고, 템플릿은 `docs/autotrade.env.example`에 있습니다.
- 공식 CLI의 기본 입력 경로는 `AUTOTRADE_LOG_DIR/bars`이고, 기본 산출물은 `AUTOTRADE_LOG_DIR/notifications.jsonl`, `AUTOTRADE_LOG_DIR/execution_state.json`, `AUTOTRADE_LOG_DIR/scheduler_state.json`입니다.
- 일일 점검 체크리스트는 `python tools/daily_inspection.py`로 생성하고, 주간 리뷰 템플릿은 `python tools/weekly_review.py`로 생성합니다.
- 위 스크립트는 `AUTOTRADE_LOG_DIR` 아래에 텍스트 산출물을 남기며, 실제 운영 실행기나 외부 알림 채널은 상위 orchestration에서 연결합니다.

## 실행 순서

1. `cp docs/autotrade.env.example .env`
2. `.env` 안의 계좌, 종목, 로그 경로 값을 실제 환경에 맞게 수정합니다.
3. `python -m autotrade.cli run-once`를 실행합니다.
4. stdout 한글 로그와 `AUTOTRADE_LOG_DIR/bars`, `AUTOTRADE_LOG_DIR/notifications.jsonl`, `AUTOTRADE_LOG_DIR/execution_state.json`을 확인합니다.

필요하면 `python -m autotrade.cli run-once --env-file /path/to/custom.env`로 다른 `.env` 파일을 지정할 수 있습니다.
지속 실행이 필요하면 `python -m autotrade.cli run-continuous`를 사용합니다.
장전 준비만 따로 실행할 때는 `python -m autotrade.cli market-open`을 사용합니다.
장종료 정리만 따로 실행할 때는 `python -m autotrade.cli market-close`를 사용합니다.
주간 리뷰만 따로 발행할 때는 `python -m autotrade.cli weekly-review --env-file /path/to/custom.env`를 사용할 수 있습니다.
패키지가 import되지 않는 환경에서는 호환 경로인 `python tools/operations.py ...`도 계속 사용할 수 있습니다.

## 필수 설정

- `AUTOTRADE_BROKER_ENV`
- `AUTOTRADE_BROKER_API_KEY`
- `AUTOTRADE_BROKER_API_SECRET`
- `AUTOTRADE_BROKER_ACCOUNT`
- `AUTOTRADE_TARGET_SYMBOLS`
- `AUTOTRADE_LOG_DIR`

## 선택 설정

- `AUTOTRADE_TELEGRAM_ENABLED`
- `AUTOTRADE_TELEGRAM_BOT_TOKEN`
- `AUTOTRADE_TELEGRAM_CHAT_ID`
- `AUTOTRADE_TELEGRAM_WARNING_CHAT_ID`
- `AUTOTRADE_TELEGRAM_ERROR_CHAT_ID`
- `AUTOTRADE_TELEGRAM_MAX_RETRIES`
- `AUTOTRADE_TELEGRAM_TIMEOUT_SECONDS`

## 장 시작 전

- 설정과 환경변수를 확인합니다.
- 계좌, 주문 가능 상태, 데이터 최신성을 점검합니다.
- 당일 운영 대상 종목과 전략 파라미터를 확인합니다.
- 장 시작 phase에 연결된 준비 작업을 실행합니다.

## 장중

- 주문과 체결 상태를 주기적으로 확인합니다.
- 예외와 경고를 기록합니다.
- 리스크 제한 조건을 초과하면 자동 진입을 멈춥니다.
- 장중 작업 주기는 `SchedulerConfig.intraday_interval`로 제어합니다.
- 실행 결과는 작업별 성공/실패와 상세 메시지로 기록됩니다.
- 실패한 job이 발생하면 runner는 알림을 발행한 뒤 안전 정지하고, 재기동 시 `scheduler_state.json` 기준으로 다음 미실행 슬롯부터 이어갑니다.

## 장마감

- 당일 주문, 체결, 잔고 상태를 정리합니다.
- 리포트를 남깁니다.
- 다음 거래일을 위한 점검 항목을 남깁니다.
- 장마감 이후에는 일일 실행 리포트와 알림 메시지를 생성할 수 있습니다.

## 운영 산출물

- 실행 로그: 작업별 phase, 예정 시각, 성공/실패, 상세 메시지를 저장합니다.
- 일일 리포트: 장 시작, 장중, 장마감별 작업 수와 실패 수를 요약합니다.
- 알림: 실패가 있으면 `error`, 실행이 없으면 `warning`, 전부 성공이면 `info` 수준으로 발행합니다.
- 텔레그램 알림: `429`, `5xx`, 네트워크 오류에는 재시도하고, 장문 메시지는 Telegram 제한 길이에 맞춰 분할합니다.
- 일일 점검 리포트: 장 시작 전, 장중, 장마감 후 점검 항목을 `passed/failed/pending` 상태로 기록합니다.
- 주간 리뷰 문서: 일일 실행 결과와 점검 상태를 주간 단위로 요약하고, 운영 회고 프롬프트를 남깁니다.
