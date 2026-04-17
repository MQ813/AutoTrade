# Tools

운영 및 개발 보조 스크립트가 들어갑니다.

현재 포함된 스크립트:

- `broker_smoke_check.py`: paper/live 읽기 전용 smoke를 실행하고 로그 파일 경로를 출력합니다.
- `live_cycle.py`: 실행 시작 시 KIS에서 전략 주기에 맞는 바를 수집해 CSV로 저장한 뒤, 전략 -> 리스크 -> 주문 -> 알림까지 한 번의 운영 사이클을 실행합니다. `--continuous`를 주면 `scheduler`를 함께 돌리며 `scheduler_state.json` 기준으로 sleep/wake와 재시작 복구를 수행합니다. 기본적으로 저장소 루트의 `.env`를 읽고, 템플릿은 `docs/live_cycle.env.example`에 있습니다. 다른 설정 파일은 `--env-file`로 지정할 수 있습니다.
- `daily_inspection.py`: 당일 점검 체크리스트 텍스트 파일을 `AUTOTRADE_LOG_DIR` 아래에 생성합니다.
- `weekly_review.py`: 현재 주간 리뷰 템플릿 텍스트 파일을 `AUTOTRADE_LOG_DIR` 아래에 생성합니다.
