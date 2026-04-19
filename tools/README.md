# Tools

운영 및 개발 보조 스크립트가 들어갑니다.

현재 포함된 스크립트:

- 공식 메인 진입점은 `src/autotrade/cli.py`입니다. `python -m autotrade.cli`로 `run-once`, `run-continuous`, `market-open`, `market-close`, `weekly-review`, `daily-inspection` 서브커맨드를 실행합니다.
- `operations.py`: 공식 CLI를 호출하는 호환 래퍼입니다.
- `broker_smoke_check.py`: paper/live 읽기 전용 smoke를 실행하고 로그 파일 경로를 출력합니다.
- `live_cycle.py`: 기존 실행 경로 호환용 래퍼입니다. 내부적으로 `autotrade.cli`의 `run-once` 또는 `run-continuous`를 호출합니다.
- `daily_inspection.py`: 기존 수동 점검 체크리스트 생성 경로용 호환 래퍼입니다. 내부적으로 `autotrade.cli`의 `daily-inspection`을 호출합니다.
- `weekly_review.py`: 기존 실행 경로 호환용 래퍼입니다. 내부적으로 `autotrade.cli`의 `weekly-review`를 호출합니다.
