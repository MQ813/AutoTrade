# 주간 종목 추천기 개발 계획

## 1. 목적

현재 AutoTrade는 사용자가 `AUTOTRADE_TARGET_SYMBOLS`를 직접 지정한다. 다음 단계는 재현 가능한 주간 추천 파이프라인이다.

1. 규칙 기반으로 주간 후보 20개를 선정한다.
2. CSV/Markdown/JSON 리포트를 저장한다.
3. Codex는 리포트로 상위 3~4개 후보의 장단점과 제외 리스크를 해석한다.
4. 사용자가 승인한 3~4개만 자동매매 대상이 된다.

이 문서는 구현 전 범위, 경계, 단계, 리스크, 검증을 고정하는 planner/manager 문서다.

## 2. 목표 흐름

### 사용자 흐름

1. 주말 또는 주간 마지막 거래일 장마감 후 추천기를 실행한다.
2. 추천기는 유니버스 필터, 점수, 상위 N개 선정, 제외 규칙으로 후보 20개를 만든다.
3. 후보와 점수 구성요소를 CSV/Markdown/JSON으로 저장한다.
4. 사용자는 Markdown 리포트를 Codex에 넣어 상위 3~4개 추천과 제외 사유를 받는다.
5. 승인 종목만 별도 승인 산출물에 기록한다.
6. 자동매매 실행기는 승인 산출물을 우선 사용하고 없으면 `AUTOTRADE_TARGET_SYMBOLS`로 fallback 한다.

### 첫 버전 비목표

- 뉴스, 공시, 재무제표, 텍스트 감성 같은 비결정적 신호.
- Codex의 점수 계산 또는 자동 종목 확정.
- `.env` 직접 덮어쓰기.
- 전체 KRX 자동 크롤링과 완전한 메타데이터 동기화.

## 3. 원칙

- 같은 입력/파라미터는 같은 결과를 낸다.
- 데이터 수집, 점수 계산, 리포트 출력, 승인 반영을 분리한다.
- 추천 결과와 승인 결과를 모두 파일로 남긴다.
- Codex는 해석/비교/요약만 하고 최종 승인은 사용자가 한다.
- 승인 산출물 우선, 환경변수 fallback 으로 기존 실행 경로를 보존한다.
- 첫 버전은 좁고 안정적인 유니버스부터 시작한다.

## 4. 모듈 경계

`strategy/selector.py`는 전략 선택 팩토리이므로 추천 로직을 섞지 않는다. 추천기는 전략 신호 계산과 별도 경계로 둔다.

신규 경계 `src/autotrade/recommendation/`:

- 유니버스 필터
- 점수 계산
- 상위 N개 선정
- 제외 규칙
- 추천 결과 모델

기존 모듈 연결:

- `config/`: `AUTOTRADE_TARGET_SYMBOLS` 유지, 추후 승인 산출물 경로 설정.
- `data/`: 바 데이터, 유니버스 메타데이터, 정합성 검증.
- `report/`: CSV/Markdown/JSON 저장과 렌더링.
- `runtime/`: 승인 종목을 실행 대상으로 해석.
- `tools/` 또는 `cli`: 추천 리포트 생성과 승인 반영 명령.

주의점:

- `runtime/live_cycle.py`는 여러 심볼 순회를 지원하지만 기본 설정은 `settings.target_symbols`.
- `runtime/operation_services.py`의 paper broker 초기화와 `broker/smoke.py`는 첫 번째 심볼을 대표로 사용한다.
- 승인 종목 3~4개 연동 시 운영 경로의 단일 대표 심볼 가정을 점검해야 한다.

## 5. 입력과 출력

입력:

- Seed universe CSV: `symbol`, `name`, `asset_type`, `sector`, `is_etf`, `is_inverse`, `is_leveraged`, `active`.
- 일봉 기준 최소 120영업일 이상 바 데이터.
- 파라미터: 최소 거래대금, 모멘텀/추세/변동성 윈도우, 업종/ETF 허용·제외 규칙, 후보 수, 승인 목표 수.

출력:

- `weekly_candidates_YYYYMMDD.csv`
- `weekly_candidates_YYYYMMDD.md`
- `weekly_candidates_YYYYMMDD.json`
- `approved_symbols_YYYYMMDD.json` 또는 동등한 승인 산출물

Markdown 리포트 필수 내용:

- 생성 시각과 기준 거래일
- 유니버스 크기
- 필터 통과 수와 탈락 사유 요약
- 후보 20개 테이블
- 총점과 구성요소 점수
- 하드 제외 종목 요약
- Codex 검토용 프롬프트 초안

## 6. 추천 로직

### 유니버스 필터

- 비활성/메타데이터 불완전/최소 바 수 미달/최소 평균 거래대금 미달 제외.
- 레버리지·인버스 ETF 제외.
- 수동 제외 업종/심볼 제거.

### 점수 계산

모든 점수는 0~100으로 정규화하고 가중합한다.

- 거래대금: 최근 20일 평균 거래대금 percentile.
- 모멘텀: 20일, 60일, 120일 수익률 조합.
- 변동성: 최근 20일 또는 60일 실현변동성 기반 패널티/적정 범위 점수.
- 추세: 가격과 이동평균 정배열, 이동평균 기울기.

가중치는 설정으로 분리하되 첫 버전 기본값은 문서에 고정한다.

### 상위 N개 선정

- 총점 내림차순.
- 동점 tie-break: 거래대금, 모멘텀, 심볼 코드.
- 기본 후보 수는 20개.

### 제외 규칙

- 동일 섹터 쏠림 상한.
- 동일 기초지수/유사 ETF 중복 상한.
- 변동성 과다, 거래대금 급감, 최근 데이터 결손 제외.
- 첫 버전은 설명 가능한 하드 룰만 넣고 해석형 리스크는 리포트에 남긴다.

## 7. Codex 역할

Codex는 점수를 계산하지 않고 추천 산출물을 해석한다.

- 후보 20개 중 3~4개 압축 제안.
- 3개 또는 4개가 적절한 이유 비교.
- 제외 종목 리스크 요약.
- 섹터/ETF 중복, 변동성, 거래대금 편향 경고.

추천기는 결정적 엔진, Codex는 해석 레이어다.

## 8. 승인 반영

권장 흐름:

1. 추천기가 후보 리포트를 만든다.
2. 사용자가 Codex 검토 후 3~4개를 확정한다.
3. 승인 산출물에 저장한다.
4. 자동매매 실행기는 승인 산출물을 우선 사용한다.
5. 승인 산출물이 없으면 `AUTOTRADE_TARGET_SYMBOLS`를 사용한다.

이유:

- `.env` 자동 수정은 추적과 복구가 어렵다.
- 승인 결과 파일은 주차별 이력 비교가 쉽다.
- fallback 으로 추천기 도입 전후를 공존시킨다.

## 9. 구현 계획

### 9.1 데이터 계약 고정

- 요약: 입력 메타데이터와 출력 모델 고정.
- 영향: `docs/stock_recommender_plan.md`, `docs/roadmap.md`, 추후 `src/autotrade/recommendation/models.py`, `tests/unit/recommendation/`.
- 단계: 유니버스 스키마, 추천 결과 포맷, 승인 산출물 포맷 확정.
- 리스크: ETF/업종 품질, 승인 포맷 변경 시 runtime 연동 확대.
- 검증: 샘플 메타데이터와 추천 결과 문서 예시 검토.

### 9.2 결정적 추천 엔진

- 요약: 필터, 점수, 랭킹, 제외 규칙을 순수 로직으로 구현.
- 영향: 추후 `filters.py`, `scoring.py`, `selection.py`, `tests/unit/recommendation/`.
- 단계: 필터, 특성/정규화, 가중합/tie-break, 제외 규칙/최종 20개.
- 리스크: 작은 유니버스의 percentile 왜곡, 과도한 점수 요소로 설명력 저하.
- 검증: fixture 고정 순위, 탈락 사유, 동점 정렬, 제외 규칙 회귀 테스트.

### 9.3 리포트 출력

- 요약: CSV/Markdown/JSON 산출물과 주간 아카이브 구조 구현.
- 영향: 추후 `src/autotrade/report/`, `src/autotrade/recommendation/reporting.py`, `tests/unit/report/`.
- 단계: CSV writer, Markdown renderer, JSON archive, Codex 프롬프트 섹션.
- 리스크: 장황한 리포트, CSV/Markdown 정렬 불일치.
- 검증: golden text, CSV 컬럼 순서 테스트.

### 9.4 승인 결과 연결

- 요약: 승인 종목 3~4개를 runtime 이 읽는 경로 구현.
- 영향: 추후 `config/`, `runtime/`, `cli.py`, `tests/unit/config/`, `tests/unit/runtime/`.
- 단계: 승인 로더, approved 우선/env fallback 규칙, 확인 CLI/도구.
- 리스크: 산출물 부재/손상 fallback, 승인 종목 수와 리스크 설정 불일치, 단일 대표 심볼 가정.
- 검증: 승인 우선 적용, fallback, 손상 파일 정책 테스트.

### 9.5 운영 연결

- 요약: 주간 리뷰와 추천기 실행 타이밍을 운영 플로우에 연결.
- 영향: 추후 `runtime/operations.py`, `docs/operations.md`, `tools/`.
- 단계: 수동 CLI 제공 후 안정화되면 `market_close` 또는 주간 배치 연결 검토.
- 리스크: 주간 리뷰와 추천기를 함께 묶으면 실패 범위 확대.
- 검증: 샘플 주간 데이터 end-to-end dry run, 기존 weekly-review 충돌 확인.

## 10. 결정 사항

- 첫 유니버스를 전체 KRX가 아닌 seed universe 로 시작할지.
- ETF와 개별주를 같은 점수 체계로 볼지, 자산군별 quota 를 둘지.
- 동일 섹터 최대 허용 개수.
- 승인 종목 기본 수를 3개로 둘지 4개 허용으로 둘지.
- 추천 리포트 생성 시점: 금요일 장마감 직후 또는 주말 수동 배치.

## 11. 권장 첫 범위

1. Seed universe 파일 기반 추천기.
2. 일봉 기반 필터/점수/랭킹/제외 규칙.
3. 주간 후보 20개 CSV/Markdown/JSON 생성.
4. 승인 종목 파일 저장.
5. Runtime 이 승인 종목 파일을 우선 읽고 없으면 기존 환경변수 사용.

실시간 뉴스, 자동 승인, 전체 시장 자동 메타데이터 수집은 2차 단계로 미룬다.
