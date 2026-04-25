# 추천기 메타데이터 운영

## 목적

주간 추천기의 seed universe CSV를 수동 편집하지 않고 한국투자 공식 `stocks_info` 원천에서 재생성한다. 현재 스크립트는 KOSPI/KOSDAQ/KONEX 종목 마스터와 업종 코드를 아래 스키마로 변환한다.

- `symbol`
- `name`
- `asset_type`
- `sector`
- `is_etf`
- `is_inverse`
- `is_leveraged`
- `active`

현재 추천기 스키마에 맞춰 `Stock`, `ETF`만 남기며 ETN, ELW, 리츠, 수익증권 등은 제외한다.

## 원천

한국투자 `stocks_info` 샘플의 DWS 마스터 파일:

- `kospi_code.mst.zip`
- `kosdaq_code.mst.zip`
- `konex_code.mst.zip`
- `idxcode.mst.zip`

기본 raw 캐시는 Git 추적 대상이 아닌 `tools/kis_stocks_info/raw/`다.

## 사용

주식 seed CSV:

```bash
.venv/bin/python tools/build_kis_seed_universe.py \
  --asset-scope stock \
  --output universe_stock.csv \
  --compare-to universe_stock.csv
```

ETF seed CSV:

```bash
.venv/bin/python tools/build_kis_seed_universe.py \
  --asset-scope etf \
  --output universe_etf.csv \
  --compare-to universe_etf.csv
```

다운로드한 raw 재사용:

```bash
.venv/bin/python tools/build_kis_seed_universe.py \
  --asset-scope stock \
  --raw-dir tools/kis_stocks_info/raw \
  --output universe_stock.csv \
  --compare-to universe_stock.csv \
  --skip-download
```

## 변환 규칙

- 업종은 중분류 우선, 없으면 대분류/소분류 fallback.
- ETF 여부는 증권그룹구분코드와 ETP 상품구분코드를 함께 사용.
- `is_inverse`, `is_leveraged`는 이름의 `인버스`, `곱버스`, `레버리지`, `2X` 패턴으로 정규화.
- `active=1`은 거래정지, 정리매매, 관리종목 플래그가 모두 꺼진 경우만.
- KONEX는 공식 업종 코드가 없으므로 `sector=KONEX`.

## 갱신 주기

- 기본: 주간 추천 리포트 생성 전, 주말 또는 주간 마지막 거래일 장마감 후 1회.
- 추가: 신규 상장/상장폐지 반영 필요 시, 또는 KIS `stocks_info` 포맷 변경 공지 시.

추천 모수와 섹터 캡이 메타데이터에 의존하므로 추천 배치 전 원천을 맞추는 것을 기본값으로 둔다.

## Diff 검토

1. `--compare-to`로 현재 seed CSV와 비교한다.
2. stdout의 added/removed/changed 개수를 확인한다.
3. `git diff -- universe_stock.csv universe_etf.csv`로 행 변경을 본다.
4. 우선 검토: 종목 추가/삭제, `sector`, `active`의 `1 -> 0` 또는 `0 -> 1`, ETF `is_inverse`/`is_leveraged`.
5. 대량 변경이면 raw 파일과 KIS 공지/샘플 저장소 변경 여부를 먼저 확인한다.

## 추천기 연결

```bash
.venv/bin/python -m autotrade.cli weekly-recommendation \
  --env-file .env \
  --universe-file universe_stock.csv
```
