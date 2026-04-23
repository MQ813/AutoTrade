# 추천기 메타데이터 운영

## 목적

주간 추천기의 seed universe CSV를 수동 편집 대신 한국투자 공식 `stocks_info` 원천으로 재생성한다.
현재 스크립트는 KOSPI, KOSDAQ, KONEX 종목 마스터와 업종 코드를 받아 아래 seed 스키마로 변환한다.

- `symbol`
- `name`
- `asset_type`
- `sector`
- `is_etf`
- `is_inverse`
- `is_leveraged`
- `active`

변환은 현재 추천기 스키마에 맞춰 `Stock` 과 `ETF`만 남긴다.
ETN, ELW, 리츠, 수익증권 등 현재 추천 정책에서 직접 다루지 않는 상품군은 제외한다.

## 원천 경로

공식 원천은 한국투자 `stocks_info` 샘플이 사용하는 DWS 마스터 파일이다.

- `kospi_code.mst.zip`
- `kosdaq_code.mst.zip`
- `konex_code.mst.zip`
- `idxcode.mst.zip`

기본 raw 캐시는 `tools/kis_stocks_info/raw/` 이다.
이 경로는 로컬 재실행용이며 Git 추적 대상이 아니다.

## 사용 방법

주식 seed CSV 생성:

```bash
.venv/bin/python tools/build_kis_seed_universe.py \
  --asset-scope stock \
  --output universe_stock.csv \
  --compare-to universe_stock.csv
```

ETF seed CSV 생성:

```bash
.venv/bin/python tools/build_kis_seed_universe.py \
  --asset-scope etf \
  --output universe_etf.csv \
  --compare-to universe_etf.csv
```

raw 다운로드를 재사용하고 싶으면 `--skip-download`를 추가한다.

```bash
.venv/bin/python tools/build_kis_seed_universe.py \
  --asset-scope stock \
  --raw-dir tools/kis_stocks_info/raw \
  --output universe_stock.csv \
  --compare-to universe_stock.csv \
  --skip-download
```

## 변환 규칙

- 업종은 공식 업종 코드의 중분류를 우선 사용하고, 없으면 대분류와 소분류로 fallback 한다.
- ETF 여부는 공식 증권그룹구분코드와 ETP 상품구분코드를 함께 본다.
- `is_inverse`, `is_leveraged`는 ETF 이름의 `인버스`, `곱버스`, `레버리지`, `2X` 패턴으로 정규화한다.
- `active`는 거래정지, 정리매매, 관리종목 플래그가 모두 꺼져 있을 때만 `1`이다.
- KONEX는 공식 업종 코드가 연결되지 않으므로 `sector=KONEX`로 둔다.

## 갱신 주기

- 기본 주기: 주간 추천 리포트를 새로 만들기 전, 주말 또는 주간 마지막 거래일 장마감 이후 1회
- 추가 갱신: 신규 상장/상장폐지 반영이 필요하거나 KIS `stocks_info` 포맷 변경 공지가 나온 경우

주기가 너무 잦을 필요는 없지만, 추천 결과의 모수와 섹터 캡이 메타데이터에 직접 의존하므로 추천 배치를 새로 돌리기 전에 원천을 한 번 맞추는 것을 기본값으로 둔다.

## Diff 검토 절차

1. `--compare-to`로 현재 운영 중인 seed CSV를 기준 비교한다.
2. 스크립트 stdout의 added/removed/changed 개수를 확인한다.
3. `git diff -- universe_stock.csv universe_etf.csv` 로 실제 행 단위 변경을 본다.
4. 아래 항목을 우선 검토한다: 종목 추가/삭제, `sector` 변경, `active`가 `1 -> 0` 또는 `0 -> 1`로 바뀐 경우, ETF의 `is_inverse` / `is_leveraged` 변경
5. 대량 변경이면 바로 운영에 반영하지 말고 raw 파일과 KIS 공지/샘플 저장소 변경 여부를 확인한다.

## 추천기 연결

생성한 CSV는 기존 주간 추천 커맨드에 바로 넣는다.

```bash
.venv/bin/python -m autotrade.cli weekly-recommendation \
  --env-file .env \
  --universe-file universe_stock.csv
```
