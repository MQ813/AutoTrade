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

초기 목표는 구현보다 경계 정의입니다. 각 패키지는 이후 단계에서 설정, 데이터, 전략, 주문, 포트폴리오, 리포트 책임을 분리해 확장합니다.
