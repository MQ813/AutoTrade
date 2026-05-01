# Graph Report - /home/minq/codes_wsl/AutoTrade  (2026-04-28)

## Corpus Check
- 125 files · ~241,954 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1768 nodes · 5414 edges · 45 communities detected
- Extraction: 63% EXTRACTED · 37% INFERRED · 0% AMBIGUOUS · INFERRED: 2012 edges (avg confidence: 0.76)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 21|Community 21]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 24|Community 24]]
- [[_COMMUNITY_Community 25|Community 25]]
- [[_COMMUNITY_Community 26|Community 26]]
- [[_COMMUNITY_Community 27|Community 27]]
- [[_COMMUNITY_Community 28|Community 28]]
- [[_COMMUNITY_Community 29|Community 29]]
- [[_COMMUNITY_Community 30|Community 30]]
- [[_COMMUNITY_Community 31|Community 31]]
- [[_COMMUNITY_Community 32|Community 32]]
- [[_COMMUNITY_Community 33|Community 33]]
- [[_COMMUNITY_Community 34|Community 34]]
- [[_COMMUNITY_Community 35|Community 35]]
- [[_COMMUNITY_Community 36|Community 36]]
- [[_COMMUNITY_Community 37|Community 37]]
- [[_COMMUNITY_Community 38|Community 38]]
- [[_COMMUNITY_Community 39|Community 39]]
- [[_COMMUNITY_Community 40|Community 40]]
- [[_COMMUNITY_Community 41|Community 41]]
- [[_COMMUNITY_Community 42|Community 42]]
- [[_COMMUNITY_Community 43|Community 43]]
- [[_COMMUNITY_Community 44|Community 44]]

## God Nodes (most connected - your core abstractions)
1. `BrokerSettings` - 49 edges
2. `KoreaInvestmentBrokerTrader` - 49 edges
3. `PaperBroker` - 47 edges
4. `FileExecutionStateStore` - 45 edges
5. `KoreaInvestmentBrokerError` - 45 edges
6. `RiskSettings` - 43 edges
7. `Bar` - 41 edges
8. `RecordingTransport` - 40 edges
9. `LiveCycleRuntime` - 39 edges
10. `OrderExecutionEngine` - 38 edges

## Surprising Connections (you probably didn't know these)
- `RiskSettings` --calls--> `test_risk_settings_accepts_operating_capital_limit()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/risk/models.py → /home/minq/codes_wsl/AutoTrade/tests/unit/config/test_models.py
- `RiskSettings` --calls--> `test_risk_settings_rejects_non_positive_operating_capital_limit()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/risk/models.py → /home/minq/codes_wsl/AutoTrade/tests/unit/config/test_models.py
- `_KisDownloadSource` --uses--> `SeedUniverseEntry`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/recommendation/kis_seed_universe.py → /home/minq/codes_wsl/AutoTrade/src/autotrade/recommendation/models.py
- `load_seed_universe_csv()` --calls--> `load_seed_universe()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/recommendation/universe.py → /home/minq/codes_wsl/AutoTrade/src/autotrade/recommendation/service.py
- `main()` --calls--> `_configure_logging()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/cli.py → /home/minq/codes_wsl/AutoTrade/src/autotrade/runtime/operations.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.03
Nodes (108): BrokerNormalizationError, Raised when broker payloads cannot be normalized to internal models., _aggregate_intraday_bars(), _build_fill_from_order_history_summary(), _build_fill_notice_subscription_message(), _build_management_order_record_from_submission(), _build_url(), CachedAccessToken (+100 more)

### Community 1 - "Community 1"
Cohesion: 0.03
Nodes (119): SeedUniverseAssetScope, _apply_inspection_updates(), _build_failure_inspection_report(), _build_loss_limit_inspection_item(), _build_market_close_inspection_report(), build_market_close_job(), _build_next_day_preparation(), _daily_fills() (+111 more)

### Community 2 - "Community 2"
Cohesion: 0.04
Nodes (78): FileRunnerControlStore, _locked_control_file(), _optional_datetime(), _optional_int(), _optional_string(), _require_aware_datetime(), _require_mapping(), _require_non_blank_text() (+70 more)

### Community 3 - "Community 3"
Cohesion: 0.03
Nodes (84): main(), collect_holiday_dates(), extract_holiday_dates_from_xls(), extract_holiday_dates_from_xls_bytes(), main(), render_holiday_module(), write_holiday_module(), path() (+76 more)

### Community 4 - "Community 4"
Cohesion: 0.05
Nodes (82): FileIntradayRiskStateStore, IntradayRiskState, _require_mapping(), _require_optional_decimal(), _require_optional_non_negative_decimal(), _require_optional_positive_decimal(), _require_text(), _serialize_optional_decimal() (+74 more)

### Community 5 - "Community 5"
Cohesion: 0.05
Nodes (69): apply_buy_fill(), apply_sell_fill(), BacktestPortfolioState, BacktestResult, BacktestTrade, build_portfolio_snapshot(), _buy_execution_price(), _calculate_buy_quantity() (+61 more)

### Community 6 - "Community 6"
Cohesion: 0.04
Nodes (71): _average_traded_value(), _build_metrics(), build_recommendation_report(), _calculate_return(), _percentile_score(), _rank_candidates(), _realized_volatility(), _resolve_as_of() (+63 more)

### Community 7 - "Community 7"
Cohesion: 0.07
Nodes (75): HttpRequest, HttpResponse, KoreaInvestmentBarSource, KoreaInvestmentBrokerReader, KoreaInvestmentBrokerTrader, _split_account(), ExecutionFill, ExecutionOrder (+67 more)

### Community 8 - "Community 8"
Cohesion: 0.06
Nodes (61): _apply_fills_to_order(), _deserialize_fill(), _deserialize_order(), _deserialize_request(), _deserialize_snapshot(), _deserialize_tracked_request(), DuplicateExecutionRequestError, ExecutionEngineError (+53 more)

### Community 9 - "Community 9"
Cohesion: 0.05
Nodes (74): _approve_symbols(), _build_and_write_weekly_recommendation(), _build_and_write_weekly_review(), _build_broker_clients(), _build_market_close_job(), _build_notifier(), _build_operation_services(), _build_paper_broker() (+66 more)

### Community 10 - "Community 10"
Cohesion: 0.06
Nodes (55): HTTPConnection, HTTPHandler, HTTPSConnection, HTTPSHandler, BackgroundNotifier, CompositeNotifier, _create_ipv4_connection(), _decode_telegram_payload() (+47 more)

### Community 11 - "Community 11"
Cohesion: 0.05
Nodes (36): BarCollectionService, BarIntegrityChecker, BarSource, BarStore, Read-only contract for loading a target universe., Read-only contract for loading OHLC bars., Write-only contract for persisting OHLC bars., Contract for validating a bar series before persistence. (+28 more)

### Community 12 - "Community 12"
Cohesion: 0.06
Nodes (40): KrxRegularSessionCalendar, publish_weekly_review_alert(), collect_strategy_bars(), collection_window_start(), execute_live_cycle(), is_last_trading_day_of_week(), latest_missed_market_close_at(), maybe_create_weekly_review() (+32 more)

### Community 13 - "Community 13"
Cohesion: 0.08
Nodes (29): BrokerReader, BrokerTrader, from_snapshot(), PaperBroker, PaperBrokerSnapshot, _PaperPosition, _require_non_negative_decimal(), _require_positive_int() (+21 more)

### Community 14 - "Community 14"
Cohesion: 0.14
Nodes (41): _calculate_drawdown(), _calculate_loss_amount(), calculate_max_buy_quantity(), _calculate_max_buy_quantity_by_cash(), _calculate_max_buy_quantity_by_entry_order_weight(), _calculate_max_buy_quantity_by_operating_capital(), _calculate_max_buy_quantity_by_weight(), _calculate_projected_position_weight() (+33 more)

### Community 15 - "Community 15"
Cohesion: 0.1
Nodes (38): main(), build_seed_universe_from_kis_files(), diff_seed_universe(), _download_and_extract_zip_member(), download_kis_stocks_info_files(), _is_inverse_name(), _is_leveraged_name(), _is_true_flag() (+30 more)

### Community 16 - "Community 16"
Cohesion: 0.16
Nodes (33): ConfigError, _load_risk_settings(), load_settings(), load_telegram_settings(), _parse_bool_setting(), _parse_broker_environment(), _parse_decimal_setting(), _parse_float_setting() (+25 more)

### Community 17 - "Community 17"
Cohesion: 0.15
Nodes (28): BacktestConfig, BacktestCostModel, BacktestEngine, BacktestOverfitCheck, BacktestPerformanceSummary, BacktestReport, build_backtest_report(), _build_overfit_check() (+20 more)

### Community 18 - "Community 18"
Cohesion: 0.15
Nodes (22): ApprovedSymbolsArtifacts, deserialize_approved_symbols_record(), load_approved_symbols_record(), load_latest_approved_symbols(), _require_mapping(), _require_text(), serialize_approved_symbols_record(), write_approved_symbols() (+14 more)

### Community 19 - "Community 19"
Cohesion: 0.19
Nodes (18): _normalize_order_price_for_signal(), is_valid_krx_order_price(), is_valid_krx_stock_order_price(), is_valid_krx_symbol_order_price(), _known_krx_etf_symbols(), krx_order_tick_size(), krx_stock_tick_size(), _krx_tick_size() (+10 more)

### Community 20 - "Community 20"
Cohesion: 0.15
Nodes (15): _add_control_arguments(), _add_runtime_arguments(), _build_parser(), main(), main_daily_inspection_compat(), main_live_cycle_compat(), main_weekly_review_compat(), main() (+7 more)

### Community 21 - "Community 21"
Cohesion: 0.2
Nodes (14): _make_bar(), _make_bars(), test_daily_trend_following_generates_buy_signal_with_reason(), test_strategy_rejects_gapped_bar_series(), test_strategy_rejects_invalid_series_structure(), test_strategy_returns_hold_for_insufficient_history(), test_thirty_minute_trend_generates_sell_signal_with_reason(), _build_signal() (+6 more)

### Community 22 - "Community 22"
Cohesion: 0.33
Nodes (4): _order(), RecordingTrader, test_file_execution_state_store_persists_snapshots_across_restart(), test_file_execution_state_store_recovers_from_corrupted_file()

### Community 23 - "Community 23"
Cohesion: 1.0
Nodes (1): Generated KRX regular-session holiday dates.  Regenerate with: `python tools/krx

### Community 24 - "Community 24"
Cohesion: 1.0
Nodes (0): 

### Community 25 - "Community 25"
Cohesion: 1.0
Nodes (0): 

### Community 26 - "Community 26"
Cohesion: 1.0
Nodes (0): 

### Community 27 - "Community 27"
Cohesion: 1.0
Nodes (0): 

### Community 28 - "Community 28"
Cohesion: 1.0
Nodes (0): 

### Community 29 - "Community 29"
Cohesion: 1.0
Nodes (0): 

### Community 30 - "Community 30"
Cohesion: 1.0
Nodes (0): 

### Community 31 - "Community 31"
Cohesion: 1.0
Nodes (0): 

### Community 32 - "Community 32"
Cohesion: 1.0
Nodes (0): 

### Community 33 - "Community 33"
Cohesion: 1.0
Nodes (0): 

### Community 34 - "Community 34"
Cohesion: 1.0
Nodes (0): 

### Community 35 - "Community 35"
Cohesion: 1.0
Nodes (0): 

### Community 36 - "Community 36"
Cohesion: 1.0
Nodes (0): 

### Community 37 - "Community 37"
Cohesion: 1.0
Nodes (0): 

### Community 38 - "Community 38"
Cohesion: 1.0
Nodes (0): 

### Community 39 - "Community 39"
Cohesion: 1.0
Nodes (1): KIS_ACCESS_TOKEN 환경변수가 없을 때만 1회 발급합니다.     반복 발급 테스트용이 아닙니다.

### Community 40 - "Community 40"
Cohesion: 1.0
Nodes (1): Raised when order execution cannot be completed safely.

### Community 41 - "Community 41"
Cohesion: 1.0
Nodes (1): Raised when a request id is reused with different payload.

### Community 42 - "Community 42"
Cohesion: 1.0
Nodes (1): Raised when an order cannot be found in local execution state.

### Community 43 - "Community 43"
Cohesion: 1.0
Nodes (1): Raised when an order transition is not allowed.

### Community 44 - "Community 44"
Cohesion: 1.0
Nodes (1): Raised when an operation can be retried safely.

## Knowledge Gaps
- **17 isolated node(s):** `Generated KRX regular-session holiday dates.  Regenerate with: `python tools/krx`, `Deterministic, read-only signal generation contract.`, `_TrendFollowingConfig`, `Raised when order execution cannot be completed safely.`, `Raised when a request id is reused with different payload.` (+12 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 23`** (2 nodes): `krx_holidays.py`, `Generated KRX regular-session holiday dates.  Regenerate with: `python tools/krx`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 24`** (2 nodes): `operations.py`, `main()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 25`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 26`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 27`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 28`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 29`** (1 nodes): `operations.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 30`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 31`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 32`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 33`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 34`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 35`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 36`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 37`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 38`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 39`** (1 nodes): `KIS_ACCESS_TOKEN 환경변수가 없을 때만 1회 발급합니다.     반복 발급 테스트용이 아닙니다.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 40`** (1 nodes): `Raised when order execution cannot be completed safely.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 41`** (1 nodes): `Raised when a request id is reused with different payload.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 42`** (1 nodes): `Raised when an order cannot be found in local execution state.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 43`** (1 nodes): `Raised when an order transition is not allowed.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 44`** (1 nodes): `Raised when an operation can be retried safely.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Bar` connect `Community 11` to `Community 0`, `Community 1`, `Community 3`, `Community 4`, `Community 5`, `Community 6`, `Community 7`, `Community 9`, `Community 12`, `Community 13`, `Community 17`, `Community 21`?**
  _High betweenness centrality (0.063) - this node is a cross-community bridge._
- **Why does `PaperBroker` connect `Community 13` to `Community 0`, `Community 1`, `Community 4`, `Community 7`, `Community 9`?**
  _High betweenness centrality (0.045) - this node is a cross-community bridge._
- **Why does `FileExecutionStateStore` connect `Community 4` to `Community 1`, `Community 3`, `Community 8`, `Community 9`, `Community 22`?**
  _High betweenness centrality (0.040) - this node is a cross-community bridge._
- **Are the 166 inferred relationships involving `ValueError` (e.g. with `.__post_init__()` and `.__post_init__()`) actually correct?**
  _`ValueError` has 166 INFERRED edges - model-reasoned connections that need verification._
- **Are the 47 inferred relationships involving `BrokerSettings` (e.g. with `ConfigError` and `Raised when required settings are missing or invalid.`) actually correct?**
  _`BrokerSettings` has 47 INFERRED edges - model-reasoned connections that need verification._
- **Are the 30 inferred relationships involving `KoreaInvestmentBrokerTrader` (e.g. with `BrokerNormalizationError` and `BrokerReader`) actually correct?**
  _`KoreaInvestmentBrokerTrader` has 30 INFERRED edges - model-reasoned connections that need verification._
- **Are the 25 inferred relationships involving `PaperBroker` (e.g. with `ReplaySessionSnapshot` and `ReplayLogEntry`) actually correct?**
  _`PaperBroker` has 25 INFERRED edges - model-reasoned connections that need verification._