# Graph Report - /home/minq/codes_wsl/AutoTrade  (2026-04-28)

## Corpus Check
- 125 files · ~239,322 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1729 nodes · 5324 edges · 42 communities detected
- Extraction: 63% EXTRACTED · 37% INFERRED · 0% AMBIGUOUS · INFERRED: 1975 edges (avg confidence: 0.76)
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

## God Nodes (most connected - your core abstractions)
1. `KoreaInvestmentBrokerTrader` - 49 edges
2. `BrokerSettings` - 47 edges
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
- `_run_smoke_for_timestamp()` --calls--> `run_read_only_smoke()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/runtime/market_open.py → /home/minq/codes_wsl/AutoTrade/src/autotrade/broker/smoke.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.04
Nodes (94): BrokerNormalizationError, Raised when broker payloads cannot be normalized to internal models., _aggregate_intraday_bars(), _build_fill_from_order_history_summary(), _build_fill_notice_subscription_message(), _build_management_order_record_from_submission(), _build_url(), CachedAccessToken (+86 more)

### Community 1 - "Community 1"
Cohesion: 0.04
Nodes (91): FileRunnerControlStore, _optional_datetime(), _optional_int(), _optional_string(), _require_aware_datetime(), _require_mapping(), _require_non_blank_text(), _require_string() (+83 more)

### Community 2 - "Community 2"
Cohesion: 0.03
Nodes (115): SeedUniverseAssetScope, _apply_inspection_updates(), _build_failure_inspection_report(), _build_loss_limit_inspection_item(), _build_market_close_inspection_report(), build_market_close_job(), _build_next_day_preparation(), _daily_fills() (+107 more)

### Community 3 - "Community 3"
Cohesion: 0.03
Nodes (116): main(), collect_holiday_dates(), extract_holiday_dates_from_xls(), extract_holiday_dates_from_xls_bytes(), main(), render_holiday_module(), write_holiday_module(), path() (+108 more)

### Community 4 - "Community 4"
Cohesion: 0.05
Nodes (82): create_live_cycle_runtime(), LiveCycleRuntime, _normalize_strategy_kind(), strategy_timeframe_for(), FileExecutionStateStore, _build_market_open_notification(), _build_pre_market_items(), _build_preview_bar() (+74 more)

### Community 5 - "Community 5"
Cohesion: 0.04
Nodes (75): _average_traded_value(), _build_metrics(), build_recommendation_report(), _calculate_return(), _percentile_score(), _rank_candidates(), _realized_volatility(), _resolve_as_of() (+67 more)

### Community 6 - "Community 6"
Cohesion: 0.05
Nodes (70): apply_buy_fill(), apply_sell_fill(), BacktestPortfolioState, build_portfolio_snapshot(), create_backtest_portfolio(), PortfolioSnapshot, _require_non_blank(), _require_non_negative_decimal() (+62 more)

### Community 7 - "Community 7"
Cohesion: 0.07
Nodes (78): HttpRequest, HttpResponse, KoreaInvestmentBarSource, KoreaInvestmentBrokerReader, KoreaInvestmentBrokerTrader, _split_account(), ExecutionFill, ExecutionOrder (+70 more)

### Community 8 - "Community 8"
Cohesion: 0.07
Nodes (39): _apply_fills_to_order(), DuplicateExecutionRequestError, ExecutionEngineError, ExecutionRetryPolicy, InMemoryExecutionStateStore, InvalidExecutionOrderStateError, _OrderAwareFillReader, OrderExecutionEngine (+31 more)

### Community 9 - "Community 9"
Cohesion: 0.06
Nodes (53): KrxRegularSessionCalendar, build_weekly_review_report(), build_session_slots(), collect_due_jobs(), _is_stale_slot(), JobContext, next_scheduled_run_at(), PendingJob (+45 more)

### Community 10 - "Community 10"
Cohesion: 0.05
Nodes (37): BarCollectionService, BarIntegrityChecker, BarSource, BarStore, Read-only contract for loading a target universe., Read-only contract for loading OHLC bars., Write-only contract for persisting OHLC bars., Contract for validating a bar series before persistence. (+29 more)

### Community 11 - "Community 11"
Cohesion: 0.07
Nodes (54): BacktestConfig, BacktestCostModel, BacktestEngine, BacktestOverfitCheck, BacktestPerformanceSummary, BacktestReport, BacktestResult, BacktestTrade (+46 more)

### Community 12 - "Community 12"
Cohesion: 0.07
Nodes (30): BrokerReader, BrokerTrader, from_snapshot(), PaperBroker, PaperBrokerSnapshot, _PaperPosition, _require_non_negative_decimal(), _require_positive_int() (+22 more)

### Community 13 - "Community 13"
Cohesion: 0.08
Nodes (55): ApprovedSymbolsArtifacts, deserialize_approved_symbols_record(), load_approved_symbols_record(), load_latest_approved_symbols(), _require_mapping(), _require_text(), serialize_approved_symbols_record(), write_approved_symbols() (+47 more)

### Community 14 - "Community 14"
Cohesion: 0.09
Nodes (43): CompositeNotifier, _decode_telegram_payload(), _display_symbol(), _extract_retry_after_seconds(), _format_telegram_body(), _format_telegram_header(), _format_telegram_messages(), _load_symbol_name_map() (+35 more)

### Community 15 - "Community 15"
Cohesion: 0.07
Nodes (30): FileIntradayRiskStateStore, IntradayRiskState, _require_mapping(), _require_optional_decimal(), _require_optional_non_negative_decimal(), _require_optional_positive_decimal(), _require_text(), _advance_market_if_supported() (+22 more)

### Community 16 - "Community 16"
Cohesion: 0.15
Nodes (41): _calculate_drawdown(), _calculate_loss_amount(), calculate_max_buy_quantity(), _calculate_max_buy_quantity_by_cash(), _calculate_max_buy_quantity_by_entry_order_weight(), _calculate_max_buy_quantity_by_operating_capital(), _calculate_max_buy_quantity_by_weight(), _calculate_projected_position_weight() (+33 more)

### Community 17 - "Community 17"
Cohesion: 0.1
Nodes (38): main(), build_seed_universe_from_kis_files(), diff_seed_universe(), _download_and_extract_zip_member(), download_kis_stocks_info_files(), _is_inverse_name(), _is_leveraged_name(), _is_true_flag() (+30 more)

### Community 18 - "Community 18"
Cohesion: 0.15
Nodes (15): _add_control_arguments(), _add_runtime_arguments(), _build_parser(), main(), main_daily_inspection_compat(), main_live_cycle_compat(), main_weekly_review_compat(), main() (+7 more)

### Community 19 - "Community 19"
Cohesion: 0.19
Nodes (18): _normalize_order_price_for_signal(), is_valid_krx_order_price(), is_valid_krx_stock_order_price(), is_valid_krx_symbol_order_price(), _known_krx_etf_symbols(), krx_order_tick_size(), krx_stock_tick_size(), _krx_tick_size() (+10 more)

### Community 20 - "Community 20"
Cohesion: 1.0
Nodes (1): Generated KRX regular-session holiday dates.  Regenerate with: `python tools/krx

### Community 21 - "Community 21"
Cohesion: 1.0
Nodes (0):

### Community 22 - "Community 22"
Cohesion: 1.0
Nodes (0):

### Community 23 - "Community 23"
Cohesion: 1.0
Nodes (0): 

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
Nodes (1): KIS_ACCESS_TOKEN 환경변수가 없을 때만 1회 발급합니다.     반복 발급 테스트용이 아닙니다.

### Community 37 - "Community 37"
Cohesion: 1.0
Nodes (1): Raised when order execution cannot be completed safely.

### Community 38 - "Community 38"
Cohesion: 1.0
Nodes (1): Raised when a request id is reused with different payload.

### Community 39 - "Community 39"
Cohesion: 1.0
Nodes (1): Raised when an order cannot be found in local execution state.

### Community 40 - "Community 40"
Cohesion: 1.0
Nodes (1): Raised when an order transition is not allowed.

### Community 41 - "Community 41"
Cohesion: 1.0
Nodes (1): Raised when an operation can be retried safely.

## Knowledge Gaps
- **17 isolated node(s):** `Generated KRX regular-session holiday dates.  Regenerate with: `python tools/krx`, `Deterministic, read-only signal generation contract.`, `_TrendFollowingConfig`, `Raised when order execution cannot be completed safely.`, `Raised when a request id is reused with different payload.` (+12 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 20`** (2 nodes): `krx_holidays.py`, `Generated KRX regular-session holiday dates.  Regenerate with: `python tools/krx`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 21`** (2 nodes): `operations.py`, `main()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 22`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 23`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 24`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 25`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 26`** (1 nodes): `operations.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 27`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 28`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 29`** (1 nodes): `__init__.py`
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
- **Thin community `Community 36`** (1 nodes): `KIS_ACCESS_TOKEN 환경변수가 없을 때만 1회 발급합니다.     반복 발급 테스트용이 아닙니다.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 37`** (1 nodes): `Raised when order execution cannot be completed safely.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 38`** (1 nodes): `Raised when a request id is reused with different payload.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 39`** (1 nodes): `Raised when an order cannot be found in local execution state.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 40`** (1 nodes): `Raised when an order transition is not allowed.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 41`** (1 nodes): `Raised when an operation can be retried safely.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `_configure_logging()` connect `Community 18` to `Community 3`?**
  _High betweenness centrality (0.059) - this node is a cross-community bridge._
- **Why does `Bar` connect `Community 10` to `Community 0`, `Community 2`, `Community 3`, `Community 4`, `Community 5`, `Community 6`, `Community 7`, `Community 9`, `Community 11`, `Community 12`?**
  _High betweenness centrality (0.057) - this node is a cross-community bridge._
- **Are the 164 inferred relationships involving `ValueError` (e.g. with `.__post_init__()` and `.__post_init__()`) actually correct?**
  _`ValueError` has 164 INFERRED edges - model-reasoned connections that need verification._
- **Are the 30 inferred relationships involving `KoreaInvestmentBrokerTrader` (e.g. with `BrokerNormalizationError` and `BrokerReader`) actually correct?**
  _`KoreaInvestmentBrokerTrader` has 30 INFERRED edges - model-reasoned connections that need verification._
- **Are the 45 inferred relationships involving `BrokerSettings` (e.g. with `ConfigError` and `Raised when required settings are missing or invalid.`) actually correct?**
  _`BrokerSettings` has 45 INFERRED edges - model-reasoned connections that need verification._
- **Are the 25 inferred relationships involving `PaperBroker` (e.g. with `ReplaySessionSnapshot` and `ReplayLogEntry`) actually correct?**
  _`PaperBroker` has 25 INFERRED edges - model-reasoned connections that need verification._
- **Are the 38 inferred relationships involving `FileExecutionStateStore` (e.g. with `_handle_market_close()` and `build_operation_services()`) actually correct?**
  _`FileExecutionStateStore` has 38 INFERRED edges - model-reasoned connections that need verification._
