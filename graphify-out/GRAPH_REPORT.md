# Graph Report - /home/minq/codes_wsl/AutoTrade  (2026-04-20)

## Corpus Check
- 114 files · ~194,782 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1428 nodes · 4130 edges · 37 communities detected
- Extraction: 64% EXTRACTED · 36% INFERRED · 0% AMBIGUOUS · INFERRED: 1473 edges (avg confidence: 0.76)
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

## God Nodes (most connected - your core abstractions)
1. `PaperBroker` - 44 edges
2. `Bar` - 39 edges
3. `OrderExecutionEngine` - 35 edges
4. `KoreaInvestmentBrokerError` - 35 edges
5. `KoreaInvestmentBrokerTrader` - 34 edges
6. `BrokerSettings` - 31 edges
7. `FileExecutionStateStore` - 31 edges
8. `RiskSettings` - 30 edges
9. `RecordingTransport` - 30 edges
10. `KoreaInvestmentBrokerReader` - 29 edges

## Surprising Connections (you probably didn't know these)
- `RiskSettings` --calls--> `test_risk_settings_accepts_operating_capital_limit()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/risk/models.py → /home/minq/codes_wsl/AutoTrade/tests/unit/config/test_models.py
- `RiskSettings` --calls--> `test_risk_settings_rejects_non_positive_operating_capital_limit()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/risk/models.py → /home/minq/codes_wsl/AutoTrade/tests/unit/config/test_models.py
- `TelegramSettings` --calls--> `test_telegram_settings_require_bot_token_and_chat_id_when_enabled()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/config/models.py → /home/minq/codes_wsl/AutoTrade/tests/unit/config/test_models.py
- `main()` --calls--> `_configure_logging()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/cli.py → /home/minq/codes_wsl/AutoTrade/src/autotrade/runtime/operations.py
- `CsvBarStore` --uses--> `Bar`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/data/storage.py → /home/minq/codes_wsl/AutoTrade/src/autotrade/data/models.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.04
Nodes (104): _apply_inspection_updates(), _build_failure_inspection_report(), _build_market_close_inspection_report(), build_market_close_job(), _build_next_day_preparation(), _daily_fills(), _daily_order_snapshots(), _market_close_detail() (+96 more)

### Community 1 - "Community 1"
Cohesion: 0.05
Nodes (68): BrokerNormalizationError, Raised when broker payloads cannot be normalized to internal models., _aggregate_intraday_bars(), _build_management_order_record_from_submission(), _build_url(), CachedAccessToken, _coerce_decimal(), _coerce_int() (+60 more)

### Community 2 - "Community 2"
Cohesion: 0.04
Nodes (65): apply_buy_fill(), apply_sell_fill(), BacktestPortfolioState, BacktestResult, BacktestTrade, build_portfolio_snapshot(), _buy_execution_price(), _calculate_buy_quantity() (+57 more)

### Community 3 - "Community 3"
Cohesion: 0.05
Nodes (71): _advance_market_if_supported(), _build_cancel_request_id(), build_live_cycle_job(), _build_request_id(), _build_risk_block_notification(), _calculate_current_equity(), _count_orders_submitted_today(), _count_unfilled_orders() (+63 more)

### Community 4 - "Community 4"
Cohesion: 0.04
Nodes (71): _average_traded_value(), _build_metrics(), build_recommendation_report(), _calculate_return(), _percentile_score(), _rank_candidates(), _realized_volatility(), _resolve_as_of() (+63 more)

### Community 5 - "Community 5"
Cohesion: 0.05
Nodes (61): KrxRegularSessionCalendar, build_market_open_preparation_job(), build_weekly_review_report(), build_market_close_job(), build_scheduled_cycle_job(), build_safe_stop_notification(), _compose_safe_stop_detail(), _failed_jobs_reason() (+53 more)

### Community 6 - "Community 6"
Cohesion: 0.04
Nodes (49): BarCollectionService, BarIntegrityChecker, BarSource, BarStore, Read-only contract for loading a target universe., Read-only contract for loading OHLC bars., Write-only contract for persisting OHLC bars., Contract for validating a bar series before persistence. (+41 more)

### Community 7 - "Community 7"
Cohesion: 0.07
Nodes (58): _apply_fills_to_order(), _deserialize_fill(), _deserialize_order(), _deserialize_request(), _deserialize_snapshot(), _deserialize_tracked_request(), DuplicateExecutionRequestError, ExecutionEngineError (+50 more)

### Community 8 - "Community 8"
Cohesion: 0.04
Nodes (58): main(), HttpResponse, _urllib_transport(), Raised when required settings are missing or invalid., _build_market_open_notification(), _build_pre_market_items(), _build_preview_bar(), _build_preview_risk_account_snapshot() (+50 more)

### Community 9 - "Community 9"
Cohesion: 0.09
Nodes (53): KoreaInvestmentBrokerReader, _split_account(), ExecutionOrder, OrderAmendRequest, OrderCancelRequest, OrderRequest, _ContractTransport, DummyBrokerReader (+45 more)

### Community 10 - "Community 10"
Cohesion: 0.07
Nodes (58): TelegramSettings, _approve_symbols(), _build_and_write_weekly_recommendation(), _build_and_write_weekly_review(), _build_market_close_job(), _build_notifier(), _build_operation_services(), _build_paper_broker() (+50 more)

### Community 11 - "Community 11"
Cohesion: 0.08
Nodes (26): BrokerReader, BrokerTrader, from_snapshot(), PaperBroker, PaperBrokerSnapshot, _PaperPosition, _require_non_negative_decimal(), _require_positive_int() (+18 more)

### Community 12 - "Community 12"
Cohesion: 0.1
Nodes (30): CompositeNotifier, _decode_telegram_payload(), _extract_retry_after_seconds(), _format_telegram_messages(), _network_retry_delay_seconds(), NotificationDeliveryError, _retry_delay_seconds(), _split_long_line() (+22 more)

### Community 13 - "Community 13"
Cohesion: 0.16
Nodes (37): _calculate_drawdown(), _calculate_loss_amount(), calculate_max_buy_quantity(), _calculate_max_buy_quantity_by_cash(), _calculate_max_buy_quantity_by_operating_capital(), _calculate_max_buy_quantity_by_weight(), _calculate_projected_position_weight(), _count_open_holdings() (+29 more)

### Community 14 - "Community 14"
Cohesion: 0.16
Nodes (32): ConfigError, _load_risk_settings(), load_settings(), load_telegram_settings(), _parse_bool_setting(), _parse_broker_environment(), _parse_decimal_setting(), _parse_float_setting() (+24 more)

### Community 15 - "Community 15"
Cohesion: 0.14
Nodes (28): BacktestConfig, BacktestCostModel, BacktestEngine, BacktestOverfitCheck, BacktestPerformanceSummary, BacktestReport, build_backtest_report(), _build_overfit_check() (+20 more)

### Community 16 - "Community 16"
Cohesion: 0.13
Nodes (23): ApprovedSymbolsArtifacts, deserialize_approved_symbols_record(), load_approved_symbols_record(), load_latest_approved_symbols(), _require_mapping(), _require_text(), serialize_approved_symbols_record(), write_approved_symbols() (+15 more)

### Community 17 - "Community 17"
Cohesion: 0.13
Nodes (21): collect_holiday_dates(), extract_holiday_dates_from_xls(), extract_holiday_dates_from_xls_bytes(), main(), render_holiday_module(), write_holiday_module(), _default_token_cache_path(), _resolve_raw_log_directory() (+13 more)

### Community 18 - "Community 18"
Cohesion: 0.19
Nodes (22): _make_bar(), test_normalize_symbol_rejects_blank_values(), test_normalize_symbol_strips_and_uppercases(), test_normalize_symbols_preserves_order(), test_normalize_symbols_rejects_duplicates_after_normalization(), test_validate_bar_series_accepts_contiguous_series(), test_validate_bar_series_allows_session_boundary_gap(), test_validate_bar_series_allows_weekend_boundary_gap() (+14 more)

### Community 19 - "Community 19"
Cohesion: 0.15
Nodes (14): _add_runtime_arguments(), _build_parser(), main(), main_daily_inspection_compat(), main_live_cycle_compat(), main_weekly_review_compat(), main(), main() (+6 more)

### Community 20 - "Community 20"
Cohesion: 0.2
Nodes (14): _make_bar(), _make_bars(), test_daily_trend_following_generates_buy_signal_with_reason(), test_strategy_rejects_gapped_bar_series(), test_strategy_rejects_invalid_series_structure(), test_strategy_returns_hold_for_insufficient_history(), test_thirty_minute_trend_generates_sell_signal_with_reason(), _build_signal() (+6 more)

### Community 21 - "Community 21"
Cohesion: 1.0
Nodes (1): Generated KRX regular-session holiday dates.  Regenerate with: `python tools/krx

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
Nodes (0): 

## Knowledge Gaps
- **11 isolated node(s):** `Generated KRX regular-session holiday dates.  Regenerate with: `python tools/krx`, `Deterministic, read-only signal generation contract.`, `_TrendFollowingConfig`, `Raised when order execution cannot be completed safely.`, `Raised when a request id is reused with different payload.` (+6 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 21`** (2 nodes): `krx_holidays.py`, `Generated KRX regular-session holiday dates.  Regenerate with: `python tools/krx`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 22`** (2 nodes): `operations.py`, `main()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 23`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 24`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 25`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 26`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 27`** (1 nodes): `operations.py`
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
- **Thin community `Community 36`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Bar` connect `Community 6` to `Community 0`, `Community 1`, `Community 2`, `Community 3`, `Community 4`, `Community 5`, `Community 8`, `Community 9`, `Community 10`, `Community 11`, `Community 15`, `Community 18`, `Community 20`?**
  _High betweenness centrality (0.059) - this node is a cross-community bridge._
- **Why does `BrokerSettings` connect `Community 8` to `Community 0`, `Community 1`, `Community 3`, `Community 9`, `Community 10`, `Community 14`?**
  _High betweenness centrality (0.051) - this node is a cross-community bridge._
- **Why does `ConfigError` connect `Community 14` to `Community 8`, `Community 2`, `Community 10`?**
  _High betweenness centrality (0.045) - this node is a cross-community bridge._
- **Are the 146 inferred relationships involving `ValueError` (e.g. with `.__post_init__()` and `.__post_init__()`) actually correct?**
  _`ValueError` has 146 INFERRED edges - model-reasoned connections that need verification._
- **Are the 22 inferred relationships involving `PaperBroker` (e.g. with `ReplaySessionSnapshot` and `ReplayLogEntry`) actually correct?**
  _`PaperBroker` has 22 INFERRED edges - model-reasoned connections that need verification._
- **Are the 37 inferred relationships involving `Bar` (e.g. with `UniverseSource` and `BarSource`) actually correct?**
  _`Bar` has 37 INFERRED edges - model-reasoned connections that need verification._
- **Are the 21 inferred relationships involving `OrderExecutionEngine` (e.g. with `.__post_init__()` and `test_replay_session_does_not_fill_restored_order_from_replayed_older_bar()`) actually correct?**
  _`OrderExecutionEngine` has 21 INFERRED edges - model-reasoned connections that need verification._