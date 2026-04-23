# Graph Report - /home/minq/codes_wsl/AutoTrade  (2026-04-23)

## Corpus Check
- 117 files · ~211,996 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1537 nodes · 4581 edges · 37 communities detected
- Extraction: 64% EXTRACTED · 36% INFERRED · 0% AMBIGUOUS · INFERRED: 1637 edges (avg confidence: 0.76)
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
2. `KoreaInvestmentBrokerError` - 42 edges
3. `Bar` - 39 edges
4. `KoreaInvestmentBrokerTrader` - 39 edges
5. `BrokerSettings` - 38 edges
6. `FileExecutionStateStore` - 36 edges
7. `OrderExecutionEngine` - 35 edges
8. `RecordingTransport` - 34 edges
9. `KoreaInvestmentBrokerReader` - 32 edges
10. `RiskSettings` - 30 edges

## Surprising Connections (you probably didn't know these)
- `RiskSettings` --calls--> `test_risk_settings_accepts_operating_capital_limit()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/risk/models.py → /home/minq/codes_wsl/AutoTrade/tests/unit/config/test_models.py
- `RiskSettings` --calls--> `test_risk_settings_rejects_non_positive_operating_capital_limit()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/risk/models.py → /home/minq/codes_wsl/AutoTrade/tests/unit/config/test_models.py
- `BrokerSettings` --calls--> `test_broker_settings_reject_blank_hts_id()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/config/models.py → /home/minq/codes_wsl/AutoTrade/tests/unit/config/test_models.py
- `TelegramSettings` --calls--> `test_telegram_settings_require_bot_token_and_chat_id_when_enabled()`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/config/models.py → /home/minq/codes_wsl/AutoTrade/tests/unit/config/test_models.py
- `_KisDownloadSource` --uses--> `SeedUniverseEntry`  [INFERRED]
  /home/minq/codes_wsl/AutoTrade/src/autotrade/recommendation/kis_seed_universe.py → /home/minq/codes_wsl/AutoTrade/src/autotrade/recommendation/models.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.04
Nodes (81): BrokerNormalizationError, Raised when broker payloads cannot be normalized to internal models., _aggregate_intraday_bars(), _build_fill_notice_subscription_message(), _build_management_order_record_from_submission(), _build_url(), CachedAccessToken, _coerce_decimal() (+73 more)

### Community 1 - "Community 1"
Cohesion: 0.04
Nodes (108): SeedUniverseAssetScope, _apply_inspection_updates(), _build_failure_inspection_report(), _build_market_close_inspection_report(), build_market_close_job(), _build_next_day_preparation(), _daily_fills(), _daily_order_snapshots() (+100 more)

### Community 2 - "Community 2"
Cohesion: 0.05
Nodes (75): should_cancel_unfilled_orders(), _advance_market_if_supported(), _build_cancel_request_id(), build_live_cycle_job(), _build_request_id(), _build_risk_block_notification(), _calculate_current_equity(), _count_orders_submitted_today() (+67 more)

### Community 3 - "Community 3"
Cohesion: 0.04
Nodes (73): _average_traded_value(), _build_metrics(), build_recommendation_report(), _calculate_return(), _percentile_score(), _rank_candidates(), _realized_volatility(), _resolve_as_of() (+65 more)

### Community 4 - "Community 4"
Cohesion: 0.05
Nodes (61): apply_buy_fill(), apply_sell_fill(), BacktestPortfolioState, BacktestResult, BacktestTrade, build_portfolio_snapshot(), _buy_execution_price(), _calculate_buy_quantity() (+53 more)

### Community 5 - "Community 5"
Cohesion: 0.05
Nodes (63): KrxRegularSessionCalendar, _safe_stop_additional_job_results(), build_weekly_review_report(), build_safe_stop_notification(), _compose_safe_stop_detail(), _failed_jobs_reason(), _require_aware_datetime(), _require_non_blank_text() (+55 more)

### Community 6 - "Community 6"
Cohesion: 0.06
Nodes (62): _apply_fills_to_order(), _deserialize_fill(), _deserialize_order(), _deserialize_request(), _deserialize_snapshot(), _deserialize_tracked_request(), DuplicateExecutionRequestError, ExecutionEngineError (+54 more)

### Community 7 - "Community 7"
Cohesion: 0.05
Nodes (78): main(), Raised when required settings are missing or invalid., AppSettings, BrokerSettings, TelegramSettings, _approve_symbols(), _build_and_write_weekly_recommendation(), _build_and_write_weekly_review() (+70 more)

### Community 8 - "Community 8"
Cohesion: 0.09
Nodes (63): HttpRequest, HttpResponse, KoreaInvestmentBrokerReader, KoreaInvestmentBrokerTrader, _split_account(), _urllib_transport(), ExecutionFill, ExecutionOrder (+55 more)

### Community 9 - "Community 9"
Cohesion: 0.05
Nodes (44): _build_market_open_notification(), build_market_open_preparation_job(), _build_pre_market_items(), _build_preview_bar(), _build_preview_risk_account_snapshot(), _calculate_current_equity(), _collect_previous_day_errors(), CollectStrategyBars (+36 more)

### Community 10 - "Community 10"
Cohesion: 0.05
Nodes (36): BarCollectionService, BarIntegrityChecker, BarSource, BarStore, Read-only contract for loading a target universe., Read-only contract for loading OHLC bars., Write-only contract for persisting OHLC bars., Contract for validating a bar series before persistence. (+28 more)

### Community 11 - "Community 11"
Cohesion: 0.07
Nodes (30): BrokerReader, BrokerTrader, from_snapshot(), PaperBroker, PaperBrokerSnapshot, _PaperPosition, _require_non_negative_decimal(), _require_positive_int() (+22 more)

### Community 12 - "Community 12"
Cohesion: 0.1
Nodes (36): _make_bar(), _make_bars(), test_daily_trend_following_generates_buy_signal_with_reason(), test_strategy_rejects_gapped_bar_series(), test_strategy_rejects_invalid_series_structure(), test_strategy_returns_hold_for_insufficient_history(), test_thirty_minute_trend_generates_sell_signal_with_reason(), _make_bar() (+28 more)

### Community 13 - "Community 13"
Cohesion: 0.1
Nodes (38): main(), build_seed_universe_from_kis_files(), diff_seed_universe(), _download_and_extract_zip_member(), download_kis_stocks_info_files(), _is_inverse_name(), _is_leveraged_name(), _is_true_flag() (+30 more)

### Community 14 - "Community 14"
Cohesion: 0.11
Nodes (29): CompositeNotifier, _decode_telegram_payload(), _extract_retry_after_seconds(), _format_telegram_messages(), _network_retry_delay_seconds(), NotificationDeliveryError, _retry_delay_seconds(), _split_long_line() (+21 more)

### Community 15 - "Community 15"
Cohesion: 0.16
Nodes (36): _calculate_drawdown(), _calculate_loss_amount(), calculate_max_buy_quantity(), _calculate_max_buy_quantity_by_cash(), _calculate_max_buy_quantity_by_operating_capital(), _calculate_max_buy_quantity_by_weight(), _calculate_projected_position_weight(), _count_open_holdings() (+28 more)

### Community 16 - "Community 16"
Cohesion: 0.16
Nodes (33): ConfigError, _load_risk_settings(), load_settings(), load_telegram_settings(), _parse_bool_setting(), _parse_broker_environment(), _parse_decimal_setting(), _parse_float_setting() (+25 more)

### Community 17 - "Community 17"
Cohesion: 0.14
Nodes (28): BacktestConfig, BacktestCostModel, BacktestEngine, BacktestOverfitCheck, BacktestPerformanceSummary, BacktestReport, build_backtest_report(), _build_overfit_check() (+20 more)

### Community 18 - "Community 18"
Cohesion: 0.1
Nodes (26): collect_holiday_dates(), extract_holiday_dates_from_xls(), extract_holiday_dates_from_xls_bytes(), main(), render_holiday_module(), write_holiday_module(), path(), _load_daily_inspection_module() (+18 more)

### Community 19 - "Community 19"
Cohesion: 0.14
Nodes (22): ApprovedSymbolsArtifacts, deserialize_approved_symbols_record(), load_approved_symbols_record(), load_latest_approved_symbols(), _require_mapping(), _require_text(), serialize_approved_symbols_record(), write_approved_symbols() (+14 more)

### Community 20 - "Community 20"
Cohesion: 0.15
Nodes (14): _add_runtime_arguments(), _build_parser(), main(), main_daily_inspection_compat(), main_live_cycle_compat(), main_weekly_review_compat(), main(), main() (+6 more)

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

- **Why does `Bar` connect `Community 10` to `Community 0`, `Community 1`, `Community 2`, `Community 3`, `Community 4`, `Community 5`, `Community 7`, `Community 8`, `Community 9`, `Community 11`, `Community 12`, `Community 17`?**
  _High betweenness centrality (0.052) - this node is a cross-community bridge._
- **Why does `path()` connect `Community 18` to `Community 0`, `Community 3`, `Community 6`, `Community 7`, `Community 13`, `Community 16`?**
  _High betweenness centrality (0.049) - this node is a cross-community bridge._
- **Why does `FileExecutionStateStore` connect `Community 2` to `Community 1`, `Community 9`, `Community 6`, `Community 7`?**
  _High betweenness centrality (0.046) - this node is a cross-community bridge._
- **Are the 152 inferred relationships involving `ValueError` (e.g. with `.__post_init__()` and `.__post_init__()`) actually correct?**
  _`ValueError` has 152 INFERRED edges - model-reasoned connections that need verification._
- **Are the 22 inferred relationships involving `PaperBroker` (e.g. with `ReplaySessionSnapshot` and `ReplayLogEntry`) actually correct?**
  _`PaperBroker` has 22 INFERRED edges - model-reasoned connections that need verification._
- **Are the 6 inferred relationships involving `KoreaInvestmentBrokerError` (e.g. with `BrokerNormalizationError` and `BrokerReader`) actually correct?**
  _`KoreaInvestmentBrokerError` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 37 inferred relationships involving `Bar` (e.g. with `UniverseSource` and `BarSource`) actually correct?**
  _`Bar` has 37 INFERRED edges - model-reasoned connections that need verification._