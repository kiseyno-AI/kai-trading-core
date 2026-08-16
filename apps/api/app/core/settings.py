from typing import Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra='ignore',
    )

    app_name: str = 'kai-trading-core-api'
    app_env: str = 'development'
    app_debug: bool = True
    upstream_bearer_token: Optional[str] = None
    api_request_logging_enabled: bool = True
    upstream_http_logging_enabled: bool = True
    api_log_directory: str = 'var/log'
    api_request_log_file_name: str = 'api-requests.log'
    upstream_http_log_file_name: str = 'upstream-http.log'
    api_log_rotation_max_bytes: int = 20 * 1024 * 1024
    api_log_rotation_backup_count: int = 10
    startup_maintenance_enabled: bool = True
    background_runner_enabled: bool = True

    db_auto_upgrade: bool = True
    database_url: Optional[str] = None
    db_pool_size: int = Field(default=10, ge=1, le=200)
    db_max_overflow: int = Field(default=10, ge=0, le=200)
    db_pool_timeout_seconds: int = Field(default=5, ge=1, le=120)
    db_pool_recycle_seconds: int = Field(default=900, ge=0, le=86_400)
    db_pool_use_lifo: bool = True
    db_connect_timeout_seconds: int = Field(default=5, ge=1, le=120)
    db_healthcheck_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    db_long_checkout_warning_seconds: float = Field(default=10.0, gt=0, le=3600)
    db_statement_timeout_ms: int = 3000
    db_lock_timeout_ms: int = 5000
    agent_feedback_db_statement_timeout_ms: int = Field(default=30_000, ge=3_000, le=300_000)
    agent_feedback_db_lock_timeout_ms: int = Field(default=15_000, ge=1_000, le=120_000)
    db_pool_watchdog_enabled: bool = False
    db_pool_watchdog_cancel_enabled: bool = False
    db_pool_watchdog_interval_seconds: float = 10.0
    db_pool_watchdog_cancel_after_seconds: int = 30
    db_pool_watchdog_active_connection_threshold: int = 8
    db_pool_watchdog_cancel_limit: int = 10
    postgres_host: str = 'localhost'
    postgres_port: int = 5432
    postgres_db: str = 'kai_trading'
    postgres_user: str = 'kai_trading_app'
    postgres_password: str = 'kai_trading_dev'

    kis_app_key: str = ''
    kis_app_secret: str = ''
    kis_base_url: str = 'https://openapi.koreainvestment.com:9443'
    kis_mock_base_url: str = 'https://openapivts.koreainvestment.com:29443'
    kis_use_mock: bool = True
    market_snapshot_kis_use_real: bool = True
    market_snapshot_refresh_interval_seconds: int = 3600
    kis_hts_id: str = ''
    kis_token_refresh_margin_seconds: int = 300
    kis_realtime_trending_cache_ttl_seconds: int = 20
    kis_realtime_order_notice_enabled: bool = True
    kis_realtime_price_enabled: bool = True
    kis_realtime_price_tr_id: str = 'H0STCNT0'
    kis_realtime_price_max_subscriptions: int = 40
    kis_realtime_price_subscription_refresh_seconds: float = 30.0
    kis_realtime_price_idle_poll_seconds: float = 5.0
    kis_realtime_price_stale_after_seconds: float = 30.0
    kis_realtime_price_subscribe_pause_seconds: float = 0.05
    kis_realtime_price_unsubscribe_tr_type: str = '2'
    kis_realtime_price_sse_interval_seconds: float = 1.0
    kis_realtime_ws_url: str = 'ws://ops.koreainvestment.com:21000'
    kis_realtime_mock_ws_url: str = 'ws://ops.koreainvestment.com:31000'
    kis_realtime_reconnect_base_delay_seconds: float = 3.0
    kis_realtime_reconnect_max_delay_seconds: float = 30.0
    kis_order_reconcile_enabled: bool = True
    kis_order_reconcile_startup_enabled: bool = True
    kis_order_reconcile_on_reconnect_enabled: bool = True
    kis_order_reconcile_interval_seconds: int = 300
    kis_order_reconcile_stale_after_minutes: int = 15
    kis_order_reconcile_recent_days: int = 3
    kis_order_reconcile_batch_limit: int = 100
    kis_intraday_snapshot_enabled: bool = True
    kis_intraday_snapshot_market_code: str = 'J'
    kis_intraday_snapshot_max_ranking_pages: int = 2
    kis_intraday_snapshot_news_pages: int = 2
    kis_intraday_snapshot_news_limit: int = 5
    portfolio_account_references: str = ''
    portfolio_summary_cache_ttl_seconds: int = 60
    signal_decider_next_open_enabled: bool = False
    signal_decider_next_open_dry_run: bool = True
    signal_decider_next_open_account_targets: str = ''
    signal_decider_next_open_min_confidence_score_10: float = 6.0
    signal_decider_next_open_min_expected_return_pct: float = 0.0
    signal_decider_next_open_min_reward_risk_ratio: float = Field(default=1.30, gt=0, le=10)
    signal_decider_next_open_require_active_policy: bool = True
    signal_decider_next_open_block_on_data_warnings: bool = True
    signal_decider_next_open_block_on_specialist_disagreement: bool = True
    signal_decider_next_open_require_target_and_stop: bool = True
    signal_decider_wait_conflict_gate_mode: str = 'report_only'
    signal_decider_exec_integration_mode: str = 'disabled'
    signal_decider_exec_base_url: str = 'http://127.0.0.1:8010'
    signal_decider_exec_bearer_token: Optional[str] = None
    signal_decider_exec_dispatch_source: str = 'kai-trading-core'
    signal_decider_exec_runtime_mode: str = 'mock'
    signal_decider_exec_request_timeout_seconds: float = 10.0
    signal_decider_exec_dashboard_cache_ttl_seconds: int = 5
    signal_decider_exec_dashboard_refresh_debounce_seconds: float = 15.0
    signal_decider_exec_dashboard_refresh_timeout_seconds: float = 2.0
    signal_decider_intraday_auto_sell_enabled: bool = False
    signal_decider_intraday_auto_sell_dry_run: bool = True
    signal_decider_intraday_auto_sell_cooldown_seconds: int = 120

    bok_ecos_api_key: str = Field(
        default='',
        validation_alias=AliasChoices('BOK_ECOS_API_KEY', 'BOK_API_KEY'),
    )
    bok_ecos_base_url: str = Field(
        default='https://ecos.bok.or.kr/api',
        validation_alias=AliasChoices('BOK_ECOS_BASE_URL', 'BOK_BASE_URL'),
    )

    koreaexim_api_key: str = ''
    koreaexim_base_url: str = 'https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON'
    koreaexim_fallback_days: int = 2

    macro_context_enabled: bool = True
    macro_context_refresh_min_interval_hours: int = 24
    macro_context_stale_after_hours: int = 72
    macro_context_request_timeout_seconds: float = 15.0
    macro_context_world_bank_enabled: bool = True
    macro_context_world_bank_base_url: str = 'https://api.worldbank.org/v2'
    macro_context_world_bank_country: str = 'KOR'
    macro_context_world_bank_series: str = 'NY.GDP.MKTP.KD.ZG,FP.CPI.TOTL.ZG,SL.UEM.TOTL.ZS'
    macro_context_world_bank_mrnev: int = 1
    macro_context_fred_enabled: bool = True
    macro_context_fred_series: str = 'DEXKOUS'
    fred_api_key: str = ''
    fred_base_url: str = 'https://api.stlouisfed.org'

    upbit_base_url: str = 'https://api.upbit.com'
    upbit_request_timeout_seconds: float = 10.0
    upbit_market_whitelist: str = 'KRW-BTC,KRW-ETH'

    naver_search_client_id: str = ''
    naver_search_client_secret: str = ''
    naver_search_base_url: str = 'https://openapi.naver.com/v1/search/news.json'
    external_news_default_display: int = 10
    external_news_request_timeout_seconds: float = 10.0
    external_news_context_enabled: bool = True
    external_news_context_limit: int = 3
    external_news_context_lookback_days: int = 14
    external_news_collect_throttle_seconds: float = 0.15

    dart_api_key: str = ''
    dart_base_url: str = 'https://opendart.fss.or.kr/api'
    dart_corp_code_cache_ttl_seconds: int = 86400
    dart_sync_recent_days: int = 7
    dart_sync_page_count: int = 100
    dart_fundamentals_min_business_year: int = 2015
    dart_fundamentals_default_report_code: str = '11011'
    dart_fundamentals_default_fs_div: str = 'CFS'
    dart_fundamentals_daily_retry_cycles: int = 3
    dart_fundamentals_event_timeout_seconds: int = 60
    dart_fundamentals_stale_run_minutes: int = 60
    dart_fundamentals_daily_initial_backfill_enabled: bool = False
    dart_fundamentals_daily_incremental_lookback_days: int = 45

    eod_auto_pykrx_enabled: bool = True
    eod_auto_pykrx_poll_seconds: int = 60
    eod_auto_pykrx_after_close_minutes: int = 10
    eod_auto_pykrx_markets: str = 'KOSPI,KOSDAQ,KONEX'
    eod_auto_pykrx_refresh_universe: bool = True
    eod_auto_pykrx_stale_run_minutes: int = 30
    eod_pykrx_request_timeout_seconds: int = 20
    eod_pykrx_enrichment_pause_seconds: float = 0.05
    eod_pykrx_enrichment_retry_count: int = 2
    eod_pykrx_enrichment_retry_backoff_seconds: float = 0.5

    openclaw_base_url: str = 'http://127.0.0.1:18789'
    openclaw_gateway_token: Optional[str] = None
    openclaw_state_dir: str = '~/.openclaw'
    openclaw_agent_timeout_seconds: float = 0.0
    openclaw_agent_max_retries: int = 2
    openclaw_agent_retry_backoff_seconds: float = 2.0
    openclaw_agent_parallel_subtasks: int = 5
    openclaw_agent_symbol_chunk_size: int = 1
    openclaw_agent_max_prompt_chars: int = 0
    openclaw_feedback_trigger_after_fundamentals_refresh: bool = False
    openclaw_feedback_retry_max_cycles: int = 3
    openclaw_feedback_retry_outer_batch_size: int = 20
    openclaw_feedback_broad_candidate_cap_percentile: float = Field(
        default=0.10,
        validation_alias=AliasChoices(
            'OPENCLAW_FEEDBACK_BROAD_CANDIDATE_CAP_PERCENTILE',
            'OPENCLAW_FEEDBACK_CANDIDATE_CAP_PERCENTILE',
        ),
    )
    openclaw_feedback_broad_candidate_cap_min_symbols: int = Field(
        default=150,
        validation_alias=AliasChoices(
            'OPENCLAW_FEEDBACK_BROAD_CANDIDATE_CAP_MIN_SYMBOLS',
            'OPENCLAW_FEEDBACK_CANDIDATE_CAP_MIN_SYMBOLS',
        ),
    )
    openclaw_feedback_broad_candidate_cap_max_symbols: int = Field(
        default=200,
        validation_alias=AliasChoices(
            'OPENCLAW_FEEDBACK_BROAD_CANDIDATE_CAP_MAX_SYMBOLS',
            'OPENCLAW_FEEDBACK_CANDIDATE_CAP_MAX_SYMBOLS',
        ),
    )
    openclaw_feedback_candidate_cap_percentile: float = 0.10
    openclaw_feedback_candidate_cap_min_symbols: int = 150
    openclaw_feedback_candidate_cap_max_symbols: int = 300
    openclaw_feedback_role_cap_eod_summarizer: int = 200
    openclaw_feedback_role_cap_technical_summarizer: int = 150
    openclaw_feedback_role_cap_fundamental_summarizer: int = 120
    openclaw_feedback_role_cap_short_term_decider: int = 80
    openclaw_feedback_role_cap_signal_decider: int = 80
    openclaw_feedback_requested_scope_cap_bypass_max_symbols: int = 50
    openclaw_feedback_force_include_holdings_enabled: bool = True
    openclaw_feedback_force_include_active_watch_enabled: bool = True
    openclaw_feedback_force_include_prior_decisions_enabled: bool = True
    openclaw_feedback_force_include_prior_missed_opportunities_enabled: bool = True
    openclaw_feedback_force_include_prior_missed_opportunities_broad_scope_enabled: bool = False
    openclaw_feedback_force_include_abnormal_movement_broad_scope_enabled: bool = False
    openclaw_feedback_force_include_over_role_cap_broad_scope_enabled: bool = False
    openclaw_feedback_force_include_event_symbols_enabled: bool = True
    openclaw_feedback_upstream_reuse_enabled: bool = True
    openclaw_feedback_prior_block_history_enabled: bool = True
    openclaw_feedback_fast_path_bypass_prerequisite_gate: bool = True
    openclaw_agent_subtask_poll_seconds: float = 2.0
    openclaw_agent_batch_timeout_seconds: float = 300.0
    openclaw_agent_slot_cooldown_seconds: float = 600.0
    openclaw_session_cleanup_timeout_seconds: float = 30.0
    openclaw_session_cleanup_batch_limit: int = 100



settings = Settings()
