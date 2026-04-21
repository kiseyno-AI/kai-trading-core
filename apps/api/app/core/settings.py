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

    db_auto_upgrade: bool = True
    database_url: Optional[str] = None
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
    kis_token_refresh_margin_seconds: int = 300
    kis_realtime_trending_cache_ttl_seconds: int = 20

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

    eod_auto_pykrx_enabled: bool = True
    eod_auto_pykrx_poll_seconds: int = 60
    eod_auto_pykrx_after_close_minutes: int = 10
    eod_auto_pykrx_markets: str = 'KOSPI,KOSDAQ,KONEX'
    eod_auto_pykrx_refresh_universe: bool = True
    eod_auto_pykrx_stale_run_minutes: int = 30
    eod_pykrx_request_timeout_seconds: int = 20
    eod_trigger_fundamentals_after_capture: bool = False

    openclaw_base_url: str = 'http://127.0.0.1:18789'
    openclaw_gateway_token: Optional[str] = None
    openclaw_agent_timeout_seconds: float = 0.0
    openclaw_agent_max_retries: int = 2
    openclaw_agent_retry_backoff_seconds: float = 2.0
    openclaw_agent_parallel_subtasks: int = 10
    openclaw_agent_symbol_chunk_size: int = 1
    openclaw_agent_max_prompt_chars: int = 0
    openclaw_feedback_trigger_after_fundamentals_refresh: bool = False
    openclaw_feedback_retry_max_cycles: int = 3
    openclaw_feedback_retry_outer_batch_size: int = 20
    openclaw_agent_subtask_poll_seconds: float = 2.0
    openclaw_agent_subtask_timeout_seconds: float = 300.0
    openclaw_agent_slot_cooldown_seconds: float = 600.0
    openclaw_session_cleanup_batch_limit: int = 100



settings = Settings()
