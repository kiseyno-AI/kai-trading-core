from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore',
    )

    app_name: str = 'kai-trading-core-api'
    app_env: str = 'development'
    app_debug: bool = True
    upstream_bearer_token: Optional[str] = None

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

    bok_api_key: str = ''
    bok_base_url: str = 'https://ecos.bok.or.kr/api/StatisticSearch'

    koreaexim_api_key: str = ''
    koreaexim_base_url: str = 'https://www.koreaexim.go.kr/site/program/financial/exchangeJSON'

    dart_api_key: str = ''
    dart_base_url: str = 'https://opendart.fss.or.kr/api'
    dart_corp_code_cache_ttl_seconds: int = 86400


settings = Settings()
