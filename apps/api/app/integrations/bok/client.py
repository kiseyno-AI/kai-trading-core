from __future__ import annotations

import httpx
from urllib.parse import quote

from app.core.audit_logging import log_fetch_event, request_with_audit
from app.core.settings import settings


class ECOSAPIError(RuntimeError):
    pass


class ECOSConfigurationError(RuntimeError):
    pass


class ECOSClient:
    def __init__(self) -> None:
        self.base_url = settings.bok_ecos_base_url.rstrip('/')
        self.api_key = settings.bok_ecos_api_key.strip()

    async def fetch_series(
        self,
        *,
        stat_code: str,
        cycle: str,
        start_period: str,
        end_period: str,
        item_code_1: str | None = None,
        item_code_2: str | None = None,
        item_code_3: str | None = None,
        item_code_4: str | None = None,
        start_row: int = 1,
        end_row: int = 100,
        lang: str = 'kr',
    ) -> dict:
        if not self.api_key:
            raise ECOSConfigurationError('BOK_ECOS_API_KEY is not configured')

        path_parts = [
            'StatisticSearch',
            quote(self.api_key, safe=''),
            'json',
            quote(lang, safe=''),
            str(start_row),
            str(end_row),
            quote(stat_code, safe=''),
            quote(cycle, safe=''),
            quote(start_period, safe=''),
            quote(end_period, safe=''),
        ]
        item_codes = [item_code_1, item_code_2, item_code_3, item_code_4]
        while item_codes and not item_codes[-1]:
            item_codes.pop()
        for item_code in item_codes:
            path_parts.append(quote(item_code or '', safe=''))

        url = f"{self.base_url}/{'/'.join(path_parts)}"
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await request_with_audit(
                client,
                'bok_ecos',
                'GET',
                url,
                secret_values=[self.api_key],
            )
            if response.status_code >= 400:
                raise ECOSAPIError(f'ECOS request failed [{response.status_code}]: {response.text}')

        payload = response.json()
        result = payload.get('RESULT')
        if isinstance(result, dict) and result.get('CODE') not in (None, 'INFO-000'):
            message = result.get('MESSAGE') or 'Unknown ECOS error'
            log_fetch_event(
                service='bok_ecos',
                operation='fetch_series',
                status='failed',
                stat_code=stat_code,
                cycle=cycle,
                start_period=start_period,
                end_period=end_period,
                item_code_1=item_code_1,
                item_code_2=item_code_2,
                item_code_3=item_code_3,
                item_code_4=item_code_4,
                result_code=result['CODE'],
                error_message=message,
            )
            raise ECOSAPIError(f"ECOS error [{result['CODE']}]: {message}")
        rows = payload.get('StatisticSearch', {}).get('row', []) if isinstance(payload.get('StatisticSearch'), dict) else []
        log_fetch_event(
            service='bok_ecos',
            operation='fetch_series',
            status='completed',
            stat_code=stat_code,
            cycle=cycle,
            start_period=start_period,
            end_period=end_period,
            item_code_1=item_code_1,
            item_code_2=item_code_2,
            item_code_3=item_code_3,
            item_code_4=item_code_4,
            row_count=len(rows),
        )
        return payload


BOKClient = ECOSClient
