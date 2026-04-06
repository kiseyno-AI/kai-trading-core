from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from app.core.settings import settings
from app.integrations.fx.client import KoreaEximAPIError, KoreaEximClient


def _normalize_numeric(value: str | None) -> float | None:
    if value in (None, ''):
        return None
    try:
        return float(value.replace(',', ''))
    except ValueError as exc:
        raise KoreaEximAPIError(f'Korea Exim returned invalid numeric value: {value}') from exc


def _split_currency_unit(cur_unit: str | None) -> tuple[str | None, int]:
    if not cur_unit:
        return None, 1
    if '(' not in cur_unit or not cur_unit.endswith(')'):
        return cur_unit, 1

    code, _, suffix = cur_unit.partition('(')
    unit_value = suffix.rstrip(')')
    try:
        return code, int(unit_value)
    except ValueError:
        return cur_unit, 1


class KoreaEximRateNotFoundError(RuntimeError):
    pass


class KoreaEximInvalidSearchDateError(ValueError):
    pass


class KoreaEximService:
    def __init__(self, client: KoreaEximClient | None = None) -> None:
        self.client = client or KoreaEximClient()

    async def get_exchange_rates(
        self,
        *,
        search_date: str | None = None,
        currency: str | None = None,
        fallback_days: int | None = None,
        data: str = 'AP01',
    ) -> dict[str, Any]:
        requested_date = search_date or date.today().strftime('%Y%m%d')
        normalized_currency = currency.upper() if currency else None
        effective_fallback_days = settings.koreaexim_fallback_days if fallback_days is None else max(fallback_days, 0)
        try:
            requested_day = datetime.strptime(requested_date, '%Y%m%d').date()
        except ValueError as exc:
            raise KoreaEximInvalidSearchDateError(f'Invalid search_date: {requested_date}') from exc

        for day_offset in range(effective_fallback_days + 1):
            effective_date = (requested_day - timedelta(days=day_offset)).strftime('%Y%m%d')
            rows = await self.client.fetch_exchange_rates(search_date=effective_date, data=data)
            items = self._normalize_rows(rows, currency=normalized_currency)
            if items:
                return {
                    'provider': 'koreaexim',
                    'data_code': data,
                    'requested_date': requested_date,
                    'effective_date': effective_date,
                    'fallback_applied': effective_date != requested_date,
                    'currency': normalized_currency,
                    'count': len(items),
                    'items': items,
                }

        currency_text = normalized_currency or 'all currencies'
        raise KoreaEximRateNotFoundError(
            f'No Korea Exim FX rates found for {currency_text} from {requested_date} within {effective_fallback_days} fallback days'
        )

    def _normalize_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        currency: str | None = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for row in rows:
            code, unit = _split_currency_unit(row.get('cur_unit'))
            if currency and code != currency:
                continue

            items.append(
                {
                    'currency_code': code,
                    'currency_name': row.get('cur_nm'),
                    'currency_unit': unit,
                    'raw_currency_unit': row.get('cur_unit'),
                    'telegraphic_buy_rate': _normalize_numeric(row.get('ttb')),
                    'telegraphic_sell_rate': _normalize_numeric(row.get('tts')),
                    'exchange_rate': _normalize_numeric(row.get('deal_bas_r')),
                    'book_price': _normalize_numeric(row.get('bkpr')),
                    'yearly_fee_rate': _normalize_numeric(row.get('yy_efee_r')),
                    'ten_day_fee_rate': _normalize_numeric(row.get('ten_dd_efee_r')),
                    'kftc_book_price': _normalize_numeric(row.get('kftc_bkpr')),
                    'kftc_exchange_rate': _normalize_numeric(row.get('kftc_deal_bas_r')),
                    'result_code': str(row.get('result')) if row.get('result') is not None else None,
                }
            )
        return items


fx_service = KoreaEximService()
