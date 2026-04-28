from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.brokers.kis.client import KISClient
from app.brokers.kis.provider import kis_client
from app.core.settings import settings
from app.fx.service import KoreaEximService, fx_service


class MarketSnapshotConfigurationError(RuntimeError):
    pass


class MarketSnapshotUpstreamError(RuntimeError):
    pass


@dataclass(frozen=True)
class MarketSnapshotTarget:
    key: str
    label: str
    index_code: str
    investor_market_code: str
    investor_sector_code: str


# Official KIS choices for the market-condition card:
# - "국내업종 현재지수" (`inquire-index-price`, TR `FHPUP02100000`) returns the
#   current KOSPI/KOSDAQ level, change, breadth, and trading activity in one call.
# - "시장별 투자자매매동향(시세)" (`inquire-investor-time-by-market`,
#   TR `FHPTJ04030000`) returns market-level intraday participant flow, which is a
#   better fit for a compact market-condition card than per-symbol or top-list APIs.
MARKET_SNAPSHOT_TARGETS: tuple[MarketSnapshotTarget, ...] = (
    MarketSnapshotTarget(
        key='kospi',
        label='KOSPI',
        index_code='0001',
        investor_market_code='KSP',
        investor_sector_code='0001',
    ),
    MarketSnapshotTarget(
        key='kosdaq',
        label='KOSDAQ',
        index_code='1001',
        investor_market_code='KSQ',
        investor_sector_code='1001',
    ),
)


def _parse_float(value: Any, *, field_name: str) -> float | None:
    if value in (None, ''):
        return None
    text = str(value).replace(',', '').strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise MarketSnapshotUpstreamError(f'KIS returned a non-numeric {field_name}: {value!r}') from exc


def _parse_int(value: Any, *, field_name: str) -> int | None:
    if value in (None, ''):
        return None
    text = str(value).replace(',', '').strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError as exc:
            raise MarketSnapshotUpstreamError(f'KIS returned a non-integer {field_name}: {value!r}') from exc


def _require_mapping(value: Any, *, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise MarketSnapshotUpstreamError(f'KIS payload did not include a valid {field_name} object')


def _normalize_compact_date(value: Any) -> str | None:
    if value in (None, ''):
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f'{text[:4]}-{text[4:6]}-{text[6:8]}'
    return text


class MarketSnapshotService:
    def __init__(
        self,
        *,
        kis_api_client: KISClient | None = None,
        koreaexim_service: KoreaEximService | None = None,
    ) -> None:
        self.kis_api_client = kis_api_client or kis_client
        self.koreaexim_service = koreaexim_service or fx_service

    async def get_market_snapshot(self) -> dict[str, Any]:
        if settings.kis_use_mock:
            raise MarketSnapshotConfigurationError(
                'KIS market snapshot requires KIS_USE_MOCK=false because the selected index and investor-flow APIs are real-domain only'
            )

        # Prime the shared token cache once before fanning out multiple KIS reads.
        await self.kis_api_client.issue_access_token()

        market_rows, usd_krw_payload = await asyncio.gather(
            asyncio.gather(*(self._fetch_market_row(target) for target in MARKET_SNAPSHOT_TARGETS)),
            self.koreaexim_service.get_exchange_rates(currency='USD'),
        )

        return {
            'as_of': datetime.now(ZoneInfo('Asia/Seoul')),
            'markets': {
                target.key: row
                for target, row in zip(MARKET_SNAPSHOT_TARGETS, market_rows, strict=True)
            },
            'usd_krw': self._build_usd_krw_snapshot(usd_krw_payload),
        }

    async def _fetch_market_row(self, target: MarketSnapshotTarget) -> dict[str, Any]:
        index_payload, flow_payload = await asyncio.gather(
            self.kis_api_client.fetch_market_index_price(index_code=target.index_code),
            self.kis_api_client.fetch_investor_time_by_market(
                market_code=target.investor_market_code,
                sector_code=target.investor_sector_code,
            ),
        )

        index_output = _require_mapping(index_payload.get('output'), field_name='output')
        flow_output = self._extract_flow_output(flow_payload)

        return {
            'label': target.label,
            'index_code': target.index_code,
            'value': _parse_float(index_output.get('bstp_nmix_prpr'), field_name='bstp_nmix_prpr'),
            'change': _parse_float(index_output.get('bstp_nmix_prdy_vrss'), field_name='bstp_nmix_prdy_vrss'),
            'change_percent': _parse_float(
                index_output.get('bstp_nmix_prdy_ctrt'),
                field_name='bstp_nmix_prdy_ctrt',
            ),
            'open': _parse_float(index_output.get('bstp_nmix_oprc'), field_name='bstp_nmix_oprc'),
            'high': _parse_float(index_output.get('bstp_nmix_hgpr'), field_name='bstp_nmix_hgpr'),
            'low': _parse_float(index_output.get('bstp_nmix_lwpr'), field_name='bstp_nmix_lwpr'),
            'volume': _parse_int(index_output.get('acml_vol'), field_name='acml_vol'),
            'trading_value': _parse_int(index_output.get('acml_tr_pbmn'), field_name='acml_tr_pbmn'),
            'advancers': _parse_int(index_output.get('ascn_issu_cnt'), field_name='ascn_issu_cnt'),
            'unchanged': _parse_int(index_output.get('stnr_issu_cnt'), field_name='stnr_issu_cnt'),
            'decliners': _parse_int(index_output.get('down_issu_cnt'), field_name='down_issu_cnt'),
            'foreign_net_quantity': _parse_int(flow_output.get('frgn_ntby_qty'), field_name='frgn_ntby_qty'),
            'foreign_net_value': _parse_int(flow_output.get('frgn_ntby_tr_pbmn'), field_name='frgn_ntby_tr_pbmn'),
            'institution_net_quantity': _parse_int(flow_output.get('orgn_ntby_qty'), field_name='orgn_ntby_qty'),
            'institution_net_value': _parse_int(
                flow_output.get('orgn_ntby_tr_pbmn'),
                field_name='orgn_ntby_tr_pbmn',
            ),
        }

    def _extract_flow_output(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        rows = payload.get('output')
        if not isinstance(rows, list) or not rows:
            raise MarketSnapshotUpstreamError('KIS investor-flow payload did not include a usable output row')
        first_row = rows[0]
        return _require_mapping(first_row, field_name='output[0]')

    def _build_usd_krw_snapshot(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        items = payload.get('items')
        if not isinstance(items, list) or not items:
            raise MarketSnapshotUpstreamError('Korea Exim USD payload did not include any items')
        first_row = items[0]
        if not isinstance(first_row, Mapping):
            raise MarketSnapshotUpstreamError('Korea Exim USD payload returned a malformed rate row')

        return {
            'pair': 'USD/KRW',
            'rate': first_row.get('exchange_rate'),
            'effective_date': _normalize_compact_date(payload.get('effective_date')),
            'fallback_applied': bool(payload.get('fallback_applied')),
        }


market_snapshot_service = MarketSnapshotService()
