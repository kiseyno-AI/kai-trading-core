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


@dataclass(frozen=True)
class MarketSnapshotRuntimeStatus:
    ready: bool
    global_kis_environment: str
    effective_kis_environment: str
    effective_kis_base_url: str
    uses_real_domain_override: bool
    market_snapshot_kis_use_real: bool
    kis_app_key_configured: bool
    kis_app_secret_configured: bool
    koreaexim_api_key_configured: bool
    blocking_reasons: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            'ready': self.ready,
            'global_kis_environment': self.global_kis_environment,
            'effective_kis_environment': self.effective_kis_environment,
            'effective_kis_base_url': self.effective_kis_base_url,
            'uses_real_domain_override': self.uses_real_domain_override,
            'market_snapshot_kis_use_real': self.market_snapshot_kis_use_real,
            'kis_app_key_configured': self.kis_app_key_configured,
            'kis_app_secret_configured': self.kis_app_secret_configured,
            'koreaexim_api_key_configured': self.koreaexim_api_key_configured,
            'blocking_reasons': list(self.blocking_reasons),
        }


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
        real_domain_kis_api_client: KISClient | None = None,
    ) -> None:
        self._uses_default_kis_client = kis_api_client is None
        self.kis_api_client = kis_api_client or kis_client
        self.koreaexim_service = koreaexim_service or fx_service
        self.real_domain_kis_api_client = real_domain_kis_api_client

    def runtime_status(self) -> dict[str, Any]:
        return self._runtime_status().to_payload()

    def _runtime_status(self) -> MarketSnapshotRuntimeStatus:
        global_kis_environment = 'demo' if settings.kis_use_mock else 'real'
        uses_real_domain_override = bool(settings.kis_use_mock and settings.market_snapshot_kis_use_real)
        effective_kis_environment = 'real' if settings.market_snapshot_kis_use_real else global_kis_environment
        effective_kis_base_url = (
            settings.kis_base_url if effective_kis_environment == 'real' else settings.kis_mock_base_url
        )

        blocking_reasons: list[str] = []
        if effective_kis_environment != 'real':
            blocking_reasons.append(
                'Market snapshot requires real-domain KIS access because the selected index and investor-flow APIs '
                'are real-domain only. Enable MARKET_SNAPSHOT_KIS_USE_REAL or run with KIS_USE_MOCK=false.'
            )
        if not settings.kis_app_key:
            blocking_reasons.append('KIS_APP_KEY must be configured for market snapshot.')
        if not settings.kis_app_secret:
            blocking_reasons.append('KIS_APP_SECRET must be configured for market snapshot.')
        if not settings.koreaexim_api_key:
            blocking_reasons.append('KOREAEXIM_API_KEY must be configured for market snapshot USD/KRW data.')

        return MarketSnapshotRuntimeStatus(
            ready=not blocking_reasons,
            global_kis_environment=global_kis_environment,
            effective_kis_environment=effective_kis_environment,
            effective_kis_base_url=effective_kis_base_url,
            uses_real_domain_override=uses_real_domain_override,
            market_snapshot_kis_use_real=settings.market_snapshot_kis_use_real,
            kis_app_key_configured=bool(settings.kis_app_key),
            kis_app_secret_configured=bool(settings.kis_app_secret),
            koreaexim_api_key_configured=bool(settings.koreaexim_api_key),
            blocking_reasons=tuple(blocking_reasons),
        )

    def _configuration_error(self) -> MarketSnapshotConfigurationError:
        status = self._runtime_status()
        if not status.blocking_reasons:
            return MarketSnapshotConfigurationError('Market snapshot runtime is not ready.')
        if len(status.blocking_reasons) == 1:
            return MarketSnapshotConfigurationError(status.blocking_reasons[0])
        return MarketSnapshotConfigurationError(
            'Market snapshot runtime is not ready: ' + '; '.join(status.blocking_reasons)
        )

    def _effective_kis_client(self) -> KISClient:
        if not self._uses_default_kis_client:
            return self.kis_api_client
        if settings.market_snapshot_kis_use_real and settings.kis_use_mock:
            if self.real_domain_kis_api_client is None:
                self.real_domain_kis_api_client = KISClient(use_mock=False)
            return self.real_domain_kis_api_client
        return self.kis_api_client

    async def get_market_snapshot(self) -> dict[str, Any]:
        status = self._runtime_status()
        if not status.ready:
            raise self._configuration_error()

        kis_api_client = self._effective_kis_client()

        # Prime the shared token cache once before fanning out multiple KIS reads.
        await kis_api_client.issue_access_token()

        market_rows, usd_krw_payload = await asyncio.gather(
            asyncio.gather(*(self._fetch_market_row(kis_api_client, target) for target in MARKET_SNAPSHOT_TARGETS)),
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

    async def _fetch_market_row(self, kis_api_client: KISClient, target: MarketSnapshotTarget) -> dict[str, Any]:
        index_payload, flow_payload = await asyncio.gather(
            kis_api_client.fetch_market_index_price(index_code=target.index_code),
            kis_api_client.fetch_investor_time_by_market(
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
