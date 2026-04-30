from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.brokers.kis.client import KISClient
from app.brokers.kis.provider import kis_client
from app.core.settings import settings
from app.fx.service import KoreaEximRateNotFoundError, KoreaEximService, fx_service
from app.integrations.fx.client import KoreaEximAPIError


class MarketSnapshotConfigurationError(RuntimeError):
    pass


class MarketSnapshotUpstreamError(RuntimeError):
    pass


logger = logging.getLogger(__name__)
SEOUL_TZ = ZoneInfo('Asia/Seoul')


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
        self._fx_state_lock = threading.Lock()
        self._cached_usd_krw_snapshot: dict[str, Any] | None = None
        self._fx_quota_exhausted_search_date: str | None = None
        self._fx_quota_exhausted_detail: str | None = None

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

    def _now(self) -> datetime:
        return datetime.now(SEOUL_TZ)

    def _requested_fx_search_date(self) -> str:
        return self._now().strftime('%Y%m%d')

    def _load_cached_usd_krw_snapshot(self) -> dict[str, Any] | None:
        with self._fx_state_lock:
            if self._cached_usd_krw_snapshot is None:
                return None
            return dict(self._cached_usd_krw_snapshot)

    def _store_cached_usd_krw_snapshot(self, snapshot: Mapping[str, Any]) -> None:
        with self._fx_state_lock:
            self._cached_usd_krw_snapshot = dict(snapshot)

    def _quota_exhaustion_detail(self, *, search_date: str) -> str | None:
        with self._fx_state_lock:
            if self._fx_quota_exhausted_search_date == search_date:
                return self._fx_quota_exhausted_detail or f'Korea Exim quota already exhausted for {search_date}'
        return None

    def _record_quota_exhaustion(self, *, search_date: str, detail: str) -> None:
        with self._fx_state_lock:
            self._fx_quota_exhausted_search_date = search_date
            self._fx_quota_exhausted_detail = detail

    def _clear_quota_exhaustion(self) -> None:
        with self._fx_state_lock:
            self._fx_quota_exhausted_search_date = None
            self._fx_quota_exhausted_detail = None

    async def get_market_snapshot(self) -> dict[str, Any]:
        status = self._runtime_status()
        if not status.ready:
            raise self._configuration_error()

        kis_api_client = self._effective_kis_client()

        # Prime the shared token cache once before fanning out multiple KIS reads.
        await kis_api_client.issue_access_token()

        market_rows_task = asyncio.gather(
            *(self._fetch_market_row(kis_api_client, target) for target in MARKET_SNAPSHOT_TARGETS)
        )
        usd_krw_task = asyncio.create_task(self._fetch_usd_krw_snapshot())

        try:
            market_rows = await market_rows_task
            usd_krw_snapshot = await usd_krw_task
        except Exception:
            usd_krw_task.cancel()
            raise

        response = {
            'as_of': self._now(),
            'markets': {
                target.key: row
                for target, row in zip(MARKET_SNAPSHOT_TARGETS, market_rows, strict=True)
            },
            'usd_krw': usd_krw_snapshot,
        }
        response.update(self._build_snapshot_metadata(usd_krw_snapshot=usd_krw_snapshot))
        return response

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

    async def _fetch_usd_krw_snapshot(self) -> dict[str, Any]:
        requested_date = self._requested_fx_search_date()
        quota_detail = self._quota_exhaustion_detail(search_date=requested_date)
        if quota_detail is not None:
            logger.warning(
                'Skipping Korea Exim USD/KRW fetch for %s because quota exhaustion is already recorded: %s',
                requested_date,
                quota_detail,
            )
            return self._build_cached_or_missing_usd_krw_snapshot(
                stale_reason='koreaexim_quota_exhausted',
                detail=quota_detail,
            )

        try:
            payload = await self.koreaexim_service.get_exchange_rates(
                search_date=requested_date,
                currency='USD',
            )
            snapshot = self._build_usd_krw_snapshot(payload, source='live', captured_at=self._now())
        except KoreaEximAPIError as exc:
            detail = str(exc)
            stale_reason = self._classify_fx_stale_reason(detail)
            if stale_reason == 'koreaexim_quota_exhausted':
                self._record_quota_exhaustion(search_date=requested_date, detail=detail)
            logger.warning('Market snapshot FX degraded: %s', detail)
            return self._build_cached_or_missing_usd_krw_snapshot(
                stale_reason=stale_reason,
                detail=detail,
            )
        except KoreaEximRateNotFoundError as exc:
            detail = str(exc)
            logger.warning('Market snapshot FX missing: %s', detail)
            return self._build_cached_or_missing_usd_krw_snapshot(
                stale_reason='koreaexim_rate_not_found',
                detail=detail,
            )
        except MarketSnapshotUpstreamError as exc:
            detail = str(exc)
            logger.warning('Market snapshot FX payload invalid: %s', detail)
            return self._build_cached_or_missing_usd_krw_snapshot(
                stale_reason='koreaexim_payload_invalid',
                detail=detail,
            )

        self._clear_quota_exhaustion()
        self._store_cached_usd_krw_snapshot(snapshot)
        return snapshot

    def _build_usd_krw_snapshot(
        self,
        payload: Mapping[str, Any],
        *,
        source: str,
        captured_at: datetime,
    ) -> dict[str, Any]:
        items = payload.get('items')
        if not isinstance(items, list) or not items:
            raise MarketSnapshotUpstreamError('Korea Exim USD payload did not include any items')
        first_row = items[0]
        if not isinstance(first_row, Mapping):
            raise MarketSnapshotUpstreamError('Korea Exim USD payload returned a malformed rate row')

        fallback_applied = bool(payload.get('fallback_applied'))
        stale_reason = 'koreaexim_date_fallback_applied' if fallback_applied else None
        detail = 'Korea Exim date fallback applied' if fallback_applied else None

        return {
            'pair': 'USD/KRW',
            'rate': first_row.get('exchange_rate'),
            'effective_date': _normalize_compact_date(payload.get('effective_date')),
            'fallback_applied': fallback_applied,
            'status': 'stale' if fallback_applied else 'ready',
            'source': source,
            'stale_reason': stale_reason,
            'detail': detail,
            'captured_at': captured_at,
        }

    def _build_cached_or_missing_usd_krw_snapshot(
        self,
        *,
        stale_reason: str,
        detail: str,
    ) -> dict[str, Any]:
        cached_snapshot = self._load_cached_usd_krw_snapshot()
        if cached_snapshot is not None and cached_snapshot.get('rate') is not None:
            cached_snapshot['status'] = 'stale'
            cached_snapshot['source'] = 'cache'
            cached_snapshot['stale_reason'] = stale_reason
            cached_snapshot['detail'] = detail
            return cached_snapshot

        return {
            'pair': 'USD/KRW',
            'rate': None,
            'effective_date': None,
            'fallback_applied': False,
            'status': 'missing',
            'source': 'unavailable',
            'stale_reason': stale_reason,
            'detail': detail,
            'captured_at': None,
        }

    def _build_snapshot_metadata(self, *, usd_krw_snapshot: Mapping[str, Any]) -> dict[str, Any]:
        missing_fields: list[str] = []
        if usd_krw_snapshot.get('rate') is None:
            missing_fields.append('usd_krw.rate')
        if usd_krw_snapshot.get('effective_date') is None:
            missing_fields.append('usd_krw.effective_date')

        if missing_fields:
            status = 'partial'
        elif usd_krw_snapshot.get('status') == 'stale':
            status = 'stale'
        else:
            status = 'ready'

        return {
            'status': status,
            'missing_fields': missing_fields,
            'stale_reason': usd_krw_snapshot.get('stale_reason'),
        }

    def _classify_fx_stale_reason(self, detail: str) -> str:
        if 'result=4' in detail:
            return 'koreaexim_quota_exhausted'
        return 'koreaexim_upstream_error'


market_snapshot_service = MarketSnapshotService()
