from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.brokers.kis.client import KISClient, KISClientError
from app.brokers.kis.provider import kis_client
from app.core.settings import settings
from app.fx.snapshot_store import (
    KOREAEXIM_PROVIDER,
    USD_KRW_PAIR,
    FXRateSnapshotStore,
    fx_snapshot_store,
)
from app.fx.service import KoreaEximRateNotFoundError, KoreaEximService, fx_service
from app.integrations.fx.client import KoreaEximAPIError
from app.market.snapshot_store import (
    MarketSnapshotCacheRecord,
    MarketSnapshotCacheStore,
    market_snapshot_cache_store,
)


class MarketSnapshotConfigurationError(RuntimeError):
    pass


class MarketSnapshotUpstreamError(RuntimeError):
    pass


class MarketSnapshotUnavailableError(RuntimeError):
    pass


logger = logging.getLogger(__name__)
SEOUL_TZ = ZoneInfo('Asia/Seoul')
# KIS exposes market-level value fields as `*_tr_pbmn`; for the dashboard contract we keep the
# index trading value in that original million-KRW scale, but normalize investor-flow values to KRW.
KIS_TRADING_VALUE_UNIT = 'million_krw'
KRW_UNIT = 'krw'
KIS_TR_PBMN_TO_KRW_MULTIPLIER = 1_000_000
LEGACY_INVESTOR_FLOW_UNITS = frozenset({KIS_TRADING_VALUE_UNIT, 'kis_net_value_raw'})


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


def _parse_tr_pbmn_as_krw(value: Any, *, field_name: str) -> int | None:
    parsed_value = _parse_int(value, field_name=field_name)
    if parsed_value is None:
        return None
    return parsed_value * KIS_TR_PBMN_TO_KRW_MULTIPLIER


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


def _normalize_iso_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith('Z'):
        normalized = normalized[:-1] + '+00:00'
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _normalize_market_snapshot_market_row(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = deepcopy(dict(payload))
    has_trading_value_unit = str(payload.get('trading_value_unit') or '').strip() != ''
    flow_unit = str(payload.get('investor_flow_value_unit') or '').strip().lower()

    if not has_trading_value_unit:
        row['trading_value_unit'] = KIS_TRADING_VALUE_UNIT

    # Cached rows written before the investor-flow migration stored `*_tr_pbmn` values directly
    # and omitted both unit markers. Normalize those rows on read so stale cache consumers keep
    # seeing KRW-scale values until the next successful refresh rewrites the payload.
    if flow_unit in LEGACY_INVESTOR_FLOW_UNITS or (not flow_unit and not has_trading_value_unit):
        row['foreign_net_value'] = _parse_tr_pbmn_as_krw(
            payload.get('foreign_net_value'),
            field_name='foreign_net_value',
        )
        row['institution_net_value'] = _parse_tr_pbmn_as_krw(
            payload.get('institution_net_value'),
            field_name='institution_net_value',
        )
        row['investor_flow_value_unit'] = KRW_UNIT
        return row

    if not flow_unit or flow_unit == KRW_UNIT:
        row['investor_flow_value_unit'] = KRW_UNIT
        return row

    row['foreign_net_value'] = None
    row['institution_net_value'] = None
    row['investor_flow_value_unit'] = KRW_UNIT
    return row


def normalize_market_snapshot_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized_payload = deepcopy(dict(payload))
    markets = normalized_payload.get('markets')
    if not isinstance(markets, Mapping):
        return normalized_payload

    normalized_payload['markets'] = {
        key: _normalize_market_snapshot_market_row(row) if isinstance(row, Mapping) else deepcopy(row)
        for key, row in markets.items()
    }
    return normalized_payload


class MarketSnapshotService:
    def __init__(
        self,
        *,
        kis_api_client: KISClient | None = None,
        koreaexim_service: KoreaEximService | None = None,
        real_domain_kis_api_client: KISClient | None = None,
        cache_store: MarketSnapshotCacheStore | None = None,
        fx_snapshot_store_instance: FXRateSnapshotStore | None = None,
        refresh_interval_seconds: int | None = None,
    ) -> None:
        self._uses_default_kis_client = kis_api_client is None
        self.kis_api_client = kis_api_client or kis_client
        self.koreaexim_service = koreaexim_service or fx_service
        self.real_domain_kis_api_client = real_domain_kis_api_client
        self.cache_store = cache_store or market_snapshot_cache_store
        self.fx_snapshot_store = fx_snapshot_store_instance or fx_snapshot_store
        configured_interval = (
            settings.market_snapshot_refresh_interval_seconds
            if refresh_interval_seconds is None
            else refresh_interval_seconds
        )
        self.refresh_interval_seconds = max(int(configured_interval), 0)
        self._refresh_lock = asyncio.Lock()

    def runtime_status(self) -> dict[str, Any]:
        payload = self._runtime_status().to_payload()
        try:
            payload['cache'] = self._build_cache_status_payload(self.cache_store.load())
        except Exception as exc:
            payload['cache'] = {
                'available': False,
                'has_snapshot': False,
                'snapshot_status': None,
                'as_of': None,
                'last_refresh_attempted_at': None,
                'last_refresh_succeeded_at': None,
                'next_refresh_due_at': None,
                'last_refresh_status': 'unknown',
                'last_refresh_error': str(exc),
                'refresh_interval_seconds': self.refresh_interval_seconds,
            }
        return payload

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

    def _requested_fx_search_date(self, *, now: datetime | None = None) -> str:
        return (now or self._now()).strftime('%Y%m%d')

    async def get_market_snapshot(self) -> dict[str, Any]:
        try:
            cache_record = self.cache_store.load()
        except Exception as exc:
            raise MarketSnapshotUnavailableError(f'Market snapshot cache unavailable: {exc}') from exc
        payload = self._decorate_cached_payload(cache_record)
        if payload is not None:
            return payload

        runtime_status = self._runtime_status()
        if not runtime_status.ready:
            raise self._configuration_error()

        if cache_record is not None and cache_record.last_refresh_error:
            raise MarketSnapshotUnavailableError(
                'Market snapshot cache is empty and the last refresh failed: '
                + cache_record.last_refresh_error
            )
        raise MarketSnapshotUnavailableError(
            'Market snapshot cache is empty. Wait for the background refresh to complete.'
        )

    async def refresh_market_snapshot_if_due(
        self,
        *,
        trigger: str,
        force: bool = False,
    ) -> dict[str, Any] | None:
        async with self._refresh_lock:
            cache_record = self.cache_store.load()
            if not force and not self._refresh_is_due(cache_record):
                return self._decorate_cached_payload(cache_record)
            return await self._refresh_market_snapshot(cache_record=cache_record, trigger=trigger)

    async def _refresh_market_snapshot(
        self,
        *,
        cache_record: MarketSnapshotCacheRecord | None,
        trigger: str,
    ) -> dict[str, Any] | None:
        attempted_at = self._now()
        cached_payload = cache_record.payload if cache_record is not None else None
        last_successful_refresh_at = cache_record.last_refresh_succeeded_at if cache_record is not None else None
        runtime_status = self._runtime_status()

        if not runtime_status.ready:
            error = str(self._configuration_error())
            stale_payload = self._build_stale_payload_from_cache(
                cached_payload,
                stale_reason='market_snapshot_configuration_blocked',
            )
            stored_record = self.cache_store.save(
                payload=stale_payload,
                last_refresh_attempted_at=attempted_at,
                last_refresh_succeeded_at=last_successful_refresh_at,
                last_refresh_status='error',
                last_refresh_error=error,
                refresh_trigger=trigger,
            )
            if stale_payload is not None:
                logger.warning('Market snapshot refresh blocked by configuration; serving cached snapshot: %s', error)
                return self._decorate_cached_payload(stored_record)
            raise self._configuration_error()

        try:
            snapshot = await self._build_live_snapshot(cache_record=cache_record, refreshed_at=attempted_at)
        except Exception as exc:
            stale_payload = self._build_stale_payload_from_cache(
                cached_payload,
                stale_reason=self._classify_refresh_failure_reason(exc),
            )
            stored_record = self.cache_store.save(
                payload=stale_payload,
                last_refresh_attempted_at=attempted_at,
                last_refresh_succeeded_at=last_successful_refresh_at,
                last_refresh_status='error',
                last_refresh_error=str(exc),
                refresh_trigger=trigger,
            )
            if stale_payload is not None:
                logger.warning('Market snapshot refresh failed; serving stale cached snapshot: %s', exc)
                return self._decorate_cached_payload(stored_record)
            raise

        refresh_status = 'success' if snapshot.get('status') == 'ready' else 'degraded'
        refresh_error = None
        if refresh_status == 'degraded':
            usd_krw_snapshot = snapshot.get('usd_krw')
            if isinstance(usd_krw_snapshot, Mapping):
                refresh_error = str(usd_krw_snapshot.get('detail') or snapshot.get('stale_reason') or 'snapshot degraded')
            else:
                refresh_error = str(snapshot.get('stale_reason') or 'snapshot degraded')
        stored_record = self.cache_store.save(
            payload=snapshot,
            last_refresh_attempted_at=attempted_at,
            last_refresh_succeeded_at=attempted_at,
            last_refresh_status=refresh_status,
            last_refresh_error=refresh_error,
            refresh_trigger=trigger,
        )
        return self._decorate_cached_payload(stored_record)

    async def _build_live_snapshot(
        self,
        *,
        cache_record: MarketSnapshotCacheRecord | None,
        refreshed_at: datetime,
    ) -> dict[str, Any]:
        kis_api_client = self._effective_kis_client()

        # Prime the shared token cache once before fanning out multiple KIS reads.
        await kis_api_client.issue_access_token()

        market_rows_task = asyncio.gather(
            *(self._fetch_market_row(kis_api_client, target) for target in MARKET_SNAPSHOT_TARGETS)
        )
        usd_krw_task = asyncio.create_task(
            self._fetch_usd_krw_snapshot(cache_record=cache_record, refreshed_at=refreshed_at)
        )

        try:
            market_rows = await market_rows_task
            usd_krw_snapshot = await usd_krw_task
        except Exception:
            usd_krw_task.cancel()
            raise

        response = {
            'as_of': refreshed_at.isoformat(),
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
            'trading_value_unit': KIS_TRADING_VALUE_UNIT,
            'advancers': _parse_int(index_output.get('ascn_issu_cnt'), field_name='ascn_issu_cnt'),
            'unchanged': _parse_int(index_output.get('stnr_issu_cnt'), field_name='stnr_issu_cnt'),
            'decliners': _parse_int(index_output.get('down_issu_cnt'), field_name='down_issu_cnt'),
            'foreign_net_quantity': _parse_int(flow_output.get('frgn_ntby_qty'), field_name='frgn_ntby_qty'),
            'foreign_net_value': _parse_tr_pbmn_as_krw(
                flow_output.get('frgn_ntby_tr_pbmn'),
                field_name='frgn_ntby_tr_pbmn',
            ),
            'institution_net_quantity': _parse_int(flow_output.get('orgn_ntby_qty'), field_name='orgn_ntby_qty'),
            'institution_net_value': _parse_tr_pbmn_as_krw(
                flow_output.get('orgn_ntby_tr_pbmn'),
                field_name='orgn_ntby_tr_pbmn',
            ),
            'investor_flow_value_unit': KRW_UNIT,
        }

    def _extract_flow_output(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        rows = payload.get('output')
        if not isinstance(rows, list) or not rows:
            raise MarketSnapshotUpstreamError('KIS investor-flow payload did not include a usable output row')
        first_row = rows[0]
        return _require_mapping(first_row, field_name='output[0]')

    async def _fetch_usd_krw_snapshot(
        self,
        *,
        cache_record: MarketSnapshotCacheRecord | None,
        refreshed_at: datetime,
    ) -> dict[str, Any]:
        requested_date = self._requested_fx_search_date(now=refreshed_at)
        quota_detail = self._quota_exhaustion_detail(cache_record=cache_record, search_date=requested_date)
        cached_snapshot = self._latest_usable_usd_krw_snapshot(cache_record)
        if quota_detail is not None:
            logger.warning(
                'Skipping Korea Exim USD/KRW fetch for %s because quota exhaustion is already recorded: %s',
                requested_date,
                quota_detail,
            )
            return self._build_cached_or_missing_usd_krw_snapshot(
                cached_snapshot=cached_snapshot,
                stale_reason='koreaexim_quota_exhausted',
                detail=quota_detail,
            )

        try:
            payload = await self.koreaexim_service.get_exchange_rates(
                search_date=requested_date,
                currency='USD',
            )
            snapshot = self._build_usd_krw_snapshot(
                payload,
                source='live',
                captured_at=refreshed_at.isoformat(),
            )
            self._persist_latest_usd_krw_snapshot(payload=payload, snapshot=snapshot)
            return snapshot
        except KoreaEximAPIError as exc:
            detail = str(exc)
            stale_reason = self._classify_fx_stale_reason(detail)
            logger.warning('Market snapshot FX degraded: %s', detail)
            return self._build_cached_or_missing_usd_krw_snapshot(
                cached_snapshot=cached_snapshot,
                stale_reason=stale_reason,
                detail=detail,
            )
        except KoreaEximRateNotFoundError as exc:
            detail = str(exc)
            logger.warning('Market snapshot FX missing: %s', detail)
            return self._build_cached_or_missing_usd_krw_snapshot(
                cached_snapshot=cached_snapshot,
                stale_reason='koreaexim_rate_not_found',
                detail=detail,
            )
        except MarketSnapshotUpstreamError as exc:
            detail = str(exc)
            logger.warning('Market snapshot FX payload invalid: %s', detail)
            return self._build_cached_or_missing_usd_krw_snapshot(
                cached_snapshot=cached_snapshot,
                stale_reason='koreaexim_payload_invalid',
                detail=detail,
            )

    def _quota_exhaustion_detail(
        self,
        *,
        cache_record: MarketSnapshotCacheRecord | None,
        search_date: str,
    ) -> str | None:
        if cache_record is None:
            return None
        detail = (cache_record.last_refresh_error or '').strip()
        attempted_at = cache_record.last_refresh_attempted_at
        if 'result=4' not in detail or attempted_at is None:
            return None
        attempted_search_date = attempted_at.astimezone(SEOUL_TZ).strftime('%Y%m%d')
        if attempted_search_date != search_date:
            return None
        return detail

    def _latest_usable_usd_krw_snapshot(
        self,
        cache_record: MarketSnapshotCacheRecord | None,
    ) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        persisted_snapshot = self._persisted_usd_krw_snapshot()
        if persisted_snapshot is not None and persisted_snapshot.get('rate') is not None:
            candidates.append(persisted_snapshot)
        cached_snapshot = self._cached_usd_krw_snapshot(cache_record)
        if cached_snapshot is not None and cached_snapshot.get('rate') is not None:
            candidates.append(cached_snapshot)
        if not candidates:
            return None
        return max(candidates, key=self._snapshot_recency)

    def _persisted_usd_krw_snapshot(self) -> dict[str, Any] | None:
        try:
            record = self.fx_snapshot_store.load_latest_usable(
                provider=KOREAEXIM_PROVIDER,
                currency_pair=USD_KRW_PAIR,
            )
        except Exception as exc:
            logger.warning('Failed to load persisted USD/KRW snapshot fallback: %s', exc)
            return None
        if record is None:
            return None
        return record.to_market_snapshot_payload()

    def _cached_usd_krw_snapshot(
        self,
        cache_record: MarketSnapshotCacheRecord | None,
    ) -> dict[str, Any] | None:
        if cache_record is None or not isinstance(cache_record.payload, dict):
            return None
        cached_snapshot = cache_record.payload.get('usd_krw')
        if not isinstance(cached_snapshot, Mapping):
            return None
        return deepcopy(dict(cached_snapshot))

    def _persist_latest_usd_krw_snapshot(
        self,
        *,
        payload: Mapping[str, Any],
        snapshot: Mapping[str, Any],
    ) -> None:
        rate = snapshot.get('rate')
        effective_date = payload.get('effective_date')
        requested_date = payload.get('requested_date')
        captured_at = snapshot.get('captured_at')
        if rate is None or effective_date in (None, '') or requested_date in (None, '') or captured_at in (None, ''):
            return
        try:
            self.fx_snapshot_store.upsert_snapshot(
                provider=str(payload.get('provider') or KOREAEXIM_PROVIDER),
                currency_pair=USD_KRW_PAIR,
                base_currency='USD',
                quote_currency='KRW',
                exchange_rate=float(rate),
                requested_date=str(requested_date),
                effective_date=str(effective_date),
                fallback_applied=bool(snapshot.get('fallback_applied')),
                snapshot_status=str(snapshot.get('status') or 'ready'),
                stale_reason=str(snapshot.get('stale_reason')) if snapshot.get('stale_reason') else None,
                detail=str(snapshot.get('detail')) if snapshot.get('detail') else None,
                payload=deepcopy(dict(payload)),
                captured_at=str(captured_at),
            )
        except Exception as exc:
            logger.warning('Failed to persist durable USD/KRW snapshot: %s', exc)

    def _build_usd_krw_snapshot(
        self,
        payload: Mapping[str, Any],
        *,
        source: str,
        captured_at: str,
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
        cached_snapshot: Mapping[str, Any] | None,
        stale_reason: str,
        detail: str,
    ) -> dict[str, Any]:
        if cached_snapshot is not None and cached_snapshot.get('rate') is not None:
            payload = deepcopy(dict(cached_snapshot))
            payload['status'] = 'stale'
            payload['source'] = 'cache'
            payload['stale_reason'] = stale_reason
            payload['detail'] = detail
            return payload

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

    def _classify_refresh_failure_reason(self, exc: Exception) -> str:
        if isinstance(exc, MarketSnapshotConfigurationError):
            return 'market_snapshot_configuration_blocked'
        if isinstance(exc, (KISClientError, MarketSnapshotUpstreamError)):
            return 'market_snapshot_kis_upstream_error'
        if isinstance(exc, (KoreaEximAPIError, KoreaEximRateNotFoundError)):
            return 'market_snapshot_fx_upstream_error'
        return 'market_snapshot_refresh_failed'

    def _build_stale_payload_from_cache(
        self,
        cached_payload: dict[str, Any] | None,
        *,
        stale_reason: str,
    ) -> dict[str, Any] | None:
        if not isinstance(cached_payload, dict):
            return None
        payload = normalize_market_snapshot_payload(cached_payload)
        payload['status'] = 'stale'
        payload['stale_reason'] = stale_reason
        missing_fields = payload.get('missing_fields')
        if isinstance(missing_fields, list):
            payload['missing_fields'] = list(missing_fields)
        else:
            payload['missing_fields'] = []
        return payload

    def _decorate_cached_payload(
        self,
        cache_record: MarketSnapshotCacheRecord | None,
    ) -> dict[str, Any] | None:
        if cache_record is None or not isinstance(cache_record.payload, dict):
            return None
        payload = normalize_market_snapshot_payload(cache_record.payload)
        payload['freshness'] = self._build_freshness_payload(cache_record)
        return payload

    def _build_cache_status_payload(
        self,
        cache_record: MarketSnapshotCacheRecord | None,
    ) -> dict[str, Any]:
        return {
            'available': True,
            'has_snapshot': bool(cache_record and cache_record.payload),
            'snapshot_status': cache_record.snapshot_status if cache_record is not None else None,
            'as_of': self._format_datetime(cache_record.as_of) if cache_record is not None else None,
            'last_refresh_attempted_at': (
                self._format_datetime(cache_record.last_refresh_attempted_at) if cache_record is not None else None
            ),
            'last_refresh_succeeded_at': (
                self._format_datetime(cache_record.last_refresh_succeeded_at) if cache_record is not None else None
            ),
            'next_refresh_due_at': self._format_datetime(self._next_refresh_due_at(cache_record)),
            'last_refresh_status': cache_record.last_refresh_status if cache_record is not None else 'never',
            'last_refresh_error': cache_record.last_refresh_error if cache_record is not None else None,
            'refresh_interval_seconds': self.refresh_interval_seconds,
        }

    def _build_freshness_payload(
        self,
        cache_record: MarketSnapshotCacheRecord,
    ) -> dict[str, Any]:
        return {
            'served_from_cache': True,
            'refresh_interval_seconds': self.refresh_interval_seconds,
            'last_refresh_attempted_at': self._format_datetime(cache_record.last_refresh_attempted_at),
            'last_refresh_succeeded_at': self._format_datetime(cache_record.last_refresh_succeeded_at),
            'next_refresh_due_at': self._format_datetime(self._next_refresh_due_at(cache_record)),
            'last_refresh_status': cache_record.last_refresh_status,
            'last_refresh_error': cache_record.last_refresh_error,
        }

    def _format_datetime(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

    def _snapshot_recency(self, snapshot: Mapping[str, Any]) -> datetime:
        captured_at = _normalize_iso_datetime(snapshot.get('captured_at'))
        if captured_at is None:
            return datetime.min.replace(tzinfo=SEOUL_TZ)
        if captured_at.tzinfo is None:
            return captured_at.replace(tzinfo=SEOUL_TZ)
        return captured_at

    def _refresh_is_due(self, cache_record: MarketSnapshotCacheRecord | None) -> bool:
        if cache_record is None:
            return True
        if self.refresh_interval_seconds == 0:
            return True
        next_due_at = self._next_refresh_due_at(cache_record)
        if next_due_at is None:
            return True
        return self._now() >= next_due_at

    def _next_refresh_due_at(
        self,
        cache_record: MarketSnapshotCacheRecord | None,
    ) -> datetime | None:
        if cache_record is None:
            return None
        reference = cache_record.last_refresh_attempted_at or cache_record.last_refresh_succeeded_at or cache_record.as_of
        if reference is None and isinstance(cache_record.payload, dict):
            reference = _normalize_iso_datetime(cache_record.payload.get('as_of'))
        if reference is None:
            return None
        return reference + timedelta(seconds=self.refresh_interval_seconds)


market_snapshot_service = MarketSnapshotService()
