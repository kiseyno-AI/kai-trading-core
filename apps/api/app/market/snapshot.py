from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from app.brokers.kis.client import KISClient, KISClientError
from app.brokers.kis.provider import kis_client
from app.core.settings import settings
from app.eod.master_state import master_refresh_state
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
KIS_INDEX_OBSERVATION_SOURCE = 'kis_index_intraday_bar'
KIS_BREADTH_VERIFICATION_BASIS = (
    'kis_directional_buckets_with_same_date_completed_master_upper_bound'
)
KIS_BREADTH_MASTER_COUNT_RELATION = 'upper_bound'
KIS_BREADTH_LIMIT_BUCKET_SEMANTICS = 'included_in_direction_counts'
KIS_BREADTH_MIN_MASTER_COVERAGE_RATIO = 0.95


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
        parsed = float(text)
    except ValueError as exc:
        raise MarketSnapshotUpstreamError(f'KIS returned a non-numeric {field_name}: {value!r}') from exc
    if not math.isfinite(parsed):
        raise MarketSnapshotUpstreamError(f'KIS returned a non-finite {field_name}: {value!r}')
    return parsed


def _parse_int(value: Any, *, field_name: str) -> int | None:
    if value in (None, ''):
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r'[+-]?(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\.0+)?', text) is None:
        raise MarketSnapshotUpstreamError(f'KIS returned a non-integer {field_name}: {value!r}')
    try:
        parsed = Decimal(text.replace(',', ''))
    except InvalidOperation as exc:
        raise MarketSnapshotUpstreamError(f'KIS returned a non-integer {field_name}: {value!r}') from exc
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        raise MarketSnapshotUpstreamError(f'KIS returned a non-integer {field_name}: {value!r}')
    return int(parsed)


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
        self._refresh_task: asyncio.Task[dict[str, Any] | None] | None = None

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
        force_index: bool = False,
    ) -> dict[str, Any] | None:
        async with self._refresh_lock:
            if self._refresh_task is not None and not self._refresh_task.done():
                refresh_task = self._refresh_task
            else:
                cache_record = self.cache_store.load()
                optional_legs_due = self._optional_legs_are_due(cache_record)
                if (
                    not force
                    and not force_index
                    and not self._refresh_is_due(cache_record)
                    and not optional_legs_due
                ):
                    return self._decorate_cached_payload(cache_record)
                include_optional_legs = force or optional_legs_due
                attempted_at = self._now()
                claim_refresh_bucket = getattr(
                    self.cache_store,
                    'try_claim_refresh_bucket',
                    None,
                )
                if force_index and not force and callable(claim_refresh_bucket):
                    interval_seconds = max(
                        int(settings.market_snapshot_open_drive_refresh_interval_seconds),
                        1,
                    )
                    local_midnight = attempted_at.replace(
                        hour=0,
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                    elapsed_seconds = int(
                        (attempted_at - local_midnight).total_seconds()
                    )
                    bucket_started_at = local_midnight + timedelta(
                        seconds=(elapsed_seconds // interval_seconds)
                        * interval_seconds
                    )
                    claimed = claim_refresh_bucket(
                        bucket_started_at=bucket_started_at,
                        attempted_at=attempted_at,
                        refresh_trigger=trigger,
                    )
                    if not claimed:
                        return self._decorate_cached_payload(self.cache_store.load())
                    cache_record = self.cache_store.load()
                refresh_task = asyncio.create_task(
                    self._refresh_market_snapshot(
                        cache_record=cache_record,
                        trigger=trigger,
                        include_optional_legs=include_optional_legs,
                        attempted_at=attempted_at,
                    ),
                    name='market-snapshot-refresh',
                )
                self._refresh_task = refresh_task
        return await asyncio.shield(refresh_task)

    async def _refresh_market_snapshot(
        self,
        *,
        cache_record: MarketSnapshotCacheRecord | None,
        trigger: str,
        include_optional_legs: bool,
        attempted_at: datetime,
    ) -> dict[str, Any] | None:
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
            snapshot = await self._build_live_snapshot(
                cache_record=cache_record,
                refreshed_at=attempted_at,
                include_optional_legs=include_optional_legs,
            )
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
            last_refresh_succeeded_at=(
                _normalize_iso_datetime(snapshot.get('collected_at'))
                or self._now()
            ),
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
        include_optional_legs: bool,
    ) -> dict[str, Any]:
        kis_api_client = self._effective_kis_client()

        # Prime the shared token cache once before fanning out multiple KIS reads.
        await kis_api_client.issue_access_token()

        cached_markets = (
            cache_record.payload.get('markets')
            if cache_record is not None and isinstance(cache_record.payload, Mapping)
            else {}
        )
        if not isinstance(cached_markets, Mapping):
            cached_markets = {}
        market_rows_task = asyncio.gather(
            *(
                self._fetch_market_row(
                    kis_api_client,
                    target,
                    include_optional_legs=include_optional_legs,
                    cached_market_row=(
                        cached_markets.get(target.key)
                        if isinstance(cached_markets.get(target.key), Mapping)
                        else None
                    ),
                )
                for target in MARKET_SNAPSHOT_TARGETS
            )
        )
        usd_krw_task = (
            asyncio.create_task(
                self._fetch_usd_krw_snapshot(cache_record=cache_record, refreshed_at=refreshed_at)
            )
            if include_optional_legs
            else None
        )

        try:
            market_rows = await market_rows_task
            if usd_krw_task is not None:
                usd_krw_snapshot = await usd_krw_task
                fx_leg_status = str(usd_krw_snapshot.get('status') or 'unavailable')
            else:
                cached_fx = self._cached_usd_krw_snapshot(cache_record)
                usd_krw_snapshot = cached_fx or self._build_cached_or_missing_usd_krw_snapshot(
                    cached_snapshot=None,
                    stale_reason='not_refreshed_index_only',
                    detail='The optional FX leg was not due during this index-only refresh.',
                )
                cached_fx_status = str((cached_fx or {}).get('status') or '').strip().lower()
                fx_leg_status = (
                    'cached'
                    if cached_fx is not None
                    and cached_fx.get('rate') is not None
                    and cached_fx_status in {'ready', 'stale'}
                    else cached_fx_status
                    if cached_fx_status in {'missing', 'unavailable'}
                    else 'unavailable'
                )
        except Exception:
            if usd_krw_task is not None:
                usd_krw_task.cancel()
            raise

        provider_observed_at = max(
            (
                observed_at
                for row in market_rows
                if (observed_at := _normalize_iso_datetime(row.get('provider_observed_at'))) is not None
            ),
            default=None,
        )
        collected_at = self._now()
        for target, row in zip(MARKET_SNAPSHOT_TARGETS, market_rows, strict=True):
            row['breadth_completeness'] = self._build_breadth_completeness(
                target=target,
                market_row=row,
            )
        response = {
            'as_of': provider_observed_at.isoformat() if provider_observed_at is not None else None,
            'collected_at': collected_at.isoformat(),
            'optional_legs_refreshed_at': (
                collected_at.isoformat()
                if include_optional_legs
                else (
                    cache_record.payload.get('optional_legs_refreshed_at')
                    if cache_record is not None and isinstance(cache_record.payload, Mapping)
                    else None
                )
            ),
            'markets': {
                target.key: row
                for target, row in zip(MARKET_SNAPSHOT_TARGETS, market_rows, strict=True)
            },
            'usd_krw': usd_krw_snapshot,
        }
        response.update(
            self._build_snapshot_metadata(
                market_rows=market_rows,
                usd_krw_snapshot=usd_krw_snapshot,
                fx_leg_status=fx_leg_status,
            )
        )
        return response

    @staticmethod
    def _build_breadth_completeness(
        *,
        target: MarketSnapshotTarget,
        market_row: Mapping[str, Any],
    ) -> dict[str, Any]:
        unverified = {
            'status': 'unverified',
            'reason': 'same_date_completed_kis_master_proof_unavailable',
            'expected_symbol_count': None,
            'observed_symbol_count': None,
            'coverage_ratio': None,
            'limit_bucket_semantics': 'unknown',
            'verification_basis': None,
            'master_count_relation': None,
            'expected_universe': None,
        }
        try:
            state = master_refresh_state.snapshot()
            symbols = list(master_refresh_state.symbols)
        except Exception:
            return unverified

        provider_observed_at = _normalize_iso_datetime(market_row.get('provider_observed_at'))
        completed_at = _normalize_iso_datetime(state.get('last_completed_at'))
        if (
            provider_observed_at is None
            or provider_observed_at.tzinfo is None
            or provider_observed_at.utcoffset() is None
            or completed_at is None
            or completed_at.tzinfo is None
            or completed_at.utcoffset() is None
            or state.get('last_status') != 'completed'
            or state.get('market_date') != provider_observed_at.astimezone(SEOUL_TZ).date().isoformat()
        ):
            return unverified

        symbol_codes = [
            str(getattr(symbol, 'symbol', '') or '').strip().upper()
            for symbol in symbols
            if str(getattr(symbol, 'market', '') or '').strip().upper() == target.label
            and str(getattr(symbol, 'symbol', '') or '').strip()
        ]
        all_symbol_codes = [
            str(getattr(symbol, 'symbol', '') or '').strip().upper()
            for symbol in symbols
            if str(getattr(symbol, 'symbol', '') or '').strip()
        ]
        expected_symbol_count = len(symbol_codes)
        if (
            expected_symbol_count <= 0
            or len(set(symbol_codes)) != expected_symbol_count
            or len(set(all_symbol_codes)) != len(all_symbol_codes)
            or int(state.get('symbol_count') or 0) != len(all_symbol_codes)
            or int(state.get(f'{target.key}_count') or 0) != expected_symbol_count
        ):
            return {
                **unverified,
                'reason': 'kis_master_membership_count_inconsistent',
            }

        raw_buckets = market_row.get('breadth_raw_buckets')
        if not isinstance(raw_buckets, Mapping):
            return {
                **unverified,
                'reason': 'provider_five_bucket_payload_missing',
                'expected_symbol_count': expected_symbol_count,
            }
        bucket_names = (
            'upper_limit_count',
            'advancers',
            'unchanged',
            'decliners',
            'lower_limit_count',
        )
        bucket_values: dict[str, int] = {}
        for name in bucket_names:
            value = raw_buckets.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                return {
                    **unverified,
                    'reason': 'provider_five_bucket_payload_invalid',
                    'expected_symbol_count': expected_symbol_count,
                }
            bucket_values[name] = value

        directional_count = (
            bucket_values['advancers']
            + bucket_values['unchanged']
            + bucket_values['decliners']
        )
        raw_coverage_ratio = directional_count / expected_symbol_count
        coverage_ratio = round(raw_coverage_ratio, 6)
        if directional_count <= 0:
            return {
                **unverified,
                'reason': 'provider_directional_count_invalid',
                'expected_symbol_count': expected_symbol_count,
                'observed_symbol_count': directional_count,
                'coverage_ratio': coverage_ratio,
            }
        if (
            bucket_values['upper_limit_count'] > bucket_values['advancers']
            or bucket_values['lower_limit_count'] > bucket_values['decliners']
        ):
            return {
                **unverified,
                'reason': 'provider_limit_bucket_subset_invalid',
                'expected_symbol_count': expected_symbol_count,
                'observed_symbol_count': directional_count,
                'coverage_ratio': coverage_ratio,
            }
        if directional_count > expected_symbol_count:
            return {
                **unverified,
                'reason': 'provider_directional_count_exceeds_master_upper_bound',
                'expected_symbol_count': expected_symbol_count,
                'observed_symbol_count': directional_count,
                'coverage_ratio': coverage_ratio,
            }
        if raw_coverage_ratio < KIS_BREADTH_MIN_MASTER_COVERAGE_RATIO:
            return {
                **unverified,
                'reason': 'provider_directional_coverage_below_minimum',
                'expected_symbol_count': expected_symbol_count,
                'observed_symbol_count': directional_count,
                'coverage_ratio': coverage_ratio,
            }

        membership_fingerprint = hashlib.sha256(
            '\n'.join(sorted(symbol_codes)).encode('utf-8')
        ).hexdigest()
        return {
            'status': 'verified',
            'reason': None,
            'expected_symbol_count': expected_symbol_count,
            'observed_symbol_count': directional_count,
            'coverage_ratio': coverage_ratio,
            'limit_bucket_semantics': KIS_BREADTH_LIMIT_BUCKET_SEMANTICS,
            'verification_basis': KIS_BREADTH_VERIFICATION_BASIS,
            'master_count_relation': KIS_BREADTH_MASTER_COUNT_RELATION,
            'expected_universe': {
                'provider': 'kis_master',
                'market_date': state.get('market_date'),
                'completed_at': completed_at.isoformat(),
                'symbol_count': expected_symbol_count,
                'relation_to_observed_count': KIS_BREADTH_MASTER_COUNT_RELATION,
                'membership_sha256': membership_fingerprint,
            },
        }

    async def _fetch_market_row(
        self,
        kis_api_client: KISClient,
        target: MarketSnapshotTarget,
        *,
        include_optional_legs: bool,
        cached_market_row: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        index_payload_before = await kis_api_client.fetch_market_index_price(
            index_code=target.index_code
        )
        index_before_received_at = self._now()
        try:
            index_time_result: Mapping[str, Any] | BaseException = (
                await kis_api_client.fetch_market_index_time_series(
                    index_code=target.index_code
                )
            )
        except Exception as exc:  # noqa: BLE001 - preserve optional proof failure
            index_time_result = exc
        index_payload = await kis_api_client.fetch_market_index_price(
            index_code=target.index_code
        )
        index_received_at = self._now()
        index_output_before = _require_mapping(
            index_payload_before.get('output'),
            field_name='output',
        )
        index_output = _require_mapping(index_payload.get('output'), field_name='output')
        provider_observation_error: str | None = None
        provider_observed_at: datetime | None = None
        observation_payload_match: bool | None = None
        if isinstance(index_time_result, BaseException):
            provider_observation_error = str(index_time_result)
        else:
            try:
                provider_observed_at = self._extract_provider_index_observed_at(index_time_result)
                observation_payload_match = self._provider_observation_matches_index_payload(
                    index_output_before=index_output_before,
                    index_output_after=index_output,
                    observation_payload=index_time_result,
                    provider_observed_at=provider_observed_at,
                    index_before_received_at=index_before_received_at,
                    index_after_received_at=index_received_at,
                )
                if not observation_payload_match:
                    provider_observation_error = (
                        'KIS index time-series observation does not fit the current-read turnover bracket'
                    )
            except MarketSnapshotUpstreamError as exc:
                provider_observation_error = str(exc)
        provider_observation_age_seconds = (
            (index_received_at - provider_observed_at).total_seconds()
            if provider_observed_at is not None
            else None
        )
        provider_observation_current_session = bool(
            provider_observed_at is not None
            and provider_observed_at.astimezone(SEOUL_TZ).date()
            == index_received_at.astimezone(SEOUL_TZ).date()
            and time(9, 0)
            <= provider_observed_at.astimezone(SEOUL_TZ).time().replace(tzinfo=None)
            <= time(15, 30)
            and provider_observation_age_seconds is not None
            and 0
            <= provider_observation_age_seconds
            <= settings.market_snapshot_open_drive_max_age_seconds
            and observation_payload_match is True
        )
        if (
            provider_observed_at is not None
            and not provider_observation_current_session
            and provider_observation_error is None
        ):
            provider_observation_error = (
                'KIS index observation is not a fresh current regular-session observation'
            )
        flow_error: str | None = None
        if include_optional_legs:
            try:
                flow_payload = await kis_api_client.fetch_investor_time_by_market(
                    market_code=target.investor_market_code,
                    sector_code=target.investor_sector_code,
                )
                flow_output: Mapping[str, Any] = self._extract_flow_output(flow_payload)
                flow_status = 'ready'
            except Exception as exc:
                logger.warning('Market snapshot investor-flow leg failed for %s: %s', target.label, exc)
                flow_output = {}
                flow_error = str(exc)
                flow_status = 'unavailable'
        else:
            flow_output = {}
            cached_flow_leg = (
                cached_market_row.get('flow_leg')
                if isinstance(cached_market_row, Mapping)
                and isinstance(cached_market_row.get('flow_leg'), Mapping)
                else {}
            )
            cached_flow_status = str(cached_flow_leg.get('status') or '').strip().lower()
            flow_status = (
                'cached'
                if cached_flow_status in {'ready', 'cached'}
                else 'unavailable'
            )
            flow_error = (
                str(cached_flow_leg.get('error'))
                if cached_flow_leg.get('error') not in (None, '')
                else None
            )

        upper_limit_count = _parse_int(index_output.get('uplm_issu_cnt'), field_name='uplm_issu_cnt')
        advancers = _parse_int(index_output.get('ascn_issu_cnt'), field_name='ascn_issu_cnt')
        unchanged = _parse_int(index_output.get('stnr_issu_cnt'), field_name='stnr_issu_cnt')
        decliners = _parse_int(index_output.get('down_issu_cnt'), field_name='down_issu_cnt')
        lower_limit_count = _parse_int(index_output.get('lslm_issu_cnt'), field_name='lslm_issu_cnt')

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
            'prior_trading_value': _parse_int(index_output.get('prdy_tr_pbmn'), field_name='prdy_tr_pbmn'),
            'prior_trading_value_unit': KIS_TRADING_VALUE_UNIT,
            'upper_limit_count': upper_limit_count,
            'advancers': advancers,
            'unchanged': unchanged,
            'decliners': decliners,
            'lower_limit_count': lower_limit_count,
            'breadth_raw_buckets': {
                'upper_limit_count': upper_limit_count,
                'advancers': advancers,
                'unchanged': unchanged,
                'decliners': decliners,
                'lower_limit_count': lower_limit_count,
            },
            'provider_business_date': (
                provider_observed_at.date().isoformat() if provider_observed_at is not None else None
            ),
            'provider_session_time': (
                provider_observed_at.strftime('%H:%M:%S') if provider_observed_at is not None else None
            ),
            'provider_observed_at': (
                provider_observed_at.isoformat() if provider_observed_at is not None else None
            ),
            'index_leg': {
                'status': 'ready' if provider_observation_current_session else 'unverified',
                'observation_source': KIS_INDEX_OBSERVATION_SOURCE,
                'collected_at': index_received_at.isoformat(),
                'current_before_collected_at': (
                    index_before_received_at.isoformat()
                ),
                'current_after_collected_at': index_received_at.isoformat(),
                'provider_observed_at': (
                    provider_observed_at.isoformat() if provider_observed_at is not None else None
                ),
                'observation_age_seconds': provider_observation_age_seconds,
                'observation_payload_match': observation_payload_match,
                'error': provider_observation_error,
            },
            'flow_leg': {
                'status': flow_status,
                'source': 'kis_investor_time_by_market',
                'error': flow_error,
            },
            'foreign_net_quantity': (
                _parse_int(flow_output.get('frgn_ntby_qty'), field_name='frgn_ntby_qty')
                if include_optional_legs
                else (cached_market_row or {}).get('foreign_net_quantity')
            ),
            'foreign_net_value': (
                _parse_tr_pbmn_as_krw(
                    flow_output.get('frgn_ntby_tr_pbmn'),
                    field_name='frgn_ntby_tr_pbmn',
                )
                if include_optional_legs
                else (cached_market_row or {}).get('foreign_net_value')
            ),
            'institution_net_quantity': (
                _parse_int(flow_output.get('orgn_ntby_qty'), field_name='orgn_ntby_qty')
                if include_optional_legs
                else (cached_market_row or {}).get('institution_net_quantity')
            ),
            'institution_net_value': (
                _parse_tr_pbmn_as_krw(
                    flow_output.get('orgn_ntby_tr_pbmn'),
                    field_name='orgn_ntby_tr_pbmn',
                )
                if include_optional_legs
                else (cached_market_row or {}).get('institution_net_value')
            ),
            'investor_flow_value_unit': KRW_UNIT,
        }

    def _extract_provider_index_observed_at(self, payload: Mapping[str, Any]) -> datetime:
        rows = payload.get('output2')
        if not isinstance(rows, list) or not rows:
            raise MarketSnapshotUpstreamError(
                'KIS index intraday payload did not include provider business-date/time rows'
            )
        candidates: list[datetime] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            business_date = str(row.get('stck_bsop_date') or '').strip()
            session_time = str(row.get('stck_cntg_hour') or '').strip()
            if len(business_date) != 8 or not business_date.isdigit():
                continue
            if len(session_time) != 6 or not session_time.isdigit():
                continue
            try:
                parsed = datetime.strptime(
                    f'{business_date}{session_time}',
                    '%Y%m%d%H%M%S',
                ).replace(tzinfo=SEOUL_TZ)
            except ValueError:
                continue
            candidates.append(parsed)
        if not candidates:
            raise MarketSnapshotUpstreamError(
                'KIS index intraday payload had no valid provider business-date/time row'
            )
        return max(candidates)

    @staticmethod
    def _provider_observation_matches_index_payload(
        *,
        index_output_before: Mapping[str, Any],
        index_output_after: Mapping[str, Any],
        observation_payload: Mapping[str, Any],
        provider_observed_at: datetime,
        index_before_received_at: datetime,
        index_after_received_at: datetime,
    ) -> bool:
        rows = observation_payload.get('output2')
        if not isinstance(rows, list):
            return False
        expected_date = provider_observed_at.astimezone(SEOUL_TZ).strftime('%Y%m%d')
        expected_time = provider_observed_at.astimezone(SEOUL_TZ).strftime('%H%M%S')
        observed_row = next(
            (
                row
                for row in rows
                if isinstance(row, Mapping)
                and str(row.get('stck_bsop_date') or '').strip() == expected_date
                and str(row.get('stck_cntg_hour') or '').strip() == expected_time
            ),
            None,
        )
        if observed_row is None:
            return False
        before_price = _parse_float(
            index_output_before.get('bstp_nmix_prpr'),
            field_name='before.bstp_nmix_prpr',
        )
        after_price = _parse_float(
            index_output_after.get('bstp_nmix_prpr'),
            field_name='after.bstp_nmix_prpr',
        )
        observed_value = _parse_float(
            observed_row.get('bstp_nmix_prpr'),
            field_name='output2.bstp_nmix_prpr',
        )
        before_turnover = _parse_int(
            index_output_before.get('acml_tr_pbmn'),
            field_name='before.acml_tr_pbmn',
        )
        after_turnover = _parse_int(
            index_output_after.get('acml_tr_pbmn'),
            field_name='after.acml_tr_pbmn',
        )
        observed_turnover = _parse_int(
            observed_row.get('acml_tr_pbmn'),
            field_name='output2.acml_tr_pbmn',
        )
        if (
            index_before_received_at.tzinfo is None
            or index_before_received_at.utcoffset() is None
            or index_after_received_at.tzinfo is None
            or index_after_received_at.utcoffset() is None
            or provider_observed_at.tzinfo is None
            or provider_observed_at.utcoffset() is None
            or index_before_received_at > index_after_received_at
            or provider_observed_at > index_after_received_at
            or provider_observed_at.astimezone(SEOUL_TZ).date()
            != index_before_received_at.astimezone(SEOUL_TZ).date()
            or index_before_received_at.astimezone(SEOUL_TZ).date()
            != index_after_received_at.astimezone(SEOUL_TZ).date()
            or before_price is None
            or before_price <= 0
            or after_price is None
            or after_price <= 0
            or observed_value is None
            or observed_value <= 0
            or before_turnover is None
            or before_turnover < 0
            or after_turnover is None
            or after_turnover < before_turnover
            or observed_turnover is None
            or observed_turnover < 0
        ):
            return False
        if provider_observed_at <= index_before_received_at:
            return observed_turnover <= before_turnover <= after_turnover
        return before_turnover <= observed_turnover <= after_turnover

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
        if not settings.koreaexim_api_key:
            return self._build_cached_or_missing_usd_krw_snapshot(
                cached_snapshot=self._latest_usable_usd_krw_snapshot(cache_record),
                stale_reason='koreaexim_not_configured',
                detail='KOREAEXIM_API_KEY is not configured; the optional FX leg was skipped.',
            )
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
            response_received_at = self._now()
            snapshot = self._build_usd_krw_snapshot(
                payload,
                source='live',
                captured_at=response_received_at.isoformat(),
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

    def _build_snapshot_metadata(
        self,
        *,
        market_rows: list[Mapping[str, Any]],
        usd_krw_snapshot: Mapping[str, Any],
        fx_leg_status: str,
    ) -> dict[str, Any]:
        missing_fields: list[str] = []
        index_statuses = [
            str(_require_mapping(row.get('index_leg'), field_name='index_leg').get('status'))
            for row in market_rows
        ]
        flow_statuses = [
            str(_require_mapping(row.get('flow_leg'), field_name='flow_leg').get('status'))
            for row in market_rows
        ]
        index_leg_status = (
            'ready'
            if index_statuses and all(status == 'ready' for status in index_statuses)
            else 'unverified'
            if index_statuses and all(status == 'unverified' for status in index_statuses)
            else 'partial'
        )
        flow_leg_status = (
            'ready'
            if flow_statuses and all(status == 'ready' for status in flow_statuses)
            else 'cached'
            if flow_statuses and all(status == 'cached' for status in flow_statuses)
            else 'unavailable'
            if flow_statuses and all(status == 'unavailable' for status in flow_statuses)
            else 'partial'
        )
        if flow_leg_status not in {'ready', 'cached'}:
            missing_fields.append('markets.investor_flow')
        if usd_krw_snapshot.get('rate') is None:
            missing_fields.append('usd_krw.rate')
        if usd_krw_snapshot.get('effective_date') is None:
            missing_fields.append('usd_krw.effective_date')

        if index_leg_status != 'ready' or missing_fields:
            status = 'partial'
        elif usd_krw_snapshot.get('status') == 'stale':
            status = 'stale'
        else:
            status = 'ready'

        return {
            'status': status,
            'missing_fields': missing_fields,
            'stale_reason': usd_krw_snapshot.get('stale_reason'),
            'legs': {
                'index': {'status': index_leg_status},
                'flow': {'status': flow_leg_status},
                'fx': {'status': fx_leg_status},
            },
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

    def _optional_legs_are_due(self, cache_record: MarketSnapshotCacheRecord | None) -> bool:
        if cache_record is None or not isinstance(cache_record.payload, Mapping):
            return True
        refreshed_at = _normalize_iso_datetime(cache_record.payload.get('optional_legs_refreshed_at'))
        if refreshed_at is None:
            return True
        return self._now() >= refreshed_at + timedelta(seconds=self.refresh_interval_seconds)

    def _next_refresh_due_at(
        self,
        cache_record: MarketSnapshotCacheRecord | None,
    ) -> datetime | None:
        if cache_record is None:
            return None
        due_candidates: list[datetime] = []
        reference = (
            cache_record.last_refresh_attempted_at
            or cache_record.last_refresh_succeeded_at
            or cache_record.as_of
        )
        if reference is None and isinstance(cache_record.payload, dict):
            reference = _normalize_iso_datetime(cache_record.payload.get('as_of'))
        if reference is not None:
            due_candidates.append(reference + timedelta(seconds=self.refresh_interval_seconds))
        if isinstance(cache_record.payload, Mapping):
            optional_legs_refreshed_at = _normalize_iso_datetime(
                cache_record.payload.get('optional_legs_refreshed_at')
            )
            if optional_legs_refreshed_at is not None:
                due_candidates.append(
                    optional_legs_refreshed_at
                    + timedelta(seconds=self.refresh_interval_seconds)
                )
        return min(due_candidates) if due_candidates else None


market_snapshot_service = MarketSnapshotService()
