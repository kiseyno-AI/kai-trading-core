from __future__ import annotations

import asyncio
import copy
import hashlib
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from app.brokers.kis.client import KISClient
from app.brokers.kis.provider import kis_real_client
from app.eod.pykrx_enrichment import is_decision_usable_pykrx_enrichment_row
from app.market.clock import async_get_market_state
from app.market.snapshot import KIS_INDEX_OBSERVATION_SOURCE, market_snapshot_service
from app.market_data_store import load_daily_pykrx_enrichment_rows
from app.quotes.kis_realtime_prices import (
    KIS_REALTIME_PRICE_SOURCE,
    KISRealtimePriceService,
    kis_realtime_price_service,
)
from app.market.symbol_codes import normalize_symbol_text as _normalize_symbol

SEOUL_TZ = ZoneInfo('Asia/Seoul')
LIVE_SECTOR_PROJECTION_SCHEMA_VERSION = 'live_sector_projection_v1'
LIVE_SECTOR_PROJECTION_SOURCE = 'core_verified_pykrx_sector_plus_kis_current_session'
LIVE_SECTOR_PROJECTION_MAX_AGE_SECONDS = 360
LIVE_SECTOR_PROJECTION_MAX_FUTURE_SKEW_SECONDS = 30
LIVE_SECTOR_PROJECTION_CACHE_TTL_SECONDS = 30
LIVE_SECTOR_PROJECTION_MAX_COHORT_SYMBOLS = 8
LIVE_SECTOR_PROJECTION_MIN_MARKET_CAP_COVERAGE_RATIO = Decimal('0.60')
LIVE_SECTOR_PROJECTION_QUOTE_CONCURRENCY = 4
KRX_REGULAR_SESSION_OPEN = time(9, 0)
KRX_REGULAR_SESSION_CLOSE = time(15, 30)
SUPPORTED_MARKETS = frozenset({'KOSPI', 'KOSDAQ'})


@dataclass(frozen=True)
class _ProjectionCacheEntry:
    stored_at: datetime
    payload: dict[str, Any]


class LiveSectorProjectionService:
    """Build a current-session sector proxy without reusing EOD performance.

    Persisted PyKRX rows supply only the sector identity and prior-session market
    cap weights. Every performance value comes from a same-session KIS quote or
    KIS market-index observation and is rejected when its provider timestamp is
    missing, stale, or on another business date.
    """

    def __init__(
        self,
        *,
        sector_rows_loader: Callable[..., list[dict[str, Any]]] = load_daily_pykrx_enrichment_rows,
        market_state_loader: Callable[[], Awaitable[Any]] = async_get_market_state,
        market_snapshot_loader: Callable[[], Awaitable[dict[str, Any]]] | None = None,
        realtime_price_service: KISRealtimePriceService = kis_realtime_price_service,
        quote_client: KISClient = kis_real_client,
        now: Callable[[], datetime] | None = None,
        max_age_seconds: int = LIVE_SECTOR_PROJECTION_MAX_AGE_SECONDS,
        max_cohort_symbols: int = LIVE_SECTOR_PROJECTION_MAX_COHORT_SYMBOLS,
        min_market_cap_coverage_ratio: Decimal = (
            LIVE_SECTOR_PROJECTION_MIN_MARKET_CAP_COVERAGE_RATIO
        ),
        cache_ttl_seconds: int = LIVE_SECTOR_PROJECTION_CACHE_TTL_SECONDS,
        quote_concurrency: int = LIVE_SECTOR_PROJECTION_QUOTE_CONCURRENCY,
    ) -> None:
        self.sector_rows_loader = sector_rows_loader
        self.market_state_loader = market_state_loader
        self.market_snapshot_loader = (
            market_snapshot_loader or market_snapshot_service.get_market_snapshot
        )
        self.realtime_price_service = realtime_price_service
        self.quote_client = quote_client
        self._now = now or (lambda: datetime.now(UTC))
        self.max_age_seconds = max(1, int(max_age_seconds))
        self.max_cohort_symbols = max(1, int(max_cohort_symbols))
        self.min_market_cap_coverage_ratio = min(
            Decimal('1'),
            max(Decimal('0'), Decimal(min_market_cap_coverage_ratio)),
        )
        self.cache_ttl_seconds = max(0, int(cache_ttl_seconds))
        self.quote_concurrency = max(1, int(quote_concurrency))
        self._cache_lock = asyncio.Lock()
        self._cache: dict[tuple[str, ...], _ProjectionCacheEntry] = {}
        self._inflight: dict[tuple[str, ...], asyncio.Task[dict[str, Any]]] = {}

    async def get_projection(
        self,
        *,
        symbol: str,
        source_market_date: date,
    ) -> dict[str, Any]:
        normalized_symbol = _normalize_symbol(symbol)
        if not normalized_symbol:
            raise ValueError('symbol is required')

        checked_at = _aware_utc(self._now())
        market_state = await self._load_market_state()
        session_date = _market_state_date(market_state)
        state_name = str(_value(market_state, 'state') or '').strip().upper()
        is_open = _value(market_state, 'is_open') is True
        if session_date is None or not state_name:
            return _base_unavailable_payload(
                symbol=normalized_symbol,
                source_market_date=source_market_date,
                market_date=session_date,
                reason='market_state_unavailable',
                missing_fields=['market_state'],
            )
        if source_market_date > session_date:
            return _base_unavailable_payload(
                symbol=normalized_symbol,
                source_market_date=source_market_date,
                market_date=session_date,
                reason='sector_mapping_date_is_after_current_session',
                missing_fields=['sector_identity'],
            )

        identity_result = await asyncio.to_thread(
            self._load_verified_sector_identity,
            symbol=normalized_symbol,
            source_market_date=source_market_date,
        )
        if identity_result['status'] != 'available':
            return _base_unavailable_payload(
                symbol=normalized_symbol,
                source_market_date=source_market_date,
                market_date=session_date,
                reason=str(identity_result['reason']),
                market=identity_result.get('market'),
                sector_identity=identity_result.get('sector_identity'),
                cohort=identity_result.get('cohort'),
                missing_fields=list(identity_result.get('missing_fields') or ['sector_identity']),
            )

        market = str(identity_result['market'])
        identity = dict(identity_result['sector_identity'])
        cohort = copy.deepcopy(identity_result['cohort'])
        if state_name == 'PREOPEN':
            return _pending_payload(
                symbol=normalized_symbol,
                source_market_date=source_market_date,
                market_date=session_date,
                market=market,
                identity=identity,
                cohort=cohort,
                reason='current_session_sector_performance_pending_market_open',
            )
        if state_name != 'OPEN' or not is_open:
            return _base_unavailable_payload(
                symbol=normalized_symbol,
                source_market_date=source_market_date,
                market_date=session_date,
                market=market,
                reason='current_session_sector_performance_unavailable_market_not_open',
                sector_identity=identity,
                cohort=cohort,
                missing_fields=['current_session_quotes', 'current_session_benchmark'],
            )

        if _decimal(cohort.get('market_cap_coverage_ratio')) < self.min_market_cap_coverage_ratio:
            return _base_unavailable_payload(
                symbol=normalized_symbol,
                source_market_date=source_market_date,
                market_date=session_date,
                market=market,
                reason='sector_cohort_market_cap_coverage_below_threshold',
                sector_identity=identity,
                cohort=cohort,
                missing_fields=['sector_cohort_coverage'],
            )

        cache_key = (
            source_market_date.isoformat(),
            market,
            str(identity['sector_name']),
            session_date.isoformat(),
            str(cohort['membership_sha256']),
        )
        projection, served_from_cache = await self._load_or_build_dynamic_projection(
            cache_key=cache_key,
            market=market,
            session_date=session_date,
            cohort=cohort,
            checked_at=checked_at,
        )
        return {
            'schema_version': LIVE_SECTOR_PROJECTION_SCHEMA_VERSION,
            'status': projection['status'],
            'reason': projection['reason'],
            'symbol': normalized_symbol,
            'market': market,
            'market_date': session_date.isoformat(),
            'source_market_date': source_market_date.isoformat(),
            'observed_at': projection.get('observed_at'),
            'source': LIVE_SECTOR_PROJECTION_SOURCE,
            'sector_identity': identity,
            'cohort': projection['cohort'],
            'benchmark': projection.get('benchmark'),
            'projection': projection['projection'],
            'missing_fields': list(projection.get('missing_fields') or []),
            'served_from_cache': served_from_cache,
        }

    async def _load_market_state(self) -> Any:
        try:
            return await self.market_state_loader()
        except Exception:
            return None

    def _load_verified_sector_identity(
        self,
        *,
        symbol: str,
        source_market_date: date,
    ) -> dict[str, Any]:
        candidate_rows = self.sector_rows_loader(
            market_date=source_market_date,
            categories=['sector_classification'],
            scope_types=['symbol'],
            symbols=[symbol],
            source='pykrx',
            limit=10,
        )
        candidate_row = next(
            (
                row
                for row in candidate_rows
                if _row_symbol(row) == symbol
                and is_decision_usable_pykrx_enrichment_row(row)
            ),
            None,
        )
        if candidate_row is None:
            return {
                'status': 'unavailable',
                'reason': 'verified_pykrx_sector_identity_missing',
                'market': None,
                'sector_identity': None,
                'cohort': _empty_cohort(self),
                'missing_fields': ['sector_identity'],
            }

        market = str(candidate_row.get('market') or '').strip().upper()
        candidate_payload = _mapping(candidate_row.get('payload'))
        sector_name = str(candidate_payload.get('sector_name') or '').strip()
        if market not in SUPPORTED_MARKETS or not sector_name:
            return {
                'status': 'unavailable',
                'reason': 'verified_pykrx_sector_identity_invalid',
                'market': market or None,
                'sector_identity': None,
                'cohort': _empty_cohort(self),
                'missing_fields': ['sector_identity'],
            }

        market_rows = self.sector_rows_loader(
            market_date=source_market_date,
            categories=['sector_classification'],
            scope_types=['symbol'],
            markets=[market],
            source='pykrx',
            limit=None,
        )
        membership = _verified_market_membership(market_rows, market=market)
        if membership is None:
            return {
                'status': 'unavailable',
                'reason': 'pykrx_sector_market_membership_unverified',
                'market': market,
                'sector_identity': None,
                'cohort': _empty_cohort(self),
                'missing_fields': ['sector_market_membership_proof'],
            }

        sector_rows = [
            row
            for row in market_rows
            if str(_mapping(row.get('payload')).get('sector_name') or '').strip()
            == sector_name
        ]
        weighted_rows: list[dict[str, Any]] = []
        for row in sector_rows:
            market_cap = _positive_int(_mapping(row.get('payload')).get('market_cap'))
            if market_cap is None:
                return {
                    'status': 'unavailable',
                    'reason': 'sector_market_cap_weights_incomplete',
                    'market': market,
                    'sector_identity': _sector_identity_payload(
                        row=candidate_row,
                        market=market,
                        sector_name=sector_name,
                        source_market_date=source_market_date,
                        membership=membership,
                    ),
                    'cohort': _empty_cohort(self),
                    'missing_fields': ['sector_market_cap_weights'],
                }
            weighted_rows.append(
                {
                    'symbol': _row_symbol(row),
                    'prior_market_cap': market_cap,
                }
            )
        if not weighted_rows or not any(row['symbol'] == symbol for row in weighted_rows):
            return {
                'status': 'unavailable',
                'reason': 'candidate_sector_membership_inconsistent',
                'market': market,
                'sector_identity': None,
                'cohort': _empty_cohort(self),
                'missing_fields': ['sector_identity'],
            }

        weighted_rows.sort(key=lambda row: (-row['prior_market_cap'], row['symbol']))
        total_market_cap = sum(row['prior_market_cap'] for row in weighted_rows)
        selected_rows = weighted_rows[: self.max_cohort_symbols]
        selected_market_cap = sum(row['prior_market_cap'] for row in selected_rows)
        coverage_ratio = Decimal(selected_market_cap) / Decimal(total_market_cap)
        cohort_membership_sha = _cohort_membership_sha256(selected_rows)
        return {
            'status': 'available',
            'reason': None,
            'market': market,
            'sector_identity': _sector_identity_payload(
                row=candidate_row,
                market=market,
                sector_name=sector_name,
                source_market_date=source_market_date,
                membership=membership,
            ),
            'cohort': {
                'basis': 'prior_session_market_cap_top_cohort',
                'max_symbols': self.max_cohort_symbols,
                'required_market_cap_coverage_ratio': _decimal_number(
                    self.min_market_cap_coverage_ratio
                ),
                'sector_symbol_count': len(weighted_rows),
                'selected_symbol_count': len(selected_rows),
                'sector_market_cap': total_market_cap,
                'selected_market_cap': selected_market_cap,
                'market_cap_coverage_ratio': _decimal_number(coverage_ratio),
                'membership_sha256': cohort_membership_sha,
                'symbols': selected_rows,
            },
            'missing_fields': [],
        }

    async def _load_or_build_dynamic_projection(
        self,
        *,
        cache_key: tuple[str, ...],
        market: str,
        session_date: date,
        cohort: dict[str, Any],
        checked_at: datetime,
    ) -> tuple[dict[str, Any], bool]:
        task: asyncio.Task[dict[str, Any]]
        async with self._cache_lock:
            self._prune_cache(checked_at)
            cached = self._cache.get(cache_key)
            if cached is not None and self._cached_projection_is_current(
                cached,
                checked_at=checked_at,
            ):
                return copy.deepcopy(cached.payload), True
            task = self._inflight.get(cache_key)  # type: ignore[assignment]
            if task is None:
                task = asyncio.create_task(
                    self._build_dynamic_projection(
                        market=market,
                        session_date=session_date,
                        cohort=copy.deepcopy(cohort),
                        checked_at=checked_at,
                    ),
                    name='live-sector-projection',
                )
                self._inflight[cache_key] = task
        try:
            payload = await asyncio.shield(task)
        finally:
            if task.done():
                async with self._cache_lock:
                    if self._inflight.get(cache_key) is task:
                        self._inflight.pop(cache_key, None)
        async with self._cache_lock:
            self._cache[cache_key] = _ProjectionCacheEntry(
                stored_at=_aware_utc(self._now()),
                payload=copy.deepcopy(payload),
            )
        return payload, False

    def _prune_cache(self, checked_at: datetime) -> None:
        ttl = timedelta(seconds=self.cache_ttl_seconds)
        expired = [
            key
            for key, entry in self._cache.items()
            if checked_at < entry.stored_at or checked_at - entry.stored_at >= ttl
        ]
        for key in expired:
            self._cache.pop(key, None)

    def _cached_projection_is_current(
        self,
        entry: _ProjectionCacheEntry,
        *,
        checked_at: datetime,
    ) -> bool:
        if self.cache_ttl_seconds <= 0:
            return False
        age = checked_at - entry.stored_at
        if age < timedelta(0) or age >= timedelta(seconds=self.cache_ttl_seconds):
            return False
        if entry.payload.get('status') != 'ready':
            return True
        observed_at = _aware_datetime(entry.payload.get('observed_at'))
        return observed_at is not None and _observation_is_current(
            observed_at,
            session_date=date.fromisoformat(str(entry.payload['market_date'])),
            checked_at=checked_at,
            max_age_seconds=self.max_age_seconds,
        )

    async def _build_dynamic_projection(
        self,
        *,
        market: str,
        session_date: date,
        cohort: dict[str, Any],
        checked_at: datetime,
    ) -> dict[str, Any]:
        benchmark = await self._load_current_benchmark(
            market=market,
            session_date=session_date,
            checked_at=checked_at,
        )
        if benchmark is None:
            return _dynamic_unavailable(
                market_date=session_date,
                cohort=cohort,
                reason='current_session_market_benchmark_unavailable',
                missing_fields=['current_session_benchmark'],
            )

        semaphore = asyncio.Semaphore(self.quote_concurrency)

        async def load_quote(member: dict[str, Any]) -> dict[str, Any]:
            async with semaphore:
                quote = await self._load_current_quote(
                    symbol=str(member['symbol']),
                    session_date=session_date,
                    checked_at=checked_at,
                )
            return {
                **member,
                **quote,
            }

        quoted_members = await asyncio.gather(
            *(load_quote(member) for member in cohort['symbols'])
        )
        live_members = [
            member
            for member in quoted_members
            if member.get('quote_status') == 'available'
            and _decimal(member.get('change_pct')) is not None
        ]
        sector_market_cap = int(cohort['sector_market_cap'])
        live_market_cap = sum(int(member['prior_market_cap']) for member in live_members)
        live_coverage = Decimal(live_market_cap) / Decimal(sector_market_cap)
        cohort_with_quotes = {
            **cohort,
            'live_symbol_count': len(live_members),
            'live_market_cap': live_market_cap,
            'live_market_cap_coverage_ratio': _decimal_number(live_coverage),
            'symbols': quoted_members,
        }
        if live_coverage < self.min_market_cap_coverage_ratio:
            return _dynamic_unavailable(
                market_date=session_date,
                cohort=cohort_with_quotes,
                benchmark=benchmark,
                reason='current_session_sector_quote_coverage_below_threshold',
                missing_fields=['current_session_sector_quote_coverage'],
            )

        weighted_sum = sum(
            Decimal(int(member['prior_market_cap']))
            * _decimal(member['change_pct'])
            for member in live_members
        )
        sector_return = weighted_sum / Decimal(live_market_cap)
        benchmark_return = _decimal(benchmark['change_percent'])
        assert benchmark_return is not None
        relative_return = sector_return - benchmark_return
        observations = [
            _aware_datetime(benchmark['observed_at']),
            *(_aware_datetime(member.get('observed_at')) for member in live_members),
        ]
        observed_at = min(item for item in observations if item is not None)
        return {
            'status': 'ready',
            'reason': None,
            'market_date': session_date.isoformat(),
            'observed_at': observed_at.isoformat(),
            'cohort': cohort_with_quotes,
            'benchmark': benchmark,
            'projection': {
                'status': 'ready',
                'basis': 'prior_session_market_cap_weighted_live_quote_proxy_vs_live_market_index',
                'sector_return_pct': _decimal_number(sector_return),
                'benchmark_return_pct': _decimal_number(benchmark_return),
                'relative_return_pct': _decimal_number(relative_return),
                'live_market_cap_coverage_ratio': _decimal_number(live_coverage),
            },
            'missing_fields': [],
        }

    async def _load_current_benchmark(
        self,
        *,
        market: str,
        session_date: date,
        checked_at: datetime,
    ) -> dict[str, Any] | None:
        try:
            snapshot = await self.market_snapshot_loader()
        except Exception:
            return None
        if str(snapshot.get('status') or '').strip().lower() not in {
            'ready',
            'partial',
            'stale',
        }:
            return None
        markets = snapshot.get('markets')
        if not isinstance(markets, Mapping):
            return None
        raw_market = markets.get(market.lower())
        if not isinstance(raw_market, Mapping):
            raw_market = next(
                (
                    value
                    for value in markets.values()
                    if isinstance(value, Mapping)
                    and str(value.get('label') or '').strip().upper() == market
                ),
                None,
            )
        if not isinstance(raw_market, Mapping):
            return None
        index_leg = raw_market.get('index_leg')
        observed_at = _aware_datetime(raw_market.get('provider_observed_at'))
        if (
            str(raw_market.get('label') or '').strip().upper() != market
            or str(raw_market.get('provider_business_date') or '')
            != session_date.isoformat()
            or not isinstance(index_leg, Mapping)
            or str(index_leg.get('status') or '').strip().lower() != 'ready'
            or str(index_leg.get('observation_source') or '')
            != KIS_INDEX_OBSERVATION_SOURCE
            or observed_at is None
            or not _observation_is_current(
                observed_at,
                session_date=session_date,
                checked_at=checked_at,
                max_age_seconds=self.max_age_seconds,
            )
        ):
            return None
        leg_observed_at = _aware_datetime(index_leg.get('provider_observed_at'))
        if leg_observed_at is None or leg_observed_at != observed_at:
            return None
        change_percent = _decimal(raw_market.get('change_percent'))
        if change_percent is None:
            return None
        return {
            'market': market,
            'source': 'kis_market_snapshot_index',
            'provider_business_date': session_date.isoformat(),
            'observed_at': observed_at.isoformat(),
            'change_percent': _decimal_number(change_percent),
        }

    async def _load_current_quote(
        self,
        *,
        symbol: str,
        session_date: date,
        checked_at: datetime,
    ) -> dict[str, Any]:
        realtime = self._current_realtime_quote(
            symbol=symbol,
            session_date=session_date,
            checked_at=checked_at,
        )
        if realtime is not None:
            return realtime
        if getattr(self.quote_client, 'use_mock', None) is not False:
            return _unavailable_quote('real_kis_quote_client_unavailable')
        try:
            raw = await self.quote_client.fetch_domestic_quote(symbol=symbol)
        except Exception:
            return _unavailable_quote('kis_quote_request_failed')
        return _normalize_current_session_rest_quote(
            raw,
            session_date=session_date,
            checked_at=checked_at,
            max_age_seconds=self.max_age_seconds,
        )

    def _current_realtime_quote(
        self,
        *,
        symbol: str,
        session_date: date,
        checked_at: datetime,
    ) -> dict[str, Any] | None:
        client = getattr(self.realtime_price_service, 'client', None)
        if getattr(client, 'use_mock', None) is not False:
            return None
        fields = self.realtime_price_service.price_fields_for_symbol(symbol)
        observed_at = _aware_datetime(fields.get('price_as_of'))
        change_pct = _decimal(fields.get('change_pct'))
        current_price = _decimal(fields.get('current_price'))
        if (
            fields.get('price_status') != 'available'
            or fields.get('price_source') != KIS_REALTIME_PRICE_SOURCE
            or observed_at is None
            or change_pct is None
            or current_price is None
            or current_price <= 0
            or not _observation_is_current(
                observed_at,
                session_date=session_date,
                checked_at=checked_at,
                max_age_seconds=self.max_age_seconds,
            )
        ):
            return None
        return {
            'quote_status': 'available',
            'quote_source': KIS_REALTIME_PRICE_SOURCE,
            'observed_at': observed_at.isoformat(),
            'current_price': _decimal_number(current_price),
            'change_pct': _decimal_number(change_pct),
            'missing_reason': None,
        }


def _verified_market_membership(
    rows: Sequence[dict[str, Any]],
    *,
    market: str,
) -> dict[str, Any] | None:
    usable_rows = [
        row
        for row in rows
        if str(row.get('market') or '').strip().upper() == market
        and row.get('source') == 'pykrx'
        and is_decision_usable_pykrx_enrichment_row(row)
    ]
    symbols = {_row_symbol(row) for row in usable_rows if _row_symbol(row)}
    if not symbols or len(usable_rows) != len(symbols):
        return None
    membership_sha = _membership_sha256(symbols)
    for row in usable_rows:
        payload = _mapping(row.get('payload'))
        if (
            payload.get('universe_market_complete') is not True
            or _positive_int(payload.get('universe_reference_count')) != len(symbols)
            or str(payload.get('universe_reference_sha256') or '') != membership_sha
            or str(payload.get('universe_reference_source') or '')
            != 'pykrx_market_ticker_list'
        ):
            return None
    return {
        'symbol_count': len(symbols),
        'sha256': membership_sha,
        'source': 'pykrx_market_ticker_list',
    }


def _sector_identity_payload(
    *,
    row: dict[str, Any],
    market: str,
    sector_name: str,
    source_market_date: date,
    membership: dict[str, Any],
) -> dict[str, Any]:
    return {
        'status': 'available',
        'sector_name': sector_name,
        'market': market,
        'mapping_date': source_market_date.isoformat(),
        'mapping_source': 'pykrx',
        'captured_at': row.get('captured_at'),
        'universe_market_complete': True,
        'universe_reference_count': membership['symbol_count'],
        'universe_reference_sha256': membership['sha256'],
        'universe_reference_source': membership['source'],
    }


def _normalize_current_session_rest_quote(
    raw: Mapping[str, Any],
    *,
    session_date: date,
    checked_at: datetime,
    max_age_seconds: int,
) -> dict[str, Any]:
    output = raw.get('output')
    if not isinstance(output, Mapping):
        return _unavailable_quote('kis_quote_output_missing')
    raw_business_date = str(output.get('stck_bsop_date') or '').strip()
    raw_time = str(output.get('stck_cntg_hour') or '').strip()
    if (
        len(raw_business_date) != 8
        or not raw_business_date.isdigit()
        or len(raw_time) < 6
        or not raw_time[:6].isdigit()
    ):
        return _unavailable_quote('kis_quote_provider_timestamp_missing')
    try:
        business_date = datetime.strptime(raw_business_date, '%Y%m%d').date()
        local_time = datetime.strptime(raw_time[:6], '%H%M%S').time()
    except ValueError:
        return _unavailable_quote('kis_quote_provider_timestamp_invalid')
    observed_at = datetime.combine(business_date, local_time, tzinfo=SEOUL_TZ)
    change_pct = _decimal(output.get('prdy_ctrt'))
    current_price = _decimal(output.get('stck_prpr'))
    if (
        business_date != session_date
        or local_time < KRX_REGULAR_SESSION_OPEN
        or local_time > KRX_REGULAR_SESSION_CLOSE
        or change_pct is None
        or current_price is None
        or current_price <= 0
        or not _observation_is_current(
            observed_at,
            session_date=session_date,
            checked_at=checked_at,
            max_age_seconds=max_age_seconds,
        )
    ):
        return _unavailable_quote('kis_quote_not_current_session')
    return {
        'quote_status': 'available',
        'quote_source': 'kis_rest_inquire_price',
        'observed_at': observed_at.isoformat(),
        'current_price': _decimal_number(current_price),
        'change_pct': _decimal_number(change_pct),
        'missing_reason': None,
    }


def _observation_is_current(
    observed_at: datetime,
    *,
    session_date: date,
    checked_at: datetime,
    max_age_seconds: int,
) -> bool:
    if observed_at.astimezone(SEOUL_TZ).date() != session_date:
        return False
    age_seconds = (checked_at - observed_at.astimezone(UTC)).total_seconds()
    return (
        age_seconds >= -LIVE_SECTOR_PROJECTION_MAX_FUTURE_SKEW_SECONDS
        and age_seconds <= max_age_seconds
    )


def _base_unavailable_payload(
    *,
    symbol: str,
    source_market_date: date,
    market_date: date | None,
    reason: str,
    market: str | None = None,
    sector_identity: dict[str, Any] | None = None,
    cohort: dict[str, Any] | None = None,
    missing_fields: list[str] | None = None,
) -> dict[str, Any]:
    return {
        'schema_version': LIVE_SECTOR_PROJECTION_SCHEMA_VERSION,
        'status': 'unavailable',
        'reason': reason,
        'symbol': symbol,
        'market': market,
        'market_date': market_date.isoformat() if market_date else None,
        'source_market_date': source_market_date.isoformat(),
        'observed_at': None,
        'source': LIVE_SECTOR_PROJECTION_SOURCE,
        'sector_identity': sector_identity,
        'cohort': cohort or {},
        'benchmark': None,
        'projection': {
            'status': 'unavailable',
            'basis': 'current_session_evidence_required',
            'sector_return_pct': None,
            'benchmark_return_pct': None,
            'relative_return_pct': None,
            'live_market_cap_coverage_ratio': None,
        },
        'missing_fields': missing_fields or [],
        'served_from_cache': False,
    }


def _pending_payload(
    *,
    symbol: str,
    source_market_date: date,
    market_date: date,
    market: str,
    identity: dict[str, Any],
    cohort: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    payload = _base_unavailable_payload(
        symbol=symbol,
        source_market_date=source_market_date,
        market_date=market_date,
        reason=reason,
        market=market,
        sector_identity=identity,
        cohort=cohort,
        missing_fields=['current_session_quotes', 'current_session_benchmark'],
    )
    payload['status'] = 'pending_current_session'
    payload['projection']['status'] = 'pending_current_session'
    return payload


def _dynamic_unavailable(
    *,
    market_date: date,
    cohort: dict[str, Any],
    reason: str,
    missing_fields: list[str],
    benchmark: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        'status': 'unavailable',
        'reason': reason,
        'market_date': market_date.isoformat(),
        'observed_at': None,
        'cohort': cohort,
        'benchmark': benchmark,
        'projection': {
            'status': 'unavailable',
            'basis': 'current_session_evidence_required',
            'sector_return_pct': None,
            'benchmark_return_pct': (
                benchmark.get('change_percent') if benchmark else None
            ),
            'relative_return_pct': None,
            'live_market_cap_coverage_ratio': cohort.get(
                'live_market_cap_coverage_ratio'
            ),
        },
        'missing_fields': missing_fields,
    }


def _empty_cohort(service: LiveSectorProjectionService) -> dict[str, Any]:
    return {
        'basis': 'prior_session_market_cap_top_cohort',
        'max_symbols': service.max_cohort_symbols,
        'required_market_cap_coverage_ratio': _decimal_number(
            service.min_market_cap_coverage_ratio
        ),
        'sector_symbol_count': 0,
        'selected_symbol_count': 0,
        'sector_market_cap': None,
        'selected_market_cap': None,
        'market_cap_coverage_ratio': None,
        'membership_sha256': None,
        'symbols': [],
    }


def _unavailable_quote(reason: str) -> dict[str, Any]:
    return {
        'quote_status': 'unavailable',
        'quote_source': None,
        'observed_at': None,
        'current_price': None,
        'change_pct': None,
        'missing_reason': reason,
    }


def _market_state_date(state: Any) -> date | None:
    raw_date = _value(state, 'current_calendar_date')
    try:
        explicit_date = date.fromisoformat(str(raw_date)) if raw_date else None
    except ValueError:
        return None
    server_time = _aware_datetime(_value(state, 'server_time'))
    if server_time is None:
        return None
    server_date = server_time.astimezone(SEOUL_TZ).date()
    if explicit_date is not None and explicit_date != server_date:
        return None
    return explicit_date or server_date


def _value(value: Any, key: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _row_symbol(row: Mapping[str, Any]) -> str:
    return _normalize_symbol(row.get('symbol') or row.get('scope_key'))


def _membership_sha256(symbols: Sequence[str] | set[str]) -> str:
    normalized = sorted(
        {_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol)}
    )
    return hashlib.sha256('\n'.join(normalized).encode('utf-8')).hexdigest()


def _cohort_membership_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    values = [
        f"{_normalize_symbol(row.get('symbol'))}:{int(row['prior_market_cap'])}"
        for row in rows
    ]
    return hashlib.sha256('\n'.join(values).encode('utf-8')).hexdigest()


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 and str(value).strip() == str(parsed) else None


def _decimal(value: Any) -> Decimal | None:
    if value in (None, '') or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).replace(',', '').strip())
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_number(value: Decimal) -> float:
    return float(value.quantize(Decimal('0.000001')))


def _aware_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('now must be timezone-aware')
    return value.astimezone(UTC)


live_sector_projection_service = LiveSectorProjectionService()
