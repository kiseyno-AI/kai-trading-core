from __future__ import annotations

import argparse
import asyncio
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Sequence

from pykrx import stock
from pykrx.website.comm import webio

from app.agent_context.derived_contexts import daily_price_context_service
from app.core.audit_logging import log_fetch_event
from app.core.settings import settings
from app.eod.master import get_master_symbols
from app.eod.pykrx_pipeline_support import (
    build_daily_retry_context,
    build_daily_skip_result,
)
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.eod.pykrx_enrichment import pykrx_enrichment_service
from app.eod.action_summaries import daily_action_summary_service
from app.eod.pykrx_pipeline_runner import PykrxPipelineDependencies, run_market_data_pipeline
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.signals.daily_signal_features import daily_signal_feature_service
from app.signals.market_risk_filter import daily_market_risk_filter_service
from app.market_data_store import (
    advisory_eod_collection_lock,
    compute_and_persist_daily_trending_ranks,
    count_eod_run_persisted_symbols,
    count_eod_collection_runs,
    complete_eod_collection_run,
    create_eod_collection_run,
    load_daily_price_bar_dates_for_symbol,
    list_symbols_with_daily_bars_in_range,
    load_eod_run_persisted_symbols,
    load_symbols,
    load_latest_eod_collection_run,
    persist_daily_price_bars,
    update_daily_price_bar_fundamentals,
    update_eod_collection_run_progress,
    upsert_symbol_universe,
)

logger = logging.getLogger(__name__)
_PYKRX_REQUEST_TIMEOUT_LOCK = threading.RLock()

DEFAULT_START_DATE = '19560101'
DEFAULT_MARKETS = ('KOSPI', 'KOSDAQ', 'KONEX')
MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE = 5
RETRYABLE_DAILY_STATUSES = {'completed_with_errors', 'failed'}
PYKRX_STAGE_SEQUENCE = (
    'price_collection',
    'pykrx_enrichment',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
    'signal_features',
    'market_risk_filter',
)


def _log_pykrx_fetch(operation: str, status: str, **fields) -> None:
    log_fetch_event(service='pykrx', operation=operation, status=status, **fields)


@dataclass
class PykrxSymbol:
    symbol: str
    market: str
    name_ko: str
    standard_code: str | None = None


def _normalize_yyyymmdd(value: str | None, *, default: str | None = None) -> str:
    candidate = value or default
    if not candidate:
        raise ValueError('A date value is required.')
    normalized = candidate.replace('-', '')
    datetime.strptime(normalized, '%Y%m%d')
    return normalized


def _to_iso_date(value: object) -> str:
    if hasattr(value, 'date'):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _normalize_iso_date(value: str) -> str:
    return datetime.strptime(value, '%Y%m%d').date().isoformat()


@contextmanager
def _pykrx_request_timeout(timeout_seconds: int):
    if timeout_seconds <= 0:
        yield
        return

    with _PYKRX_REQUEST_TIMEOUT_LOCK:
        original_get = webio.requests.get
        original_post = webio.requests.post

        def _wrap_with_timeout(func):
            def _wrapped(*args, **kwargs):
                kwargs.setdefault('timeout', timeout_seconds)
                return func(*args, **kwargs)

            return _wrapped

        webio.requests.get = _wrap_with_timeout(original_get)
        webio.requests.post = _wrap_with_timeout(original_post)
        try:
            yield
        finally:
            webio.requests.get = original_get
            webio.requests.post = original_post


def _call_pykrx(func, *args, **kwargs):
    with _pykrx_request_timeout(settings.eod_pykrx_request_timeout_seconds):
        return func(*args, **kwargs)


def _coerce_pykrx_ticker_name(value: object, *, ticker: str) -> str:
    """Return a DB-safe ticker name from pykrx output.

    pykrx normally returns a plain string from ``get_market_ticker_name``.
    During provider/session instability it can return non-scalar objects (for
    example an HTML-parsed DataFrame).  Those objects cannot be bound into the
    symbols table by psycopg, so keep the daily pipeline moving by falling back
    to the ticker code and logging the unexpected shape for diagnosis.
    """

    if isinstance(value, str):
        normalized = value.strip()
        return normalized or ticker
    if value is None:
        return ticker

    logger.warning(
        'pykrx ticker name lookup returned non-string value for %s: %s',
        ticker,
        type(value).__name__,
    )
    return ticker


def _get_market_ohlcv_by_date(start_date: str, end_date: str, symbol: str):
    getter = getattr(stock, 'get_market_ohlcv_by_date', None) or getattr(stock, 'get_market_ohlcv')
    return _call_pykrx(getter, start_date, end_date, symbol)


def _get_market_ohlcv_by_ticker(trade_date: str, market: str):
    """Return market-wide OHLCV for one date.

    PyKRX's per-symbol date-range API can omit/zero 거래대금 depending on the
    endpoint/version, while the market-wide daily API carries provider trading
    value.  Use this as the authoritative turnover source for EOD bars.
    """
    getter = getattr(stock, 'get_market_ohlcv_by_ticker', None) or getattr(stock, 'get_market_ohlcv')
    return _call_pykrx(getter, trade_date, market)


def _get_market_fundamental_by_date(start_date: str, end_date: str, symbol: str):
    getter = getattr(stock, 'get_market_fundamental_by_date', None) or getattr(stock, 'get_market_fundamental')
    return _call_pykrx(getter, start_date, end_date, symbol)


def _is_auto_daily_trigger(trigger: str) -> bool:
    return trigger.startswith('auto_')


def _dedupe_symbol_codes(symbols: Sequence[object]) -> list[str]:
    deduped: dict[str, None] = {}
    for symbol in symbols:
        normalized = str(symbol).strip()
        if not normalized:
            continue
        if normalized.isdigit():
            normalized = normalized.zfill(6)
        deduped.setdefault(normalized, None)
    return list(deduped.keys())


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_activity_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _update_run_stage_metadata(
    metadata: dict | None,
    *,
    stage: str,
    status: str,
    current_step: str,
    current_activity: str,
    processed: int | None = None,
    total: int | None = None,
    succeeded: int | None = None,
    failed: int | None = None,
    current_symbol: str | None = None,
    current_period: str | None = None,
    extra: dict | None = None,
) -> dict:
    updated = dict(metadata or {})
    now = _utcnow_iso()

    stage_progress = dict(updated.get('stage_progress') or {})
    entry = dict(stage_progress.get(stage) or {})
    entry.setdefault('started_at', now)
    entry['updated_at'] = now
    entry['status'] = status
    entry['current_step'] = current_step
    entry['current_activity'] = current_activity

    if processed is not None:
        entry['processed'] = int(processed)
    if total is not None:
        entry['total'] = int(total)
    if succeeded is not None:
        entry['succeeded'] = int(succeeded)
    if failed is not None:
        entry['failed'] = int(failed)
    if current_symbol is not None:
        entry['current_symbol'] = current_symbol
    else:
        entry.pop('current_symbol', None)
    if current_period is not None:
        entry['current_period'] = current_period
    else:
        entry.pop('current_period', None)
    if extra:
        entry.update(extra)
    if status in {'completed', 'partial', 'failed'}:
        entry['completed_at'] = now

    stage_progress[stage] = entry
    updated['stage'] = stage
    updated['current_step'] = current_step
    updated['current_activity'] = current_activity
    updated['last_progress_at'] = now
    updated['stage_progress'] = stage_progress

    if current_symbol is not None:
        updated['current_symbol'] = current_symbol
    else:
        updated.pop('current_symbol', None)
    if current_period is not None:
        updated['current_period'] = current_period
    else:
        updated.pop('current_period', None)

    return updated


class PykrxBackfillService:
    symbol_pause_seconds = 0.02
    name_pause_seconds = 0.005

    def refresh_recent_universe(
        self,
        *,
        markets: Sequence[str] = DEFAULT_MARKETS,
        limit: int | None = None,
        deactivate_missing: bool = True,
    ) -> dict:
        _log_pykrx_fetch(
            'refresh_recent_universe',
            'started',
            markets=list(markets),
            limit=limit,
            deactivate_missing=deactivate_missing,
        )
        snapshot = self.fetch_recent_universe_snapshot(markets=markets, limit=limit)
        symbols = snapshot['symbols']
        persisted = self._persist_universe(symbols, deactivate_missing=deactivate_missing)
        result = {
            'source': 'pykrx',
            'markets': list(markets),
            'symbol_count': len(symbols),
            'persisted_count': persisted,
            'latest_trade_date': snapshot['latest_trade_date'],
            'universe_source': snapshot['universe_source'],
            'deactivate_missing': deactivate_missing,
        }
        _log_pykrx_fetch('refresh_recent_universe', 'completed', **result)
        return result

    def _persist_universe(self, symbols: Sequence[PykrxSymbol], *, deactivate_missing: bool) -> int:
        return upsert_symbol_universe(
            symbols,
            source='pykrx',
            stale_sources=['pykrx', 'kis_master'] if deactivate_missing else [],
            stale_markets=list({item.market for item in symbols}),
        )

    def fetch_recent_universe(
        self,
        *,
        markets: Sequence[str] = DEFAULT_MARKETS,
        limit: int | None = None,
    ) -> list[PykrxSymbol]:
        return self.fetch_recent_universe_snapshot(markets=markets, limit=limit)['symbols']

    def fetch_recent_universe_snapshot(
        self,
        *,
        markets: Sequence[str] = DEFAULT_MARKETS,
        limit: int | None = None,
    ) -> dict:
        latest_trade_date = self.resolve_latest_trade_date()
        as_of_date = latest_trade_date.replace('-', '')
        pykrx_snapshot = self._fetch_pykrx_market_universe(as_of_date=as_of_date, markets=markets, limit=limit)
        items = pykrx_snapshot['symbols']
        universe_source = pykrx_snapshot['universe_source']
        if not items:
            items = self._fetch_master_fallback_universe(markets=markets, limit=limit)
            universe_source = 'kis_master_fallback'
        return {
            'symbols': items,
            'latest_trade_date': latest_trade_date,
            'universe_source': universe_source,
        }

    def _fetch_pykrx_market_universe(
        self,
        *,
        as_of_date: str,
        markets: Sequence[str],
        limit: int | None = None,
    ) -> dict:
        items: list[PykrxSymbol] = []
        used_master_fallback = False
        for market in markets:
            _log_pykrx_fetch('fetch_market_universe', 'started', market=market, as_of_date=as_of_date, limit=limit)
            try:
                frame = _call_pykrx(stock.get_market_ohlcv_by_ticker, as_of_date, market=market)
            except Exception as exc:
                logger.warning('pykrx market universe fetch failed for %s on %s: %s', market, as_of_date, exc)
                _log_pykrx_fetch(
                    'fetch_market_universe',
                    'failed',
                    market=market,
                    as_of_date=as_of_date,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    fallback_source='kis_master',
                )
                fallback_items = self._fetch_master_fallback_for_market(
                    market=market,
                    as_of_date=as_of_date,
                    current_count=len(items),
                    limit=limit,
                )
                used_master_fallback = used_master_fallback or bool(fallback_items)
                items.extend(fallback_items)
                if limit and len(items) >= limit:
                    break
                continue
            tickers = self._extract_market_universe_tickers(frame)
            if not tickers:
                logger.warning('pykrx market universe returned no usable tickers for %s on %s', market, as_of_date)
                _log_pykrx_fetch(
                    'fetch_market_universe',
                    'empty',
                    market=market,
                    as_of_date=as_of_date,
                    fallback_source='kis_master',
                )
                fallback_items = self._fetch_master_fallback_for_market(
                    market=market,
                    as_of_date=as_of_date,
                    current_count=len(items),
                    limit=limit,
                )
                used_master_fallback = used_master_fallback or bool(fallback_items)
                items.extend(fallback_items)
                if limit and len(items) >= limit:
                    break
                continue
            for ticker in tickers:
                try:
                    raw_name_ko = _call_pykrx(stock.get_market_ticker_name, ticker)
                except Exception:
                    logger.warning('Falling back to ticker as name for %s', ticker)
                    name_ko = ticker
                else:
                    name_ko = _coerce_pykrx_ticker_name(raw_name_ko, ticker=ticker)
                items.append(
                    PykrxSymbol(
                        symbol=ticker,
                        market=market,
                        name_ko=name_ko,
                    )
                )
                if self.name_pause_seconds:
                    time.sleep(self.name_pause_seconds)
                if limit and len(items) >= limit:
                    _log_pykrx_fetch(
                        'fetch_market_universe',
                        'completed',
                        market=market,
                        as_of_date=as_of_date,
                        ticker_count=len(tickers),
                        collected_count=len(items),
                        limit=limit,
                        fallback_used=used_master_fallback,
                    )
                    return {
                        'symbols': items,
                        'universe_source': (
                            'pykrx_market_ohlcv_with_master_fallback'
                            if used_master_fallback
                            else 'pykrx_market_ohlcv'
                        ),
                    }
            _log_pykrx_fetch(
                'fetch_market_universe',
                'completed',
                market=market,
                as_of_date=as_of_date,
                ticker_count=len(tickers),
                collected_count=len(items),
                limit=limit,
                fallback_used=used_master_fallback,
            )
        return {
            'symbols': items,
            'universe_source': (
                'pykrx_market_ohlcv_with_master_fallback' if used_master_fallback else 'pykrx_market_ohlcv'
            ),
        }

    def _extract_market_universe_tickers(self, frame) -> list[str]:
        if getattr(frame, 'empty', True):
            return []
        index = getattr(frame, 'index', None)
        if index is None:
            return []
        raw_tickers = index.tolist() if hasattr(index, 'tolist') else list(index)
        items: list[str] = []
        for ticker in raw_tickers:
            normalized = str(ticker).zfill(6)
            if not normalized.isdigit() or len(normalized) != 6:
                continue
            items.append(normalized)
        return items

    def _fetch_master_fallback_for_market(
        self,
        *,
        market: str,
        as_of_date: str,
        current_count: int,
        limit: int | None,
    ) -> list[PykrxSymbol]:
        remaining = None if limit is None else max(limit - current_count, 0)
        if remaining == 0:
            return []
        logger.warning('Falling back to KIS master universe for %s on %s', market, as_of_date)
        return self._fetch_master_fallback_universe(markets=(market,), limit=remaining)

    def _fetch_master_fallback_universe(
        self,
        *,
        markets: Sequence[str],
        limit: int | None = None,
    ) -> list[PykrxSymbol]:
        allowed_markets = {market.upper() for market in markets}
        items: list[PykrxSymbol] = []
        for entry in get_master_symbols():
            if entry.market.upper() not in allowed_markets:
                continue
            if not entry.symbol.isdigit() or len(entry.symbol) != 6:
                continue
            items.append(
                PykrxSymbol(
                    symbol=entry.symbol,
                    market=entry.market,
                    name_ko=entry.name_ko,
                    standard_code=entry.standard_code,
                )
            )
            if limit and len(items) >= limit:
                _log_pykrx_fetch(
                    'fetch_master_fallback_universe',
                    'completed',
                    markets=list(markets),
                    symbol_count=len(items),
                    limit=limit,
                )
                return items
        _log_pykrx_fetch(
            'fetch_master_fallback_universe',
            'completed',
            markets=list(markets),
            symbol_count=len(items),
            limit=limit,
        )
        return items

    def load_active_universe(
        self,
        *,
        markets: Sequence[str] = DEFAULT_MARKETS,
        symbol: str | None = None,
        limit: int | None = None,
    ) -> list[PykrxSymbol]:
        rows = load_symbols(markets=list(markets), limit=limit)
        items: list[PykrxSymbol] = []
        seen: set[str] = set()
        allowed_markets = {market.upper() for market in markets}
        for row in rows:
            symbol_code = str(row['symbol']).strip()
            market = str(row['market'] or '').strip()
            if not symbol_code.isdigit() or len(symbol_code) != 6:
                continue
            if market.upper() not in allowed_markets:
                continue
            if symbol_code in seen:
                continue
            seen.add(symbol_code)
            items.append(
                PykrxSymbol(
                    symbol=symbol_code,
                    market=market,
                    name_ko=row['name_ko'],
                    standard_code=row['standard_code'],
                )
            )
        if symbol:
            items = [item for item in items if item.symbol == symbol]
        return items

    def find_symbol(self, symbol: str, *, markets: Sequence[str] = DEFAULT_MARKETS) -> PykrxSymbol | None:
        target = symbol.strip()
        for entry in get_master_symbols():
            if entry.symbol != target:
                continue
            if entry.market.upper() not in {market.upper() for market in markets}:
                return None
            return PykrxSymbol(
                symbol=entry.symbol,
                market=entry.market,
                name_ko=entry.name_ko,
                standard_code=entry.standard_code,
            )
        return None

    def resolve_latest_trade_date(
        self,
        *,
        end_date: str | None = None,
        probe_symbol: str = '005930',
    ) -> str:
        return _to_iso_date(datetime.strptime(self._resolve_reference_trade_date(end_date=end_date, probe_symbol=probe_symbol), '%Y%m%d'))

    def _resolve_reference_trade_date(
        self,
        *,
        end_date: str | None = None,
        probe_symbol: str = '005930',
        lookback_days: int = 730,
    ) -> str:
        normalized_end = _normalize_yyyymmdd(end_date, default=date.today().strftime('%Y%m%d'))
        end_dt = datetime.strptime(normalized_end, '%Y%m%d')
        start_dt = end_dt - timedelta(days=lookback_days)
        frame = _get_market_ohlcv_by_date(start_dt.strftime('%Y%m%d'), normalized_end, probe_symbol)
        if frame.empty:
            raise RuntimeError(f'Unable to resolve a recent pykrx trading day on or before {normalized_end}.')
        return frame.index.max().strftime('%Y%m%d')

    def run_backfill(
        self,
        *,
        start_date: str = DEFAULT_START_DATE,
        end_date: str | None = None,
        markets: Sequence[str] = DEFAULT_MARKETS,
        trigger: str = 'manual',
        refresh_universe: bool = True,
        limit: int | None = None,
        symbol: str | None = None,
    ) -> dict:
        normalized_start = _normalize_yyyymmdd(start_date)
        normalized_end = _normalize_yyyymmdd(end_date, default=date.today().strftime('%Y%m%d'))
        return self._run_market_data_pipeline(
            start_date=normalized_start,
            end_date=normalized_end,
            markets=markets,
            trigger=trigger,
            refresh_universe=refresh_universe,
            limit=limit,
            symbol=symbol,
            mode='historical_backfill',
        )

    def run_daily_pipeline_if_due(
        self,
        *,
        market_date: str,
        markets: Sequence[str] = DEFAULT_MARKETS,
        trigger: str = 'auto_close',
        refresh_universe: bool = True,
        force: bool = False,
    ) -> dict:
        normalized_market_date = _normalize_yyyymmdd(market_date)
        market_date_iso = _normalize_iso_date(normalized_market_date)
        latest_trade_date = self.resolve_latest_trade_date(end_date=normalized_market_date)

        if latest_trade_date != market_date_iso:
            result = build_daily_skip_result(
                reason='trade_date_not_available_yet',
                market_date=market_date_iso,
                latest_available_trade_date=latest_trade_date,
            )
            _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
            return result

        with advisory_eod_collection_lock(market='KRX', trade_date=market_date_iso, source='pykrx') as lock_acquired:
            if not lock_acquired:
                result = build_daily_skip_result(
                    reason='already_running',
                    market_date=market_date_iso,
                    latest_available_trade_date=latest_trade_date,
                )
                _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                return result

            latest_run = load_latest_eod_collection_run(
                market='KRX',
                trade_date=market_date_iso,
                source='pykrx',
            )
            if latest_run and not force:
                latest_run = self.reconcile_latest_daily_run_status(
                    trade_date=market_date_iso,
                    latest_run=latest_run,
                )
            if latest_run and latest_run['status'] == 'running' and not force:
                result = build_daily_skip_result(
                    reason='already_running',
                    market_date=market_date_iso,
                    latest_available_trade_date=latest_trade_date,
                    existing_run=latest_run,
                )
                _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                return result
            symbols_override: list[PykrxSymbol] | None = None
            retry_context: dict | None = None
            if not force and latest_run:
                latest_status = latest_run.get('status')
                latest_failed = int(latest_run.get('symbols_failed') or 0)
                if latest_status == 'completed' and latest_failed == 0:
                    result = build_daily_skip_result(
                        reason='already_completed',
                        market_date=market_date_iso,
                        latest_available_trade_date=latest_trade_date,
                        existing_run=latest_run,
                    )
                    _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                    return result
                if latest_status == 'completed_with_errors' and latest_failed == 0:
                    result = build_daily_skip_result(
                        reason='already_completed_with_errors',
                        market_date=market_date_iso,
                        latest_available_trade_date=latest_trade_date,
                        existing_run=latest_run,
                    )
                    _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                    return result
                if _is_auto_daily_trigger(trigger) and self._is_retryable_daily_run(latest_run):
                    automatic_run_count = count_eod_collection_runs(
                        market='KRX',
                        trade_date=market_date_iso,
                        source='pykrx',
                        trigger_prefix='auto_',
                    )
                    if automatic_run_count >= MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE:
                        result = build_daily_skip_result(
                            reason='auto_retry_limit_reached',
                            market_date=market_date_iso,
                            latest_available_trade_date=latest_trade_date,
                            existing_run=latest_run,
                            auto_retry={
                                'attempts': automatic_run_count,
                                'max_attempts': MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE,
                            },
                        )
                        _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                        return result
                    symbols_override = self._load_retry_symbols_for_run(latest_run, markets=markets)
                    if not symbols_override:
                        result = build_daily_skip_result(
                            reason='no_failed_symbols_to_retry',
                            market_date=market_date_iso,
                            latest_available_trade_date=latest_trade_date,
                            existing_run=latest_run,
                            auto_retry={
                                'attempts': automatic_run_count,
                                'max_attempts': MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE,
                            },
                        )
                        _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                        return result
                    retry_context = build_daily_retry_context(
                        latest_run,
                        attempt=automatic_run_count + 1,
                        max_attempts=MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE,
                        symbol_count=len(symbols_override),
                    )

            result = self._run_market_data_pipeline(
                start_date=normalized_market_date,
                end_date=normalized_market_date,
                markets=markets,
                trigger=trigger,
                refresh_universe=False if symbols_override is not None else refresh_universe,
                mode='daily_auto',
                symbols_override=symbols_override,
                retry_context=retry_context,
                selected_stages=PYKRX_STAGE_SEQUENCE,
                chain_fundamentals_followup=True,
            )
            result['triggered'] = True
            _log_pykrx_fetch(
                'run_daily_pipeline_if_due',
                result['status'],
                **{key: value for key, value in result.items() if key != 'status'},
            )
            return result

    def reconcile_latest_daily_run_status(
        self,
        *,
        trade_date: str | None = None,
        latest_run: dict | None = None,
    ) -> dict | None:
        resolved_run = latest_run
        resolved_trade_date = trade_date or (
            str(latest_run.get('trade_date')) if latest_run is not None and latest_run.get('trade_date') else None
        )
        if resolved_trade_date is None:
            raise ValueError('trade_date is required to reconcile pykrx daily run status')

        if resolved_run is None:
            resolved_run = load_latest_eod_collection_run(
                market='KRX',
                trade_date=resolved_trade_date,
                source='pykrx',
            )
        if resolved_run and resolved_run.get('status') == 'running' and self._is_stale_running_run(resolved_run):
            self._fail_stale_running_run(resolved_run)
            resolved_run = load_latest_eod_collection_run(
                market='KRX',
                trade_date=resolved_trade_date,
                source='pykrx',
            )
        return resolved_run

    def run_daily_pipeline_stages(
        self,
        *,
        market_date: str,
        stages: Sequence[str],
        markets: Sequence[str] = DEFAULT_MARKETS,
        trigger: str = 'stage_orchestrator',
        refresh_universe: bool = True,
        force: bool = False,
        symbols: Sequence[str] | None = None,
    ) -> dict:
        requested_stages = [stage for stage in PYKRX_STAGE_SEQUENCE if stage in set(stages)]
        if not requested_stages:
            raise ValueError('At least one pykrx stage must be requested.')
        if 'price_collection' not in requested_stages:
            raise ValueError('price_collection must be included when running grouped pykrx stages.')

        normalized_market_date = _normalize_yyyymmdd(market_date)
        market_date_iso = _normalize_iso_date(normalized_market_date)
        latest_trade_date = self.resolve_latest_trade_date(end_date=normalized_market_date)
        if latest_trade_date != market_date_iso:
            return build_daily_skip_result(
                reason='trade_date_not_available_yet',
                market_date=market_date_iso,
                latest_available_trade_date=latest_trade_date,
            )

        symbols_override = None
        if symbols:
            requested_symbols = {str(item).strip().zfill(6) for item in symbols if str(item).strip()}
            symbols_override = [
                item
                for item in self.load_active_universe(markets=markets)
                if item.symbol in requested_symbols
            ]
            if not symbols_override:
                raise RuntimeError('No active pykrx symbols matched the requested stage scope.')

        with advisory_eod_collection_lock(market='KRX', trade_date=market_date_iso, source='pykrx') as lock_acquired:
            if not lock_acquired:
                result = build_daily_skip_result(
                    reason='already_running',
                    market_date=market_date_iso,
                    latest_available_trade_date=latest_trade_date,
                )
                _log_pykrx_fetch('run_daily_pipeline_stages', 'skipped', **result)
                return result

            result = self._run_market_data_pipeline(
                start_date=normalized_market_date,
                end_date=normalized_market_date,
                markets=markets,
                trigger=trigger,
                refresh_universe=refresh_universe,
                mode='daily_stage_run',
                symbols_override=symbols_override,
                selected_stages=requested_stages,
                chain_fundamentals_followup=False,
            )
            result['triggered'] = True
            result['forced'] = force
            _log_pykrx_fetch(
                'run_daily_pipeline_stages',
                result['status'],
                **{key: value for key, value in result.items() if key != 'status'},
            )
            return result

    def _is_retryable_daily_run(self, run: dict) -> bool:
        status = run.get('status')
        if status not in RETRYABLE_DAILY_STATUSES:
            return False
        if status == 'failed':
            return True
        return int(run.get('symbols_failed') or 0) > 0

    def _load_retry_symbols_for_run(self, run: dict, *, markets: Sequence[str]) -> list[PykrxSymbol]:
        symbol_codes = self._failed_symbol_codes_for_run(run, markets=markets)
        if not symbol_codes:
            return []

        rows = load_symbols(
            is_active=True,
            source='pykrx',
            markets=list(markets),
            symbols=symbol_codes,
        )
        by_symbol = {
            row['symbol']: PykrxSymbol(
                symbol=row['symbol'],
                market=row['market'],
                name_ko=row['name_ko'],
                standard_code=row['standard_code'],
            )
            for row in rows
        }

        items: list[PykrxSymbol] = []
        for symbol_code in symbol_codes:
            item = by_symbol.get(symbol_code)
            if item is None:
                item = self.find_symbol(symbol_code, markets=markets)
            if item is not None:
                items.append(item)
        return items

    def _failed_symbol_codes_for_run(self, run: dict, *, markets: Sequence[str]) -> list[str]:
        metadata = run.get('run_metadata') if isinstance(run.get('run_metadata'), dict) else {}
        metadata_failed_symbols = metadata.get('failed_symbols') if metadata else None
        if metadata_failed_symbols:
            return _dedupe_symbol_codes(metadata_failed_symbols)

        persisted_symbols = set(load_eod_run_persisted_symbols(run['id']))
        requested_symbols = metadata.get('requested_symbols') if metadata else None
        if requested_symbols:
            return [
                symbol
                for symbol in _dedupe_symbol_codes(requested_symbols)
                if symbol not in persisted_symbols
            ]

        active_universe = self.load_active_universe(markets=markets)
        return [item.symbol for item in active_universe if item.symbol not in persisted_symbols]

    def _run_metadata_with_failed_symbols(self, metadata: dict | None, failed_symbols: Sequence[str]) -> dict:
        updated = dict(metadata or {})
        failed_symbol_codes = _dedupe_symbol_codes(failed_symbols)
        updated['failed_symbols'] = failed_symbol_codes
        updated['failed_symbols_count'] = len(failed_symbol_codes)
        return updated

    def _run_activity_at(self, run: dict) -> datetime | None:
        metadata = run.get('run_metadata') if isinstance(run.get('run_metadata'), dict) else {}
        progress_at = metadata.get('last_progress_at')
        activity_at = _parse_activity_datetime(progress_at)
        if activity_at is not None:
            return activity_at
        return _parse_activity_datetime(run.get('started_at'))

    def _is_stale_running_run(self, run: dict) -> bool:
        stale_minutes = settings.eod_auto_pykrx_stale_run_minutes
        activity_at = self._run_activity_at(run)
        if run.get('status') != 'running' or stale_minutes <= 0 or activity_at is None:
            return False
        age = datetime.now(timezone.utc) - activity_at
        return age >= timedelta(minutes=stale_minutes)

    def _mark_stale_running_stage_progress_failed(
        self,
        metadata: dict | None,
        *,
        error_message: str,
    ) -> dict:
        updated = dict(metadata or {})
        stage_progress = dict(updated.get('stage_progress') or {})
        now = _utcnow_iso()
        running_stage: str | None = None

        for stage_name, raw_entry in list(stage_progress.items()):
            if not isinstance(raw_entry, dict):
                continue
            entry = dict(raw_entry)
            if str(entry.get('status') or '') != 'running':
                continue
            running_stage = stage_name
            entry['status'] = 'failed'
            entry['current_step'] = 'stale_run_failed'
            entry['current_activity'] = error_message
            entry['updated_at'] = now
            entry['completed_at'] = now
            stage_progress[stage_name] = entry

        if running_stage is not None:
            updated['stage_progress'] = stage_progress
            updated['stage'] = running_stage
            updated['current_step'] = 'stale_run_failed'
            updated['current_activity'] = error_message
            updated['last_progress_at'] = now
        else:
            current_stage = str(updated.get('stage') or '').strip()
            if current_stage:
                updated = _update_run_stage_metadata(
                    updated,
                    stage=current_stage,
                    status='failed',
                    current_step='stale_run_failed',
                    current_activity=error_message,
                    current_symbol=updated.get('current_symbol'),
                    current_period=updated.get('current_period'),
                )
            else:
                updated['last_progress_at'] = now

        updated['stale_failure_reason'] = error_message
        updated['stale_failed_at'] = now
        return updated

    def _fail_stale_running_run(self, run: dict) -> None:
        persisted_symbols = count_eod_run_persisted_symbols(run['id'])
        symbols_total = int(run.get('symbols_total') or 0)
        symbols_succeeded = max(int(run.get('symbols_succeeded') or 0), persisted_symbols)
        symbols_failed = max(int(run.get('symbols_failed') or 0), max(symbols_total - symbols_succeeded, 0))
        metadata = run.get('run_metadata') if isinstance(run.get('run_metadata'), dict) else {}
        markets = tuple(metadata.get('markets') or DEFAULT_MARKETS)
        failed_symbols = self._failed_symbol_codes_for_run(run, markets=markets)
        if failed_symbols:
            symbols_failed = max(symbols_failed, len(failed_symbols))
        error_message = (
            f"Marked stale after {settings.eod_auto_pykrx_stale_run_minutes} minutes without completion."
        )
        metadata = self._mark_stale_running_stage_progress_failed(metadata, error_message=error_message)
        logger.warning('Marking stale pykrx EOD run %s as failed: %s', run['id'], error_message)
        _log_pykrx_fetch(
            'mark_stale_collection_run',
            'failed',
            run_id=run['id'],
            trade_date=run.get('trade_date'),
            symbols_total=symbols_total,
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            error_message=error_message,
        )
        if failed_symbols:
            metadata = self._run_metadata_with_failed_symbols(metadata, failed_symbols)
        complete_eod_collection_run(
            run['id'],
            status='failed',
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            error_message=error_message,
            run_metadata=metadata,
        )

    def _run_market_data_pipeline(
        self,
        *,
        start_date: str,
        end_date: str,
        markets: Sequence[str],
        trigger: str,
        refresh_universe: bool,
        mode: str,
        limit: int | None = None,
        symbol: str | None = None,
        symbols_override: Sequence[PykrxSymbol] | None = None,
        retry_context: dict | None = None,
        selected_stages: Sequence[str] | None = None,
        chain_fundamentals_followup: bool = True,
    ) -> dict:
        return run_market_data_pipeline(
            self,
            dependencies=PykrxPipelineDependencies(
                log_fetch=_log_pykrx_fetch,
                create_eod_collection_run=create_eod_collection_run,
                update_eod_collection_run_progress=update_eod_collection_run_progress,
                complete_eod_collection_run=complete_eod_collection_run,
                persist_daily_price_bars=persist_daily_price_bars,
                get_market_ohlcv_by_date=_get_market_ohlcv_by_date,
                get_market_ohlcv_by_ticker=_get_market_ohlcv_by_ticker,
                price_context_service=daily_price_context_service,
                technical_snapshot_service=daily_technical_snapshot_service,
                action_summary_service=daily_action_summary_service,
                compute_trending_ranks=compute_and_persist_daily_trending_ranks,
                pykrx_enrichment_service=pykrx_enrichment_service,
                signal_feature_service=daily_signal_feature_service,
                market_risk_filter_service=daily_market_risk_filter_service,
                dart_fundamentals_capture_service=dart_fundamentals_capture_service,
                settings=settings,
                normalize_yyyymmdd=_normalize_yyyymmdd,
                update_run_stage_metadata=_update_run_stage_metadata,
            ),
            start_date=start_date,
            end_date=end_date,
            markets=markets,
            trigger=trigger,
            refresh_universe=refresh_universe,
            mode=mode,
            limit=limit,
            symbol=symbol,
            symbols_override=symbols_override,
            retry_context=retry_context,
            selected_stages=selected_stages,
            chain_fundamentals_followup=chain_fundamentals_followup,
        )

    def _load_symbol_fundamentals(self, *, symbol: str, start_date: str, end_date: str):
        try:
            frame = _get_market_fundamental_by_date(start_date, end_date, symbol)
        except Exception as exc:
            logger.warning('pykrx fundamentals fetch failed for %s between %s and %s: %s', symbol, start_date, end_date, exc)
            _log_pykrx_fetch(
                'fetch_symbol_fundamentals',
                'failed',
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            return None
        if getattr(frame, 'empty', True):
            _log_pykrx_fetch(
                'fetch_symbol_fundamentals',
                'empty',
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )
            return None
        _log_pykrx_fetch(
            'fetch_symbol_fundamentals',
            'completed',
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            row_count=len(frame.index),
        )
        return frame

    @staticmethod
    def _normalize_fundamental_columns(frame):
        if frame is None or getattr(frame, 'empty', True):
            return {}
        normalized: dict[date, dict[str, float | None]] = {}
        for trade_date, row in frame.sort_index().iterrows():
            normalized_trade_date = trade_date.date() if hasattr(trade_date, 'date') else trade_date
            normalized[normalized_trade_date] = {
                'per': PykrxBackfillService._to_optional_float(row.get('PER')),
                'pbr': PykrxBackfillService._to_optional_float(row.get('PBR')),
                'bps': PykrxBackfillService._to_optional_float(row.get('BPS')),
                'eps': PykrxBackfillService._to_optional_float(row.get('EPS')),
                'dividend_yield': PykrxBackfillService._to_optional_float(row.get('DIV')),
                'dps': PykrxBackfillService._to_optional_float(row.get('DPS')),
            }
        return normalized

    @staticmethod
    def _to_optional_float(value) -> float | None:
        if value is None:
            return None
        try:
            if hasattr(value, 'item'):
                value = value.item()
            converted = float(value)
        except (TypeError, ValueError):
            return None
        if converted != converted:
            return None
        return converted

    def _map_symbol_bars(self, frame, *, fundamentals_frame=None) -> list[dict]:
        items: list[dict] = []
        fundamentals_by_date = self._normalize_fundamental_columns(fundamentals_frame)
        for trade_date, row in frame.sort_index().iterrows():
            normalized_trade_date = trade_date.date() if hasattr(trade_date, 'date') else trade_date
            fundamentals = fundamentals_by_date.get(normalized_trade_date, {})
            items.append(
                {
                    'trade_date': normalized_trade_date,
                    'open_price': int(row.get('시가', 0) or 0),
                    'high_price': int(row.get('고가', 0) or 0),
                    'low_price': int(row.get('저가', 0) or 0),
                    'close_price': int(row.get('종가', 0) or 0),
                    'volume': int(row.get('거래량', 0) or 0),
                    'turnover': int(row.get('거래대금', 0) or 0),
                    'per': fundamentals.get('per'),
                    'pbr': fundamentals.get('pbr'),
                    'bps': fundamentals.get('bps'),
                    'eps': fundamentals.get('eps'),
                    'dividend_yield': fundamentals.get('dividend_yield'),
                    'dps': fundamentals.get('dps'),
                }
            )
        return items

    def backfill_daily_bar_fundamentals(
        self,
        *,
        start_date: str,
        end_date: str | None = None,
        markets: Sequence[str] = DEFAULT_MARKETS,
        limit: int | None = None,
        symbol: str | None = None,
    ) -> dict:
        normalized_start = _normalize_yyyymmdd(start_date)
        normalized_end = _normalize_yyyymmdd(end_date, default=date.today().strftime('%Y%m%d'))
        if symbol:
            target_symbols = [symbol]
        else:
            target_symbols = list_symbols_with_daily_bars_in_range(
                start_date=_normalize_iso_date(normalized_start),
                end_date=_normalize_iso_date(normalized_end),
            )
        if limit is not None:
            target_symbols = target_symbols[:limit]

        universe_by_symbol = {item.symbol: item for item in self.load_active_universe(markets=markets, limit=None)}
        updated_symbols = 0
        updated_rows = 0
        failed_symbols: list[str] = []

        for symbol_code in target_symbols:
            item = universe_by_symbol.get(symbol_code) or PykrxSymbol(symbol=symbol_code, market='KRX', name_ko=symbol_code)
            try:
                trade_dates = load_daily_price_bar_dates_for_symbol(
                    item.symbol,
                    start_date=_normalize_iso_date(normalized_start),
                    end_date=_normalize_iso_date(normalized_end),
                )
                if not trade_dates:
                    continue
                fundamentals_frame = self._load_symbol_fundamentals(
                    symbol=item.symbol,
                    start_date=normalized_start,
                    end_date=normalized_end,
                )
                fundamentals_by_date = self._normalize_fundamental_columns(fundamentals_frame)
                matched = {trade_date: fundamentals_by_date.get(trade_date, {}) for trade_date in trade_dates if trade_date in fundamentals_by_date}
                if not matched:
                    continue
                updated_count = update_daily_price_bar_fundamentals(item.symbol, matched, source='pykrx')
                if updated_count > 0:
                    updated_symbols += 1
                    updated_rows += updated_count
            except Exception as exc:
                failed_symbols.append(item.symbol)
                logger.warning('pykrx fundamentals backfill failed for %s: %s', item.symbol, exc)
                _log_pykrx_fetch(
                    'backfill_daily_bar_fundamentals',
                    'failed',
                    symbol=item.symbol,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if self.symbol_pause_seconds:
                time.sleep(self.symbol_pause_seconds)

        result = {
            'source': 'pykrx',
            'start_date': _normalize_iso_date(normalized_start),
            'end_date': _normalize_iso_date(normalized_end),
            'symbol_count': len(target_symbols),
            'updated_symbols': updated_symbols,
            'updated_rows': updated_rows,
            'failed_symbols': failed_symbols,
        }
        _log_pykrx_fetch('backfill_daily_bar_fundamentals', 'completed', **result)
        return result


PYKRX_BACKFILL_SERVICE = PykrxBackfillService()


def _parse_markets(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return DEFAULT_MARKETS
    return tuple(item.strip().upper() for item in raw.split(',') if item.strip())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='pykrx-based EOD universe refresh and historical backfill')
    subparsers = parser.add_subparsers(dest='command', required=True)

    refresh_parser = subparsers.add_parser('refresh-universe', help='Refresh the active pykrx symbol universe')
    refresh_parser.add_argument('--markets', default=','.join(DEFAULT_MARKETS))
    refresh_parser.add_argument('--limit', type=int)

    backfill_parser = subparsers.add_parser('backfill', help='Backfill pykrx historical daily bars')
    backfill_parser.add_argument('--start-date', default=DEFAULT_START_DATE)
    backfill_parser.add_argument('--end-date')
    backfill_parser.add_argument('--markets', default=','.join(DEFAULT_MARKETS))
    backfill_parser.add_argument('--limit', type=int)
    backfill_parser.add_argument('--symbol')
    backfill_parser.add_argument('--trigger', default='manual_cli')
    backfill_parser.add_argument('--skip-universe-refresh', action='store_true')
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    markets = _parse_markets(args.markets)

    if args.command == 'refresh-universe':
        result = PYKRX_BACKFILL_SERVICE.refresh_recent_universe(
            markets=markets,
            limit=args.limit,
            deactivate_missing=args.limit is None,
        )
        print(result)
        return 0

    if args.command == 'backfill':
        result = PYKRX_BACKFILL_SERVICE.run_backfill(
            start_date=args.start_date,
            end_date=args.end_date,
            markets=markets,
            trigger=args.trigger,
            refresh_universe=not args.skip_universe_refresh,
            limit=args.limit,
            symbol=args.symbol,
        )
        print(result)
        return 0

    parser.error(f'Unsupported command: {args.command}')
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
