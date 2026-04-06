from __future__ import annotations

import argparse
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Sequence

from pykrx import stock
from pykrx.website.comm import webio

from app.core.settings import settings
from app.eod.master import get_master_symbols
from app.market_data_store import (
    advisory_eod_collection_lock,
    count_eod_run_persisted_symbols,
    complete_eod_collection_run,
    create_eod_collection_run,
    load_symbols,
    load_latest_eod_collection_run,
    persist_daily_price_bars,
    update_eod_collection_run_progress,
    upsert_symbol_universe,
)

logger = logging.getLogger(__name__)

DEFAULT_START_DATE = '19560101'
DEFAULT_MARKETS = ('KOSPI', 'KOSDAQ', 'KONEX')


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


def _get_market_ohlcv_by_date(start_date: str, end_date: str, symbol: str):
    getter = getattr(stock, 'get_market_ohlcv_by_date', None) or getattr(stock, 'get_market_ohlcv')
    return _call_pykrx(getter, start_date, end_date, symbol)


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
        snapshot = self.fetch_recent_universe_snapshot(markets=markets, limit=limit)
        symbols = snapshot['symbols']
        persisted = self._persist_universe(symbols, deactivate_missing=deactivate_missing)
        return {
            'source': 'pykrx',
            'markets': list(markets),
            'symbol_count': len(symbols),
            'persisted_count': persisted,
            'latest_trade_date': snapshot['latest_trade_date'],
            'universe_source': snapshot['universe_source'],
            'deactivate_missing': deactivate_missing,
        }

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
        items = self._fetch_pykrx_market_universe(as_of_date=as_of_date, markets=markets, limit=limit)
        universe_source = 'pykrx_market_ohlcv'
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
    ) -> list[PykrxSymbol]:
        items: list[PykrxSymbol] = []
        for market in markets:
            try:
                frame = _call_pykrx(stock.get_market_ohlcv_by_ticker, as_of_date, market=market)
            except Exception as exc:
                logger.warning('pykrx market universe fetch failed for %s on %s: %s', market, as_of_date, exc)
                return []
            if frame.empty or '시가' not in frame.columns:
                logger.warning('pykrx market universe returned no usable rows for %s on %s', market, as_of_date)
                return []
            tickers = [str(ticker).zfill(6) for ticker in frame.index.tolist()]
            for ticker in tickers:
                if not ticker.isdigit() or len(ticker) != 6:
                    continue
                try:
                    name_ko = _call_pykrx(stock.get_market_ticker_name, ticker)
                except Exception:
                    logger.warning('Falling back to ticker as name for %s', ticker)
                    name_ko = ticker
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
                    return items
        return items

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
                return items
        return items

    def load_active_universe(
        self,
        *,
        markets: Sequence[str] = DEFAULT_MARKETS,
        symbol: str | None = None,
        limit: int | None = None,
    ) -> list[PykrxSymbol]:
        rows = load_symbols(is_active=True, source='pykrx', markets=list(markets), limit=limit)
        items = [
            PykrxSymbol(
                symbol=row['symbol'],
                market=row['market'],
                name_ko=row['name_ko'],
                standard_code=row['standard_code'],
            )
            for row in rows
        ]
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
        return self._run_collection(
            start_date=normalized_start,
            end_date=normalized_end,
            markets=markets,
            trigger=trigger,
            refresh_universe=refresh_universe,
            limit=limit,
            symbol=symbol,
            mode='historical_backfill',
        )

    def run_daily_collection_if_due(
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
            return {
                'triggered': False,
                'reason': 'trade_date_not_available_yet',
                'market_date': market_date_iso,
                'latest_available_trade_date': latest_trade_date,
                'source': 'pykrx',
            }

        with advisory_eod_collection_lock(market='KRX', trade_date=market_date_iso, source='pykrx') as lock_acquired:
            if not lock_acquired:
                return {
                    'triggered': False,
                    'reason': 'already_running',
                    'market_date': market_date_iso,
                    'latest_available_trade_date': latest_trade_date,
                    'source': 'pykrx',
                }

            latest_run = load_latest_eod_collection_run(
                market='KRX',
                trade_date=market_date_iso,
                source='pykrx',
            )
            if latest_run and latest_run['status'] == 'running' and not force:
                if self._is_stale_running_run(latest_run):
                    self._fail_stale_running_run(latest_run)
                else:
                    return {
                        'triggered': False,
                        'reason': 'already_running',
                        'market_date': market_date_iso,
                        'latest_available_trade_date': latest_trade_date,
                        'source': 'pykrx',
                        'existing_run': latest_run,
                    }
                latest_run = load_latest_eod_collection_run(
                    market='KRX',
                    trade_date=market_date_iso,
                    source='pykrx',
                )
            if latest_run and latest_run['status'] == 'running' and not force:
                return {
                    'triggered': False,
                    'reason': 'already_running',
                    'market_date': market_date_iso,
                    'latest_available_trade_date': latest_trade_date,
                    'source': 'pykrx',
                    'existing_run': latest_run,
                }
            if (
                not force
                and latest_run
                and latest_run['status'] == 'completed'
                and latest_run['symbols_failed'] == 0
            ):
                return {
                    'triggered': False,
                    'reason': 'already_completed',
                    'market_date': market_date_iso,
                    'latest_available_trade_date': latest_trade_date,
                    'source': 'pykrx',
                    'existing_run': latest_run,
                }

            result = self._run_collection(
                start_date=normalized_market_date,
                end_date=normalized_market_date,
                markets=markets,
                trigger=trigger,
                refresh_universe=refresh_universe,
                mode='daily_auto',
            )
            result['triggered'] = True
            return result

    def _is_stale_running_run(self, run: dict) -> bool:
        stale_minutes = settings.eod_auto_pykrx_stale_run_minutes
        started_at = run.get('started_at')
        if run.get('status') != 'running' or stale_minutes <= 0 or not started_at:
            return False
        started = datetime.fromisoformat(started_at)
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - started.astimezone(timezone.utc)
        return age >= timedelta(minutes=stale_minutes)

    def _fail_stale_running_run(self, run: dict) -> None:
        persisted_symbols = count_eod_run_persisted_symbols(run['id'])
        symbols_total = int(run.get('symbols_total') or 0)
        symbols_succeeded = max(int(run.get('symbols_succeeded') or 0), persisted_symbols)
        symbols_failed = max(int(run.get('symbols_failed') or 0), max(symbols_total - symbols_succeeded, 0))
        error_message = (
            f"Marked stale after {settings.eod_auto_pykrx_stale_run_minutes} minutes without completion."
        )
        logger.warning('Marking stale pykrx EOD run %s as failed: %s', run['id'], error_message)
        complete_eod_collection_run(
            run['id'],
            status='failed',
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            error_message=error_message,
        )

    def _run_collection(
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
    ) -> dict:
        normalized_start = _normalize_yyyymmdd(start_date)
        normalized_end = _normalize_yyyymmdd(end_date)

        if refresh_universe:
            snapshot = self.fetch_recent_universe_snapshot(markets=markets, limit=limit)
            symbols = snapshot['symbols']
            deactivate_missing = limit is None and symbol is None
            persisted = self._persist_universe(symbols, deactivate_missing=deactivate_missing)
            universe_snapshot = {
                'source': 'pykrx',
                'markets': list(markets),
                'symbol_count': len(symbols),
                'persisted_count': persisted,
                'latest_trade_date': snapshot['latest_trade_date'],
                'universe_source': snapshot['universe_source'],
                'deactivate_missing': deactivate_missing,
            }
        else:
            universe_snapshot = None
            symbols = self.load_active_universe(markets=markets, symbol=symbol, limit=limit)

        if symbol:
            symbols = [item for item in symbols if item.symbol == symbol]
            if not symbols:
                matched = self.find_symbol(symbol, markets=markets)
                if matched is not None:
                    symbols = [matched]
        if not symbols:
            raise RuntimeError('No active pykrx symbols are available for backfill.')

        latest_trade_date = self.resolve_latest_trade_date(end_date=normalized_end)
        run_metadata = {
            'mode': mode,
            'requested_start_date': normalized_start,
            'requested_end_date': normalized_end,
            'latest_available_trade_date': latest_trade_date,
            'markets': list(markets),
            'symbol': symbol,
            'limited_run': limit is not None,
            'universe_refreshed': refresh_universe,
            'universe_snapshot': universe_snapshot,
        }
        run_id = create_eod_collection_run(
            market='KRX',
            trade_date=latest_trade_date,
            symbols_total=len(symbols),
            trigger=trigger,
            source='pykrx',
            run_metadata=run_metadata,
        )

        symbols_succeeded = 0
        symbols_failed = 0
        failed_symbols: list[str] = []
        bars_upserted = 0

        try:
            for item in symbols:
                try:
                    frame = _get_market_ohlcv_by_date(normalized_start, normalized_end, item.symbol)
                    if frame.empty:
                        raise RuntimeError('No OHLCV rows returned.')
                    bars = self._map_symbol_bars(frame)
                    bars_upserted += persist_daily_price_bars(
                        item.symbol,
                        bars,
                        market=item.market,
                        source='pykrx',
                        run_id=run_id,
                        name_ko=item.name_ko,
                        standard_code=item.standard_code,
                    )
                    symbols_succeeded += 1
                except Exception as exc:
                    symbols_failed += 1
                    failed_symbols.append(item.symbol)
                    logger.warning('pykrx backfill failed for %s: %s', item.symbol, exc)
                update_eod_collection_run_progress(
                    run_id,
                    symbols_succeeded=symbols_succeeded,
                    symbols_failed=symbols_failed,
                )
                if self.symbol_pause_seconds:
                    time.sleep(self.symbol_pause_seconds)
        except Exception as exc:
            complete_eod_collection_run(
                run_id,
                status='failed',
                symbols_succeeded=symbols_succeeded,
                symbols_failed=symbols_failed,
                error_message=str(exc),
            )
            raise

        status = 'completed' if symbols_failed == 0 else 'completed_with_errors'
        complete_eod_collection_run(
            run_id,
            status=status,
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            error_message=None if not failed_symbols else f'Failed symbols: {",".join(failed_symbols[:20])}',
        )
        return {
            'run_id': run_id,
            'source': 'pykrx',
            'status': status,
            'markets': list(markets),
            'start_date': normalized_start,
            'requested_end_date': normalized_end,
            'latest_available_trade_date': latest_trade_date,
            'symbols_total': len(symbols),
            'symbols_succeeded': symbols_succeeded,
            'symbols_failed': symbols_failed,
            'failed_symbols': failed_symbols[:50],
            'bars_upserted': bars_upserted,
            'refresh_universe': refresh_universe,
            'universe_snapshot': universe_snapshot,
        }

    def _map_symbol_bars(self, frame) -> list[dict]:
        items: list[dict] = []
        for trade_date, row in frame.sort_index().iterrows():
            normalized_trade_date = trade_date.date() if hasattr(trade_date, 'date') else trade_date
            items.append(
                {
                    'trade_date': normalized_trade_date,
                    'open_price': int(row.get('시가', 0) or 0),
                    'high_price': int(row.get('고가', 0) or 0),
                    'low_price': int(row.get('저가', 0) or 0),
                    'close_price': int(row.get('종가', 0) or 0),
                    'volume': int(row.get('거래량', 0) or 0),
                    'turnover': int(row.get('거래대금', 0) or 0),
                }
            )
        return items


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
