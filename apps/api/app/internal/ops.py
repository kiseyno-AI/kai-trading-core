from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any, Iterable

from app.core.settings import settings
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.db import init_database
from app.eod.action_summaries import daily_action_summary_service
from app.eod.auto_pykrx import auto_pykrx_eod_scheduler
from app.eod.master import refresh_master_if_due
from app.eod.master_state import master_refresh_state
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.eod.state import job_state
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.market.clock import get_market_state
from app.market_data_store import (
    compute_and_persist_daily_trending_ranks,
    load_tracked_symbols,
    persist_market_status_snapshot,
)
from app.quotes.service import QuoteService
from app.scanner.state import scanner_state


def bootstrap_runtime() -> None:
    init_database()
    scanner_state.hydrate(load_tracked_symbols())


def get_market_status() -> dict[str, Any]:
    state = get_market_state()
    persist_market_status_snapshot(state, source='internal_ops')
    return state.__dict__


async def get_tracked_quotes() -> dict[str, Any]:
    bootstrap_runtime()
    return await QuoteService().get_tracked_quotes()


def get_tracked_symbols() -> dict[str, Any]:
    bootstrap_runtime()
    return scanner_state.snapshot()


def get_eod_status() -> dict[str, Any]:
    bootstrap_runtime()
    return {
        'job': job_state.snapshot(),
        'master': master_refresh_state.snapshot(),
        'pykrx_auto': auto_pykrx_eod_scheduler.snapshot(),
    }


def run_pykrx_daily_if_due(
    *,
    market_date: str | None = None,
    markets: str | None = None,
    force: bool = False,
    refresh_universe: bool | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_markets = tuple(item.strip().upper() for item in (markets or settings.eod_auto_pykrx_markets).split(',') if item.strip())
    return PYKRX_BACKFILL_SERVICE.run_daily_collection_if_due(
        market_date=market_date or get_market_state().server_time[:10],
        markets=parsed_markets or DEFAULT_MARKETS,
        trigger='internal_ops_cli',
        refresh_universe=settings.eod_auto_pykrx_refresh_universe if refresh_universe is None else refresh_universe,
        force=force,
    )


def run_dart_fundamentals_daily_if_due(
    *,
    market_date: str | None = None,
    symbol_limit: int | None = None,
    symbol_offset: int = 0,
    capture_limit: int = 500,
    force: bool = False,
    fs_div: str | None = None,
) -> dict[str, Any]:
    bootstrap_runtime()
    return asyncio.run(
        dart_fundamentals_capture_service.run_daily_incremental_refresh(
            market_date=market_date,
            symbol_limit=symbol_limit,
            symbol_offset=symbol_offset,
            capture_limit=capture_limit,
            force=force,
            fs_div=fs_div,
            trigger='internal_ops_cli',
        )
    )


def refresh_master_if_due_internal(*, force: bool = False) -> dict[str, Any]:
    bootstrap_runtime()
    symbols = refresh_master_if_due(force=force)
    return {
        'refreshed': True,
        'symbol_count': len(symbols),
        'master': master_refresh_state.snapshot(),
    }


def compute_technical_snapshots(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_technical_snapshot_service.compute_and_persist_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source='internal_ops_cli',
    )


def compute_action_summaries(*, market_date: str, symbols: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
    return daily_action_summary_service.compute_and_persist_for_market_date(
        market_date=market_date,
        symbols=parsed_symbols,
        source='internal_ops_cli',
    )


def compute_trending_ranks(*, market_date: str, periods: str | None = None) -> dict[str, Any]:
    bootstrap_runtime()
    parsed_periods = [item.strip().lower() for item in periods.split(',') if item.strip()] if periods else None
    return compute_and_persist_daily_trending_ranks(
        market_date=market_date,
        periods=parsed_periods,
        source='internal_ops_cli',
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Internal kai-trading-core operations without self-HTTP.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    subparsers.add_parser('market-status', help='Return /market/status directly from services')
    subparsers.add_parser('quotes-tracked', help='Return /quotes/tracked directly from services')
    subparsers.add_parser('scanner-tracked-symbols', help='Return /scanner/tracked-symbols directly from services')
    subparsers.add_parser('eod-status', help='Return /eod/status directly from services')

    refresh_master_parser = subparsers.add_parser('eod-master-refresh-if-due', help='Run master refresh directly')
    refresh_master_parser.add_argument('--force', action='store_true')

    pykrx_daily_parser = subparsers.add_parser('eod-pykrx-daily-run-if-due', help='Run pykrx daily EOD directly')
    pykrx_daily_parser.add_argument('--market-date')
    pykrx_daily_parser.add_argument('--markets')
    pykrx_daily_parser.add_argument('--force', action='store_true')
    pykrx_daily_parser.add_argument('--refresh-universe', choices=('true', 'false'))

    dart_daily_parser = subparsers.add_parser(
        'dart-fundamentals-daily-run-if-due',
        help='Run DART fundamentals daily incremental refresh directly',
    )
    dart_daily_parser.add_argument('--market-date')
    dart_daily_parser.add_argument('--symbol-limit', type=int)
    dart_daily_parser.add_argument('--symbol-offset', type=int, default=0)
    dart_daily_parser.add_argument('--capture-limit', type=int, default=500)
    dart_daily_parser.add_argument('--force', action='store_true')
    dart_daily_parser.add_argument('--fs-div')

    technical_parser = subparsers.add_parser('eod-technical-snapshots-compute', help='Compute technical snapshots directly')
    technical_parser.add_argument('--market-date', required=True)
    technical_parser.add_argument('--symbols')

    action_parser = subparsers.add_parser('eod-action-summaries-compute', help='Compute action summaries directly')
    action_parser.add_argument('--market-date', required=True)
    action_parser.add_argument('--symbols')

    trending_parser = subparsers.add_parser(
        'eod-trending-ranks-compute',
        help='Compute stored day-based trending ranks directly',
    )
    trending_parser.add_argument('--market-date', required=True)
    trending_parser.add_argument('--periods')
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == 'market-status':
        payload = get_market_status()
    elif args.command == 'quotes-tracked':
        payload = asyncio.run(get_tracked_quotes())
    elif args.command == 'scanner-tracked-symbols':
        payload = get_tracked_symbols()
    elif args.command == 'eod-status':
        payload = get_eod_status()
    elif args.command == 'eod-master-refresh-if-due':
        payload = refresh_master_if_due_internal(force=args.force)
    elif args.command == 'eod-pykrx-daily-run-if-due':
        payload = run_pykrx_daily_if_due(
            market_date=args.market_date,
            markets=args.markets,
            force=args.force,
            refresh_universe=None if args.refresh_universe is None else args.refresh_universe == 'true',
        )
    elif args.command == 'dart-fundamentals-daily-run-if-due':
        payload = run_dart_fundamentals_daily_if_due(
            market_date=args.market_date,
            symbol_limit=args.symbol_limit,
            symbol_offset=args.symbol_offset,
            capture_limit=args.capture_limit,
            force=args.force,
            fs_div=args.fs_div,
        )
    elif args.command == 'eod-technical-snapshots-compute':
        payload = compute_technical_snapshots(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-action-summaries-compute':
        payload = compute_action_summaries(market_date=args.market_date, symbols=args.symbols)
    elif args.command == 'eod-trending-ranks-compute':
        payload = compute_trending_ranks(market_date=args.market_date, periods=args.periods)
    else:
        parser.error(f'Unsupported command: {args.command}')

    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
