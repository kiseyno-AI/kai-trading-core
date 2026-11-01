from __future__ import annotations

import argparse
import asyncio
from typing import Any, Callable, Iterable, Mapping, Sequence

from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from app.market_data_store import (
    load_daily_market_risk_filter_results,
    load_latest_available_daily_market_risk_filter_result_date,
    replace_daily_market_risk_filter_results_exact_scope,
    upsert_daily_market_risk_filter_result,
    upsert_daily_market_risk_filter_results,
)
from app.repositories.eod_run_repository import daily_price_bar_writer_admission
from app.signals.factor_coverage import (
    build_symbol_results,
    merge_symbol_failures,
    persist_symbol_items_with_isolation,
    run_symbol_computation,
)
from app.signals.market_risk_evaluators import (
    MARKET_RISK_FILTER_SOURCE,
    build_market_risk_filter_result_from_snapshot,
)
from app.signals.market_risk_snapshot import (
    DEFAULT_FETCH_CONCURRENCY,
    DEFAULT_MARKET_CODE,
    LATEST_FALLBACK_POLICY,
    DailyMarketRiskSnapshotService,
    daily_market_risk_snapshot_service,
    normalize_market_date,
    normalize_optional_market_date,
    normalize_symbol,
    normalize_symbols,
)


MARKET_RISK_FILTER_SCOPE_MANIFEST_VERSION = (
    'market_risk_filter_scope_outcomes_v1'
)


def _snapshot_collection_summary(snapshot_collection: dict[str, Any]) -> dict[str, Any]:
    return {
        'market_date': snapshot_collection.get('market_date'),
        'source': snapshot_collection.get('source'),
        'feature_version': snapshot_collection.get('feature_version'),
        'summary': snapshot_collection.get('summary') or {},
        'requested_symbols': list(snapshot_collection.get('requested_symbols') or []),
        'matched_symbols': list(snapshot_collection.get('matched_symbols') or []),
        'missing_symbols': list(snapshot_collection.get('missing_symbols') or []),
        'snapshots_persisted': int(snapshot_collection.get('snapshots_persisted') or 0),
        'snapshots_failed': int(snapshot_collection.get('snapshots_failed') or 0),
        'failed_symbols': list(snapshot_collection.get('failed_symbols') or []),
        'failed_symbol_errors': list(snapshot_collection.get('failed_symbol_errors') or []),
    }


def _public_filter_item(item: dict[str, Any]) -> dict[str, Any]:
    public_item = dict(item)
    public_item.pop('_snapshot_fields', None)
    return public_item


class DailyMarketRiskFilterService:
    def __init__(self, *, snapshot_service: DailyMarketRiskSnapshotService | None = None) -> None:
        self.snapshot_service = snapshot_service or daily_market_risk_snapshot_service

    @daily_price_bar_writer_admission
    async def rebuild_for_market_date(
        self,
        *,
        market_date: str,
        symbols: Sequence[str] | None = None,
        source: str = MARKET_RISK_FILTER_SOURCE,
        market_code: str = DEFAULT_MARKET_CODE,
        run_id: int | None = None,
        fetch_concurrency: int | None = None,
        progress_callback: Callable[..., Any] | None = None,
        exact_scope_symbols: Sequence[str] | None = None,
        authoritative_market_scope: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = normalize_market_date(market_date)
        normalized_symbols = normalize_symbols(symbols)
        snapshot_kwargs: dict[str, Any] = {
            'market_date': normalized_market_date,
            'symbols': normalized_symbols,
            'market_code': market_code,
            'run_id': run_id,
            'fetch_concurrency': fetch_concurrency,
            'progress_callback': progress_callback,
        }
        if exact_scope_symbols is not None:
            snapshot_kwargs.update(
                exact_scope_symbols=exact_scope_symbols,
                authoritative_market_scope=authoritative_market_scope,
            )
        snapshot_collection = (
            await self.snapshot_service.collect_for_market_date(
                **snapshot_kwargs
            )
        )
        if exact_scope_symbols is not None and not (
            isinstance(snapshot_collection.get('scope_finalization'), Mapping)
            and snapshot_collection['scope_finalization'].get('ready') is True
        ):
            raise RuntimeError(
                'market-risk snapshot atomic exact-scope finalization proof '
                'is unavailable'
            )
        snapshot_items = list(snapshot_collection.get('items') or [])
        requested_symbols = list(snapshot_collection.get('requested_symbols') or [])
        matched_symbols = list(snapshot_collection.get('matched_symbols') or [])
        missing_symbols = list(snapshot_collection.get('missing_symbols') or [])

        (
            items,
            computation_failed_symbols,
            computation_failed_symbol_errors,
        ) = build_symbol_results(
            snapshot_items,
            symbol_of=lambda snapshot: str(snapshot.get('symbol') or ''),
            build=lambda snapshot: run_symbol_computation(
                lambda: build_market_risk_filter_result_from_snapshot(
                    snapshot,
                    source=source,
                    eod_run_id=run_id,
                )
            ),
        )
        items.sort(
            key=lambda item: (
                item['passed_filter'],
                not item['watchlist_flagged'],
                item['symbol'],
            )
        )

        persisted_items: list[dict[str, Any]] = []
        failed_symbols, failed_symbol_errors = merge_symbol_failures(
            (
                list(snapshot_collection.get('failed_symbols') or []),
                list(snapshot_collection.get('failed_symbol_errors') or []),
            ),
            (computation_failed_symbols, computation_failed_symbol_errors),
        )
        if progress_callback is not None:
            progress_callback(
                status='running',
                current_step='persisting_market_risk_filter_results',
                current_activity='Persisting daily KIS market-risk filter results',
                processed=0,
                total=len(items),
                succeeded=0,
                failed=len(failed_symbols),
                current_symbol=(items[0].get('symbol') if items else None),
            )
        scope_finalization: dict[str, Any] | None = None
        if exact_scope_symbols is not None:
            publication = (
                replace_daily_market_risk_filter_results_exact_scope(
                    market_date=normalized_market_date,
                    results=items,
                    exact_symbols=exact_scope_symbols,
                    authoritative_market_scope=authoritative_market_scope,
                )
            )
            successful_symbol_set = set(publication['successful_symbols'])
            persisted_items = [
                _public_filter_item(item)
                for item in items
                if item['symbol'] in successful_symbol_set
            ]
            failed_symbols, failed_symbol_errors = merge_symbol_failures(
                (failed_symbols, failed_symbol_errors),
                (
                    publication['failed_symbols'],
                    publication['failed_symbol_errors'],
                ),
            )
            scope_finalization = publication['scope_finalization']
        elif items:
            (
                written_items,
                write_failed_symbols,
                write_failed_symbol_errors,
            ) = persist_symbol_items_with_isolation(
                items,
                bulk_write=upsert_daily_market_risk_filter_results,
                item_write=upsert_daily_market_risk_filter_result,
            )
            persisted_items = [
                _public_filter_item(item) for item in written_items
            ]
            failed_symbols, failed_symbol_errors = merge_symbol_failures(
                (failed_symbols, failed_symbol_errors),
                (write_failed_symbols, write_failed_symbol_errors),
            )
        if progress_callback is not None:
            progress_callback(
                status='running',
                current_step='persisting_market_risk_filter_results',
                current_activity='Persisted daily KIS market-risk filter result batch',
                processed=len(items),
                total=len(items),
                succeeded=len(persisted_items),
                failed=len(failed_symbols),
                current_symbol=None,
            )

        selected_symbols = (
            normalize_symbols(exact_scope_symbols)
            if exact_scope_symbols is not None
            else list(requested_symbols or matched_symbols)
        )
        successful_symbols = [item['symbol'] for item in persisted_items]
        successful_symbol_set = set(successful_symbols)
        retry_debt_symbols = [
            symbol
            for symbol in selected_symbols
            if symbol not in successful_symbol_set
        ]
        failed_symbols, failed_symbol_errors = merge_symbol_failures(
            (failed_symbols, failed_symbol_errors),
            (
                retry_debt_symbols,
                [
                    {'symbol': symbol, 'error': 'result_not_persisted'}
                    for symbol in retry_debt_symbols
                ],
            ),
        )

        result = {
            'market_date': normalized_market_date.isoformat(),
            'source': source,
            'feature_version': source,
            'requested_symbols': requested_symbols,
            'matched_symbols': matched_symbols,
            'missing_symbols': missing_symbols,
            'snapshot_collection': _snapshot_collection_summary(snapshot_collection),
            'summary': {
                'matched_symbol_count': len(matched_symbols),
                'evaluated_count': len(items),
                'passed_count': sum(1 for item in items if item.get('passed_filter')),
                'watchlist_count': sum(1 for item in items if item.get('watchlist_flagged')),
                'screening_rejected_count': sum(
                    1 for item in items if not item.get('passed_filter')
                ),
            },
            'results_persisted': len(persisted_items),
            'results_failed': len(failed_symbols),
            'failed_symbols': failed_symbols,
            'failed_symbol_errors': failed_symbol_errors,
            'items': persisted_items,
            **build_exact_symbol_scope_outcome(
                manifest_version=(
                    MARKET_RISK_FILTER_SCOPE_MANIFEST_VERSION
                ),
                selected_symbols=selected_symbols,
                successful_symbols=successful_symbols,
                retry_debt_symbols=retry_debt_symbols,
            ),
        }
        if scope_finalization is not None:
            result['scope_finalization'] = scope_finalization
        return result

    def inspect(
        self,
        *,
        market_date: str | None = None,
        symbol: str | None = None,
        limit: int | None = None,
        summary_only: bool = False,
    ) -> dict[str, Any]:
        normalized_requested_market_date = normalize_optional_market_date(market_date)
        normalized_symbol = normalize_symbol(symbol) if symbol is not None else None
        requested_symbols = [normalized_symbol] if normalized_symbol else []
        resolved_market_date = load_latest_available_daily_market_risk_filter_result_date(
            market_date=normalized_requested_market_date,
            symbols=requested_symbols or None,
        )
        items = (
            load_daily_market_risk_filter_results(
                market_date=resolved_market_date,
                symbols=requested_symbols or None,
                limit=(1 if normalized_symbol else limit),
            )
            if resolved_market_date is not None
            else []
        )
        matched_symbols = [item['symbol'] for item in items]
        matched_symbol_set = set(matched_symbols)
        response_matched_symbols = [] if summary_only and not normalized_symbol else matched_symbols
        requested_market_date = (
            normalized_requested_market_date.isoformat() if normalized_requested_market_date is not None else None
        )
        is_latest_fallback = (
            requested_market_date is not None
            and resolved_market_date is not None
            and resolved_market_date != requested_market_date
        )
        summary = {
            'entries': len(items),
            'requested_symbol': normalized_symbol,
            'matched_symbols': response_matched_symbols,
            'missing_symbols': [value for value in requested_symbols if value not in matched_symbol_set],
            'passed_count': sum(1 for item in items if item.get('passed_filter')),
            'watchlist_count': sum(1 for item in items if item.get('watchlist_flagged')),
            'screening_rejected_count': sum(
                1 for item in items if not item.get('passed_filter')
            ),
        }
        payload: dict[str, Any] = {
            'requested_market_date': requested_market_date,
            'resolved_market_date': resolved_market_date,
            'market_date': resolved_market_date,
            'latest_mode': normalized_requested_market_date is None,
            'is_latest_fallback': is_latest_fallback,
            'fallback_policy': LATEST_FALLBACK_POLICY,
            'feature_version': (items[0].get('feature_version') if items else MARKET_RISK_FILTER_SOURCE),
            'summary': summary,
        }
        if normalized_symbol:
            payload['item'] = items[0] if items else None
        elif summary_only:
            payload['items'] = []
        else:
            payload['items'] = items
        return payload


daily_market_risk_filter_service = DailyMarketRiskFilterService()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Compute daily KIS market risk filter rows from status snapshots')
    parser.add_argument('--market-date', required=True, help='Market date in YYYY-MM-DD format')
    parser.add_argument('--symbols')
    parser.add_argument('--source', default=MARKET_RISK_FILTER_SOURCE)
    parser.add_argument('--market-code', default=DEFAULT_MARKET_CODE)
    parser.add_argument('--fetch-concurrency', type=int, default=DEFAULT_FETCH_CONCURRENCY)
    return parser


async def _run_from_args(args: argparse.Namespace) -> dict[str, Any]:
    parsed_symbols = [item.strip() for item in str(args.symbols or '').split(',') if item.strip()] or None
    return await daily_market_risk_filter_service.rebuild_for_market_date(
        market_date=args.market_date,
        symbols=parsed_symbols,
        source=args.source,
        market_code=args.market_code,
        fetch_concurrency=args.fetch_concurrency,
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = asyncio.run(_run_from_args(args))
    print(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
