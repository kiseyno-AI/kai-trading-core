from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, Sequence


def build_daily_skip_result(
    *,
    reason: str,
    market_date: str,
    latest_available_trade_date: str,
    existing_run: Mapping[str, Any] | None = None,
    auto_retry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        'triggered': False,
        'reason': reason,
        'market_date': market_date,
        'latest_available_trade_date': latest_available_trade_date,
        'source': 'pykrx',
    }
    if existing_run is not None:
        result['existing_run'] = existing_run
    if auto_retry is not None:
        result['auto_retry'] = auto_retry
    return result


def build_daily_retry_context(
    run: Mapping[str, Any],
    *,
    attempt: int,
    max_attempts: int,
    symbol_count: int,
) -> dict[str, Any]:
    context = {
        'retry_of_run_id': run.get('id'),
        'previous_status': run.get('status'),
        'previous_symbols_failed': int(run.get('symbols_failed') or 0),
        'attempt': int(attempt),
        'max_attempts': int(max_attempts),
        'symbol_count': int(symbol_count),
    }
    metadata = (
        run.get('run_metadata')
        if isinstance(run.get('run_metadata'), Mapping)
        else {}
    )
    price_retry_symbols = {
        str(symbol).strip()
        for key in ('failed_symbols', 'unattempted_symbols')
        for symbol in (metadata.get(key) or [])
        if str(symbol).strip()
    }
    inherited_downstream_symbols = list(
        dict.fromkeys(
            str(symbol).strip()
            for symbol in (
                [
                    *(metadata.get('downstream_retry_symbols') or []),
                    *(
                        metadata.get(
                            'inherited_downstream_retry_symbols'
                        )
                        or []
                    ),
                ]
            )
            if str(symbol).strip()
            and str(symbol).strip() not in price_retry_symbols
        )
    )
    if inherited_downstream_symbols:
        context['inherited_downstream_retry_symbols'] = (
            inherited_downstream_symbols
        )
    universe_snapshot = metadata.get('universe_snapshot')
    if isinstance(universe_snapshot, Mapping):
        # A retry may contain only the failed price subset. Keep the original
        # exact-date membership proof attached to every descendant so recovery
        # after midnight does not depend on a fresh provider response.
        context['inherited_universe_snapshot'] = dict(universe_snapshot)
    return context


def make_stage_progress_callback(
    persist_stage_progress: Callable[..., None],
    *,
    stage: str,
    default_step: str,
    default_activity: str,
    include_current_symbol: bool = False,
    include_current_period: bool = False,
) -> Callable[[Mapping[str, Any]], None]:
    def _callback(progress: Mapping[str, Any]) -> None:
        details = progress.get('details') if isinstance(progress.get('details'), dict) else None
        persist_stage_progress(
            stage=stage,
            status=str(progress.get('status') or 'running'),
            current_step=str(progress.get('current_step') or default_step),
            current_activity=str(progress.get('current_activity') or default_activity),
            processed=progress.get('processed'),
            total=progress.get('total'),
            succeeded=progress.get('succeeded'),
            failed=progress.get('failed'),
            current_symbol=progress.get('current_symbol') if include_current_symbol else None,
            current_period=progress.get('current_period') if include_current_period else None,
            extra=details,
        )

    return _callback


def build_stage_failure_result(
    *,
    market_date: str,
    source: str,
    error: Exception | str,
    totals: Mapping[str, Any] | None = None,
    failed_symbols: Sequence[str] | None = None,
    items_key: str | None = None,
    items: Sequence[Any] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        'market_date': market_date,
        'source': source,
        'error': str(error),
    }
    if totals:
        result.update(totals)
    if failed_symbols is not None:
        result['failed_symbols'] = list(failed_symbols)[:50]
    if items_key is not None:
        result[items_key] = list(items or [])
    if extra:
        result.update(extra)
    return result


def append_stage_error(
    errors: list[str],
    *,
    direct_error: str | None = None,
    result: Mapping[str, Any] | None = None,
    failed_key: str | None = None,
    incomplete_prefix: str | None = None,
    failed_symbols_key: str = 'failed_symbols',
    exclude_symbols: Iterable[str] | None = None,
    total_keys: Sequence[str] = (),
    max_non_blocking_missing_ratio: float | None = None,
) -> None:
    if direct_error:
        errors.append(direct_error)
        return
    if result is None or failed_key is None or incomplete_prefix is None:
        return

    failed_count = int(result.get(failed_key) or 0)
    if failed_count <= 0:
        return

    if max_non_blocking_missing_ratio is not None:
        total_count = _resolve_total_count(result=result, total_keys=total_keys)
        if total_count > 0 and failed_count / total_count < max_non_blocking_missing_ratio:
            return

    failed_symbols = [
        str(symbol)
        for symbol in (result.get(failed_symbols_key) or [])
        if str(symbol).strip()
    ]
    if exclude_symbols is not None:
        excluded = {str(symbol) for symbol in exclude_symbols}
        failed_symbols = [symbol for symbol in failed_symbols if symbol not in excluded]
        if not failed_symbols:
            return

    errors.append(incomplete_prefix + ','.join(failed_symbols[:20]))


def _resolve_total_count(*, result: Mapping[str, Any], total_keys: Sequence[str]) -> int:
    for key in total_keys:
        value = result.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            parsed = len(value)
            if parsed > 0:
                return parsed
            continue
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    return 0
