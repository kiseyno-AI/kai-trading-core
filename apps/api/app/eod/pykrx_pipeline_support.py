from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable, Iterable, Mapping, Sequence


_STATUS_RESULT_MAX_DEPTH = 3
_STATUS_RESULT_MAX_MAPPING_ITEMS = 40
_STATUS_RESULT_MAX_SEQUENCE_ITEMS = 10
_STATUS_RESULT_MAX_STRING_LENGTH = 512
_EOD_RUN_SUMMARY_KEYS = (
    'id',
    'market',
    'trade_date',
    'source',
    'trigger',
    'status',
    'symbols_total',
    'symbols_succeeded',
    'symbols_failed',
    'error_message',
    'started_at',
    'completed_at',
    'updated_at',
)
_RUN_METADATA_SUMMARY_KEYS = (
    'daily_pipeline_run_id',
    'pipeline_run_id',
    'current_stage',
    'current_step',
    'current_activity',
    'mode',
    'point_in_time_recovery',
    'updated_at',
    'completed_at',
)
_RUN_METADATA_SYMBOL_LIST_KEYS = (
    'failed_symbols',
    'unattempted_symbols',
    'price_retry_symbols',
    'downstream_retry_symbols',
    'inherited_downstream_retry_symbols',
)


def _compact_status_string(value: str) -> str:
    if len(value) <= _STATUS_RESULT_MAX_STRING_LENGTH:
        return value
    return f'{value[: _STATUS_RESULT_MAX_STRING_LENGTH - 3]}...'


def _compact_status_value(value: Any, *, depth: int = 0) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _compact_status_string(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        if (
            set(value) == {'field_count'}
            and type(value.get('field_count')) is int
            and value['field_count'] >= 0
        ):
            return {'field_count': value['field_count']}
        if (
            set(value) == {'item_count', 'sample'}
            and type(value.get('item_count')) is int
            and value['item_count'] >= 0
            and isinstance(value.get('sample'), list)
            and value['item_count'] > len(value['sample'])
        ):
            return {
                'item_count': value['item_count'],
                'sample': [
                    _compact_status_value(item, depth=depth + 1)
                    for item in value['sample']
                ],
            }
        if depth >= _STATUS_RESULT_MAX_DEPTH:
            return {'field_count': len(value)}
        compacted: dict[str, Any] = {}
        existing_omitted = value.get('_omitted_field_count')
        omitted = (
            existing_omitted
            if type(existing_omitted) is int and existing_omitted >= 0
            else 0
        )
        for item_key, item_value in value.items():
            normalized_key = str(item_key)
            if normalized_key == '_omitted_field_count':
                continue
            if normalized_key == 'run_metadata':
                omitted += 1
                continue
            if len(compacted) >= _STATUS_RESULT_MAX_MAPPING_ITEMS:
                omitted += 1
                continue
            compacted[normalized_key] = _compact_status_value(
                item_value,
                depth=depth + 1,
            )
        if omitted:
            compacted['_omitted_field_count'] = omitted
        return compacted
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        sample = [
            _compact_status_value(item, depth=depth + 1)
            for item in items[:_STATUS_RESULT_MAX_SEQUENCE_ITEMS]
        ]
        if len(items) <= _STATUS_RESULT_MAX_SEQUENCE_ITEMS:
            return sample
        return {
            'item_count': len(items),
            'sample': sample,
        }
    return _compact_status_string(str(value))


def compact_eod_run_summary(run: Mapping[str, Any]) -> dict[str, Any]:
    summary = {
        key: _compact_status_value(run.get(key))
        for key in _EOD_RUN_SUMMARY_KEYS
        if key in run
    }
    metadata = run.get('run_metadata')
    if not isinstance(metadata, Mapping):
        metadata_summary = run.get('metadata_summary')
        if isinstance(metadata_summary, Mapping):
            summary['metadata_summary'] = {
                str(key): _compact_status_value(value)
                for key, value in metadata_summary.items()
            }
        return summary

    metadata_summary = {
        key: _compact_status_value(metadata.get(key))
        for key in _RUN_METADATA_SUMMARY_KEYS
        if key in metadata
    }
    for key in _RUN_METADATA_SYMBOL_LIST_KEYS:
        values = metadata.get(key)
        if isinstance(values, (list, tuple, set)):
            metadata_summary[f'{key}_count'] = len(values)
    stage_progress = metadata.get('stage_progress')
    if isinstance(stage_progress, Mapping):
        metadata_summary['stage_statuses'] = {
            str(stage): _compact_status_value(
                progress.get('status')
                if isinstance(progress, Mapping)
                else progress
            )
            for stage, progress in list(stage_progress.items())[
                :_STATUS_RESULT_MAX_SEQUENCE_ITEMS
            ]
        }
        metadata_summary['stage_count'] = len(stage_progress)
    if metadata_summary:
        summary['metadata_summary'] = metadata_summary
    return summary


def compact_daily_pipeline_result(
    result: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if result is None:
        return None
    compacted: dict[str, Any] = {}
    existing_omitted = result.get('_omitted_field_count')
    omitted = (
        existing_omitted
        if type(existing_omitted) is int and existing_omitted >= 0
        else 0
    )
    for key, value in result.items():
        normalized_key = str(key)
        if normalized_key == '_omitted_field_count':
            continue
        if normalized_key == 'run_metadata':
            omitted += 1
            continue
        if len(compacted) >= _STATUS_RESULT_MAX_MAPPING_ITEMS:
            omitted += 1
            continue
        if normalized_key == 'existing_run' and isinstance(value, Mapping):
            compacted[normalized_key] = compact_eod_run_summary(value)
        else:
            compacted[normalized_key] = _compact_status_value(value, depth=1)
    if omitted:
        compacted['_omitted_field_count'] = omitted
    return compacted


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
    previous_retry = (
        metadata.get('retry')
        if isinstance(metadata.get('retry'), Mapping)
        else {}
    )
    baseline_run_id = previous_retry.get('baseline_run_id') or run.get('id')
    baseline_symbols_total = previous_retry.get('baseline_symbols_total')
    if baseline_symbols_total is None:
        baseline_symbols_total = run.get('symbols_total')
    baseline_symbols_succeeded = previous_retry.get(
        'baseline_symbols_succeeded'
    )
    if baseline_symbols_succeeded is None:
        baseline_symbols_succeeded = run.get('symbols_succeeded')
    if int(baseline_symbols_succeeded or 0) > 0:
        context.update(
            {
                'baseline_run_id': baseline_run_id,
                'baseline_symbols_total': int(baseline_symbols_total or 0),
                'baseline_symbols_succeeded': int(
                    baseline_symbols_succeeded or 0
                ),
            }
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
