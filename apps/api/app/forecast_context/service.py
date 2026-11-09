"""Optional, point-in-time price forecasts attached only at decider dispatch.

The MLX worker runs in a bounded subprocess and loads once per uncached batch.
Identical inputs reuse an in-memory result; source revalidation never infers.
"""
from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from datetime import date, datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
from threading import Lock
from time import monotonic, perf_counter
from typing import Any

from app.core.numbers import finite_float
from app.core.settings import settings
from app.eod.finality import validate_finalized_krx_write_date
from app.forecast_context.mlx_runner import MAX_INPUT_POINTS, MODEL_ID, MODEL_REVISION, PACKAGE_VERSION
from app.market.session import KRX_EXCHANGE_CALENDAR, KRX_EXCHANGE_CALENDAR_SOURCE
from app.market_data_store import load_recent_price_bars_by_symbol

logger = logging.getLogger(__name__)
HORIZONS = (1, 5, 20)
SCHEMA_VERSION = 'timesfm3_decision_evidence_v1'
MAX_CACHE_ITEMS = 4096
FAILURE_COOLDOWN_SECONDS = 60.0
API_ROOT = Path(__file__).resolve().parents[2]


class ForecastUnavailable(ValueError):
    pass


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(',', ':'), allow_nan=False,
    ).encode()).hexdigest()


def _aware_datetime(value: datetime | str | None) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except (ValueError, TypeError) as exc:
        raise ForecastUnavailable('timezone_aware_cutoff_required') from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ForecastUnavailable('timezone_aware_cutoff_required')
    return parsed.astimezone(timezone.utc)


def _validate_date(market_date: date, cutoff: datetime) -> None:
    if cutoff > datetime.now(timezone.utc):
        raise ForecastUnavailable('future_knowledge_cutoff')
    try:
        validate_finalized_krx_write_date(market_date, now=cutoff, operation='TimesFM evidence')
    except ValueError as exc:
        raise ForecastUnavailable('session_not_finalized_at_cutoff') from exc
    if not KRX_EXCHANGE_CALENDAR.is_session(market_date.isoformat()):
        raise ForecastUnavailable('not_a_krx_session')


def _prepare_window(
    *, symbol: str, market_date: date, bars: list[dict], cutoff: datetime,
    context_length: int, min_context_length: int,
) -> dict[str, Any]:
    if min_context_length > context_length:
        raise ForecastUnavailable('minimum_context_exceeds_context_length')
    if not bars:
        raise ForecastUnavailable('missing_price_history')
    rows = sorted(bars, key=lambda row: str(row.get('trade_date') or ''))[-context_length:]
    normalized = []
    for row in rows:
        try:
            trade_date = date.fromisoformat(str(row.get('trade_date')))
            captured_at = _aware_datetime(row.get('captured_at'))
        except (ValueError, TypeError) as exc:
            raise ForecastUnavailable('invalid_price_timestamp') from exc
        if trade_date > market_date or captured_at >= cutoff:
            raise ForecastUnavailable('price_not_available_at_cutoff')
        close = finite_float(row.get('close_price'))
        if close is None or close <= 0:
            raise ForecastUnavailable('invalid_close_price')
        normalized.append({
            'trade_date': trade_date.isoformat(), 'close_price': close,
            'captured_at': captured_at.isoformat(), 'source': str(row.get('source') or ''),
        })
    dates = [row['trade_date'] for row in normalized]
    if len(set(dates)) != len(dates):
        raise ForecastUnavailable('duplicate_price_dates')
    if dates[-1] != market_date.isoformat():
        raise ForecastUnavailable('missing_market_date_bar')
    if len(rows) < min_context_length:
        raise ForecastUnavailable('insufficient_price_history')
    sessions = KRX_EXCHANGE_CALENDAR.sessions_in_range(dates[0], dates[-1])
    if dates != [session.date().isoformat() for session in sessions]:
        raise ForecastUnavailable('missing_or_non_session_price_bars')
    identity = {
        'symbol': symbol, 'market_date': market_date.isoformat(),
        'model_id': MODEL_ID, 'model_revision': MODEL_REVISION,
        'package_version': PACKAGE_VERSION, 'context_length': context_length,
        'horizons': list(HORIZONS), 'target': 'stored_daily_close',
        'target_transform': 'none', 'rows': normalized,
    }
    return {
        'values': [row['close_price'] for row in normalized],
        'input_fingerprint': _fingerprint(identity),
        'context_length': context_length, 'context_points': len(rows),
        'input_start_date': dates[0], 'input_end_date': dates[-1],
        'input_max_captured_at': max(row['captured_at'] for row in normalized),
        'input_sources': sorted({row['source'] for row in normalized}),
        'anchor_close': normalized[-1]['close_price'],
    }


def unavailable_evidence(symbol: str, market_date: date, reason: str) -> dict[str, Any]:
    evidence = {
        'schema_version': SCHEMA_VERSION, 'status': 'unavailable', 'reason': reason,
        'symbol': symbol, 'market_date': market_date.isoformat(),
        'model_id': MODEL_ID, 'model_revision': MODEL_REVISION,
        'package_version': PACKAGE_VERSION,
        'decision_use': 'optional_auxiliary_evidence',
    }
    evidence['evidence_fingerprint'] = _fingerprint(evidence)
    return evidence


def _run_worker(request: dict[str, Any]) -> dict[str, Any]:
    # A dedicated interpreter may hold MLX without changing the API environment.
    # Provision the pinned checkpoint with the benchmark CLI before enabling.
    result = subprocess.run(
        [settings.timesfm_python_executable or sys.executable,
         '-m', 'app.forecast_context.mlx_runner'],
        cwd=API_ROOT, input=json.dumps(request, allow_nan=False),
        capture_output=True, text=True, check=True,
        timeout=settings.timesfm_timeout_seconds,
        env={**os.environ, 'HF_HUB_OFFLINE': '1', 'HF_HUB_DISABLE_PROGRESS_BARS': '1'},
    )
    payload = json.loads(result.stdout)
    if not isinstance(payload, dict) or any(
        payload.get(key) != expected for key, expected in (
            ('model_id', MODEL_ID), ('model_revision', MODEL_REVISION),
            ('package_version', PACKAGE_VERSION), ('backend', 'mlx'),
        )
    ):
        raise ForecastUnavailable('worker_model_identity_mismatch')
    return payload


def _validated_quantiles(value: Any) -> list[list[float]]:
    if not isinstance(value, list) or len(value) != max(HORIZONS):
        raise ForecastUnavailable('invalid_forecast_shape')
    result = []
    for row in value:
        if not isinstance(row, list) or len(row) != 9:
            raise ForecastUnavailable('invalid_forecast_shape')
        numbers = [finite_float(item) for item in row]
        if any(item is None or item <= 0 for item in numbers):
            raise ForecastUnavailable('invalid_forecast_values')
        if numbers != sorted(numbers):
            raise ForecastUnavailable('crossed_forecast_quantiles')
        result.append(numbers)
    return result


class TimesFMForecastService:
    def __init__(self) -> None:
        self._lock = Lock()
        self._cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._retry_after = 0.0

    def build_for_symbols(
        self, *, market_date: date, symbols: list[str],
        knowledge_cutoff_at: str | None,
    ) -> dict[str, dict[str, Any]]:
        if not settings.timesfm_enabled or not symbols:
            return {}
        symbols = list(dict.fromkeys(symbols))
        if len(symbols) > MAX_CACHE_ITEMS or len(symbols) * settings.timesfm_context_length > MAX_INPUT_POINTS:
            return {s: unavailable_evidence(s, market_date, 'forecast_batch_too_large') for s in symbols}
        try:
            cutoff = _aware_datetime(knowledge_cutoff_at)
            _validate_date(market_date, cutoff)
            histories = load_recent_price_bars_by_symbol(
                market_date=market_date, symbols=symbols,
                history_days=settings.timesfm_context_length,
            )
        except Exception as exc:
            reason = str(exc) if isinstance(exc, ForecastUnavailable) else 'source_history_unavailable'
            return {s: unavailable_evidence(s, market_date, reason) for s in symbols}
        result, windows = {}, {}
        for symbol in symbols:
            try:
                windows[symbol] = _prepare_window(
                    symbol=symbol, market_date=market_date, bars=histories.get(symbol, []),
                    cutoff=cutoff, context_length=settings.timesfm_context_length,
                    min_context_length=settings.timesfm_min_context_length,
                )
            except Exception as exc:
                reason = str(exc) if isinstance(exc, ForecastUnavailable) else 'invalid_price_history'
                result[symbol] = unavailable_evidence(symbol, market_date, reason)
        if not windows:
            return result
        if not self._lock.acquire(timeout=settings.timesfm_timeout_seconds):
            result.update({s: unavailable_evidence(s, market_date, 'forecast_worker_busy') for s in windows})
            return result
        try:
            now = monotonic()
            for window in windows.values():
                key = window['input_fingerprint']
                cached = self._cache.get(key)
                if cached is not None and 'retry_after' in cached and now >= cached['retry_after']:
                    del self._cache[key]
            missing = [s for s, window in windows.items() if window['input_fingerprint'] not in self._cache]
            failure_reason = 'forecast_worker_cooldown'
            if missing and now >= self._retry_after:
                try:
                    started = perf_counter()
                    response = _run_worker({
                        'model_id': MODEL_ID, 'model_revision': MODEL_REVISION,
                        'context_length': settings.timesfm_context_length,
                        'horizon': max(HORIZONS), 'batch_size': settings.timesfm_batch_size,
                        'series': [{'symbol': s, 'values': windows[s]['values']} for s in missing],
                    })
                    worker_ms = (perf_counter() - started) * 1000
                    items = response.get('items')
                    if not isinstance(items, list) or len(items) != len(missing):
                        raise ForecastUnavailable('worker_symbol_mismatch')
                    by_symbol = {row['symbol']: row for row in items}
                    if set(by_symbol) != set(missing):
                        raise ForecastUnavailable('worker_symbol_mismatch')
                    for symbol in missing:
                        try:
                            quantiles = _validated_quantiles(by_symbol[symbol].get('quantiles'))
                            self._cache[windows[symbol]['input_fingerprint']] = {
                                'quantiles': quantiles, 'generated_at': datetime.now(timezone.utc).isoformat(),
                                'worker_batch_ms': round(worker_ms, 3),
                                'batch_symbols': len(missing),
                            }
                        except ForecastUnavailable as exc:
                            self._cache[windows[symbol]['input_fingerprint']] = {
                                'reason': str(exc),
                                'retry_after': monotonic() + FAILURE_COOLDOWN_SECONDS,
                            }
                except Exception as exc:
                    self._retry_after = monotonic() + FAILURE_COOLDOWN_SECONDS
                    failure_reason = ('forecast_worker_timeout' if isinstance(exc, subprocess.TimeoutExpired)
                                      else str(exc) if isinstance(exc, ForecastUnavailable)
                                      else 'forecast_worker_failed')
                    logger.warning('TimesFM evidence unavailable: %s (%s)', failure_reason, type(exc).__name__)
            for symbol, window in windows.items():
                if symbol in result:
                    continue
                cached = self._cache.get(window['input_fingerprint'])
                if cached is None:
                    result[symbol] = unavailable_evidence(symbol, market_date, failure_reason)
                    continue
                self._cache.move_to_end(window['input_fingerprint'])
                if 'reason' in cached:
                    result[symbol] = unavailable_evidence(symbol, market_date, cached['reason'])
                    continue
                anchor = window['anchor_close']
                horizons = []
                for step in HORIZONS:
                    q10, q50, q90 = (cached['quantiles'][step - 1][i] for i in (0, 4, 8))
                    horizons.append({
                        'trading_bars': step, 'price_q10': q10, 'price_q50': q50, 'price_q90': q90,
                        'return_q10_pct': (q10 / anchor - 1) * 100,
                        'median_return_pct': (q50 / anchor - 1) * 100,
                        'return_q90_pct': (q90 / anchor - 1) * 100,
                    })
                evidence = {
                    'schema_version': SCHEMA_VERSION, 'status': 'available',
                    'symbol': symbol, 'market_date': market_date.isoformat(),
                    'knowledge_cutoff_at': cutoff.isoformat(),
                    'model_id': MODEL_ID, 'model_revision': MODEL_REVISION,
                    'package_version': PACKAGE_VERSION, 'backend': 'mlx',
                    'price_basis': 'stored_daily_close', 'adjustment_status': 'not_verified',
                    'target_transform': 'none', 'horizon_unit': 'trading_bars',
                    'calendar_source': KRX_EXCHANGE_CALENDAR_SOURCE,
                    'calibration_status': 'not_evaluated', 'decision_use': 'optional_auxiliary_evidence',
                    **{k: v for k, v in window.items() if k != 'values'},
                    'generated_at': cached['generated_at'], 'horizons': horizons,
                    'timing': {'worker_batch_ms': cached['worker_batch_ms'],
                               'batch_symbols': cached['batch_symbols'],
                               'scope': 'original_uncached_worker_batch_including_load'},
                }
                evidence['evidence_fingerprint'] = _fingerprint(evidence)
                result[symbol] = evidence
            while len(self._cache) > MAX_CACHE_ITEMS:
                self._cache.popitem(last=False)
            return deepcopy(result)
        finally:
            self._lock.release()

    def validate_evidence(
        self, *, market_date: date, evidence_by_symbol: dict[str, dict],
        knowledge_cutoff_at: str | None,
    ) -> dict[str, str]:
        """Validate saved evidence against current source rows; never run MLX.

        This also runs for previously available evidence after the feature is
        disabled, so existing snapshots cannot bypass their source fence.
        """
        invalid, available = {}, {}
        for symbol, evidence in evidence_by_symbol.items():
            if not isinstance(evidence, dict) or evidence.get('status') not in {'available', 'unavailable'}:
                invalid[symbol] = 'invalid_forecast_evidence'
                continue
            try:
                saved_hash = evidence.get('evidence_fingerprint')
                if not saved_hash or saved_hash != _fingerprint({k: v for k, v in evidence.items() if k != 'evidence_fingerprint'}):
                    raise ForecastUnavailable('forecast_evidence_fingerprint_mismatch')
                if (evidence.get('symbol') != symbol or evidence.get('market_date') != market_date.isoformat()
                        or evidence.get('model_id') != MODEL_ID or evidence.get('model_revision') != MODEL_REVISION
                        or evidence.get('package_version') != PACKAGE_VERSION
                        or evidence.get('schema_version') != SCHEMA_VERSION):
                    raise ForecastUnavailable('forecast_evidence_identity_mismatch')
                if evidence['status'] == 'unavailable':
                    continue
                cutoff = _aware_datetime(knowledge_cutoff_at)
                if cutoff != _aware_datetime(evidence.get('knowledge_cutoff_at')):
                    raise ForecastUnavailable('forecast_cutoff_mismatch')
                _validate_date(market_date, cutoff)
                length = evidence.get('context_length')
                if not isinstance(length, int) or not 128 <= length <= 2048:
                    raise ForecastUnavailable('invalid_forecast_context_length')
                available[symbol] = (evidence, cutoff, length)
            except Exception as exc:
                invalid[symbol] = str(exc) if isinstance(exc, ForecastUnavailable) else 'invalid_forecast_evidence'
        if not available:
            return invalid
        try:
            histories = load_recent_price_bars_by_symbol(
                market_date=market_date, symbols=list(available),
                history_days=max(item[2] for item in available.values()),
            )
            for symbol, (evidence, cutoff, length) in available.items():
                try:
                    window = _prepare_window(
                        symbol=symbol, market_date=market_date, bars=histories.get(symbol, []),
                        cutoff=cutoff, context_length=length, min_context_length=32,
                    )
                    if window['input_fingerprint'] != evidence.get('input_fingerprint'):
                        invalid[symbol] = 'forecast_source_changed'
                except Exception:
                    invalid[symbol] = 'forecast_source_unavailable'
        except Exception:
            invalid.update({s: 'forecast_source_revalidation_failed' for s in available})
        return invalid


timesfm_forecast_service = TimesFMForecastService()
