"""Isolated TimesFM 3.0 MLX inference and latency measurement.

Run from apps/api: python -m app.forecast_context.mlx_runner < request.json
The module imports only the standard library until inference is requested.
"""

from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from importlib import metadata
import json
import math
from pathlib import Path
import platform
import random
import re
import statistics
import sys
from time import perf_counter
from typing import Any


MODEL_ID = 'google/timesfm-3.0-pytorch'
MODEL_REVISION = '43046b85ec22d584a13f8098c2ed39c889e129c2'
PACKAGE_VERSION = '3.0.2'
MAX_INPUT_BYTES = 128 * 1024 * 1024
MAX_INPUT_POINTS = 4_194_304
MAX_OUTPUT_VALUES = 2_000_000
QUANTILE_LEVELS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]


class RunnerError(ValueError):
    """A safe, input-independent error code for the subprocess boundary."""


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise RunnerError(f'invalid_{name}')
    return value


def _finite_number(value: Any) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value) and abs(value) <= 1e30
    except (OverflowError, ValueError):
        return False


def validate_request(payload: Any) -> dict[str, Any]:
    """Validate resource bounds without importing ML packages or loading weights."""
    if not isinstance(payload, dict):
        raise RunnerError('invalid_request')
    if payload.get('model_id', MODEL_ID) != MODEL_ID:
        raise RunnerError('unsupported_model_id')
    if payload.get('model_revision', MODEL_REVISION) != MODEL_REVISION:
        raise RunnerError('unsupported_model_revision')
    context = _bounded_int(payload.get('context_length', 512), 'context_length', 32, 2048)
    horizon = _bounded_int(payload.get('horizon', 20), 'horizon', 1, 128)
    batch = _bounded_int(payload.get('batch_size', 32), 'batch_size', 1, 32)
    series = payload.get('series')
    if not isinstance(series, list) or not 1 <= len(series) <= 4096:
        raise RunnerError('invalid_series_count')
    if len(series) * horizon * len(QUANTILE_LEVELS) > MAX_OUTPUT_VALUES:
        raise RunnerError('output_limit_exceeded')
    symbols: set[str] = set()
    points = 0
    for item in series:
        if not isinstance(item, dict):
            raise RunnerError('invalid_series')
        symbol, values = item.get('symbol'), item.get('values')
        if not isinstance(symbol, str) or not re.fullmatch(r'[A-Za-z0-9._:-]{1,40}', symbol):
            raise RunnerError('invalid_symbol')
        if symbol in symbols:
            raise RunnerError('duplicate_symbol')
        symbols.add(symbol)
        if not isinstance(values, list) or not 32 <= len(values) <= context:
            raise RunnerError('invalid_context_values')
        points += len(values)
        if points > MAX_INPUT_POINTS:
            raise RunnerError('input_limit_exceeded')
        if not all(_finite_number(value) for value in values):
            raise RunnerError('nonfinite_or_invalid_input')
    return {
        'model_id': MODEL_ID,
        'model_revision': MODEL_REVISION,
        'context_length': context,
        'horizon': horizon,
        'batch_size': batch,
        'series': series,
    }


def _load_runtime():
    if platform.system() != 'Darwin' or platform.machine() not in ('arm64', 'aarch64'):
        raise RunnerError('native_apple_silicon_required')
    try:
        installed = metadata.version('timesfm')
    except metadata.PackageNotFoundError:
        raise RunnerError('timesfm_dependency_missing') from None
    if installed != PACKAGE_VERSION:
        raise RunnerError('timesfm_package_version_mismatch')
    try:
        import numpy as np
        import mlx.core as mx
        from timesfm3.mlx import TimesFM3Forecaster
    except ImportError:
        raise RunnerError('mlx_dependencies_unavailable') from None
    return np, mx, TimesFM3Forecaster


class TimesFM3MLXRunner:
    """Load once per worker instance; manually bound every MLX inference batch."""

    def __init__(self) -> None:
        self._model = None
        self._np = None
        self._mx = None

    def _load_model(self) -> float:
        if self._model is not None:
            return 0.0
        start = perf_counter()
        self._np, self._mx, forecaster = _load_runtime()
        self._model = forecaster.from_pretrained(
            MODEL_ID,
            revision=MODEL_REVISION,
            compile=True,
            max_context_length=2048,
        )
        self._mx.synchronize()
        return (perf_counter() - start) * 1000

    def forecast(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = validate_request(payload)
        load_ms = self._load_model()
        self._mx.synchronize()
        start = perf_counter()
        items = []
        series = request['series']
        # MLX 3.0.2 ignores per_core_batch_size on the univariate fast path.
        for offset in range(0, len(series), request['batch_size']):
            chunk = series[offset:offset + request['batch_size']]
            symbols = [item['symbol'] for item in chunk]
            arrays = [self._np.asarray(item['values'], dtype=self._np.float32) for item in chunk]
            outputs = list(self._model.predict_batch(
                contexts=arrays,
                ts_ids=symbols,
                horizon=request['horizon'],
                return_quantiles=True,
                use_symmetric_averaging=False,
                make_positive=False,
                sort_quantiles=True,
                use_znorm=False,
                padding_mode='none',
            ))
            if len(outputs) != len(chunk):
                raise RunnerError('output_count_mismatch')
            for symbol, output in zip(symbols, outputs):
                if output.ts_id != symbol or output.quantiles is None:
                    raise RunnerError('output_identity_mismatch')
                quantiles = output.quantiles.tolist()
                if not isinstance(quantiles, list) or len(quantiles) != request['horizon']:
                    raise RunnerError('invalid_output_shape')
                for row in quantiles:
                    if not isinstance(row, list) or len(row) != len(QUANTILE_LEVELS):
                        raise RunnerError('invalid_output_shape')
                    if not all(_finite_number(value) for value in row):
                        raise RunnerError('nonfinite_or_invalid_output')
                    if any(left > right for left, right in zip(row, row[1:])):
                        raise RunnerError('crossed_output_quantiles')
                items.append({'symbol': symbol, 'quantiles': quantiles})
        self._mx.synchronize()
        return {
            'model_id': MODEL_ID,
            'model_revision': MODEL_REVISION,
            'package_version': PACKAGE_VERSION,
            'backend': 'mlx',
            'load_ms': load_ms,
            'inference_ms': (perf_counter() - start) * 1000,
            'items': items,
        }

    def benchmark(
        self, payload: dict[str, Any], *, repeats: int = 30, warmups: int = 5,
        data_source: str = 'synthetic',
    ) -> dict[str, Any]:
        """Measure synchronized calls, keeping load and first-shape compilation separate."""
        repeats = _bounded_int(repeats, 'repeats', 1, 200)
        warmups = _bounded_int(warmups, 'warmups', 0, 20)
        request = validate_request(payload)
        first = self.forecast(request)
        for _ in range(warmups):
            self.forecast(request)
        self._mx.synchronize()
        self._mx.reset_peak_memory()
        timings = [self.forecast(request)['inference_ms'] for _ in range(repeats)]
        self._mx.synchronize()
        count = len(request['series'])
        return {
            'model_id': MODEL_ID,
            'model_revision': MODEL_REVISION,
            'package_version': PACKAGE_VERSION,
            'backend': 'mlx',
            'data_source': data_source,
            'synthetic': data_source == 'synthetic',
            'context_length': request['context_length'],
            'actual_context_lengths': sorted({len(item['values']) for item in request['series']}),
            'horizon': request['horizon'],
            'batch_size': request['batch_size'],
            'series_count': count,
            'repeats': repeats,
            'warmups': warmups,
            'load_ms': first['load_ms'],
            'first_call_ms': first['inference_ms'],
            'p50_batch_ms': _percentile(timings, 0.5),
            'p95_batch_ms': _percentile(timings, 0.95),
            'mean_per_symbol_ms': statistics.mean(timings) / count,
            'mlx_peak_memory_bytes': int(self._mx.get_peak_memory()),
            'timing_scope': 'array conversion, synchronized inference and output validation; excludes data access and model load',
        }


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = math.floor(index)
    upper = math.ceil(index)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def _read_payload(path: str | None) -> Any:
    if path:
        with Path(path).open('rb') as handle:
            raw = handle.read(MAX_INPUT_BYTES + 1)
    else:
        raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        raise RunnerError('input_bytes_limit_exceeded')
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        raise RunnerError('invalid_json') from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', help='JSON request file; default: stdin for inference')
    parser.add_argument('--benchmark', action='store_true')
    parser.add_argument('--context-length', type=int, default=512)
    parser.add_argument('--horizon', type=int, default=20)
    parser.add_argument('--batch-size', type=int, default=1)
    parser.add_argument('--repeats', type=int, default=30)
    parser.add_argument('--warmups', type=int, default=5)
    args = parser.parse_args(argv)
    try:
        # Library progress belongs on stderr; stdout is exactly one JSON response.
        with redirect_stdout(sys.stderr):
            if args.benchmark:
                batch_size = _bounded_int(args.batch_size, 'batch_size', 1, 32)
                if args.input:
                    request = validate_request(_read_payload(args.input))
                    if len(request['series']) < batch_size:
                        raise RunnerError('benchmark_requires_more_series')
                    request = {**request, 'batch_size': batch_size, 'series': request['series'][:batch_size]}
                    source = 'input_file'
                else:
                    context = _bounded_int(args.context_length, 'context_length', 32, 2048)
                    rng = random.Random(0)
                    request = {
                        'context_length': context, 'horizon': args.horizon, 'batch_size': batch_size,
                        'series': [
                            {'symbol': f'synthetic-{i}', 'values': [
                                100 + 0.01 * t + 2 * math.sin(t / 17 + i) + rng.gauss(0, 0.2)
                                for t in range(context)
                            ]} for i in range(batch_size)
                        ],
                    }
                    source = 'synthetic'
                result = TimesFM3MLXRunner().benchmark(request, repeats=args.repeats, warmups=args.warmups, data_source=source)
            else:
                result = TimesFM3MLXRunner().forecast(_read_payload(args.input))
        encoded = json.dumps(result, allow_nan=False, separators=(',', ':'))
        sys.stdout.write(encoded + '\n')
        return 0
    except RunnerError as error:
        sys.stderr.write(f'timesfm_runner_error: {error}\n')
    except Exception:
        # Provider exceptions may contain paths, tokens or user inputs.
        sys.stderr.write('timesfm_runner_error: inference_failed\n')
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
