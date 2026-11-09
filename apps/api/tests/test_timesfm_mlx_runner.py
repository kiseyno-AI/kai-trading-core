import copy
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from app.forecast_context import mlx_runner as module


def _request(count=1, batch_size=2):
    return {
        'model_id': module.MODEL_ID,
        'model_revision': module.MODEL_REVISION,
        'context_length': 128,
        'horizon': 5,
        'batch_size': batch_size,
        'series': [
            {'symbol': f'{index:06d}', 'values': [100.0] * 128}
            for index in range(count)
        ],
    }


@pytest.fixture
def fake_runtime(monkeypatch):
    state = SimpleNamespace(loads=[], calls=[], mutate=None, syncs=0)

    def synchronize():
        state.syncs += 1

    class Forecaster:
        @classmethod
        def from_pretrained(cls, model_id, **kwargs):
            state.loads.append((model_id, kwargs))
            return cls()

        def predict_batch(self, **kwargs):
            state.calls.append(kwargs)
            for symbol in kwargs['ts_ids']:
                rows = [[float(q) for q in range(9)] for _ in range(kwargs['horizon'])]
                output = SimpleNamespace(
                    ts_id=symbol, quantiles=SimpleNamespace(tolist=lambda rows=rows: rows),
                )
                if state.mutate:
                    state.mutate(output, rows)
                yield output

    numpy = SimpleNamespace(asarray=lambda values, dtype: list(values), float32='float32')
    mlx = SimpleNamespace(
        synchronize=synchronize,
        reset_peak_memory=lambda: None,
        get_peak_memory=lambda: 1234,
    )
    monkeypatch.setattr(module, '_load_runtime', lambda: (numpy, mlx, Forecaster))
    return state


def test_module_import_does_not_require_optional_dependencies():
    result = subprocess.run(
        [sys.executable, '-S', '-c', (
            'import sys; from app.forecast_context import mlx_runner; '
            'assert not {"numpy", "mlx", "timesfm3"}.intersection(sys.modules)'
        )],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr


def test_forecast_bounds_batches_loads_once_and_preserves_symbol_order(fake_runtime):
    runner = module.TimesFM3MLXRunner()
    request = _request(count=5)
    request['series'][0]['values'] = [100.0] * 64
    first = runner.forecast(request)
    second = runner.forecast(request)

    assert len(fake_runtime.loads) == 1
    assert fake_runtime.loads[0][1]['revision'] == module.MODEL_REVISION
    assert [len(call['contexts']) for call in fake_runtime.calls] == [2, 2, 1, 2, 2, 1]
    assert len(fake_runtime.calls[0]['contexts'][0]) == 64
    assert [item['symbol'] for item in first['items']] == [item['symbol'] for item in request['series']]
    assert first['items'][0]['quantiles'][0] == list(range(9))
    assert second['load_ms'] == 0
    assert fake_runtime.syncs > 0


@pytest.mark.parametrize('change', [
    lambda request: request.update(model_id='other/model'),
    lambda request: request.update(model_revision='main'),
    lambda request: request.update(batch_size=33),
    lambda request: request.update(horizon=True),
    lambda request: request['series'][0].update(values=[1.0] * 31),
    lambda request: request['series'][0]['values'].__setitem__(0, float('nan')),
    lambda request: request['series'][0]['values'].__setitem__(0, float('inf')),
    lambda request: request['series'][0]['values'].__setitem__(0, True),
    lambda request: request['series'].append(copy.deepcopy(request['series'][0])),
])
def test_invalid_request_fails_before_model_load(fake_runtime, change):
    request = _request()
    change(request)
    with pytest.raises(module.RunnerError):
        module.TimesFM3MLXRunner().forecast(request)
    assert fake_runtime.loads == []


@pytest.mark.parametrize('mutation,error', [
    (lambda output, rows: setattr(output, 'ts_id', 'wrong'), 'output_identity_mismatch'),
    (lambda output, rows: rows[0].__setitem__(4, float('nan')), 'nonfinite_or_invalid_output'),
    (lambda output, rows: rows[0].__setitem__(4, -1), 'crossed_output_quantiles'),
    (lambda output, rows: rows[0].pop(), 'invalid_output_shape'),
])
def test_invalid_model_output_is_not_returned_as_evidence(fake_runtime, mutation, error):
    fake_runtime.mutate = mutation
    with pytest.raises(module.RunnerError, match=error):
        module.TimesFM3MLXRunner().forecast(_request())


def test_package_mismatch_rejected_before_importing_mlx(monkeypatch):
    monkeypatch.setattr(module.platform, 'system', lambda: 'Darwin')
    monkeypatch.setattr(module.platform, 'machine', lambda: 'arm64')
    monkeypatch.setattr(module.metadata, 'version', lambda name: '0.0.0')
    with pytest.raises(module.RunnerError, match='timesfm_package_version_mismatch'):
        module._load_runtime()


def test_benchmark_separates_load_and_first_call_and_marks_synthetic(fake_runtime):
    report = module.TimesFM3MLXRunner().benchmark(_request(count=2), repeats=3, warmups=2)
    assert len(fake_runtime.loads) == 1
    assert len(fake_runtime.calls) == 6
    assert report['synthetic'] is True
    assert report['mlx_peak_memory_bytes'] == 1234
    assert report['load_ms'] >= 0
    assert report['first_call_ms'] >= 0
    assert report['p95_batch_ms'] >= report['p50_batch_ms']


def test_cli_emits_one_json_object_and_routes_provider_noise_to_stderr(tmp_path, monkeypatch, capsys):
    path = tmp_path / 'request.json'
    path.write_text(json.dumps(_request()))

    def forecast(self, request):
        print('provider progress')
        return {'items': [], 'backend': 'mlx'}

    monkeypatch.setattr(module.TimesFM3MLXRunner, 'forecast', forecast)
    assert module.main(['--input', str(path)]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {'items': [], 'backend': 'mlx'}
    assert 'provider progress' in captured.err


def test_cli_does_not_echo_provider_exception_content(tmp_path, monkeypatch, capsys):
    path = tmp_path / 'request.json'
    path.write_text(json.dumps(_request()))

    def forecast(self, request):
        raise RuntimeError('private-provider-token')

    monkeypatch.setattr(module.TimesFM3MLXRunner, 'forecast', forecast)
    assert module.main(['--input', str(path)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ''
    assert captured.err == 'timesfm_runner_error: inference_failed\n'
