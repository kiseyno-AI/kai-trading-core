from copy import deepcopy
from datetime import date
import subprocess
from unittest.mock import Mock

import pytest

from app.core.settings import settings
from app.forecast_context import service


MARKET_DATE = date(2026, 11, 6)
CUTOFF = '2026-11-06T14:00:00+00:00'


def price_bars(count=128):
    sessions = service.KRX_EXCHANGE_CALENDAR.sessions_in_range('2025-01-01', MARKET_DATE.isoformat())[-count:]
    return [
        {'trade_date': session.date().isoformat(), 'close_price': 100,
         'captured_at': '2026-11-06T12:00:00+00:00', 'source': 'pykrx'}
        for session in reversed(sessions)
    ]


def worker_response(request):
    return {
        'model_id': service.MODEL_ID, 'model_revision': service.MODEL_REVISION,
        'package_version': service.PACKAGE_VERSION, 'backend': 'mlx',
        'items': [
            {'symbol': item['symbol'], 'quantiles': [[98, 99, 99.5, 100, 101, 102, 103, 103.5, 104] for _ in range(20)]}
            for item in request['series']
        ],
    }


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setattr(settings, 'timesfm_enabled', True)
    monkeypatch.setattr(settings, 'timesfm_context_length', 512)
    monkeypatch.setattr(settings, 'timesfm_min_context_length', 128)
    monkeypatch.setattr(settings, 'timesfm_batch_size', 32)
    histories = {'005930': price_bars()}
    loader = Mock(side_effect=lambda **_: deepcopy(histories))
    worker = Mock(side_effect=worker_response)
    monkeypatch.setattr(service, 'load_recent_price_bars_by_symbol', loader)
    monkeypatch.setattr(service, '_run_worker', worker)
    return service.TimesFMForecastService(), histories, loader, worker


def build(instance, symbols=None, cutoff=CUTOFF):
    return instance.build_for_symbols(
        market_date=MARKET_DATE, symbols=symbols or ['005930'], knowledge_cutoff_at=cutoff,
    )


def test_disabled_never_reads_prices_or_runs_model(configured, monkeypatch):
    instance, _, loader, worker = configured
    monkeypatch.setattr(settings, 'timesfm_enabled', False)
    assert build(instance) == {}
    loader.assert_not_called()
    worker.assert_not_called()


def test_sorted_price_window_quantiles_percent_units_and_cache(configured):
    instance, _, _, worker = configured
    evidence = build(instance)['005930']
    assert evidence['status'] == 'available'
    assert evidence['context_points'] == 128
    assert evidence['input_end_date'] == MARKET_DATE.isoformat()
    assert evidence['calibration_status'] == 'not_evaluated'
    assert evidence['adjustment_status'] == 'not_verified'
    assert 'confidence' not in evidence
    assert [row['trading_bars'] for row in evidence['horizons']] == [1, 5, 20]
    for horizon in evidence['horizons']:
        assert horizon['return_q10_pct'] == pytest.approx(-2)
        assert horizon['median_return_pct'] == pytest.approx(1)
        assert horizon['return_q90_pct'] == pytest.approx(4)
    assert build(instance)['005930'] == evidence
    worker.assert_called_once()
    assert len(worker.call_args.args[0]['series'][0]['values']) == 128


@pytest.mark.parametrize('mutation,reason', [
    (lambda rows: rows[0].update(captured_at=CUTOFF), 'price_not_available_at_cutoff'),
    (lambda rows: rows[0].update(captured_at='2026-11-07T00:00:00+00:00'), 'price_not_available_at_cutoff'),
    (lambda rows: rows[0].update(captured_at='2026-11-06T12:00:00'), 'invalid_price_timestamp'),
    (lambda rows: rows[0].update(close_price=float('nan')), 'invalid_close_price'),
    (lambda rows: rows[0].update(close_price=0), 'invalid_close_price'),
    (lambda rows: rows[1].update(trade_date=rows[0]['trade_date']), 'duplicate_price_dates'),
    (lambda rows: rows.pop(0), 'missing_market_date_bar'),
    (lambda rows: rows.pop(), 'insufficient_price_history'),
])
def test_bad_price_data_does_not_infer(configured, mutation, reason):
    instance, histories, _, worker = configured
    mutation(histories['005930'])
    evidence = build(instance)['005930']
    assert evidence['status'] == 'unavailable'
    assert evidence['reason'] == reason
    worker.assert_not_called()


def test_missing_session_is_not_silently_compressed(configured):
    instance, histories, _, worker = configured
    histories['005930'] = price_bars(129)
    histories['005930'].pop(50)
    assert build(instance)['005930']['reason'] == 'missing_or_non_session_price_bars'
    worker.assert_not_called()


def test_pre_finality_cutoff_never_reads_price_history(configured):
    instance, _, loader, worker = configured
    evidence = build(instance, cutoff='2026-11-06T06:30:00+00:00')['005930']
    assert evidence['reason'] == 'session_not_finalized_at_cutoff'
    loader.assert_not_called()
    worker.assert_not_called()


def test_model_timeout_is_optional_and_retry_is_bounded(configured):
    instance, _, _, worker = configured
    worker.side_effect = subprocess.TimeoutExpired('worker', 120)
    assert build(instance)['005930']['reason'] == 'forecast_worker_timeout'
    assert build(instance)['005930']['reason'] == 'forecast_worker_cooldown'
    worker.assert_called_once()


@pytest.mark.parametrize('invalid_row', [
    [1] * 8, [1, 2, 3, 4, float('inf'), 6, 7, 8, 9],
    [1, 2, 3, 4, 3, 6, 7, 8, 9], [-1] * 9,
])
def test_malformed_or_crossed_quantiles_never_become_evidence(configured, invalid_row):
    instance, _, _, worker = configured
    result = worker_response({'series': [{'symbol': '005930'}]})
    result['items'][0]['quantiles'][0] = invalid_row
    worker.side_effect = None
    worker.return_value = result
    evidence = build(instance)['005930']
    assert evidence['status'] == 'unavailable'
    assert 'horizons' not in evidence
    assert build(instance)['005930'] == evidence
    worker.assert_called_once()


def test_invalid_forecast_retries_after_input_cooldown(configured, monkeypatch):
    instance, _, _, worker = configured
    clock = Mock(return_value=100.0)
    monkeypatch.setattr(service, 'monotonic', clock)
    invalid = worker_response({'series': [{'symbol': '005930'}]})
    invalid['items'][0]['quantiles'][0] = [-1] * 9
    worker.side_effect = None
    worker.return_value = invalid
    assert build(instance)['005930']['reason'] == 'invalid_forecast_values'
    worker.side_effect = worker_response
    clock.return_value = 100.0 + service.FAILURE_COOLDOWN_SECONDS - 1
    assert build(instance)['005930']['status'] == 'unavailable'
    worker.assert_called_once()
    clock.return_value += 1
    assert build(instance)['005930']['status'] == 'available'
    assert worker.call_count == 2


def test_invalid_forecast_cooldown_is_isolated_by_input(configured):
    instance, histories, _, worker = configured
    histories['000660'] = price_bars()

    def mixed_response(request):
        response = worker_response(request)
        for item in response['items']:
            if item['symbol'] == '005930':
                item['quantiles'][0] = [-1] * 9
        return response

    worker.side_effect = mixed_response
    assert build(instance)['005930']['reason'] == 'invalid_forecast_values'
    result = build(instance, ['005930', '000660'])
    assert result['005930']['status'] == 'unavailable'
    assert result['000660']['status'] == 'available'
    assert [item['symbol'] for item in worker.call_args.args[0]['series']] == ['000660']
    worker.side_effect = worker_response
    histories['005930'][70]['close_price'] = 99
    assert build(instance)['005930']['status'] == 'available'
    assert worker.call_count == 3


def test_batch_failure_does_not_hide_valid_other_symbol(configured):
    instance, histories, _, worker = configured
    histories['000660'] = price_bars()
    histories['000660'][0]['close_price'] = 0
    result = build(instance, ['005930', '000660'])
    assert result['005930']['status'] == 'available'
    assert result['000660']['status'] == 'unavailable'
    assert [row['symbol'] for row in worker.call_args.args[0]['series']] == ['005930']


def test_source_revalidation_never_infers_and_catches_historical_revision(configured):
    instance, histories, _, worker = configured
    evidence = build(instance)
    assert instance.validate_evidence(
        market_date=MARKET_DATE, evidence_by_symbol=evidence, knowledge_cutoff_at=CUTOFF,
    ) == {}
    histories['005930'][70]['close_price'] = 99
    assert instance.validate_evidence(
        market_date=MARKET_DATE, evidence_by_symbol=evidence, knowledge_cutoff_at=CUTOFF,
    ) == {'005930': 'forecast_source_changed'}
    worker.assert_called_once()


def test_evidence_mutation_and_cutoff_change_are_rejected(configured):
    instance, _, _, _ = configured
    evidence = build(instance)
    assert instance.validate_evidence(
        market_date=MARKET_DATE, evidence_by_symbol=evidence,
        knowledge_cutoff_at='2026-11-06T14:01:00+00:00',
    ) == {'005930': 'forecast_cutoff_mismatch'}
    evidence['005930']['horizons'][0]['price_q50'] = 1000
    assert instance.validate_evidence(
        market_date=MARKET_DATE, evidence_by_symbol=evidence, knowledge_cutoff_at=CUTOFF,
    ) == {'005930': 'forecast_evidence_fingerprint_mismatch'}


def test_status_mutation_cannot_bypass_input_fence(configured):
    instance, _, _, _ = configured
    evidence = build(instance)
    evidence['005930']['status'] = 'unavailable'
    assert instance.validate_evidence(
        market_date=MARKET_DATE, evidence_by_symbol=evidence, knowledge_cutoff_at=CUTOFF,
    ) == {'005930': 'forecast_evidence_fingerprint_mismatch'}


def test_authentic_unavailable_is_neutral_without_prices_or_model(configured):
    instance, _, loader, worker = configured
    evidence = {'005930': service.unavailable_evidence('005930', MARKET_DATE, 'forecast_worker_failed')}
    assert instance.validate_evidence(
        market_date=MARKET_DATE, evidence_by_symbol=evidence, knowledge_cutoff_at=CUTOFF,
    ) == {}
    loader.assert_not_called()
    worker.assert_not_called()


def test_worker_runs_offline_with_timeout_and_exact_interpreter(monkeypatch):
    response = worker_response({'series': [{'symbol': '005930'}]})
    run = Mock(return_value=subprocess.CompletedProcess([], 0, service.json.dumps(response), ''))
    monkeypatch.setattr(service.subprocess, 'run', run)
    monkeypatch.setattr(settings, 'timesfm_python_executable', '/test/worker/python')
    assert service._run_worker({'series': []})['backend'] == 'mlx'
    assert run.call_args.args[0] == ['/test/worker/python', '-m', 'app.forecast_context.mlx_runner']
    assert run.call_args.kwargs['env']['HF_HUB_OFFLINE'] == '1'
    assert run.call_args.kwargs['timeout'] == settings.timesfm_timeout_seconds
    assert run.call_args.kwargs.get('shell', False) is False


def test_worker_wrong_model_identity_is_rejected(monkeypatch):
    response = worker_response({'series': []})
    response['model_revision'] = 'another-revision'
    monkeypatch.setattr(service.subprocess, 'run', Mock(return_value=subprocess.CompletedProcess([], 0, service.json.dumps(response), '')))
    with pytest.raises(service.ForecastUnavailable, match='worker_model_identity_mismatch'):
        service._run_worker({})
