import asyncio
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
import json
from threading import get_ident

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db_models import USIndexInputSnapshot
from app.market.us_index_collector import collect_us_index_inputs
from app.market.us_index_snapshot_store import load_us_index_snapshot


AS_OF = datetime(2026, 11, 24, 16, tzinfo=UTC)


def _payload(code):
    return {
        'rt_cd': '0', 'output1': {'stck_shrn_iscd': code},
        'output2': [
            {'stck_bsop_date': '20261123', 'ovrs_nmix_prpr': '7100.123456789012345678'},
            {'stck_bsop_date': '20261120', 'ovrs_nmix_prpr': '7000'},
        ],
    }


class FakeKISClient:
    use_mock = True

    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    async def fetch_overseas_index_daily_prices(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.get(kwargs['index_code'], _payload(kwargs['index_code']))
        if isinstance(response, BaseException):
            raise response
        return response


@pytest.fixture
def database(tmp_path):
    engine = create_engine(f'sqlite+pysqlite:///{tmp_path / "collector.sqlite"}')
    USIndexInputSnapshot.__table__.create(engine)

    @contextmanager
    def factory():
        with Session(engine) as session, session.begin():
            yield session

    yield factory
    engine.dispose()


def test_collects_three_indices_into_readable_committed_snapshots(database):
    client = FakeKISClient()
    receipts = iter(AS_OF + timedelta(seconds=i) for i in (1, 2, 3))

    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, client=client, session_factory=database, clock=lambda: next(receipts),
    ))

    assert result['status'] == 'completed'
    assert result['data_complete'] is True
    assert result['counts'] == {'saved': 3, 'unavailable': 0, 'failed': 0}
    assert result['session_date'] == '2026-11-23'
    assert result['previous_session_date'] == '2026-11-20'
    assert client.calls == [
        {'index_code': code, 'start_date': '20261120', 'end_date': '20261123'}
        for code in ('SPX', 'COMP', 'SOX')
    ]
    for i, item in enumerate(result['indices'], start=1):
        saved = load_us_index_snapshot(item['snapshot_id'], session_factory=database)
        assert saved.prices.index_code == item['index_code']
        assert saved.prices.price == Decimal('7100.123456789012345678')
        assert saved.as_of == AS_OF
        assert saved.collected_at == AS_OF + timedelta(seconds=i)
        assert saved.source_environment == 'mock'


def test_one_provider_failure_keeps_other_snapshots_without_retry_or_secret_leak(database):
    client = FakeKISClient({'COMP': RuntimeError('credential=not-a-real-secret')})

    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, client=client, session_factory=database,
    ))

    assert result['status'] == 'partial'
    assert result['data_complete'] is False
    assert result['counts'] == {'saved': 2, 'unavailable': 0, 'failed': 1}
    assert len(client.calls) == 3
    assert result['indices'][1] == {
        'index_code': 'COMP', 'status': 'failed', 'stage': 'fetch', 'error_type': 'RuntimeError',
    }
    assert 'not-a-real-secret' not in json.dumps(result)
    for item in (result['indices'][0], result['indices'][2]):
        assert load_us_index_snapshot(item['snapshot_id'], session_factory=database) is not None


@pytest.mark.parametrize('rows', [[], {}, [{'stck_bsop_date': '20261120', 'ovrs_nmix_prpr': '7000'}]])
def test_missing_required_prices_are_completed_without_fabricating_a_snapshot(rows):
    payload = _payload('SPX')
    payload['output2'] = rows
    client = FakeKISClient({'SPX': payload})

    def no_database():
        raise AssertionError('Unavailable prices must not open the database')

    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, index_codes=('SPX',), client=client, session_factory=no_database,
    ))

    assert result['status'] == 'completed'
    assert result['data_complete'] is False
    assert result['counts'] == {'saved': 0, 'unavailable': 1, 'failed': 0}
    assert result['indices'] == [{
        'index_code': 'SPX', 'status': 'unavailable', 'stage': 'validate',
        'reason': 'missing_session_prices',
    }]


def test_retry_can_request_only_failed_indices_and_deduplicates_codes(database):
    client = FakeKISClient()
    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, index_codes=(' comp ', 'COMP'), client=client, session_factory=database,
    ))

    assert client.calls == [{'index_code': 'COMP', 'start_date': '20261120', 'end_date': '20261123'}]
    assert result['counts']['saved'] == 1
    assert result['indices'][0]['index_code'] == 'COMP'


@pytest.mark.parametrize('codes', [[], (), 'SPX', None, [None], ['SPX', 'UNKNOWN']])
def test_bad_selection_is_rejected_before_any_provider_call(codes, database):
    client = FakeKISClient()
    with pytest.raises(ValueError, match='index_codes'):
        asyncio.run(collect_us_index_inputs(
            as_of=AS_OF, index_codes=codes, client=client, session_factory=database,
        ))
    assert client.calls == []


@pytest.mark.parametrize('payload', [
    None, {'rt_cd': '1', 'output2': []},
    {'rt_cd': '0', 'output1': {'stck_shrn_iscd': 'WRONG'}, 'output2': _payload('SPX')['output2']},
    {'rt_cd': '0', 'output2': 'malformed'},
])
def test_bad_provider_data_is_failure_and_does_not_block_the_next_index(payload, database):
    client = FakeKISClient({'SPX': payload})
    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, client=client, session_factory=database,
    ))

    assert result['counts'] == {'saved': 2, 'unavailable': 0, 'failed': 1}
    assert result['indices'][0] == {
        'index_code': 'SPX', 'status': 'failed', 'stage': 'validate', 'error_type': 'ValueError',
    }


@pytest.mark.parametrize('error_type', [KeyError, IndexError, LookupError])
def test_unexpected_lookup_errors_are_technical_failures_not_missing_prices(error_type, database):
    class BrokenProviderHeader(dict):
        def get(self, key, default=None):
            raise error_type('unexpected provider mapping lookup')

    payload = _payload('SPX')
    payload['output1'] = BrokenProviderHeader()
    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, index_codes=('SPX',), client=FakeKISClient({'SPX': payload}),
        session_factory=database,
    ))

    assert result['counts'] == {'saved': 0, 'unavailable': 0, 'failed': 1}
    assert result['indices'][0] == {
        'index_code': 'SPX', 'status': 'failed', 'stage': 'validate',
        'error_type': error_type.__name__,
    }


def test_database_failure_is_not_misclassified_as_missing_provider_prices(database):
    attempts = 0

    @contextmanager
    def intermittent_database():
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            raise KeyError('database credential must not reach the summary')
        with database() as session:
            yield session

    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, client=FakeKISClient(), session_factory=intermittent_database,
    ))

    assert result['counts'] == {'saved': 2, 'unavailable': 0, 'failed': 1}
    assert result['indices'][1] == {
        'index_code': 'COMP', 'status': 'failed', 'stage': 'store', 'error_type': 'KeyError',
    }
    assert 'credential' not in json.dumps(result)
    assert load_us_index_snapshot(result['indices'][2]['snapshot_id'], session_factory=database)


def test_all_fetch_failures_are_failed_without_retries(database):
    client = FakeKISClient({code: KeyError('provider lookup failed') for code in ('SPX', 'COMP', 'SOX')})
    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, client=client, session_factory=database,
    ))

    assert result['status'] == 'failed'
    assert result['counts'] == {'saved': 0, 'unavailable': 0, 'failed': 3}
    assert all(item['stage'] == 'fetch' for item in result['indices'])
    assert len(client.calls) == 3


def test_missing_one_index_keeps_completed_processing_with_incomplete_data(database):
    client = FakeKISClient({'COMP': {'rt_cd': '0', 'output2': []}})
    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, client=client, session_factory=database,
    ))

    assert result['status'] == 'completed'
    assert result['data_complete'] is False
    assert result['counts'] == {'saved': 2, 'unavailable': 1, 'failed': 0}


def test_request_window_does_not_advance_when_collection_crosses_the_next_us_close(database):
    client = FakeKISClient()
    result = asyncio.run(collect_us_index_inputs(
        as_of=datetime(2026, 11, 24, 20, 59, 59, tzinfo=UTC),
        index_codes=('SPX',), client=client, session_factory=database,
        clock=lambda: datetime(2026, 11, 24, 21, 0, 1, tzinfo=UTC),
    ))

    assert client.calls[0]['end_date'] == '20261123'
    saved = load_us_index_snapshot(result['indices'][0]['snapshot_id'], session_factory=database)
    assert saved.prices.session.session_date.isoformat() == '2026-11-23'
    assert saved.collected_at.isoformat() == '2026-11-24T21:00:01+00:00'


@pytest.mark.parametrize('as_of', [None, '2026-11-24', datetime(2026, 11, 24)])
def test_invalid_as_of_never_calls_the_provider(as_of, database):
    client = FakeKISClient()
    with pytest.raises(ValueError, match='timezone-aware'):
        asyncio.run(collect_us_index_inputs(
            as_of=as_of, client=client, session_factory=database,
        ))
    assert client.calls == []


def test_cancellation_propagates_instead_of_collecting_more_indices(database):
    client = FakeKISClient({'SPX': asyncio.CancelledError()})
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(collect_us_index_inputs(
            as_of=AS_OF, client=client, session_factory=database,
        ))
    assert len(client.calls) == 1


def test_default_client_uses_real_quote_domain_and_records_its_provenance(database, monkeypatch):
    client = FakeKISClient()
    client.use_mock = False
    monkeypatch.setattr('app.market.us_index_collector.kis_real_client', client)
    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, index_codes=('SPX',), session_factory=database,
    ))

    saved = load_us_index_snapshot(result['indices'][0]['snapshot_id'], session_factory=database)
    assert result['source_environment'] == saved.source_environment == 'real'
    assert len(client.calls) == 1


def test_manual_cli_runs_the_real_collector_and_outputs_snapshot_ids(database, capsys):
    from app.internal.us_index_collect import main

    client = FakeKISClient()
    exit_code = main(
        ['--as-of', '2026-11-24T16:00:00+00:00'],
        collector=partial(collect_us_index_inputs, client=client, session_factory=database),
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert result['counts'] == {'saved': 3, 'unavailable': 0, 'failed': 0}
    for item in result['indices']:
        assert load_us_index_snapshot(item['snapshot_id'], session_factory=database)


@pytest.mark.parametrize(('response', 'expected_exit', 'state'), [
    ({'rt_cd': '0', 'output2': []}, 0, 'unavailable'),
    (RuntimeError('provider error'), 1, 'failed'),
])
def test_cli_exit_code_separates_unavailable_data_from_technical_failure(
    response, expected_exit, state, database, capsys,
):
    from app.internal.us_index_collect import main

    client = FakeKISClient({'COMP': response})
    exit_code = main(
        ['--indices', ' comp ', '--as-of', '2026-11-24T11:00:00-05:00'],
        collector=partial(collect_us_index_inputs, client=client, session_factory=database),
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == expected_exit
    assert result['indices'][0]['status'] == state
    assert result['as_of'] == '2026-11-24T16:00:00+00:00'
    assert [call['index_code'] for call in client.calls] == ['COMP']


@pytest.mark.parametrize('arguments', [
    ['--indices', ''], ['--indices', 'SPX,WRONG'], ['--indices', 'SPX,'],
    ['--as-of', '2026-11-24T16:00:00'], ['--as-of', 'invalid'],
])
def test_cli_bad_arguments_exit_before_requesting_quotes(arguments, database, capsys):
    from app.internal.us_index_collect import main

    client = FakeKISClient()
    with pytest.raises(SystemExit) as exc:
        main(arguments, collector=partial(
            collect_us_index_inputs, client=client, session_factory=database,
        ))
    assert exc.value.code == 2
    assert client.calls == []
    assert capsys.readouterr().out == ''


def test_cli_help_does_not_contact_the_provider(database, capsys):
    from app.internal.us_index_collect import main

    client = FakeKISClient()
    with pytest.raises(SystemExit) as exc:
        main(['--help'], collector=partial(
            collect_us_index_inputs, client=client, session_factory=database,
        ))
    assert exc.value.code == 0
    assert '--indices' in capsys.readouterr().out
    assert client.calls == []


def test_database_work_does_not_block_the_event_loop_thread(database):
    caller_thread = get_ident()

    @contextmanager
    def background_database():
        assert get_ident() != caller_thread
        with database() as session:
            yield session

    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, index_codes=('SPX',), client=FakeKISClient(),
        session_factory=background_database,
    ))
    assert result['counts']['saved'] == 1
