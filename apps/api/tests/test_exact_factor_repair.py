from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from app.db import session_scope
from app.db_models import (
    DailyAdvancedValueFactorResult,
    DailyValueFactorResult,
    Symbol,
)
from app.eod import exact_factor_repair
from app.eod.market_membership import market_membership_sha256
from app.eod.exact_factor_repair import (
    FACTOR_RESULT_MODELS,
    ExactFactorRepairService,
    prune_stale_factor_rows,
    verify_exact_factor_rows,
)


class _Index:
    def __init__(self, values: list[str]) -> None:
        self._values = values

    def tolist(self) -> list[str]:
        return list(self._values)


class _Frame:
    empty = False

    def __init__(self, values: list[str]) -> None:
        self.index = _Index(values)


def _membership_by_market() -> dict[str, list[str]]:
    return {
        'KOSPI': [
            *[f'{value:06d}' for value in range(1, 701)],
            '00104K',
        ],
        'KOSDAQ': [f'{value:06d}' for value in range(100_000, 101_200)],
    }


@contextmanager
def _acquired_lock(**_kwargs):
    yield True


def _service(
    *,
    apply_results: list[tuple[str, dict]] | None = None,
    prune_calls: list[dict] | None = None,
    completion_calls: list[dict] | None = None,
):
    membership = _membership_by_market()
    exact_symbols = sorted(
        symbol
        for symbol in membership['KOSPI'] + membership['KOSDAQ']
        if symbol.isdigit() and len(symbol) == 6
    )
    calls = apply_results if apply_results is not None else []

    def stage_runner(stage_id: str):
        def run(**kwargs):
            calls.append((stage_id, kwargs))
            return {
                'results_persisted': len(exact_symbols),
                'results_failed': 0,
                'missing_symbols': [],
            }

        return run

    observed_prune_calls = prune_calls if prune_calls is not None else []

    def verify(**_kwargs):
        return {
            stage_id: {
                'exact_row_count': len(exact_symbols),
                'fresh_exact_row_count': len(exact_symbols),
            }
            for stage_id in FACTOR_RESULT_MODELS
        }

    def prune(**kwargs):
        observed_prune_calls.append(kwargs)
        return {stage_id: 7 for stage_id in FACTOR_RESULT_MODELS}

    observed_completion_calls = (
        completion_calls if completion_calls is not None else []
    )

    def record_completion(**kwargs):
        observed_completion_calls.append(kwargs)
        return {'updated_batch_count': 0}

    service = ExactFactorRepairService(
        trade_date_resolver=lambda **_kwargs: '2026-09-23',
        market_ohlcv_loader=lambda _date, market: _Frame(membership[market]),
        price_bar_symbols_loader=lambda _date: [*exact_symbols, '999999'],
        stale_details_loader=lambda **_kwargs: {
            stage_id: {'count': 7, 'symbol_preview': ['999999']}
            for stage_id in FACTOR_RESULT_MODELS
        },
        applied_scope_verifier=verify,
        stale_pruner=prune,
        completion_recorder=record_completion,
        pending_batch_loader=lambda: [],
        lock_factory=_acquired_lock,
        stage_runners={
            stage_id: stage_runner(stage_id)
            for stage_id in FACTOR_RESULT_MODELS
        },
    )
    return service, exact_symbols, calls


def _pinned_membership(exact_symbols: list[str]) -> dict[str, object]:
    provider_membership = [*exact_symbols, '00104K']
    return {
        'expected_provider_membership_count': len(provider_membership),
        'expected_membership_sha256': market_membership_sha256(
            provider_membership
        ),
    }


def _stored_eod_membership_run(
    membership_by_market: dict[str, list[str]],
    *,
    run_id: int = 321,
) -> dict[str, Any]:
    membership = {
        'schema_version': 'pykrx_market_membership_v1',
        'membership_basis': 'end_date_snapshot',
        'symbol_count': sum(map(len, membership_by_market.values())),
        'markets': {
            market: {
                'symbols': symbols,
                'symbol_count': len(symbols),
                'membership_sha256': market_membership_sha256(symbols),
            }
            for market, symbols in membership_by_market.items()
        },
    }
    return {
        'id': run_id,
        'market': 'KRX',
        'trade_date': '2026-09-23',
        'source': 'pykrx',
        'status': 'completed',
        'run_metadata': {
            'universe_snapshot': {
                'complete': True,
                'source': 'pykrx',
                'markets': ['KOSPI', 'KOSDAQ'],
                'latest_trade_date': '2026-09-23',
                'universe_source': 'pykrx_market_ohlcv',
                'membership_basis': 'end_date_snapshot',
                'symbol_count': membership['symbol_count'],
                'market_status': {
                    'KOSPI': 'pykrx_market_ohlcv',
                    'KOSDAQ': 'pykrx_market_ohlcv',
                },
                'market_errors': [],
                'unavailable_markets': [],
                'market_membership': membership,
            },
        },
    }


def _refresh_stored_membership_evidence(run: dict[str, Any]) -> None:
    snapshot = run['run_metadata']['universe_snapshot']
    membership = snapshot['market_membership']
    total = 0
    for entry in membership['markets'].values():
        symbols = entry['symbols']
        entry['symbol_count'] = len(symbols)
        entry['membership_sha256'] = market_membership_sha256(symbols)
        total += len(symbols)
    membership['symbol_count'] = total
    snapshot['symbol_count'] = total


def test_live_provider_failure_uses_latest_strict_same_date_eod_membership() -> None:
    membership = _membership_by_market()
    loader_calls: list[dict] = []

    def provider_unavailable(_date: str, _market: str):
        raise RuntimeError('provider unavailable')

    def load_latest_run(**kwargs):
        loader_calls.append(kwargs)
        return [_stored_eod_membership_run(membership)]

    service, exact_symbols, calls = _service()
    service.market_ohlcv_loader = provider_unavailable
    service.stored_eod_run_loader = load_latest_run

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'ready'
    assert result['exact_membership_count'] == len(exact_symbols)
    assert result['membership_evidence_source'] == 'stored_eod_run_metadata'
    assert result['membership_evidence_run_id'] == 321
    assert result['membership_evidence_run_source'] == 'pykrx'
    assert loader_calls == [
        {
            'market': 'KRX',
            'trade_date': '2026-09-23',
            'source': 'pykrx',
            'limit': 20,
        }
    ]
    assert calls == []


def test_trade_date_provider_failure_uses_strict_same_date_eod_membership() -> None:
    membership = _membership_by_market()
    market_loader_called = False

    def resolver_unavailable(**_kwargs):
        raise RuntimeError('calendar provider unavailable')

    def should_not_load_market(_date: str, _market: str):
        nonlocal market_loader_called
        market_loader_called = True
        raise AssertionError('market provider must not run after calendar failure')

    service, exact_symbols, _calls = _service()
    service.trade_date_resolver = resolver_unavailable
    service.market_ohlcv_loader = should_not_load_market
    service.stored_eod_run_loader = lambda **_kwargs: [
        _stored_eod_membership_run(membership)
    ]

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'ready'
    assert result['exact_membership_count'] == len(exact_symbols)
    assert result['membership_evidence_source'] == 'stored_eod_run_metadata'
    assert result['membership_evidence_run_id'] == 321
    assert market_loader_called is False


def test_stored_fallback_skips_newer_unsuitable_retry_for_latest_valid_run() -> None:
    membership = _membership_by_market()
    loader_calls: list[dict] = []
    unsuitable_retry = {
        'id': 322,
        'market': 'KRX',
        'trade_date': '2026-09-23',
        'source': 'pykrx',
        'status': 'failed',
        'run_metadata': {},
    }
    valid_run = _stored_eod_membership_run(membership, run_id=321)

    def load_recent_runs(**kwargs):
        loader_calls.append(kwargs)
        return [unsuitable_retry, valid_run]

    service, _exact_symbols, _calls = _service()
    service.market_ohlcv_loader = lambda *_args: (_ for _ in ()).throw(
        RuntimeError('provider unavailable')
    )
    service.stored_eod_run_loader = load_recent_runs

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'ready'
    assert result['membership_evidence_run_id'] == 321
    assert loader_calls == [
        {
            'market': 'KRX',
            'trade_date': '2026-09-23',
            'source': 'pykrx',
            'limit': 20,
        }
    ]


def test_live_membership_is_preferred_without_reading_stored_runs() -> None:
    service, _exact_symbols, _calls = _service()
    service.stored_eod_run_loader = lambda **_kwargs: (_ for _ in ()).throw(
        AssertionError('stored fallback must not run when live PyKRX succeeds')
    )

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'ready'
    assert result['membership_evidence_source'] == 'live_pykrx_market_ohlcv'
    assert result['membership_evidence_run_id'] is None
    assert result['membership_evidence_run_source'] is None


@pytest.mark.parametrize(
    ('field_path', 'invalid_value'),
    [
        (('market',), 'NASDAQ'),
        (('status',), 'running'),
        (('run_metadata', 'universe_snapshot', 'complete'), False),
        (
            ('run_metadata', 'universe_snapshot', 'latest_trade_date'),
            '2026-09-22',
        ),
        (
            ('run_metadata', 'universe_snapshot', 'universe_source'),
            'pykrx_market_ohlcv_with_master_fallback',
        ),
        (
            (
                'run_metadata',
                'universe_snapshot',
                'market_status',
                'KOSPI',
            ),
            'kis_master_fallback',
        ),
        (
            ('run_metadata', 'universe_snapshot', 'market_errors'),
            [{'market': 'KOSPI', 'error': 'provider failed'}],
        ),
        (
            ('run_metadata', 'universe_snapshot', 'unavailable_markets'),
            ['KOSPI'],
        ),
        (
            (
                'run_metadata',
                'universe_snapshot',
                'market_membership',
                'markets',
                'KOSPI',
                'membership_sha256',
            ),
            '0' * 64,
        ),
        (
            (
                'run_metadata',
                'universe_snapshot',
                'market_membership',
                'markets',
                'KOSPI',
                'symbol_count',
            ),
            1,
        ),
    ],
)
def test_stored_fallback_rejects_non_strict_snapshot_evidence(
    field_path: tuple[str, ...],
    invalid_value: object,
) -> None:
    run = deepcopy(_stored_eod_membership_run(_membership_by_market()))
    target = run
    for field in field_path[:-1]:
        target = target[field]
    target[field_path[-1]] = invalid_value
    service, _exact_symbols, _calls = _service()
    service.market_ohlcv_loader = lambda *_args: (_ for _ in ()).throw(
        RuntimeError('provider unavailable')
    )
    service.stored_eod_run_loader = lambda **_kwargs: [run]

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'invalid_exact_market_membership'


def test_stored_fallback_rejects_cross_market_duplicate_membership() -> None:
    run = _stored_eod_membership_run(_membership_by_market())
    markets = run['run_metadata']['universe_snapshot']['market_membership'][
        'markets'
    ]
    markets['KOSDAQ']['symbols'].append(markets['KOSPI']['symbols'][0])
    _refresh_stored_membership_evidence(run)
    service, _exact_symbols, _calls = _service()
    service.market_ohlcv_loader = lambda *_args: (_ for _ in ()).throw(
        RuntimeError('provider unavailable')
    )
    service.stored_eod_run_loader = lambda **_kwargs: [run]

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'invalid_exact_market_membership'


def test_stored_fallback_rejects_market_below_numeric_safety_floor() -> None:
    run = _stored_eod_membership_run(_membership_by_market())
    markets = run['run_metadata']['universe_snapshot']['market_membership'][
        'markets'
    ]
    markets['KOSPI']['symbols'] = markets['KOSPI']['symbols'][:699]
    _refresh_stored_membership_evidence(run)
    service, _exact_symbols, _calls = _service()
    service.market_ohlcv_loader = lambda *_args: (_ for _ in ()).throw(
        RuntimeError('provider unavailable')
    )
    service.stored_eod_run_loader = lambda **_kwargs: [run]

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'invalid_exact_market_membership'


def test_stored_fallback_accepts_completed_with_errors_when_proof_is_exact() -> None:
    run = _stored_eod_membership_run(_membership_by_market())
    run['status'] = 'completed_with_errors'
    service, _exact_symbols, _calls = _service()
    service.market_ohlcv_loader = lambda *_args: (_ for _ in ()).throw(
        RuntimeError('provider unavailable')
    )
    service.stored_eod_run_loader = lambda **_kwargs: [run]

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'ready'
    assert result['membership_evidence_run_id'] == 321


def test_apply_persists_stored_membership_provenance() -> None:
    membership = _membership_by_market()
    completion_calls: list[dict] = []
    service, exact_symbols, _calls = _service(
        completion_calls=completion_calls,
    )
    service.market_ohlcv_loader = lambda *_args: (_ for _ in ()).throw(
        RuntimeError('provider unavailable')
    )
    service.stored_eod_run_loader = lambda **_kwargs: [
        _stored_eod_membership_run(membership, run_id=548)
    ]

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=True,
        **_pinned_membership(exact_symbols),
    )

    assert result['status'] == 'completed'
    assert len(completion_calls) == 1
    assert completion_calls[0]['membership_evidence_source'] == (
        'stored_eod_run_metadata'
    )
    assert completion_calls[0]['membership_evidence_run_id'] == 548
    assert completion_calls[0]['membership_evidence_run_source'] == 'pykrx'


def test_dry_run_discloses_exact_scope_and_never_runs_factor_stage() -> None:
    service, exact_symbols, calls = _service()

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'ready'
    assert result['applied'] is False
    assert result['exact_membership_count'] == len(exact_symbols)
    assert result['stale_outside_membership_price_bar_count'] == 1
    assert result['exact_provider_membership_count'] == len(exact_symbols) + 1
    assert result['provider_excluded_from_ranking_count'] == 1
    assert result['provider_excluded_from_ranking_counts_by_market'] == {
        'KOSPI': 1,
        'KOSDAQ': 0,
    }
    assert result['provider_excluded_from_ranking_preview'] == [
        {'market': 'KOSPI', 'code': '00104K'}
    ]
    assert result['stale_outside_membership_counts'] == {
        stage_id: 7 for stage_id in FACTOR_RESULT_MODELS
    }
    assert result['stale_outside_membership'] == {
        stage_id: {'count': 7, 'symbol_preview': ['999999']}
        for stage_id in FACTOR_RESULT_MODELS
    }
    assert len(result['membership_sha256']) == 64
    assert result['stages'] == list(FACTOR_RESULT_MODELS)
    assert calls == []


def test_apply_passes_same_exact_membership_to_every_stage_in_order() -> None:
    calls: list[tuple[str, dict]] = []
    service, exact_symbols, _ = _service(apply_results=calls)

    result = service.repair_for_market_date(
        market_date=date(2026, 9, 23),
        apply=True,
        **_pinned_membership(exact_symbols),
    )

    assert result['status'] == 'completed'
    assert result['applied'] is True
    assert [stage_id for stage_id, _kwargs in calls] == list(
        FACTOR_RESULT_MODELS
    )
    for _stage_id, kwargs in calls:
        assert kwargs['symbols'] == exact_symbols
        assert kwargs['ranking_symbols'] == exact_symbols


def test_apply_requires_reviewed_membership_count_and_hash() -> None:
    service, exact_symbols, calls = _service()

    unpinned = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=True,
    )
    mismatched = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=True,
        expected_provider_membership_count=len(exact_symbols),
        expected_membership_sha256='0' * 64,
    )

    assert unpinned['status'] == 'blocked'
    assert unpinned['reason'] == 'expected_membership_evidence_required'
    assert mismatched['status'] == 'blocked'
    assert mismatched['reason'] == 'expected_membership_evidence_mismatch'
    assert calls == []


def test_apply_rejects_provider_membership_that_differs_from_pending_frozen_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_calls: list[tuple[str, dict]] = []
    prune_calls: list[dict] = []
    completion_calls: list[dict] = []
    service, exact_symbols, _ = _service(
        apply_results=stage_calls,
        prune_calls=prune_calls,
        completion_calls=completion_calls,
    )
    service.pending_batch_loader = lambda: [object()]
    monkeypatch.setattr(
        exact_factor_repair,
        'resolve_pending_frozen_price_dependent_membership',
        lambda **_kwargs: tuple(exact_symbols[:-1]),
    )

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=True,
        **_pinned_membership(exact_symbols),
    )

    assert result['status'] == 'blocked'
    assert result['reason'] == (
        'provider_membership_mismatch_pending_frozen_scope'
    )
    assert result['provider_ranking_membership_count'] == len(exact_symbols)
    assert result['frozen_membership_count'] == len(exact_symbols) - 1
    assert stage_calls == []
    assert prune_calls == []
    assert completion_calls == []


def test_apply_stops_partial_when_stage_does_not_persist_exact_membership() -> None:
    service, exact_symbols, _calls = _service()
    stage_runners = dict(service.stage_runners)
    stage_runners['value_factors'] = lambda **_kwargs: {
        'results_persisted': len(exact_symbols) - 1,
        'results_failed': 0,
        'missing_symbols': [],
    }
    service.stage_runners = stage_runners

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=True,
        **_pinned_membership(exact_symbols),
    )

    assert result['status'] == 'partial'
    assert result['reason'] == 'factor_stage_incomplete'
    assert result['failed_stage'] == 'value_factors'
    assert 'low_risk_factors' not in result['stage_results']


def test_prune_requires_apply() -> None:
    service, _exact_symbols, calls = _service()

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
        prune_stale=True,
    )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'prune_stale_requires_apply'
    assert calls == []


def test_prune_runs_only_after_all_exact_stages_succeed() -> None:
    stage_calls: list[tuple[str, dict]] = []
    prune_calls: list[dict] = []
    service, exact_symbols, _ = _service(
        apply_results=stage_calls,
        prune_calls=prune_calls,
    )

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=True,
        prune_stale=True,
        **_pinned_membership(exact_symbols),
    )

    assert result['status'] == 'completed'
    assert len(stage_calls) == len(FACTOR_RESULT_MODELS)
    assert len(prune_calls) == 1
    assert prune_calls[0]['exact_symbols'] == exact_symbols
    assert prune_calls[0]['protected_symbols'] == ['00104K']
    assert prune_calls[0]['expected_stale_counts'] == {
        stage_id: 7 for stage_id in FACTOR_RESULT_MODELS
    }
    assert result['pruned_stale_counts'] == {
        stage_id: 7 for stage_id in FACTOR_RESULT_MODELS
    }


def test_prune_is_not_called_after_partial_factor_stage() -> None:
    prune_calls: list[dict] = []
    service, exact_symbols, _calls = _service(prune_calls=prune_calls)
    service.stage_runners['piotroski_filter'] = lambda **_kwargs: {
        'results_persisted': len(exact_symbols) - 1,
        'results_failed': 0,
        'missing_symbols': [],
    }

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=True,
        prune_stale=True,
        **_pinned_membership(exact_symbols),
    )

    assert result['status'] == 'partial'
    assert prune_calls == []


def test_apply_rejects_success_counters_when_database_verification_fails() -> None:
    service, _exact_symbols, _calls = _service()
    service.applied_scope_verifier = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError('rows were not written')
    )

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=True,
        **_pinned_membership(_exact_symbols),
    )

    assert result['status'] == 'partial'
    assert result['applied'] is False
    assert result['reason'] == 'factor_persistence_verification_failed'
    assert result['error_type'] == 'RuntimeError'


def test_blocks_when_resolved_date_is_not_requested_session() -> None:
    service, _exact_symbols, _calls = _service()
    service.trade_date_resolver = lambda **_kwargs: '2026-09-22'

    result = service.repair_for_market_date(
        market_date='2026-09-23',
        apply=False,
    )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'not_an_exact_trading_session'


def test_cli_returns_nonzero_for_blocked_result(monkeypatch, capsys) -> None:
    observed: dict = {}

    def blocked(**kwargs):
        observed.update(kwargs)
        return {'status': 'blocked', 'reason': 'test'}

    monkeypatch.setattr(
        exact_factor_repair.exact_factor_repair_service,
        'repair_for_market_date',
        blocked,
    )

    exit_code = exact_factor_repair.main(
        ['--market-date', '2026-09-23', '--apply', '--prune-stale']
    )

    assert exit_code == 1
    assert observed == {
        'market_date': '2026-09-23',
        'apply': True,
        'prune_stale': True,
        'expected_provider_membership_count': None,
        'expected_membership_sha256': None,
    }
    assert json.loads(capsys.readouterr().out)['status'] == 'blocked'


def _unique_numeric_symbol() -> str:
    return f'{900_000 + (int(uuid4().hex[:8], 16) % 99_999):06d}'


def test_stale_scope_query_is_factor_date_index_driven() -> None:
    query = exact_factor_repair._build_factor_symbols_for_date_query(
        DailyAdvancedValueFactorResult,
        market_date=date(2026, 9, 23),
    )
    compiled = str(query.compile(compile_kwargs={'literal_binds': True}))

    assert 'FROM daily_advanced_value_factor_results JOIN symbols' in compiled
    assert 'symbols.id = daily_advanced_value_factor_results.symbol_id' in compiled
    assert (
        "daily_advanced_value_factor_results.market_date = '2026-09-23'"
        in compiled
    )
    assert 'symbols.market' not in compiled
    assert 'NOT IN' not in compiled


def _unique_alphanumeric_symbol() -> str:
    return f'{int(uuid4().hex[:8], 16) % 100_000:05d}K'


def _insert_value_factor_rows(
    *,
    market_date: date,
    exact_symbol: str,
    stale_symbols: list[str],
    updated_at: datetime,
) -> None:
    with session_scope() as session:
        symbols = [exact_symbol, *stale_symbols]
        symbol_rows = []
        for symbol in symbols:
            row = Symbol(
                symbol=symbol,
                name_ko=symbol,
                market='KOSPI',
                source='test',
                is_active=True,
            )
            session.add(row)
            symbol_rows.append(row)
        session.flush()
        for row in symbol_rows:
            session.add(
                DailyValueFactorResult(
                    symbol_id=row.id,
                    market_date=market_date,
                    source='test',
                    per_status='available',
                    value_status='scored',
                    factor_payload={},
                    captured_at=updated_at,
                    updated_at=updated_at,
                )
            )


def _cleanup_symbols(symbols: list[str]) -> None:
    with session_scope(statement_timeout_ms=120_000) as session:
        symbol_ids = list(
            session.scalars(select(Symbol.id).where(Symbol.symbol.in_(symbols)))
        )
        if symbol_ids:
            session.execute(
                delete(DailyValueFactorResult).where(
                    DailyValueFactorResult.symbol_id.in_(symbol_ids)
                )
            )
        session.execute(delete(Symbol).where(Symbol.symbol.in_(symbols)))


def test_stale_scope_loader_includes_all_date_local_rows_outside_membership(
    monkeypatch,
) -> None:
    market_date = date(2099, 1, 9)
    exact_symbol = _unique_numeric_symbol()
    stale_symbol = _unique_numeric_symbol()
    while stale_symbol == exact_symbol:
        stale_symbol = _unique_numeric_symbol()
    non_krx_symbol = _unique_numeric_symbol()
    while non_krx_symbol in {exact_symbol, stale_symbol}:
        non_krx_symbol = _unique_numeric_symbol()
    symbols = [exact_symbol, stale_symbol, non_krx_symbol]
    repair_started_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    _insert_value_factor_rows(
        market_date=market_date,
        exact_symbol=exact_symbol,
        stale_symbols=[stale_symbol],
        updated_at=datetime.now(timezone.utc),
    )
    with session_scope() as session:
        non_krx_row = Symbol(
            symbol=non_krx_symbol,
            name_ko=non_krx_symbol,
            market='NASDAQ',
            source='test',
            is_active=True,
        )
        session.add(non_krx_row)
        session.flush()
        session.add(
            DailyValueFactorResult(
                symbol_id=non_krx_row.id,
                market_date=market_date,
                source='test',
                per_status='available',
                value_status='scored',
                factor_payload={},
                captured_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )
    monkeypatch.setattr(
        exact_factor_repair,
        'FACTOR_RESULT_MODELS',
        {'value_factors': DailyValueFactorResult},
    )
    try:
        result = exact_factor_repair.load_stale_outside_membership_details(
            market_date=market_date,
            exact_symbols=[exact_symbol],
        )

        assert result == {
            'value_factors': {
                'count': 2,
                'symbol_preview': sorted([stale_symbol, non_krx_symbol]),
            }
        }
        assert prune_stale_factor_rows(
            market_date=market_date,
            exact_symbols=[exact_symbol],
            repair_started_at=repair_started_at,
            expected_stale_counts={
                stage_id: details['count']
                for stage_id, details in result.items()
            },
        ) == {'value_factors': 2}
    finally:
        _cleanup_symbols(symbols)


def test_stale_scope_loader_filters_large_membership_in_memory(
    monkeypatch,
) -> None:
    market_date = date(2026, 9, 23)
    exact_symbols = [f'{value:06d}' for value in range(1, 2_768)]
    protected_symbol = '00104K'
    stale_symbol = '999999'
    observed_market_dates: list[date] = []

    class _Session:
        @staticmethod
        def scalars(_statement):
            return [exact_symbols[0], protected_symbol, stale_symbol]

    @contextmanager
    def _session_scope(**kwargs):
        assert kwargs == {
            'statement_timeout_ms': (
                exact_factor_repair.EXACT_FACTOR_REPAIR_STATEMENT_TIMEOUT_MS
            )
        }
        yield _Session()

    def _query_builder(
        _model,
        *,
        market_date: date,
    ):
        assert market_date == date(2026, 9, 23)
        observed_market_dates.append(market_date)
        return object()

    monkeypatch.setattr(
        exact_factor_repair,
        'FACTOR_RESULT_MODELS',
        {'value_factors': DailyValueFactorResult},
    )
    monkeypatch.setattr(exact_factor_repair, 'session_scope', _session_scope)
    monkeypatch.setattr(
        exact_factor_repair,
        '_build_factor_symbols_for_date_query',
        _query_builder,
    )

    result = exact_factor_repair.load_stale_outside_membership_details(
        market_date=market_date,
        exact_symbols=exact_symbols,
        protected_symbols=[protected_symbol],
    )

    assert observed_market_dates == [market_date]
    assert result == {
        'value_factors': {
            'count': 1,
            'symbol_preview': [stale_symbol],
        }
    }


def test_database_verifier_rejects_same_count_wrong_symbol(monkeypatch) -> None:
    market_date = date(2099, 1, 10)
    expected_symbol = _unique_numeric_symbol()
    wrong_symbol = _unique_numeric_symbol()
    while wrong_symbol == expected_symbol:
        wrong_symbol = _unique_numeric_symbol()
    _insert_value_factor_rows(
        market_date=market_date,
        exact_symbol=wrong_symbol,
        stale_symbols=[],
        updated_at=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        exact_factor_repair,
        'FACTOR_RESULT_MODELS',
        {'value_factors': DailyValueFactorResult},
    )
    try:
        with pytest.raises(RuntimeError, match='persistence verification failed'):
            verify_exact_factor_rows(
                market_date=market_date,
                exact_symbols=[expected_symbol],
                repair_started_at=datetime.now(timezone.utc) - timedelta(seconds=1),
            )
    finally:
        _cleanup_symbols([wrong_symbol])


def test_prune_deletes_only_date_local_outside_membership_rows_atomically(
    monkeypatch,
) -> None:
    market_date = date(2099, 1, 11)
    exact_symbol = _unique_numeric_symbol()
    stale_symbol = _unique_numeric_symbol()
    while stale_symbol == exact_symbol:
        stale_symbol = _unique_numeric_symbol()
    symbols = [exact_symbol, stale_symbol]
    repair_started_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    _insert_value_factor_rows(
        market_date=market_date,
        exact_symbol=exact_symbol,
        stale_symbols=[stale_symbol],
        updated_at=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        exact_factor_repair,
        'FACTOR_RESULT_MODELS',
        {'value_factors': DailyValueFactorResult},
    )
    try:
        result = prune_stale_factor_rows(
            market_date=market_date,
            exact_symbols=[exact_symbol],
            repair_started_at=repair_started_at,
            expected_stale_counts={'value_factors': 1},
        )

        assert result == {'value_factors': 1}
        with session_scope() as session:
            remaining = list(
                session.scalars(
                    select(Symbol.symbol)
                    .join(
                        DailyValueFactorResult,
                        DailyValueFactorResult.symbol_id == Symbol.id,
                    )
                    .where(DailyValueFactorResult.market_date == market_date)
                )
            )
        assert remaining == [exact_symbol]
    finally:
        _cleanup_symbols(symbols)


def test_prune_retains_provider_member_excluded_from_ranking(monkeypatch) -> None:
    market_date = date(2099, 1, 14)
    exact_symbol = _unique_numeric_symbol()
    protected_symbol = _unique_alphanumeric_symbol()
    stale_symbol = _unique_numeric_symbol()
    while stale_symbol == exact_symbol:
        stale_symbol = _unique_numeric_symbol()
    symbols = [exact_symbol, protected_symbol, stale_symbol]
    repair_started_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    _insert_value_factor_rows(
        market_date=market_date,
        exact_symbol=exact_symbol,
        stale_symbols=[protected_symbol, stale_symbol],
        updated_at=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        exact_factor_repair,
        'FACTOR_RESULT_MODELS',
        {'value_factors': DailyValueFactorResult},
    )
    try:
        result = prune_stale_factor_rows(
            market_date=market_date,
            exact_symbols=[exact_symbol],
            protected_symbols=[protected_symbol],
            repair_started_at=repair_started_at,
            expected_stale_counts={'value_factors': 1},
        )

        assert result == {'value_factors': 1}
        with session_scope() as session:
            remaining = set(
                session.scalars(
                    select(Symbol.symbol)
                    .join(
                        DailyValueFactorResult,
                        DailyValueFactorResult.symbol_id == Symbol.id,
                    )
                    .where(DailyValueFactorResult.market_date == market_date)
                )
            )
        assert remaining == {exact_symbol, protected_symbol}
    finally:
        _cleanup_symbols(symbols)


def test_prune_rolls_back_when_expected_stale_count_changed(monkeypatch) -> None:
    market_date = date(2099, 1, 12)
    exact_symbol = _unique_numeric_symbol()
    stale_symbols = [_unique_numeric_symbol(), _unique_numeric_symbol()]
    while len({exact_symbol, *stale_symbols}) != 3:
        stale_symbols = [_unique_numeric_symbol(), _unique_numeric_symbol()]
    symbols = [exact_symbol, *stale_symbols]
    repair_started_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    _insert_value_factor_rows(
        market_date=market_date,
        exact_symbol=exact_symbol,
        stale_symbols=stale_symbols,
        updated_at=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        exact_factor_repair,
        'FACTOR_RESULT_MODELS',
        {'value_factors': DailyValueFactorResult},
    )
    try:
        with pytest.raises(RuntimeError, match='stale count changed'):
            prune_stale_factor_rows(
                market_date=market_date,
                exact_symbols=[exact_symbol],
                repair_started_at=repair_started_at,
                expected_stale_counts={'value_factors': 1},
            )

        with session_scope() as session:
            remaining_count = len(
                list(
                    session.scalars(
                        select(DailyValueFactorResult.id).where(
                            DailyValueFactorResult.market_date == market_date
                        )
                    )
                )
            )
        assert remaining_count == 3
    finally:
        _cleanup_symbols(symbols)


def test_prune_refuses_to_delete_when_exact_row_was_not_freshly_rebuilt(
    monkeypatch,
) -> None:
    market_date = date(2099, 1, 13)
    exact_symbol = _unique_numeric_symbol()
    stale_symbol = _unique_numeric_symbol()
    while stale_symbol == exact_symbol:
        stale_symbol = _unique_numeric_symbol()
    symbols = [exact_symbol, stale_symbol]
    repair_started_at = datetime.now(timezone.utc)
    _insert_value_factor_rows(
        market_date=market_date,
        exact_symbol=exact_symbol,
        stale_symbols=[stale_symbol],
        updated_at=repair_started_at - timedelta(minutes=1),
    )
    monkeypatch.setattr(
        exact_factor_repair,
        'FACTOR_RESULT_MODELS',
        {'value_factors': DailyValueFactorResult},
    )
    try:
        with pytest.raises(RuntimeError, match='not fully fresh'):
            prune_stale_factor_rows(
                market_date=market_date,
                exact_symbols=[exact_symbol],
                repair_started_at=repair_started_at,
                expected_stale_counts={'value_factors': 1},
            )

        with session_scope() as session:
            remaining_count = len(
                list(
                    session.scalars(
                        select(DailyValueFactorResult.id).where(
                            DailyValueFactorResult.market_date == market_date
                        )
                    )
                )
            )
        assert remaining_count == 2
    finally:
        _cleanup_symbols(symbols)
