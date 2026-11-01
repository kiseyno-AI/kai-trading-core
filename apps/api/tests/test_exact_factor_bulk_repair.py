from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import json
from unittest.mock import MagicMock, patch

import pytest

from app.eod.derived_stage_repair import MANIFEST_SCHEMA_VERSION
from app.eod.exact_factor_bulk_repair import (
    CHECKPOINT_SCHEMA_VERSION,
    FACTOR_PRICE_HISTORY_ROWS,
    FACTOR_REPAIR_IMPLEMENTATION_CONTRACT,
    FACTOR_SOURCE_COMPONENTS,
    ExactFactorBulkRepair,
    exact_factor_bulk_guard,
    _fingerprint_price_history,
    _load_factor_source_fingerprints,
)
from app.eod.exact_factor_repair import (
    FACTOR_RESULT_MODELS,
    MEMBERSHIP_POLICY_STORED_ONLY,
)
from app.eod.market_membership import market_membership_sha256
from app.internal.exact_repair_manifest import canonical_json_sha256


SYMBOLS = ['000660', '005930']
MARKET_DATE = '2026-10-28'
INPUT_SHA256 = 'a' * 64
PENDING_PROOF_SHA256 = 'b' * 64
PENDING_PROOF_BATCH_IDS = ['missing-bar-authority']
PROTECTED_PROVIDER_CODE = '00104K'


def _manifest(*, symbols=None):
    exact = list(symbols or SYMBOLS)
    return {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'date_count': 1,
        'dates': [
            {
                'market_date': MARKET_DATE,
                'symbol_count': len(exact),
                'membership_sha256': market_membership_sha256(exact),
                'symbols': exact,
            }
        ],
    }


def _dry_result(*, provider_hash=None):
    return {
        'market_date': MARKET_DATE,
        'mode': 'dry_run',
        'status': 'ready',
        'membership_policy': MEMBERSHIP_POLICY_STORED_ONLY,
        'membership_evidence_source': 'stored_eod_run_metadata',
        'membership_evidence_run_id': 77,
        'membership_evidence_run_source': 'pykrx',
        'membership_evidence_sha256': None,
        'membership_evidence_repair_batch_ids': [],
        'pending_frozen_membership': True,
        'exact_membership_count': len(SYMBOLS),
        'ranking_membership_sha256': market_membership_sha256(SYMBOLS),
        'exact_provider_membership_count': len(SYMBOLS),
        'provider_excluded_from_ranking_codes': [],
        'membership_sha256': (
            provider_hash or market_membership_sha256(SYMBOLS)
        ),
    }


def _fingerprints(*, suffix='v1'):
    return {
        stage_id: {
            'row_count': len(SYMBOLS),
            'output_sha256': f'{stage_id}-{suffix}',
        }
        for stage_id in FACTOR_RESULT_MODELS
    }


def _source_fingerprints(*, suffix='v1'):
    result = {
        component: {
            'row_count': len(SYMBOLS),
            'source_sha256': canonical_json_sha256(
                {'component': component, 'suffix': suffix}
            ),
        }
        for component in FACTOR_SOURCE_COMPONENTS
    }
    result['price_history']['history_row_limit'] = FACTOR_PRICE_HISTORY_ROWS
    result['symbol_metadata'].update(
        {
            'scope_symbol_count': len(SYMBOLS),
            'requested_symbol_count': len(SYMBOLS),
            'fundamentals_scope_symbols': list(SYMBOLS),
            'fundamentals_issuer_aliases': {},
            'placeholder_name_count': 0,
        }
    )
    return result


def test_price_history_streaming_fingerprint_matches_canonical_payload() -> None:
    bars = {
        '000660': [
            {
                'trade_date': '2026-10-28',
                'close_price': 123,
                'per': '7.125000',
                'source': '한국거래소',
                'captured_at': 'ignored',
            }
        ],
        '005930': [
            {
                'trade_date': '2026-10-28',
                'close_price': 456,
                'per': None,
                'source': 'pykrx',
                'captured_at': None,
            },
            {
                'trade_date': '2026-10-27',
                'close_price': 450,
                'per': '8.500000',
                'source': 'pykrx',
                'captured_at': 'ignored',
            },
        ],
    }

    def load(*, market_date, symbols, history_days):
        assert market_date == date.fromisoformat(MARKET_DATE)
        assert history_days == FACTOR_PRICE_HISTORY_ROWS
        return {symbol: bars[symbol] for symbol in symbols}

    with (
        patch(
            'app.eod.exact_factor_bulk_repair.'
            'FACTOR_PRICE_FINGERPRINT_SYMBOL_CHUNK_SIZE',
            1,
        ),
        patch(
            'app.eod.exact_factor_bulk_repair.'
            'load_recent_price_bars_by_symbol',
            side_effect=load,
        ) as loader,
    ):
        fingerprint = _fingerprint_price_history(
            market_date=date.fromisoformat(MARKET_DATE),
            symbols=SYMBOLS,
        )

    expected_payload = [
        [
            symbol,
            [
                {
                    key: value
                    for key, value in sorted(bar.items())
                    if key != 'captured_at'
                }
                for bar in bars[symbol]
            ],
        ]
        for symbol in SYMBOLS
    ]
    assert loader.call_count == len(SYMBOLS)
    assert fingerprint == {
        'history_row_limit': FACTOR_PRICE_HISTORY_ROWS,
        'row_count': 3,
        'source_sha256': canonical_json_sha256(expected_payload),
    }


def test_source_fingerprint_expands_preferred_fundamentals_issuer_scope() -> None:
    preferred_symbol = '005935'
    issuer_symbol = '005930'
    session = MagicMock()
    empty_result = MagicMock()
    empty_result.scalars.return_value = []
    session.execute.return_value = empty_result

    @contextmanager
    def fake_session_scope(**_kwargs):
        yield session

    with (
        patch(
            'app.eod.exact_factor_bulk_repair.session_scope',
            fake_session_scope,
        ),
        patch(
            'app.eod.exact_factor_bulk_repair.'
            'load_recent_price_bars_by_symbol',
            return_value={preferred_symbol: []},
        ),
        patch(
            'app.eod.exact_factor_bulk_repair.'
            'resolve_preferred_fundamentals_issuer_symbols',
            return_value={preferred_symbol: issuer_symbol},
        ),
    ):
        fingerprints = _load_factor_source_fingerprints(
            MARKET_DATE,
            [preferred_symbol],
        )

    symbol_scope = fingerprints['symbol_metadata']
    assert symbol_scope['requested_symbol_count'] == 1
    assert symbol_scope['fundamentals_scope_symbols'] == [
        issuer_symbol,
        preferred_symbol,
    ]
    assert symbol_scope['fundamentals_issuer_aliases'] == {
        preferred_symbol: issuer_symbol,
    }
    snapshot_statement = session.execute.call_args_list[3].args[0]
    snapshot_parameters = snapshot_statement.compile().params
    assert sorted(snapshot_parameters['stock_code_1']) == [
        issuer_symbol,
        preferred_symbol,
    ]


def test_source_fingerprint_binds_preferred_alias_mapping_not_only_union() -> None:
    preferred_symbols = ['000885', '005935']
    first_aliases = {
        '000885': '000880',
        '005935': '005930',
    }
    second_aliases = {
        '000885': '005930',
        '005935': '000880',
    }

    def load(alias_mapping):
        session = MagicMock()
        empty_result = MagicMock()
        empty_result.scalars.return_value = []
        session.execute.return_value = empty_result

        @contextmanager
        def fake_session_scope(**_kwargs):
            yield session

        with (
            patch(
                'app.eod.exact_factor_bulk_repair.session_scope',
                fake_session_scope,
            ),
            patch(
                'app.eod.exact_factor_bulk_repair.'
                'load_recent_price_bars_by_symbol',
                return_value={symbol: [] for symbol in preferred_symbols},
            ),
            patch(
                'app.eod.exact_factor_bulk_repair.'
                'resolve_preferred_fundamentals_issuer_symbols',
                return_value=alias_mapping,
            ),
        ):
            return _load_factor_source_fingerprints(
                MARKET_DATE,
                preferred_symbols,
            )['symbol_metadata']

    first = load(first_aliases)
    second = load(second_aliases)

    assert first['fundamentals_scope_symbols'] == (
        second['fundamentals_scope_symbols']
    )
    assert first['fundamentals_issuer_aliases'] == first_aliases
    assert second['fundamentals_issuer_aliases'] == second_aliases
    assert first['source_sha256'] != second['source_sha256']


def _mapping_result(*, scalars=(), mappings=()):
    result = MagicMock()
    result.scalars.return_value = list(scalars)
    mapped = MagicMock()
    mapped.all.return_value = list(mappings)
    mapped.__iter__.return_value = iter(list(mappings))
    result.mappings.return_value = mapped
    return result


def test_source_fingerprint_unions_piotroski_and_advanced_snapshots() -> None:
    symbol = '005930'
    piotroski_snapshot_id = 10
    advanced_snapshot_id = 20
    session = MagicMock()
    session.execute.side_effect = [
        _mapping_result(
            scalars=[
                SimpleNamespace(
                    symbol=symbol,
                    name_ko='Samsung Electronics',
                    market='KOSPI',
                    is_active=True,
                )
            ]
        ),
        _mapping_result(),
        _mapping_result(),
        _mapping_result(
            mappings=[
                {
                    'snapshot_id': piotroski_snapshot_id,
                    'stock_code': symbol,
                    'business_year': 2025,
                    'row_rank': 1,
                }
            ]
        ),
        _mapping_result(
            mappings=[
                {
                    'snapshot_id': advanced_snapshot_id,
                    'row_rank': 1,
                }
            ]
        ),
        _mapping_result(
            mappings=[
                {
                    'id': piotroski_snapshot_id,
                    'disclosure_event_id': 100,
                    'rcept_no': 'P10',
                    'corp_code': 'C10',
                    'stock_code': symbol,
                    'corp_name': 'Samsung Electronics',
                    'business_year': 2025,
                    'report_code': '11011',
                    'fs_div': 'CFS',
                    'period_type': 'semiannual',
                    'currency': 'KRW',
                    'captured_at': None,
                    'source_status': '000',
                },
                {
                    'id': advanced_snapshot_id,
                    'disclosure_event_id': 200,
                    'rcept_no': 'A20',
                    'corp_code': 'C20',
                    'stock_code': symbol,
                    'corp_name': 'Samsung Electronics',
                    'business_year': 2026,
                    'report_code': '11011',
                    'fs_div': 'CFS',
                    'period_type': 'annual',
                    'currency': 'KRW',
                    'captured_at': None,
                    'source_status': '000',
                },
            ]
        ),
        _mapping_result(
            mappings=[
                {
                    'id': 100,
                    'rcept_no': 'P10',
                    'corp_code': 'C10',
                    'stock_code': symbol,
                    'receipt_date': date(2026, 3, 1),
                    'fundamentals_capture_status': 'captured',
                },
                {
                    'id': 200,
                    'rcept_no': 'A20',
                    'corp_code': 'C20',
                    'stock_code': symbol,
                    'receipt_date': date(2026, 4, 1),
                    'fundamentals_capture_status': 'failed',
                },
            ]
        ),
        _mapping_result(
            mappings=[
                {'id': 1000, 'snapshot_id': piotroski_snapshot_id},
                {'id': 2000, 'snapshot_id': advanced_snapshot_id},
            ]
        ),
        _mapping_result(
            mappings=[
                {'id': 1001, 'snapshot_id': piotroski_snapshot_id},
                {'id': 2001, 'snapshot_id': advanced_snapshot_id},
            ]
        ),
        _mapping_result(),
        _mapping_result(),
    ]

    @contextmanager
    def fake_session_scope(**_kwargs):
        yield session

    with (
        patch(
            'app.eod.exact_factor_bulk_repair.session_scope',
            fake_session_scope,
        ),
        patch(
            'app.eod.exact_factor_bulk_repair.'
            'load_recent_price_bars_by_symbol',
            return_value={symbol: []},
        ),
        patch(
            'app.eod.exact_factor_bulk_repair.'
            'resolve_preferred_fundamentals_issuer_symbols',
            return_value={},
        ),
    ):
        fingerprints = _load_factor_source_fingerprints(
            MARKET_DATE,
            [symbol],
        )

    snapshot_statement = session.execute.call_args_list[5].args[0]
    snapshot_ids = snapshot_statement.compile().params['id_1']
    assert sorted(snapshot_ids) == [
        piotroski_snapshot_id,
        advanced_snapshot_id,
    ]
    assert fingerprints['dart_fundamentals_snapshots']['row_count'] == 2
    assert fingerprints['dart_disclosure_events']['row_count'] == 2
    assert fingerprints['dart_fundamentals_rows']['row_count'] == 2
    assert fingerprints['dart_fundamentals_highlights']['row_count'] == 2


def _apply_result(*, suffix='v1'):
    membership_hash = market_membership_sha256(SYMBOLS)
    return {
        'implementation_contract': FACTOR_REPAIR_IMPLEMENTATION_CONTRACT,
        **_dry_result(),
        'mode': 'apply',
        'status': 'completed',
        'applied': True,
        'stage_results': {
            stage_id: {
                'results_persisted': len(SYMBOLS),
                'scope_finalization': {
                    'stage': stage_id,
                    'market_date': MARKET_DATE,
                    'exact_symbol_count': len(SYMBOLS),
                    'exact_membership_sha256': membership_hash,
                    'successful_symbol_count': len(SYMBOLS),
                    'successful_membership_sha256': membership_hash,
                    'authoritative_market_scope': True,
                    'authoritative_markets': ['KOSDAQ', 'KOSPI'],
                    'all_market_authority': False,
                    'deleted_stale_row_count': 0,
                    'ready': True,
                },
            }
            for stage_id in FACTOR_RESULT_MODELS
        },
        'completion_evidence': {
            'updated_batch_count': 1,
            'exact_membership_count': len(SYMBOLS),
            'ranking_membership_sha256': membership_hash,
            'provider_membership_sha256': membership_hash,
            'membership_evidence_source': 'stored_eod_run_metadata',
            'membership_evidence_run_id': 77,
            'membership_evidence_run_source': 'pykrx',
            'membership_evidence_sha256': None,
            'membership_evidence_repair_batch_ids': [],
            'daily_price_input_sha256': INPUT_SHA256,
            'stage_output_fingerprints': _fingerprints(suffix=suffix),
        },
    }


def _pending_dry_result(*, proof_sha256=PENDING_PROOF_SHA256):
    provider_membership = [*SYMBOLS, PROTECTED_PROVIDER_CODE]
    result = _dry_result(
        provider_hash=market_membership_sha256(provider_membership)
    )
    result.update(
        {
            'membership_evidence_source': (
                'pending_missing_bar_repair_metadata'
            ),
            'membership_evidence_run_id': None,
            'membership_evidence_run_source': 'pykrx',
            'membership_evidence_sha256': proof_sha256,
            'membership_evidence_repair_batch_ids': (
                list(PENDING_PROOF_BATCH_IDS)
            ),
            'exact_provider_membership_count': len(provider_membership),
            'provider_excluded_from_ranking_codes': [
                PROTECTED_PROVIDER_CODE
            ],
        }
    )
    return result


def _pending_apply_result(*, proof_sha256=PENDING_PROOF_SHA256):
    result = _apply_result()
    dry_result = _pending_dry_result(proof_sha256=proof_sha256)
    result.update(
        {
            key: value
            for key, value in dry_result.items()
            if key not in {'mode', 'status'}
        }
    )
    result['completion_evidence'].update(
        {
            'updated_batch_count': 2,
            'provider_membership_sha256': dry_result['membership_sha256'],
            'membership_evidence_source': dry_result[
                'membership_evidence_source'
            ],
            'membership_evidence_run_id': None,
            'membership_evidence_run_source': 'pykrx',
            'membership_evidence_sha256': proof_sha256,
            'membership_evidence_repair_batch_ids': list(
                PENDING_PROOF_BATCH_IDS
            ),
        }
    )
    return result


class _RepairService:
    def __init__(self, *, fail_first_apply=False, on_apply=None):
        self.calls = []
        self.fail_first_apply = fail_first_apply
        self.on_apply = on_apply
        self.apply_calls = 0

    def repair_for_market_date(self, **kwargs):
        self.calls.append(kwargs)
        if not kwargs['apply']:
            return _dry_result()
        self.apply_calls += 1
        if self.fail_first_apply and self.apply_calls == 1:
            return {
                **_apply_result(),
                'status': 'partial',
                'applied': False,
                'reason': 'factor_stage_failed',
            }
        if self.on_apply is not None:
            self.on_apply()
        return _apply_result()


class _PendingRepairService:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.apply_calls = 0
        self.proof_sha256 = PENDING_PROOF_SHA256

    def repair_for_market_date(self, **kwargs):
        self.calls.append(kwargs)
        if not kwargs['apply']:
            return _pending_dry_result(proof_sha256=self.proof_sha256)
        self.apply_calls += 1
        return _pending_apply_result(proof_sha256=self.proof_sha256)


@contextmanager
def _guard(**_kwargs):
    yield True, None


@contextmanager
def _blocked_guard(**_kwargs):
    yield False, 'active_daily_or_eod_writer'


def _pending():
    return [
        {
            'required_rebuild_trade_dates': [MARKET_DATE],
            'factor_rebuild_metadata': {},
        }
    ]


def _completion_state(*, suffix='v1', source_suffix='v1'):
    return {
        'source_fingerprints': _source_fingerprints(suffix=source_suffix),
        'input_row_count': len(SYMBOLS),
        'input_sha256': INPUT_SHA256,
        'output_fingerprints': _fingerprints(suffix=suffix),
        'output_memberships': {
            stage_id: list(SYMBOLS) for stage_id in FACTOR_RESULT_MODELS
        },
        'metadata_verified': True,
        'pending_batch_count': 1,
        'provider_membership_sha256': market_membership_sha256(SYMBOLS),
        'membership_evidence_source': 'stored_eod_run_metadata',
        'membership_evidence_run_id': 77,
        'membership_evidence_run_source': 'pykrx',
        'membership_evidence_sha256': None,
        'membership_evidence_repair_batch_ids': [],
    }


def _pending_completion_state(*, proof_sha256=PENDING_PROOF_SHA256):
    result = _completion_state()
    result.update(
        {
            'pending_batch_count': 2,
            'provider_membership_sha256': market_membership_sha256(
                [*SYMBOLS, PROTECTED_PROVIDER_CODE]
            ),
            'membership_evidence_source': (
                'pending_missing_bar_repair_metadata'
            ),
            'membership_evidence_run_id': None,
            'membership_evidence_run_source': 'pykrx',
            'membership_evidence_sha256': proof_sha256,
            'membership_evidence_repair_batch_ids': list(
                PENDING_PROOF_BATCH_IDS
            ),
        }
    )
    return result


def _repair(service=None, **overrides):
    kwargs = {
        'repair_service': service or _RepairService(),
        'stored_symbols_loader': lambda _market_date: SYMBOLS,
        'pending_batch_loader': _pending,
        'frozen_membership_loader': (
            lambda **_kwargs: tuple(SYMBOLS)
        ),
        'completion_state_loader': lambda *_args: _completion_state(),
        'guard_factory': _guard,
        'finality_validator': lambda _market_date: None,
    }
    kwargs.update(overrides)
    return ExactFactorBulkRepair(_manifest(), **kwargs)


def test_factor_guard_requests_global_symbol_writer_fence() -> None:
    observed = {}

    @contextmanager
    def derived_guard(**kwargs):
        observed.update(kwargs)
        yield True, None

    with patch(
        'app.eod.exact_factor_bulk_repair.exact_derived_stage_guard',
        derived_guard,
    ):
        with exact_factor_bulk_guard(
            market_date=date.fromisoformat(MARKET_DATE)
        ) as guard:
            assert guard == (True, None)

    assert observed['lock_symbol_writes'] is True


def test_dry_run_checkpoints_stored_only_count_and_hash_proof() -> None:
    service = _RepairService()
    repair = _repair(service)
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        result = repair.run(checkpoint_path=checkpoint)
        saved = json.loads(checkpoint.read_text())

    assert result['status'] == 'dry_run'
    assert result['reviewed_dates'] == 1
    assert saved['manifest_sha256'] == repair.manifest_sha256
    assert saved['schema_version'] == CHECKPOINT_SCHEMA_VERSION
    unit = saved['units'][MARKET_DATE]
    assert unit['status'] == 'reviewed'
    assert unit['dry_proof']['implementation_contract'] == (
        FACTOR_REPAIR_IMPLEMENTATION_CONTRACT
    )
    assert unit['dry_proof']['exact_symbol_count'] == len(SYMBOLS)
    assert unit['dry_proof']['ranking_membership_sha256'] == (
        market_membership_sha256(SYMBOLS)
    )
    assert service.calls == [
        {
            'market_date': MARKET_DATE,
            'apply': False,
            'membership_policy': MEMBERSHIP_POLICY_STORED_ONLY,
        }
    ]


def test_apply_requires_checkpointed_dry_proof() -> None:
    service = _RepairService()
    repair = _repair(service)
    with TemporaryDirectory() as directory:
        result = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=Path(directory) / 'factor.json',
        )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'reviewed_dry_proof_required'
    assert service.calls == []


def test_apply_revalidates_review_then_prunes_exact_date_scope() -> None:
    service = _RepairService()
    repair = _repair(service)
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        reviewed = repair.run(checkpoint_path=checkpoint)
        applied = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert reviewed['status'] == 'dry_run'
    assert applied['status'] == 'completed'
    assert service.calls[-1] == {
        'market_date': MARKET_DATE,
        'apply': True,
        'prune_stale': True,
        'expected_provider_membership_count': len(SYMBOLS),
        'expected_membership_sha256': market_membership_sha256(SYMBOLS),
        'expected_membership_evidence_source': 'stored_eod_run_metadata',
        'expected_membership_evidence_run_id': 77,
        'expected_membership_evidence_run_source': 'pykrx',
        'expected_membership_evidence_sha256': None,
        'expected_membership_evidence_repair_batch_ids': [],
        'membership_policy': MEMBERSHIP_POLICY_STORED_ONLY,
    }


def test_completion_state_is_loaded_once_per_review_boundary() -> None:
    service = _RepairService()
    loads = []

    def completion_state_loader(market_date, symbols):
        loads.append((market_date, tuple(symbols)))
        return _completion_state()

    repair = _repair(
        service,
        completion_state_loader=completion_state_loader,
    )
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        reviewed = repair.run(checkpoint_path=checkpoint)
        assert reviewed['status'] == 'dry_run'
        assert len(loads) == 1

        applied = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
        assert applied['status'] == 'completed'
        assert len(loads) == 3

        resumed = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert resumed['status'] == 'completed'
    assert resumed['skipped_dates'] == 1
    assert len(loads) == 4
    assert loads == [(MARKET_DATE, tuple(SYMBOLS))] * 4
    assert service.apply_calls == 1


def test_pending_proof_lineage_may_be_subset_of_affected_batches() -> None:
    service = _PendingRepairService()
    repair = _repair(
        service,
        pending_batch_loader=lambda: [
            {'repair_batch_id': 'missing-bar-authority'},
            {'repair_batch_id': 'correction-destination'},
        ],
        completion_state_loader=lambda *_args: _pending_completion_state(
            proof_sha256=service.proof_sha256
        ),
    )
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        reviewed = repair.run(checkpoint_path=checkpoint)
        saved = json.loads(checkpoint.read_text())
        applied = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    proof = saved['units'][MARKET_DATE]['dry_proof']
    assert reviewed['status'] == 'dry_run'
    assert proof['membership_evidence_source'] == (
        'pending_missing_bar_repair_metadata'
    )
    assert proof['membership_evidence_sha256'] == PENDING_PROOF_SHA256
    assert proof['membership_evidence_repair_batch_ids'] == [
        'missing-bar-authority'
    ]
    assert proof['protected_provider_codes'] == [PROTECTED_PROVIDER_CODE]
    assert applied['status'] == 'completed'
    assert service.apply_calls == 1
    assert service.calls[-1][
        'expected_membership_evidence_repair_batch_ids'
    ] == ['missing-bar-authority']
    assert service.calls[-1]['expected_membership_evidence_sha256'] == (
        PENDING_PROOF_SHA256
    )


def test_pending_proof_lineage_drift_blocks_before_apply() -> None:
    service = _PendingRepairService()
    repair = _repair(
        service,
        completion_state_loader=lambda *_args: _pending_completion_state(
            proof_sha256=service.proof_sha256
        ),
    )
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        reviewed = repair.run(checkpoint_path=checkpoint)
        service.proof_sha256 = 'c' * 64
        applied = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert reviewed['status'] == 'dry_run'
    assert applied['status'] == 'blocked'
    assert applied['reason'] == 'reviewed_dry_proof_drift'
    assert service.apply_calls == 0


def test_apply_refuses_provider_count_or_hash_drift_after_review() -> None:
    class _DriftingService(_RepairService):
        def repair_for_market_date(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs['apply']:
                raise AssertionError('apply must not run after proof drift')
            if len(self.calls) == 1:
                return _dry_result()
            protected_code = '00104K'
            drifted = _dry_result(
                provider_hash=market_membership_sha256(
                    [*SYMBOLS, protected_code]
                )
            )
            drifted['exact_provider_membership_count'] = len(SYMBOLS) + 1
            drifted['provider_excluded_from_ranking_codes'] = [
                protected_code
            ]
            return drifted

    service = _DriftingService()
    repair = _repair(service)
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        repair.run(checkpoint_path=checkpoint)
        result = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'reviewed_dry_proof_drift'
    assert all(call['apply'] is False for call in service.calls)


def test_active_writer_refuses_before_scope_or_service_access() -> None:
    service = _RepairService()

    def explode(*_args, **_kwargs):
        raise AssertionError('downstream state must not be read without fence')

    repair = _repair(
        service,
        guard_factory=_blocked_guard,
        stored_symbols_loader=explode,
        pending_batch_loader=explode,
        frozen_membership_loader=explode,
        completion_state_loader=explode,
    )
    with TemporaryDirectory() as directory:
        result = repair.run(checkpoint_path=Path(directory) / 'factor.json')

    assert result['status'] == 'blocked'
    assert result['reason'] == 'active_daily_or_eod_writer'
    assert service.calls == []


def test_frozen_membership_mismatch_never_calls_factor_service() -> None:
    service = _RepairService()
    repair = _repair(
        service,
        frozen_membership_loader=lambda **_kwargs: ('000660',),
    )
    with TemporaryDirectory() as directory:
        result = repair.run(checkpoint_path=Path(directory) / 'factor.json')

    assert result['status'] == 'blocked'
    assert result['failed_dates'] == 1
    assert service.calls == []


def test_stored_price_membership_may_be_a_reviewed_scope_superset() -> None:
    service = _RepairService()
    repair = _repair(
        service,
        stored_symbols_loader=lambda _market_date: [*SYMBOLS, '105560'],
    )
    with TemporaryDirectory() as directory:
        result = repair.run(checkpoint_path=Path(directory) / 'factor.json')

    assert result['status'] == 'dry_run'
    assert result['failed_dates'] == 0
    assert len(service.calls) == 1


def test_failed_date_retries_from_reviewed_proof() -> None:
    service = _RepairService(fail_first_apply=True)
    repair = _repair(service)
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        repair.run(checkpoint_path=checkpoint)
        first = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
        resumed = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert first['status'] == 'partial'
    assert resumed['status'] == 'completed'
    assert service.apply_calls == 2


def test_completed_checkpoint_reruns_after_live_factor_output_drift() -> None:
    live = {'suffix': 'v1'}

    def reset_live():
        live['suffix'] = 'v1'

    service = _RepairService(on_apply=reset_live)
    repair = _repair(
        service,
        completion_state_loader=(
            lambda *_args: _completion_state(suffix=live['suffix'])
        ),
    )
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        repair.run(checkpoint_path=checkpoint)
        first = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
        live['suffix'] = 'deleted-row-drift'
        resumed = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert first['status'] == 'completed'
    assert resumed['status'] == 'completed'
    assert resumed['skipped_dates'] == 0
    assert service.apply_calls == 2


def test_completed_checkpoint_skips_only_with_live_output_and_evidence() -> None:
    service = _RepairService()
    repair = _repair(service)
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        repair.run(checkpoint_path=checkpoint)
        repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
        resumed = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert resumed['status'] == 'completed'
    assert resumed['skipped_dates'] == 1
    assert service.apply_calls == 1


def test_completed_checkpoint_blocks_after_nonprice_source_drift() -> None:
    live = {'source_suffix': 'v1'}
    service = _RepairService()
    repair = _repair(
        service,
        completion_state_loader=lambda *_args: _completion_state(
            source_suffix=live['source_suffix']
        ),
    )
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        repair.run(checkpoint_path=checkpoint)
        first = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
        live['source_suffix'] = 'valuation-drift'
        resumed = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert first['status'] == 'completed'
    assert resumed['status'] == 'blocked'
    assert resumed['reason'] == 'reviewed_dry_proof_drift'
    assert service.apply_calls == 1


@pytest.mark.parametrize('component', FACTOR_SOURCE_COMPONENTS)
def test_each_source_component_digest_drift_blocks_apply(component) -> None:
    live = {'drift': False}
    service = _RepairService()

    def completion_state_loader(*_args):
        state = _completion_state()
        if live['drift']:
            state['source_fingerprints'][component]['source_sha256'] = (
                canonical_json_sha256(
                    {'component': component, 'suffix': 'drifted'}
                )
            )
        return state

    repair = _repair(
        service,
        completion_state_loader=completion_state_loader,
    )
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        reviewed = repair.run(checkpoint_path=checkpoint)
        live['drift'] = True
        applied = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert reviewed['status'] == 'dry_run'
    assert applied['status'] == 'blocked'
    assert applied['reason'] == 'reviewed_dry_proof_drift'
    assert service.apply_calls == 0


def test_source_change_during_apply_fails_closed() -> None:
    live = {'source_suffix': 'before'}

    def drift_source():
        live['source_suffix'] = 'after'

    service = _RepairService(on_apply=drift_source)
    repair = _repair(
        service,
        completion_state_loader=lambda *_args: _completion_state(
            source_suffix=live['source_suffix']
        ),
    )
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        repair.run(checkpoint_path=checkpoint)
        result = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
        saved = json.loads(checkpoint.read_text())

    assert result['status'] == 'partial'
    assert saved['units'][MARKET_DATE]['status'] == 'failed'
    assert 'source dependency changed during repair' in (
        saved['units'][MARKET_DATE]['error_message']
    )


def test_placeholder_symbol_metadata_fails_before_factor_publication() -> None:
    service = _RepairService()

    def state(*_args):
        value = _completion_state()
        value['source_fingerprints']['symbol_metadata'][
            'placeholder_name_count'
        ] = 1
        return value

    repair = _repair(service, completion_state_loader=state)
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        repair.run(checkpoint_path=checkpoint)
        result = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'reviewed_dry_proof_required'
    assert service.apply_calls == 0


def test_manifest_hash_drift_is_rejected_before_service_call() -> None:
    service = _RepairService()
    repair = _repair(service)
    with TemporaryDirectory() as directory:
        checkpoint = Path(directory) / 'factor.json'
        repair.run(checkpoint_path=checkpoint)
        drifted_symbols = ['000660']
        drifted = ExactFactorBulkRepair(
            _manifest(symbols=drifted_symbols),
            repair_service=service,
            stored_symbols_loader=lambda _market_date: drifted_symbols,
            pending_batch_loader=_pending,
            frozen_membership_loader=lambda **_kwargs: tuple(drifted_symbols),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        prior_call_count = len(service.calls)
        result = drifted.run(checkpoint_path=checkpoint)

    assert result['status'] == 'blocked'
    assert result['reason'] == 'checkpoint_manifest_mismatch'
    assert len(service.calls) == prior_call_count


def test_live_validation_allows_reviewed_nonranking_provider_codes() -> None:
    protected_code = '00104K'
    provider_hash = market_membership_sha256([*SYMBOLS, protected_code])
    repair = _repair()
    item = repair.manifest['dates'][0]
    dry_result = _dry_result(provider_hash=provider_hash)
    dry_result['exact_provider_membership_count'] = len(SYMBOLS) + 1
    dry_result['provider_excluded_from_ranking_codes'] = [protected_code]
    dry_proof = repair._dry_proof(dry_result, item)
    dry_proof.update(
        {
            'source_fingerprints': _source_fingerprints(),
            'daily_price_input_count': len(SYMBOLS),
            'daily_price_input_sha256': INPUT_SHA256,
        }
    )
    applied = _apply_result()
    applied['source_fingerprints'] = _source_fingerprints()
    applied['membership_sha256'] = provider_hash
    applied['completion_evidence']['provider_membership_sha256'] = provider_hash
    unit = {
        'status': 'completed',
        'item_sha256': 'unused',
        'dry_proof': dry_proof,
        'result': applied,
    }
    live = _completion_state()
    live['provider_membership_sha256'] = provider_hash
    for stage_id in FACTOR_RESULT_MODELS:
        live['output_memberships'][stage_id] = sorted(
            [*SYMBOLS, protected_code]
        )
    repair.completion_state_loader = lambda *_args: live

    assert repair._completed_checkpoint_is_valid(unit, item)
    assert repair._completed_live_state_is_valid(unit, item)
