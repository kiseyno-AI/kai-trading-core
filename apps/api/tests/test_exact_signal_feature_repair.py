from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import SQLAlchemyError

from app.db import session_scope
from app.db_models import DailySignalFeature, Symbol
from app.eod.derived_stage_repair import MANIFEST_SCHEMA_VERSION
from app.eod.exact_signal_feature_repair import (
    CHECKPOINT_SCHEMA_VERSION,
    IMPLEMENTATION_CONTRACT,
    PRICE_HISTORY_ROWS,
    SOURCE_COMPONENTS,
    ExactSignalFeatureRepair,
    _build_parser,
    _load_completion_state,
    prune_stale_signal_feature_rows,
    verify_exact_signal_feature_rows,
)
from app.eod.market_membership import market_membership_sha256
from app.internal.exact_repair_manifest import (
    canonical_json_sha256,
    checkpoint_file_lock,
)


SYMBOLS = ['000660', '005930']
MARKET_DATE = '2026-10-28'
REPAIR_STARTED_AT = datetime(2026, 10, 29, tzinfo=timezone.utc)


def _manifest(*, symbols=None, market_date=MARKET_DATE):
    exact = list(symbols or SYMBOLS)
    return {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'date_count': 1,
        'dates': [
            {
                'market_date': market_date,
                'symbol_count': len(exact),
                'membership_sha256': market_membership_sha256(exact),
                'symbols': exact,
            }
        ],
    }


def _source_fingerprints(*, suffix='v1'):
    result = {
        component: {
            'row_count': (
                len(SYMBOLS)
                if component not in {
                    'action_summaries',
                    'direct_signal_evidence',
                }
                else 0
            ),
            'source_sha256': canonical_json_sha256(
                {'component': component, 'suffix': suffix}
            ),
        }
        for component in SOURCE_COMPONENTS
    }
    result['price_history'].update(
        {
            'row_count': len(SYMBOLS) * PRICE_HISTORY_ROWS,
            'history_row_limit': PRICE_HISTORY_ROWS,
        }
    )
    result['symbol_metadata'].update(
        {
            'scope_symbol_count': len(SYMBOLS),
            'placeholder_name_count': 0,
        }
    )
    for component in ('piotroski_filter', 'market_risk_filter'):
        result[component].update(
            {
                'exact_scope_row_count': len(SYMBOLS),
                'exact_scope_membership_sha256': (
                    market_membership_sha256(SYMBOLS)
                ),
            }
        )
    return result


def _stale_extras(*, count=1, suffix='v1'):
    symbols = ['999999'] if count else []
    return {
        'row_count': count,
        'membership_sha256': market_membership_sha256(symbols),
        'output_sha256': canonical_json_sha256(
            [{'symbol': symbol, 'suffix': suffix} for symbol in symbols]
        ),
    }


def _state(
    *,
    source_suffix='v1',
    membership=None,
    output_suffix='v1',
    stale_count=1,
    stale_suffix='v1',
):
    resolved_membership = list(
        membership if membership is not None else [*SYMBOLS, '999999']
    )
    return {
        'source_fingerprints': _source_fingerprints(
            suffix=source_suffix
        ),
        'output_membership': resolved_membership,
        'output_fingerprint': {
            'row_count': len(resolved_membership),
            'output_sha256': canonical_json_sha256(
                {
                    'membership': resolved_membership,
                    'suffix': output_suffix,
                }
            ),
        },
        'stale_extras': _stale_extras(
            count=stale_count,
            suffix=stale_suffix,
        ),
    }


def _completed_state(*, source_suffix='v1', output_suffix='completed'):
    return _state(
        source_suffix=source_suffix,
        membership=SYMBOLS,
        output_suffix=output_suffix,
        stale_count=0,
    )


@contextmanager
def _guard(*, acquired=True, reason=None, **_kwargs):
    yield acquired, reason


def _rebuild_result():
    return {
        'exact_scope': True,
        'features_persisted': len(SYMBOLS),
        'scope_symbol_count': len(SYMBOLS),
        'scope_membership_sha256': market_membership_sha256(SYMBOLS),
        'preserved_direct_source_rows': 1,
    }


def _verification_result():
    return {
        'exact_row_count': len(SYMBOLS),
        'fresh_exact_row_count': len(SYMBOLS),
        'membership_sha256': market_membership_sha256(SYMBOLS),
    }


def _prune_result(*, stale_count=1, stale_suffix='v1'):
    stale = _stale_extras(count=stale_count, suffix=stale_suffix)
    return {
        'reviewed_stale_row_count': stale['row_count'],
        'reviewed_stale_membership_sha256': stale['membership_sha256'],
        'reviewed_stale_output_sha256': stale['output_sha256'],
        'pruned_stale_row_count': stale_count,
        'remaining_symbol_count': len(SYMBOLS),
        'remaining_membership_sha256': market_membership_sha256(SYMBOLS),
    }


def _prune_result_for_expected(expected):
    return {
        'reviewed_stale_row_count': expected['row_count'],
        'reviewed_stale_membership_sha256': expected[
            'membership_sha256'
        ],
        'reviewed_stale_output_sha256': expected['output_sha256'],
        'pruned_stale_row_count': expected['row_count'],
        'remaining_symbol_count': len(SYMBOLS),
        'remaining_membership_sha256': market_membership_sha256(SYMBOLS),
    }


def _repair(
    *,
    state_loader,
    rebuild_runner=None,
    exact_verifier=None,
    stale_pruner=None,
    guard_factory=_guard,
    finality_validator=lambda _market_date: None,
):
    return ExactSignalFeatureRepair(
        _manifest(),
        rebuild_runner=rebuild_runner or (lambda **_kwargs: _rebuild_result()),
        stored_symbols_loader=lambda _market_date: [*SYMBOLS, '999999'],
        pending_batch_loader=lambda: [{'repair_batch_id': 'batch-1'}],
        frozen_membership_loader=lambda **_kwargs: SYMBOLS,
        completion_state_loader=state_loader,
        exact_verifier=exact_verifier
        or (lambda **_kwargs: _verification_result()),
        stale_pruner=stale_pruner
        or (
            lambda **kwargs: _prune_result_for_expected(
                kwargs['expected_stale_extras']
            )
        ),
        guard_factory=guard_factory,
        finality_validator=finality_validator,
        clock=lambda: REPAIR_STARTED_AT,
    )


def _dry_review(checkpoint: Path, *, state=None):
    repair = _repair(
        state_loader=lambda _market_date, _symbols: state or _state()
    )
    result = repair.run(checkpoint_path=checkpoint)
    assert result['status'] == 'dry_run'
    return repair, result


def test_cli_defaults_to_dry_run() -> None:
    args = _build_parser().parse_args(
        ['--manifest', 'manifest.json', '--checkpoint', 'checkpoint.json']
    )
    assert args.apply is False
    assert args.dry_run is False


def test_dry_run_writes_hash_bound_reviewed_checkpoint() -> None:
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair, result = _dry_review(checkpoint)
        saved = json.loads(checkpoint.read_text())

    assert result['reviewed_dates'] == 1
    assert result['applied'] is False
    assert saved['schema_version'] == CHECKPOINT_SCHEMA_VERSION
    assert saved['manifest_sha256'] == repair.manifest_sha256
    unit = saved['units'][MARKET_DATE]
    assert unit['status'] == 'reviewed'
    assert unit['dry_proof']['implementation_contract'] == (
        IMPLEMENTATION_CONTRACT
    )
    assert unit['dry_proof']['stale_extras']['row_count'] == 1


def test_checkpoint_is_required_even_for_dry_run() -> None:
    repair = _repair(state_loader=lambda *_args: _state())
    result = repair.run()
    assert result['status'] == 'blocked'
    assert result['reason'] == 'checkpoint_path_required'


def test_checkpoint_refuses_a_concurrent_repair_writer() -> None:
    repair = _repair(state_loader=lambda *_args: _state())
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        with checkpoint_file_lock(checkpoint) as acquired:
            assert acquired is True
            result = repair.run(checkpoint_path=checkpoint)

    assert result['status'] == 'blocked'
    assert result['reason'] == 'checkpoint_lock_unavailable'


@pytest.mark.parametrize(
    ('expected_hash', 'reason'),
    [
        (None, 'expected_manifest_sha256_required'),
        ('0' * 64, 'expected_manifest_sha256_mismatch'),
    ],
)
def test_apply_requires_exact_reviewed_manifest_hash(
    expected_hash,
    reason,
) -> None:
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair, _result = _dry_review(checkpoint)
        result = repair.run(
            apply=True,
            expected_manifest_sha256=expected_hash,
            checkpoint_path=checkpoint,
        )
    assert result['status'] == 'blocked'
    assert result['reason'] == reason


def test_apply_requires_a_reviewed_dry_proof() -> None:
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair = _repair(state_loader=lambda *_args: _state())
        result = repair.run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
    assert result['status'] == 'blocked'
    assert result['reason'] == 'reviewed_dry_proof_required'


def test_apply_persists_verifies_then_prunes_and_checkpoints_completion() -> None:
    calls = []
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        reviewed = _state()
        dry_repair, _result = _dry_review(checkpoint, state=reviewed)
        states = iter(
            [
                reviewed,
                _state(output_suffix='persisted'),
                _completed_state(),
            ]
        )

        def rebuild_runner(**_kwargs):
            calls.append('persist')
            return _rebuild_result()

        def exact_verifier(**_kwargs):
            calls.append('verify')
            return _verification_result()

        def stale_pruner(**kwargs):
            calls.append('prune')
            assert kwargs['expected_stale_extras'] == _stale_extras()
            return _prune_result()

        repair = _repair(
            state_loader=lambda *_args: next(states),
            rebuild_runner=rebuild_runner,
            exact_verifier=exact_verifier,
            stale_pruner=stale_pruner,
        )
        result = repair.run(
            apply=True,
            expected_manifest_sha256=dry_repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
        saved = json.loads(checkpoint.read_text())

    assert result['status'] == 'completed'
    assert result['applied'] is True
    assert calls == ['persist', 'verify', 'prune']
    assert saved['units'][MARKET_DATE]['status'] == 'completed'
    assert saved['units'][MARKET_DATE]['result']['stale_prune'][
        'pruned_stale_row_count'
    ] == 1


def test_incomplete_exact_persistence_never_prunes() -> None:
    prune = Mock()
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        dry_repair, _result = _dry_review(checkpoint)
        repair = _repair(
            state_loader=lambda *_args: _state(),
            rebuild_runner=lambda **_kwargs: {
                **_rebuild_result(),
                'features_persisted': 1,
            },
            stale_pruner=prune,
        )
        result = repair.run(
            apply=True,
            expected_manifest_sha256=dry_repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'partial'
    prune.assert_not_called()


def test_failed_exact_verification_never_prunes() -> None:
    prune = Mock()
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        dry_repair, _result = _dry_review(checkpoint)
        repair = _repair(
            state_loader=lambda *_args: _state(),
            exact_verifier=lambda **_kwargs: {
                **_verification_result(),
                'fresh_exact_row_count': 1,
            },
            stale_pruner=prune,
        )
        result = repair.run(
            apply=True,
            expected_manifest_sha256=dry_repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'partial'
    prune.assert_not_called()


def test_source_drift_after_persistence_never_prunes() -> None:
    prune = Mock()
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        reviewed = _state()
        dry_repair, _result = _dry_review(checkpoint, state=reviewed)
        states = iter(
            [reviewed, _state(source_suffix='changed', output_suffix='persisted')]
        )
        repair = _repair(
            state_loader=lambda *_args: next(states),
            stale_pruner=prune,
        )
        result = repair.run(
            apply=True,
            expected_manifest_sha256=dry_repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'partial'
    prune.assert_not_called()


def test_reviewed_source_or_stale_scope_drift_blocks_before_write() -> None:
    rebuild = Mock()
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        dry_repair, _result = _dry_review(checkpoint)
        repair = _repair(
            state_loader=lambda *_args: _state(stale_suffix='changed'),
            rebuild_runner=rebuild,
        )
        result = repair.run(
            apply=True,
            expected_manifest_sha256=dry_repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'reviewed_dry_proof_drift'
    rebuild.assert_not_called()


def test_prune_failure_is_checkpointed_and_resume_retries_same_date() -> None:
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        reviewed = _state()
        dry_repair, _result = _dry_review(checkpoint, state=reviewed)
        first_states = iter(
            [reviewed, _state(output_suffix='persisted')]
        )
        first = _repair(
            state_loader=lambda *_args: next(first_states),
            stale_pruner=lambda **_kwargs: (_ for _ in ()).throw(
                RuntimeError('prune failed')
            ),
        )
        failed = first.run(
            apply=True,
            expected_manifest_sha256=dry_repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
        second_states = iter(
            [
                reviewed,
                _state(output_suffix='persisted-again'),
                _completed_state(output_suffix='resumed'),
            ]
        )
        resumed = _repair(
            state_loader=lambda *_args: next(second_states)
        ).run(
            apply=True,
            expected_manifest_sha256=dry_repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert failed['status'] == 'partial'
    assert resumed['status'] == 'completed'


def test_guard_exit_failure_reconciles_durable_signal_rows() -> None:
    @contextmanager
    def failing_exit_guard(**_kwargs):
        yield True, None
        raise SQLAlchemyError('guard commit failed')

    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        reviewed = _state()
        dry_repair, _result = _dry_review(checkpoint, state=reviewed)
        states = iter(
            [
                reviewed,
                _state(output_suffix='persisted'),
                _completed_state(),
                _completed_state(),
            ]
        )
        result = _repair(
            state_loader=lambda *_args: next(states),
            guard_factory=failing_exit_guard,
        ).run(
            apply=True,
            expected_manifest_sha256=dry_repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )
        saved = json.loads(checkpoint.read_text())

    assert result['status'] == 'partial'
    assert result['applied'] is False
    assert result['reason'] == 'guard_transaction_exit_failed'
    assert result['uncertain_commit'] is True
    assert result['checkpoint_persisted'] is True
    assert result['reconciled_proof_count'] == 1
    assert saved['units'][MARKET_DATE]['last_uncertain_attempt'][
        'live_completion_verified'
    ] is True


def test_checkpoint_failure_reconciles_durable_signal_rows() -> None:
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        reviewed = _state()
        dry_repair, _result = _dry_review(checkpoint, state=reviewed)
        states = iter(
            [
                reviewed,
                _state(output_suffix='persisted'),
                _completed_state(),
                _completed_state(),
            ]
        )
        with patch(
            'app.eod.exact_signal_feature_repair.write_checkpoint',
            side_effect=OSError('checkpoint disk unavailable'),
        ):
            result = _repair(
                state_loader=lambda *_args: next(states)
            ).run(
                apply=True,
                expected_manifest_sha256=dry_repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

    assert result['status'] == 'partial'
    assert result['applied'] is False
    assert result['reason'] == 'checkpoint_write_failed_after_commit'
    assert result['uncertain_commit'] is True
    assert result['checkpoint_persisted'] is False
    assert result['reconciled_proof_count'] == 1
    assert result['error_type'] == 'OSError'


def _completed_checkpoint(checkpoint: Path, *, initial_stale_count=0):
    reviewed_state = (
        _completed_state(output_suffix='before')
        if initial_stale_count == 0
        else _state()
    )
    dry_repair, _result = _dry_review(checkpoint, state=reviewed_state)
    apply_states = iter(
        [
            reviewed_state,
            reviewed_state,
            _completed_state(),
        ]
    )
    applied = _repair(state_loader=lambda *_args: next(apply_states)).run(
        apply=True,
        expected_manifest_sha256=dry_repair.manifest_sha256,
        checkpoint_path=checkpoint,
    )
    assert applied['status'] == 'completed'
    return dry_repair


def test_completed_date_is_skipped_after_live_revalidation() -> None:
    rebuild = Mock()
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair = _completed_checkpoint(checkpoint)
        result = _repair(
            state_loader=lambda *_args: _completed_state(),
            rebuild_runner=rebuild,
        ).run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'completed'
    assert result['skipped_dates'] == 1
    rebuild.assert_not_called()


def test_completed_output_drift_rebuilds_idempotently() -> None:
    rebuild = Mock(return_value=_rebuild_result())
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair = _completed_checkpoint(checkpoint)
        drifted = _state(
            membership=[SYMBOLS[0]],
            output_suffix='deleted-row',
            stale_count=0,
        )
        states = iter(
            [drifted, drifted, _completed_state(output_suffix='repaired')]
        )
        result = _repair(
            state_loader=lambda *_args: next(states),
            rebuild_runner=rebuild,
            stale_pruner=lambda **_kwargs: _prune_result(stale_count=0),
        ).run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'completed'
    rebuild.assert_called_once()


def test_completed_output_drift_rebuilds_after_original_nonzero_prune() -> None:
    rebuild = Mock(return_value=_rebuild_result())
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair = _completed_checkpoint(
            checkpoint,
            initial_stale_count=1,
        )
        drifted = _state(
            membership=[SYMBOLS[0]],
            output_suffix='deleted-row-after-prune',
            stale_count=0,
        )
        states = iter(
            [drifted, drifted, _completed_state(output_suffix='repaired')]
        )
        result = _repair(
            state_loader=lambda *_args: next(states),
            rebuild_runner=rebuild,
            stale_pruner=lambda **_kwargs: _prune_result(stale_count=0),
        ).run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'completed'
    rebuild.assert_called_once()


def test_new_stale_extra_after_completion_requires_new_dry_review() -> None:
    rebuild = Mock()
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair = _completed_checkpoint(checkpoint)
        result = _repair(
            state_loader=lambda *_args: _state(stale_suffix='new-extra'),
            rebuild_runner=rebuild,
        ).run(
            apply=True,
            expected_manifest_sha256=repair.manifest_sha256,
            checkpoint_path=checkpoint,
        )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'reviewed_dry_proof_drift'
    rebuild.assert_not_called()


def test_market_finality_failure_is_checkpointed_without_writes() -> None:
    rebuild = Mock()
    guard = Mock()
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair = _repair(
            state_loader=lambda *_args: _state(),
            rebuild_runner=rebuild,
            guard_factory=guard,
            finality_validator=lambda _market_date: (_ for _ in ()).throw(
                ValueError('market not final')
            ),
        )
        result = repair.run(checkpoint_path=checkpoint)

    assert result['status'] == 'blocked'
    assert result['failed_dates'] == 1
    guard.assert_not_called()
    rebuild.assert_not_called()


def test_active_official_writer_blocks_before_source_review() -> None:
    state_loader = Mock()

    @contextmanager
    def blocked_guard(**_kwargs):
        yield False, 'active_daily_or_eod_writer'

    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        result = _repair(
            state_loader=state_loader,
            guard_factory=blocked_guard,
        ).run(checkpoint_path=checkpoint)

    assert result['status'] == 'blocked'
    assert result['reason'] == 'active_daily_or_eod_writer'
    state_loader.assert_not_called()


def test_manifest_scope_must_match_pending_frozen_membership() -> None:
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair = ExactSignalFeatureRepair(
            _manifest(),
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            pending_batch_loader=lambda: [{'repair_batch_id': 'batch-1'}],
            frozen_membership_loader=lambda **_kwargs: [SYMBOLS[0]],
            completion_state_loader=lambda *_args: _state(),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        result = repair.run(checkpoint_path=checkpoint)
        saved = json.loads(checkpoint.read_text())

    assert result['status'] == 'blocked'
    assert saved.get('units', {}).get(MARKET_DATE, {}).get('error_type') == (
        'ValueError'
    )


def test_checkpoint_bound_to_manifest_and_date_item_hash() -> None:
    with TemporaryDirectory() as temporary:
        checkpoint = Path(temporary) / 'signal-checkpoint.json'
        repair, _result = _dry_review(checkpoint)
        saved = json.loads(checkpoint.read_text())
        saved['manifest_sha256'] = '0' * 64
        checkpoint.write_text(json.dumps(saved))
        result = repair.run(checkpoint_path=checkpoint)
        assert result['reason'] == 'checkpoint_manifest_mismatch'

        saved['manifest_sha256'] = repair.manifest_sha256
        saved['units'][MARKET_DATE]['item_sha256'] = 'f' * 64
        checkpoint.write_text(json.dumps(saved))
        result = repair.run(checkpoint_path=checkpoint)

    assert result['reason'] == 'checkpoint_date_hash_mismatch'


def test_source_fingerprint_contract_requires_complete_factor_coverage() -> None:
    incomplete = _source_fingerprints()
    incomplete['composite_factors']['row_count'] = 1
    assert (
        ExactSignalFeatureRepair._validated_source_fingerprints(
            incomplete,
            expected_symbol_count=len(SYMBOLS),
            expected_membership_sha256=market_membership_sha256(
                SYMBOLS
            ),
        )
        is None
    )


def test_source_fingerprint_contract_allows_full_date_universe_rows() -> None:
    fingerprints = _source_fingerprints()
    fingerprints['piotroski_filter']['row_count'] = len(SYMBOLS) + 3
    fingerprints['market_risk_filter']['row_count'] = len(SYMBOLS) + 4

    assert ExactSignalFeatureRepair._validated_source_fingerprints(
        fingerprints,
        expected_symbol_count=len(SYMBOLS),
        expected_membership_sha256=market_membership_sha256(SYMBOLS),
    ) is not None


@pytest.mark.parametrize(
    ('component', 'row_count'),
    [
        ('piotroski_filter', 0),
        ('piotroski_filter', len(SYMBOLS) - 1),
        ('market_risk_filter', 0),
        ('market_risk_filter', len(SYMBOLS) - 1),
    ],
)
def test_source_fingerprint_contract_rejects_incomplete_filter_coverage(
    component,
    row_count,
) -> None:
    fingerprints = _source_fingerprints()
    fingerprints[component]['row_count'] = row_count
    fingerprints[component]['exact_scope_row_count'] = row_count
    fingerprints[component]['exact_scope_membership_sha256'] = (
        market_membership_sha256(SYMBOLS[:row_count])
    )

    assert ExactSignalFeatureRepair._validated_source_fingerprints(
        fingerprints,
        expected_symbol_count=len(SYMBOLS),
        expected_membership_sha256=market_membership_sha256(SYMBOLS),
    ) is None


@pytest.mark.parametrize(
    'component',
    ['piotroski_filter', 'market_risk_filter'],
)
def test_source_fingerprint_contract_rejects_wrong_filter_membership(
    component,
) -> None:
    fingerprints = _source_fingerprints()
    fingerprints[component]['exact_scope_membership_sha256'] = (
        market_membership_sha256(['999999', SYMBOLS[1]])
    )

    assert ExactSignalFeatureRepair._validated_source_fingerprints(
        fingerprints,
        expected_symbol_count=len(SYMBOLS),
        expected_membership_sha256=market_membership_sha256(SYMBOLS),
    ) is None


def test_reviewed_manifest_hash_matches_factor_manifest_contract() -> None:
    repair = _repair(state_loader=lambda *_args: _state())
    assert repair.manifest_sha256 == canonical_json_sha256(
        repair.manifest
    )
    assert repair.manifest['dates'][0]['membership_sha256'] == (
        market_membership_sha256(SYMBOLS)
    )
    assert date.fromisoformat(repair.manifest['dates'][0]['market_date']) == (
        date(2026, 10, 28)
    )


def _unique_numeric_symbol() -> str:
    return f'{uuid4().int % 1_000_000:06d}'


def _unique_alphanumeric_symbol() -> str:
    return f'{uuid4().int % 10_000:04d}A1'


def _insert_signal_rows(
    *,
    market_date: date,
    symbols: list[str],
    updated_at: datetime,
) -> None:
    with session_scope() as session:
        for symbol in symbols:
            symbol_row = session.scalar(
                select(Symbol).where(Symbol.symbol == symbol)
            )
            if symbol_row is None:
                symbol_row = Symbol(
                    symbol=symbol,
                    name_ko=f'test-{symbol}',
                    market='KOSPI',
                    source='test',
                    is_active=True,
                )
                session.add(symbol_row)
                session.flush()
            session.add(
                DailySignalFeature(
                    symbol_id=symbol_row.id,
                    market_date=market_date,
                    source='test',
                    signal_count=0,
                    feature_payload={
                        'feature_version': 'test',
                        'symbol': symbol,
                        'market_date': market_date.isoformat(),
                        'sources': {},
                        'fallback_features': {
                            'price_action': {'close_price': 1}
                        },
                    },
                    captured_at=updated_at,
                    updated_at=updated_at,
                )
            )


def _cleanup_signal_symbols(symbols: list[str]) -> None:
    with session_scope() as session:
        symbol_ids = list(
            session.scalars(
                select(Symbol.id).where(Symbol.symbol.in_(symbols))
            )
        )
        if symbol_ids:
            session.execute(
                delete(DailySignalFeature).where(
                    DailySignalFeature.symbol_id.in_(symbol_ids)
                )
            )
        session.execute(delete(Symbol).where(Symbol.symbol.in_(symbols)))


def test_database_verifier_rejects_same_count_wrong_symbol() -> None:
    market_date = date(2099, 2, 1)
    expected_symbol = _unique_numeric_symbol()
    wrong_symbol = _unique_numeric_symbol()
    while wrong_symbol == expected_symbol:
        wrong_symbol = _unique_numeric_symbol()
    _insert_signal_rows(
        market_date=market_date,
        symbols=[wrong_symbol],
        updated_at=datetime.now(timezone.utc),
    )
    try:
        with pytest.raises(RuntimeError, match='verification failed'):
            verify_exact_signal_feature_rows(
                market_date=market_date,
                exact_symbols=[expected_symbol],
                repair_started_at=(
                    datetime.now(timezone.utc) - timedelta(seconds=1)
                ),
            )
    finally:
        _cleanup_signal_symbols([wrong_symbol])


def test_database_prune_deletes_only_reviewed_date_local_extras() -> None:
    market_date = date(2099, 2, 2)
    other_date = date(2099, 2, 3)
    exact_symbol = _unique_numeric_symbol()
    stale_symbol = _unique_alphanumeric_symbol()
    symbols = [exact_symbol, stale_symbol]
    updated_at = datetime.now(timezone.utc)
    _insert_signal_rows(
        market_date=market_date,
        symbols=symbols,
        updated_at=updated_at,
    )
    _insert_signal_rows(
        market_date=other_date,
        symbols=[stale_symbol],
        updated_at=updated_at,
    )
    try:
        expected_stale = _load_completion_state(
            market_date.isoformat(),
            [exact_symbol],
        )['stale_extras']
        result = prune_stale_signal_feature_rows(
            market_date=market_date,
            exact_symbols=[exact_symbol],
            repair_started_at=updated_at - timedelta(seconds=1),
            expected_stale_extras=expected_stale,
        )

        assert result['pruned_stale_row_count'] == 1
        with session_scope() as session:
            remaining = list(
                session.scalars(
                    select(Symbol.symbol)
                    .join(
                        DailySignalFeature,
                        DailySignalFeature.symbol_id == Symbol.id,
                    )
                    .where(DailySignalFeature.market_date == market_date)
                )
            )
        assert remaining == [exact_symbol]

        with session_scope() as session:
            assert session.scalar(
                select(DailySignalFeature.id)
                .join(Symbol, Symbol.id == DailySignalFeature.symbol_id)
                .where(
                    DailySignalFeature.market_date == other_date,
                    Symbol.symbol == stale_symbol,
                )
            ) is not None
    finally:
        _cleanup_signal_symbols(symbols)


def test_database_prune_rolls_back_when_stale_content_hash_changed() -> None:
    market_date = date(2099, 2, 4)
    exact_symbol = _unique_numeric_symbol()
    stale_symbol = _unique_numeric_symbol()
    while stale_symbol == exact_symbol:
        stale_symbol = _unique_numeric_symbol()
    symbols = [exact_symbol, stale_symbol]
    updated_at = datetime.now(timezone.utc)
    _insert_signal_rows(
        market_date=market_date,
        symbols=symbols,
        updated_at=updated_at,
    )
    try:
        expected_stale = dict(
            _load_completion_state(
                market_date.isoformat(),
                [exact_symbol],
            )['stale_extras']
        )
        expected_stale['output_sha256'] = '0' * 64
        with pytest.raises(RuntimeError, match='stale extras changed'):
            prune_stale_signal_feature_rows(
                market_date=market_date,
                exact_symbols=[exact_symbol],
                repair_started_at=updated_at - timedelta(seconds=1),
                expected_stale_extras=expected_stale,
            )

        with session_scope() as session:
            remaining = set(
                session.scalars(
                    select(Symbol.symbol)
                    .join(
                        DailySignalFeature,
                        DailySignalFeature.symbol_id == Symbol.id,
                    )
                    .where(DailySignalFeature.market_date == market_date)
                )
            )
        assert remaining == set(symbols)
    finally:
        _cleanup_signal_symbols(symbols)


def test_database_prune_refuses_when_exact_row_is_not_fresh() -> None:
    market_date = date(2099, 2, 5)
    exact_symbol = _unique_numeric_symbol()
    stale_symbol = _unique_numeric_symbol()
    while stale_symbol == exact_symbol:
        stale_symbol = _unique_numeric_symbol()
    symbols = [exact_symbol, stale_symbol]
    repair_started_at = datetime.now(timezone.utc)
    _insert_signal_rows(
        market_date=market_date,
        symbols=symbols,
        updated_at=repair_started_at - timedelta(minutes=1),
    )
    try:
        expected_stale = _load_completion_state(
            market_date.isoformat(),
            [exact_symbol],
        )['stale_extras']
        with pytest.raises(RuntimeError, match='not fully fresh'):
            prune_stale_signal_feature_rows(
                market_date=market_date,
                exact_symbols=[exact_symbol],
                repair_started_at=repair_started_at,
                expected_stale_extras=expected_stale,
            )
    finally:
        _cleanup_signal_symbols(symbols)
