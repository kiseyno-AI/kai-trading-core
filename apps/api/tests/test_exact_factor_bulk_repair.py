from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import json
from unittest.mock import patch

from app.eod.derived_stage_repair import MANIFEST_SCHEMA_VERSION
from app.eod.exact_factor_bulk_repair import (
    FACTOR_PRICE_HISTORY_ROWS,
    FACTOR_SOURCE_COMPONENTS,
    ExactFactorBulkRepair,
    exact_factor_bulk_guard,
)
from app.eod.exact_factor_repair import (
    FACTOR_RESULT_MODELS,
    MEMBERSHIP_POLICY_STORED_ONLY,
)
from app.eod.market_membership import market_membership_sha256
from app.internal.exact_repair_manifest import canonical_json_sha256


SYMBOLS = ['000660', '005930']
MARKET_DATE = '2026-10-28'


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
            'placeholder_name_count': 0,
        }
    )
    return result


def _apply_result(*, suffix='v1'):
    membership_hash = market_membership_sha256(SYMBOLS)
    return {
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
            'daily_price_input_sha256': 'input-v1',
            'stage_output_fingerprints': _fingerprints(suffix=suffix),
        },
    }


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
        'input_sha256': 'input-v1',
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
    }


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
    unit = saved['units'][MARKET_DATE]
    assert unit['status'] == 'reviewed'
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
        'membership_policy': MEMBERSHIP_POLICY_STORED_ONLY,
    }


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
    repair = _repair(
        service,
        guard_factory=_blocked_guard,
        stored_symbols_loader=lambda _market_date: (_ for _ in ()).throw(
            AssertionError('scope must not be read without the writer fence')
        ),
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


def test_completed_checkpoint_reruns_after_nonprice_source_drift() -> None:
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
    assert resumed['status'] == 'completed'
    assert resumed['skipped_dates'] == 0
    assert service.apply_calls == 2


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

    assert result['status'] == 'partial'
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
