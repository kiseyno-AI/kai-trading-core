import hashlib
import json
from types import SimpleNamespace

from app.orders.exec_integration import (
    _delta_dispatch_batch_key,
    _delta_dispatch_plan_id,
    _partition_exec_dispatch_rows,
    _row_has_accepted_exec_dispatch,
)
from app.orders.execution_handoff_contract import (
    EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
    EXECUTION_HANDOFF_CAPABILITY,
    EXECUTION_HANDOFF_CONTRACT_VERSION,
)


def _row(
    row_id: int,
    snapshot_id: int,
    symbol: str,
    dispatch_status: str | None = None,
) -> SimpleNamespace:
    core_item_key = f'row-{row_id}:next_open_buy'
    exec_dispatch = {'dispatch_status': dispatch_status} if dispatch_status else {}
    if dispatch_status == 'accepted':
        dispatch_material_fingerprint = f'{row_id:064x}'
        execution_plan_id = f'exec-plan-{row_id}'
        destination_material = {
            'schema_version': 'core_exec_destination_binding_v1',
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'base_url': 'http://exec.test:80',
            'runtime_mode': 'mock',
            'dispatch_source': 'kai-trading-core',
            'account_reference': 'KIS:98123456-01',
        }
        destination_binding = {
            **destination_material,
            'destination_fingerprint': hashlib.sha256(
                json.dumps(
                    destination_material,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(',', ':'),
                ).encode('utf-8')
            ).hexdigest(),
        }
        contract_ack = {
            'schema_version': EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'capability': EXECUTION_HANDOFF_CAPABILITY,
            'plan_id': f'plan-{row_id}',
            'status': 'accepted',
            'dispatch_material_fingerprint': dispatch_material_fingerprint,
        }
        exec_dispatch.update(
            {
                'plan_id': f'plan-{row_id}',
                'destination_binding': destination_binding,
                'item_links': [
                    {
                        'core_item_key': core_item_key,
                        'exec_plan_id': execution_plan_id,
                        'exec_plan_item_id': f'exec-item-{row_id}',
                        'remote_status': 'accepted',
                    }
                ],
                'contract_proof': {
                    'schema_version': 'core_exec_dispatch_proof_v2',
                    'execution_plan_id': execution_plan_id,
                    'contract_ack': contract_ack,
                    'dispatch_material_fingerprint': dispatch_material_fingerprint,
                    'destination_binding': destination_binding,
                    'submitted_core_item_keys': [core_item_key],
                    'accepted_item_bindings': [
                        {
                            'core_item_key': core_item_key,
                            'execution_plan_id': execution_plan_id,
                            'execution_plan_item_id': f'exec-item-{row_id}',
                        }
                    ],
                    'accepted_item_count': 1,
                    'rejected_item_count': 0,
                },
                'last_request_summary': {
                    'metadata': {
                        'dispatch_material_fingerprint': dispatch_material_fingerprint,
                    },
                },
                'last_response_summary': {
                    'execution_plan_id': execution_plan_id,
                    'contract_ack': contract_ack,
                    'row_items': [
                        {
                            'core_item_key': core_item_key,
                            'execution_plan_id': execution_plan_id,
                            'execution_plan_item_id': f'exec-item-{row_id}',
                            'status': 'accepted',
                        }
                    ],
                },
            }
        )
    return SimpleNamespace(
        id=row_id,
        decision_snapshot_id=snapshot_id,
        symbol=symbol,
        account_reference='KIS:98123456-01',
        validation_payload={},
        submission_payload={'exec_dispatch': exec_dispatch},
    )


def test_partition_exec_dispatch_rows_sends_only_rows_not_already_accepted() -> None:
    accepted = _row(1, 101, '005930', 'accepted')
    partial = _row(2, 102, '000660', 'partial')
    failed = _row(3, 103, '035420', 'failed')
    new = _row(4, 104, '035720')

    already_dispatched, pending = _partition_exec_dispatch_rows(
        [accepted, partial, failed, new],
        force=False,
    )

    assert [row.id for row in already_dispatched] == [1]
    assert [row.id for row in pending] == [2, 3, 4]


def test_legacy_or_incomplete_accepted_dispatch_is_not_trusted_as_v4_proof() -> None:
    legacy = _row(1, 101, '005930')
    legacy.submission_payload = {
        'exec_dispatch': {'dispatch_status': 'accepted', 'plan_id': 'legacy-plan'},
    }
    incomplete = _row(2, 102, '000660', 'accepted')
    incomplete.submission_payload['exec_dispatch']['item_links'][0][
        'exec_plan_item_id'
    ] = None

    assert _row_has_accepted_exec_dispatch(legacy) is False
    assert _row_has_accepted_exec_dispatch(incomplete) is False
    assert _row_has_accepted_exec_dispatch(_row(3, 103, '035420', 'accepted')) is True
    guarded, pending = _partition_exec_dispatch_rows(
        [legacy, incomplete],
        force=False,
    )
    assert guarded == [legacy, incomplete]
    assert pending == []


def test_accepted_dispatch_proof_rejects_duplicate_remote_item_ids() -> None:
    row = _row(1, 101, '005930', 'accepted')
    exec_dispatch = row.submission_payload['exec_dispatch']
    first_link = exec_dispatch['item_links'][0]
    second_key = 'row-1:target_sell'
    exec_dispatch['contract_proof']['submitted_core_item_keys'].append(second_key)
    exec_dispatch['contract_proof']['accepted_item_count'] = 2
    exec_dispatch['item_links'].append(
        {
            **first_link,
            'core_item_key': second_key,
        }
    )

    assert _row_has_accepted_exec_dispatch(row) is False


def test_partition_exec_dispatch_rows_force_never_reposts_remote_authority_claims() -> None:
    rows = [_row(1, 101, '005930', 'accepted'), _row(2, 102, '000660')]

    guarded, pending = _partition_exec_dispatch_rows(rows, force=True)

    assert guarded == [rows[0]]
    assert pending == [rows[1]]


def test_delta_dispatch_batch_key_is_stable_and_changes_with_feedback_snapshot() -> None:
    first = _row(1, 101, '005930')
    second = _row(2, 102, '000660')

    key = _delta_dispatch_batch_key([first, second])

    assert _delta_dispatch_batch_key([second, first]) == key
    assert _delta_dispatch_batch_key([first, _row(2, 999, '000660')]) != key
    policy_revision = _row(1, 101, '005930')
    policy_revision.validation_payload = {
        'preopen_policy_fingerprint': 'next_open_preopen_policy_v1:changed',
    }
    assert _delta_dispatch_batch_key([policy_revision, second]) != key
    redispatch_generation = _row(1, 101, '005930')
    redispatch_generation.validation_payload = {
        'dispatch_generation_nonce': 'incident-recovery-2',
    }
    assert _delta_dispatch_batch_key([redispatch_generation, second]) != key


def test_delta_dispatch_plan_id_keeps_suffix_within_exec_contract_limit() -> None:
    batch_key = '123456789abc'
    plan_id = _delta_dispatch_plan_id('core-' + ('x' * 180), batch_key)

    assert len(plan_id) == 128
    assert plan_id.endswith(f'-delta-{batch_key}')
