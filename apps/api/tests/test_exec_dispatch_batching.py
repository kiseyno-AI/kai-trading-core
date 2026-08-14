from types import SimpleNamespace

from app.orders.exec_integration import (
    _delta_dispatch_batch_key,
    _delta_dispatch_plan_id,
    _partition_exec_dispatch_rows,
)


def _row(
    row_id: int,
    snapshot_id: int,
    symbol: str,
    dispatch_status: str | None = None,
) -> SimpleNamespace:
    exec_dispatch = {'dispatch_status': dispatch_status} if dispatch_status else {}
    return SimpleNamespace(
        id=row_id,
        decision_snapshot_id=snapshot_id,
        symbol=symbol,
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

    assert [row.id for row in already_dispatched] == [1, 2]
    assert [row.id for row in pending] == [3, 4]


def test_partition_exec_dispatch_rows_force_preserves_existing_redispatch_behavior() -> None:
    rows = [_row(1, 101, '005930', 'accepted'), _row(2, 102, '000660')]

    already_dispatched, pending = _partition_exec_dispatch_rows(rows, force=True)

    assert already_dispatched == []
    assert pending == rows


def test_delta_dispatch_batch_key_is_stable_and_changes_with_feedback_snapshot() -> None:
    first = _row(1, 101, '005930')
    second = _row(2, 102, '000660')

    key = _delta_dispatch_batch_key([first, second])

    assert _delta_dispatch_batch_key([second, first]) == key
    assert _delta_dispatch_batch_key([first, _row(2, 999, '000660')]) != key


def test_delta_dispatch_plan_id_keeps_suffix_within_exec_contract_limit() -> None:
    batch_key = '123456789abc'
    plan_id = _delta_dispatch_plan_id('core-' + ('x' * 180), batch_key)

    assert len(plan_id) == 128
    assert plan_id.endswith(f'-delta-{batch_key}')
