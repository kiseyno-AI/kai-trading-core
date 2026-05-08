"""dedupe signal-decider snapshots by feedback id

Revision ID: 20260508_0050
Revises: 20260508_0049
Create Date: 2026-05-08 15:30:00
"""

from collections import defaultdict

from alembic import op
import sqlalchemy as sa


revision = '20260508_0050'
down_revision = '20260508_0049'
branch_labels = None
depends_on = None

UNIQUE_INDEX_NAME = 'uq_signal_decider_decision_snapshots_feedback_id_not_null'


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {index['name'] for index in inspector.get_indexes(table_name)}


def _load_duplicate_snapshot_groups(
    bind,
    snapshot_table: sa.Table,
) -> list[tuple[int, int, list[int]]]:
    rows = bind.execute(
        sa.select(snapshot_table.c.id, snapshot_table.c.feedback_id)
        .where(snapshot_table.c.feedback_id.is_not(None))
        .order_by(
            snapshot_table.c.feedback_id.asc(),
            snapshot_table.c.created_at.desc(),
            snapshot_table.c.id.desc(),
        )
    ).all()
    grouped_snapshot_ids: dict[int, list[int]] = defaultdict(list)
    for snapshot_id, feedback_id in rows:
        if feedback_id is None:
            continue
        grouped_snapshot_ids[int(feedback_id)].append(int(snapshot_id))
    duplicate_groups: list[tuple[int, int, list[int]]] = []
    for feedback_id, snapshot_ids in grouped_snapshot_ids.items():
        if len(snapshot_ids) <= 1:
            continue
        duplicate_groups.append((feedback_id, snapshot_ids[0], snapshot_ids[1:]))
    return duplicate_groups


def _prefer_keep_snapshot_row(rows: list[dict], *, keep_snapshot_id: int) -> dict:
    keep_rows = [
        row
        for row in rows
        if int(row['decision_snapshot_id']) == int(keep_snapshot_id)
    ]
    if keep_rows:
        return max(keep_rows, key=lambda row: int(row['id']))
    return max(rows, key=lambda row: int(row['id']))


def _rewrite_review_payload(payload: object, *, keep_snapshot_id: int) -> object:
    if not isinstance(payload, dict):
        return payload
    rewritten = dict(payload)
    rewritten['snapshot_id'] = int(keep_snapshot_id)
    return rewritten


def _repoint_simple_snapshot_fk(bind, table: sa.Table, *, keep_snapshot_id: int, duplicate_snapshot_ids: list[int]) -> None:
    if not duplicate_snapshot_ids:
        return
    bind.execute(
        sa.update(table)
        .where(table.c.decision_snapshot_id.in_(duplicate_snapshot_ids))
        .values(decision_snapshot_id=int(keep_snapshot_id))
    )


def _repair_realized_outcomes(
    bind,
    outcomes_table: sa.Table,
    *,
    feedback_id: int,
    keep_snapshot_id: int,
    duplicate_snapshot_ids: list[int],
) -> tuple[dict[int, int], list[int]]:
    snapshot_ids = [int(keep_snapshot_id), *[int(value) for value in duplicate_snapshot_ids]]
    rows = bind.execute(
        sa.select(outcomes_table).where(outcomes_table.c.decision_snapshot_id.in_(snapshot_ids))
    ).mappings().all()
    rows_by_key: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        rows_by_key[int(row['horizon_days'])].append(dict(row))

    realized_outcome_id_remap: dict[int, int] = {}
    delete_ids: list[int] = []
    for group_rows in rows_by_key.values():
        preferred = _prefer_keep_snapshot_row(group_rows, keep_snapshot_id=int(keep_snapshot_id))
        update_values: dict[str, object] = {}
        if int(preferred['decision_snapshot_id']) != int(keep_snapshot_id):
            update_values['decision_snapshot_id'] = int(keep_snapshot_id)
        if preferred.get('feedback_id') != int(feedback_id):
            update_values['feedback_id'] = int(feedback_id)
        if update_values:
            bind.execute(
                sa.update(outcomes_table)
                .where(outcomes_table.c.id == int(preferred['id']))
                .values(**update_values)
            )
        for row in group_rows:
            if int(row['id']) == int(preferred['id']):
                continue
            realized_outcome_id_remap[int(row['id'])] = int(preferred['id'])
            delete_ids.append(int(row['id']))
    return realized_outcome_id_remap, delete_ids


def _repair_evaluations(
    bind,
    evaluations_table: sa.Table,
    *,
    feedback_id: int,
    keep_snapshot_id: int,
    duplicate_snapshot_ids: list[int],
    realized_outcome_id_remap: dict[int, int],
) -> list[int]:
    snapshot_ids = [int(keep_snapshot_id), *[int(value) for value in duplicate_snapshot_ids]]
    rows = bind.execute(
        sa.select(evaluations_table).where(evaluations_table.c.decision_snapshot_id.in_(snapshot_ids))
    ).mappings().all()
    rows_by_key: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for row in rows:
        rows_by_key[(int(row['horizon_days']), str(row['evaluation_version']))].append(dict(row))

    delete_ids: list[int] = []
    for group_rows in rows_by_key.values():
        preferred = _prefer_keep_snapshot_row(group_rows, keep_snapshot_id=int(keep_snapshot_id))
        update_values: dict[str, object] = {}
        if int(preferred['decision_snapshot_id']) != int(keep_snapshot_id):
            update_values['decision_snapshot_id'] = int(keep_snapshot_id)
        if preferred.get('feedback_id') != int(feedback_id):
            update_values['feedback_id'] = int(feedback_id)
        realized_outcome_id = preferred.get('realized_outcome_id')
        if realized_outcome_id is not None and int(realized_outcome_id) in realized_outcome_id_remap:
            update_values['realized_outcome_id'] = realized_outcome_id_remap[int(realized_outcome_id)]
        if update_values:
            bind.execute(
                sa.update(evaluations_table)
                .where(evaluations_table.c.id == int(preferred['id']))
                .values(**update_values)
            )
        for row in group_rows:
            if int(row['id']) == int(preferred['id']):
                continue
            delete_ids.append(int(row['id']))
    return delete_ids


def _repair_review_items(
    bind,
    review_items_table: sa.Table,
    *,
    keep_snapshot_id: int,
    duplicate_snapshot_ids: list[int],
) -> list[int]:
    snapshot_ids = [int(keep_snapshot_id), *[int(value) for value in duplicate_snapshot_ids]]
    rows = bind.execute(
        sa.select(review_items_table).where(review_items_table.c.decision_snapshot_id.in_(snapshot_ids))
    ).mappings().all()
    rows_by_key: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for row in rows:
        rows_by_key[(str(row['evaluation_version']), int(row['primary_horizon_days']))].append(dict(row))

    delete_ids: list[int] = []
    for group_rows in rows_by_key.values():
        preferred = _prefer_keep_snapshot_row(group_rows, keep_snapshot_id=int(keep_snapshot_id))
        update_values: dict[str, object] = {}
        if int(preferred['decision_snapshot_id']) != int(keep_snapshot_id):
            update_values['decision_snapshot_id'] = int(keep_snapshot_id)
        rewritten_item_payload = _rewrite_review_payload(
            preferred.get('item_payload'),
            keep_snapshot_id=int(keep_snapshot_id),
        )
        if rewritten_item_payload != preferred.get('item_payload'):
            update_values['item_payload'] = rewritten_item_payload
        rewritten_detail_payload = _rewrite_review_payload(
            preferred.get('detail_payload'),
            keep_snapshot_id=int(keep_snapshot_id),
        )
        if rewritten_detail_payload != preferred.get('detail_payload'):
            update_values['detail_payload'] = rewritten_detail_payload
        if update_values:
            bind.execute(
                sa.update(review_items_table)
                .where(review_items_table.c.id == int(preferred['id']))
                .values(**update_values)
            )
        for row in group_rows:
            if int(row['id']) == int(preferred['id']):
                continue
            delete_ids.append(int(row['id']))
    return delete_ids


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, 'signal_decider_decision_snapshots'):
        return

    metadata = sa.MetaData()
    snapshot_table = sa.Table('signal_decider_decision_snapshots', metadata, autoload_with=bind)
    duplicate_groups = _load_duplicate_snapshot_groups(bind, snapshot_table)
    if duplicate_groups:
        tables: dict[str, sa.Table] = {}
        for table_name in (
            'signal_decider_realized_outcomes',
            'signal_decider_evaluations',
            'signal_decider_calendar_review_items',
            'signal_decider_execution_plan_items',
            'signal_decider_actual_execution_outcomes',
            'decider_post_decision_evaluations',
        ):
            if _table_exists(inspector, table_name):
                tables[table_name] = sa.Table(table_name, sa.MetaData(), autoload_with=bind)

        for feedback_id, keep_snapshot_id, duplicate_snapshot_ids in duplicate_groups:
            realized_outcome_id_remap: dict[int, int] = {}
            realized_outcome_delete_ids: list[int] = []
            if 'signal_decider_realized_outcomes' in tables:
                realized_outcome_id_remap, realized_outcome_delete_ids = _repair_realized_outcomes(
                    bind,
                    tables['signal_decider_realized_outcomes'],
                    feedback_id=feedback_id,
                    keep_snapshot_id=keep_snapshot_id,
                    duplicate_snapshot_ids=duplicate_snapshot_ids,
                )

            evaluation_delete_ids: list[int] = []
            if 'signal_decider_evaluations' in tables:
                evaluation_delete_ids = _repair_evaluations(
                    bind,
                    tables['signal_decider_evaluations'],
                    feedback_id=feedback_id,
                    keep_snapshot_id=keep_snapshot_id,
                    duplicate_snapshot_ids=duplicate_snapshot_ids,
                    realized_outcome_id_remap=realized_outcome_id_remap,
                )

            review_item_delete_ids: list[int] = []
            if 'signal_decider_calendar_review_items' in tables:
                review_item_delete_ids = _repair_review_items(
                    bind,
                    tables['signal_decider_calendar_review_items'],
                    keep_snapshot_id=keep_snapshot_id,
                    duplicate_snapshot_ids=duplicate_snapshot_ids,
                )

            if 'signal_decider_execution_plan_items' in tables:
                _repoint_simple_snapshot_fk(
                    bind,
                    tables['signal_decider_execution_plan_items'],
                    keep_snapshot_id=keep_snapshot_id,
                    duplicate_snapshot_ids=duplicate_snapshot_ids,
                )
            if 'signal_decider_actual_execution_outcomes' in tables:
                _repoint_simple_snapshot_fk(
                    bind,
                    tables['signal_decider_actual_execution_outcomes'],
                    keep_snapshot_id=keep_snapshot_id,
                    duplicate_snapshot_ids=duplicate_snapshot_ids,
                )
            if 'decider_post_decision_evaluations' in tables:
                _repoint_simple_snapshot_fk(
                    bind,
                    tables['decider_post_decision_evaluations'],
                    keep_snapshot_id=keep_snapshot_id,
                    duplicate_snapshot_ids=duplicate_snapshot_ids,
                )

            if review_item_delete_ids:
                bind.execute(
                    sa.delete(tables['signal_decider_calendar_review_items']).where(
                        tables['signal_decider_calendar_review_items'].c.id.in_(review_item_delete_ids)
                    )
                )
            if evaluation_delete_ids:
                bind.execute(
                    sa.delete(tables['signal_decider_evaluations']).where(
                        tables['signal_decider_evaluations'].c.id.in_(evaluation_delete_ids)
                    )
                )
            if realized_outcome_delete_ids:
                bind.execute(
                    sa.delete(tables['signal_decider_realized_outcomes']).where(
                        tables['signal_decider_realized_outcomes'].c.id.in_(realized_outcome_delete_ids)
                    )
                )

            bind.execute(
                sa.delete(snapshot_table).where(snapshot_table.c.id.in_(duplicate_snapshot_ids))
            )

    inspector = sa.inspect(bind)
    index_names = _index_names(inspector, 'signal_decider_decision_snapshots')
    if UNIQUE_INDEX_NAME not in index_names:
        op.create_index(
            UNIQUE_INDEX_NAME,
            'signal_decider_decision_snapshots',
            ['feedback_id'],
            unique=True,
            postgresql_where=sa.text('feedback_id IS NOT NULL'),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, 'signal_decider_decision_snapshots'):
        return
    if UNIQUE_INDEX_NAME in _index_names(inspector, 'signal_decider_decision_snapshots'):
        op.drop_index(UNIQUE_INDEX_NAME, table_name='signal_decider_decision_snapshots')
