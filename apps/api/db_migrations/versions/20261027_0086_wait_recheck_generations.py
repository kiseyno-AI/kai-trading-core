"""preserve lineage across bounded same-day WAIT rechecks

Existing evidence rows become generation one with no parent.  A rearmed event
must retain its predecessor and uses a new source snapshot, so every model
decision remains independently auditable instead of overwriting the first
touch. Failed provider attempts keep append-only context history before the
same event envelope is refreshed by new realtime evidence. Downgrade is
intentionally refused once a lane has multiple durable generations because the
former uniqueness contract cannot represent them.

Revision ID: 20261027_0086
Revises: 20261025_0085
Create Date: 2026-10-27 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20261027_0086'
down_revision = '20261025_0085'
branch_labels = None
depends_on = None


TABLE_NAME = 'signal_decider_wait_recheck_events'
OLD_UNIQUE_NAME = 'uq_signal_decider_wait_recheck_target_symbol_agent'
NEW_UNIQUE_NAME = (
    'uq_signal_decider_wait_recheck_target_symbol_agent_source'
)
GENERATION_CHECK_NAME = 'ck_signal_decider_wait_recheck_generation_lineage'
REARMED_FROM_FK_NAME = 'fk_signal_decider_wait_recheck_rearmed_from'
REARMED_FROM_INDEX_NAME = 'ix_signal_decider_wait_recheck_rearmed_from'


def upgrade() -> None:
    op.add_column(
        TABLE_NAME,
        sa.Column(
            'generation',
            sa.Integer(),
            server_default=sa.text('1'),
            nullable=False,
        ),
    )
    op.add_column(
        TABLE_NAME,
        sa.Column('rearmed_from_event_id', sa.Integer(), nullable=True),
    )
    op.add_column(
        TABLE_NAME,
        sa.Column(
            'attempt_history_payload',
            sa.JSON(),
            server_default=sa.text("'[]'::json"),
            nullable=False,
        ),
    )
    op.create_foreign_key(
        REARMED_FROM_FK_NAME,
        TABLE_NAME,
        TABLE_NAME,
        ['rearmed_from_event_id'],
        ['id'],
        ondelete='RESTRICT',
    )
    op.create_index(
        REARMED_FROM_INDEX_NAME,
        TABLE_NAME,
        ['rearmed_from_event_id'],
        unique=False,
    )
    op.create_check_constraint(
        GENERATION_CHECK_NAME,
        TABLE_NAME,
        'generation >= 1 '
        'AND ((generation = 1 AND rearmed_from_event_id IS NULL) '
        'OR (generation > 1 AND rearmed_from_event_id IS NOT NULL)) '
        'AND (rearmed_from_event_id IS NULL OR rearmed_from_event_id <> id)',
    )
    op.drop_constraint(
        OLD_UNIQUE_NAME,
        TABLE_NAME,
        type_='unique',
    )
    op.create_unique_constraint(
        NEW_UNIQUE_NAME,
        TABLE_NAME,
        [
            'target_trade_date',
            'symbol',
            'agent_name',
            'source_snapshot_id',
        ],
    )


def downgrade() -> None:
    multi_generation_row = op.get_bind().execute(
        sa.text(
            """
            SELECT target_trade_date, symbol, agent_name,
                   COUNT(*) AS generation_count
            FROM signal_decider_wait_recheck_events
            GROUP BY target_trade_date, symbol, agent_name
            HAVING COUNT(*) > 1
                OR MAX(generation) > 1
                OR COUNT(rearmed_from_event_id) > 0
                OR BOOL_OR(
                    attempt_history_payload::jsonb <> '[]'::jsonb
                )
            LIMIT 1
            """
        )
    ).first()
    if multi_generation_row is not None:
        raise RuntimeError(
            '20261027_0086 downgrade is forward-only after same-lane '
            'multi-generation WAIT recheck evidence or retry attempt audit '
            'history exists; do not delete evidence rows, apply a forward '
            'migration instead'
        )

    op.drop_constraint(
        NEW_UNIQUE_NAME,
        TABLE_NAME,
        type_='unique',
    )
    op.create_unique_constraint(
        OLD_UNIQUE_NAME,
        TABLE_NAME,
        ['target_trade_date', 'symbol', 'agent_name'],
    )
    op.drop_constraint(
        GENERATION_CHECK_NAME,
        TABLE_NAME,
        type_='check',
    )
    op.drop_index(REARMED_FROM_INDEX_NAME, table_name=TABLE_NAME)
    op.drop_constraint(
        REARMED_FROM_FK_NAME,
        TABLE_NAME,
        type_='foreignkey',
    )
    op.drop_column(TABLE_NAME, 'rearmed_from_event_id')
    op.drop_column(TABLE_NAME, 'attempt_history_payload')
    op.drop_column(TABLE_NAME, 'generation')
