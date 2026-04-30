"""signal decider calendar review read model

Revision ID: 20260430_0043
Revises: 20260430_0042
Create Date: 2026-04-30 17:15:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260430_0043'
down_revision = '20260430_0042'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {index['name'] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'signal_decider_calendar_review_runs'):
        op.create_table(
            'signal_decider_calendar_review_runs',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('market_date_from', sa.Date(), nullable=False),
            sa.Column('market_date_to', sa.Date(), nullable=False),
            sa.Column('as_of_market_date', sa.Date(), nullable=True),
            sa.Column('lookback_days', sa.Integer(), nullable=True),
            sa.Column('evaluation_version', sa.String(length=64), nullable=False, server_default='signal_decider_eval_v1'),
            sa.Column('horizons_payload', sa.JSON(), nullable=False),
            sa.Column('primary_horizons_payload', sa.JSON(), nullable=False),
            sa.Column('symbols_payload', sa.JSON(), nullable=True),
            sa.Column('trigger', sa.String(length=64), nullable=False, server_default='manual'),
            sa.Column('status', sa.String(length=32), nullable=False, server_default='running'),
            sa.Column('force_recompute', sa.Boolean(), nullable=False, server_default=sa.text('false')),
            sa.Column('summary_payload', sa.JSON(), nullable=True),
            sa.Column('error_message', sa.Text(), nullable=True),
            sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.PrimaryKeyConstraint('id'),
        )

    inspector = sa.inspect(bind)
    existing_run_indexes = _index_names(inspector, 'signal_decider_calendar_review_runs')
    run_indexes = (
        ('ix_signal_decider_calendar_review_runs_market_date_from', ['market_date_from']),
        ('ix_signal_decider_calendar_review_runs_market_date_to', ['market_date_to']),
        ('ix_signal_decider_calendar_review_runs_as_of_market_date', ['as_of_market_date']),
        ('ix_signal_decider_calendar_review_runs_evaluation_version', ['evaluation_version']),
        ('ix_signal_decider_calendar_review_runs_trigger', ['trigger']),
        ('ix_signal_decider_calendar_review_runs_status', ['status']),
        ('ix_signal_decider_calendar_review_runs_completed_at', ['completed_at']),
        (
            'ix_signal_decider_calendar_review_runs_window_status_created',
            ['market_date_from', 'market_date_to', 'status', 'created_at'],
        ),
        (
            'ix_signal_decider_calendar_review_runs_asof_status_created',
            ['as_of_market_date', 'status', 'created_at'],
        ),
    )
    for index_name, columns in run_indexes:
        if index_name not in existing_run_indexes:
            op.create_index(index_name, 'signal_decider_calendar_review_runs', columns, unique=False)
            existing_run_indexes.add(index_name)

    if not _table_exists(inspector, 'signal_decider_calendar_review_items'):
        op.create_table(
            'signal_decider_calendar_review_items',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('decision_snapshot_id', sa.Integer(), nullable=False),
            sa.Column('review_run_id', sa.Integer(), nullable=True),
            sa.Column('market_date', sa.Date(), nullable=False),
            sa.Column('symbol', sa.String(length=32), nullable=False),
            sa.Column('policy_version', sa.String(length=64), nullable=False),
            sa.Column('decision_action', sa.String(length=16), nullable=False),
            sa.Column('evaluation_version', sa.String(length=64), nullable=False, server_default='signal_decider_eval_v1'),
            sa.Column('primary_horizon_days', sa.Integer(), nullable=False),
            sa.Column('review_status', sa.String(length=32), nullable=False),
            sa.Column('insight_code', sa.String(length=64), nullable=False),
            sa.Column('insight_priority', sa.String(length=16), nullable=False),
            sa.Column('quality_verdict', sa.String(length=64), nullable=False),
            sa.Column('quality_reason_code', sa.String(length=64), nullable=True),
            sa.Column('quality_reason_text', sa.Text(), nullable=True),
            sa.Column('review_conclusion', sa.Text(), nullable=False),
            sa.Column('review_summary_text', sa.Text(), nullable=False),
            sa.Column('path_outcome', sa.String(length=32), nullable=False),
            sa.Column('intraday_path_result', sa.String(length=32), nullable=False),
            sa.Column('item_payload', sa.JSON(), nullable=False),
            sa.Column('detail_payload', sa.JSON(), nullable=False),
            sa.Column('refreshed_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.ForeignKeyConstraint(
                ['decision_snapshot_id'],
                ['signal_decider_decision_snapshots.id'],
                ondelete='CASCADE',
            ),
            sa.ForeignKeyConstraint(
                ['review_run_id'],
                ['signal_decider_calendar_review_runs.id'],
                ondelete='SET NULL',
            ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint(
                'decision_snapshot_id',
                'evaluation_version',
                'primary_horizon_days',
                name='uq_signal_decider_calendar_review_items_snapshot_version_primary',
            ),
        )

    inspector = sa.inspect(bind)
    existing_item_indexes = _index_names(inspector, 'signal_decider_calendar_review_items')
    item_indexes = (
        ('ix_signal_decider_calendar_review_items_decision_snapshot_id', ['decision_snapshot_id']),
        ('ix_signal_decider_calendar_review_items_review_run_id', ['review_run_id']),
        ('ix_signal_decider_calendar_review_items_market_date', ['market_date']),
        ('ix_signal_decider_calendar_review_items_symbol', ['symbol']),
        ('ix_signal_decider_calendar_review_items_policy_version', ['policy_version']),
        ('ix_signal_decider_calendar_review_items_decision_action', ['decision_action']),
        ('ix_signal_decider_calendar_review_items_evaluation_version', ['evaluation_version']),
        ('ix_signal_decider_calendar_review_items_primary_horizon_days', ['primary_horizon_days']),
        ('ix_signal_decider_calendar_review_items_review_status', ['review_status']),
        ('ix_signal_decider_calendar_review_items_insight_code', ['insight_code']),
        ('ix_signal_decider_calendar_review_items_insight_priority', ['insight_priority']),
        ('ix_signal_decider_calendar_review_items_quality_verdict', ['quality_verdict']),
        ('ix_signal_decider_calendar_review_items_path_outcome', ['path_outcome']),
        ('ix_signal_decider_calendar_review_items_intraday_path_result', ['intraday_path_result']),
        ('ix_signal_decider_calendar_review_items_refreshed_at', ['refreshed_at']),
        (
            'ix_signal_decider_calendar_review_items_market_version_primary',
            ['market_date', 'evaluation_version', 'primary_horizon_days'],
        ),
        (
            'ix_signal_decider_calendar_review_items_symbol_market_primary',
            ['symbol', 'market_date', 'primary_horizon_days'],
        ),
        (
            'ix_signal_decider_calendar_review_items_status_market',
            ['review_status', 'market_date'],
        ),
        (
            'ix_signal_decider_calendar_review_items_insight_market',
            ['insight_code', 'market_date'],
        ),
    )
    for index_name, columns in item_indexes:
        if index_name not in existing_item_indexes:
            op.create_index(index_name, 'signal_decider_calendar_review_items', columns, unique=False)
            existing_item_indexes.add(index_name)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'signal_decider_calendar_review_items'):
        op.drop_table('signal_decider_calendar_review_items')

    inspector = sa.inspect(bind)
    if _table_exists(inspector, 'signal_decider_calendar_review_runs'):
        op.drop_table('signal_decider_calendar_review_runs')
