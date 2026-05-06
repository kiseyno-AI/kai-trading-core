"""add generic decider post-decision evaluations

Revision ID: 20260506_0044
Revises: 20260430_0043
Create Date: 2026-05-06 19:10:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260506_0044'
down_revision = '20260430_0043'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'decider_post_decision_evaluations'):
        return

    op.create_table(
        'decider_post_decision_evaluations',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('feedback_id', sa.Integer(), nullable=False),
        sa.Column('decision_snapshot_id', sa.Integer(), nullable=True),
        sa.Column('agent_name', sa.String(length=64), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('symbol', sa.String(length=32), nullable=False),
        sa.Column('window_days', sa.Integer(), nullable=False, server_default='5'),
        sa.Column('decision_action', sa.String(length=32), nullable=False, server_default='wait'),
        sa.Column('entry_trade_date', sa.Date(), nullable=True),
        sa.Column('entry_price', sa.BigInteger(), nullable=True),
        sa.Column('target_price', sa.BigInteger(), nullable=True),
        sa.Column('stop_loss_price', sa.BigInteger(), nullable=True),
        sa.Column('first_target_hit_trade_date', sa.Date(), nullable=True),
        sa.Column('first_stop_hit_trade_date', sa.Date(), nullable=True),
        sa.Column('mfe_pct', sa.Numeric(12, 6), nullable=True),
        sa.Column('mae_pct', sa.Numeric(12, 6), nullable=True),
        sa.Column('outcome_label', sa.String(length=64), nullable=False),
        sa.Column('outcome_reason', sa.Text(), nullable=False),
        sa.Column('data_quality_warnings_payload', sa.JSON(), nullable=False),
        sa.Column('metadata_payload', sa.JSON(), nullable=False),
        sa.Column('computed_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['feedback_id'], ['agent_context_feedback.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['decision_snapshot_id'], ['signal_decider_decision_snapshots.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'feedback_id',
            'window_days',
            name='uq_decider_post_decision_evaluations_feedback_window',
        ),
    )
    op.create_index(
        'ix_decider_post_decision_evaluations_market_agent_window',
        'decider_post_decision_evaluations',
        ['market_date', 'agent_name', 'window_days'],
        unique=False,
    )
    op.create_index(
        'ix_decider_post_decision_evaluations_symbol_market',
        'decider_post_decision_evaluations',
        ['symbol', 'market_date'],
        unique=False,
    )
    op.create_index(
        'ix_decider_post_decision_evaluations_label_market',
        'decider_post_decision_evaluations',
        ['outcome_label', 'market_date'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_feedback_id'),
        'decider_post_decision_evaluations',
        ['feedback_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_decision_snapshot_id'),
        'decider_post_decision_evaluations',
        ['decision_snapshot_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_agent_name'),
        'decider_post_decision_evaluations',
        ['agent_name'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_market_date'),
        'decider_post_decision_evaluations',
        ['market_date'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_symbol'),
        'decider_post_decision_evaluations',
        ['symbol'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_window_days'),
        'decider_post_decision_evaluations',
        ['window_days'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_decision_action'),
        'decider_post_decision_evaluations',
        ['decision_action'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_entry_trade_date'),
        'decider_post_decision_evaluations',
        ['entry_trade_date'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_first_target_hit_trade_date'),
        'decider_post_decision_evaluations',
        ['first_target_hit_trade_date'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_first_stop_hit_trade_date'),
        'decider_post_decision_evaluations',
        ['first_stop_hit_trade_date'],
        unique=False,
    )
    op.create_index(
        op.f('ix_decider_post_decision_evaluations_outcome_label'),
        'decider_post_decision_evaluations',
        ['outcome_label'],
        unique=False,
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, 'decider_post_decision_evaluations'):
        return

    op.drop_index(op.f('ix_decider_post_decision_evaluations_outcome_label'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_first_stop_hit_trade_date'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_first_target_hit_trade_date'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_entry_trade_date'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_decision_action'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_window_days'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_symbol'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_market_date'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_agent_name'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_decision_snapshot_id'), table_name='decider_post_decision_evaluations')
    op.drop_index(op.f('ix_decider_post_decision_evaluations_feedback_id'), table_name='decider_post_decision_evaluations')
    op.drop_index('ix_decider_post_decision_evaluations_label_market', table_name='decider_post_decision_evaluations')
    op.drop_index('ix_decider_post_decision_evaluations_symbol_market', table_name='decider_post_decision_evaluations')
    op.drop_index('ix_decider_post_decision_evaluations_market_agent_window', table_name='decider_post_decision_evaluations')
    op.drop_table('decider_post_decision_evaluations')
