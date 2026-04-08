
"""add daily action summaries

Revision ID: 20260408_0005
Revises: 20260408_0004
Create Date: 2026-04-08 13:20:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260408_0005'
down_revision = '20260408_0004'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'daily_action_summaries'):
        return

    op.create_table(
        'daily_action_summaries',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('technical_snapshot_id', sa.Integer(), nullable=True),
        sa.Column('eod_run_id', sa.Integer(), nullable=True),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('action_label', sa.String(length=32), nullable=False),
        sa.Column('bias_direction', sa.String(length=16), nullable=False),
        sa.Column('signal_score', sa.Integer(), nullable=False),
        sa.Column('confidence_tier', sa.String(length=16), nullable=False),
        sa.Column('risk_tier', sa.String(length=16), nullable=False),
        sa.Column('trigger_price', sa.Numeric(precision=14, scale=4), nullable=True),
        sa.Column('invalidation_price', sa.Numeric(precision=14, scale=4), nullable=True),
        sa.Column('support_price', sa.Numeric(precision=14, scale=4), nullable=True),
        sa.Column('resistance_price', sa.Numeric(precision=14, scale=4), nullable=True),
        sa.Column('reasons', sa.JSON(), nullable=False),
        sa.Column('summary_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['eod_run_id'], ['eod_collection_runs.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['technical_snapshot_id'], ['daily_technical_snapshots.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_action_summaries_symbol_market_date'),
    )
    op.create_index(op.f('ix_daily_action_summaries_symbol_id'), 'daily_action_summaries', ['symbol_id'], unique=False)
    op.create_index(
        op.f('ix_daily_action_summaries_technical_snapshot_id'),
        'daily_action_summaries',
        ['technical_snapshot_id'],
        unique=False,
    )
    op.create_index(op.f('ix_daily_action_summaries_eod_run_id'), 'daily_action_summaries', ['eod_run_id'], unique=False)
    op.create_index(op.f('ix_daily_action_summaries_market_date'), 'daily_action_summaries', ['market_date'], unique=False)
    op.create_index(op.f('ix_daily_action_summaries_action_label'), 'daily_action_summaries', ['action_label'], unique=False)
    op.create_index(op.f('ix_daily_action_summaries_bias_direction'), 'daily_action_summaries', ['bias_direction'], unique=False)
    op.create_index(op.f('ix_daily_action_summaries_signal_score'), 'daily_action_summaries', ['signal_score'], unique=False)
    op.create_index(
        op.f('ix_daily_action_summaries_confidence_tier'),
        'daily_action_summaries',
        ['confidence_tier'],
        unique=False,
    )
    op.create_index(op.f('ix_daily_action_summaries_risk_tier'), 'daily_action_summaries', ['risk_tier'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_daily_action_summaries_risk_tier'), table_name='daily_action_summaries')
    op.drop_index(op.f('ix_daily_action_summaries_confidence_tier'), table_name='daily_action_summaries')
    op.drop_index(op.f('ix_daily_action_summaries_signal_score'), table_name='daily_action_summaries')
    op.drop_index(op.f('ix_daily_action_summaries_bias_direction'), table_name='daily_action_summaries')
    op.drop_index(op.f('ix_daily_action_summaries_action_label'), table_name='daily_action_summaries')
    op.drop_index(op.f('ix_daily_action_summaries_market_date'), table_name='daily_action_summaries')
    op.drop_index(op.f('ix_daily_action_summaries_eod_run_id'), table_name='daily_action_summaries')
    op.drop_index(op.f('ix_daily_action_summaries_technical_snapshot_id'), table_name='daily_action_summaries')
    op.drop_index(op.f('ix_daily_action_summaries_symbol_id'), table_name='daily_action_summaries')
    op.drop_table('daily_action_summaries')
