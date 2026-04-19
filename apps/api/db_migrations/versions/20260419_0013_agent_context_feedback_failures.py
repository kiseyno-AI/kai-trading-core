"""agent context feedback failures

Revision ID: 20260419_0013
Revises: 20260419_0012
Create Date: 2026-04-19 16:41:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260419_0013'
down_revision = '20260419_0012'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'agent_context_feedback_failures',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('symbol', sa.String(length=32), nullable=False),
        sa.Column('agent_name', sa.String(length=64), nullable=False),
        sa.Column('run_id', sa.String(length=128), nullable=True),
        sa.Column('failure_kind', sa.String(length=64), nullable=False),
        sa.Column('retryable', sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column('summary_payload', sa.JSON(), nullable=False),
        sa.Column('context_payload', sa.JSON(), nullable=True),
        sa.Column('received_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('market_date', 'symbol', 'agent_name', name='uq_agent_context_feedback_failures_market_symbol_agent'),
    )
    op.create_index(
        'ix_agent_context_feedback_failures_market_symbol_agent_received',
        'agent_context_feedback_failures',
        ['market_date', 'symbol', 'agent_name', 'received_at'],
        unique=False,
    )
    op.create_index(op.f('ix_agent_context_feedback_failures_market_date'), 'agent_context_feedback_failures', ['market_date'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_failures_symbol'), 'agent_context_feedback_failures', ['symbol'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_failures_agent_name'), 'agent_context_feedback_failures', ['agent_name'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_failures_run_id'), 'agent_context_feedback_failures', ['run_id'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_failures_failure_kind'), 'agent_context_feedback_failures', ['failure_kind'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_failures_retryable'), 'agent_context_feedback_failures', ['retryable'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_failures_received_at'), 'agent_context_feedback_failures', ['received_at'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_agent_context_feedback_failures_received_at'), table_name='agent_context_feedback_failures')
    op.drop_index(op.f('ix_agent_context_feedback_failures_retryable'), table_name='agent_context_feedback_failures')
    op.drop_index(op.f('ix_agent_context_feedback_failures_failure_kind'), table_name='agent_context_feedback_failures')
    op.drop_index(op.f('ix_agent_context_feedback_failures_run_id'), table_name='agent_context_feedback_failures')
    op.drop_index(op.f('ix_agent_context_feedback_failures_agent_name'), table_name='agent_context_feedback_failures')
    op.drop_index(op.f('ix_agent_context_feedback_failures_symbol'), table_name='agent_context_feedback_failures')
    op.drop_index(op.f('ix_agent_context_feedback_failures_market_date'), table_name='agent_context_feedback_failures')
    op.drop_index('ix_agent_context_feedback_failures_market_symbol_agent_received', table_name='agent_context_feedback_failures')
    op.drop_table('agent_context_feedback_failures')
