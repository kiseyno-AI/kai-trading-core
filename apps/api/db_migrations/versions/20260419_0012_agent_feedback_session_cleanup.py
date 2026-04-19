"""agent feedback session cleanup failures

Revision ID: 20260419_0012
Revises: 20260417_0011
Create Date: 2026-04-19 13:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260419_0012'
down_revision = '20260417_0011'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'agent_feedback_session_cleanup_failures',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('symbol', sa.String(length=32), nullable=False),
        sa.Column('agent_name', sa.String(length=64), nullable=False),
        sa.Column('run_id', sa.String(length=128), nullable=True),
        sa.Column('session_key', sa.String(length=255), nullable=False),
        sa.Column('provider', sa.String(length=32), nullable=False, server_default='openclaw'),
        sa.Column('delete_transcript', sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column('emit_lifecycle_hooks', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('cleanup_attempts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_attempt_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('next_attempt_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('cleanup_result', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('session_key', name='uq_agent_feedback_session_cleanup_failures_session_key'),
    )
    op.create_index(
        'ix_agent_feedback_session_cleanup_failures_next_attempt',
        'agent_feedback_session_cleanup_failures',
        ['next_attempt_at'],
        unique=False,
    )
    op.create_index(
        'ix_agent_feedback_session_cleanup_failures_market_agent',
        'agent_feedback_session_cleanup_failures',
        ['market_date', 'agent_name'],
        unique=False,
    )
    op.create_index(op.f('ix_agent_feedback_session_cleanup_failures_market_date'), 'agent_feedback_session_cleanup_failures', ['market_date'], unique=False)
    op.create_index(op.f('ix_agent_feedback_session_cleanup_failures_symbol'), 'agent_feedback_session_cleanup_failures', ['symbol'], unique=False)
    op.create_index(op.f('ix_agent_feedback_session_cleanup_failures_agent_name'), 'agent_feedback_session_cleanup_failures', ['agent_name'], unique=False)
    op.create_index(op.f('ix_agent_feedback_session_cleanup_failures_run_id'), 'agent_feedback_session_cleanup_failures', ['run_id'], unique=False)
    op.create_index(op.f('ix_agent_feedback_session_cleanup_failures_last_attempt_at'), 'agent_feedback_session_cleanup_failures', ['last_attempt_at'], unique=False)
    op.create_index(op.f('ix_agent_feedback_session_cleanup_failures_next_attempt_at'), 'agent_feedback_session_cleanup_failures', ['next_attempt_at'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_agent_feedback_session_cleanup_failures_next_attempt_at'), table_name='agent_feedback_session_cleanup_failures')
    op.drop_index(op.f('ix_agent_feedback_session_cleanup_failures_last_attempt_at'), table_name='agent_feedback_session_cleanup_failures')
    op.drop_index(op.f('ix_agent_feedback_session_cleanup_failures_run_id'), table_name='agent_feedback_session_cleanup_failures')
    op.drop_index(op.f('ix_agent_feedback_session_cleanup_failures_agent_name'), table_name='agent_feedback_session_cleanup_failures')
    op.drop_index(op.f('ix_agent_feedback_session_cleanup_failures_symbol'), table_name='agent_feedback_session_cleanup_failures')
    op.drop_index(op.f('ix_agent_feedback_session_cleanup_failures_market_date'), table_name='agent_feedback_session_cleanup_failures')
    op.drop_index('ix_agent_feedback_session_cleanup_failures_market_agent', table_name='agent_feedback_session_cleanup_failures')
    op.drop_index('ix_agent_feedback_session_cleanup_failures_next_attempt', table_name='agent_feedback_session_cleanup_failures')
    op.drop_table('agent_feedback_session_cleanup_failures')
