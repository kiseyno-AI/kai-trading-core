"""agent context feedback runs

Revision ID: 20260419_0014
Revises: 20260419_0013
Create Date: 2026-04-19 17:21:30.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260419_0014'
down_revision = '20260419_0013'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'agent_context_feedback_runs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('run_id', sa.String(length=128), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False, server_default='running'),
        sa.Column('symbols_total', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('completed_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('failed_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('request_payload', sa.JSON(), nullable=True),
        sa.Column('summary_payload', sa.JSON(), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('run_id', name='uq_agent_context_feedback_runs_run_id'),
    )
    op.create_index(op.f('ix_agent_context_feedback_runs_run_id'), 'agent_context_feedback_runs', ['run_id'], unique=True)
    op.create_index(op.f('ix_agent_context_feedback_runs_market_date'), 'agent_context_feedback_runs', ['market_date'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_runs_status'), 'agent_context_feedback_runs', ['status'], unique=False)
    op.create_index(
        'ix_agent_context_feedback_runs_market_date_status_started',
        'agent_context_feedback_runs',
        ['market_date', 'status', 'started_at'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_agent_context_feedback_runs_market_date_status_started', table_name='agent_context_feedback_runs')
    op.drop_index(op.f('ix_agent_context_feedback_runs_status'), table_name='agent_context_feedback_runs')
    op.drop_index(op.f('ix_agent_context_feedback_runs_market_date'), table_name='agent_context_feedback_runs')
    op.drop_index(op.f('ix_agent_context_feedback_runs_run_id'), table_name='agent_context_feedback_runs')
    op.drop_table('agent_context_feedback_runs')
