"""add agent context feedback

Revision ID: 20260415_0009
Revises: 20260414_0008
Create Date: 2026-04-15 09:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260415_0009'
down_revision = '20260414_0008'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'agent_context_feedback'):
        return

    op.create_table(
        'agent_context_feedback',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('symbol', sa.String(length=32), nullable=False),
        sa.Column('agent_name', sa.String(length=64), nullable=False),
        sa.Column('agent_version', sa.String(length=64), nullable=True),
        sa.Column('run_id', sa.String(length=128), nullable=True),
        sa.Column('summary_payload', sa.JSON(), nullable=False),
        sa.Column('feedback_payload', sa.JSON(), nullable=False),
        sa.Column('context_payload', sa.JSON(), nullable=True),
        sa.Column('received_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_agent_context_feedback_market_date'), 'agent_context_feedback', ['market_date'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_symbol'), 'agent_context_feedback', ['symbol'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_agent_name'), 'agent_context_feedback', ['agent_name'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_run_id'), 'agent_context_feedback', ['run_id'], unique=False)
    op.create_index(op.f('ix_agent_context_feedback_received_at'), 'agent_context_feedback', ['received_at'], unique=False)
    op.create_index(
        'ix_agent_context_feedback_market_symbol_agent_received',
        'agent_context_feedback',
        ['market_date', 'symbol', 'agent_name', 'received_at'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_agent_context_feedback_market_symbol_agent_received', table_name='agent_context_feedback')
    op.drop_index(op.f('ix_agent_context_feedback_received_at'), table_name='agent_context_feedback')
    op.drop_index(op.f('ix_agent_context_feedback_run_id'), table_name='agent_context_feedback')
    op.drop_index(op.f('ix_agent_context_feedback_agent_name'), table_name='agent_context_feedback')
    op.drop_index(op.f('ix_agent_context_feedback_symbol'), table_name='agent_context_feedback')
    op.drop_index(op.f('ix_agent_context_feedback_market_date'), table_name='agent_context_feedback')
    op.drop_table('agent_context_feedback')
