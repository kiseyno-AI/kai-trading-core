"""Add market-isolated US agent feedback storage."""

from alembic import op
import sqlalchemy as sa


revision = '20260819_0072'
down_revision = '20260819_0071'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'us_agent_feedback',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('agent_name', sa.String(length=64), nullable=False),
        sa.Column('run_id', sa.String(length=128), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('context_payload', sa.JSON(), nullable=False),
        sa.Column('response_payload', sa.JSON(), nullable=False),
        sa.Column('normalized_feedback_payload', sa.JSON(), nullable=False),
        sa.Column('received_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('market_date', 'symbol_id', 'agent_name', name='uq_us_agent_feedback_date_symbol_agent'),
    )
    op.create_index('ix_us_agent_feedback_market_date', 'us_agent_feedback', ['market_date'])
    op.create_index('ix_us_agent_feedback_symbol_id', 'us_agent_feedback', ['symbol_id'])
    op.create_index('ix_us_agent_feedback_agent_name', 'us_agent_feedback', ['agent_name'])
    op.create_index('ix_us_agent_feedback_run_id', 'us_agent_feedback', ['run_id'])
    op.create_index('ix_us_agent_feedback_status', 'us_agent_feedback', ['status'])
    op.create_index('ix_us_agent_feedback_date_agent', 'us_agent_feedback', ['market_date', 'agent_name'])


def downgrade() -> None:
    op.drop_table('us_agent_feedback')
