"""Add durable US-equity pipeline run claims."""

from alembic import op
import sqlalchemy as sa


revision = '20260819_0073'
down_revision = '20260819_0072'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'us_equity_pipeline_runs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('pipeline_version', sa.String(length=32), nullable=False),
        sa.Column('run_id', sa.String(length=128), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('attempt_count', sa.Integer(), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('stages_payload', sa.JSON(), nullable=False),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('market_date', 'pipeline_version', name='uq_us_equity_pipeline_date_version'),
    )
    op.create_index('ix_us_equity_pipeline_runs_market_date', 'us_equity_pipeline_runs', ['market_date'])
    op.create_index('ix_us_equity_pipeline_runs_run_id', 'us_equity_pipeline_runs', ['run_id'])
    op.create_index('ix_us_equity_pipeline_runs_status', 'us_equity_pipeline_runs', ['status'])
    op.create_index(
        'ix_us_equity_pipeline_status_heartbeat',
        'us_equity_pipeline_runs',
        ['status', 'heartbeat_at'],
    )


def downgrade() -> None:
    op.drop_table('us_equity_pipeline_runs')
