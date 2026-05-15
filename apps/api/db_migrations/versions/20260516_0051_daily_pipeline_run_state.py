"""daily pipeline run state

Revision ID: 20260516_0051
Revises: 20260508_0050
Create Date: 2026-05-16 09:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260516_0051'
down_revision = '20260508_0050'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_pipeline_runs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('trigger', sa.String(length=64), nullable=False, server_default='manual'),
        sa.Column('status', sa.String(length=32), nullable=False, server_default='running'),
        sa.Column('current_stage', sa.String(length=64), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('run_metadata', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_daily_pipeline_runs_market_date'), 'daily_pipeline_runs', ['market_date'], unique=False)
    op.create_index(op.f('ix_daily_pipeline_runs_status'), 'daily_pipeline_runs', ['status'], unique=False)
    op.create_index(op.f('ix_daily_pipeline_runs_current_stage'), 'daily_pipeline_runs', ['current_stage'], unique=False)
    op.create_index(op.f('ix_daily_pipeline_runs_started_at'), 'daily_pipeline_runs', ['started_at'], unique=False)
    op.create_index(
        'ix_daily_pipeline_runs_market_date_status_started',
        'daily_pipeline_runs',
        ['market_date', 'status', 'started_at'],
        unique=False,
    )

    op.create_table(
        'daily_pipeline_stage_runs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('pipeline_run_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('stage_id', sa.String(length=64), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False, server_default='running'),
        sa.Column('processed', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('total', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('succeeded', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('failed', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('remaining', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('current_step', sa.String(length=128), nullable=True),
        sa.Column('current_activity', sa.Text(), nullable=True),
        sa.Column('current_symbol', sa.String(length=32), nullable=True),
        sa.Column('current_period', sa.String(length=32), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('source_run_ids', sa.JSON(), nullable=True),
        sa.Column('details', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['pipeline_run_id'], ['daily_pipeline_runs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('pipeline_run_id', 'stage_id', name='uq_daily_pipeline_stage_runs_run_stage'),
    )
    op.create_index(
        op.f('ix_daily_pipeline_stage_runs_pipeline_run_id'),
        'daily_pipeline_stage_runs',
        ['pipeline_run_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_pipeline_stage_runs_market_date'),
        'daily_pipeline_stage_runs',
        ['market_date'],
        unique=False,
    )
    op.create_index(op.f('ix_daily_pipeline_stage_runs_status'), 'daily_pipeline_stage_runs', ['status'], unique=False)
    op.create_index(op.f('ix_daily_pipeline_stage_runs_started_at'), 'daily_pipeline_stage_runs', ['started_at'], unique=False)
    op.create_index(
        'ix_daily_pipeline_stage_runs_stage_status',
        'daily_pipeline_stage_runs',
        ['stage_id', 'status'],
        unique=False,
    )
    op.create_index(
        'ix_daily_pipeline_stage_runs_market_stage_status',
        'daily_pipeline_stage_runs',
        ['market_date', 'stage_id', 'status'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_daily_pipeline_stage_runs_market_stage_status', table_name='daily_pipeline_stage_runs')
    op.drop_index('ix_daily_pipeline_stage_runs_stage_status', table_name='daily_pipeline_stage_runs')
    op.drop_index(op.f('ix_daily_pipeline_stage_runs_started_at'), table_name='daily_pipeline_stage_runs')
    op.drop_index(op.f('ix_daily_pipeline_stage_runs_status'), table_name='daily_pipeline_stage_runs')
    op.drop_index(op.f('ix_daily_pipeline_stage_runs_market_date'), table_name='daily_pipeline_stage_runs')
    op.drop_index(op.f('ix_daily_pipeline_stage_runs_pipeline_run_id'), table_name='daily_pipeline_stage_runs')
    op.drop_table('daily_pipeline_stage_runs')

    op.drop_index('ix_daily_pipeline_runs_market_date_status_started', table_name='daily_pipeline_runs')
    op.drop_index(op.f('ix_daily_pipeline_runs_started_at'), table_name='daily_pipeline_runs')
    op.drop_index(op.f('ix_daily_pipeline_runs_current_stage'), table_name='daily_pipeline_runs')
    op.drop_index(op.f('ix_daily_pipeline_runs_status'), table_name='daily_pipeline_runs')
    op.drop_index(op.f('ix_daily_pipeline_runs_market_date'), table_name='daily_pipeline_runs')
    op.drop_table('daily_pipeline_runs')
