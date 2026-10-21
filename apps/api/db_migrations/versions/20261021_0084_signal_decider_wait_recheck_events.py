"""persist one-shot intraday WAIT recheck evidence

Revision ID: 20261021_0084
Revises: 20261015_0083
Create Date: 2026-10-21 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20261021_0084'
down_revision = '20261015_0083'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'signal_decider_wait_recheck_events',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('source_snapshot_id', sa.Integer(), nullable=False),
        sa.Column('result_snapshot_id', sa.Integer(), nullable=True),
        sa.Column('source_snapshot_fingerprint', sa.String(length=64), nullable=False),
        sa.Column('source_market_date', sa.Date(), nullable=False),
        sa.Column('target_trade_date', sa.Date(), nullable=False),
        sa.Column('symbol', sa.String(length=32), nullable=False),
        sa.Column('agent_name', sa.String(length=64), nullable=False),
        sa.Column('trigger_price', sa.BigInteger(), nullable=False),
        sa.Column('chase_limit_price', sa.BigInteger(), nullable=True),
        sa.Column('target_price', sa.BigInteger(), nullable=False),
        sa.Column('stop_loss_price', sa.BigInteger(), nullable=False),
        sa.Column('observed_price', sa.BigInteger(), nullable=False),
        sa.Column('observed_current_price', sa.BigInteger(), nullable=True),
        sa.Column('observed_bid_price', sa.BigInteger(), nullable=True),
        sa.Column('price_basis', sa.String(length=32), nullable=False),
        sa.Column('quote_source', sa.String(length=32), nullable=False),
        sa.Column('price_as_of', sa.DateTime(timezone=True), nullable=False),
        sa.Column('observed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('status', sa.String(length=16), nullable=False),
        sa.Column('attempt_count', sa.Integer(), nullable=False),
        sa.Column('request_run_id', sa.String(length=128), nullable=True),
        sa.Column('result_action', sa.String(length=32), nullable=True),
        sa.Column('context_payload', sa.JSON(), nullable=False),
        sa.Column('context_sha256', sa.String(length=64), nullable=False),
        sa.Column('last_error_payload', sa.JSON(), nullable=True),
        sa.Column('last_attempt_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'failed', 'expired')",
            name='ck_signal_decider_wait_recheck_status',
        ),
        sa.CheckConstraint(
            'trigger_price > 0 '
            'AND (chase_limit_price IS NULL OR '
            '(chase_limit_price >= trigger_price AND target_price > chase_limit_price)) '
            'AND target_price > trigger_price AND stop_loss_price < trigger_price',
            name='ck_signal_decider_wait_recheck_price_band',
        ),
        sa.CheckConstraint(
            'observed_price >= trigger_price AND observed_price < target_price '
            'AND (chase_limit_price IS NULL OR observed_price <= chase_limit_price)',
            name='ck_signal_decider_wait_recheck_observed_band',
        ),
        sa.ForeignKeyConstraint(
            ['result_snapshot_id'],
            ['signal_decider_decision_snapshots.id'],
            ondelete='SET NULL',
        ),
        sa.ForeignKeyConstraint(
            ['source_snapshot_id'],
            ['signal_decider_decision_snapshots.id'],
            ondelete='RESTRICT',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'target_trade_date',
            'symbol',
            name='uq_signal_decider_wait_recheck_target_symbol',
        ),
    )
    op.create_index(
        'ix_signal_decider_wait_recheck_events_source_market_date',
        'signal_decider_wait_recheck_events',
        ['source_market_date'],
        unique=False,
    )
    op.create_index(
        'ix_signal_decider_wait_recheck_events_target_trade_date',
        'signal_decider_wait_recheck_events',
        ['target_trade_date'],
        unique=False,
    )
    op.create_index(
        'ix_signal_decider_wait_recheck_events_symbol',
        'signal_decider_wait_recheck_events',
        ['symbol'],
        unique=False,
    )
    op.create_index(
        'ix_signal_decider_wait_recheck_events_status',
        'signal_decider_wait_recheck_events',
        ['status'],
        unique=False,
    )
    op.create_index(
        'ix_signal_decider_wait_recheck_events_request_run_id',
        'signal_decider_wait_recheck_events',
        ['request_run_id'],
        unique=False,
    )
    op.create_index(
        'ix_signal_decider_wait_recheck_source_snapshot',
        'signal_decider_wait_recheck_events',
        ['source_snapshot_id'],
        unique=False,
    )
    op.create_index(
        'ix_signal_decider_wait_recheck_target_status_updated',
        'signal_decider_wait_recheck_events',
        ['target_trade_date', 'status', 'updated_at'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        'ix_signal_decider_wait_recheck_target_status_updated',
        table_name='signal_decider_wait_recheck_events',
    )
    op.drop_index(
        'ix_signal_decider_wait_recheck_source_snapshot',
        table_name='signal_decider_wait_recheck_events',
    )
    op.drop_index(
        'ix_signal_decider_wait_recheck_events_request_run_id',
        table_name='signal_decider_wait_recheck_events',
    )
    op.drop_index(
        'ix_signal_decider_wait_recheck_events_status',
        table_name='signal_decider_wait_recheck_events',
    )
    op.drop_index(
        'ix_signal_decider_wait_recheck_events_symbol',
        table_name='signal_decider_wait_recheck_events',
    )
    op.drop_index(
        'ix_signal_decider_wait_recheck_events_target_trade_date',
        table_name='signal_decider_wait_recheck_events',
    )
    op.drop_index(
        'ix_signal_decider_wait_recheck_events_source_market_date',
        table_name='signal_decider_wait_recheck_events',
    )
    op.drop_table('signal_decider_wait_recheck_events')
