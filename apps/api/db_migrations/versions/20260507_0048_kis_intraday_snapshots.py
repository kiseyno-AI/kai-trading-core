"""add kis intraday snapshot persistence

Revision ID: 20260507_0048
Revises: 20260507_0047
Create Date: 2026-05-07 18:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260507_0048'
down_revision = '20260507_0047'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'kis_intraday_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_code', sa.String(length=8), nullable=False, server_default='J'),
        sa.Column('source', sa.String(length=32), nullable=False, server_default='kis_intraday_snapshot_v1'),
        sa.Column('feature_version', sa.String(length=32), nullable=False, server_default='kis_intraday_snapshot_v1'),
        sa.Column('availability_status', sa.String(length=16), nullable=False, server_default='available'),
        sa.Column('freshness_status', sa.String(length=16), nullable=False, server_default='fresh'),
        sa.Column('as_of_market_date', sa.Date(), nullable=True),
        sa.Column('as_of_timestamp', sa.DateTime(timezone=True), nullable=True),
        sa.Column('snapshot_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_kis_intraday_snapshots_availability_status',
        'kis_intraday_snapshots',
        ['availability_status'],
    )
    op.create_index(
        'ix_kis_intraday_snapshots_as_of_market_date',
        'kis_intraday_snapshots',
        ['as_of_market_date'],
    )
    op.create_index(
        'ix_kis_intraday_snapshots_captured_at',
        'kis_intraday_snapshots',
        ['captured_at'],
    )
    op.create_index(
        'ix_kis_intraday_snapshots_symbol_captured_at',
        'kis_intraday_snapshots',
        ['symbol_id', 'captured_at'],
    )
    op.create_index(
        'ix_kis_intraday_snapshots_symbol_id',
        'kis_intraday_snapshots',
        ['symbol_id'],
    )

    op.create_table(
        'current_kis_intraday_snapshot_cache',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_code', sa.String(length=8), nullable=False, server_default='J'),
        sa.Column('source', sa.String(length=32), nullable=False, server_default='kis_intraday_snapshot_v1'),
        sa.Column('feature_version', sa.String(length=32), nullable=False, server_default='kis_intraday_snapshot_v1'),
        sa.Column('availability_status', sa.String(length=16), nullable=False, server_default='available'),
        sa.Column('freshness_status', sa.String(length=16), nullable=False, server_default='fresh'),
        sa.Column('as_of_market_date', sa.Date(), nullable=True),
        sa.Column('as_of_timestamp', sa.DateTime(timezone=True), nullable=True),
        sa.Column('snapshot_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', name='uq_current_kis_intraday_snapshot_cache_symbol_id'),
    )
    op.create_index(
        'ix_current_kis_intraday_snapshot_cache_as_of_market_date',
        'current_kis_intraday_snapshot_cache',
        ['as_of_market_date'],
    )
    op.create_index(
        'ix_current_kis_intraday_snapshot_cache_availability_status',
        'current_kis_intraday_snapshot_cache',
        ['availability_status'],
    )
    op.create_index(
        'ix_current_kis_intraday_snapshot_cache_symbol_id',
        'current_kis_intraday_snapshot_cache',
        ['symbol_id'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_current_kis_intraday_snapshot_cache_symbol_id',
        table_name='current_kis_intraday_snapshot_cache',
    )
    op.drop_index(
        'ix_current_kis_intraday_snapshot_cache_availability_status',
        table_name='current_kis_intraday_snapshot_cache',
    )
    op.drop_index(
        'ix_current_kis_intraday_snapshot_cache_as_of_market_date',
        table_name='current_kis_intraday_snapshot_cache',
    )
    op.drop_table('current_kis_intraday_snapshot_cache')

    op.drop_index(
        'ix_kis_intraday_snapshots_symbol_id',
        table_name='kis_intraday_snapshots',
    )
    op.drop_index(
        'ix_kis_intraday_snapshots_symbol_captured_at',
        table_name='kis_intraday_snapshots',
    )
    op.drop_index(
        'ix_kis_intraday_snapshots_captured_at',
        table_name='kis_intraday_snapshots',
    )
    op.drop_index(
        'ix_kis_intraday_snapshots_as_of_market_date',
        table_name='kis_intraday_snapshots',
    )
    op.drop_index(
        'ix_kis_intraday_snapshots_availability_status',
        table_name='kis_intraday_snapshots',
    )
    op.drop_table('kis_intraday_snapshots')
