"""portfolio asset snapshots

Revision ID: 20260704_0053
Revises: 20260620_0052
Create Date: 2026-07-04 00:53:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260704_0053'
down_revision = '20260620_0052'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'portfolio_asset_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('account_reference', sa.String(length=64), nullable=False),
        sa.Column('account_label', sa.String(length=128), nullable=True),
        sa.Column('total_assets', sa.Numeric(20, 2), nullable=False),
        sa.Column('currency', sa.String(length=16), nullable=False),
        sa.Column('as_of', sa.DateTime(timezone=True), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('source', sa.String(length=64), nullable=False),
        sa.Column('snapshot_payload', sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_portfolio_asset_snapshots_account_captured',
        'portfolio_asset_snapshots',
        ['account_reference', 'captured_at'],
        unique=False,
    )
    op.create_index(
        'ix_portfolio_asset_snapshots_captured_at',
        'portfolio_asset_snapshots',
        ['captured_at'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_portfolio_asset_snapshots_captured_at', table_name='portfolio_asset_snapshots')
    op.drop_index('ix_portfolio_asset_snapshots_account_captured', table_name='portfolio_asset_snapshots')
    op.drop_table('portfolio_asset_snapshots')
