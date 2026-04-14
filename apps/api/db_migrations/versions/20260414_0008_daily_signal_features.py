"""add daily signal features

Revision ID: 20260414_0008
Revises: 20260414_0007
Create Date: 2026-04-14 18:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260414_0008'
down_revision = '20260414_0007'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'daily_signal_features'):
        return

    op.create_table(
        'daily_signal_features',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('signal_count', sa.Integer(), nullable=False),
        sa.Column('volume_power_rank', sa.Integer(), nullable=True),
        sa.Column('volume_power', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('overtime_volume_rank', sa.Integer(), nullable=True),
        sa.Column('overtime_volume', sa.BigInteger(), nullable=True),
        sa.Column('overtime_fluctuation_rank', sa.Integer(), nullable=True),
        sa.Column('overtime_fluctuation_rate', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('feature_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_signal_features_symbol_market_date'),
    )
    op.create_index(op.f('ix_daily_signal_features_symbol_id'), 'daily_signal_features', ['symbol_id'], unique=False)
    op.create_index(op.f('ix_daily_signal_features_market_date'), 'daily_signal_features', ['market_date'], unique=False)
    op.create_index(op.f('ix_daily_signal_features_signal_count'), 'daily_signal_features', ['signal_count'], unique=False)
    op.create_index(
        op.f('ix_daily_signal_features_volume_power_rank'),
        'daily_signal_features',
        ['volume_power_rank'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_signal_features_overtime_volume_rank'),
        'daily_signal_features',
        ['overtime_volume_rank'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_signal_features_overtime_fluctuation_rank'),
        'daily_signal_features',
        ['overtime_fluctuation_rank'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f('ix_daily_signal_features_overtime_fluctuation_rank'), table_name='daily_signal_features')
    op.drop_index(op.f('ix_daily_signal_features_overtime_volume_rank'), table_name='daily_signal_features')
    op.drop_index(op.f('ix_daily_signal_features_volume_power_rank'), table_name='daily_signal_features')
    op.drop_index(op.f('ix_daily_signal_features_signal_count'), table_name='daily_signal_features')
    op.drop_index(op.f('ix_daily_signal_features_market_date'), table_name='daily_signal_features')
    op.drop_index(op.f('ix_daily_signal_features_symbol_id'), table_name='daily_signal_features')
    op.drop_table('daily_signal_features')
