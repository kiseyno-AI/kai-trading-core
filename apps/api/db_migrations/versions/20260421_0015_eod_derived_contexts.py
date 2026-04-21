"""add persisted eod derived contexts

Revision ID: 20260421_0015
Revises: 20260419_0014
Create Date: 2026-04-21 16:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260421_0015'
down_revision = '20260419_0014'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'daily_price_contexts'):
        op.create_table(
            'daily_price_contexts',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('symbol_id', sa.Integer(), nullable=False),
            sa.Column('eod_run_id', sa.Integer(), nullable=True),
            sa.Column('market_date', sa.Date(), nullable=False),
            sa.Column('source', sa.String(length=32), nullable=False),
            sa.Column('context_payload', sa.JSON(), nullable=False),
            sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['eod_run_id'], ['eod_collection_runs.id'], ondelete='SET NULL'),
            sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_price_contexts_symbol_market_date'),
        )
        op.create_index(op.f('ix_daily_price_contexts_symbol_id'), 'daily_price_contexts', ['symbol_id'], unique=False)
        op.create_index(op.f('ix_daily_price_contexts_eod_run_id'), 'daily_price_contexts', ['eod_run_id'], unique=False)
        op.create_index(op.f('ix_daily_price_contexts_market_date'), 'daily_price_contexts', ['market_date'], unique=False)

    if not _table_exists(inspector, 'daily_market_participation_contexts'):
        op.create_table(
            'daily_market_participation_contexts',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('market', sa.String(length=32), nullable=False),
            sa.Column('market_date', sa.Date(), nullable=False),
            sa.Column('source', sa.String(length=32), nullable=False),
            sa.Column('context_payload', sa.JSON(), nullable=False),
            sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint(
                'market',
                'market_date',
                name='uq_daily_market_participation_contexts_market_market_date',
            ),
        )
        op.create_index(
            op.f('ix_daily_market_participation_contexts_market'),
            'daily_market_participation_contexts',
            ['market'],
            unique=False,
        )
        op.create_index(
            op.f('ix_daily_market_participation_contexts_market_date'),
            'daily_market_participation_contexts',
            ['market_date'],
            unique=False,
        )


def downgrade() -> None:
    if op.get_bind() is None:
        return

    op.drop_index(
        op.f('ix_daily_market_participation_contexts_market_date'),
        table_name='daily_market_participation_contexts',
    )
    op.drop_index(
        op.f('ix_daily_market_participation_contexts_market'),
        table_name='daily_market_participation_contexts',
    )
    op.drop_table('daily_market_participation_contexts')

    op.drop_index(op.f('ix_daily_price_contexts_market_date'), table_name='daily_price_contexts')
    op.drop_index(op.f('ix_daily_price_contexts_eod_run_id'), table_name='daily_price_contexts')
    op.drop_index(op.f('ix_daily_price_contexts_symbol_id'), table_name='daily_price_contexts')
    op.drop_table('daily_price_contexts')
