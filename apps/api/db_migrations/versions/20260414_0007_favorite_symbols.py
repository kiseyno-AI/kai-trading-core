"""add favorite symbols

Revision ID: 20260414_0007a
Revises: 20260414_0007
Create Date: 2026-04-14 13:35:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260414_0007a'
down_revision = '20260414_0007'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'favorite_symbols'):
        return

    op.create_table(
        'favorite_symbols',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('sort_order', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', name='uq_favorite_symbols_symbol_id'),
    )
    op.create_index(op.f('ix_favorite_symbols_symbol_id'), 'favorite_symbols', ['symbol_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_favorite_symbols_symbol_id'), table_name='favorite_symbols')
    op.drop_table('favorite_symbols')
