"""Add market-neutral symbol metadata for US equities."""

from alembic import op
import sqlalchemy as sa


revision = '20260819_0069'
down_revision = '20260809_0068'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('symbols', sa.Column('name_en', sa.String(length=255), nullable=True))
    op.add_column('symbols', sa.Column('country_code', sa.String(length=2), nullable=True))
    op.add_column('symbols', sa.Column('currency_code', sa.String(length=3), nullable=True))
    op.add_column('symbols', sa.Column('exchange_code', sa.String(length=32), nullable=True))
    op.add_column('symbols', sa.Column('regulatory_id', sa.String(length=32), nullable=True))
    op.add_column(
        'symbols',
        sa.Column('source_payload', sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False),
    )
    op.create_index('ix_symbols_country_code', 'symbols', ['country_code'], unique=False)
    op.create_index('ix_symbols_currency_code', 'symbols', ['currency_code'], unique=False)
    op.create_index('ix_symbols_exchange_code', 'symbols', ['exchange_code'], unique=False)
    op.create_index('ix_symbols_regulatory_id', 'symbols', ['regulatory_id'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_symbols_regulatory_id', table_name='symbols')
    op.drop_index('ix_symbols_exchange_code', table_name='symbols')
    op.drop_index('ix_symbols_currency_code', table_name='symbols')
    op.drop_index('ix_symbols_country_code', table_name='symbols')
    op.drop_column('symbols', 'source_payload')
    op.drop_column('symbols', 'regulatory_id')
    op.drop_column('symbols', 'exchange_code')
    op.drop_column('symbols', 'currency_code')
    op.drop_column('symbols', 'country_code')
    op.drop_column('symbols', 'name_en')
