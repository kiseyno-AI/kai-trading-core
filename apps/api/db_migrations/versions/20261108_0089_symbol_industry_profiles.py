"""symbol industry codes and business lines

Revision ID: 20261108_0089
Revises: 20261108_0088
Create Date: 2026-11-08 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20261108_0089'
down_revision = '20261108_0088'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'symbol_industry_profiles',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('symbol', sa.String(length=16), nullable=False),
        sa.Column('corp_code', sa.String(length=16), nullable=True),
        sa.Column('corp_name', sa.String(length=255), nullable=True),
        sa.Column('ksic_code', sa.String(length=16), nullable=True),
        sa.Column('krx_sector', sa.String(length=64), nullable=True),
        sa.Column('business_line', sa.String(length=64), nullable=False),
        sa.Column('line_source', sa.String(length=32), nullable=False),
        sa.Column('effective_from', sa.Date(), nullable=False),
        sa.Column('last_verified_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint('symbol', 'effective_from', name='uq_symbol_industry_profiles_symbol_from'),
    )
    op.create_index('ix_symbol_industry_profiles_symbol', 'symbol_industry_profiles', ['symbol'])
    op.create_index('ix_symbol_industry_profiles_business_line', 'symbol_industry_profiles', ['business_line'])


def downgrade() -> None:
    op.drop_table('symbol_industry_profiles')
