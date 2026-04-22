"""add index/detail fields to dart fundamentals highlights

Revision ID: 20260422_0018
Revises: 20260422_0017
Create Date: 2026-04-22 09:08:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260422_0018'
down_revision = '20260422_0017'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('dart_fundamentals_highlights', sa.Column('source_api', sa.String(length=64), nullable=True))
    op.add_column('dart_fundamentals_highlights', sa.Column('index_code', sa.String(length=64), nullable=True))
    op.add_column('dart_fundamentals_highlights', sa.Column('index_class_code', sa.String(length=64), nullable=True))
    op.add_column('dart_fundamentals_highlights', sa.Column('index_class_name', sa.String(length=255), nullable=True))
    op.add_column('dart_fundamentals_highlights', sa.Column('period_end_date', sa.Date(), nullable=True))
    op.add_column('dart_fundamentals_highlights', sa.Column('detail_payload', sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column('dart_fundamentals_highlights', 'detail_payload')
    op.drop_column('dart_fundamentals_highlights', 'period_end_date')
    op.drop_column('dart_fundamentals_highlights', 'index_class_name')
    op.drop_column('dart_fundamentals_highlights', 'index_class_code')
    op.drop_column('dart_fundamentals_highlights', 'index_code')
    op.drop_column('dart_fundamentals_highlights', 'source_api')
