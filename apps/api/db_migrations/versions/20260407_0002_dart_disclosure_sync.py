"""add dart disclosure sync tables

Revision ID: 20260407_0002
Revises: 20260405_0001
Create Date: 2026-04-07 17:20:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260407_0002'
down_revision = '20260405_0001'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'dart_disclosure_sync_runs'):
        op.create_table(
            'dart_disclosure_sync_runs',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('trigger', sa.String(length=32), nullable=False),
            sa.Column('status', sa.String(length=32), nullable=False),
            sa.Column('window_start', sa.Date(), nullable=False),
            sa.Column('window_end', sa.Date(), nullable=False),
            sa.Column('page_size', sa.Integer(), nullable=False),
            sa.Column('pages_fetched', sa.Integer(), nullable=False),
            sa.Column('events_seen', sa.Integer(), nullable=False),
            sa.Column('events_inserted', sa.Integer(), nullable=False),
            sa.Column('events_updated', sa.Integer(), nullable=False),
            sa.Column('financial_report_events', sa.Integer(), nullable=False),
            sa.Column('error_message', sa.Text(), nullable=True),
            sa.Column('run_metadata', sa.JSON(), nullable=True),
            sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index(op.f('ix_dart_disclosure_sync_runs_status'), 'dart_disclosure_sync_runs', ['status'], unique=False)
        op.create_index(
            op.f('ix_dart_disclosure_sync_runs_window_end'),
            'dart_disclosure_sync_runs',
            ['window_end'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_disclosure_sync_runs_window_start'),
            'dart_disclosure_sync_runs',
            ['window_start'],
            unique=False,
        )
        inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'dart_disclosure_events'):
        op.create_table(
            'dart_disclosure_events',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('rcept_no', sa.String(length=32), nullable=False),
            sa.Column('corp_code', sa.String(length=16), nullable=False),
            sa.Column('corp_name', sa.String(length=255), nullable=False),
            sa.Column('stock_code', sa.String(length=16), nullable=True),
            sa.Column('corp_cls', sa.String(length=8), nullable=True),
            sa.Column('report_nm', sa.String(length=255), nullable=False),
            sa.Column('filing_category', sa.String(length=64), nullable=False),
            sa.Column('is_financial_report', sa.Boolean(), nullable=False),
            sa.Column('filer_name', sa.String(length=255), nullable=True),
            sa.Column('receipt_date', sa.Date(), nullable=True),
            sa.Column('remark', sa.String(length=255), nullable=True),
            sa.Column('payload', sa.JSON(), nullable=False),
            sa.Column('first_seen_run_id', sa.Integer(), nullable=True),
            sa.Column('last_seen_run_id', sa.Integer(), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['first_seen_run_id'], ['dart_disclosure_sync_runs.id'], ondelete='SET NULL'),
            sa.ForeignKeyConstraint(['last_seen_run_id'], ['dart_disclosure_sync_runs.id'], ondelete='SET NULL'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('rcept_no'),
        )
        op.create_index(op.f('ix_dart_disclosure_events_corp_code'), 'dart_disclosure_events', ['corp_code'], unique=False)
        op.create_index(
            op.f('ix_dart_disclosure_events_filing_category'),
            'dart_disclosure_events',
            ['filing_category'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_disclosure_events_first_seen_run_id'),
            'dart_disclosure_events',
            ['first_seen_run_id'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_disclosure_events_is_financial_report'),
            'dart_disclosure_events',
            ['is_financial_report'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_disclosure_events_last_seen_run_id'),
            'dart_disclosure_events',
            ['last_seen_run_id'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_disclosure_events_rcept_no'),
            'dart_disclosure_events',
            ['rcept_no'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_disclosure_events_receipt_date'),
            'dart_disclosure_events',
            ['receipt_date'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_disclosure_events_stock_code'),
            'dart_disclosure_events',
            ['stock_code'],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index(op.f('ix_dart_disclosure_events_stock_code'), table_name='dart_disclosure_events')
    op.drop_index(op.f('ix_dart_disclosure_events_receipt_date'), table_name='dart_disclosure_events')
    op.drop_index(op.f('ix_dart_disclosure_events_rcept_no'), table_name='dart_disclosure_events')
    op.drop_index(op.f('ix_dart_disclosure_events_last_seen_run_id'), table_name='dart_disclosure_events')
    op.drop_index(op.f('ix_dart_disclosure_events_is_financial_report'), table_name='dart_disclosure_events')
    op.drop_index(op.f('ix_dart_disclosure_events_first_seen_run_id'), table_name='dart_disclosure_events')
    op.drop_index(op.f('ix_dart_disclosure_events_filing_category'), table_name='dart_disclosure_events')
    op.drop_index(op.f('ix_dart_disclosure_events_corp_code'), table_name='dart_disclosure_events')
    op.drop_table('dart_disclosure_events')
    op.drop_index(op.f('ix_dart_disclosure_sync_runs_window_start'), table_name='dart_disclosure_sync_runs')
    op.drop_index(op.f('ix_dart_disclosure_sync_runs_window_end'), table_name='dart_disclosure_sync_runs')
    op.drop_index(op.f('ix_dart_disclosure_sync_runs_status'), table_name='dart_disclosure_sync_runs')
    op.drop_table('dart_disclosure_sync_runs')
