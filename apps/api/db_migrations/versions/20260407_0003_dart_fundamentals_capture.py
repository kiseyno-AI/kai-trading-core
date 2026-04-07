"""add dart fundamentals capture tables

Revision ID: 20260407_0003
Revises: 20260407_0002
Create Date: 2026-04-07 19:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260407_0003'
down_revision = '20260407_0002'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {column['name'] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'dart_disclosure_events'):
        columns = _column_names(inspector, 'dart_disclosure_events')
        if 'fundamentals_capture_status' not in columns:
            op.add_column(
                'dart_disclosure_events',
                sa.Column('fundamentals_capture_status', sa.String(length=32), server_default='pending', nullable=False),
            )
            op.create_index(
                op.f('ix_dart_disclosure_events_fundamentals_capture_status'),
                'dart_disclosure_events',
                ['fundamentals_capture_status'],
                unique=False,
            )
        if 'fundamentals_capture_attempts' not in columns:
            op.add_column(
                'dart_disclosure_events',
                sa.Column('fundamentals_capture_attempts', sa.Integer(), server_default='0', nullable=False),
            )
        if 'fundamentals_last_attempt_at' not in columns:
            op.add_column(
                'dart_disclosure_events',
                sa.Column('fundamentals_last_attempt_at', sa.DateTime(timezone=True), nullable=True),
            )
        if 'fundamentals_last_success_at' not in columns:
            op.add_column(
                'dart_disclosure_events',
                sa.Column('fundamentals_last_success_at', sa.DateTime(timezone=True), nullable=True),
            )
        if 'fundamentals_last_error' not in columns:
            op.add_column('dart_disclosure_events', sa.Column('fundamentals_last_error', sa.Text(), nullable=True))
        op.execute(
            sa.text(
                """
                UPDATE dart_disclosure_events
                SET fundamentals_capture_status = CASE
                    WHEN is_financial_report THEN 'pending'
                    ELSE 'not_applicable'
                END
                WHERE fundamentals_capture_status IS NULL
                   OR fundamentals_capture_status = 'pending'
                """
            )
        )
        op.alter_column('dart_disclosure_events', 'fundamentals_capture_status', server_default=None)
        op.alter_column('dart_disclosure_events', 'fundamentals_capture_attempts', server_default=None)
        inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'dart_fundamentals_capture_runs'):
        op.create_table(
            'dart_fundamentals_capture_runs',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('trigger', sa.String(length=32), nullable=False),
            sa.Column('status', sa.String(length=32), nullable=False),
            sa.Column('limit_requested', sa.Integer(), nullable=True),
            sa.Column('force', sa.Boolean(), nullable=False),
            sa.Column('fs_div', sa.String(length=8), nullable=False),
            sa.Column('events_seen', sa.Integer(), nullable=False),
            sa.Column('events_captured', sa.Integer(), nullable=False),
            sa.Column('events_failed', sa.Integer(), nullable=False),
            sa.Column('events_skipped', sa.Integer(), nullable=False),
            sa.Column('error_message', sa.Text(), nullable=True),
            sa.Column('run_metadata', sa.JSON(), nullable=True),
            sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index(
            op.f('ix_dart_fundamentals_capture_runs_status'),
            'dart_fundamentals_capture_runs',
            ['status'],
            unique=False,
        )
        inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'dart_fundamentals_snapshots'):
        op.create_table(
            'dart_fundamentals_snapshots',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('disclosure_event_id', sa.Integer(), nullable=False),
            sa.Column('capture_run_id', sa.Integer(), nullable=True),
            sa.Column('rcept_no', sa.String(length=32), nullable=False),
            sa.Column('corp_code', sa.String(length=16), nullable=False),
            sa.Column('stock_code', sa.String(length=16), nullable=True),
            sa.Column('corp_name', sa.String(length=255), nullable=False),
            sa.Column('business_year', sa.Integer(), nullable=False),
            sa.Column('report_code', sa.String(length=8), nullable=False),
            sa.Column('filing_category', sa.String(length=64), nullable=False),
            sa.Column('fs_div', sa.String(length=8), nullable=False),
            sa.Column('statement_type', sa.String(length=32), nullable=False),
            sa.Column('period_type', sa.String(length=32), nullable=False),
            sa.Column('period_end_month', sa.Integer(), nullable=True),
            sa.Column('currency', sa.String(length=16), nullable=True),
            sa.Column('source_status', sa.String(length=8), nullable=True),
            sa.Column('source_message', sa.Text(), nullable=True),
            sa.Column('request_payload', sa.JSON(), nullable=False),
            sa.Column('response_payload', sa.JSON(), nullable=False),
            sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['capture_run_id'], ['dart_fundamentals_capture_runs.id'], ondelete='SET NULL'),
            sa.ForeignKeyConstraint(['disclosure_event_id'], ['dart_disclosure_events.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint(
                'disclosure_event_id',
                'business_year',
                'report_code',
                'fs_div',
                name='uq_dart_fundamentals_snapshots_event_period_fs_div',
            ),
        )
        op.create_index(
            op.f('ix_dart_fundamentals_snapshots_business_year'),
            'dart_fundamentals_snapshots',
            ['business_year'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_fundamentals_snapshots_capture_run_id'),
            'dart_fundamentals_snapshots',
            ['capture_run_id'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_fundamentals_snapshots_corp_code'),
            'dart_fundamentals_snapshots',
            ['corp_code'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_fundamentals_snapshots_disclosure_event_id'),
            'dart_fundamentals_snapshots',
            ['disclosure_event_id'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_fundamentals_snapshots_filing_category'),
            'dart_fundamentals_snapshots',
            ['filing_category'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_fundamentals_snapshots_rcept_no'),
            'dart_fundamentals_snapshots',
            ['rcept_no'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_fundamentals_snapshots_report_code'),
            'dart_fundamentals_snapshots',
            ['report_code'],
            unique=False,
        )
        op.create_index(
            op.f('ix_dart_fundamentals_snapshots_stock_code'),
            'dart_fundamentals_snapshots',
            ['stock_code'],
            unique=False,
        )
        inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'dart_fundamentals_rows'):
        op.create_table(
            'dart_fundamentals_rows',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('snapshot_id', sa.Integer(), nullable=False),
            sa.Column('row_key', sa.String(length=255), nullable=False),
            sa.Column('receipt_number', sa.String(length=32), nullable=True),
            sa.Column('report_code', sa.String(length=8), nullable=True),
            sa.Column('business_year', sa.String(length=8), nullable=True),
            sa.Column('statement_type', sa.String(length=32), nullable=True),
            sa.Column('statement_name', sa.String(length=255), nullable=True),
            sa.Column('account_id', sa.String(length=255), nullable=True),
            sa.Column('account_name', sa.String(length=255), nullable=True),
            sa.Column('account_detail', sa.String(length=255), nullable=True),
            sa.Column('current_period_name', sa.String(length=255), nullable=True),
            sa.Column('current_amount', sa.BigInteger(), nullable=True),
            sa.Column('current_cumulative_amount', sa.BigInteger(), nullable=True),
            sa.Column('prior_period_name', sa.String(length=255), nullable=True),
            sa.Column('prior_amount', sa.BigInteger(), nullable=True),
            sa.Column('prior_quarter_name', sa.String(length=255), nullable=True),
            sa.Column('prior_quarter_amount', sa.BigInteger(), nullable=True),
            sa.Column('prior_cumulative_amount', sa.BigInteger(), nullable=True),
            sa.Column('pre_prior_period_name', sa.String(length=255), nullable=True),
            sa.Column('pre_prior_amount', sa.BigInteger(), nullable=True),
            sa.Column('row_order', sa.Integer(), nullable=True),
            sa.Column('currency', sa.String(length=16), nullable=True),
            sa.Column('payload', sa.JSON(), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['snapshot_id'], ['dart_fundamentals_snapshots.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('snapshot_id', 'row_key', name='uq_dart_fundamentals_rows_snapshot_row_key'),
        )
        op.create_index(
            op.f('ix_dart_fundamentals_rows_snapshot_id'),
            'dart_fundamentals_rows',
            ['snapshot_id'],
            unique=False,
        )
        inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'dart_fundamentals_highlights'):
        op.create_table(
            'dart_fundamentals_highlights',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('snapshot_id', sa.Integer(), nullable=False),
            sa.Column('metric_key', sa.String(length=64), nullable=False),
            sa.Column('metric_type', sa.String(length=32), nullable=False),
            sa.Column('statement_type', sa.String(length=32), nullable=True),
            sa.Column('statement_name', sa.String(length=255), nullable=True),
            sa.Column('account_id', sa.String(length=255), nullable=True),
            sa.Column('account_name', sa.String(length=255), nullable=True),
            sa.Column('current_amount', sa.BigInteger(), nullable=True),
            sa.Column('current_cumulative_amount', sa.BigInteger(), nullable=True),
            sa.Column('prior_amount', sa.BigInteger(), nullable=True),
            sa.Column('prior_cumulative_amount', sa.BigInteger(), nullable=True),
            sa.Column('pre_prior_amount', sa.BigInteger(), nullable=True),
            sa.Column('currency', sa.String(length=16), nullable=True),
            sa.Column('ratio_value', sa.Numeric(precision=18, scale=4), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['snapshot_id'], ['dart_fundamentals_snapshots.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('snapshot_id', 'metric_key', name='uq_dart_fundamentals_highlights_snapshot_metric_key'),
        )
        op.create_index(
            op.f('ix_dart_fundamentals_highlights_snapshot_id'),
            'dart_fundamentals_highlights',
            ['snapshot_id'],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index(op.f('ix_dart_fundamentals_highlights_snapshot_id'), table_name='dart_fundamentals_highlights')
    op.drop_table('dart_fundamentals_highlights')
    op.drop_index(op.f('ix_dart_fundamentals_rows_snapshot_id'), table_name='dart_fundamentals_rows')
    op.drop_table('dart_fundamentals_rows')
    op.drop_index(op.f('ix_dart_fundamentals_snapshots_stock_code'), table_name='dart_fundamentals_snapshots')
    op.drop_index(op.f('ix_dart_fundamentals_snapshots_report_code'), table_name='dart_fundamentals_snapshots')
    op.drop_index(op.f('ix_dart_fundamentals_snapshots_rcept_no'), table_name='dart_fundamentals_snapshots')
    op.drop_index(op.f('ix_dart_fundamentals_snapshots_filing_category'), table_name='dart_fundamentals_snapshots')
    op.drop_index(op.f('ix_dart_fundamentals_snapshots_disclosure_event_id'), table_name='dart_fundamentals_snapshots')
    op.drop_index(op.f('ix_dart_fundamentals_snapshots_corp_code'), table_name='dart_fundamentals_snapshots')
    op.drop_index(op.f('ix_dart_fundamentals_snapshots_capture_run_id'), table_name='dart_fundamentals_snapshots')
    op.drop_index(op.f('ix_dart_fundamentals_snapshots_business_year'), table_name='dart_fundamentals_snapshots')
    op.drop_table('dart_fundamentals_snapshots')
    op.drop_index(op.f('ix_dart_fundamentals_capture_runs_status'), table_name='dart_fundamentals_capture_runs')
    op.drop_table('dart_fundamentals_capture_runs')
    op.drop_column('dart_disclosure_events', 'fundamentals_last_error')
    op.drop_column('dart_disclosure_events', 'fundamentals_last_success_at')
    op.drop_column('dart_disclosure_events', 'fundamentals_last_attempt_at')
    op.drop_column('dart_disclosure_events', 'fundamentals_capture_attempts')
    op.drop_index(
        op.f('ix_dart_disclosure_events_fundamentals_capture_status'),
        table_name='dart_disclosure_events',
    )
    op.drop_column('dart_disclosure_events', 'fundamentals_capture_status')
