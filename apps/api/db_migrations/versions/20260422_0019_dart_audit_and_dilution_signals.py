"""add dart audit and dilution signal tables

Revision ID: 20260422_0019
Revises: 20260422_0018
Create Date: 2026-04-22 10:15:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260422_0019'
down_revision = '20260422_0018'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'dart_audit_signals',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('disclosure_event_id', sa.Integer(), nullable=True),
        sa.Column('rcept_no', sa.String(length=32), nullable=False),
        sa.Column('corp_code', sa.String(length=16), nullable=False),
        sa.Column('stock_code', sa.String(length=16), nullable=True),
        sa.Column('corp_name', sa.String(length=255), nullable=False),
        sa.Column('receipt_date', sa.Date(), nullable=True),
        sa.Column('report_nm', sa.String(length=255), nullable=True),
        sa.Column('opinion_code', sa.String(length=32), nullable=True),
        sa.Column('opinion_label', sa.String(length=64), nullable=True),
        sa.Column('has_audit_issue', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('source_section', sa.String(length=255), nullable=True),
        sa.Column('evidence_text', sa.Text(), nullable=True),
        sa.Column('payload', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['disclosure_event_id'], ['dart_disclosure_events.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('rcept_no', name='uq_dart_audit_signals_rcept_no'),
    )
    op.create_index('ix_dart_audit_signals_stock_receipt', 'dart_audit_signals', ['stock_code', 'receipt_date'])

    op.create_table(
        'dart_dilution_signals',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('disclosure_event_id', sa.Integer(), nullable=True),
        sa.Column('rcept_no', sa.String(length=32), nullable=False),
        sa.Column('corp_code', sa.String(length=16), nullable=False),
        sa.Column('stock_code', sa.String(length=16), nullable=True),
        sa.Column('corp_name', sa.String(length=255), nullable=False),
        sa.Column('receipt_date', sa.Date(), nullable=True),
        sa.Column('report_nm', sa.String(length=255), nullable=True),
        sa.Column('signal_type', sa.String(length=64), nullable=False),
        sa.Column('has_dilution_signal', sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column('security_type', sa.String(length=64), nullable=True),
        sa.Column('reason', sa.String(length=255), nullable=True),
        sa.Column('payload', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['disclosure_event_id'], ['dart_disclosure_events.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('rcept_no', name='uq_dart_dilution_signals_rcept_no'),
    )
    op.create_index('ix_dart_dilution_signals_stock_receipt', 'dart_dilution_signals', ['stock_code', 'receipt_date'])


def downgrade() -> None:
    op.drop_index('ix_dart_dilution_signals_stock_receipt', table_name='dart_dilution_signals')
    op.drop_table('dart_dilution_signals')
    op.drop_index('ix_dart_audit_signals_stock_receipt', table_name='dart_audit_signals')
    op.drop_table('dart_audit_signals')
