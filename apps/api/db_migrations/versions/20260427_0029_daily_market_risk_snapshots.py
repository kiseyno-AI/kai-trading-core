"""add daily market risk snapshots

Revision ID: 20260427_0029
Revises: 20260427_0028
Create Date: 2026-04-27 16:45:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260427_0029'
down_revision = '20260427_0028'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_market_risk_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('eod_run_id', sa.Integer(), nullable=True),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('market_code', sa.String(length=8), nullable=False, server_default='J'),
        sa.Column('source_endpoint', sa.String(length=64), nullable=False, server_default='inquire-price-2'),
        sa.Column('market_warning_code', sa.String(length=8), nullable=True),
        sa.Column('market_warning_name', sa.String(length=64), nullable=True),
        sa.Column('is_management_item', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_investment_caution', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_short_overheated', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('short_overheat_class_code', sa.String(length=8), nullable=True),
        sa.Column('is_short_sale_overheated', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('vi_class_code', sa.String(length=8), nullable=True),
        sa.Column('is_vi_triggered', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_dishonest_disclosure', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_trade_halted', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('snapshot_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['eod_run_id'], ['eod_collection_runs.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_market_risk_snapshots_symbol_market_date'),
    )
    op.create_index(
        'ix_daily_market_risk_snapshots_eod_run_id',
        'daily_market_risk_snapshots',
        ['eod_run_id'],
    )
    op.create_index(
        'ix_daily_market_risk_snapshots_market_date',
        'daily_market_risk_snapshots',
        ['market_date'],
    )
    op.create_index(
        'ix_daily_market_risk_snapshots_market_halted',
        'daily_market_risk_snapshots',
        ['market_date', 'is_trade_halted'],
    )
    op.create_index(
        'ix_daily_market_risk_snapshots_market_management',
        'daily_market_risk_snapshots',
        ['market_date', 'is_management_item'],
    )
    op.create_index(
        'ix_daily_market_risk_snapshots_market_warning',
        'daily_market_risk_snapshots',
        ['market_date', 'market_warning_code'],
    )
    op.create_index(
        'ix_daily_market_risk_snapshots_symbol_id',
        'daily_market_risk_snapshots',
        ['symbol_id'],
    )
    op.execute(
        """
        INSERT INTO daily_market_risk_snapshots (
            symbol_id,
            eod_run_id,
            market_date,
            source,
            market_code,
            source_endpoint,
            market_warning_code,
            market_warning_name,
            is_management_item,
            is_investment_caution,
            is_short_overheated,
            short_overheat_class_code,
            is_short_sale_overheated,
            vi_class_code,
            is_vi_triggered,
            is_dishonest_disclosure,
            is_trade_halted,
            snapshot_payload,
            captured_at,
            created_at,
            updated_at
        )
        SELECT
            symbol_id,
            eod_run_id,
            market_date,
            'kis_quote2_snapshot_v1',
            COALESCE(filter_payload ->> 'market_code', 'J'),
            COALESCE(filter_payload ->> 'source_endpoint', 'inquire-price-2'),
            market_warning_code,
            market_warning_name,
            is_management_item,
            is_investment_caution,
            is_short_overheated,
            short_overheat_class_code,
            is_short_sale_overheated,
            vi_class_code,
            is_vi_triggered,
            is_dishonest_disclosure,
            is_trade_halted,
            json_build_object(
                'feature_version', 'kis_quote2_snapshot_v1',
                'market_code', COALESCE(filter_payload ->> 'market_code', 'J'),
                'source_endpoint', COALESCE(filter_payload ->> 'source_endpoint', 'inquire-price-2'),
                'raw', COALESCE(filter_payload -> 'raw', '{}'::json)
            ),
            captured_at,
            created_at,
            updated_at
        FROM daily_market_risk_filter_results
        """
    )


def downgrade() -> None:
    op.drop_index(
        'ix_daily_market_risk_snapshots_symbol_id',
        table_name='daily_market_risk_snapshots',
    )
    op.drop_index(
        'ix_daily_market_risk_snapshots_market_warning',
        table_name='daily_market_risk_snapshots',
    )
    op.drop_index(
        'ix_daily_market_risk_snapshots_market_management',
        table_name='daily_market_risk_snapshots',
    )
    op.drop_index(
        'ix_daily_market_risk_snapshots_market_halted',
        table_name='daily_market_risk_snapshots',
    )
    op.drop_index(
        'ix_daily_market_risk_snapshots_market_date',
        table_name='daily_market_risk_snapshots',
    )
    op.drop_index(
        'ix_daily_market_risk_snapshots_eod_run_id',
        table_name='daily_market_risk_snapshots',
    )
    op.drop_table('daily_market_risk_snapshots')
