"""Add isolated US equity price, fundamental, and screening storage."""

from alembic import op
import sqlalchemy as sa


revision = '20260819_0070'
down_revision = '20260819_0069'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'us_market_data_collection_runs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('universe_count', sa.Integer(), nullable=False),
        sa.Column('price_history_count', sa.Integer(), nullable=False),
        sa.Column('price_eligible_count', sa.Integer(), nullable=False),
        sa.Column('fundamental_eligible_count', sa.Integer(), nullable=False),
        sa.Column('error_count', sa.Integer(), nullable=False),
        sa.Column('summary_payload', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_us_market_data_collection_runs_market_date', 'us_market_data_collection_runs', ['market_date'])
    op.create_index('ix_us_market_data_collection_runs_status', 'us_market_data_collection_runs', ['status'])
    op.create_index(
        'ix_us_market_data_runs_date_status',
        'us_market_data_collection_runs',
        ['market_date', 'status'],
    )

    op.create_table(
        'us_equity_price_bars',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('collection_run_id', sa.Integer(), nullable=True),
        sa.Column('trade_date', sa.Date(), nullable=False),
        sa.Column('open_price', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('high_price', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('low_price', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('close_price', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('volume', sa.BigInteger(), nullable=False),
        sa.Column('turnover_usd', sa.Numeric(precision=24, scale=6), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['collection_run_id'], ['us_market_data_collection_runs.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'symbol_id',
            'trade_date',
            'source',
            name='uq_us_equity_price_bars_symbol_date_source',
        ),
    )
    op.create_index('ix_us_equity_price_bars_symbol_id', 'us_equity_price_bars', ['symbol_id'])
    op.create_index('ix_us_equity_price_bars_collection_run_id', 'us_equity_price_bars', ['collection_run_id'])
    op.create_index('ix_us_equity_price_bars_trade_date', 'us_equity_price_bars', ['trade_date'])
    op.create_index('ix_us_equity_price_bars_date_symbol', 'us_equity_price_bars', ['trade_date', 'symbol_id'])

    op.create_table(
        'us_equity_fundamental_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('cik', sa.String(length=10), nullable=False),
        sa.Column('accession_number', sa.String(length=32), nullable=False),
        sa.Column('form', sa.String(length=16), nullable=False),
        sa.Column('filed_date', sa.Date(), nullable=False),
        sa.Column('period_end_date', sa.Date(), nullable=True),
        sa.Column('fiscal_year', sa.Integer(), nullable=True),
        sa.Column('fiscal_period', sa.String(length=16), nullable=True),
        sa.Column('revenue_usd', sa.Numeric(precision=28, scale=6), nullable=True),
        sa.Column('net_income_usd', sa.Numeric(precision=28, scale=6), nullable=True),
        sa.Column('assets_usd', sa.Numeric(precision=28, scale=6), nullable=True),
        sa.Column('liabilities_usd', sa.Numeric(precision=28, scale=6), nullable=True),
        sa.Column('equity_usd', sa.Numeric(precision=28, scale=6), nullable=True),
        sa.Column('shares_outstanding', sa.Numeric(precision=28, scale=6), nullable=True),
        sa.Column('source_payload', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'accession_number', name='uq_us_fundamentals_symbol_accession'),
    )
    op.create_index('ix_us_equity_fundamental_snapshots_symbol_id', 'us_equity_fundamental_snapshots', ['symbol_id'])
    op.create_index('ix_us_equity_fundamental_snapshots_cik', 'us_equity_fundamental_snapshots', ['cik'])
    op.create_index('ix_us_equity_fundamental_snapshots_filed_date', 'us_equity_fundamental_snapshots', ['filed_date'])
    op.create_index('ix_us_fundamentals_filed_symbol', 'us_equity_fundamental_snapshots', ['filed_date', 'symbol_id'])

    op.create_table(
        'us_equity_screening_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('collection_run_id', sa.Integer(), nullable=True),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('stage', sa.String(length=32), nullable=False),
        sa.Column('eligible_for_agent', sa.Boolean(), nullable=False),
        sa.Column('rank', sa.Integer(), nullable=True),
        sa.Column('composite_score', sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column('metrics_payload', sa.JSON(), nullable=False),
        sa.Column('decision_payload', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['collection_run_id'], ['us_market_data_collection_runs.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_us_equity_screening_symbol_date'),
    )
    op.create_index('ix_us_equity_screening_snapshots_symbol_id', 'us_equity_screening_snapshots', ['symbol_id'])
    op.create_index('ix_us_equity_screening_snapshots_collection_run_id', 'us_equity_screening_snapshots', ['collection_run_id'])
    op.create_index('ix_us_equity_screening_snapshots_market_date', 'us_equity_screening_snapshots', ['market_date'])
    op.create_index('ix_us_equity_screening_snapshots_stage', 'us_equity_screening_snapshots', ['stage'])
    op.create_index(
        'ix_us_equity_screening_snapshots_eligible_for_agent',
        'us_equity_screening_snapshots',
        ['eligible_for_agent'],
    )
    op.create_index(
        'ix_us_screening_date_eligible_rank',
        'us_equity_screening_snapshots',
        ['market_date', 'eligible_for_agent', 'rank'],
    )


def downgrade() -> None:
    op.drop_table('us_equity_screening_snapshots')
    op.drop_table('us_equity_fundamental_snapshots')
    op.drop_table('us_equity_price_bars')
    op.drop_table('us_market_data_collection_runs')
