"""initial market schema

Revision ID: 20260405_0001
Revises:
Create Date: 2026-04-05 12:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260405_0001'
down_revision = None
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {column['name'] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    legacy_tracked_symbols = False

    if _table_exists(inspector, 'tracked_symbols') and 'symbol' in _column_names(inspector, 'tracked_symbols'):
        op.rename_table('tracked_symbols', 'tracked_symbols_legacy')
        inspector = sa.inspect(bind)
        legacy_tracked_symbols = True

    if not _table_exists(inspector, 'symbols'):
        op.create_table(
            'symbols',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('symbol', sa.String(length=32), nullable=False),
            sa.Column('standard_code', sa.String(length=32), nullable=True),
            sa.Column('name_ko', sa.String(length=255), nullable=False),
            sa.Column('market', sa.String(length=32), nullable=False),
            sa.Column('instrument_type', sa.String(length=32), nullable=False),
            sa.Column('source', sa.String(length=32), nullable=False),
            sa.Column('is_active', sa.Boolean(), nullable=False),
            sa.Column('last_refreshed_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('symbol'),
        )
        op.create_index(op.f('ix_symbols_market'), 'symbols', ['market'], unique=False)
        op.create_index(op.f('ix_symbols_symbol'), 'symbols', ['symbol'], unique=False)
        inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'tracked_symbols'):
        op.create_table(
            'tracked_symbols',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('symbol_id', sa.Integer(), nullable=False),
            sa.Column('sort_order', sa.Integer(), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('symbol_id', name='uq_tracked_symbols_symbol_id'),
        )
        op.create_index(op.f('ix_tracked_symbols_symbol_id'), 'tracked_symbols', ['symbol_id'], unique=False)
        inspector = sa.inspect(bind)

    if legacy_tracked_symbols:
        legacy_rows = bind.execute(
            sa.text('SELECT symbol, created_at FROM tracked_symbols_legacy ORDER BY id')
        ).mappings()
        for sort_order, row in enumerate(legacy_rows):
            symbol_id = bind.execute(
                sa.text(
                    """
                    INSERT INTO symbols (
                        symbol,
                        name_ko,
                        market,
                        instrument_type,
                        source,
                        is_active,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :symbol,
                        :name_ko,
                        'KRX',
                        'equity',
                        'legacy_tracked_symbol',
                        true,
                        COALESCE(:created_at, now()),
                        now()
                    )
                    ON CONFLICT (symbol) DO UPDATE
                    SET updated_at = now()
                    RETURNING id
                    """
                ),
                {
                    'symbol': row['symbol'],
                    'name_ko': row['symbol'],
                    'created_at': row['created_at'],
                },
            ).scalar_one()
            bind.execute(
                sa.text(
                    """
                    INSERT INTO tracked_symbols (symbol_id, sort_order, created_at)
                    VALUES (:symbol_id, :sort_order, COALESCE(:created_at, now()))
                    ON CONFLICT (symbol_id) DO UPDATE
                    SET sort_order = EXCLUDED.sort_order
                    """
                ),
                {
                    'symbol_id': symbol_id,
                    'sort_order': sort_order,
                    'created_at': row['created_at'],
                },
            )
        op.drop_table('tracked_symbols_legacy')
        inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'quote_snapshots'):
        op.create_table(
            'quote_snapshots',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('symbol_id', sa.Integer(), nullable=False),
            sa.Column('market_state', sa.String(length=32), nullable=False),
            sa.Column('price', sa.BigInteger(), nullable=False),
            sa.Column('change', sa.BigInteger(), nullable=False),
            sa.Column('change_pct', sa.Numeric(precision=12, scale=4), nullable=False),
            sa.Column('volume', sa.BigInteger(), nullable=False),
            sa.Column('turnover', sa.BigInteger(), nullable=False),
            sa.Column('company_name', sa.String(length=255), nullable=True),
            sa.Column('as_of', sa.String(length=32), nullable=True),
            sa.Column('source', sa.String(length=32), nullable=False),
            sa.Column('is_live', sa.Boolean(), nullable=False),
            sa.Column('fallback', sa.Boolean(), nullable=False),
            sa.Column('payload', sa.JSON(), nullable=False),
            sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index(op.f('ix_quote_snapshots_captured_at'), 'quote_snapshots', ['captured_at'], unique=False)
        op.create_index(op.f('ix_quote_snapshots_symbol_id'), 'quote_snapshots', ['symbol_id'], unique=False)

    if not _table_exists(inspector, 'current_quote_cache'):
        op.create_table(
            'current_quote_cache',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('symbol_id', sa.Integer(), nullable=False),
            sa.Column('market_state', sa.String(length=32), nullable=False),
            sa.Column('price', sa.BigInteger(), nullable=False),
            sa.Column('change', sa.BigInteger(), nullable=False),
            sa.Column('change_pct', sa.Numeric(precision=12, scale=4), nullable=False),
            sa.Column('volume', sa.BigInteger(), nullable=False),
            sa.Column('turnover', sa.BigInteger(), nullable=False),
            sa.Column('company_name', sa.String(length=255), nullable=True),
            sa.Column('as_of', sa.String(length=32), nullable=True),
            sa.Column('source', sa.String(length=32), nullable=False),
            sa.Column('is_live', sa.Boolean(), nullable=False),
            sa.Column('fallback', sa.Boolean(), nullable=False),
            sa.Column('payload', sa.JSON(), nullable=False),
            sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('symbol_id', name='uq_current_quote_cache_symbol_id'),
        )
        op.create_index(op.f('ix_current_quote_cache_symbol_id'), 'current_quote_cache', ['symbol_id'], unique=False)

    if not _table_exists(inspector, 'market_status_snapshots'):
        op.create_table(
            'market_status_snapshots',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('market', sa.String(length=32), nullable=False),
            sa.Column('state', sa.String(length=32), nullable=False),
            sa.Column('is_trading_day', sa.Boolean(), nullable=False),
            sa.Column('is_open', sa.Boolean(), nullable=False),
            sa.Column('reason', sa.String(length=255), nullable=True),
            sa.Column('server_time', sa.DateTime(timezone=True), nullable=False),
            sa.Column('session_open_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('session_close_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('source', sa.String(length=32), nullable=False),
            sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index(op.f('ix_market_status_snapshots_market'), 'market_status_snapshots', ['market'], unique=False)
        op.create_index(op.f('ix_market_status_snapshots_server_time'), 'market_status_snapshots', ['server_time'], unique=False)
        op.create_index(op.f('ix_market_status_snapshots_state'), 'market_status_snapshots', ['state'], unique=False)

    if not _table_exists(inspector, 'eod_collection_runs'):
        op.create_table(
            'eod_collection_runs',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('market', sa.String(length=32), nullable=False),
            sa.Column('trade_date', sa.Date(), nullable=False),
            sa.Column('source', sa.String(length=32), nullable=False),
            sa.Column('trigger', sa.String(length=32), nullable=False),
            sa.Column('status', sa.String(length=32), nullable=False),
            sa.Column('symbols_total', sa.Integer(), nullable=False),
            sa.Column('symbols_succeeded', sa.Integer(), nullable=False),
            sa.Column('symbols_failed', sa.Integer(), nullable=False),
            sa.Column('error_message', sa.Text(), nullable=True),
            sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('run_metadata', sa.JSON(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index(op.f('ix_eod_collection_runs_status'), 'eod_collection_runs', ['status'], unique=False)
        op.create_index(op.f('ix_eod_collection_runs_trade_date'), 'eod_collection_runs', ['trade_date'], unique=False)

    if not _table_exists(inspector, 'daily_price_bars'):
        op.create_table(
            'daily_price_bars',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('symbol_id', sa.Integer(), nullable=False),
            sa.Column('eod_run_id', sa.Integer(), nullable=True),
            sa.Column('trade_date', sa.Date(), nullable=False),
            sa.Column('open_price', sa.BigInteger(), nullable=False),
            sa.Column('high_price', sa.BigInteger(), nullable=False),
            sa.Column('low_price', sa.BigInteger(), nullable=False),
            sa.Column('close_price', sa.BigInteger(), nullable=False),
            sa.Column('volume', sa.BigInteger(), nullable=False),
            sa.Column('turnover', sa.BigInteger(), nullable=False),
            sa.Column('source', sa.String(length=32), nullable=False),
            sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['eod_run_id'], ['eod_collection_runs.id'], ondelete='SET NULL'),
            sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('symbol_id', 'trade_date', name='uq_daily_price_bars_symbol_trade_date'),
        )
        op.create_index(op.f('ix_daily_price_bars_eod_run_id'), 'daily_price_bars', ['eod_run_id'], unique=False)
        op.create_index(op.f('ix_daily_price_bars_symbol_id'), 'daily_price_bars', ['symbol_id'], unique=False)
        op.create_index(op.f('ix_daily_price_bars_trade_date'), 'daily_price_bars', ['trade_date'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_daily_price_bars_trade_date'), table_name='daily_price_bars')
    op.drop_index(op.f('ix_daily_price_bars_symbol_id'), table_name='daily_price_bars')
    op.drop_index(op.f('ix_daily_price_bars_eod_run_id'), table_name='daily_price_bars')
    op.drop_table('daily_price_bars')
    op.drop_index(op.f('ix_eod_collection_runs_trade_date'), table_name='eod_collection_runs')
    op.drop_index(op.f('ix_eod_collection_runs_status'), table_name='eod_collection_runs')
    op.drop_table('eod_collection_runs')
    op.drop_index(op.f('ix_market_status_snapshots_state'), table_name='market_status_snapshots')
    op.drop_index(op.f('ix_market_status_snapshots_server_time'), table_name='market_status_snapshots')
    op.drop_index(op.f('ix_market_status_snapshots_market'), table_name='market_status_snapshots')
    op.drop_table('market_status_snapshots')
    op.drop_index(op.f('ix_current_quote_cache_symbol_id'), table_name='current_quote_cache')
    op.drop_table('current_quote_cache')
    op.drop_index(op.f('ix_quote_snapshots_symbol_id'), table_name='quote_snapshots')
    op.drop_index(op.f('ix_quote_snapshots_captured_at'), table_name='quote_snapshots')
    op.drop_table('quote_snapshots')
    op.drop_index(op.f('ix_tracked_symbols_symbol_id'), table_name='tracked_symbols')
    op.drop_table('tracked_symbols')
    op.drop_index(op.f('ix_symbols_symbol'), table_name='symbols')
    op.drop_index(op.f('ix_symbols_market'), table_name='symbols')
    op.drop_table('symbols')
