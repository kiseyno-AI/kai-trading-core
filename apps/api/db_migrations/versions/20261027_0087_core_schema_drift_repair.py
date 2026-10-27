"""reconcile Core metadata with the deployed schema

This revision is deliberately additive.  Production already owns many of the
indexes below, while a database built only from the migration chain does not.
The upgrade therefore preserves every existing index, normalizes the two
known fresh-chain advanced-value names without rebuilding them, and creates
only missing indexes.

The concurrent index statements commit independently.  If an index build is
interrupted, first repair any INVALID index PostgreSQL left behind, then rerun
the upgrade; completed statements are intentionally safe to repeat.

Downgrade is intentionally forward-only.  Once this revision has run there is
no reliable way to distinguish a pre-existing production index from an index
created by 0087, so a destructive downgrade could remove production-owned
objects.  Restore a verified backup or apply a forward repair instead.

Revision ID: 20261027_0087
Revises: 20261027_0086
Create Date: 2026-10-27 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20261027_0087'
down_revision = '20261027_0086'
branch_labels = None
depends_on = None


PORTFOLIO_TABLE_NAME = 'portfolio_asset_snapshots'
FAVORITE_TABLE_NAME = 'favorite_symbols'

ADVANCED_VALUE_INDEX_RENAMES = (
    (
        'ix_daily_advanced_value_factor_results_fcf_yield_valid',
        'ix_daily_advanced_value_factor_results_fcf_yield_valid__ba82',
        'fcf_yield_valid_for_scoring',
    ),
    (
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_valid',
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_val_13f1',
        'ev_to_ebitda_valid_for_scoring',
    ),
)

# These are the 39 exact indexes observed in the deployed production schema.
# Keep the names and PostgreSQL options explicit: several intentionally use
# descending keys, NULLS LAST, INCLUDE columns, or a partial predicate.
PRESERVED_INDEX_DDLS = (
    (
        'ix_agent_context_feedback_market_agent_symbol_received_desc',
        'agent_context_feedback',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_agent_context_feedback_market_agent_symbol_received_desc '
        'ON agent_context_feedback '
        '(market_date, agent_name, symbol, received_at DESC, id DESC)',
    ),
    (
        'ix_agent_context_feedback_symbol_agent_market_received',
        'agent_context_feedback',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_agent_context_feedback_symbol_agent_market_received '
        'ON agent_context_feedback '
        '(symbol, agent_name, market_date DESC, received_at DESC)',
    ),
    (
        'ix_agent_context_feedback_symbol_market_received',
        'agent_context_feedback',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_agent_context_feedback_symbol_market_received '
        'ON agent_context_feedback '
        '(symbol, market_date DESC, received_at DESC)',
    ),
    (
        'ix_agent_context_feedback_runs_market_started_desc',
        'agent_context_feedback_runs',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_agent_context_feedback_runs_market_started_desc '
        'ON agent_context_feedback_runs (market_date, started_at DESC, id DESC)',
    ),
    (
        'ix_agent_context_feedback_runs_market_started_id',
        'agent_context_feedback_runs',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_agent_context_feedback_runs_market_started_id '
        'ON agent_context_feedback_runs (market_date, started_at DESC, id DESC)',
    ),
    (
        'ix_daily_advanced_value_factor_results_advanced_value_c_0a02',
        'daily_advanced_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_advanced_value_factor_results_advanced_value_c_0a02 '
        'ON daily_advanced_value_factor_results (advanced_value_classification)',
    ),
    (
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_val_13f1',
        'daily_advanced_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_val_13f1 '
        'ON daily_advanced_value_factor_results (ev_to_ebitda_valid_for_scoring)',
    ),
    (
        'ix_daily_advanced_value_factor_results_fcf_yield_valid__ba82',
        'daily_advanced_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_advanced_value_factor_results_fcf_yield_valid__ba82 '
        'ON daily_advanced_value_factor_results (fcf_yield_valid_for_scoring)',
    ),
    (
        'ix_daily_composite_factor_results_composite_classification',
        'daily_composite_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_composite_factor_results_composite_classification '
        'ON daily_composite_factor_results (composite_classification)',
    ),
    (
        'ix_daily_piotroski_filter_results_market_date_cover',
        'daily_piotroski_filter_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_piotroski_filter_results_market_date_cover '
        'ON daily_piotroski_filter_results (market_date, id)',
    ),
    (
        'ix_daily_price_bars_symbol_trade_date_desc_cover',
        'daily_price_bars',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_price_bars_symbol_trade_date_desc_cover '
        'ON daily_price_bars (symbol_id, trade_date DESC) '
        'INCLUDE (open_price, high_price, low_price, close_price, volume, '
        'turnover, pbr, per, dividend_yield)',
    ),
    (
        'ix_daily_pykrx_enrichment_rows_category',
        'daily_pykrx_enrichment_rows',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_pykrx_enrichment_rows_category '
        'ON daily_pykrx_enrichment_rows (category)',
    ),
    (
        'ix_daily_pykrx_enrichment_rows_market',
        'daily_pykrx_enrichment_rows',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_pykrx_enrichment_rows_market '
        'ON daily_pykrx_enrichment_rows (market)',
    ),
    (
        'ix_daily_pykrx_enrichment_rows_market_date',
        'daily_pykrx_enrichment_rows',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_pykrx_enrichment_rows_market_date '
        'ON daily_pykrx_enrichment_rows (market_date)',
    ),
    (
        'ix_daily_pykrx_enrichment_rows_scope_key',
        'daily_pykrx_enrichment_rows',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_pykrx_enrichment_rows_scope_key '
        'ON daily_pykrx_enrichment_rows (scope_key)',
    ),
    (
        'ix_daily_pykrx_enrichment_rows_scope_type',
        'daily_pykrx_enrichment_rows',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_pykrx_enrichment_rows_scope_type '
        'ON daily_pykrx_enrichment_rows (scope_type)',
    ),
    (
        'ix_daily_pykrx_enrichment_rows_source',
        'daily_pykrx_enrichment_rows',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_pykrx_enrichment_rows_source '
        'ON daily_pykrx_enrichment_rows (source)',
    ),
    (
        'ix_daily_pykrx_enrichment_rows_symbol_id',
        'daily_pykrx_enrichment_rows',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_pykrx_enrichment_rows_symbol_id '
        'ON daily_pykrx_enrichment_rows (symbol_id)',
    ),
    (
        'ix_pykrx_enrichment_market_category_order',
        'daily_pykrx_enrichment_rows',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_pykrx_enrichment_market_category_order '
        'ON daily_pykrx_enrichment_rows '
        '(market_date, category, market, scope_type, scope_key)',
    ),
    (
        'ix_daily_quality_factor_results_accruals_ratio_rank',
        'daily_quality_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_quality_factor_results_accruals_ratio_rank '
        'ON daily_quality_factor_results (accruals_ratio_rank)',
    ),
    (
        'ix_daily_quality_factor_results_debt_ratio_rank',
        'daily_quality_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_quality_factor_results_debt_ratio_rank '
        'ON daily_quality_factor_results (debt_ratio_rank)',
    ),
    (
        'ix_daily_quality_factor_results_gross_profit_to_assets_rank',
        'daily_quality_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_quality_factor_results_gross_profit_to_assets_rank '
        'ON daily_quality_factor_results (gross_profit_to_assets_rank)',
    ),
    (
        'ix_daily_quality_factor_results_operating_cash_flow_to__24ef',
        'daily_quality_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_quality_factor_results_operating_cash_flow_to__24ef '
        'ON daily_quality_factor_results '
        '(operating_cash_flow_to_net_income_rank)',
    ),
    (
        'ix_daily_quality_factor_results_roa_rank',
        'daily_quality_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_quality_factor_results_roa_rank '
        'ON daily_quality_factor_results (roa_rank)',
    ),
    (
        'ix_daily_quality_factor_results_roe_rank',
        'daily_quality_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_quality_factor_results_roe_rank '
        'ON daily_quality_factor_results (roe_rank)',
    ),
    (
        'ix_daily_technical_snapshots_eod_run_symbol_captured',
        'daily_technical_snapshots',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_technical_snapshots_eod_run_symbol_captured '
        'ON daily_technical_snapshots (eod_run_id, symbol_id, captured_at)',
    ),
    (
        'ix_daily_technical_snapshots_market_symbol',
        'daily_technical_snapshots',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_technical_snapshots_market_symbol '
        'ON daily_technical_snapshots (market_date, symbol_id)',
    ),
    (
        'ix_daily_trending_ranks_eod_run_captured',
        'daily_trending_ranks',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_trending_ranks_eod_run_captured '
        'ON daily_trending_ranks (eod_run_id, captured_at)',
    ),
    (
        'ix_daily_unified_value_factor_results_dividend_yield_rank',
        'daily_unified_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_unified_value_factor_results_dividend_yield_rank '
        'ON daily_unified_value_factor_results (dividend_yield_rank)',
    ),
    (
        'ix_daily_unified_value_factor_results_ev_to_ebitda_rank',
        'daily_unified_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_unified_value_factor_results_ev_to_ebitda_rank '
        'ON daily_unified_value_factor_results (ev_to_ebitda_rank)',
    ),
    (
        'ix_daily_unified_value_factor_results_fcf_yield_rank',
        'daily_unified_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_unified_value_factor_results_fcf_yield_rank '
        'ON daily_unified_value_factor_results (fcf_yield_rank)',
    ),
    (
        'ix_daily_unified_value_factor_results_pbr_rank',
        'daily_unified_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_unified_value_factor_results_pbr_rank '
        'ON daily_unified_value_factor_results (pbr_rank)',
    ),
    (
        'ix_daily_unified_value_factor_results_per_rank',
        'daily_unified_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_unified_value_factor_results_per_rank '
        'ON daily_unified_value_factor_results (per_rank)',
    ),
    (
        'ix_daily_unified_value_factor_results_unified_value_rank',
        'daily_unified_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_unified_value_factor_results_unified_value_rank '
        'ON daily_unified_value_factor_results (unified_value_rank)',
    ),
    (
        'ix_daily_unified_value_factor_results_unified_value_status',
        'daily_unified_value_factor_results',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_daily_unified_value_factor_results_unified_value_status '
        'ON daily_unified_value_factor_results (unified_value_status)',
    ),
    (
        'ix_dart_events_captured_stock_receipt_id',
        'dart_disclosure_events',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_dart_events_captured_stock_receipt_id '
        'ON dart_disclosure_events '
        '(stock_code, receipt_date DESC NULLS LAST, id) '
        "WHERE fundamentals_capture_status = 'captured'",
    ),
    (
        'ix_dart_fund_snap_fs_report_period_year_captured',
        'dart_fundamentals_snapshots',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_dart_fund_snap_fs_report_period_year_captured '
        'ON dart_fundamentals_snapshots '
        '(fs_div, report_code, period_type, business_year, captured_at DESC, '
        'disclosure_event_id)',
    ),
    (
        'ix_dart_fund_snap_symbol_fs_event_order',
        'dart_fundamentals_snapshots',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_dart_fund_snap_symbol_fs_event_order '
        'ON dart_fundamentals_snapshots '
        '(stock_code, fs_div, disclosure_event_id, business_year DESC, '
        'report_code DESC, id DESC)',
    ),
    (
        'ix_signal_decider_snapshots_market_symbol_agent_created_desc',
        'signal_decider_decision_snapshots',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'ix_signal_decider_snapshots_market_symbol_agent_created_desc '
        'ON signal_decider_decision_snapshots '
        '(market_date, symbol, agent_name, created_at DESC, id DESC)',
    ),
)

DESIRED_TECHNICAL_INDEX_DDL = (
    'ix_daily_technical_snapshots_eod_run_captured_id',
    'daily_technical_snapshots',
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
    'ix_daily_technical_snapshots_eod_run_captured_id '
    'ON daily_technical_snapshots (eod_run_id, captured_at DESC, id)',
)

FAVORITE_INDEX_DDL = (
    'ix_favorite_symbols_symbol_id',
    FAVORITE_TABLE_NAME,
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
    'ix_favorite_symbols_symbol_id ON favorite_symbols (symbol_id)',
)


def _require_postgresql(bind: sa.Connection) -> None:
    if bind.dialect.name != 'postgresql':
        raise RuntimeError(
            '20261027_0087 requires PostgreSQL because the preserved index '
            'contract uses CONCURRENTLY, INCLUDE, and PostgreSQL predicates'
        )


def _assert_required_tables(inspector: sa.Inspector) -> set[str]:
    table_names = set(inspector.get_table_names())
    required_table_names = {
        table_name for _index_name, table_name, _ddl in PRESERVED_INDEX_DDLS
    }
    required_table_names.update(
        {
            DESIRED_TECHNICAL_INDEX_DDL[1],
            PORTFOLIO_TABLE_NAME,
            'symbols',
        }
    )
    missing_table_names = sorted(required_table_names - table_names)
    if missing_table_names:
        raise RuntimeError(
            '20261027_0087 cannot repair an incomplete schema; missing '
            f"required tables: {', '.join(missing_table_names)}"
        )
    return table_names


def _assert_no_invalid_target_indexes(bind: sa.Connection) -> None:
    target_index_names = {
        index_name for index_name, _table_name, _ddl in PRESERVED_INDEX_DDLS
    }
    target_index_names.update(
        {
            DESIRED_TECHNICAL_INDEX_DDL[0],
            FAVORITE_INDEX_DDL[0],
        }
    )
    target_index_names.update(
        legacy_name
        for legacy_name, _target_name, _column_name
        in ADVANCED_VALUE_INDEX_RENAMES
    )
    invalid_index = bind.execute(
        sa.text(
            'SELECT index_class.relname, pg_index.indisvalid, '
            'pg_index.indisready '
            'FROM pg_index '
            'JOIN pg_class AS index_class '
            'ON index_class.oid = pg_index.indexrelid '
            'JOIN pg_namespace AS index_namespace '
            'ON index_namespace.oid = index_class.relnamespace '
            'WHERE index_namespace.nspname = current_schema() '
            'AND index_class.relname IN :index_names '
            'AND (NOT pg_index.indisvalid OR NOT pg_index.indisready) '
            'LIMIT 1'
        ).bindparams(sa.bindparam('index_names', expanding=True)),
        {'index_names': sorted(target_index_names)},
    ).first()
    if invalid_index is not None:
        raise RuntimeError(
            '20261027_0087 found an invalid or not-ready target index '
            f'({invalid_index[0]}); CREATE INDEX CONCURRENTLY IF NOT EXISTS '
            'would skip it, so repair that index explicitly before retrying'
        )


def _portfolio_as_of_requires_not_null(
    bind: sa.Connection,
    inspector: sa.Inspector,
) -> bool:
    columns = {
        column['name']: column
        for column in inspector.get_columns(PORTFOLIO_TABLE_NAME)
    }
    as_of_column = columns.get('as_of')
    if as_of_column is None:
        raise RuntimeError(
            '20261027_0087 requires portfolio_asset_snapshots.as_of to exist'
        )

    nullable = as_of_column.get('nullable')
    if nullable is False:
        return False
    if nullable is not True:
        raise RuntimeError(
            '20261027_0087 could not determine whether '
            'portfolio_asset_snapshots.as_of is nullable'
        )

    null_row = bind.execute(
        sa.text(
            'SELECT id FROM portfolio_asset_snapshots '
            'WHERE as_of IS NULL LIMIT 1'
        )
    ).first()
    if null_row is not None:
        raise RuntimeError(
            '20261027_0087 refuses to set '
            'portfolio_asset_snapshots.as_of NOT NULL while NULL rows exist; '
            'repair the data explicitly and rerun the migration'
        )
    return True


def _advanced_value_renames(
    inspector: sa.Inspector,
) -> tuple[tuple[str, str], ...]:
    table_name = 'daily_advanced_value_factor_results'
    indexes = {
        index['name']: index
        for index in inspector.get_indexes(table_name)
    }
    renames: list[tuple[str, str]] = []
    for legacy_name, target_name, expected_column in ADVANCED_VALUE_INDEX_RENAMES:
        legacy_index = indexes.get(legacy_name)
        target_index = indexes.get(target_name)
        if legacy_index is not None and target_index is not None:
            raise RuntimeError(
                '20261027_0087 found both legacy and canonical advanced-value '
                f'indexes ({legacy_name}, {target_name}); refusing to choose '
                'or delete either index automatically'
            )

        existing_index = target_index or legacy_index
        if existing_index is not None:
            column_names = tuple(existing_index.get('column_names') or ())
            if column_names != (expected_column,) or existing_index.get('unique'):
                raise RuntimeError(
                    '20261027_0087 found an unexpected definition for '
                    f"{existing_index['name']}: columns={column_names!r}, "
                    f"unique={bool(existing_index.get('unique'))!r}"
                )

        if legacy_index is not None:
            renames.append((legacy_name, target_name))
    return tuple(renames)


def _create_favorite_symbols_table() -> None:
    op.create_table(
        FAVORITE_TABLE_NAME,
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('sort_order', sa.Integer(), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ['symbol_id'],
            ['symbols.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'symbol_id',
            name='uq_favorite_symbols_symbol_id',
        ),
    )


def upgrade() -> None:
    bind = op.get_bind()
    _require_postgresql(bind)
    inspector = sa.inspect(bind)

    # Complete every fail-closed check before the first schema mutation.  This
    # matters because the later concurrent index builds commit independently.
    _assert_no_invalid_target_indexes(bind)
    table_names = _assert_required_tables(inspector)
    alter_portfolio_as_of = _portfolio_as_of_requires_not_null(
        bind,
        inspector,
    )
    advanced_value_renames = _advanced_value_renames(inspector)

    # Protect the transactional table/column/rename repairs from waiting
    # indefinitely for catalog locks.  SET LOCAL expires when the
    # autocommit block commits this transaction.
    op.execute(sa.text("SET LOCAL lock_timeout = '5s'"))

    if FAVORITE_TABLE_NAME not in table_names:
        _create_favorite_symbols_table()

    if alter_portfolio_as_of:
        op.alter_column(
            PORTFOLIO_TABLE_NAME,
            'as_of',
            existing_type=sa.DateTime(timezone=True),
            existing_nullable=True,
            nullable=False,
        )

    for legacy_name, target_name in advanced_value_renames:
        op.execute(
            sa.text(
                f'ALTER INDEX {legacy_name} RENAME TO {target_name}'
            )
        )

    index_ddls = (
        *PRESERVED_INDEX_DDLS,
        DESIRED_TECHNICAL_INDEX_DDL,
        FAVORITE_INDEX_DDL,
    )
    with op.get_context().autocommit_block():
        # Index creation may legitimately outlive the application's normal
        # statement budget.  A short lock timeout keeps deployment fail-closed
        # instead of waiting indefinitely for the brief catalog/table locks.
        op.execute(sa.text('SET statement_timeout = 0'))
        op.execute(sa.text("SET lock_timeout = '5s'"))
        try:
            for _index_name, _table_name, ddl in index_ddls:
                op.execute(sa.text(ddl))
        finally:
            op.execute(sa.text('RESET lock_timeout'))
            op.execute(sa.text('RESET statement_timeout'))


def downgrade() -> None:
    raise RuntimeError(
        '20261027_0087 is a forward-only schema repair: the database does not '
        'record whether each repaired object predated 0087, so downgrade '
        'cannot safely remove indexes, relax portfolio_asset_snapshots.as_of, '
        'or drop favorite_symbols. Restore a verified pre-upgrade backup or '
        'apply a reviewed forward migration instead.'
    )
