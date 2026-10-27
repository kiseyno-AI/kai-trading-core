from __future__ import annotations

import importlib
import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Index, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from app.db_models import Base


def _indexes(table_name: str) -> dict[str, Index]:
    table = Base.metadata.tables[table_name]
    return {index.name: index for index in table.indexes}


def _unique_constraints(table_name: str) -> dict[str | None, tuple[str, ...]]:
    table = Base.metadata.tables[table_name]
    return {
        constraint.name: tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }


def _check_constraints(table_name: str) -> dict[str | None, str]:
    table = Base.metadata.tables[table_name]
    return {
        constraint.name: str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }


def _foreign_keys(
    table_name: str,
) -> dict[str | None, tuple[tuple[str, ...], tuple[str, ...], str | None]]:
    table = Base.metadata.tables[table_name]
    return {
        constraint.name: (
            tuple(element.parent.name for element in constraint.elements),
            tuple(element.target_fullname for element in constraint.elements),
            constraint.ondelete,
        )
        for constraint in table.constraints
        if isinstance(constraint, ForeignKeyConstraint)
    }


def _postgres_index_ddl(table_name: str, index_name: str) -> str:
    index = _indexes(table_name)[index_name]
    return str(CreateIndex(index).compile(dialect=postgresql.dialect()))


class SchemaMetadataContractTests(unittest.TestCase):
    def test_wait_recheck_event_metadata_supports_lineage_generations(self) -> None:
        table_name = 'signal_decider_wait_recheck_events'
        table = Base.metadata.tables[table_name]
        constraints = _unique_constraints(
            table_name
        )

        self.assertEqual(
            constraints[
                'uq_signal_decider_wait_recheck_target_symbol_agent_source'
            ],
            (
                'target_trade_date',
                'symbol',
                'agent_name',
                'source_snapshot_id',
            ),
        )
        self.assertFalse(table.c.generation.nullable)
        self.assertEqual(str(table.c.generation.server_default.arg), '1')
        self.assertTrue(table.c.rearmed_from_event_id.nullable)
        self.assertEqual(
            _foreign_keys(table_name)[
                'fk_signal_decider_wait_recheck_rearmed_from'
            ],
            (
                ('rearmed_from_event_id',),
                ('signal_decider_wait_recheck_events.id',),
                'RESTRICT',
            ),
        )
        generation_check = _check_constraints(table_name)[
            'ck_signal_decider_wait_recheck_generation_lineage'
        ]
        self.assertIn('generation >= 1', generation_check)
        self.assertIn('generation = 1', generation_check)
        self.assertIn('generation > 1', generation_check)
        self.assertEqual(
            tuple(
                column.name
                for column in _indexes(table_name)[
                    'ix_signal_decider_wait_recheck_rearmed_from'
                ].columns
            ),
            ('rearmed_from_event_id',),
        )

    def test_wait_recheck_generation_migration_refuses_lossy_downgrade(
        self,
    ) -> None:
        migration = importlib.import_module(
            'db_migrations.versions.'
            '20261027_0086_wait_recheck_generations'
        )
        bind = MagicMock()
        bind.execute.return_value.first.return_value = (
            '2026-10-27',
            '005930',
            'signal-decider',
            2,
        )

        with (
            patch.object(migration.op, 'get_bind', return_value=bind),
            patch.object(migration.op, 'drop_constraint') as drop_constraint,
            patch.object(migration.op, 'drop_index') as drop_index,
            patch.object(migration.op, 'drop_column') as drop_column,
            patch.object(
                migration.op,
                'create_unique_constraint',
            ) as create_unique_constraint,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'forward-only.*same-lane.*multi-generation',
            ):
                migration.downgrade()

        drop_constraint.assert_not_called()
        drop_index.assert_not_called()
        drop_column.assert_not_called()
        create_unique_constraint.assert_not_called()
        downgrade_guard_sql = str(bind.execute.call_args.args[0])
        self.assertIn('attempt_history_payload::jsonb', downgrade_guard_sql)
        self.assertIn("'[]'::jsonb", downgrade_guard_sql)

        bind.reset_mock()
        bind.execute.return_value.first.return_value = None
        with (
            patch.object(migration.op, 'get_bind', return_value=bind),
            patch.object(migration.op, 'drop_constraint') as drop_constraint,
            patch.object(migration.op, 'drop_index') as drop_index,
            patch.object(migration.op, 'drop_column') as drop_column,
            patch.object(
                migration.op,
                'create_unique_constraint',
            ) as create_unique_constraint,
        ):
            migration.downgrade()

        drop_constraint.assert_any_call(
            'uq_signal_decider_wait_recheck_target_symbol_agent_source',
            'signal_decider_wait_recheck_events',
            type_='unique',
        )
        drop_constraint.assert_any_call(
            'ck_signal_decider_wait_recheck_generation_lineage',
            'signal_decider_wait_recheck_events',
            type_='check',
        )
        drop_constraint.assert_any_call(
            'fk_signal_decider_wait_recheck_rearmed_from',
            'signal_decider_wait_recheck_events',
            type_='foreignkey',
        )
        create_unique_constraint.assert_called_once_with(
            'uq_signal_decider_wait_recheck_target_symbol_agent',
            'signal_decider_wait_recheck_events',
            ['target_trade_date', 'symbol', 'agent_name'],
        )
        drop_index.assert_called_once_with(
            'ix_signal_decider_wait_recheck_rearmed_from',
            table_name='signal_decider_wait_recheck_events',
        )
        self.assertEqual(
            [call.args[1] for call in drop_column.call_args_list],
            [
                'rearmed_from_event_id',
                'attempt_history_payload',
                'generation',
            ],
        )

    def test_wait_recheck_generation_migration_upgrades_existing_rows_safely(
        self,
    ) -> None:
        migration = importlib.import_module(
            'db_migrations.versions.'
            '20261027_0086_wait_recheck_generations'
        )

        with (
            patch.object(migration.op, 'add_column') as add_column,
            patch.object(migration.op, 'create_foreign_key') as create_foreign_key,
            patch.object(migration.op, 'create_index') as create_index,
            patch.object(
                migration.op,
                'create_check_constraint',
            ) as create_check_constraint,
            patch.object(migration.op, 'drop_constraint') as drop_constraint,
            patch.object(
                migration.op,
                'create_unique_constraint',
            ) as create_unique_constraint,
        ):
            migration.upgrade()

        self.assertEqual(add_column.call_count, 3)
        generation_column = add_column.call_args_list[0].args[1]
        self.assertEqual(generation_column.name, 'generation')
        self.assertFalse(generation_column.nullable)
        self.assertEqual(str(generation_column.server_default.arg), '1')
        rearmed_column = add_column.call_args_list[1].args[1]
        self.assertEqual(rearmed_column.name, 'rearmed_from_event_id')
        self.assertTrue(rearmed_column.nullable)
        attempt_history_column = add_column.call_args_list[2].args[1]
        self.assertEqual(
            attempt_history_column.name,
            'attempt_history_payload',
        )
        self.assertFalse(attempt_history_column.nullable)
        self.assertEqual(
            str(attempt_history_column.server_default.arg),
            "'[]'::json",
        )
        create_foreign_key.assert_called_once_with(
            'fk_signal_decider_wait_recheck_rearmed_from',
            'signal_decider_wait_recheck_events',
            'signal_decider_wait_recheck_events',
            ['rearmed_from_event_id'],
            ['id'],
            ondelete='RESTRICT',
        )
        create_index.assert_called_once_with(
            'ix_signal_decider_wait_recheck_rearmed_from',
            'signal_decider_wait_recheck_events',
            ['rearmed_from_event_id'],
            unique=False,
        )
        create_check_constraint.assert_called_once()
        drop_constraint.assert_called_once_with(
            'uq_signal_decider_wait_recheck_target_symbol_agent',
            'signal_decider_wait_recheck_events',
            type_='unique',
        )
        create_unique_constraint.assert_called_once_with(
            'uq_signal_decider_wait_recheck_target_symbol_agent_source',
            'signal_decider_wait_recheck_events',
            [
                'target_trade_date',
                'symbol',
                'agent_name',
                'source_snapshot_id',
            ],
        )

    def test_wait_recheck_agent_unique_downgrade_refuses_dual_lane_data(
        self,
    ) -> None:
        migration = importlib.import_module(
            'db_migrations.versions.'
            '20261025_0085_agent_aware_wait_recheck_events'
        )
        bind = MagicMock()
        bind.execute.return_value.first.return_value = (
            '2026-10-26',
            '005930',
            2,
        )

        with (
            patch.object(migration.op, 'get_bind', return_value=bind),
            patch.object(migration.op, 'drop_constraint') as drop_constraint,
            patch.object(
                migration.op,
                'create_unique_constraint',
            ) as create_unique_constraint,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'forward-only.*dual-lane',
            ):
                migration.downgrade()

        drop_constraint.assert_not_called()
        create_unique_constraint.assert_not_called()

        bind.reset_mock()
        bind.execute.return_value.first.return_value = None
        with (
            patch.object(migration.op, 'get_bind', return_value=bind),
            patch.object(migration.op, 'drop_constraint') as drop_constraint,
            patch.object(
                migration.op,
                'create_unique_constraint',
            ) as create_unique_constraint,
        ):
            migration.downgrade()

        drop_constraint.assert_called_once_with(
            'uq_signal_decider_wait_recheck_target_symbol_agent',
            'signal_decider_wait_recheck_events',
            type_='unique',
        )
        create_unique_constraint.assert_called_once_with(
            'uq_signal_decider_wait_recheck_target_symbol',
            'signal_decider_wait_recheck_events',
            ['target_trade_date', 'symbol'],
        )

    def test_weekly_learning_artifact_metadata_is_versioned_and_queryable(self) -> None:
        table_name = 'signal_decider_weekly_learning_artifacts'
        table = Base.metadata.tables[table_name]

        self.assertTrue(
            {
                'artifact_version',
                'week_start',
                'week_end',
                'valid_through',
                'source_fingerprint',
                'content_sha256',
                'publication_status',
                'eligibility_payload',
                'learning_context_payload',
                'evidence_manifest_payload',
                'source_report_payload',
                'published_at',
            }.issubset(set(table.columns.keys()))
        )
        constraints = _unique_constraints(table_name)
        self.assertEqual(
            constraints['uq_signal_decider_weekly_learning_artifact_version'],
            ('artifact_version',),
        )
        self.assertEqual(
            constraints['uq_signal_decider_weekly_learning_source_fingerprint'],
            ('source_fingerprint',),
        )
        self.assertEqual(
            _postgres_index_ddl(
                table_name,
                'ix_signal_decider_weekly_learning_latest_published',
            ),
            'CREATE INDEX ix_signal_decider_weekly_learning_latest_published '
            'ON signal_decider_weekly_learning_artifacts '
            '(publication_status, week_end DESC, published_at DESC, id DESC)',
        )

    def test_signal_decider_policy_version_columns_share_registry_capacity(self) -> None:
        expected_columns = {
            ('signal_decider_decision_snapshots', 'policy_version'),
            ('signal_decider_realized_outcomes', 'policy_version'),
            ('signal_decider_evaluations', 'policy_version'),
            ('signal_decider_calendar_review_items', 'policy_version'),
            ('signal_decider_policy_versions', 'policy_version'),
            ('signal_decider_policy_versions', 'parent_policy_version'),
            ('signal_decider_policy_versions', 'source_candidate_version'),
            ('signal_decider_policy_candidates', 'candidate_version'),
            ('signal_decider_policy_candidates', 'base_policy_version'),
            ('signal_decider_execution_plan_items', 'policy_version'),
            ('signal_decider_actual_execution_outcomes', 'policy_version'),
        }

        for table_name, column_name in expected_columns:
            with self.subTest(table_name=table_name, column_name=column_name):
                column = Base.metadata.tables[table_name].c[column_name]
                self.assertEqual(column.type.length, 128)

    def test_migration_owned_legacy_objects_remain_in_metadata(self) -> None:
        portfolio = Base.metadata.tables['portfolio_asset_snapshots']
        self.assertEqual(
            set(portfolio.columns.keys()),
            {
                'id',
                'account_reference',
                'account_label',
                'total_assets',
                'currency',
                'as_of',
                'captured_at',
                'source',
                'snapshot_payload',
            },
        )
        self.assertEqual(
            set(_indexes('portfolio_asset_snapshots')),
            {
                'ix_portfolio_asset_snapshots_account_captured',
                'ix_portfolio_asset_snapshots_captured_at',
            },
        )

        highlights = Base.metadata.tables['dart_fundamentals_highlights']
        self.assertTrue(
            {
                'source_api',
                'index_code',
                'index_class_code',
                'index_class_name',
                'period_end_date',
                'detail_payload',
            }.issubset(set(highlights.columns.keys()))
        )

    def test_postgresql_tuned_indexes_preserve_order_and_names(self) -> None:
        self.assertEqual(
            _postgres_index_ddl(
                'daily_technical_snapshots',
                'ix_daily_technical_snapshots_eod_run_captured_id',
            ),
            'CREATE INDEX ix_daily_technical_snapshots_eod_run_captured_id '
            'ON daily_technical_snapshots (eod_run_id, captured_at DESC, id)',
        )
        self.assertEqual(
            _postgres_index_ddl(
                'eod_collection_runs',
                'ix_eod_collection_runs_market_trade_source_started_id',
            ),
            'CREATE INDEX ix_eod_collection_runs_market_trade_source_started_id '
            'ON eod_collection_runs (market, trade_date, source, started_at DESC, id DESC)',
        )
        self.assertEqual(
            _postgres_index_ddl(
                'dart_fundamentals_snapshots',
                'ix_dart_fundamentals_snapshots_fs_updated_stock',
            ),
            'CREATE INDEX ix_dart_fundamentals_snapshots_fs_updated_stock '
            'ON dart_fundamentals_snapshots (fs_div, updated_at, stock_code)',
        )

    def test_unique_constraints_and_lookup_indexes_remain_distinct(self) -> None:
        expected = {
            'symbols': (
                'symbols_symbol_key',
                ('symbol',),
                'ix_symbols_symbol',
                False,
            ),
            'dart_disclosure_events': (
                'dart_disclosure_events_rcept_no_key',
                ('rcept_no',),
                'ix_dart_disclosure_events_rcept_no',
                False,
            ),
            'agent_context_feedback_runs': (
                'uq_agent_context_feedback_runs_run_id',
                ('run_id',),
                'ix_agent_context_feedback_runs_run_id',
                True,
            ),
        }

        for table_name, (constraint_name, columns, index_name, index_unique) in expected.items():
            with self.subTest(table_name=table_name):
                self.assertEqual(_unique_constraints(table_name)[constraint_name], columns)
                index = _indexes(table_name)[index_name]
                self.assertEqual(tuple(column.name for column in index.columns), columns)
                self.assertEqual(bool(index.unique), index_unique)

    def test_metadata_does_not_reintroduce_unmigrated_single_column_indexes(self) -> None:
        absent_indexes = {
            'agent_context_feedback_runs': {
                'ix_agent_context_feedback_runs_started_at',
            },
            'daily_advanced_value_factor_results': {
                'ix_daily_advanced_value_factor_results_advanced_value_classification',
                'ix_daily_advanced_value_factor_results_ev_to_ebitda_valid_for_scoring',
                'ix_daily_advanced_value_factor_results_fcf_yield_valid_for_scoring',
            },
            'daily_composite_factor_results': {
                'ix_daily_composite_factor_results_composite_classification',
            },
            'daily_market_risk_snapshots': {
                'ix_daily_market_risk_snapshots_is_management_item',
                'ix_daily_market_risk_snapshots_is_trade_halted',
            },
            'daily_momentum_factor_results': {
                'ix_daily_momentum_factor_results_momentum_rank',
            },
            'daily_piotroski_filter_results': {
                'ix_daily_piotroski_filter_results_passed_filter',
                'ix_daily_piotroski_filter_results_piotroski_score',
            },
            'daily_pykrx_enrichment_rows': {
                'ix_daily_pykrx_enrichment_rows_category',
                'ix_daily_pykrx_enrichment_rows_market',
                'ix_daily_pykrx_enrichment_rows_market_date',
                'ix_daily_pykrx_enrichment_rows_scope_key',
                'ix_daily_pykrx_enrichment_rows_scope_type',
                'ix_daily_pykrx_enrichment_rows_source',
                'ix_daily_pykrx_enrichment_rows_symbol_id',
            },
            'daily_quality_factor_results': {
                'ix_daily_quality_factor_results_accruals_ratio_rank',
                'ix_daily_quality_factor_results_debt_ratio_rank',
                'ix_daily_quality_factor_results_gross_profit_to_assets_rank',
                'ix_daily_quality_factor_results_operating_cash_flow_to_net_income_rank',
                'ix_daily_quality_factor_results_roa_rank',
                'ix_daily_quality_factor_results_roe_rank',
            },
            'daily_unified_value_factor_results': {
                'ix_daily_unified_value_factor_results_dividend_yield_rank',
                'ix_daily_unified_value_factor_results_ev_to_ebitda_rank',
                'ix_daily_unified_value_factor_results_fcf_yield_rank',
                'ix_daily_unified_value_factor_results_pbr_rank',
                'ix_daily_unified_value_factor_results_per_rank',
                'ix_daily_unified_value_factor_results_unified_value_rank',
                'ix_daily_unified_value_factor_results_unified_value_status',
            },
            'daily_value_factor_results': {
                'ix_daily_value_factor_results_value_status',
            },
            'dart_audit_signals': {
                'ix_dart_audit_signals_corp_code',
                'ix_dart_audit_signals_disclosure_event_id',
                'ix_dart_audit_signals_has_audit_issue',
                'ix_dart_audit_signals_opinion_code',
                'ix_dart_audit_signals_opinion_label',
                'ix_dart_audit_signals_rcept_no',
                'ix_dart_audit_signals_receipt_date',
                'ix_dart_audit_signals_stock_code',
            },
            'dart_dilution_signals': {
                'ix_dart_dilution_signals_corp_code',
                'ix_dart_dilution_signals_disclosure_event_id',
                'ix_dart_dilution_signals_has_dilution_signal',
                'ix_dart_dilution_signals_rcept_no',
                'ix_dart_dilution_signals_receipt_date',
                'ix_dart_dilution_signals_signal_type',
                'ix_dart_dilution_signals_stock_code',
            },
        }

        for table_name, forbidden_names in absent_indexes.items():
            with self.subTest(table_name=table_name):
                self.assertTrue(forbidden_names.isdisjoint(_indexes(table_name)))

        advanced_indexes = _indexes('daily_advanced_value_factor_results')
        self.assertIn(
            'ix_daily_advanced_value_factor_results_fcf_yield_valid',
            advanced_indexes,
        )
        self.assertIn(
            'ix_daily_advanced_value_factor_results_ev_to_ebitda_valid',
            advanced_indexes,
        )


if __name__ == '__main__':
    unittest.main()
