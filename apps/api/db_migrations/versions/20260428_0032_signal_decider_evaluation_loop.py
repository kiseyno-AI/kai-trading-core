"""add signal decider evaluation loop tables

Revision ID: 20260428_0032
Revises: 20260428_0031
Create Date: 2026-04-28 20:15:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260428_0032'
down_revision = '20260428_0031'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'signal_decider_realized_outcomes'):
        op.create_table(
            'signal_decider_realized_outcomes',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('feedback_id', sa.Integer(), nullable=False),
            sa.Column('market_date', sa.Date(), nullable=False),
            sa.Column('symbol', sa.String(length=32), nullable=False),
            sa.Column('policy_version', sa.String(length=64), nullable=False, server_default='signal_decider_policy_v1'),
            sa.Column('decision_action', sa.String(length=16), nullable=False, server_default='wait'),
            sa.Column('horizon_days', sa.Integer(), nullable=False),
            sa.Column('outcome_status', sa.String(length=32), nullable=False, server_default='pending'),
            sa.Column('entry_trade_date', sa.Date(), nullable=True),
            sa.Column('exit_trade_date', sa.Date(), nullable=True),
            sa.Column('bars_observed', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('entry_price', sa.BigInteger(), nullable=True),
            sa.Column('exit_price', sa.BigInteger(), nullable=True),
            sa.Column('max_high_price', sa.BigInteger(), nullable=True),
            sa.Column('min_low_price', sa.BigInteger(), nullable=True),
            sa.Column('close_return_pct', sa.Numeric(12, 6), nullable=True),
            sa.Column('max_upside_return_pct', sa.Numeric(12, 6), nullable=True),
            sa.Column('max_drawdown_return_pct', sa.Numeric(12, 6), nullable=True),
            sa.Column('target_price', sa.BigInteger(), nullable=True),
            sa.Column('stop_loss_price', sa.BigInteger(), nullable=True),
            sa.Column('expected_return_pct', sa.Numeric(12, 6), nullable=True),
            sa.Column('target_hit', sa.Boolean(), nullable=False, server_default=sa.text('false')),
            sa.Column('stop_loss_hit', sa.Boolean(), nullable=False, server_default=sa.text('false')),
            sa.Column('target_stop_collision', sa.Boolean(), nullable=False, server_default=sa.text('false')),
            sa.Column('price_path_payload', sa.JSON(), nullable=True),
            sa.Column('diagnostics_payload', sa.JSON(), nullable=True),
            sa.Column('computed_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.ForeignKeyConstraint(['feedback_id'], ['agent_context_feedback.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('feedback_id', 'horizon_days', name='uq_signal_decider_realized_outcomes_feedback_horizon'),
        )
        op.create_index(
            'ix_signal_decider_realized_outcomes_market_symbol_horizon',
            'signal_decider_realized_outcomes',
            ['market_date', 'symbol', 'horizon_days'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_realized_outcomes_feedback_id'),
            'signal_decider_realized_outcomes',
            ['feedback_id'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_realized_outcomes_market_date'),
            'signal_decider_realized_outcomes',
            ['market_date'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_realized_outcomes_symbol'),
            'signal_decider_realized_outcomes',
            ['symbol'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_realized_outcomes_policy_version'),
            'signal_decider_realized_outcomes',
            ['policy_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_realized_outcomes_decision_action'),
            'signal_decider_realized_outcomes',
            ['decision_action'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_realized_outcomes_horizon_days'),
            'signal_decider_realized_outcomes',
            ['horizon_days'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_realized_outcomes_outcome_status'),
            'signal_decider_realized_outcomes',
            ['outcome_status'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_realized_outcomes_entry_trade_date'),
            'signal_decider_realized_outcomes',
            ['entry_trade_date'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_realized_outcomes_exit_trade_date'),
            'signal_decider_realized_outcomes',
            ['exit_trade_date'],
            unique=False,
        )

    if not _table_exists(inspector, 'signal_decider_evaluations'):
        op.create_table(
            'signal_decider_evaluations',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('feedback_id', sa.Integer(), nullable=False),
            sa.Column('realized_outcome_id', sa.Integer(), nullable=True),
            sa.Column('market_date', sa.Date(), nullable=False),
            sa.Column('symbol', sa.String(length=32), nullable=False),
            sa.Column('policy_version', sa.String(length=64), nullable=False, server_default='signal_decider_policy_v1'),
            sa.Column('decision_action', sa.String(length=16), nullable=False, server_default='wait'),
            sa.Column('horizon_days', sa.Integer(), nullable=False),
            sa.Column('evaluation_version', sa.String(length=64), nullable=False, server_default='signal_decider_eval_v1'),
            sa.Column('evaluation_status', sa.String(length=32), nullable=False, server_default='pending'),
            sa.Column('grade_label', sa.String(length=64), nullable=False, server_default='pending_horizon'),
            sa.Column('grade_score', sa.Numeric(12, 6), nullable=True),
            sa.Column('missed_opportunity', sa.Boolean(), nullable=True),
            sa.Column('counterfactual_action', sa.String(length=16), nullable=True),
            sa.Column('rationale_payload', sa.JSON(), nullable=False),
            sa.Column('diagnostics_payload', sa.JSON(), nullable=True),
            sa.Column('evaluated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.ForeignKeyConstraint(['feedback_id'], ['agent_context_feedback.id'], ondelete='CASCADE'),
            sa.ForeignKeyConstraint(['realized_outcome_id'], ['signal_decider_realized_outcomes.id'], ondelete='SET NULL'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint(
                'feedback_id',
                'horizon_days',
                'evaluation_version',
                name='uq_signal_decider_evaluations_feedback_horizon_version',
            ),
        )
        op.create_index(
            'ix_signal_decider_evaluations_market_symbol_horizon_version',
            'signal_decider_evaluations',
            ['market_date', 'symbol', 'horizon_days', 'evaluation_version'],
            unique=False,
        )
        op.create_index(op.f('ix_signal_decider_evaluations_feedback_id'), 'signal_decider_evaluations', ['feedback_id'], unique=False)
        op.create_index(
            op.f('ix_signal_decider_evaluations_realized_outcome_id'),
            'signal_decider_evaluations',
            ['realized_outcome_id'],
            unique=False,
        )
        op.create_index(op.f('ix_signal_decider_evaluations_market_date'), 'signal_decider_evaluations', ['market_date'], unique=False)
        op.create_index(op.f('ix_signal_decider_evaluations_symbol'), 'signal_decider_evaluations', ['symbol'], unique=False)
        op.create_index(
            op.f('ix_signal_decider_evaluations_policy_version'),
            'signal_decider_evaluations',
            ['policy_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_evaluations_decision_action'),
            'signal_decider_evaluations',
            ['decision_action'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_evaluations_horizon_days'),
            'signal_decider_evaluations',
            ['horizon_days'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_evaluations_evaluation_version'),
            'signal_decider_evaluations',
            ['evaluation_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_evaluations_evaluation_status'),
            'signal_decider_evaluations',
            ['evaluation_status'],
            unique=False,
        )
        op.create_index(op.f('ix_signal_decider_evaluations_grade_label'), 'signal_decider_evaluations', ['grade_label'], unique=False)

    if not _table_exists(inspector, 'signal_decider_policy_candidates'):
        op.create_table(
            'signal_decider_policy_candidates',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('candidate_version', sa.String(length=128), nullable=False),
            sa.Column('base_policy_version', sa.String(length=64), nullable=False),
            sa.Column('evaluation_version', sa.String(length=64), nullable=False),
            sa.Column('market_date_from', sa.Date(), nullable=False),
            sa.Column('market_date_to', sa.Date(), nullable=False),
            sa.Column('horizon_days', sa.Integer(), nullable=False),
            sa.Column('focus_action', sa.String(length=16), nullable=True),
            sa.Column('candidate_kind', sa.String(length=64), nullable=False),
            sa.Column('feature_key', sa.String(length=128), nullable=True),
            sa.Column('feature_value', sa.String(length=128), nullable=True),
            sa.Column('affected_decisions_count', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('support_count', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('contradiction_count', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('support_ratio', sa.Numeric(12, 6), nullable=True),
            sa.Column('status', sa.String(length=32), nullable=False, server_default='generated'),
            sa.Column('activation_mode', sa.String(length=32), nullable=False, server_default='manual_only'),
            sa.Column('summary', sa.Text(), nullable=False),
            sa.Column('hypothesis', sa.Text(), nullable=False),
            sa.Column('evidence_payload', sa.JSON(), nullable=False),
            sa.Column('recommendation_payload', sa.JSON(), nullable=False),
            sa.Column('generated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('activated_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('candidate_version', name='uq_signal_decider_policy_candidates_version'),
        )
        op.create_index(
            'ix_signal_decider_policy_candidates_policy_status_generated',
            'signal_decider_policy_candidates',
            ['base_policy_version', 'status', 'generated_at'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_candidate_version'),
            'signal_decider_policy_candidates',
            ['candidate_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_base_policy_version'),
            'signal_decider_policy_candidates',
            ['base_policy_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_evaluation_version'),
            'signal_decider_policy_candidates',
            ['evaluation_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_market_date_from'),
            'signal_decider_policy_candidates',
            ['market_date_from'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_market_date_to'),
            'signal_decider_policy_candidates',
            ['market_date_to'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_horizon_days'),
            'signal_decider_policy_candidates',
            ['horizon_days'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_focus_action'),
            'signal_decider_policy_candidates',
            ['focus_action'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_candidate_kind'),
            'signal_decider_policy_candidates',
            ['candidate_kind'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_feature_key'),
            'signal_decider_policy_candidates',
            ['feature_key'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_feature_value'),
            'signal_decider_policy_candidates',
            ['feature_value'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_status'),
            'signal_decider_policy_candidates',
            ['status'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_generated_at'),
            'signal_decider_policy_candidates',
            ['generated_at'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_candidates_activated_at'),
            'signal_decider_policy_candidates',
            ['activated_at'],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'signal_decider_policy_candidates'):
        op.drop_index(op.f('ix_signal_decider_policy_candidates_activated_at'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_generated_at'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_status'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_feature_value'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_feature_key'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_candidate_kind'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_focus_action'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_horizon_days'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_market_date_to'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_market_date_from'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_evaluation_version'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_base_policy_version'), table_name='signal_decider_policy_candidates')
        op.drop_index(op.f('ix_signal_decider_policy_candidates_candidate_version'), table_name='signal_decider_policy_candidates')
        op.drop_index('ix_signal_decider_policy_candidates_policy_status_generated', table_name='signal_decider_policy_candidates')
        op.drop_table('signal_decider_policy_candidates')

    if _table_exists(inspector, 'signal_decider_evaluations'):
        op.drop_index(op.f('ix_signal_decider_evaluations_grade_label'), table_name='signal_decider_evaluations')
        op.drop_index(op.f('ix_signal_decider_evaluations_evaluation_status'), table_name='signal_decider_evaluations')
        op.drop_index(op.f('ix_signal_decider_evaluations_evaluation_version'), table_name='signal_decider_evaluations')
        op.drop_index(op.f('ix_signal_decider_evaluations_horizon_days'), table_name='signal_decider_evaluations')
        op.drop_index(op.f('ix_signal_decider_evaluations_decision_action'), table_name='signal_decider_evaluations')
        op.drop_index(op.f('ix_signal_decider_evaluations_policy_version'), table_name='signal_decider_evaluations')
        op.drop_index(op.f('ix_signal_decider_evaluations_symbol'), table_name='signal_decider_evaluations')
        op.drop_index(op.f('ix_signal_decider_evaluations_market_date'), table_name='signal_decider_evaluations')
        op.drop_index(op.f('ix_signal_decider_evaluations_realized_outcome_id'), table_name='signal_decider_evaluations')
        op.drop_index(op.f('ix_signal_decider_evaluations_feedback_id'), table_name='signal_decider_evaluations')
        op.drop_index('ix_signal_decider_evaluations_market_symbol_horizon_version', table_name='signal_decider_evaluations')
        op.drop_table('signal_decider_evaluations')

    if _table_exists(inspector, 'signal_decider_realized_outcomes'):
        op.drop_index(op.f('ix_signal_decider_realized_outcomes_exit_trade_date'), table_name='signal_decider_realized_outcomes')
        op.drop_index(op.f('ix_signal_decider_realized_outcomes_entry_trade_date'), table_name='signal_decider_realized_outcomes')
        op.drop_index(op.f('ix_signal_decider_realized_outcomes_outcome_status'), table_name='signal_decider_realized_outcomes')
        op.drop_index(op.f('ix_signal_decider_realized_outcomes_horizon_days'), table_name='signal_decider_realized_outcomes')
        op.drop_index(op.f('ix_signal_decider_realized_outcomes_decision_action'), table_name='signal_decider_realized_outcomes')
        op.drop_index(op.f('ix_signal_decider_realized_outcomes_policy_version'), table_name='signal_decider_realized_outcomes')
        op.drop_index(op.f('ix_signal_decider_realized_outcomes_symbol'), table_name='signal_decider_realized_outcomes')
        op.drop_index(op.f('ix_signal_decider_realized_outcomes_market_date'), table_name='signal_decider_realized_outcomes')
        op.drop_index(op.f('ix_signal_decider_realized_outcomes_feedback_id'), table_name='signal_decider_realized_outcomes')
        op.drop_index('ix_signal_decider_realized_outcomes_market_symbol_horizon', table_name='signal_decider_realized_outcomes')
        op.drop_table('signal_decider_realized_outcomes')
