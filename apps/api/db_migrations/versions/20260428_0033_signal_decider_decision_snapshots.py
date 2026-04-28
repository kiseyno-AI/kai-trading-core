"""add durable signal decider decision snapshots

Revision ID: 20260428_0033
Revises: 20260428_0032
Create Date: 2026-04-28 22:10:00
"""

from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa


revision = '20260428_0033'
down_revision = '20260428_0032'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {column['name'] for column in inspector.get_columns(table_name)}


def _index_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {index['name'] for index in inspector.get_indexes(table_name)}


def _unique_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {constraint['name'] for constraint in inspector.get_unique_constraints(table_name)}


def _normalize_optional_text(value):
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_action(value) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {'buy', 'wait', 'reject'}:
            return normalized
    return 'wait'


def _normalize_text_list(value):
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    if not isinstance(value, list):
        return []
    items = []
    seen = set()
    for item in value:
        if item is None:
            continue
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
    return items


def _policy_version(summary_payload, response_payload, request_payload) -> str:
    request_payload = request_payload if isinstance(request_payload, dict) else {}
    response_payload = response_payload if isinstance(response_payload, dict) else {}
    summary_payload = summary_payload if isinstance(summary_payload, dict) else {}
    policy_payload = request_payload.get('signal_decider_policy')
    policy_payload = policy_payload if isinstance(policy_payload, dict) else {}
    trace_payload = request_payload.get('signal_decider_trace')
    trace_payload = trace_payload if isinstance(trace_payload, dict) else {}
    return (
        _normalize_optional_text(response_payload.get('policy_version'))
        or _normalize_optional_text(summary_payload.get('policy_version'))
        or _normalize_optional_text(policy_payload.get('current_version'))
        or _normalize_optional_text(trace_payload.get('policy_version'))
        or 'signal_decider_policy_v1'
    )


def _feature_snapshot(request_payload):
    request_payload = request_payload if isinstance(request_payload, dict) else {}
    trace_payload = request_payload.get('signal_decider_trace')
    trace_payload = trace_payload if isinstance(trace_payload, dict) else {}
    feature_snapshot = trace_payload.get('feature_snapshot')
    return feature_snapshot if isinstance(feature_snapshot, dict) else {}


def _traceability_payload(request_payload, response_payload):
    request_payload = request_payload if isinstance(request_payload, dict) else {}
    response_payload = response_payload if isinstance(response_payload, dict) else {}
    trace_payload = request_payload.get('signal_decider_trace')
    trace_payload = trace_payload if isinstance(trace_payload, dict) else {}
    traceability = trace_payload.get('traceability')
    if isinstance(traceability, dict):
        return traceability
    traceability = response_payload.get('traceability')
    if isinstance(traceability, dict):
        return traceability
    return {}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'signal_decider_decision_snapshots'):
        op.create_table(
            'signal_decider_decision_snapshots',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('feedback_id', sa.Integer(), nullable=True),
            sa.Column('market_date', sa.Date(), nullable=False),
            sa.Column('symbol', sa.String(length=32), nullable=False),
            sa.Column('agent_name', sa.String(length=64), nullable=False, server_default='signal-decider'),
            sa.Column('agent_version', sa.String(length=64), nullable=True),
            sa.Column('decision_run_id', sa.String(length=128), nullable=True),
            sa.Column('feedback_received_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('policy_version', sa.String(length=64), nullable=False, server_default='signal_decider_policy_v1'),
            sa.Column('decision_action', sa.String(length=16), nullable=False, server_default='wait'),
            sa.Column('preferred_action', sa.String(length=16), nullable=False, server_default='wait'),
            sa.Column('next_day_open_action', sa.String(length=16), nullable=False, server_default='wait'),
            sa.Column('entry_price_basis', sa.String(length=32), nullable=False, server_default='next_open'),
            sa.Column('target_price', sa.BigInteger(), nullable=True),
            sa.Column('expected_return_pct', sa.Numeric(12, 6), nullable=True),
            sa.Column('stop_loss_price', sa.BigInteger(), nullable=True),
            sa.Column('suggested_holding_period', sa.String(length=64), nullable=True),
            sa.Column('confidence_score_10', sa.Numeric(12, 6), nullable=True),
            sa.Column('conviction_score_10', sa.Numeric(12, 6), nullable=True),
            sa.Column('thesis_summary', sa.Text(), nullable=False),
            sa.Column('supporting_evidence_payload', sa.JSON(), nullable=False),
            sa.Column('decision_risks_payload', sa.JSON(), nullable=False),
            sa.Column('invalidation_conditions_payload', sa.JSON(), nullable=False),
            sa.Column('data_quality_warnings_payload', sa.JSON(), nullable=False),
            sa.Column('specialist_disagreement_payload', sa.JSON(), nullable=False),
            sa.Column('traceability_payload', sa.JSON(), nullable=False),
            sa.Column('input_feature_snapshot_payload', sa.JSON(), nullable=False),
            sa.Column('request_payload', sa.JSON(), nullable=False),
            sa.Column('response_payload', sa.JSON(), nullable=False),
            sa.Column('response_summary_payload', sa.JSON(), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.ForeignKeyConstraint(['feedback_id'], ['agent_context_feedback.id'], ondelete='SET NULL'),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index(
            'ix_signal_decider_decision_snapshots_market_symbol_created',
            'signal_decider_decision_snapshots',
            ['market_date', 'symbol', 'created_at'],
            unique=False,
        )
        op.create_index(
            'ix_signal_decider_decision_snapshots_policy_action_created',
            'signal_decider_decision_snapshots',
            ['policy_version', 'decision_action', 'created_at'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_decision_snapshots_feedback_id'),
            'signal_decider_decision_snapshots',
            ['feedback_id'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_decision_snapshots_market_date'),
            'signal_decider_decision_snapshots',
            ['market_date'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_decision_snapshots_symbol'),
            'signal_decider_decision_snapshots',
            ['symbol'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_decision_snapshots_agent_name'),
            'signal_decider_decision_snapshots',
            ['agent_name'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_decision_snapshots_decision_run_id'),
            'signal_decider_decision_snapshots',
            ['decision_run_id'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_decision_snapshots_feedback_received_at'),
            'signal_decider_decision_snapshots',
            ['feedback_received_at'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_decision_snapshots_policy_version'),
            'signal_decider_decision_snapshots',
            ['policy_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_decision_snapshots_decision_action'),
            'signal_decider_decision_snapshots',
            ['decision_action'],
            unique=False,
        )

    inspector = sa.inspect(bind)
    metadata = sa.MetaData()
    feedback_table = sa.Table('agent_context_feedback', metadata, autoload_with=bind)
    snapshot_table = sa.Table('signal_decider_decision_snapshots', metadata, autoload_with=bind)

    existing_feedback_ids = {
        row[0]
        for row in bind.execute(
            sa.select(snapshot_table.c.feedback_id).where(snapshot_table.c.feedback_id.is_not(None))
        ).all()
    }
    feedback_rows = bind.execute(
        sa.select(
            feedback_table.c.id,
            feedback_table.c.market_date,
            feedback_table.c.symbol,
            feedback_table.c.agent_name,
            feedback_table.c.agent_version,
            feedback_table.c.run_id,
            feedback_table.c.summary_payload,
            feedback_table.c.feedback_payload,
            feedback_table.c.normalized_feedback_payload,
            feedback_table.c.context_payload,
            feedback_table.c.received_at,
        ).where(feedback_table.c.agent_name == 'signal-decider')
    ).mappings().all()
    snapshot_inserts = []
    for row in feedback_rows:
        if row['id'] in existing_feedback_ids:
            continue
        summary_payload = row['summary_payload'] if isinstance(row['summary_payload'], dict) else {}
        response_payload = (
            row['normalized_feedback_payload']
            if isinstance(row['normalized_feedback_payload'], dict)
            else row['feedback_payload']
            if isinstance(row['feedback_payload'], dict)
            else {}
        )
        request_payload = row['context_payload'] if isinstance(row['context_payload'], dict) else {}
        thesis_summary = str(
            response_payload.get('thesis_summary')
            or response_payload.get('summary')
            or summary_payload.get('summary')
            or ''
        ).strip()
        decision_action = _normalize_action(
            response_payload.get('next_day_open_action') or response_payload.get('preferred_action')
        )
        received_at = row['received_at'] or datetime.now(timezone.utc)
        snapshot_inserts.append(
            {
                'feedback_id': row['id'],
                'market_date': row['market_date'],
                'symbol': row['symbol'],
                'agent_name': row['agent_name'],
                'agent_version': row['agent_version'],
                'decision_run_id': row['run_id'],
                'feedback_received_at': received_at,
                'policy_version': _policy_version(summary_payload, response_payload, request_payload),
                'decision_action': decision_action,
                'preferred_action': _normalize_action(response_payload.get('preferred_action')),
                'next_day_open_action': _normalize_action(response_payload.get('next_day_open_action')),
                'entry_price_basis': _normalize_optional_text(response_payload.get('entry_price_basis')) or 'next_open',
                'target_price': response_payload.get('target_price'),
                'expected_return_pct': response_payload.get('expected_return_pct'),
                'stop_loss_price': response_payload.get('stop_loss_price'),
                'suggested_holding_period': _normalize_optional_text(response_payload.get('suggested_holding_period')),
                'confidence_score_10': response_payload.get('confidence_score_10'),
                'conviction_score_10': response_payload.get('conviction_score_10'),
                'thesis_summary': thesis_summary,
                'supporting_evidence_payload': _normalize_text_list(response_payload.get('supporting_evidence')),
                'decision_risks_payload': _normalize_text_list(response_payload.get('decision_risks')),
                'invalidation_conditions_payload': _normalize_text_list(response_payload.get('invalidation_conditions')),
                'data_quality_warnings_payload': _normalize_text_list(response_payload.get('data_quality_warnings')),
                'specialist_disagreement_payload': _normalize_text_list(response_payload.get('specialist_disagreement')),
                'traceability_payload': _traceability_payload(request_payload, response_payload),
                'input_feature_snapshot_payload': _feature_snapshot(request_payload),
                'request_payload': request_payload,
                'response_payload': response_payload,
                'response_summary_payload': summary_payload,
                'created_at': received_at,
                'updated_at': received_at,
            }
        )
    if snapshot_inserts:
        op.bulk_insert(snapshot_table, snapshot_inserts)

    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'signal_decider_realized_outcomes'):
        columns = _column_names(inspector, 'signal_decider_realized_outcomes')
        if 'decision_snapshot_id' not in columns:
            op.add_column(
                'signal_decider_realized_outcomes',
                sa.Column('decision_snapshot_id', sa.Integer(), nullable=True),
            )
            op.create_foreign_key(
                'fk_signal_decider_realized_outcomes_snapshot_id',
                'signal_decider_realized_outcomes',
                'signal_decider_decision_snapshots',
                ['decision_snapshot_id'],
                ['id'],
                ondelete='CASCADE',
            )
        inspector = sa.inspect(bind)
        indexes = _index_names(inspector, 'signal_decider_realized_outcomes')
        if op.f('ix_signal_decider_realized_outcomes_decision_snapshot_id') not in indexes:
            op.create_index(
                op.f('ix_signal_decider_realized_outcomes_decision_snapshot_id'),
                'signal_decider_realized_outcomes',
                ['decision_snapshot_id'],
                unique=False,
            )

    if _table_exists(inspector, 'signal_decider_evaluations'):
        columns = _column_names(inspector, 'signal_decider_evaluations')
        if 'decision_snapshot_id' not in columns:
            op.add_column(
                'signal_decider_evaluations',
                sa.Column('decision_snapshot_id', sa.Integer(), nullable=True),
            )
            op.create_foreign_key(
                'fk_signal_decider_evaluations_snapshot_id',
                'signal_decider_evaluations',
                'signal_decider_decision_snapshots',
                ['decision_snapshot_id'],
                ['id'],
                ondelete='CASCADE',
            )
        inspector = sa.inspect(bind)
        indexes = _index_names(inspector, 'signal_decider_evaluations')
        if op.f('ix_signal_decider_evaluations_decision_snapshot_id') not in indexes:
            op.create_index(
                op.f('ix_signal_decider_evaluations_decision_snapshot_id'),
                'signal_decider_evaluations',
                ['decision_snapshot_id'],
                unique=False,
            )

    metadata = sa.MetaData()
    snapshot_table = sa.Table('signal_decider_decision_snapshots', metadata, autoload_with=bind)
    snapshot_rows = bind.execute(
        sa.select(snapshot_table.c.id, snapshot_table.c.feedback_id)
        .where(snapshot_table.c.feedback_id.is_not(None))
        .order_by(snapshot_table.c.id.asc())
    ).all()
    snapshot_id_by_feedback = {}
    for snapshot_id, feedback_id in snapshot_rows:
        if feedback_id is None:
            continue
        snapshot_id_by_feedback.setdefault(feedback_id, snapshot_id)

    if _table_exists(inspector, 'signal_decider_realized_outcomes'):
        outcomes_table = sa.Table('signal_decider_realized_outcomes', sa.MetaData(), autoload_with=bind)
        for feedback_id, snapshot_id in snapshot_id_by_feedback.items():
            bind.execute(
                sa.update(outcomes_table)
                .where(outcomes_table.c.feedback_id == feedback_id)
                .where(outcomes_table.c.decision_snapshot_id.is_(None))
                .values(decision_snapshot_id=snapshot_id)
            )
        unresolved = bind.execute(
            sa.select(sa.func.count())
            .select_from(outcomes_table)
            .where(outcomes_table.c.decision_snapshot_id.is_(None))
        ).scalar_one()
        if unresolved == 0:
            op.alter_column('signal_decider_realized_outcomes', 'decision_snapshot_id', nullable=False)
        inspector = sa.inspect(bind)
        unique_names = _unique_names(inspector, 'signal_decider_realized_outcomes')
        if 'uq_signal_decider_realized_outcomes_feedback_horizon' in unique_names:
            op.drop_constraint(
                'uq_signal_decider_realized_outcomes_feedback_horizon',
                'signal_decider_realized_outcomes',
                type_='unique',
            )
        inspector = sa.inspect(bind)
        unique_names = _unique_names(inspector, 'signal_decider_realized_outcomes')
        if 'uq_signal_decider_realized_outcomes_snapshot_horizon' not in unique_names:
            op.create_unique_constraint(
                'uq_signal_decider_realized_outcomes_snapshot_horizon',
                'signal_decider_realized_outcomes',
                ['decision_snapshot_id', 'horizon_days'],
            )

    if _table_exists(inspector, 'signal_decider_evaluations'):
        evaluations_table = sa.Table('signal_decider_evaluations', sa.MetaData(), autoload_with=bind)
        for feedback_id, snapshot_id in snapshot_id_by_feedback.items():
            bind.execute(
                sa.update(evaluations_table)
                .where(evaluations_table.c.feedback_id == feedback_id)
                .where(evaluations_table.c.decision_snapshot_id.is_(None))
                .values(decision_snapshot_id=snapshot_id)
            )
        unresolved = bind.execute(
            sa.select(sa.func.count())
            .select_from(evaluations_table)
            .where(evaluations_table.c.decision_snapshot_id.is_(None))
        ).scalar_one()
        if unresolved == 0:
            op.alter_column('signal_decider_evaluations', 'decision_snapshot_id', nullable=False)
        inspector = sa.inspect(bind)
        unique_names = _unique_names(inspector, 'signal_decider_evaluations')
        if 'uq_signal_decider_evaluations_feedback_horizon_version' in unique_names:
            op.drop_constraint(
                'uq_signal_decider_evaluations_feedback_horizon_version',
                'signal_decider_evaluations',
                type_='unique',
            )
        inspector = sa.inspect(bind)
        unique_names = _unique_names(inspector, 'signal_decider_evaluations')
        if 'uq_signal_decider_evaluations_snapshot_horizon_version' not in unique_names:
            op.create_unique_constraint(
                'uq_signal_decider_evaluations_snapshot_horizon_version',
                'signal_decider_evaluations',
                ['decision_snapshot_id', 'horizon_days', 'evaluation_version'],
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'signal_decider_evaluations'):
        unique_names = _unique_names(inspector, 'signal_decider_evaluations')
        if 'uq_signal_decider_evaluations_snapshot_horizon_version' in unique_names:
            op.drop_constraint(
                'uq_signal_decider_evaluations_snapshot_horizon_version',
                'signal_decider_evaluations',
                type_='unique',
            )
        inspector = sa.inspect(bind)
        unique_names = _unique_names(inspector, 'signal_decider_evaluations')
        if 'uq_signal_decider_evaluations_feedback_horizon_version' not in unique_names:
            op.create_unique_constraint(
                'uq_signal_decider_evaluations_feedback_horizon_version',
                'signal_decider_evaluations',
                ['feedback_id', 'horizon_days', 'evaluation_version'],
            )
        indexes = _index_names(inspector, 'signal_decider_evaluations')
        if op.f('ix_signal_decider_evaluations_decision_snapshot_id') in indexes:
            op.drop_index(
                op.f('ix_signal_decider_evaluations_decision_snapshot_id'),
                table_name='signal_decider_evaluations',
            )
        columns = _column_names(inspector, 'signal_decider_evaluations')
        if 'decision_snapshot_id' in columns:
            op.drop_constraint(
                'fk_signal_decider_evaluations_snapshot_id',
                'signal_decider_evaluations',
                type_='foreignkey',
            )
            op.drop_column('signal_decider_evaluations', 'decision_snapshot_id')

    inspector = sa.inspect(bind)
    if _table_exists(inspector, 'signal_decider_realized_outcomes'):
        unique_names = _unique_names(inspector, 'signal_decider_realized_outcomes')
        if 'uq_signal_decider_realized_outcomes_snapshot_horizon' in unique_names:
            op.drop_constraint(
                'uq_signal_decider_realized_outcomes_snapshot_horizon',
                'signal_decider_realized_outcomes',
                type_='unique',
            )
        inspector = sa.inspect(bind)
        unique_names = _unique_names(inspector, 'signal_decider_realized_outcomes')
        if 'uq_signal_decider_realized_outcomes_feedback_horizon' not in unique_names:
            op.create_unique_constraint(
                'uq_signal_decider_realized_outcomes_feedback_horizon',
                'signal_decider_realized_outcomes',
                ['feedback_id', 'horizon_days'],
            )
        indexes = _index_names(inspector, 'signal_decider_realized_outcomes')
        if op.f('ix_signal_decider_realized_outcomes_decision_snapshot_id') in indexes:
            op.drop_index(
                op.f('ix_signal_decider_realized_outcomes_decision_snapshot_id'),
                table_name='signal_decider_realized_outcomes',
            )
        columns = _column_names(inspector, 'signal_decider_realized_outcomes')
        if 'decision_snapshot_id' in columns:
            op.drop_constraint(
                'fk_signal_decider_realized_outcomes_snapshot_id',
                'signal_decider_realized_outcomes',
                type_='foreignkey',
            )
            op.drop_column('signal_decider_realized_outcomes', 'decision_snapshot_id')

    inspector = sa.inspect(bind)
    if _table_exists(inspector, 'signal_decider_decision_snapshots'):
        indexes = _index_names(inspector, 'signal_decider_decision_snapshots')
        for name in [
            op.f('ix_signal_decider_decision_snapshots_decision_action'),
            op.f('ix_signal_decider_decision_snapshots_policy_version'),
            op.f('ix_signal_decider_decision_snapshots_feedback_received_at'),
            op.f('ix_signal_decider_decision_snapshots_decision_run_id'),
            op.f('ix_signal_decider_decision_snapshots_agent_name'),
            op.f('ix_signal_decider_decision_snapshots_symbol'),
            op.f('ix_signal_decider_decision_snapshots_market_date'),
            op.f('ix_signal_decider_decision_snapshots_feedback_id'),
            'ix_signal_decider_decision_snapshots_policy_action_created',
            'ix_signal_decider_decision_snapshots_market_symbol_created',
        ]:
            if name in indexes:
                op.drop_index(name, table_name='signal_decider_decision_snapshots')
        op.drop_table('signal_decider_decision_snapshots')
