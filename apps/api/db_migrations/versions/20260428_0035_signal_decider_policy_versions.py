"""add signal decider policy version registry

Revision ID: 20260428_0035
Revises: 20260428_0034
Create Date: 2026-04-28 23:40:00
"""

from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa


revision = '20260428_0035'
down_revision = '20260428_0034'
branch_labels = None
depends_on = None

DEFAULT_POLICY_VERSION = 'signal_decider_policy_v1'
MANUAL_ACTIVATION_MODE = 'manual_only'


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _baseline_rule_bundle(policy_version: str) -> dict:
    return {
        'schema_version': 'signal_decider_policy_bundle_v1',
        'policy_version': policy_version,
        'review_mode': MANUAL_ACTIVATION_MODE,
        'human_activation_required': True,
        'policy_constraints': {
            'decision_support_only': True,
            'allow_execution_logic': False,
            'allow_position_sizing': False,
            'allow_leverage_guidance': False,
            'avoid_fake_precision': True,
        },
        'change_set': [],
    }


def _candidate_rule_bundle(row: sa.RowMapping) -> dict:
    return {
        'schema_version': 'signal_decider_policy_bundle_v1',
        'policy_version': row['candidate_version'],
        'base_policy_version': row['base_policy_version'],
        'evaluation_version': row['evaluation_version'],
        'review_mode': MANUAL_ACTIVATION_MODE,
        'human_activation_required': True,
        'market_date_window': {
            'from': row['market_date_from'].isoformat(),
            'to': row['market_date_to'].isoformat(),
        },
        'primary_horizon_days': row['horizon_days'],
        'focus_action': row['focus_action'],
        'candidate_kind': row['candidate_kind'],
        'proposed_changes': [
            {
                'change_type': row['candidate_kind'],
                'feature_gate': {
                    'key': row['feature_key'],
                    'value': row['feature_value'],
                },
                'support_ratio': float(row['support_ratio']) if row['support_ratio'] is not None else None,
                'support_count': row['support_count'],
                'contradiction_count': row['contradiction_count'],
                'review_note': 'Candidate generation is automatic; activation remains manual-only.',
            }
        ],
    }


def _policy_metadata(
    *,
    origin: str,
    evidence: dict | None = None,
    recommendation: dict | None = None,
    notes: list[str] | None = None,
) -> dict:
    payload = {
        'schema_version': 'signal_decider_policy_metadata_v1',
        'policy_origin': origin,
        'activation_mode': MANUAL_ACTIVATION_MODE,
        'candidate_activation_required': True,
    }
    if evidence:
        payload['evidence'] = evidence
    if recommendation:
        payload['recommendation'] = recommendation
    if notes:
        payload['notes'] = notes
    return payload


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'signal_decider_policy_versions'):
        op.create_table(
            'signal_decider_policy_versions',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('policy_version', sa.String(length=128), nullable=False),
            sa.Column('status', sa.String(length=32), nullable=False, server_default='draft'),
            sa.Column('parent_policy_version', sa.String(length=128), nullable=True),
            sa.Column('source_candidate_version', sa.String(length=128), nullable=True),
            sa.Column('change_summary', sa.Text(), nullable=False),
            sa.Column('rule_bundle_payload', sa.JSON(), nullable=False),
            sa.Column('metadata_payload', sa.JSON(), nullable=False),
            sa.Column('created_by', sa.String(length=128), nullable=False, server_default='system:bootstrap'),
            sa.Column('activated_by', sa.String(length=128), nullable=True),
            sa.Column('activated_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('retired_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('policy_version', name='uq_signal_decider_policy_versions_version'),
        )
        op.create_index(
            'ix_signal_decider_policy_versions_status_created',
            'signal_decider_policy_versions',
            ['status', 'created_at'],
            unique=False,
        )
        op.create_index(
            'ix_signal_decider_policy_versions_parent_status',
            'signal_decider_policy_versions',
            ['parent_policy_version', 'status'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_versions_policy_version'),
            'signal_decider_policy_versions',
            ['policy_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_versions_status'),
            'signal_decider_policy_versions',
            ['status'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_versions_parent_policy_version'),
            'signal_decider_policy_versions',
            ['parent_policy_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_versions_source_candidate_version'),
            'signal_decider_policy_versions',
            ['source_candidate_version'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_versions_activated_by'),
            'signal_decider_policy_versions',
            ['activated_by'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_versions_activated_at'),
            'signal_decider_policy_versions',
            ['activated_at'],
            unique=False,
        )
        op.create_index(
            op.f('ix_signal_decider_policy_versions_retired_at'),
            'signal_decider_policy_versions',
            ['retired_at'],
            unique=False,
        )

    policy_versions_table = sa.Table('signal_decider_policy_versions', sa.MetaData(), autoload_with=bind)
    existing_versions = {
        row['policy_version']
        for row in bind.execute(sa.select(policy_versions_table.c.policy_version)).mappings()
    }
    active_exists = bool(
        bind.execute(
            sa.select(policy_versions_table.c.id).where(policy_versions_table.c.status == 'active').limit(1)
        ).scalar_one_or_none()
    )
    rows_to_insert: list[dict] = []
    now = datetime.now(timezone.utc)

    if _table_exists(inspector, 'signal_decider_policy_candidates'):
        candidates_table = sa.Table('signal_decider_policy_candidates', sa.MetaData(), autoload_with=bind)
        candidate_rows = bind.execute(
            sa.select(
                candidates_table.c.candidate_version,
                candidates_table.c.base_policy_version,
                candidates_table.c.evaluation_version,
                candidates_table.c.market_date_from,
                candidates_table.c.market_date_to,
                candidates_table.c.horizon_days,
                candidates_table.c.focus_action,
                candidates_table.c.candidate_kind,
                candidates_table.c.feature_key,
                candidates_table.c.feature_value,
                candidates_table.c.affected_decisions_count,
                candidates_table.c.support_count,
                candidates_table.c.contradiction_count,
                candidates_table.c.support_ratio,
                candidates_table.c.status,
                candidates_table.c.summary,
                candidates_table.c.evidence_payload,
                candidates_table.c.recommendation_payload,
                candidates_table.c.generated_at,
                candidates_table.c.activated_at,
            )
        ).mappings()
        for row in candidate_rows:
            policy_version = row['candidate_version']
            if policy_version in existing_versions:
                continue
            status = 'active' if row['status'] == 'activated' else 'candidate'
            rows_to_insert.append(
                {
                    'policy_version': policy_version,
                    'status': status,
                    'parent_policy_version': row['base_policy_version'],
                    'source_candidate_version': row['candidate_version'],
                    'change_summary': row['summary'],
                    'rule_bundle_payload': _candidate_rule_bundle(row),
                    'metadata_payload': _policy_metadata(
                        origin='candidate_generation_backfill',
                        evidence=row['evidence_payload'],
                        recommendation=row['recommendation_payload'],
                    ),
                    'created_by': 'system:policy_registry_backfill',
                    'activated_by': 'system:policy_registry_backfill' if status == 'active' else None,
                    'activated_at': row['activated_at'] if status == 'active' else None,
                    'retired_at': None,
                    'created_at': row['generated_at'] or now,
                    'updated_at': row['generated_at'] or now,
                }
            )
            existing_versions.add(policy_version)

    if _table_exists(inspector, 'signal_decider_decision_snapshots'):
        snapshots_table = sa.Table('signal_decider_decision_snapshots', sa.MetaData(), autoload_with=bind)
        snapshot_versions = bind.execute(
            sa.select(sa.distinct(snapshots_table.c.policy_version)).where(
                snapshots_table.c.policy_version.is_not(None)
            )
        ).scalars()
        for policy_version in snapshot_versions:
            if not policy_version or policy_version in existing_versions:
                continue
            status = 'active' if policy_version == DEFAULT_POLICY_VERSION and not active_exists else 'draft'
            rows_to_insert.append(
                {
                    'policy_version': policy_version,
                    'status': status,
                    'parent_policy_version': None,
                    'source_candidate_version': None,
                    'change_summary': 'Observed legacy signal-decider policy version registered from persisted history.',
                    'rule_bundle_payload': _baseline_rule_bundle(policy_version),
                    'metadata_payload': _policy_metadata(
                        origin='observed_snapshot_backfill',
                        notes=['Observed legacy signal-decider policy version registered for queryability.'],
                    ),
                    'created_by': 'system:policy_registry_backfill',
                    'activated_by': 'system:policy_registry_backfill' if status == 'active' else None,
                    'activated_at': now if status == 'active' else None,
                    'retired_at': None,
                    'created_at': now,
                    'updated_at': now,
                }
            )
            existing_versions.add(policy_version)
            active_exists = active_exists or status == 'active'

    if DEFAULT_POLICY_VERSION not in existing_versions:
        rows_to_insert.append(
            {
                'policy_version': DEFAULT_POLICY_VERSION,
                'status': 'active' if not active_exists else 'retired',
                'parent_policy_version': None,
                'source_candidate_version': None,
                'change_summary': 'Baseline manual-only signal-decider policy.',
                'rule_bundle_payload': _baseline_rule_bundle(DEFAULT_POLICY_VERSION),
                'metadata_payload': _policy_metadata(
                    origin='bootstrap',
                    notes=['Baseline signal-decider policy registry entry.'],
                ),
                'created_by': 'system:bootstrap',
                'activated_by': 'system:bootstrap' if not active_exists else None,
                'activated_at': now if not active_exists else None,
                'retired_at': now if active_exists else None,
                'created_at': now,
                'updated_at': now,
            }
        )

    if rows_to_insert:
        bind.execute(policy_versions_table.insert(), rows_to_insert)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'signal_decider_policy_versions'):
        op.drop_index(op.f('ix_signal_decider_policy_versions_retired_at'), table_name='signal_decider_policy_versions')
        op.drop_index(op.f('ix_signal_decider_policy_versions_activated_at'), table_name='signal_decider_policy_versions')
        op.drop_index(op.f('ix_signal_decider_policy_versions_activated_by'), table_name='signal_decider_policy_versions')
        op.drop_index(op.f('ix_signal_decider_policy_versions_source_candidate_version'), table_name='signal_decider_policy_versions')
        op.drop_index(op.f('ix_signal_decider_policy_versions_parent_policy_version'), table_name='signal_decider_policy_versions')
        op.drop_index(op.f('ix_signal_decider_policy_versions_status'), table_name='signal_decider_policy_versions')
        op.drop_index(op.f('ix_signal_decider_policy_versions_policy_version'), table_name='signal_decider_policy_versions')
        op.drop_index('ix_signal_decider_policy_versions_parent_status', table_name='signal_decider_policy_versions')
        op.drop_index('ix_signal_decider_policy_versions_status_created', table_name='signal_decider_policy_versions')
        op.drop_table('signal_decider_policy_versions')
