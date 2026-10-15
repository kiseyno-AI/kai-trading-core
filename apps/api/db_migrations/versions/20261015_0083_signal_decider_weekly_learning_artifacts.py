"""persist versioned signal-decider weekly learning artifacts

Revision ID: 20261015_0083
Revises: 20260926_0082
Create Date: 2026-10-15 12:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20261015_0083'
down_revision = '20260926_0082'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'signal_decider_weekly_learning_artifacts',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('artifact_version', sa.String(length=128), nullable=False),
        sa.Column('schema_version', sa.String(length=64), nullable=False),
        sa.Column('week_start', sa.Date(), nullable=False),
        sa.Column('week_end', sa.Date(), nullable=False),
        sa.Column('valid_through', sa.Date(), nullable=False),
        sa.Column('horizon_days', sa.Integer(), nullable=False),
        sa.Column('evaluation_version', sa.String(length=64), nullable=False),
        sa.Column('min_sample_size', sa.Integer(), nullable=False),
        sa.Column('source_fingerprint', sa.String(length=64), nullable=False),
        sa.Column('content_sha256', sa.String(length=64), nullable=False),
        sa.Column('publication_status', sa.String(length=16), nullable=False),
        sa.Column('eligibility_payload', sa.JSON(), nullable=False),
        sa.Column('learning_context_payload', sa.JSON(), nullable=False),
        sa.Column('evidence_manifest_payload', sa.JSON(), nullable=False),
        sa.Column('source_report_payload', sa.JSON(), nullable=False),
        sa.Column(
            'generated_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "publication_status IN ('published', 'ineligible')",
            name='ck_signal_decider_weekly_learning_publication_status',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'artifact_version',
            name='uq_signal_decider_weekly_learning_artifact_version',
        ),
        sa.UniqueConstraint(
            'source_fingerprint',
            name='uq_signal_decider_weekly_learning_source_fingerprint',
        ),
    )
    op.create_index(
        'ix_signal_decider_weekly_learning_latest_published',
        'signal_decider_weekly_learning_artifacts',
        ['publication_status', sa.text('published_at DESC'), sa.text('id DESC')],
        unique=False,
    )
    op.create_index(
        'ix_signal_decider_weekly_learning_week',
        'signal_decider_weekly_learning_artifacts',
        ['week_end', 'week_start'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        'ix_signal_decider_weekly_learning_week',
        table_name='signal_decider_weekly_learning_artifacts',
    )
    op.drop_index(
        'ix_signal_decider_weekly_learning_latest_published',
        table_name='signal_decider_weekly_learning_artifacts',
    )
    op.drop_table('signal_decider_weekly_learning_artifacts')
