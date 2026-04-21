"""add full response columns to agent context feedback

Revision ID: 20260421_0016
Revises: 20260421_0015
Create Date: 2026-04-21 23:25:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260421_0016'
down_revision = '20260421_0015'
branch_labels = None
depends_on = None


def _column_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {column['name'] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = _column_names(inspector, 'agent_context_feedback')
    additions = [
        ('normalized_feedback_payload', sa.JSON(), True),
        ('details_payload', sa.JSON(), True),
        ('breadth_context_payload', sa.JSON(), True),
        ('leaders_laggards_payload', sa.JSON(), True),
        ('price_action_payload', sa.JSON(), True),
        ('trend_context_payload', sa.JSON(), True),
        ('support_resistance_payload', sa.JSON(), True),
        ('anomalies_and_gaps_payload', sa.JSON(), True),
        ('data_gaps_payload', sa.JSON(), True),
        ('fundamental_backdrop_payload', sa.JSON(), True),
        ('conservative_take_payload', sa.JSON(), True),
        ('improvement_notes_payload', sa.JSON(), True),
    ]
    for name, type_, nullable in additions:
        if name not in columns:
            op.add_column('agent_context_feedback', sa.Column(name, type_, nullable=nullable))


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = _column_names(inspector, 'agent_context_feedback')
    for name in [
        'improvement_notes_payload',
        'conservative_take_payload',
        'fundamental_backdrop_payload',
        'data_gaps_payload',
        'anomalies_and_gaps_payload',
        'support_resistance_payload',
        'trend_context_payload',
        'price_action_payload',
        'leaders_laggards_payload',
        'breadth_context_payload',
        'details_payload',
        'normalized_feedback_payload',
    ]:
        if name in columns:
            op.drop_column('agent_context_feedback', name)
