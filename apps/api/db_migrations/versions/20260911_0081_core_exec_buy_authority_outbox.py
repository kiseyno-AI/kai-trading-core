"""persist Core to Exec buy-authority delivery state

Revision ID: 20260911_0081
Revises: 20260907_0080
Create Date: 2026-09-11 12:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260911_0081'
down_revision = '20260907_0080'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'core_exec_buy_authority_states',
        sa.Column('scope_key', sa.String(length=64), nullable=False),
        sa.Column('provider', sa.String(length=32), nullable=False),
        sa.Column('market', sa.String(length=32), nullable=False),
        sa.Column('currency', sa.String(length=16), nullable=False),
        sa.Column('runtime_mode', sa.String(length=16), nullable=False),
        sa.Column('account_reference', sa.String(length=128), nullable=False),
        sa.Column('latest_observation_id', sa.String(length=128), nullable=False),
        sa.Column('latest_delivery_sequence', sa.Integer(), nullable=False),
        sa.Column('latest_material_fingerprint', sa.String(length=64), nullable=False),
        sa.Column('latest_action', sa.String(length=32), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source_observed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('evaluated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('delivery_status', sa.String(length=32), nullable=False),
        sa.Column('remote_scope_fingerprint', sa.String(length=64), nullable=True),
        sa.Column('remote_fence_active', sa.Boolean(), nullable=True),
        sa.Column('last_acknowledged_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "provider = 'kis' AND market = 'KRX' AND currency = 'KRW'",
            name='ck_core_exec_buy_authority_states_krx_scope',
        ),
        sa.PrimaryKeyConstraint('scope_key'),
        sa.UniqueConstraint(
            'provider',
            'market',
            'currency',
            'runtime_mode',
            'account_reference',
            name='uq_core_exec_buy_authority_states_scope',
        ),
    )
    op.create_table(
        'core_exec_buy_authority_outbox',
        sa.Column('observation_id', sa.String(length=128), nullable=False),
        sa.Column('scope_key', sa.String(length=64), nullable=False),
        sa.Column('delivery_sequence', sa.Integer(), nullable=False),
        sa.Column('material_fingerprint', sa.String(length=64), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('attempt_count', sa.Integer(), nullable=False),
        sa.Column('last_attempted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('acknowledged_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('ack_payload', sa.JSON(), nullable=True),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ['scope_key'],
            ['core_exec_buy_authority_states.scope_key'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('observation_id'),
        sa.UniqueConstraint(
            'scope_key',
            'delivery_sequence',
            name='uq_core_exec_buy_authority_outbox_scope_sequence',
        ),
    )
    op.create_index(
        'ix_core_exec_buy_authority_outbox_pending',
        'core_exec_buy_authority_outbox',
        ['scope_key', 'status', 'created_at'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        'ix_core_exec_buy_authority_outbox_pending',
        table_name='core_exec_buy_authority_outbox',
    )
    op.drop_table('core_exec_buy_authority_outbox')
    op.drop_table('core_exec_buy_authority_states')
