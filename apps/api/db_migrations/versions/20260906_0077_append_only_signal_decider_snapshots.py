"""preserve signal-decider decision snapshots as append-only history

Revision ID: 20260906_0077
Revises: 20260906_0076
Create Date: 2026-09-06 15:00:00
"""

from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from typing import Any

from alembic import op
import sqlalchemy as sa


revision = '20260906_0077'
down_revision = '20260906_0076'
branch_labels = None
depends_on = None

TABLE_NAME = 'signal_decider_decision_snapshots'
FEEDBACK_INDEX_NAME = 'ix_signal_decider_decision_snapshots_feedback_id'
LEGACY_UNIQUE_INDEX_NAME = 'uq_signal_decider_decision_snapshots_feedback_id_not_null'
FINGERPRINT_COLUMN_NAME = 'content_fingerprint'
FINGERPRINT_UNIQUE_INDEX_NAME = (
    'uq_signal_decider_snapshots_content_fingerprint'
)
FINGERPRINT_EXCLUDED_FIELDS = frozenset(
    {'id', FINGERPRINT_COLUMN_NAME, 'feedback_received_at', 'created_at', 'updated_at'}
)


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {column['name'] for column in inspector.get_columns(table_name)}


def _index_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {index['name'] for index in inspector.get_indexes(table_name)}


def _canonical_fingerprint_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, (int, float, Decimal)):
        decimal_value = Decimal(str(value))
        if not decimal_value.is_finite():
            raise RuntimeError('decision snapshot fingerprint values must be finite')
        return {'__number__': format(decimal_value.normalize(), 'f')}
    if isinstance(value, datetime):
        return {'__datetime__': value.isoformat()}
    if isinstance(value, date):
        return {'__date__': value.isoformat()}
    if isinstance(value, dict):
        return {
            str(key): _canonical_fingerprint_value(nested_value)
            for key, nested_value in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_fingerprint_value(item) for item in value]
    raise RuntimeError(
        f'unsupported decision snapshot fingerprint value type: {type(value).__name__}'
    )


def _content_fingerprint(row: dict[str, Any]) -> str:
    payload = {
        str(key): _canonical_fingerprint_value(value)
        for key, value in row.items()
        if key not in FINGERPRINT_EXCLUDED_FIELDS
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(',', ':'),
        sort_keys=True,
    ).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _backfill_content_fingerprints(bind, snapshots: sa.Table) -> None:
    seen_fingerprints = set(
        bind.execute(
            sa.select(snapshots.c.content_fingerprint).where(
                snapshots.c.content_fingerprint.is_not(None)
            )
        ).scalars()
    )
    last_id = 0
    while True:
        rows = bind.execute(
            sa.select(snapshots)
            .where(snapshots.c.id > last_id)
            .order_by(snapshots.c.id.asc())
            .limit(500)
        ).mappings().all()
        if not rows:
            return
        for raw_row in rows:
            row = dict(raw_row)
            snapshot_id = int(row['id'])
            if row.get('content_fingerprint') is not None:
                last_id = snapshot_id
                continue
            content_fingerprint = _content_fingerprint(row)
            if content_fingerprint in seen_fingerprints:
                last_id = snapshot_id
                continue
            bind.execute(
                snapshots.update()
                .where(snapshots.c.id == snapshot_id)
                .values(content_fingerprint=content_fingerprint)
            )
            seen_fingerprints.add(content_fingerprint)
            last_id = snapshot_id


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, TABLE_NAME):
        return

    if FINGERPRINT_COLUMN_NAME not in _column_names(inspector, TABLE_NAME):
        op.add_column(
            TABLE_NAME,
            sa.Column(FINGERPRINT_COLUMN_NAME, sa.String(length=64), nullable=True),
        )

    snapshots = sa.Table(TABLE_NAME, sa.MetaData(), autoload_with=bind)
    _backfill_content_fingerprints(bind, snapshots)

    inspector = sa.inspect(bind)
    index_names = _index_names(inspector, TABLE_NAME)
    if FINGERPRINT_UNIQUE_INDEX_NAME not in index_names:
        op.create_index(
            FINGERPRINT_UNIQUE_INDEX_NAME,
            TABLE_NAME,
            [FINGERPRINT_COLUMN_NAME],
            unique=True,
            postgresql_where=sa.text('content_fingerprint IS NOT NULL'),
            sqlite_where=sa.text('content_fingerprint IS NOT NULL'),
        )
    if LEGACY_UNIQUE_INDEX_NAME in index_names:
        op.drop_index(LEGACY_UNIQUE_INDEX_NAME, table_name=TABLE_NAME)
    if FEEDBACK_INDEX_NAME not in index_names:
        op.create_index(
            FEEDBACK_INDEX_NAME,
            TABLE_NAME,
            ['feedback_id'],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, TABLE_NAME):
        return

    snapshots = sa.Table(TABLE_NAME, sa.MetaData(), autoload_with=bind)
    duplicate_feedback_id = bind.execute(
        sa.select(snapshots.c.feedback_id)
        .where(snapshots.c.feedback_id.is_not(None))
        .group_by(snapshots.c.feedback_id)
        .having(sa.func.count(snapshots.c.id) > 1)
        .limit(1)
    ).scalar_one_or_none()
    if duplicate_feedback_id is not None:
        raise RuntimeError(
            'cannot downgrade append-only signal-decider snapshots while duplicate '
            'feedback_id history exists; deleting or merging history would break causality'
        )

    index_names = _index_names(inspector, TABLE_NAME)
    if FINGERPRINT_UNIQUE_INDEX_NAME in index_names:
        op.drop_index(FINGERPRINT_UNIQUE_INDEX_NAME, table_name=TABLE_NAME)
    if LEGACY_UNIQUE_INDEX_NAME not in index_names:
        op.create_index(
            LEGACY_UNIQUE_INDEX_NAME,
            TABLE_NAME,
            ['feedback_id'],
            unique=True,
            postgresql_where=sa.text('feedback_id IS NOT NULL'),
            sqlite_where=sa.text('feedback_id IS NOT NULL'),
        )

    inspector = sa.inspect(bind)
    if FINGERPRINT_COLUMN_NAME in _column_names(inspector, TABLE_NAME):
        op.drop_column(TABLE_NAME, FINGERPRINT_COLUMN_NAME)
