from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import threading
from typing import Any, Protocol

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.db import session_scope
from app.db_models import CoreExecBuyAuthorityOutbox, CoreExecBuyAuthorityState
from app.orders.buy_authority_contract import (
    ACK_SCHEMA_VERSION,
    AUTHORITY_SCHEMA_VERSION,
    OBSERVATION_ID_PREFIX,
    OBSERVATION_SCHEMA_VERSION,
    CoreBuyAuthorityAckV1,
    CoreBuyAuthorityObservationV1,
    core_buy_authority_material_fingerprint,
)


class CoreExecBuyAuthorityConflictError(RuntimeError):
    """Raised when durable idempotency material cannot be trusted."""


class _ExecBuyAuthorityClient(Protocol):
    async def post_buy_authority_observation(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]: ...


SessionContext = Callable[[], AbstractContextManager[Session]]
_SCOPE_FLUSH_LOCKS: dict[str, threading.Lock] = {}
_SCOPE_FLUSH_LOCKS_GUARD = threading.Lock()
_SCOPE_STAGE_LOCKS: dict[str, threading.Lock] = {}
_SCOPE_STAGE_LOCKS_GUARD = threading.Lock()


@dataclass(frozen=True)
class StagedCoreBuyAuthorityObservation:
    observation_id: str
    scope_key: str


@dataclass(frozen=True)
class CoreBuyAuthorityFlushResult:
    scope_key: str
    observation_id: str | None
    dispatch_allowed: bool
    ack: CoreBuyAuthorityAckV1 | None
    errors: tuple[str, ...]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        except ValueError as exc:
            raise ValueError('buy authority timestamp is invalid') from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('buy authority timestamp must include a timezone offset')
    return parsed


def _json_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return json.dumps(left, sort_keys=True, separators=(',', ':')) == json.dumps(
        right,
        sort_keys=True,
        separators=(',', ':'),
    )


def _scope_flush_lock(scope_key: str) -> threading.Lock:
    with _SCOPE_FLUSH_LOCKS_GUARD:
        return _SCOPE_FLUSH_LOCKS.setdefault(scope_key, threading.Lock())


def _scope_stage_lock(scope_key: str) -> threading.Lock:
    with _SCOPE_STAGE_LOCKS_GUARD:
        return _SCOPE_STAGE_LOCKS.setdefault(scope_key, threading.Lock())


def _scope_advisory_lock_key(scope_key: str) -> int:
    digest = hashlib.sha256(
        f'core-exec-buy-authority-delivery:{scope_key}'.encode('utf-8')
    ).digest()
    return int.from_bytes(digest[:8], byteorder='big', signed=True)


def _scope_stage_advisory_lock_key(scope_key: str) -> int:
    digest = hashlib.sha256(
        f'core-exec-buy-authority-stage:{scope_key}'.encode('utf-8')
    ).digest()
    return int.from_bytes(digest[:8], byteorder='big', signed=True)


def core_buy_authority_scope_key(
    observation: CoreBuyAuthorityObservationV1,
) -> str:
    return core_buy_authority_material_fingerprint(
        {'scope': observation.scope.model_dump(mode='python')}
    )


def build_core_buy_authority_observation(
    *,
    route_authority: dict[str, Any],
    runtime_mode: str,
    account_reference: str,
) -> CoreBuyAuthorityObservationV1:
    """Translate only Core's deterministic route authority into the wire contract."""

    if route_authority.get('applies') is not True:
        raise ValueError('current live market buy authority is not applicable')
    if route_authority.get('schema_version') != AUTHORITY_SCHEMA_VERSION:
        raise ValueError('unexpected current live market buy authority schema')
    if route_authority.get('source') != 'current_market_snapshot_cache':
        raise ValueError('unexpected current live market buy authority source')
    if route_authority.get('market') != 'KRX':
        raise ValueError('route buy authority must cover KRX exactly')

    action = route_authority.get('action')
    if action not in {'allow', 'block_new_buy'}:
        raise ValueError('route buy authority action is invalid')
    source_status = route_authority.get('status')
    if source_status not in {'available', 'unavailable'}:
        raise ValueError('route buy authority status is invalid')
    evidence = route_authority.get('market_evidence')
    if not isinstance(evidence, list):
        raise ValueError('route buy authority market evidence is missing')
    reason_codes = route_authority.get('reason_codes')
    if not isinstance(reason_codes, list):
        raise ValueError('route buy authority reason codes are missing')

    material: dict[str, Any] = {
        'schema_version': OBSERVATION_SCHEMA_VERSION,
        'authority_schema_version': AUTHORITY_SCHEMA_VERSION,
        'source': 'current_market_snapshot_cache',
        'scope': {
            'provider': 'kis',
            'market': 'KRX',
            'currency': 'KRW',
            'runtime_mode': runtime_mode,
            'account_reference': account_reference,
        },
        'market_date': date.fromisoformat(str(route_authority.get('market_date'))),
        'evaluated_at': _parse_datetime(route_authority.get('evaluated_at')),
        'source_observed_at': _parse_datetime(route_authority.get('observed_at')),
        'action': action,
        'authoritative': route_authority.get('authoritative') is True,
        'source_status': source_status,
        'reason_codes': [str(value).strip() for value in reason_codes],
        'required_markets': route_authority.get('required_markets'),
        'market_evidence': [
            {
                'market': item.get('market'),
                'status': item.get('status'),
                'authoritative': item.get('authoritative') is True,
                'action': item.get('action'),
                'observed_at': _parse_datetime(item.get('observed_at')),
                'reason_code': item.get('reason_code'),
            }
            for item in evidence
            if isinstance(item, dict)
        ],
    }
    fingerprint = core_buy_authority_material_fingerprint(material)
    material['material_fingerprint'] = fingerprint
    material['observation_id'] = f'{OBSERVATION_ID_PREFIX}{fingerprint}'
    return CoreBuyAuthorityObservationV1.model_validate(material)


class CoreExecBuyAuthorityBridge:
    def __init__(self, *, session_context: SessionContext = session_scope) -> None:
        self._session_context = session_context

    @contextmanager
    def _serialized_stage_session(self, *, scope_key: str):
        with _scope_stage_lock(scope_key):
            with self._session_context() as session:
                bind = session.get_bind()
                if bind is not None and bind.dialect.name == 'postgresql':
                    session.execute(
                        text('SELECT pg_advisory_xact_lock(:lock_key)'),
                        {'lock_key': _scope_stage_advisory_lock_key(scope_key)},
                    )
                yield session

    def stage(
        self,
        observation: CoreBuyAuthorityObservationV1,
    ) -> StagedCoreBuyAuthorityObservation:
        payload = observation.model_dump(mode='json')
        scope_key = core_buy_authority_scope_key(observation)
        conflict_message: str | None = None
        with self._serialized_stage_session(scope_key=scope_key) as session:
            state = session.execute(
                select(CoreExecBuyAuthorityState)
                .where(CoreExecBuyAuthorityState.scope_key == scope_key)
                .with_for_update()
            ).scalar_one_or_none()
            existing = session.execute(
                select(CoreExecBuyAuthorityOutbox)
                .where(
                    CoreExecBuyAuthorityOutbox.observation_id
                    == observation.observation_id
                )
                .with_for_update()
            ).scalar_one_or_none()
            if existing is not None and (
                existing.scope_key != scope_key
                or existing.material_fingerprint
                != observation.material_fingerprint
                or not _json_equal(existing.payload, payload)
            ):
                conflict_message = (
                    'conflicting durable material exists for observation_id '
                    f'{observation.observation_id}'
                )
                affected_states = {
                    candidate.scope_key: candidate
                    for candidate in (
                        state,
                        session.execute(
                            select(CoreExecBuyAuthorityState)
                            .where(
                                CoreExecBuyAuthorityState.scope_key
                                == existing.scope_key
                            )
                            .with_for_update()
                        ).scalar_one_or_none(),
                    )
                    if candidate is not None
                }
                for affected_state in affected_states.values():
                    affected_state.delivery_status = 'conflict'
                    affected_state.remote_fence_active = True
                    affected_state.last_error = conflict_message
            elif existing is not None and state is None:
                conflict_message = (
                    'durable observation exists without its authority scope state'
                )
            elif existing is None:
                if observation.action == 'block_new_buy':
                    prior_pending = session.execute(
                        select(CoreExecBuyAuthorityOutbox)
                        .where(
                            CoreExecBuyAuthorityOutbox.scope_key == scope_key,
                            CoreExecBuyAuthorityOutbox.status == 'pending',
                        )
                        .order_by(
                            CoreExecBuyAuthorityOutbox.delivery_sequence.asc(),
                        )
                        .with_for_update()
                    ).scalars()
                    for pending_row in prior_pending:
                        pending_row.status = 'superseded'
                delivery_sequence = (
                    int(state.latest_delivery_sequence) + 1
                    if state is not None
                    else 1
                )
                if state is None:
                    state = CoreExecBuyAuthorityState(
                        scope_key=scope_key,
                        provider=observation.scope.provider,
                        market=observation.scope.market,
                        currency=observation.scope.currency,
                        runtime_mode=observation.scope.runtime_mode,
                        account_reference=observation.scope.account_reference,
                        latest_observation_id=observation.observation_id,
                        latest_delivery_sequence=delivery_sequence,
                        latest_material_fingerprint=observation.material_fingerprint,
                        latest_action=observation.action,
                        market_date=observation.market_date,
                        source_observed_at=observation.source_observed_at,
                        evaluated_at=observation.evaluated_at,
                        delivery_status='pending',
                        remote_fence_active=True,
                    )
                    session.add(state)
                    # SQLAlchemy mapper ordering does not infer this dependency
                    # without an ORM relationship. Persist the parent first while
                    # keeping both rows inside the same transaction.
                    session.flush()
                else:
                    state.latest_observation_id = observation.observation_id
                    state.latest_delivery_sequence = delivery_sequence
                    state.latest_material_fingerprint = (
                        observation.material_fingerprint
                    )
                    state.latest_action = observation.action
                    state.market_date = observation.market_date
                    state.source_observed_at = observation.source_observed_at
                    state.evaluated_at = observation.evaluated_at
                    state.delivery_status = 'pending'
                    state.remote_fence_active = True
                    state.last_error = None
                session.add(
                    CoreExecBuyAuthorityOutbox(
                        observation_id=observation.observation_id,
                        scope_key=scope_key,
                        delivery_sequence=delivery_sequence,
                        material_fingerprint=observation.material_fingerprint,
                        payload=payload,
                        status='pending',
                        attempt_count=0,
                    )
                )

            if conflict_message is None:
                session.flush()

        if conflict_message is not None:
            raise CoreExecBuyAuthorityConflictError(conflict_message)
        return StagedCoreBuyAuthorityObservation(
            observation_id=observation.observation_id,
            scope_key=scope_key,
        )

    def _pending_payloads(
        self,
        *,
        scope_key: str,
    ) -> list[tuple[str, dict[str, Any]]]:
        with self._session_context() as session:
            rows = list(
                session.execute(
                    select(CoreExecBuyAuthorityOutbox)
                    .where(
                        CoreExecBuyAuthorityOutbox.scope_key == scope_key,
                        CoreExecBuyAuthorityOutbox.status == 'pending',
                    )
                    .order_by(
                        CoreExecBuyAuthorityOutbox.delivery_sequence.asc(),
                    )
                    .with_for_update()
                ).scalars()
            )
            latest_block_sequence = max(
                (
                    row.delivery_sequence
                    for row in rows
                    if isinstance(row.payload, dict)
                    and row.payload.get('action') == 'block_new_buy'
                ),
                default=None,
            )
            if latest_block_sequence is not None:
                for row in rows:
                    if row.delivery_sequence < latest_block_sequence:
                        row.status = 'superseded'
                session.flush()
                rows = [
                    row
                    for row in rows
                    if row.delivery_sequence >= latest_block_sequence
                ]
            return [(row.observation_id, dict(row.payload)) for row in rows]

    @contextmanager
    def _database_delivery_lock(self, *, scope_key: str):
        """Hold a cross-process scope lock for the complete ordered HTTP flush."""

        with self._session_context() as session:
            bind = session.get_bind()
            if bind is None or bind.dialect.name != 'postgresql':
                yield True
                return
            lock_key = _scope_advisory_lock_key(scope_key)
            acquired = bool(
                session.execute(
                    text('SELECT pg_try_advisory_lock(:lock_key)'),
                    {'lock_key': lock_key},
                ).scalar_one()
            )
            try:
                yield acquired
            finally:
                if acquired:
                    session.execute(
                        text('SELECT pg_advisory_unlock(:lock_key)'),
                        {'lock_key': lock_key},
                    )

    def _record_attempt(self, *, observation_id: str, now: datetime) -> bool:
        with self._session_context() as session:
            row = session.execute(
                select(CoreExecBuyAuthorityOutbox)
                .where(
                    CoreExecBuyAuthorityOutbox.observation_id == observation_id
                )
                .with_for_update()
            ).scalar_one()
            if row.status != 'pending':
                return False
            row.attempt_count += 1
            row.last_attempted_at = now
            session.flush()
            return True

    @staticmethod
    def _lock_state_then_outbox(
        *,
        session: Session,
        scope_key: str,
        observation_id: str,
    ) -> tuple[CoreExecBuyAuthorityState | None, CoreExecBuyAuthorityOutbox]:
        state = session.execute(
            select(CoreExecBuyAuthorityState)
            .where(CoreExecBuyAuthorityState.scope_key == scope_key)
            .with_for_update()
        ).scalar_one_or_none()
        row = session.execute(
            select(CoreExecBuyAuthorityOutbox)
            .where(CoreExecBuyAuthorityOutbox.observation_id == observation_id)
            .with_for_update()
        ).scalar_one()
        if row.scope_key != scope_key:
            raise CoreExecBuyAuthorityConflictError(
                'outbox scope changed before delivery persistence'
            )
        return state, row

    def _record_failure(
        self,
        *,
        scope_key: str,
        observation_id: str,
        error: str,
    ) -> None:
        with self._session_context() as session:
            state, row = self._lock_state_then_outbox(
                session=session,
                scope_key=scope_key,
                observation_id=observation_id,
            )
            if row.status != 'superseded':
                row.status = 'pending'
            row.last_error = error
            if state is not None and state.latest_observation_id == observation_id:
                if state.delivery_status != 'conflict':
                    state.delivery_status = 'pending'
                state.remote_fence_active = True
                state.last_error = error
            session.flush()

    def _record_ack(
        self,
        *,
        observation: CoreBuyAuthorityObservationV1,
        ack: CoreBuyAuthorityAckV1,
        now: datetime,
    ) -> None:
        conflict_message: str | None = None
        scope_key = core_buy_authority_scope_key(observation)
        with self._session_context() as session:
            state, row = self._lock_state_then_outbox(
                session=session,
                scope_key=scope_key,
                observation_id=observation.observation_id,
            )
            if row.material_fingerprint != observation.material_fingerprint:
                raise CoreExecBuyAuthorityConflictError(
                    'outbox material changed before ACK persistence'
                )
            row.status = 'acknowledged'
            row.acknowledged_at = now
            row.ack_payload = ack.model_dump(mode='json')
            row.last_error = None
            if state is not None and state.latest_observation_id == observation.observation_id:
                if (
                    state.remote_scope_fingerprint is not None
                    and state.remote_scope_fingerprint != ack.scope_fingerprint
                ):
                    conflict_message = (
                        'Exec ACK scope fingerprint changed for the same Core route'
                    )
                    row.status = 'pending'
                    row.last_error = conflict_message
                    state.delivery_status = 'conflict'
                    state.remote_fence_active = True
                    state.last_error = conflict_message
                else:
                    state.delivery_status = (
                        'acknowledged'
                        if ack.status in {'applied', 'duplicate'}
                        else 'rejected_stale'
                    )
                    state.remote_scope_fingerprint = ack.scope_fingerprint
                    state.remote_fence_active = bool(
                        ack.fence_active or ack.projection_pending
                    )
                    state.last_acknowledged_at = now
                    state.last_error = None
            session.flush()
        if conflict_message is not None:
            raise CoreExecBuyAuthorityConflictError(conflict_message)

    def _result(self, *, scope_key: str, errors: list[str]) -> CoreBuyAuthorityFlushResult:
        with self._session_context() as session:
            state = session.get(CoreExecBuyAuthorityState, scope_key)
            if state is None:
                return CoreBuyAuthorityFlushResult(
                    scope_key=scope_key,
                    observation_id=None,
                    dispatch_allowed=False,
                    ack=None,
                    errors=tuple(errors or ['buy authority state is missing']),
                )
            outbox = session.get(
                CoreExecBuyAuthorityOutbox,
                state.latest_observation_id,
            )
            ack: CoreBuyAuthorityAckV1 | None = None
            if outbox is not None and isinstance(outbox.ack_payload, dict):
                try:
                    ack = CoreBuyAuthorityAckV1.model_validate(outbox.ack_payload)
                except ValueError as exc:
                    errors.append(f'invalid persisted ACK: {exc}')
            allowed = bool(
                not errors
                and state.latest_action == 'allow'
                and state.delivery_status == 'acknowledged'
                and state.remote_fence_active is False
                and outbox is not None
                and outbox.status == 'acknowledged'
                and ack is not None
                and ack.status in {'applied', 'duplicate'}
                and ack.observation_id == state.latest_observation_id
                and ack.material_fingerprint == state.latest_material_fingerprint
                and ack.fence_active is False
                and ack.projection_pending is False
            )
            return CoreBuyAuthorityFlushResult(
                scope_key=scope_key,
                observation_id=state.latest_observation_id,
                dispatch_allowed=allowed,
                ack=ack,
                errors=tuple(errors),
            )

    async def flush_scope(
        self,
        *,
        scope_key: str,
        client: _ExecBuyAuthorityClient,
    ) -> CoreBuyAuthorityFlushResult:
        errors: list[str] = []
        local_lock = _scope_flush_lock(scope_key)
        if not local_lock.acquire(blocking=False):
            return self._result(
                scope_key=scope_key,
                errors=['buy authority delivery is already in progress'],
            )
        try:
            with self._database_delivery_lock(scope_key=scope_key) as acquired:
                if not acquired:
                    return self._result(
                        scope_key=scope_key,
                        errors=['buy authority delivery is already in progress'],
                    )
                while True:
                    pending = self._pending_payloads(scope_key=scope_key)
                    if not pending:
                        break
                    observation_id, payload = pending[0]
                    now = _utcnow()
                    if not self._record_attempt(
                        observation_id=observation_id,
                        now=now,
                    ):
                        continue
                    try:
                        if payload.get('observation_id') != observation_id:
                            raise CoreExecBuyAuthorityConflictError(
                                'outbox key conflicts with its observation payload'
                            )
                        observation = CoreBuyAuthorityObservationV1.model_validate(
                            payload
                        )
                        raw_ack = await client.post_buy_authority_observation(payload)
                        ack = CoreBuyAuthorityAckV1.model_validate(raw_ack)
                        if ack.schema_version != ACK_SCHEMA_VERSION:
                            raise ValueError('unexpected buy authority ACK schema')
                        if ack.observation_id != observation.observation_id:
                            raise ValueError('buy authority ACK observation_id mismatch')
                        if (
                            ack.material_fingerprint
                            != observation.material_fingerprint
                        ):
                            raise ValueError(
                                'buy authority ACK material fingerprint mismatch'
                            )
                        self._record_ack(
                            observation=observation,
                            ack=ack,
                            now=now,
                        )
                    except Exception as exc:  # noqa: BLE001 - uncertainty fences buys
                        error = f'{type(exc).__name__}: {exc}'
                        self._record_failure(
                            scope_key=scope_key,
                            observation_id=observation_id,
                            error=error,
                        )
                        errors.append(error)
                        break
                return self._result(scope_key=scope_key, errors=errors)
        finally:
            local_lock.release()


core_exec_buy_authority_bridge = CoreExecBuyAuthorityBridge()
