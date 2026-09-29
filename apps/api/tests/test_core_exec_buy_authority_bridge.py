from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from threading import Barrier
import unittest
from unittest.mock import AsyncMock, patch

from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db_models import (
    Base,
    CoreExecBuyAuthorityOutbox,
    CoreExecBuyAuthorityState,
)
from app.orders.buy_authority_bridge import (
    CoreExecBuyAuthorityBridge,
    CoreExecBuyAuthorityConflictError,
    _scope_advisory_lock_key,
    build_core_buy_authority_observation,
)
from app.orders.buy_authority_contract import CoreBuyAuthorityAckV1
from app.orders.exec_integration import SignalDeciderExecIntegrationService
from app.orders.exec_integration import (
    _dispatch_group_requires_core_krx_buy_authority,
)
from app.orders.next_open_execution import (
    NextOpenAccountTarget,
    _current_route_live_market_buy_authority,
)


ACCOUNT_REFERENCE = 'KIS:12345678-01'
OBSERVED_AT = datetime(2026, 9, 11, 0, 1, tzinfo=timezone.utc)


def _route_authority(
    *,
    action: str = 'allow',
    observed_at: datetime | None = OBSERVED_AT,
) -> dict[str, object]:
    available = observed_at is not None
    return {
        'schema_version': 'current_live_market_buy_authority_v1',
        'applies': True,
        'market_date': '2026-09-11',
        'source': 'current_market_snapshot_cache',
        'market': 'KRX',
        'status': 'available' if available else 'unavailable',
        'authoritative': available,
        'action': action,
        'reason_codes': ['normal' if action == 'allow' else 'risk_off'],
        'observed_at': observed_at.isoformat() if observed_at else None,
        'evaluated_at': (OBSERVED_AT + timedelta(seconds=1)).isoformat(),
        'required_markets': ['KOSPI', 'KOSDAQ'],
        'market_evidence': [
            {
                'market': market,
                'status': 'available' if available else 'unavailable',
                'authoritative': available,
                'action': action,
                'observed_at': observed_at.isoformat() if observed_at else None,
                'reason_code': 'normal' if action == 'allow' else 'risk_off',
            }
            for market in ('KOSPI', 'KOSDAQ')
        ],
    }


class _FakeClient:
    def __init__(
        self,
        *,
        fail: bool = False,
        fence_active: bool = False,
        scope_fingerprint: str = 'a' * 64,
    ) -> None:
        self.fail = fail
        self.fence_active = fence_active
        self.scope_fingerprint = scope_fingerprint
        self.payloads: list[dict[str, object]] = []
        self.execution_plan_payloads: list[dict[str, object]] = []

    async def post_buy_authority_observation(
        self,
        payload: dict[str, object],
    ) -> dict[str, object]:
        self.payloads.append(payload)
        if self.fail:
            raise RuntimeError('exec unavailable')
        return {
            'schema_version': 'core_exec_buy_authority_ack_v1',
            'status': 'applied',
            'observation_id': payload['observation_id'],
            'material_fingerprint': payload['material_fingerprint'],
            'scope_fingerprint': self.scope_fingerprint,
            'fence_active': self.fence_active,
            'projection_pending': False,
            'healthy_observation_count': 0,
            'changed_item_count': 0,
            'accepted_source_observed_at': payload['source_observed_at'],
        }

    async def post_execution_plan(
        self,
        payload: dict[str, object],
    ) -> dict[str, object]:
        self.execution_plan_payloads.append(payload)
        return {}


class CoreExecBuyAuthorityBridgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            'sqlite+pysqlite:///:memory:',
            connect_args={'check_same_thread': False},
            poolclass=StaticPool,
            future=True,
        )

        @event.listens_for(self.engine, 'connect')
        def enable_sqlite_foreign_keys(
            dbapi_connection: object,
            _connection_record: object,
        ) -> None:
            cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
            try:
                cursor.execute('PRAGMA foreign_keys=ON')
            finally:
                cursor.close()

        Base.metadata.create_all(
            self.engine,
            tables=[
                CoreExecBuyAuthorityState.__table__,
                CoreExecBuyAuthorityOutbox.__table__,
            ],
        )
        factory = sessionmaker(bind=self.engine, expire_on_commit=False, future=True)

        @contextmanager
        def test_session_scope():
            session = factory()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        self.factory = factory
        self.bridge = CoreExecBuyAuthorityBridge(session_context=test_session_scope)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_builds_strict_observation_only_from_route_authority(self) -> None:
        observation = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )

        self.assertEqual(observation.action, 'allow')
        self.assertEqual(observation.scope.account_reference, ACCOUNT_REFERENCE)
        self.assertEqual(
            observation.observation_id,
            f'buy-authority-v1:{observation.material_fingerprint}',
        )
        self.assertEqual(
            observation.material_fingerprint,
            'b26f85f5a98bcdd767a05e8a5b537994f58fd6705f53e09b66f07673841b94e8',
        )
        self.assertEqual(
            observation.source_observed_at,
            OBSERVED_AT,
        )

        with self.assertRaisesRegex(ValueError, 'not applicable'):
            build_core_buy_authority_observation(
                route_authority={**_route_authority(), 'applies': False},
                runtime_mode='live',
                account_reference=ACCOUNT_REFERENCE,
            )

    def test_delivery_lock_key_and_authority_scope_are_route_exact(self) -> None:
        self.assertIsInstance(_scope_advisory_lock_key('scope'), int)
        buy = SimpleNamespace(planned_side='buy')
        sell = SimpleNamespace(planned_side='sell')

        self.assertTrue(
            _dispatch_group_requires_core_krx_buy_authority(
                route_family='kis_krx_krw',
                rows=[buy],
            )
        )
        self.assertFalse(
            _dispatch_group_requires_core_krx_buy_authority(
                route_family='unsupported_us_equity_exec_agent_sizing',
                rows=[buy],
            )
        )
        self.assertFalse(
            _dispatch_group_requires_core_krx_buy_authority(
                route_family='kis_krx_krw',
                rows=[sell],
            )
        )
        self.assertFalse(
            SignalDeciderExecIntegrationService._row_is_dispatchable(sell)
        )

    def test_contract_rejects_future_or_wrong_day_market_evidence(self) -> None:
        future_evidence = _route_authority()
        future_evidence['market_evidence'][1]['observed_at'] = (  # type: ignore[index]
            OBSERVED_AT + timedelta(hours=1)
        ).isoformat()
        with self.assertRaisesRegex(
            ValueError,
            'evaluated_at must not precede market evidence',
        ):
            build_core_buy_authority_observation(
                route_authority=future_evidence,
                runtime_mode='live',
                account_reference=ACCOUNT_REFERENCE,
            )

        wrong_day = _route_authority()
        wrong_day['market_evidence'][1]['observed_at'] = (  # type: ignore[index]
            OBSERVED_AT + timedelta(days=1)
        ).isoformat()
        wrong_day['evaluated_at'] = (
            OBSERVED_AT + timedelta(days=1, seconds=1)
        ).isoformat()
        with self.assertRaisesRegex(
            ValueError,
            'market evidence must belong to market_date',
        ):
            build_core_buy_authority_observation(
                route_authority=wrong_day,
                runtime_mode='live',
                account_reference=ACCOUNT_REFERENCE,
            )

    def test_route_authority_requires_both_required_markets(self) -> None:
        market_state = SimpleNamespace(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            server_time='2026-09-11T09:01:01+09:00',
        )
        kospi = {
            'status': 'available',
            'authoritative': True,
            'action': 'allow',
            'observed_at': '2026-09-11T09:01:00+09:00',
            'reason_code': 'normal',
        }
        kosdaq_missing = {
            'status': 'unavailable',
            'authoritative': False,
            'action': 'block_new_buy',
            'observed_at': None,
            'reason_code': 'market_snapshot_unavailable',
        }
        with patch(
            'app.orders.next_open_execution._current_live_market_buy_authority',
            side_effect=[kospi, kosdaq_missing],
        ):
            route = _current_route_live_market_buy_authority(
                target_trade_date=date(2026, 9, 11),
                market_state=market_state,
                snapshot_context={},
            )

        self.assertEqual(route['action'], 'block_new_buy')
        self.assertEqual(route['status'], 'unavailable')
        self.assertFalse(route['authoritative'])
        self.assertIsNone(route['observed_at'])

    def test_route_authority_drops_untrusted_unavailable_evidence_timestamps(
        self,
    ) -> None:
        market_state = SimpleNamespace(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            server_time='2026-09-11T09:01:01+09:00',
        )
        kospi = {
            'status': 'available',
            'authoritative': True,
            'action': 'allow',
            'observed_at': '2026-09-11T09:01:00+09:00',
            'reason_code': 'normal',
        }
        bad_observed_at_values = (
            '2026-09-10T15:30:00+09:00',
            '2026-09-11T10:00:00+09:00',
            '2026-09-11T09:01:00',
        )

        for bad_observed_at in bad_observed_at_values:
            with self.subTest(observed_at=bad_observed_at):
                kosdaq_unavailable = {
                    'status': 'unavailable',
                    'authoritative': False,
                    'action': 'block_new_buy',
                    'observed_at': bad_observed_at,
                    'reason_code': 'market_snapshot_stale',
                }
                with patch(
                    'app.orders.next_open_execution._current_live_market_buy_authority',
                    side_effect=[kospi, kosdaq_unavailable],
                ):
                    route = _current_route_live_market_buy_authority(
                        target_trade_date=date(2026, 9, 11),
                        market_state=market_state,
                        snapshot_context={},
                    )

                self.assertEqual(route['action'], 'block_new_buy')
                self.assertIsNone(route['market_evidence'][1]['observed_at'])
                observation = build_core_buy_authority_observation(
                    route_authority=route,
                    runtime_mode='live',
                    account_reference=ACCOUNT_REFERENCE,
                )
                self.assertEqual(observation.action, 'block_new_buy')
                self.assertIsNone(observation.source_observed_at)

    async def test_stale_unavailable_evidence_still_delivers_explicit_block(
        self,
    ) -> None:
        market_state = SimpleNamespace(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            server_time='2026-09-11T09:01:01+09:00',
        )
        with patch(
            'app.orders.next_open_execution._current_live_market_buy_authority',
            side_effect=[
                {
                    'status': 'available',
                    'authoritative': True,
                    'action': 'allow',
                    'observed_at': '2026-09-11T09:01:00+09:00',
                    'reason_code': 'normal',
                },
                {
                    'status': 'unavailable',
                    'authoritative': False,
                    'action': 'block_new_buy',
                    'observed_at': '2026-09-10T15:30:00+09:00',
                    'reason_code': 'market_snapshot_stale',
                },
            ],
        ):
            route = _current_route_live_market_buy_authority(
                target_trade_date=date(2026, 9, 11),
                market_state=market_state,
                snapshot_context={},
            )

        client = _FakeClient(fence_active=True)
        service = SignalDeciderExecIntegrationService(client=client)  # type: ignore[arg-type]
        target = NextOpenAccountTarget(
            label='primary',
            account_reference=ACCOUNT_REFERENCE,
            budget_krw=Decimal('1'),
        )
        with (
            patch(
                'app.orders.exec_integration.async_get_market_state',
                new=AsyncMock(return_value=market_state),
            ),
            patch(
                'app.orders.exec_integration._load_live_market_snapshot_context',
                return_value=({}, False),
            ),
            patch(
                'app.orders.exec_integration._current_route_live_market_buy_authority',
                return_value=route,
            ),
            patch.object(
                __import__(
                    'app.orders.exec_integration',
                    fromlist=['signal_decider_next_open_execution_service'],
                ).signal_decider_next_open_execution_service,
                '_parse_account_targets',
                return_value=([target], []),
            ),
            patch(
                'app.orders.exec_integration.core_exec_buy_authority_bridge',
                self.bridge,
            ),
        ):
            result = await service._synchronize_open_buy_authority(
                target_trade_date=date(2026, 9, 11),
                account_reference=None,
                integration={'runtime_mode': 'live'},
            )

        self.assertEqual(len(client.payloads), 1)
        self.assertEqual(client.payloads[0]['action'], 'block_new_buy')
        self.assertIsNone(
            client.payloads[0]['market_evidence'][1]['observed_at'],
        )
        self.assertFalse(
            result['accounts'][ACCOUNT_REFERENCE]['dispatch_allowed'],
        )

    def test_stage_is_atomic_and_idempotent(self) -> None:
        observation = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )

        first = self.bridge.stage(observation)
        second = self.bridge.stage(observation)

        self.assertEqual(first.observation_id, second.observation_id)
        with self.factory() as session:
            self.assertEqual(
                len(session.scalars(select(CoreExecBuyAuthorityState)).all()),
                1,
            )
            self.assertEqual(
                len(session.scalars(select(CoreExecBuyAuthorityOutbox)).all()),
                1,
            )
            state = session.get(CoreExecBuyAuthorityState, first.scope_key)
            self.assertIsNotNone(state)
            self.assertEqual(state.delivery_status, 'pending')

    def test_state_and_outbox_roll_back_together_when_staging_fails(self) -> None:
        observation = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )

        def fail_outbox_insert(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError('injected outbox failure')

        event.listen(
            CoreExecBuyAuthorityOutbox,
            'before_insert',
            fail_outbox_insert,
        )
        try:
            with self.assertRaisesRegex(RuntimeError, 'injected outbox failure'):
                self.bridge.stage(observation)
        finally:
            event.remove(
                CoreExecBuyAuthorityOutbox,
                'before_insert',
                fail_outbox_insert,
            )

        with self.factory() as session:
            self.assertEqual(session.scalar(select(CoreExecBuyAuthorityState)), None)
            self.assertEqual(session.scalar(select(CoreExecBuyAuthorityOutbox)), None)

    def test_concurrent_first_stage_serializes_scope_creation(self) -> None:
        first_authority = _route_authority()
        first_authority['evaluated_at'] = (
            OBSERVED_AT + timedelta(seconds=1)
        ).isoformat()
        second_authority = _route_authority(
            observed_at=OBSERVED_AT + timedelta(seconds=2),
        )
        second_authority['evaluated_at'] = (
            OBSERVED_AT + timedelta(seconds=3)
        ).isoformat()
        observations = [
            build_core_buy_authority_observation(
                route_authority=authority,
                runtime_mode='live',
                account_reference=ACCOUNT_REFERENCE,
            )
            for authority in (first_authority, second_authority)
        ]
        start = Barrier(3)

        def stage(observation):
            start.wait(timeout=5)
            return self.bridge.stage(observation)

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(stage, observation) for observation in observations]
            start.wait(timeout=5)
            staged = [future.result(timeout=5) for future in futures]

        self.assertEqual({item.scope_key for item in staged}, {staged[0].scope_key})
        with self.factory() as session:
            states = session.scalars(select(CoreExecBuyAuthorityState)).all()
            outbox = session.scalars(
                select(CoreExecBuyAuthorityOutbox).order_by(
                    CoreExecBuyAuthorityOutbox.delivery_sequence.asc(),
                )
            ).all()
            self.assertEqual(len(states), 1)
            self.assertEqual(len(outbox), 2)
            self.assertEqual(
                [row.delivery_sequence for row in outbox],
                [1, 2],
            )

    async def test_outbox_flushes_in_durable_sequence_and_old_retry_cannot_regress_state(
        self,
    ) -> None:
        blocked = build_core_buy_authority_observation(
            route_authority=_route_authority(action='block_new_buy'),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        healthy_authority = _route_authority(
            observed_at=OBSERVED_AT + timedelta(seconds=2)
        )
        healthy_authority['evaluated_at'] = (
            OBSERVED_AT + timedelta(seconds=3)
        ).isoformat()
        healthy = build_core_buy_authority_observation(
            route_authority=healthy_authority,
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )

        first = self.bridge.stage(blocked)
        second = self.bridge.stage(healthy)
        self.bridge.stage(blocked)
        client = _FakeClient()
        await self.bridge.flush_scope(scope_key=second.scope_key, client=client)

        self.assertEqual(
            [payload['observation_id'] for payload in client.payloads],
            [blocked.observation_id, healthy.observation_id],
        )
        with self.factory() as session:
            state = session.get(CoreExecBuyAuthorityState, first.scope_key)
            self.assertIsNotNone(state)
            self.assertEqual(state.latest_observation_id, healthy.observation_id)
            self.assertEqual(state.latest_delivery_sequence, 2)

    async def test_newer_block_supersedes_older_pending_allow_across_restart(
        self,
    ) -> None:
        allow = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        block_authority = _route_authority(action='block_new_buy')
        block_authority['evaluated_at'] = (
            OBSERVED_AT + timedelta(seconds=1)
        ).isoformat()
        block = build_core_buy_authority_observation(
            route_authority=block_authority,
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )

        staged = self.bridge.stage(allow)
        self.bridge.stage(block)
        with self.factory() as session:
            allow_row = session.get(CoreExecBuyAuthorityOutbox, allow.observation_id)
            self.assertIsNotNone(allow_row)
            self.assertEqual(allow_row.status, 'superseded')

        # Simulate a restart over rows staged by the pre-supersession release.
        with self.factory.begin() as session:
            allow_row = session.get(CoreExecBuyAuthorityOutbox, allow.observation_id)
            assert allow_row is not None
            allow_row.status = 'pending'

        restarted = CoreExecBuyAuthorityBridge(
            session_context=self.bridge._session_context,
        )
        client = _FakeClient(fence_active=True)
        result = await restarted.flush_scope(
            scope_key=staged.scope_key,
            client=client,
        )

        self.assertEqual(
            [payload['action'] for payload in client.payloads],
            ['block_new_buy'],
        )
        self.assertFalse(result.dispatch_allowed)
        with self.factory() as session:
            allow_row = session.get(CoreExecBuyAuthorityOutbox, allow.observation_id)
            block_row = session.get(CoreExecBuyAuthorityOutbox, block.observation_id)
            self.assertEqual(allow_row.status, 'superseded')
            self.assertEqual(block_row.status, 'acknowledged')

    async def test_block_staged_between_selection_and_attempt_supersedes_allow(
        self,
    ) -> None:
        allow = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        block_authority = _route_authority(action='block_new_buy')
        block_authority['evaluated_at'] = (
            OBSERVED_AT + timedelta(seconds=1)
        ).isoformat()
        block = build_core_buy_authority_observation(
            route_authority=block_authority,
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )

        class ConcurrentStageBridge(CoreExecBuyAuthorityBridge):
            staged_block = False

            def _record_attempt(self, *, observation_id: str, now: datetime) -> bool:
                if observation_id == allow.observation_id and not self.staged_block:
                    self.staged_block = True
                    self.stage(block)
                return super()._record_attempt(
                    observation_id=observation_id,
                    now=now,
                )

        bridge = ConcurrentStageBridge(session_context=self.bridge._session_context)
        staged = bridge.stage(allow)
        client = _FakeClient(fence_active=True)
        result = await bridge.flush_scope(
            scope_key=staged.scope_key,
            client=client,
        )

        self.assertEqual(
            [payload['action'] for payload in client.payloads],
            ['block_new_buy'],
        )
        self.assertFalse(result.dispatch_allowed)

    async def test_block_staged_during_failed_allow_keeps_allow_superseded(
        self,
    ) -> None:
        allow = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        block_authority = _route_authority(action='block_new_buy')
        block_authority['evaluated_at'] = (
            OBSERVED_AT + timedelta(seconds=1)
        ).isoformat()
        block = build_core_buy_authority_observation(
            route_authority=block_authority,
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        staged = self.bridge.stage(allow)
        bridge = self.bridge

        class StageBlockThenFailClient(_FakeClient):
            async def post_buy_authority_observation(
                self,
                payload: dict[str, object],
            ) -> dict[str, object]:
                self.payloads.append(payload)
                bridge.stage(block)
                raise RuntimeError('allow delivery outcome unknown')

        failed = await self.bridge.flush_scope(
            scope_key=staged.scope_key,
            client=StageBlockThenFailClient(),
        )

        self.assertFalse(failed.dispatch_allowed)
        with self.factory() as session:
            allow_row = session.get(CoreExecBuyAuthorityOutbox, allow.observation_id)
            block_row = session.get(CoreExecBuyAuthorityOutbox, block.observation_id)
            self.assertEqual(allow_row.status, 'superseded')
            self.assertEqual(block_row.status, 'pending')

        recovery_client = _FakeClient(fence_active=True)
        recovered = await self.bridge.flush_scope(
            scope_key=staged.scope_key,
            client=recovery_client,
        )
        self.assertEqual(
            [payload['action'] for payload in recovery_client.payloads],
            ['block_new_buy'],
        )
        self.assertFalse(recovered.dispatch_allowed)

    async def test_concurrent_flush_cannot_deliver_an_older_block_after_newer_allow(
        self,
    ) -> None:
        blocked = build_core_buy_authority_observation(
            route_authority=_route_authority(action='block_new_buy'),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        staged_block = self.bridge.stage(blocked)
        block_started = asyncio.Event()
        release_delayed_block = asyncio.Event()
        remote = {'action': None}

        class ControlledClient(_FakeClient):
            def __init__(self, *, delay_block: bool) -> None:
                super().__init__()
                self.delay_block = delay_block

            async def post_buy_authority_observation(
                self,
                payload: dict[str, object],
            ) -> dict[str, object]:
                if self.delay_block and payload['action'] == 'block_new_buy':
                    block_started.set()
                    await release_delayed_block.wait()
                remote['action'] = payload['action']
                response = await super().post_buy_authority_observation(payload)
                response['fence_active'] = payload['action'] != 'allow'
                return response

        delayed_flush = asyncio.create_task(
            self.bridge.flush_scope(
                scope_key=staged_block.scope_key,
                client=ControlledClient(delay_block=True),
            )
        )
        await block_started.wait()

        healthy_authority = _route_authority(
            observed_at=OBSERVED_AT + timedelta(seconds=2)
        )
        healthy_authority['evaluated_at'] = (
            OBSERVED_AT + timedelta(seconds=3)
        ).isoformat()
        healthy = build_core_buy_authority_observation(
            route_authority=healthy_authority,
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        self.bridge.stage(healthy)
        newer = await self.bridge.flush_scope(
            scope_key=staged_block.scope_key,
            client=ControlledClient(delay_block=False),
        )
        self.assertFalse(newer.dispatch_allowed)
        self.assertIsNone(remote['action'])

        release_delayed_block.set()
        delayed = await delayed_flush

        self.assertEqual(remote['action'], 'allow')
        self.assertTrue(delayed.dispatch_allowed)

    def test_conflicting_same_observation_id_fails_closed(self) -> None:
        observation = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        staged = self.bridge.stage(observation)
        with self.factory.begin() as session:
            row = session.get(CoreExecBuyAuthorityOutbox, staged.observation_id)
            assert row is not None
            row.material_fingerprint = 'f' * 64

        with self.assertRaises(CoreExecBuyAuthorityConflictError):
            self.bridge.stage(observation)

        with self.factory() as session:
            state = session.get(CoreExecBuyAuthorityState, staged.scope_key)
            self.assertIsNotNone(state)
            self.assertEqual(state.delivery_status, 'conflict')
            self.assertIsNotNone(state.last_error)

    async def test_flush_requires_matching_ack_before_authorizing_dispatch(self) -> None:
        observation = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        staged = self.bridge.stage(observation)
        client = _FakeClient()

        result = await self.bridge.flush_scope(
            scope_key=staged.scope_key,
            client=client,
        )

        self.assertTrue(result.dispatch_allowed)
        self.assertIsInstance(result.ack, CoreBuyAuthorityAckV1)
        with self.factory() as session:
            outbox = session.get(CoreExecBuyAuthorityOutbox, staged.observation_id)
            state = session.get(CoreExecBuyAuthorityState, staged.scope_key)
            self.assertEqual(outbox.status, 'acknowledged')
            self.assertEqual(state.delivery_status, 'acknowledged')
            self.assertFalse(state.remote_fence_active)

    def test_ack_schema_version_is_required_and_exact(self) -> None:
        base_ack = {
            'schema_version': 'core_exec_buy_authority_ack_v1',
            'status': 'applied',
            'observation_id': 'buy-authority-v1:' + ('1' * 64),
            'material_fingerprint': '1' * 64,
            'scope_fingerprint': 'a' * 64,
            'fence_active': False,
            'projection_pending': False,
            'healthy_observation_count': 0,
            'changed_item_count': 0,
            'accepted_source_observed_at': OBSERVED_AT,
        }

        for schema_version in (None, 'core_exec_buy_authority_ack_v2'):
            with self.subTest(schema_version=schema_version):
                candidate = dict(base_ack)
                if schema_version is None:
                    candidate.pop('schema_version')
                else:
                    candidate['schema_version'] = schema_version
                with self.assertRaises(ValueError):
                    CoreBuyAuthorityAckV1.model_validate(candidate)

    def test_ack_persistence_locks_scope_state_before_outbox(self) -> None:
        observation = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        self.bridge.stage(observation)
        ack = CoreBuyAuthorityAckV1.model_validate(
            {
                'schema_version': 'core_exec_buy_authority_ack_v1',
                'status': 'applied',
                'observation_id': observation.observation_id,
                'material_fingerprint': observation.material_fingerprint,
                'scope_fingerprint': 'a' * 64,
                'fence_active': False,
                'projection_pending': False,
                'healthy_observation_count': 0,
                'changed_item_count': 0,
                'accepted_source_observed_at': observation.source_observed_at,
            }
        )
        selected_tables: list[str] = []

        def capture_selects(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            if not statement.lstrip().upper().startswith('SELECT'):
                return
            if 'FROM core_exec_buy_authority_states' in statement:
                selected_tables.append('state')
            elif 'FROM core_exec_buy_authority_outbox' in statement:
                selected_tables.append('outbox')

        event.listen(self.engine, 'before_cursor_execute', capture_selects)
        try:
            self.bridge._record_ack(
                observation=observation,
                ack=ack,
                now=OBSERVED_AT + timedelta(seconds=1),
            )
        finally:
            event.remove(self.engine, 'before_cursor_execute', capture_selects)

        self.assertGreaterEqual(len(selected_tables), 2)
        self.assertEqual(selected_tables[:2], ['state', 'outbox'])

    async def test_delivery_failure_stays_pending_and_blocks_dispatch(self) -> None:
        observation = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        staged = self.bridge.stage(observation)

        result = await self.bridge.flush_scope(
            scope_key=staged.scope_key,
            client=_FakeClient(fail=True),
        )

        self.assertFalse(result.dispatch_allowed)
        with self.factory() as session:
            outbox = session.get(CoreExecBuyAuthorityOutbox, staged.observation_id)
            state = session.get(CoreExecBuyAuthorityState, staged.scope_key)
            self.assertEqual(outbox.status, 'pending')
            self.assertEqual(outbox.attempt_count, 1)
            self.assertEqual(state.delivery_status, 'pending')
            self.assertIsNotNone(state.last_error)

    async def test_block_or_remote_recovery_fence_never_synthesizes_allow(self) -> None:
        blocked = build_core_buy_authority_observation(
            route_authority=_route_authority(action='block_new_buy'),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        staged = self.bridge.stage(blocked)
        block_result = await self.bridge.flush_scope(
            scope_key=staged.scope_key,
            client=_FakeClient(fence_active=True),
        )
        self.assertFalse(block_result.dispatch_allowed)

    async def test_changed_remote_scope_fingerprint_fails_closed(self) -> None:
        first = build_core_buy_authority_observation(
            route_authority=_route_authority(),
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        staged_first = self.bridge.stage(first)
        accepted = await self.bridge.flush_scope(
            scope_key=staged_first.scope_key,
            client=_FakeClient(scope_fingerprint='a' * 64),
        )
        self.assertTrue(accepted.dispatch_allowed)

        second_authority = _route_authority(
            observed_at=OBSERVED_AT + timedelta(seconds=2)
        )
        second_authority['evaluated_at'] = (
            OBSERVED_AT + timedelta(seconds=3)
        ).isoformat()
        second = build_core_buy_authority_observation(
            route_authority=second_authority,
            runtime_mode='live',
            account_reference=ACCOUNT_REFERENCE,
        )
        staged_second = self.bridge.stage(second)
        changed_scope = await self.bridge.flush_scope(
            scope_key=staged_second.scope_key,
            client=_FakeClient(scope_fingerprint='b' * 64),
        )

        self.assertFalse(changed_scope.dispatch_allowed)
        with self.factory() as session:
            state = session.get(CoreExecBuyAuthorityState, staged_second.scope_key)
            self.assertIsNotNone(state)
            self.assertEqual(state.delivery_status, 'conflict')
            self.assertTrue(state.remote_fence_active)

    async def test_candidate_empty_sync_still_delivers_configured_route_scope(self) -> None:
        client = _FakeClient()
        service = SignalDeciderExecIntegrationService(client=client)  # type: ignore[arg-type]
        target = NextOpenAccountTarget(
            label='primary',
            account_reference=ACCOUNT_REFERENCE,
            budget_krw=Decimal('1'),
        )
        with (
            patch(
                'app.orders.exec_integration.async_get_market_state',
                new=AsyncMock(return_value=SimpleNamespace()),
            ),
            patch(
                'app.orders.exec_integration._load_live_market_snapshot_context',
                return_value=({}, False),
            ),
            patch(
                'app.orders.exec_integration._current_route_live_market_buy_authority',
                return_value=_route_authority(),
            ),
            patch.object(
                __import__(
                    'app.orders.exec_integration',
                    fromlist=['signal_decider_next_open_execution_service'],
                ).signal_decider_next_open_execution_service,
                '_parse_account_targets',
                return_value=([target], []),
            ),
            patch(
                'app.orders.exec_integration.core_exec_buy_authority_bridge',
                self.bridge,
            ),
        ):
            result = await service._synchronize_open_buy_authority(
                target_trade_date=date(2026, 9, 11),
                account_reference=None,
                integration={'runtime_mode': 'live'},
            )

        self.assertTrue(result['accounts'][ACCOUNT_REFERENCE]['dispatch_allowed'])
        self.assertEqual(len(client.payloads), 1)
        self.assertEqual(
            client.payloads[0]['scope']['account_reference'],
            ACCOUNT_REFERENCE,
        )

    async def test_open_dispatch_never_posts_plan_when_authority_ack_blocks(
        self,
    ) -> None:
        client = _FakeClient()
        service = SignalDeciderExecIntegrationService(client=client)  # type: ignore[arg-type]
        row = SimpleNamespace(
            planned_side='buy',
            account_reference=ACCOUNT_REFERENCE,
            decision_agent_name='signal-decider',
            submission_payload={},
        )
        blocked_sync = {
            'applies': True,
            'accounts': {
                ACCOUNT_REFERENCE: {
                    'dispatch_allowed': False,
                    'errors': ['delivery failed'],
                }
            },
            'errors': [],
        }
        calendar_day = SimpleNamespace(
            calendar_source='exchange_calendars',
            calendar_source_status='authoritative',
            calendar_source_error=None,
        )
        plan_view = {'summary': {}, 'items': []}
        with (
            patch.object(
                service,
                'integration_status',
                return_value={
                    'mode': 'dispatch',
                    'runtime_mode': 'live',
                    'config_status': 'ready',
                    'issues': [],
                },
            ),
            patch.object(
                service,
                '_reconcile_pending_dispatch_write_ahead',
                new=AsyncMock(return_value=[]),
            ),
            patch.object(
                service,
                '_flush_pending_exec_revocations',
                new=AsyncMock(return_value=[]),
            ),
            patch.object(
                service,
                '_synchronize_open_buy_authority',
                new=AsyncMock(return_value=blocked_sync),
            ) as synchronize,
            patch.object(
                service,
                '_load_next_open_rows',
                return_value=[row],
            ),
            patch.object(
                service,
                '_fence_unproven_remote_acceptance',
                return_value=False,
            ),
            patch.object(service, '_load_snapshots', return_value={}),
            patch.object(
                service,
                '_group_rows_by_dispatch_scope',
                return_value={
                    (ACCOUNT_REFERENCE, 'signal-decider', 'kis_krx_krw'): [row]
                },
            ),
            patch.object(service, '_row_is_dispatchable', return_value=True),
            patch(
                'app.orders.exec_integration.async_normalize_next_open_target_trade_date',
                new=AsyncMock(return_value=date(2026, 9, 11)),
            ),
            patch(
                'app.orders.exec_integration.async_get_krx_calendar_day',
                new=AsyncMock(return_value=calendar_day),
            ),
            patch(
                'app.orders.exec_integration._async_previous_trading_day',
                new=AsyncMock(return_value=date(2026, 9, 10)),
            ),
            patch(
                'app.orders.exec_integration.settings.signal_decider_next_open_enabled',
                True,
            ),
            patch.object(
                __import__(
                    'app.orders.exec_integration',
                    fromlist=['signal_decider_next_open_execution_service'],
                ).signal_decider_next_open_execution_service,
                'list_plan_items',
                return_value=plan_view,
            ),
        ):
            result = await service.dispatch_next_open_plan(
                target_trade_date=date(2026, 9, 11),
                refresh_plan=False,
                dry_run=False,
                force=True,
            )

        synchronize.assert_awaited_once()
        self.assertEqual(client.execution_plan_payloads, [])
        self.assertEqual(
            result['dispatch']['groups'][0]['reason'],
            'core_exec_buy_authority_not_acknowledged',
        )

    async def test_open_dispatch_synchronizes_authority_after_final_plan_refresh(
        self,
    ) -> None:
        client = _FakeClient()
        service = SignalDeciderExecIntegrationService(client=client)  # type: ignore[arg-type]
        calendar_day = SimpleNamespace(
            calendar_source='exchange_calendars',
            calendar_source_status='authoritative',
            calendar_source_error=None,
        )
        fresh_sync = {
            'applies': True,
            'route_action': 'allow',
            'route_status': 'available',
            'source_observed_at': '2026-09-11T09:04:00+09:00',
            'accounts': {},
            'errors': [],
        }
        events: list[str] = []

        def refresh_plan_items(**kwargs: object) -> dict[str, object]:
            events.append(f"refresh:{kwargs['trigger']}")
            return {
                'enrichment_preflight': {
                    'status': 'ready',
                    'allowed': True,
                    'failed_categories': [],
                }
            }

        async def synchronize_authority(**_kwargs: object) -> dict[str, object]:
            events.append('authority')
            return fresh_sync

        async def reconcile_write_ahead(**kwargs: object) -> list[object]:
            authority = kwargs['buy_authority_sync']
            events.append(f"reconcile:{authority.get('route_status')}")
            return []

        async def flush_revocations(**_kwargs: object) -> list[dict[str, object]]:
            events.append('revocations')
            return [
                {
                    'status': 'revoked',
                    'replacement_actionable': True,
                }
            ]

        next_open_service = __import__(
            'app.orders.exec_integration',
            fromlist=['signal_decider_next_open_execution_service'],
        ).signal_decider_next_open_execution_service
        with (
            patch.object(
                service,
                'integration_status',
                return_value={
                    'mode': 'dispatch',
                    'runtime_mode': 'live',
                    'config_status': 'ready',
                    'issues': [],
                },
            ),
            patch.object(
                service,
                '_synchronize_open_buy_authority',
                side_effect=synchronize_authority,
            ) as synchronize,
            patch.object(
                service,
                '_reconcile_pending_dispatch_write_ahead',
                side_effect=reconcile_write_ahead,
            ),
            patch.object(
                service,
                '_flush_pending_exec_revocations',
                side_effect=flush_revocations,
            ),
            patch.object(
                service,
                '_reconcile_pending_core_activations',
                new=AsyncMock(return_value=[]),
            ),
            patch.object(service, '_load_next_open_rows', return_value=[]),
            patch.object(
                service,
                '_fence_unproven_remote_acceptance',
                return_value=False,
            ),
            patch.object(service, '_load_snapshots', return_value={}),
            patch.object(service, '_group_rows_by_dispatch_scope', return_value={}),
            patch.object(
                next_open_service,
                'refresh_plan_items',
                side_effect=refresh_plan_items,
            ),
            patch.object(
                next_open_service,
                'list_plan_items',
                return_value={'summary': {}, 'items': []},
            ),
            patch(
                'app.orders.exec_integration.async_normalize_next_open_target_trade_date',
                new=AsyncMock(return_value=date(2026, 9, 11)),
            ),
            patch(
                'app.orders.exec_integration.async_get_krx_calendar_day',
                new=AsyncMock(return_value=calendar_day),
            ),
            patch(
                'app.orders.exec_integration._async_previous_trading_day',
                new=AsyncMock(return_value=date(2026, 9, 10)),
            ),
            patch(
                'app.orders.exec_integration.settings.signal_decider_next_open_enabled',
                True,
            ),
        ):
            result = await service.dispatch_next_open_plan(
                target_trade_date=date(2026, 9, 11),
                refresh_plan=True,
                dry_run=False,
                force=True,
                trigger='post_validation',
            )

        synchronize.assert_awaited_once()
        self.assertEqual(
            events,
            [
                'reconcile:plan_refresh_pending',
                'refresh:post_validation_plan_refresh',
                'revocations',
                'refresh:post_validation_revocation_ack_refresh',
                'authority',
                'reconcile:available',
            ],
        )
        self.assertEqual(
            result['buy_authority_sync']['source_observed_at'],
            '2026-09-11T09:04:00+09:00',
        )
        self.assertEqual(client.execution_plan_payloads, [])

    async def test_calendar_early_return_still_synchronizes_deferred_authority(
        self,
    ) -> None:
        client = _FakeClient()
        service = SignalDeciderExecIntegrationService(client=client)  # type: ignore[arg-type]
        events: list[str] = []
        calendar_day = SimpleNamespace(
            calendar_source='exchange_calendars',
            calendar_source_status='fallback',
            calendar_source_error='provider unavailable',
        )

        async def synchronize_authority(**_kwargs: object) -> dict[str, object]:
            events.append('authority')
            return {
                'applies': True,
                'route_action': 'block_new_buy',
                'route_status': 'unavailable',
                'source_observed_at': None,
                'accounts': {},
                'errors': [],
            }

        async def reconcile_write_ahead(**kwargs: object) -> list[object]:
            authority = kwargs['buy_authority_sync']
            events.append(f"reconcile:{authority.get('route_status')}")
            return []

        next_open_service = __import__(
            'app.orders.exec_integration',
            fromlist=['signal_decider_next_open_execution_service'],
        ).signal_decider_next_open_execution_service
        with (
            patch.object(
                service,
                'integration_status',
                return_value={
                    'mode': 'dispatch',
                    'runtime_mode': 'live',
                    'config_status': 'ready',
                    'issues': [],
                },
            ),
            patch.object(
                service,
                '_synchronize_open_buy_authority',
                side_effect=synchronize_authority,
            ) as synchronize,
            patch.object(
                service,
                '_reconcile_pending_dispatch_write_ahead',
                side_effect=reconcile_write_ahead,
            ),
            patch.object(
                next_open_service,
                'refresh_plan_items',
            ) as refresh_plan,
            patch.object(
                next_open_service,
                'list_plan_items',
                return_value={'summary': {}, 'items': []},
            ),
            patch(
                'app.orders.exec_integration.async_normalize_next_open_target_trade_date',
                new=AsyncMock(return_value=date(2026, 9, 11)),
            ),
            patch(
                'app.orders.exec_integration.async_get_krx_calendar_day',
                new=AsyncMock(return_value=calendar_day),
            ),
            patch(
                'app.orders.exec_integration._async_previous_trading_day',
                new=AsyncMock(return_value=date(2026, 9, 10)),
            ),
            patch(
                'app.orders.exec_integration.settings.signal_decider_next_open_enabled',
                True,
            ),
        ):
            result = await service.dispatch_next_open_plan(
                target_trade_date=date(2026, 9, 11),
                refresh_plan=True,
                dry_run=False,
                force=False,
                trigger='calendar_fallback',
            )

        synchronize.assert_awaited_once()
        refresh_plan.assert_not_called()
        self.assertEqual(
            events,
            ['reconcile:plan_refresh_pending', 'authority'],
        )
        self.assertEqual(result['summary']['reason'], 'calendar_unavailable')
        self.assertEqual(client.execution_plan_payloads, [])


if __name__ == '__main__':
    unittest.main()
