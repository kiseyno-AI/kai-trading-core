import asyncio
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import copy
from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db_models import SignalDeciderExecutionPlanItem
from app.orders import core_quantity_refresh
from app.orders.core_quantity_refresh import CoreQuantityRefreshService


NOW = datetime(2026, 11, 11, 0, 0, tzinfo=timezone.utc)


def runtime_snapshot(now=NOW):
    return {'core_quantity_account': {
        'as_of': now.isoformat(), 'account_submission_uncertain': False,
        'account_local_unacknowledged_buy_notional_krw': '0',
        'limits': {'max_symbol_budget_krw': '3000000', 'max_total_budget_krw': '9000000',
                   'risk_fraction': '0.015', 'max_symbol_fraction': '0.30'},
    }}


class RuntimeClient:
    async def get_runtime_status(self):
        return runtime_snapshot()


def plan_row(key=1, **changes):
    return SimpleNamespace(
        id=key, account_reference='KIS:test-00', target_trade_date=date(2026, 11, 11),
        execution_window='next_open', account_label=None, allocation_budget_krw=1000,
        source_market_date=date(2026, 11, 10), decision_snapshot_id=key,
        decision_agent_name='signal-decider', symbol='005930', policy_version='test',
        planned_side='buy', broker_name='kis', estimated_reference_price=100,
        planned_order_type='market', planned_time_in_force='day', planned_quantity=None,
        estimated_notional_krw=None, client_order_id=None,
        validation_payload={}, traceability_payload={}, submission_payload={}, **changes,
    )


def snapshot(key=1):
    return SimpleNamespace(
        id=key, agent_name='signal-decider', target_price=130, stop_loss_price=95,
        confidence_score_10=10, expected_return_pct=30, traceability_payload={},
        response_payload={'trigger_price': 100, 'chase_limit_price': 100},
    )


class Accounts:
    now = NOW

    async def get_account_summary(self, *, account_reference):
        return {'account_reference': account_reference, 'as_of': self.now.isoformat(),
                'valuation': {'total_asset_value': 100000}}

    async def get_positions(self, *, account_reference):
        return {'account_reference': account_reference, 'as_of': self.now.isoformat(),
                'completeness': {'status': 'complete'}, 'items': []}

    async def get_buying_power(self, **kwargs):
        return {'account_reference': kwargs['account_reference'], 'as_of': self.now.isoformat(),
                'buying_power': {'orderable_cash': 10000}}


class Integration:
    client = RuntimeClient()

    def _row_is_dispatchable(self, row):
        return True

    def _accepted_execution_plan_id_for_rows(self, rows):
        return None


def test_initial_preview_sizes_from_real_account_inputs_without_mutating_price_contract():
    row = plan_row()
    service = CoreQuantityRefreshService(Integration(), account_service=Accounts())
    result = asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    policy = row.submission_payload['core_quantity_sizing']['initial_policy']
    assert result['prepared_count'] == 1
    assert policy['type'] == 'core_sized'
    assert policy['target_quantity'] == 100
    assert policy['revision'] == 1
    assert datetime.fromisoformat(policy['valid_until'].replace('Z', '+00:00')) == NOW + timedelta(hours=2)
    assert len(policy['sizing_inputs']['price_contract_digest']) == 64
    assert row.validation_payload == {}
    assert row.traceability_payload == {}


def test_stale_account_snapshot_yields_observation_policy_without_inventing_cash():
    row = plan_row()
    service = CoreQuantityRefreshService(Integration(), account_service=Accounts())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW + timedelta(minutes=6), dry_run=True))
    state = row.submission_payload['core_quantity_sizing']
    assert state['initial_policy']['target_quantity'] == 0
    assert 'account_snapshot_unavailable' in state['initial_policy']['sizing_inputs']['reasons']


def test_current_preopen_quantity_remains_valid_when_the_market_opens(monkeypatch):
    accounts = Accounts()
    accounts.now = NOW - timedelta(minutes=15)
    row = plan_row()
    integration = Integration()

    async def runtime():
        return runtime_snapshot(accounts.now)

    monkeypatch.setattr(integration.client, 'get_runtime_status', runtime)
    service = CoreQuantityRefreshService(integration, account_service=accounts)
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=accounts.now, dry_run=True))
    policy = row.submission_payload['core_quantity_sizing']['initial_policy']
    assert policy['target_quantity'] == 100
    assert datetime.fromisoformat(policy['valid_until'].replace('Z', '+00:00')) > NOW


def test_plan_reference_change_during_collection_cannot_publish_old_quantity(persisted_runtime, monkeypatch):
    integration, accounts, _ = persisted_runtime
    original_buying_power = accounts.get_buying_power

    async def changed_buying_power(**kwargs):
        with core_quantity_refresh.session_scope() as session:
            session.get(SignalDeciderExecutionPlanItem, 1).estimated_reference_price = 101
        return await original_buying_power(**kwargs)

    monkeypatch.setattr(accounts, 'get_buying_power', changed_buying_power)
    rows = integration._load_next_open_rows(target_trade_date=NOW.date())
    service = CoreQuantityRefreshService(integration, account_service=accounts)
    result = asyncio.run(service.prepare_initial(rows, {1: snapshot()}, now=NOW))
    assert result['prepared_count'] == 0
    assert result['items'][0]['reason_code'] == 'quantity_source_authority_changed'
    stored = integration._load_next_open_rows(target_trade_date=NOW.date())[0]
    assert 'core_quantity_sizing' not in (stored.submission_payload or {})


@pytest.fixture
def persisted_runtime(monkeypatch):
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    SignalDeciderExecutionPlanItem.__table__.create(engine)

    @contextmanager
    def local_session():
        with Session(engine, expire_on_commit=False) as session:
            yield session
            session.commit()

    monkeypatch.setattr(core_quantity_refresh, 'session_scope', local_session)
    row = SignalDeciderExecutionPlanItem(**vars(plan_row()))
    with local_session() as session:
        session.add(row)

    class Client:
        calls = []
        lose_next_ack = False
        execution = {'target_quantity': 100, 'revision': 1, 'filled_quantity': 20,
                     'open_quantity': 10, 'remaining_quantity': 70, 'submission_uncertain': False}

        async def get_runtime_status(self):
            return runtime_snapshot(accounts.now)

        async def list_execution_plans(self, *, external_plan_id):
            return {'items': [{'external_plan_id': external_plan_id, 'execution_plan_id': 'remote-1',
                               'local_unacknowledged_buy_notional_krw': '0',
                               'account_submission_uncertain': False,
                               'item_states': [{'core_item_key': '1:entry', 'core_quantity_state': self.execution}]}]}

        async def post_execution_plan_quantities(self, payload):
            self.calls.append(copy.deepcopy(payload))
            request = payload['items'][0]
            self.execution = {**self.execution, 'revision': request['revision'], 'target_quantity': request['target_quantity']}
            if self.lose_next_ack:
                self.lose_next_ack = False
                raise TimeoutError('simulated lost ACK after remote commit')
            return {'items': [{'core_item_key': '1:entry', 'execution_plan_id': 'remote-1',
                               'execution_item_id': 'entry-1', 'status': 'updated',
                               'revision': request['revision'], 'target_quantity': request['target_quantity']}]}

    class StoredIntegration(Integration):
        client = Client()

        def _load_next_open_rows(self, *, target_trade_date):
            with local_session() as session:
                rows = session.scalars(select(SignalDeciderExecutionPlanItem)).all()
                for row in rows:
                    session.expunge(row)
                return rows

        def _load_snapshots(self, rows):
            return {1: snapshot()}

        def _accepted_execution_plan_id_for_rows(self, rows):
            return 'remote-1' if (rows[0].submission_payload or {}).get('exec_dispatch') else None

    async def market(now):
        return SimpleNamespace(state='OPEN', is_trading_day=True, calendar_source='kis_chk_holiday',
                               calendar_source_status='ok')

    monkeypatch.setattr(core_quantity_refresh, 'async_get_market_state', market)
    integration = StoredIntegration()
    accounts = Accounts()

    def accept():
        with local_session() as session:
            current = session.get(SignalDeciderExecutionPlanItem, 1)
            current.submission_payload = {**current.submission_payload, 'exec_dispatch': {
                'plan_id': 'external-1', 'dispatch_status': 'accepted',
                'item_links': [{'side': 'buy', 'core_item_key': '1:entry'}],
            }}

    yield integration, accounts, accept
    engine.dispose()


def test_two_hour_refresh_survives_restart_and_changes_only_quantity(persisted_runtime):
    integration, accounts, accept = persisted_runtime
    rows = integration._load_next_open_rows(target_trade_date=NOW.date())
    service = CoreQuantityRefreshService(integration, accounts)
    asyncio.run(service.prepare_initial(rows, {1: snapshot()}, now=NOW))
    initial = copy.deepcopy(rows[0].submission_payload['core_quantity_sizing']['initial_policy'])
    accept()
    assert asyncio.run(service.refresh_if_due(NOW.date(), now=NOW + timedelta(minutes=30)))['skipped_count'] == 1
    accounts.now = NOW + timedelta(hours=2)
    result = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now))
    assert result['updated_count'] == 1
    request = integration.client.calls[0]['items'][0]
    assert request['target_quantity'] == 130
    assert request['expected_revision'] == 1
    assert request['revision'] == 2
    assert not {'trigger_price', 'chase_limit_price', 'target_price', 'stop_loss_price'} & request.keys()
    stored = integration._load_next_open_rows(target_trade_date=NOW.date())[0]
    assert stored.submission_payload['core_quantity_sizing']['initial_policy'] == initial
    restarted = CoreQuantityRefreshService(integration, accounts)
    assert asyncio.run(restarted.refresh_if_due(NOW.date(), now=accounts.now + timedelta(minutes=5)))['skipped_count'] == 1


def test_lost_ack_retries_identical_write_ahead_request_after_restart(persisted_runtime):
    integration, accounts, accept = persisted_runtime
    service = CoreQuantityRefreshService(integration, accounts)
    asyncio.run(service.prepare_initial(integration._load_next_open_rows(target_trade_date=NOW.date()), {1: snapshot()}, now=NOW))
    accept()
    integration.client.lose_next_ack = True
    accounts.now = NOW + timedelta(hours=2)
    result = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now))
    assert result['failed_count'] == 1
    assert len(integration.client.calls) == 1
    restarted = CoreQuantityRefreshService(integration, accounts)
    result = asyncio.run(restarted.refresh_if_due(NOW.date(), now=accounts.now + timedelta(seconds=10)))
    assert result['updated_count'] == 1
    assert integration.client.calls[1] == integration.client.calls[0]


def test_prepared_initial_authority_is_never_regenerated_by_later_account_reads():
    row = plan_row()
    service = CoreQuantityRefreshService(Integration(), Accounts())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    initial = copy.deepcopy(row.submission_payload)
    result = asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW + timedelta(minutes=6), dry_run=True))
    assert result['prepared_count'] == 0
    assert row.submission_payload == initial


def test_first_open_refresh_rechecks_a_preopen_allocation_before_two_hours(persisted_runtime):
    integration, accounts, accept = persisted_runtime
    service = CoreQuantityRefreshService(integration, accounts)
    accounts.now = NOW - timedelta(minutes=30)
    rows = integration._load_next_open_rows(target_trade_date=NOW.date())
    asyncio.run(service.prepare_initial(rows, {1: snapshot()}, now=accounts.now))
    assert rows[0].submission_payload['core_quantity_sizing']['calculated_phase'] == 'preopen'
    accept()
    accounts.now = NOW
    result = asyncio.run(service.refresh_if_due(NOW.date(), now=NOW))
    assert result['updated_count'] == 1
    policy = integration.client.calls[0]['items'][0]
    assert datetime.fromisoformat(policy['valid_until'].replace('Z', '+00:00')) == NOW + timedelta(hours=2)


def test_uncertain_submission_freezes_increases_without_changing_the_price_contract(persisted_runtime):
    integration, accounts, accept = persisted_runtime
    service = CoreQuantityRefreshService(integration, accounts)
    asyncio.run(service.prepare_initial(integration._load_next_open_rows(target_trade_date=NOW.date()), {1: snapshot()}, now=NOW))
    accept()
    integration.client.execution['submission_uncertain'] = True
    accounts.now = NOW + timedelta(hours=2)
    result = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now))
    assert result['updated_count'] == 1
    request = integration.client.calls[0]['items'][0]
    assert request['target_quantity'] == 100
    assert request['sizing_inputs']['increases_frozen_submission_uncertain'] is True


def test_missing_current_account_evidence_withdraws_only_unfilled_buy_authority(persisted_runtime):
    integration, accounts, accept = persisted_runtime
    service = CoreQuantityRefreshService(integration, accounts)
    asyncio.run(service.prepare_initial(integration._load_next_open_rows(target_trade_date=NOW.date()), {1: snapshot()}, now=NOW))
    accept()
    result = asyncio.run(service.refresh_if_due(NOW.date(), now=NOW + timedelta(hours=2)))
    assert result['updated_count'] == 1
    request = integration.client.calls[0]['items'][0]
    assert request['target_quantity'] == 20
    assert 'account_snapshot_unavailable' in request['sizing_inputs']['reasons']


def test_closed_session_and_pre_eight_preopen_do_not_read_or_update_execution_state(persisted_runtime, monkeypatch):
    integration, accounts, accept = persisted_runtime
    service = CoreQuantityRefreshService(integration, accounts)
    asyncio.run(service.prepare_initial(integration._load_next_open_rows(target_trade_date=NOW.date()), {1: snapshot()}, now=NOW))
    accept()

    async def closed(now):
        return SimpleNamespace(state='CLOSED', is_trading_day=True, calendar_source='kis_chk_holiday',
                               calendar_source_status='ok')

    monkeypatch.setattr(core_quantity_refresh, 'async_get_market_state', closed)
    assert asyncio.run(service.refresh_if_due(NOW.date(), now=NOW + timedelta(hours=7)))['status'] == 'outside_refresh_session'

    async def preopen(now):
        return SimpleNamespace(state='PREOPEN', is_trading_day=True, calendar_source='kis_chk_holiday',
                               calendar_source_status='ok')

    monkeypatch.setattr(core_quantity_refresh, 'async_get_market_state', preopen)
    assert asyncio.run(service.refresh_if_due(NOW.date(), now=NOW - timedelta(hours=2)))['status'] == 'outside_refresh_session'
    assert integration.client.calls == []


def test_unsupported_us_symbol_does_not_receive_a_krx_quantity_policy():
    row = plan_row()
    row.symbol = 'AAPL'
    service = CoreQuantityRefreshService(Integration(), Accounts())
    result = asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    assert result['prepared_count'] == 0
    assert row.submission_payload == {}


def test_stored_server_technical_snapshot_supplies_volatility_to_quantity_sizing():
    row = plan_row()
    snap = snapshot()
    snap.request_payload = {'symbol_day_context': {'technical_snapshot': {'atr_pct_14': 20}}}
    service = CoreQuantityRefreshService(Integration(), Accounts())
    asyncio.run(service.prepare_initial([row], {1: snap}, now=NOW, dry_run=True))
    assert row.submission_payload['core_quantity_sizing']['policy']['target_quantity'] == 75


def test_account_responses_timestamped_after_async_collection_started_are_fresh():
    class DelayedAccounts(Accounts):
        now = NOW + timedelta(milliseconds=10)

        async def get_account_summary(self, **kwargs):
            await asyncio.sleep(0.02)
            return await super().get_account_summary(**kwargs)

        async def get_positions(self, **kwargs):
            await asyncio.sleep(0.02)
            return await super().get_positions(**kwargs)

        async def get_buying_power(self, **kwargs):
            await asyncio.sleep(0.02)
            return await super().get_buying_power(**kwargs)

    row = plan_row()
    service = CoreQuantityRefreshService(Integration(), DelayedAccounts())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    assert row.submission_payload['core_quantity_sizing']['policy']['target_quantity'] == 100


def test_collection_elapsed_time_does_not_admit_an_unbounded_future_account_snapshot():
    accounts = Accounts()
    accounts.now = NOW + timedelta(hours=1)
    row = plan_row()
    service = CoreQuantityRefreshService(Integration(), accounts)
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    policy = row.submission_payload['core_quantity_sizing']['policy']
    assert policy['target_quantity'] == 0
    assert policy['sizing_inputs']['account_error'] == 'quantity_account_snapshot_stale_or_unbound'


@pytest.mark.parametrize('lost_reduction_ack', [False, True])
def test_one_due_lane_refreshes_the_whole_account_cohort_to_preserve_cash_allocation(persisted_runtime, monkeypatch, lost_reduction_ack):
    integration, accounts, _ = persisted_runtime
    second = plan_row(2)
    second.symbol = '000660'
    second.decision_agent_name = 'short-term-decider'
    with core_quantity_refresh.session_scope() as session:
        session.add(SignalDeciderExecutionPlanItem(**vars(second)))
    second_snapshot = snapshot(2)
    second_snapshot.agent_name = 'short-term-decider'
    snapshots = {1: snapshot(), 2: second_snapshot}
    monkeypatch.setattr(integration, '_load_snapshots', lambda rows: snapshots)
    monkeypatch.setattr(integration, '_accepted_execution_plan_id_for_rows', lambda rows: (
        f'remote-{rows[0].id}' if (rows[0].submission_payload or {}).get('exec_dispatch') else None
    ))
    cash = 10000
    original_buying_power = accounts.get_buying_power

    async def buying_power(**kwargs):
        payload = await original_buying_power(**kwargs)
        payload['buying_power']['orderable_cash'] = cash
        return payload

    monkeypatch.setattr(accounts, 'get_buying_power', buying_power)
    service = CoreQuantityRefreshService(integration, accounts)
    rows = integration._load_next_open_rows(target_trade_date=NOW.date())
    assert asyncio.run(service.prepare_initial(rows, snapshots, now=NOW))['prepared_count'] == 2
    states = {key: {'target_quantity': 50, 'revision': 1, 'filled_quantity': 0,
                    'open_quantity': 0, 'remaining_quantity': 50, 'submission_uncertain': False}
              for key in (1, 2)}
    if lost_reduction_ack:
        states[1]['filled_quantity'] = 30
    with core_quantity_refresh.session_scope() as session:
        for row in session.scalars(select(SignalDeciderExecutionPlanItem)):
            state = copy.deepcopy(row.submission_payload['core_quantity_sizing'])
            if row.id == 2:
                state['policy'].update(revision=2, calculated_at=(NOW + timedelta(hours=1)).isoformat(),
                                       valid_until=(NOW + timedelta(hours=3)).isoformat())
                state['last_ack_at'] = (NOW + timedelta(hours=1)).isoformat()
                states[2]['revision'] = 2
            row.submission_payload = {**row.submission_payload, 'core_quantity_sizing': state, 'exec_dispatch': {
                'plan_id': f'external-{row.id}', 'dispatch_status': 'accepted',
                'item_links': [{'side': 'buy', 'core_item_key': f'{row.id}:entry'}],
            }}

    async def list_plans(*, external_plan_id):
        key = int(external_plan_id.rsplit('-', 1)[1])
        return {'items': [{'external_plan_id': external_plan_id, 'execution_plan_id': f'remote-{key}',
                           'local_unacknowledged_buy_notional_krw': '0', 'account_submission_uncertain': False,
                           'item_states': [{'core_item_key': f'{key}:entry', 'core_quantity_state': states[key]}]}]}

    lose_reduction = lost_reduction_ack

    async def post_quantities(payload):
        nonlocal lose_reduction
        integration.client.calls.append(copy.deepcopy(payload))
        request = payload['items'][0]
        key = int(request['core_item_key'].split(':')[0])
        states[key].update(target_quantity=request['target_quantity'], revision=request['revision'])
        if key == 2 and lose_reduction:
            lose_reduction = False
            raise TimeoutError('lost reduction ACK')
        return {'items': [{'core_item_key': request['core_item_key'], 'execution_plan_id': f'remote-{key}',
                           'execution_item_id': f'entry-{key}', 'status': 'updated',
                           'revision': request['revision'], 'target_quantity': request['target_quantity']}]}

    monkeypatch.setattr(integration.client, 'list_execution_plans', list_plans)
    monkeypatch.setattr(integration.client, 'post_execution_plan_quantities', post_quantities)
    cash = 6000
    accounts.now = NOW + timedelta(hours=2)
    result = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now))
    if lost_reduction_ack:
        assert result['updated_count'] == 0
        assert result['failed_count'] == 1
        assert result['skipped_count'] == 1
        assert [call['items'][0]['core_item_key'] for call in integration.client.calls] == ['2:entry']
        assert states[1]['target_quantity'] == 50
        retry = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now + timedelta(seconds=1)))
        assert retry['updated_count'] == 1
        assert integration.client.calls[0] == integration.client.calls[1]
        refreshed = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now + timedelta(seconds=2)))
        assert refreshed['updated_count'] == 2
        assert [states[key]['target_quantity'] for key in (1, 2)] == [60, 30]
        return
    assert result['updated_count'] == 2
    assert {request['items'][0]['core_item_key'] for request in integration.client.calls} == {'1:entry', '2:entry'}
    assert [states[key]['target_quantity'] for key in (1, 2)] == [30, 30]
    assert sum(state['target_quantity'] * 100 for state in states.values()) == cash
    assert result['skipped_count'] == 0

    # A lost ACK from one cohort must settle before any newer cohort is allocated.
    accounts.now = NOW + timedelta(hours=4)
    with core_quantity_refresh.session_scope() as session:
        row = session.get(SignalDeciderExecutionPlanItem, 1)
        state = copy.deepcopy(row.submission_payload['core_quantity_sizing'])
        policy = state['policy']
        state['pending_request'] = {
            'external_plan_id': 'external-1', 'execution_plan_id': 'remote-1',
            'core_item_key': '1:entry', 'decision_snapshot_id': '1',
            'expected_revision': policy['revision'], 'revision': policy['revision'] + 1,
            'target_quantity': 30, 'calculated_at': accounts.now.isoformat(),
            'valid_until': (accounts.now + timedelta(hours=2)).isoformat(),
            'sizing_inputs': policy['sizing_inputs'],
        }
        row.submission_payload = {**row.submission_payload, 'core_quantity_sizing': state}
    retry = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now))
    assert retry['updated_count'] == 1
    assert retry['skipped_count'] == 1
    assert len(integration.client.calls) == 3
    assert integration.client.calls[-1]['items'][0]['core_item_key'] == '1:entry'
    next_tick = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now + timedelta(seconds=1)))
    assert next_tick['updated_count'] == 2
    assert len(integration.client.calls) == 5


@pytest.mark.parametrize('invalid', ['missing_caps', 'negative_cap', 'unknown_uncertainty'])
def test_missing_or_invalid_exec_operational_limits_retains_observation_only(invalid, monkeypatch):
    integration = Integration()
    packet = runtime_snapshot()
    if invalid == 'missing_caps':
        del packet['core_quantity_account']['limits']
    elif invalid == 'negative_cap':
        packet['core_quantity_account']['limits']['max_symbol_budget_krw'] = '-1'
    else:
        del packet['core_quantity_account']['account_submission_uncertain']

    async def runtime():
        return packet

    monkeypatch.setattr(integration.client, 'get_runtime_status', runtime)
    row = plan_row()
    service = CoreQuantityRefreshService(integration, Accounts())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    policy = row.submission_payload['core_quantity_sizing']['policy']
    assert policy['target_quantity'] == 0
    assert 'account_snapshot_unavailable' in policy['sizing_inputs']['reasons']


def test_prior_session_uncertainty_blocks_initial_new_buy_without_a_current_plan(monkeypatch):
    integration = Integration()

    async def runtime():
        packet = runtime_snapshot()
        packet['core_quantity_account']['account_submission_uncertain'] = True
        return packet

    monkeypatch.setattr(integration.client, 'get_runtime_status', runtime)
    row = plan_row()
    service = CoreQuantityRefreshService(integration, Accounts())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    assert row.submission_payload['core_quantity_sizing']['policy']['target_quantity'] == 0


def test_large_account_initial_policy_obeys_active_exec_absolute_symbol_limit():
    class LargeAccount(Accounts):
        async def get_account_summary(self, **kwargs):
            payload = await super().get_account_summary(**kwargs)
            payload['valuation']['total_asset_value'] = 100000000
            return payload

        async def get_buying_power(self, **kwargs):
            payload = await super().get_buying_power(**kwargs)
            payload['buying_power']['orderable_cash'] = 100000000
            return payload

    row = plan_row()
    service = CoreQuantityRefreshService(Integration(), LargeAccount())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    policy = row.submission_payload['core_quantity_sizing']['policy']
    assert policy['target_quantity'] == 30000
    assert policy['sizing_inputs']['operational_limits']['max_symbol_budget_krw'] == '3000000'


def test_account_local_reservation_is_subtracted_once_and_not_summed_again_from_plans(persisted_runtime, monkeypatch):
    integration, accounts, accept = persisted_runtime
    service = CoreQuantityRefreshService(integration, accounts)
    asyncio.run(service.prepare_initial(integration._load_next_open_rows(target_trade_date=NOW.date()), {1: snapshot()}, now=NOW))
    accept()
    accounts.now = NOW + timedelta(hours=2)
    runtime_calls = 0
    original_list = integration.client.list_execution_plans

    async def runtime():
        nonlocal runtime_calls
        runtime_calls += 1
        packet = runtime_snapshot(accounts.now)
        packet['core_quantity_account']['account_local_unacknowledged_buy_notional_krw'] = '1000'
        return packet

    async def list_plans(**kwargs):
        packet = await original_list(**kwargs)
        packet['items'][0]['local_unacknowledged_buy_notional_krw'] = '5000'
        return packet

    monkeypatch.setattr(integration.client, 'get_runtime_status', runtime)
    monkeypatch.setattr(integration.client, 'list_execution_plans', list_plans)
    result = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now))
    assert result['updated_count'] == 1
    assert runtime_calls == 1
    assert integration.client.calls[0]['items'][0]['target_quantity'] == 120


@pytest.mark.parametrize(('limit', 'value', 'quantity'), [('risk_fraction', '0.001', 20), ('max_symbol_fraction', '0.01', 10)])
def test_active_exec_fraction_limits_can_only_tighten_core_sizing(monkeypatch, limit, value, quantity):
    integration = Integration()

    async def runtime():
        packet = runtime_snapshot()
        packet['core_quantity_account']['limits'][limit] = value
        return packet

    monkeypatch.setattr(integration.client, 'get_runtime_status', runtime)
    row = plan_row()
    service = CoreQuantityRefreshService(integration, Accounts())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    assert row.submission_payload['core_quantity_sizing']['policy']['target_quantity'] == quantity


def test_runtime_account_snapshot_must_be_fresh_even_when_broker_reads_are_current(monkeypatch):
    integration = Integration()

    async def runtime():
        return runtime_snapshot(NOW - timedelta(seconds=301))

    monkeypatch.setattr(integration.client, 'get_runtime_status', runtime)
    row = plan_row()
    service = CoreQuantityRefreshService(integration, Accounts())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    policy = row.submission_payload['core_quantity_sizing']['policy']
    assert policy['target_quantity'] == 0
    assert policy['sizing_inputs']['account_error'] == 'quantity_exec_account_state_stale'


def test_runtime_limits_are_loaded_once_for_two_independent_lanes_in_one_account(monkeypatch):
    integration = Integration()
    calls = 0

    async def runtime():
        nonlocal calls
        calls += 1
        return runtime_snapshot()

    monkeypatch.setattr(integration.client, 'get_runtime_status', runtime)
    first, second = plan_row(), plan_row(2)
    second.decision_agent_name = 'short-term-decider'
    second_snap = snapshot(2)
    second_snap.agent_name = 'short-term-decider'
    service = CoreQuantityRefreshService(integration, Accounts())
    result = asyncio.run(service.prepare_initial([first, second], {1: snapshot(), 2: second_snap}, now=NOW, dry_run=True))
    assert result['prepared_count'] == 2
    assert calls == 1


@pytest.mark.parametrize('constraint', ['cash', 'cash_with_pending', 'symbol', 'total'])
def test_late_admission_reserves_existing_core_targets_before_authorizing_a_new_lane(constraint):
    class Account(Accounts):
        async def get_account_summary(self, **kwargs):
            payload = await super().get_account_summary(**kwargs)
            payload['valuation']['total_asset_value'] = 100000000 if constraint == 'total' else 100000
            return payload

        async def get_buying_power(self, **kwargs):
            payload = await super().get_buying_power(**kwargs)
            payload['buying_power']['orderable_cash'] = {'cash': 10000, 'cash_with_pending': 10000, 'symbol': 100000, 'total': 100000000}[constraint]
            return payload

    class Client(RuntimeClient):
        async def list_execution_plans(self, *, external_plan_id):
            return {'items': [{'external_plan_id': external_plan_id, 'execution_plan_id': 'remote-1',
                               'item_states': [{'core_item_key': f'{row.id}:entry', 'core_quantity_state': {
                                   'target_quantity': row.submission_payload['core_quantity_sizing']['policy']['target_quantity'],
                                   'revision': 1, 'filled_quantity': 20 if constraint == 'cash_with_pending' else 0,
                                   'open_quantity': 10 if constraint == 'cash_with_pending' else 0,
                                   'submission_uncertain': False,
                               }} for row in old]}]}

    class ScopedIntegration(Integration):
        client = Client()

        def _accepted_execution_plan_id_for_rows(self, rows):
            return 'remote-1' if rows[0].submission_payload.get('exec_dispatch') else None

    integration = ScopedIntegration()
    service = CoreQuantityRefreshService(integration, Account())
    old = [plan_row(index + 1) for index in range(3 if constraint == 'total' else 1)]
    snapshots = {}
    for row, symbol in zip(old, ['005930', '000660', '035420']):
        row.symbol = symbol
        snapshots[row.id] = snapshot(row.id)
    asyncio.run(service.prepare_initial(old, snapshots, now=NOW, dry_run=True))
    original_targets = [row.submission_payload['core_quantity_sizing']['policy']['target_quantity'] for row in old]
    for row in old:
        row.submission_payload['exec_dispatch'] = {
            'plan_id': 'external-1', 'dispatch_status': 'accepted',
            'item_links': [{'side': 'buy', 'core_item_key': f'{row.id}:entry'}],
        }
    new = plan_row(len(old) + 1)
    new.symbol = '005930' if constraint == 'symbol' else '035720'
    new.decision_agent_name = 'short-term-decider'
    snapshots[new.id] = snapshot(new.id)
    snapshots[new.id].agent_name = 'short-term-decider'
    result = asyncio.run(service.prepare_initial([*old, new], snapshots, now=NOW, dry_run=True))
    assert result['prepared_count'] == 1
    assert new.submission_payload['core_quantity_sizing']['policy']['target_quantity'] == (30 if constraint == 'cash_with_pending' else 0)
    assert [row.submission_payload['core_quantity_sizing']['policy']['target_quantity'] for row in old] == original_targets


def test_unknown_legacy_quantity_reservation_does_not_authorize_a_late_core_buy():
    class Client(RuntimeClient):
        async def list_execution_plans(self, *, external_plan_id):
            return {'items': [{'external_plan_id': external_plan_id, 'execution_plan_id': 'remote-1',
                               'item_states': [{'core_item_key': '1:entry', 'status': 'accepted'}]}]}

    class ScopedIntegration(Integration):
        client = Client()

        def _accepted_execution_plan_id_for_rows(self, rows):
            return 'remote-1' if rows[0].submission_payload.get('exec_dispatch') else None

    existing, new = plan_row(), plan_row(2)
    existing.submission_payload = {'exec_dispatch': {'plan_id': 'external-1', 'dispatch_status': 'accepted',
                                                    'item_links': [{'side': 'buy', 'core_item_key': '1:entry'}]}}
    new.symbol = '000660'
    service = CoreQuantityRefreshService(ScopedIntegration(), Accounts())
    result = asyncio.run(service.prepare_initial([existing, new], {1: snapshot(), 2: snapshot(2)}, now=NOW, dry_run=True))
    assert result['prepared_count'] == 1
    policy = new.submission_payload['core_quantity_sizing']['policy']
    assert policy['target_quantity'] == 0
    assert policy['sizing_inputs']['account_error'] == 'quantity_legacy_quantity_reservations_unproven'


def test_unsent_source_replacement_retires_old_sizing_and_prepares_revision_one_again():
    row = plan_row()
    service = CoreQuantityRefreshService(Integration(), Accounts())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    old_policy = copy.deepcopy(row.submission_payload['core_quantity_sizing']['initial_policy'])
    row.decision_snapshot_id = 2
    row.estimated_reference_price = 200
    replaced = snapshot(2)
    replaced.target_price = 260
    replaced.stop_loss_price = 190
    replaced.response_payload = {'trigger_price': 200, 'chase_limit_price': 200}
    result = asyncio.run(service.prepare_initial([row], {2: replaced}, now=NOW, dry_run=True))
    assert result['prepared_count'] == 1
    state = row.submission_payload['core_quantity_sizing']
    assert state['initial_policy']['revision'] == 1
    assert state['initial_policy']['target_quantity'] == 50
    assert state['initial_policy']['sizing_inputs']['price_contract_digest'] != old_policy['sizing_inputs']['price_contract_digest']
    assert service.policy_matches_source(row, replaced) is True
    assert len(state['retired_initial_policies']) == 1


def test_source_replacement_after_remote_acceptance_keeps_old_quantity_authority_and_reports_block(persisted_runtime):
    integration, accounts, accept = persisted_runtime
    service = CoreQuantityRefreshService(integration, accounts)
    asyncio.run(service.prepare_initial(integration._load_next_open_rows(target_trade_date=NOW.date()), {1: snapshot()}, now=NOW))
    accept()
    with core_quantity_refresh.session_scope() as session:
        session.get(SignalDeciderExecutionPlanItem, 1).estimated_reference_price = 101
    rows = integration._load_next_open_rows(target_trade_date=NOW.date())
    old_state = copy.deepcopy(rows[0].submission_payload['core_quantity_sizing'])
    result = asyncio.run(service.prepare_initial(rows, {1: snapshot()}, now=NOW))
    assert result['failed_count'] == 1
    assert result['items'][0]['reason_code'] == 'quantity_source_binding_changed_after_dispatch'
    assert rows[0].submission_payload['core_quantity_sizing'] == old_state
    assert service.policy_matches_source(rows[0], snapshot()) is False


def test_closed_entry_family_does_not_receive_a_new_buy_allowance(persisted_runtime):
    integration, accounts, accept = persisted_runtime
    service = CoreQuantityRefreshService(integration, accounts)
    asyncio.run(service.prepare_initial(integration._load_next_open_rows(target_trade_date=NOW.date()), {1: snapshot()}, now=NOW))
    accept()
    integration.client.execution['entry_closed'] = True
    accounts.now = NOW + timedelta(hours=2)
    result = asyncio.run(service.refresh_if_due(NOW.date(), now=accounts.now))
    assert result['updated_count'] == 1
    request = integration.client.calls[0]['items'][0]
    assert request['target_quantity'] == 20
    assert 'entry_family_closed' in request['sizing_inputs']['reasons']


def test_source_replacement_cannot_discard_a_pending_remote_dispatch():
    row = plan_row()
    service = CoreQuantityRefreshService(Integration(), Accounts())
    asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    old_state = copy.deepcopy(row.submission_payload['core_quantity_sizing'])
    row.estimated_reference_price = 101
    row.submission_payload['exec_dispatch'] = {'dispatch_status': 'pending_remote_ack', 'plan_id': 'unknown-outcome'}
    result = asyncio.run(service.prepare_initial([row], {1: snapshot()}, now=NOW, dry_run=True))
    assert result['failed_count'] == 1
    assert result['items'][0]['reason_code'] == 'quantity_source_binding_changed_after_dispatch'
    assert row.submission_payload['core_quantity_sizing'] == old_state
