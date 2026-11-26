import asyncio
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
from contextlib import contextmanager
from types import SimpleNamespace

import httpx
import pytest

from app.market.session import KRX_EXCHANGE_CALENDAR
from app.overnight_rebound.evidence import RuleEvidenceAdapter, resolve_rule_session
from tests.test_us_rebound_execution_contract import NOW, policy, setup


def daily_rows():
    days = KRX_EXCHANGE_CALENDAR.sessions_in_range('2026-11-01', '2026-11-26')[-15:]
    return [{
        'stck_bsop_date': day.strftime('%Y%m%d'), 'stck_oprc': '10000',
        'stck_hgpr': '10100', 'stck_lwpr': '9900', 'stck_clpr': '10000',
        'acml_vol': '100000', 'acml_tr_pbmn': '1000000000',
    } for day in days]


class RawKIS:
    use_mock = False

    def __init__(self):
        self.calls = []
        self.chart = {'rt_cd': '0', 'output2': daily_rows()}
        self.balance = {'rt_cd': '0', 'output1': [], 'output2': {'tot_evlu_amt': '1000000'}}
        self.assets = {'rt_cd': '0', 'output2': {'tot_asst_amt': '1000000'}}
        self.power = {'rt_cd': '0', 'output': {
            'ord_psbl_cash': '500000', 'nrcvb_buy_amt': '400000', 'nrcvb_buy_qty': '39',
            'psbl_qty_calc_unpr': '10050', 'max_buy_amt': '5000000', 'max_buy_qty': '500',
        }}
        self.daily = {'rt_cd': '0', 'output1': [{
            'ord_dt': '20261127', 'odno': '0000000001', 'pdno': '005930',
            'excg_id_dvsn_cd': 'KRX', 'ord_gno_brno': '001',
            'ord_qty': '10', 'rmn_qty': '5', 'tot_ccld_qty': '5', 'ord_unpr': '10000', 'cncl_yn': 'N',
        }], 'output2': {}}
        self.cancelable = {'rt_cd': '0', 'output': [{**self.daily['output1'][0], 'psbl_qty': '5'}]}
        self.continuation = ''

    async def _request(self, method, path, *, deadline_guard, **kwargs):
        deadline_guard()
        self.calls.append((method, path, kwargs))
        assert method == 'GET'
        if path.endswith('/inquire-daily-itemchartprice'):
            return deepcopy(self.chart), ''
        mapping = {
            '/inquire-balance': self.balance, '/inquire-account-balance': self.assets,
            '/inquire-psbl-order': self.power, '/inquire-daily-ccld': self.daily,
            '/inquire-psbl-rvsecncl': self.cancelable,
        }
        for suffix, body in mapping.items():
            if path.endswith(suffix):
                return deepcopy(body), self.continuation if suffix == '/inquire-daily-ccld' else ''
        raise AssertionError('unexpected raw path')


def test_market_evidence_uses_exact_prior_krx_session_and_contiguous_provider_etf_bars():
    kis = RawKIS()
    adapter = RuleEvidenceAdapter(kis_client=kis, exec_client=None)
    session = resolve_rule_session(date(2026, 11, 27)).value
    result = asyncio.run(adapter.market(symbol='069500', session=session, mode='live', clock=lambda: NOW, max_pages=2))

    assert result.status == 'available'
    assert result.value.prior_close_date == date(2026, 11, 26)
    assert result.value.prior_close == Decimal('10000')
    assert result.value.volatility_pct == Decimal('2')
    assert result.value.daily_value_krw == Decimal('1000000000')
    assert result.value.observed_at == NOW
    assert len(kis.calls) == 1
    assert kis.calls[0][2]['params']['FID_INPUT_ISCD'] == '069500'
    assert 'account_reference' not in result.public_status()


def live_setup():
    explicit = policy(execution_mode='live', validation_status='validated_live',
                      validation_evidence_id='test-validation', validation_evidence_hash='f' * 64)
    prepared = setup(policy=explicit, account_reference='KIS:12345678-01',
                     session=resolve_rule_session(date(2026, 11, 27)).value).setup
    return prepared, explicit


@contextmanager
def empty_database():
    class Session:
        def execute(self, statement):
            class Result:
                def mappings(self):
                    return self
                def all(self):
                    return []
            return Result()
    yield Session()


def exec_response(request):
    if request.url.path.endswith('/capabilities'):
        assert request.headers['X-Core-Account-Reference'] == 'KIS:12345678-01'
        return httpx.Response(200, json={
            'capabilities': ['rule_origin_krx_us_rebound_v1'],
            'runtime_binding': {'mode': 'live', 'provider': 'kis', 'configured_route_fingerprint': 'a' * 32,
                                'account_reference_matches': True},
        })
    if request.url.path == '/runtime/status':
        return httpx.Response(200, json={
            'mode': 'live', 'core_quantity_account': {
                'as_of': NOW.isoformat(), 'account_submission_uncertain': False,
                'account_local_unacknowledged_buy_notional_krw': '0',
                'limits': {'max_symbol_budget_krw': '500000', 'max_total_budget_krw': '100000',
                           'risk_fraction': '0.015', 'max_symbol_fraction': '0.30'},
            },
        })
    if request.url.path == '/v4/execution-plans':
        return httpx.Response(200, json=[])
    raise AssertionError('unexpected Exec path')


def account_result(kis, monkeypatch, handler=exec_response, publication=None, use_default_publication=False):
    monkeypatch.setattr('app.orders.account_inquiry_service._utc_now', lambda: NOW)
    prepared, explicit = live_setup()

    async def read():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            options = {} if use_default_publication else {'publication_reader': lambda **kwargs: publication}
            adapter = RuleEvidenceAdapter(kis_client=kis, exec_client=client, session_factory=empty_database,
                account_target_reader=lambda: ([SimpleNamespace(account_reference='KIS:12345678-01')], []), **options)
            return await adapter.account(setup=prepared, policy=explicit, clock=lambda: NOW, max_pages=2)
    return asyncio.run(read())


def test_unknown_original_book_uses_proven_full_cap_not_fictional_zero(monkeypatch):
    kis = RawKIS()
    result = account_result(kis, monkeypatch)

    assert result.status == 'no_capacity'
    assert result.value.ord_psbl_cash == Decimal('500000')
    assert result.value.nrcvb_buy_amt == Decimal('400000')
    assert result.value.nrcvb_buy_qty == 39
    assert result.value.unproven_broker_pending_reserve_krw == Decimal('50000')
    assert result.value.baseline_entitlement_krw == Decimal('100000')
    assert result.value.remaining_account_buy_notional_krw == 0
    assert result.value.read_group_id
    assert 'original_entitlement_full_shared_cap_bound' in result.reasons
    assert all(method == 'GET' for method, _, _ in kis.calls)


def completed_empty_publication():
    from app.eod.daily_pipeline_stages import DailyPipelineStageService

    roles = ['short-term-decider', 'signal-decider']
    scopes = {role: ['005930'] for role in roles}
    identities = [{'symbol': '005930', 'decision_agent_name': role, 'decision_snapshot_id': index,
                   'decision_run_id': f'decision-{index}'} for index, role in enumerate(roles, 1)]
    signal = {
        'schema_version': 'stage_completion_scope_v1', 'kind': 'signal_decider_terminal_progression',
        'roles': roles, 'source_run_id': 'source-parent', 'source_summary_run_id': 'summary-parent',
        'knowledge_cutoff_at': '2026-11-26T07:00:00+00:00',
        'admitted_symbols': ['005930'], 'completed_symbols': ['005930'],
        'completed_symbols_by_role': scopes, 'failed_symbols_by_role': {role: [] for role in roles},
        'role_symbol_scopes': scopes, 'decision_identities': identities,
        'total_items': 2, 'completed_items': 2, 'failed_items': 0,
    }
    signal['proof_digest'] = DailyPipelineStageService._stage_completion_scope_proof_digest(signal)
    plan = {
        'schema_version': 'stage_completion_scope_v1', 'kind': 'next_open_execution_plan_parent_scope',
        'parent_run_id': 'source-parent', 'parent_summary_run_id': 'summary-parent',
        'parent_admitted_symbols': ['005930'], 'parent_completed_symbols': ['005930'],
        'authority_fingerprint': 'a' * 64, 'generation_fingerprint': 'b' * 64,
        'role_symbol_scopes': scopes,
        'decision_partition': [{**identity, 'canonical_action': 'reject', 'authority_fingerprint': 'a' * 64,
                               'generation_fingerprint': 'b' * 64, 'disposition': 'non_actionable',
                               'included_items': [], 'exclusion': {'kind': 'non_actionable', 'reason_code': 'latest_snapshot_not_actionable'}}
                              for identity in identities],
        'decision_count': 2, 'included_decision_count': 0, 'excluded_decision_count': 2,
        'item_count': 0, 'active_item_count': 0, 'blocked_item_count': 0, 'manifest_digest': 'c' * 64,
    }
    plan['proof_digest'] = DailyPipelineStageService._stage_completion_scope_proof_digest(plan)
    def stage(stage_id, proof, total):
        return {
            'pipeline_run_id': 11, 'stage_id': stage_id, 'market_date': '2026-11-26', 'status': 'completed',
            'completed_at': '2026-11-26T18:00:00+09:00',
            'processed': total, 'total': total, 'succeeded': total, 'failed': 0, 'remaining': 0,
            'source_run_ids': {'parent_signal_decider_run_id': 'source-parent', 'signal_decider_run_id': 'source-parent',
                               'source_summary_run_id': 'summary-parent'},
            'details': {'completion_scope_proof': proof, 'result_summary': {
                'request': {'target_trade_date': '2026-11-27'},
                'summary': {'target_trade_date': '2026-11-27', 'source_market_date': '2026-11-26'},
            }},
        }
    plan_stage = stage('next_open_execution_plan', plan, 0)
    plan_stage['details']['nonblocking_scope_proof'] = {
        'schema_version': 'nonblocking_downstream_scope_v1', 'kind': 'next_open_plan_active_scope',
        'active_symbols': [], 'active_scope': [], 'item_count': 0, 'active_item_count': 0, 'blocked_item_count': 0,
    }
    return {
        'signal_stage': stage('signal_decider', signal, 2), 'plan_stage': plan_stage,
        'current': {
            'source_run_id': 'source-parent', 'role_symbol_scopes': scopes, 'blocked': [],
            'selected': [{
                'id': identity['decision_snapshot_id'], 'symbol': '005930', 'agent_name': identity['decision_agent_name'],
                'decision_run_id': identity['decision_run_id'], 'market_date': '2026-11-26',
                'decision_action': 'reject', 'next_day_open_action': 'reject', 'entry_price_basis': 'next_open',
                'target_price': None, 'stop_loss_price': None, 'traceability_payload': {},
                'request_payload': {'symbol_day_context': {'market': 'KOSPI'}},
                'response_payload': {'tactical_action': 'reject'},
            } for identity in identities],
        },
    }


def test_healthy_completed_nonempty_original_cohort_with_no_actionable_decisions_proves_zero_entitlement(monkeypatch):
    kis = RawKIS()
    kis.daily['output1'], kis.cancelable['output'] = [], []
    result = account_result(kis, monkeypatch, publication=completed_empty_publication())

    assert result.status == 'available'
    assert result.value.baseline_entitlement_krw == 0
    assert result.value.remaining_account_buy_notional_krw == Decimal('100000')
    assert 'original_completed_nonactionable_publication' in result.reasons
    from app.overnight_rebound.sizing import size_rule_entry
    prepared, explicit = live_setup()
    assert size_rule_entry(setup=prepared, policy=explicit, account=result.value, now=NOW).target_quantity == 9


@pytest.mark.parametrize('calculation_price', ['9000', '13000'])
def test_provider_calculation_basis_keeps_the_independent_worst_price_cash_limit(monkeypatch, calculation_price):
    kis = RawKIS()
    kis.daily['output1'], kis.cancelable['output'] = [], []
    kis.power['output']['psbl_qty_calc_unpr'] = calculation_price
    result = account_result(kis, monkeypatch, publication=completed_empty_publication())

    assert result.status == 'available'
    assert result.value.order_possible_price == Decimal('10050')
    from app.overnight_rebound.sizing import size_rule_entry
    prepared, explicit = live_setup()
    assert size_rule_entry(setup=prepared, policy=explicit, account=result.value, now=NOW).target_quantity == 9


@pytest.mark.parametrize('change', ['no_proof', 'wrong_target', 'newer_buy', 'wrong_snapshot', 'trigger_wait'])
def test_fake_or_superseded_empty_original_publication_cannot_prove_zero(monkeypatch, change):
    published = completed_empty_publication()
    if change == 'no_proof':
        published['plan_stage']['details'].pop('completion_scope_proof')
    elif change == 'wrong_target':
        published['plan_stage']['details']['result_summary']['request']['target_trade_date'] = '2026-11-26'
    elif change == 'wrong_snapshot':
        published['current']['selected'][1]['id'] = 999
    elif change == 'newer_buy':
        published['current']['selected'][1]['next_day_open_action'] = 'buy'
    else:
        published['current']['selected'][1].update(next_day_open_action='wait', entry_price_basis='trigger_limit',
            target_price=10500, stop_loss_price=9800,
            response_payload={'trigger_price': 10000, 'chase_limit_price': 10050})
    result = account_result(RawKIS(), monkeypatch, publication=published)
    assert result.status == 'no_capacity'
    assert result.value.baseline_entitlement_krw == Decimal('100000')


@pytest.mark.parametrize(('change', 'reason'), [
    ('missing_cash_only', 'required_numeric_evidence_missing'),
    ('missing_equity', 'required_numeric_evidence_missing'),
    ('partial_daily', 'provider_pagination_unproven'),
    ('market_open_order', 'broker_open_order_price_unproven'),
    ('missing_cancelable', 'broker_open_order_not_reconciled'),
    ('contradictory_cancelable', 'broker_cancelable_reconciliation_unproven'),
    ('contradictory_order', 'broker_order_quantities_contradictory'),
    ('foreign_order', 'broker_order_scope_unproven'),
])
def test_partial_or_unproven_raw_vendor_book_never_becomes_executable_zero(monkeypatch, change, reason):
    kis = RawKIS()
    if change == 'missing_cash_only':
        del kis.power['output']['nrcvb_buy_amt']  # max_buy_amt intentionally remains very large.
    elif change == 'missing_equity':
        del kis.balance['output2']['tot_evlu_amt']  # normalized/asset summary fallback is forbidden.
    elif change == 'partial_daily':
        kis.continuation = 'F'
    elif change == 'market_open_order':
        kis.daily['output1'][0]['ord_unpr'] = '0'
    elif change == 'missing_cancelable':
        kis.cancelable['output'] = []
    elif change == 'contradictory_cancelable':
        kis.cancelable['output'][0]['psbl_qty'] = '4'
    elif change == 'contradictory_order':
        kis.daily['output1'][0]['tot_ccld_qty'] = '7'
    else:
        kis.daily['output1'][0]['cano'] = 'different-account'
    result = account_result(kis, monkeypatch)
    assert result.value is None
    assert result.status == 'unavailable'
    assert reason in result.reasons


def test_false_complete_positions_with_nan_value_are_malformed_not_zero(monkeypatch):
    kis = RawKIS()
    kis.balance['output1'] = [{'pdno': '005930', 'hldg_qty': '1', 'evlu_amt': 'NaN'}]
    result = account_result(kis, monkeypatch)
    assert result.value is None
    assert result.status == 'failed'
    assert result.reasons == ('malformed_provider_evidence',)


def test_zero_provider_calculation_basis_is_malformed_not_a_funded_grant(monkeypatch):
    kis = RawKIS()
    kis.power['output']['psbl_qty_calc_unpr'] = '0'
    result = account_result(kis, monkeypatch, publication=completed_empty_publication())
    assert result.value is None and result.status == 'failed'
    assert result.reasons == ('malformed_provider_evidence',)


@pytest.mark.parametrize('change', ['missing_previous', 'holiday_bar', 'inconsistent_ohlc', 'zero_atr'])
def test_etf_history_requires_exact_completed_calendar_bars_and_usable_volatility(change):
    kis = RawKIS()
    if change == 'missing_previous':
        kis.chart['output2'].pop()
    elif change == 'holiday_bar':
        kis.chart['output2'].append({**kis.chart['output2'][-1], 'stck_bsop_date': '20261121'})
    elif change == 'inconsistent_ohlc':
        kis.chart['output2'][-1]['stck_hgpr'] = '9000'
    else:
        for row in kis.chart['output2']:
            row['stck_hgpr'] = row['stck_lwpr'] = '10000'
    adapter = RuleEvidenceAdapter(kis_client=kis, exec_client=None)
    result = asyncio.run(adapter.market(symbol='069500', session=resolve_rule_session(date(2026, 11, 27)).value,
                                       mode='live', clock=lambda: NOW, max_pages=2))
    assert result.value is None and result.status == 'unavailable'


def test_read_that_crosses_cutoff_returns_no_market_evidence_or_later_request():
    kis = RawKIS()
    current = [NOW]
    original = kis._request
    async def delayed(*args, **kwargs):
        payload = await original(*args, **kwargs)
        current[0] = NOW.replace(minute=40)
        return payload
    kis._request = delayed
    adapter = RuleEvidenceAdapter(kis_client=kis, exec_client=None)
    result = asyncio.run(adapter.market(symbol='069500', session=resolve_rule_session(date(2026, 11, 27)).value,
                                       mode='live', clock=lambda: current[0], max_pages=2))
    assert result.value is None
    assert result.reasons == ('evidence_window_expired',)
    assert len(kis.calls) == 1


def test_paper_never_falls_back_to_a_real_or_shared_credential_client():
    from app.brokers.kis.client import KISClient
    session = resolve_rule_session(date(2026, 11, 27)).value
    real = RawKIS()
    for client, reason in ((real, 'kis_read_client_mode_mismatch'), (KISClient(use_mock=True), 'dedicated_paper_client_unavailable'), (None, 'kis_read_client_unavailable')):
        result = asyncio.run(RuleEvidenceAdapter(kis_client=client, exec_client=None).market(
            symbol='069500', session=session, mode='paper', clock=lambda: NOW, max_pages=2,
        ))
        assert result.value is None and result.reasons == (reason,)
    assert real.calls == []


def test_zero_vendor_cash_capacity_is_reported_as_no_capacity_with_honest_values(monkeypatch):
    kis = RawKIS()
    kis.daily['output1'], kis.cancelable['output'] = [], []
    kis.power['output'].update(nrcvb_buy_amt='0', nrcvb_buy_qty='0')
    result = account_result(kis, monkeypatch, publication=completed_empty_publication())
    assert result.status == 'no_capacity'
    assert result.value.nrcvb_buy_amt == result.value.nrcvb_buy_qty == 0


@pytest.mark.parametrize('change', ['changed_route', 'stale_runtime', 'uncertain_submission'])
def test_exec_scope_changes_or_stale_account_components_are_unavailable(monkeypatch, change):
    capabilities = [0]
    def handler(request):
        result = exec_response(request)
        payload = result.json()
        if request.url.path.endswith('/capabilities'):
            capabilities[0] += 1
            if change == 'changed_route' and capabilities[0] > 1:
                payload['runtime_binding']['configured_route_fingerprint'] = 'b' * 32
        if request.url.path == '/runtime/status':
            if change == 'stale_runtime':
                payload['core_quantity_account']['as_of'] = NOW.replace(hour=7, minute=54).isoformat()
            if change == 'uncertain_submission':
                payload['core_quantity_account']['account_submission_uncertain'] = True
        return httpx.Response(200, json=payload)
    result = account_result(RawKIS(), monkeypatch, handler=handler)
    assert result.value is None and result.status == 'unavailable'


def test_all_venue_selector_survives_every_daily_page_and_order_ids_do_not_collapse_across_venues(monkeypatch):
    kis = RawKIS()
    nxt = {**kis.daily['output1'][0], 'excg_id_dvsn_cd': 'NXT'}
    kis.daily['output1'].append(nxt)
    kis.cancelable['output'].append({**nxt, 'psbl_qty': '5'})
    result = account_result(kis, monkeypatch)
    assert result.value.unproven_broker_pending_reserve_krw == Decimal('100000')
    daily_calls = [kwargs for _, path, kwargs in kis.calls if path.endswith('/inquire-daily-ccld')]
    assert daily_calls and all(item['params']['EXCG_ID_DVSN_CD'] == 'ALL' for item in daily_calls)


def test_mixed_case_exchange_id_is_normalized_without_inventing_numeric_division_mapping(monkeypatch):
    kis = RawKIS()
    kis.daily['output1'][0]['excg_id_dvsn_Cd'] = kis.daily['output1'][0].pop('excg_id_dvsn_cd')
    result = account_result(kis, monkeypatch)
    assert result.value.unproven_broker_pending_reserve_krw == Decimal('50000')
    kis.daily['output1'][0].pop('excg_id_dvsn_Cd')
    kis.daily['output1'][0]['excg_dvsn_cd'] = '01'
    result = account_result(kis, monkeypatch)
    assert result.value is None and 'broker_order_identity_unproven' in result.reasons


def test_default_publication_adapter_reads_existing_history_and_snapshot_helpers_only(monkeypatch):
    published = completed_empty_publication()
    def history(**kwargs):
        assert kwargs == {'market_date': '2026-11-26'}
        return {key: {'run': {'id': 11, 'market_date': '2026-11-26', 'trigger': 'manual'},
                      'stage': published[field]} for key, field in (
            ('signal_decider', 'signal_stage'), ('next_open_execution_plan', 'plan_stage'),
        )}
    def current(**kwargs):
        assert kwargs['source_market_date'] == date(2026, 11, 26)
        snapshots = {}
        for item in published['current']['selected']:
            value = {**item, 'market_date': date(2026, 11, 26)}
            snapshots[(item['symbol'], item['agent_name'])] = SimpleNamespace(**value)
        return {'source_run': {'run_id': 'source-parent'}, 'selected_by_key': snapshots,
                'source_role_symbol_scopes': published['current']['role_symbol_scopes'],
                'failures_by_key': {}, 'blocked_source_scope_keys': set(),
                'weekly_policy_rebind_blocked_scope_keys': set()}
    monkeypatch.setattr('app.eod.pipeline_status._load_daily_pipeline_stage_history', history)
    monkeypatch.setattr('app.orders.next_open_execution.signal_decider_next_open_execution_service._load_latest_decider_scope_state', current)
    kis = RawKIS()
    kis.daily['output1'], kis.cancelable['output'] = [], []
    result = account_result(kis, monkeypatch, use_default_publication=True)
    assert result.status == 'available'
    assert result.value.baseline_entitlement_krw == 0


def test_legacy_remote_book_cannot_override_a_healthy_empty_publication(monkeypatch):
    def handler(request):
        if request.url.path == '/v4/execution-plans':
            return httpx.Response(200, json=[{'execution_plan_id': 'legacy', 'item_states': [{'status': 'accepted'}]}])
        return exec_response(request)
    result = account_result(RawKIS(), monkeypatch, handler=handler, publication=completed_empty_publication())
    assert result.status == 'no_capacity'
    assert result.value.baseline_entitlement_krw == Decimal('100000')


def test_account_group_keeps_private_proof_out_of_public_diagnostics(monkeypatch):
    import json
    from app.internal.exact_repair_manifest import canonical_json_sha256

    result = account_result(RawKIS(), monkeypatch)
    assert result.value.read_group_id == canonical_json_sha256(result.private_proof)
    public = json.dumps(result.public_status())
    assert '12345678' not in public
    assert 'ord_psbl_cash' not in public
    assert 'private_proof' not in public


def test_soft_batch_timeout_does_not_start_another_provider_request():
    kis = RawKIS()
    original = kis._request
    async def slow(*args, **kwargs):
        await asyncio.sleep(0.05)
        return await original(*args, **kwargs)
    kis._request = slow
    result = asyncio.run(RuleEvidenceAdapter(kis_client=kis, exec_client=None).market(
        symbol='069500', session=resolve_rule_session(date(2026, 11, 27)).value,
        mode='live', clock=lambda: NOW, max_pages=2, budget_seconds=0.01,
    ))
    assert result.value is None and result.status == 'unavailable'
    assert result.error_type == 'TimeoutError'
    assert kis.calls == []


def test_original_publication_change_inside_a_read_group_is_unavailable(monkeypatch):
    monkeypatch.setattr('app.orders.account_inquiry_service._utc_now', lambda: NOW)
    prepared, explicit = live_setup()
    count = [0]
    def publication(**kwargs):
        count[0] += 1
        return completed_empty_publication() if count[0] == 1 else None
    async def read():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(exec_response)) as client:
            adapter = RuleEvidenceAdapter(kis_client=RawKIS(), exec_client=client, session_factory=empty_database,
                account_target_reader=lambda: ([SimpleNamespace(account_reference='KIS:12345678-01')], []),
                publication_reader=publication)
            return await adapter.account(setup=prepared, policy=explicit, clock=lambda: NOW, max_pages=2)
    result = asyncio.run(read())
    assert result.value is None
    assert result.reasons == ('original_authority_changed_during_read',)

