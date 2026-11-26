"""Bounded read-only evidence for the independent rule; no order/planner calls."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from importlib.metadata import version
from time import monotonic
from types import SimpleNamespace
from typing import Any

import httpx

from app.brokers.kis.client import KISClient, KISClientError
from app.eod.kis_final_bar_repair import extract_kis_final_bars
from app.eod.technical_snapshots import DailyTechnicalSnapshotService
from app.internal.exact_repair_manifest import canonical_json_sha256
from app.market.session import KRX_EXCHANGE_CALENDAR, KRX_EXCHANGE_CALENDAR_SOURCE
from app.orders.account_inquiry_service import TradingAccountInquiryService
from app.overnight_rebound.execution_contract import (
    ETF_NAMES, KST, RuleMarketEvidence, RuleSessionEvidence, RuleExecutionPlan, _aware,
)
from app.overnight_rebound.sizing import RuleAccountEvidence, size_rule_entry


@dataclass(frozen=True)
class EvidenceResult:
    value: Any | None
    status: str
    reasons: tuple[str, ...] = ()
    error_type: str | None = None
    private_proof: dict | None = field(default=None, repr=False)

    def public_status(self):
        return {'status': self.status, 'reasons': list(self.reasons),
                **({'error_type': self.error_type} if self.error_type else {})}


class _Unavailable(ValueError):
    pass


class _Malformed(ValueError):
    pass


def _decimal(value, *, positive=False, whole=False):
    if isinstance(value, bool) or value is None or value == '':
        raise _Unavailable('required_numeric_evidence_missing')
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise _Malformed('invalid_numeric_evidence') from None
    if not number.is_finite() or number < 0 or positive and number <= 0 or whole and number != number.to_integral_value():
        raise _Malformed('invalid_numeric_evidence')
    return number


def resolve_rule_session(target_trade_date: date) -> EvidenceResult:
    if type(target_trade_date) is not date:
        raise ValueError('target session must be a date')
    try:
        day = target_trade_date.isoformat()
        if not KRX_EXCHANGE_CALENDAR.is_session(day):
            return EvidenceResult(None, 'unavailable', ('krx_closed',))
        opened = KRX_EXCHANGE_CALENDAR.session_open(day).to_pydatetime().astimezone(KST)
        closed = KRX_EXCHANGE_CALENDAR.session_close(day).to_pydatetime().astimezone(KST)
        if opened.time() != time(9) or closed.time() != time(15, 30):
            return EvidenceResult(None, 'unavailable', ('special_session_unsupported',))
        return EvidenceResult(RuleSessionEvidence(
            target_trade_date=target_trade_date,
            previous_trade_date=KRX_EXCHANGE_CALENDAR.previous_session(day).date(),
            session_open_at=opened, session_close_at=closed,
            calendar_status='authoritative', special_hours=False,
            calendar_source=f'{KRX_EXCHANGE_CALENDAR_SOURCE};exchange_calendars={version("exchange_calendars")}',
        ), 'available')
    except (ValueError, KeyError, IndexError):
        return EvidenceResult(None, 'unavailable', ('calendar_evidence_unavailable',))


class _Batch:
    def __init__(self, day, clock, budget_seconds):
        if isinstance(budget_seconds, bool) or not 0 < budget_seconds <= 90:
            raise ValueError('read batch budget must be positive and at most 90 seconds')
        self.day, self.clock, self.budget = day, clock, budget_seconds
        self.started = monotonic()
        self.last = _aware(clock())
        self.started_clock = self.last
        self.components = []
        self.check()

    def check(self):
        now = _aware(self.clock())
        if now < self.last:
            raise _Unavailable('evidence_clock_reversed')
        self.last = now
        if now.date() != self.day or now.time() >= time(8, 40):
            raise _Unavailable('evidence_window_expired')
        if monotonic() - self.started >= self.budget or (now - self.started_clock).total_seconds() >= self.budget:
            raise _Unavailable('evidence_budget_expired')
        return now

    async def read(self, label, call):
        requested = self.check()
        remaining = min(self.budget - (monotonic() - self.started),
                        (datetime.combine(self.day, time(8, 40), tzinfo=KST) - requested).total_seconds())
        value = await asyncio.wait_for(call(), timeout=remaining)
        received = self.check()
        self.components.append({
            'component': label, 'requested_at': requested.isoformat(), 'received_at': received.isoformat(),
            'content_hash': canonical_json_sha256(value),
        })
        return value


class _ReadKIS(KISClient):
    """Reuse raw pagination and inquiry parsers with per-page deadline checks."""

    def __init__(self, client, batch, max_pages):
        if type(max_pages) is not int or not 1 <= max_pages <= 10:
            raise ValueError('explicit max_pages must be an integer from 1 through 10')
        self.client, self.batch, self.max_pages = client, batch, max_pages
        self.use_mock = client.use_mock
        self.balance = None

    async def _request(self, method, path, **kwargs):
        if method != 'GET' or path not in {
            '/uapi/domestic-stock/v1/trading/inquire-balance',
            '/uapi/domestic-stock/v1/trading/inquire-account-balance',
            '/uapi/domestic-stock/v1/trading/inquire-psbl-order',
            '/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl',
            '/uapi/domestic-stock/v1/trading/inquire-daily-ccld',
            '/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice',
        }:
            raise ValueError('rule evidence permits only its read-only KIS endpoints')
        return await self.batch.read(path, lambda: self.client._request(
            method, path, **kwargs, deadline_guard=self.batch.check,
        ))

    async def fetch_domestic_account_balance(self, **kwargs):
        if self.balance is None:
            self.balance = await super().fetch_domestic_account_balance(**kwargs, max_pages=self.max_pages)
        return self.balance


def _result_error(exc):
    if isinstance(exc, _Unavailable):
        return EvidenceResult(None, 'unavailable', (str(exc),))
    if isinstance(exc, (TimeoutError, KISClientError, httpx.HTTPError)):
        return EvidenceResult(None, 'unavailable', ('provider_read_unavailable',), type(exc).__name__)
    if isinstance(exc, _Malformed):
        return EvidenceResult(None, 'failed', ('malformed_provider_evidence',), type(exc).__name__)
    return EvidenceResult(None, 'failed', ('evidence_adapter_error',), type(exc).__name__)


def _paged_rows(payload, field, max_pages):
    if not isinstance(payload, dict) or payload.get('max_pages_reached') is not False:
        raise _Unavailable('provider_pagination_unproven')
    pages = payload.get('raw_pages')
    if not isinstance(pages, list) or not pages or type(payload.get('page_count')) is not int or payload['page_count'] != len(pages) or len(pages) > max_pages:
        raise _Unavailable('provider_pagination_unproven')
    rows = []
    for page in pages:
        if not isinstance(page, dict) or page.get('rt_cd') != '0' or not isinstance(page.get(field), list) or any(not isinstance(row, dict) for row in page[field]):
            raise _Unavailable('provider_page_incomplete')
        rows.extend(page[field])
    if rows != payload.get(field):
        raise _Unavailable('provider_merged_rows_mismatch')
    return rows


def _symbol(row):
    value = row.get('pdno')
    if not isinstance(value, str) or len(value) != 6 or not value.isascii() or not value.isdigit():
        raise _Unavailable('provider_symbol_unproven')
    return value


def _order_row(row):
    normalized = {}
    for key, value in row.items():
        lower = str(key).lower()
        if lower in normalized and normalized[lower] != value:
            raise _Unavailable('broker_order_duplicate_contradictory')
        normalized[lower] = value
    return normalized


def _broker_reservations(daily, cancelable, *, day, account, max_pages):
    """Both complete raw inquiries are necessary; cancelable alone is insufficient."""
    daily_rows = _paged_rows(daily, 'output1', max_pages)
    cancel_rows = _paged_rows(cancelable, 'output', max_pages)
    orders = {}
    for raw in daily_rows:
        row = _order_row(raw)
        if row.get('ord_dt') != day.strftime('%Y%m%d') or row.get('sll_buy_dvsn_cd', '02') != '02' or row.get('cano', account.account_number) != account.account_number or row.get('acnt_prdt_cd', account.account_product_code) != account.account_product_code:
            raise _Unavailable('broker_order_scope_unproven')
        order_id = row.get('odno')
        if not isinstance(order_id, str) or not order_id.strip():
            raise _Unavailable('broker_order_identity_unproven')
        venue = row.get('excg_id_dvsn_cd')
        branches = (row.get('ord_gno_brno'), row.get('ord_orgno'))
        symbol = _symbol(row)
        quantity, filled, remaining = [_decimal(row.get(name), whole=True) for name in ('ord_qty', 'tot_ccld_qty', 'rmn_qty')]
        price = _decimal(row.get('ord_unpr'))
        cancelled = row.get('cncl_yn')
        if cancelled not in ('Y', 'N') or filled > quantity or remaining > quantity - filled or cancelled == 'Y' and remaining or cancelled == 'N' and quantity != filled + remaining:
            raise _Unavailable('broker_order_quantities_contradictory')
        if remaining and price <= 0:
            raise _Unavailable('broker_open_order_price_unproven')
        if remaining and (not isinstance(venue, str) or not venue.strip() or not any(isinstance(value, str) and value.strip() for value in branches)):
            raise _Unavailable('broker_order_identity_unproven')
        key = (account.account_reference, row['ord_dt'], venue, *branches, order_id)
        normalized = {'symbol': symbol, 'quantity': quantity, 'filled': filled, 'remaining': remaining, 'price': price,
                      'venue': venue, 'branches': branches, 'order_id': order_id, 'day': row['ord_dt']}
        if key in orders and orders[key] != normalized:
            raise _Unavailable('broker_order_duplicate_contradictory')
        orders[key] = normalized
    cancel_by_key = {}
    for raw in cancel_rows:
        row = _order_row(raw)
        order_id = row.get('odno')
        raw_day = row.get('ord_dt', day.strftime('%Y%m%d'))
        if row.get('cano', account.account_number) != account.account_number or row.get('acnt_prdt_cd', account.account_product_code) != account.account_product_code:
            raise _Unavailable('broker_order_scope_unproven')
        matches = [(key, item) for key, item in orders.items() if item['order_id'] == order_id and item['day'] == raw_day
                   and item['symbol'] == _symbol(row)
                   and ('excg_id_dvsn_cd' not in row or row['excg_id_dvsn_cd'] == item['venue'])
                   and ('ord_gno_brno' not in row or row['ord_gno_brno'] == item['branches'][0])
                   and ('ord_orgno' not in row or row['ord_orgno'] == item['branches'][1])]
        if len(matches) != 1:
            raise _Unavailable('broker_cancelable_reconciliation_unproven')
        key, matching = matches[0]
        possible = _decimal(row.get('psbl_qty'), whole=True)
        if possible != matching['remaining']:
            raise _Unavailable('broker_cancelable_reconciliation_unproven')
        for name, expected in (('ord_qty', matching['quantity']), ('tot_ccld_qty', matching['filled']), ('ord_unpr', matching['price'])):
            if name in row and _decimal(row[name]) != expected:
                raise _Unavailable('broker_cancelable_reconciliation_unproven')
        if key in cancel_by_key and cancel_by_key[key] != possible:
            raise _Unavailable('broker_order_duplicate_contradictory')
        cancel_by_key[key] = possible
    if any(row['remaining'] and key not in cancel_by_key for key, row in orders.items()):
        raise _Unavailable('broker_open_order_not_reconciled')
    by_symbol = {}
    for row in orders.values():
        amount = row['remaining'] * row['price']
        by_symbol[row['symbol']] = by_symbol.get(row['symbol'], Decimal(0)) + amount
    return by_symbol


def _core_plan_rows(session_factory, account_reference, target_trade_date):
    from sqlalchemy import select
    from app.db import session_scope
    from app.db_models import SignalDeciderExecutionPlanItem as Plan

    with (session_factory or session_scope)() as session:
        return [dict(row) for row in session.execute(select(
            Plan.id, Plan.decision_agent_name, Plan.symbol, Plan.planned_side,
            Plan.eligibility_status, Plan.execution_status, Plan.submission_payload,
        ).where(
            Plan.account_reference == account_reference, Plan.target_trade_date == target_trade_date,
            Plan.decision_agent_name.in_(('signal-decider', 'short-term-decider')),
        )).mappings().all()]


def _route_from_original_configuration(reader, account_reference):
    from app.orders.account_scope import parse_kis_account_reference
    if reader is None:
        from app.orders.next_open_execution import signal_decider_next_open_execution_service
        reader = signal_decider_next_open_execution_service._parse_account_targets
    targets, errors = reader()
    if errors or len(targets) != 1 or targets[0].account_reference != account_reference:
        raise _Unavailable('single_configured_account_route_unproven')
    try:
        return parse_kis_account_reference(account_reference)
    except ValueError:
        raise _Unavailable('single_configured_account_route_unproven') from None


def _original_publication(*, source_market_date, target_trade_date):
    """Read existing publication history/current snapshots, without advancing it."""
    from app.eod.pipeline_status import EODPipelineStatusService, _load_daily_pipeline_stage_history
    from app.orders.next_open_execution import signal_decider_next_open_execution_service

    history = _load_daily_pipeline_stage_history(market_date=source_market_date.isoformat())
    source = history.get('signal_decider')
    planned = history.get('next_open_execution_plan')
    if not source or not planned:
        return None
    stages = {name: item['stage'] for name, item in history.items()
              if item['run']['id'] == planned['run']['id']}
    rebuilt = EODPipelineStatusService._rebuild_scheduled_terminal_stages(
        official_run={**planned['run'], 'stages': stages}, market_date=source_market_date.isoformat(),
    )
    state = signal_decider_next_open_execution_service._load_latest_decider_scope_state(
        source_market_date=source_market_date, target_trade_date=target_trade_date,
    )
    snapshots = []
    for snapshot in state.get('selected_by_key', {}).values():
        if snapshot is None:
            return None
        snapshots.append({
            **{name: getattr(snapshot, name, None) for name in (
                'id', 'symbol', 'agent_name', 'decision_run_id', 'decision_action', 'next_day_open_action',
                'entry_price_basis', 'target_price', 'stop_loss_price', 'traceability_payload',
                'request_payload', 'response_payload', 'symbol_day_market_claim', 'company_market_claim',
                'projected_tactical_action',
            )},
            'market_date': snapshot.market_date.isoformat(),
        })
    snapshots.sort(key=lambda item: (item['symbol'], item['agent_name']))
    return {
        'signal_stage': rebuilt.get('signal_decider', source['stage']),
        'plan_stage': rebuilt.get('next_open_execution_plan', planned['stage']),
        'current': {
            'source_run_id': (state.get('source_run') or {}).get('run_id'),
            'role_symbol_scopes': state.get('source_role_symbol_scopes'), 'selected': snapshots,
            'blocked': sorted([list(key) for field in (
                'failures_by_key', 'blocked_source_scope_keys', 'weekly_policy_rebind_blocked_scope_keys',
            ) for key in state.get(field, ())]),
        },
    }


def _completed_empty_publication(publication, *, source_market_date, target_trade_date, now):
    from app.eod.daily_pipeline_stages import DailyPipelineStageService
    from app.eod.pipeline_status import EODPipelineStatusService
    from app.orders.next_open_execution import _is_actionable_snapshot, _is_krx_next_open_snapshot

    if not isinstance(publication, dict):
        return False
    source = publication.get('signal_stage', {})
    planned = publication.get('plan_stage', {})
    if type(planned.get('pipeline_run_id')) is not int or planned['pipeline_run_id'] <= 0 or source.get('pipeline_run_id') != planned.get('pipeline_run_id') or source.get('stage_id') != 'signal_decider' or planned.get('stage_id') != 'next_open_execution_plan':
        return False
    if any(stage.get('market_date') != source_market_date.isoformat() or stage.get('status') != 'completed' or stage.get('failed') != 0 or stage.get('remaining') != 0 for stage in (source, planned)):
        return False
    try:
        if any(_aware(datetime.fromisoformat(stage['completed_at'])) > now for stage in (source, planned)):
            return False
    except (KeyError, TypeError, ValueError):
        return False
    source_proof = DailyPipelineStageService.validated_stage_completion_scope_proof('signal_decider', source.get('details', {}).get('completion_scope_proof'))
    plan_proof = DailyPipelineStageService.validated_stage_completion_scope_proof('next_open_execution_plan', planned.get('details', {}).get('completion_scope_proof'))
    if source_proof is None or plan_proof is None or source_proof['failed_items'] != 0 or plan_proof['included_decision_count'] != 0 or plan_proof['item_count'] != 0:
        return False
    if _aware(datetime.fromisoformat(source_proof['knowledge_cutoff_at'])) > now:
        return False
    if not EODPipelineStatusService._next_open_execution_plan_has_completion_proof(candidate=planned, details=planned.get('details', {})):
        return False
    summary = planned.get('details', {}).get('result_summary', {})
    if summary.get('request', {}).get('target_trade_date') != target_trade_date.isoformat() or summary.get('summary', {}).get('target_trade_date') != target_trade_date.isoformat() or summary.get('summary', {}).get('source_market_date') != source_market_date.isoformat():
        return False
    if source_proof['source_run_id'] != plan_proof['parent_run_id'] or source_proof['source_summary_run_id'] != plan_proof['parent_summary_run_id']:
        return False
    current = publication.get('current', {})
    if current.get('source_run_id') != source_proof['source_run_id'] or current.get('blocked') != [] or current.get('role_symbol_scopes') != plan_proof['role_symbol_scopes']:
        return False
    identities = {(item['symbol'], item['decision_agent_name']): (item['decision_snapshot_id'], item['decision_run_id']) for item in source_proof['decision_identities']}
    partition = {(item['symbol'], item['decision_agent_name']): (item['decision_snapshot_id'], item['decision_run_id']) for item in plan_proof['decision_partition']}
    selected = current.get('selected')
    if not isinstance(selected, list) or len(selected) != len(identities) or partition != identities:
        return False
    observed = {}
    for item in selected:
        key = (item.get('symbol'), item.get('agent_name'))
        snapshot = SimpleNamespace(**item)
        if item.get('market_date') != source_market_date.isoformat() or key in observed or not _is_krx_next_open_snapshot(snapshot) or _is_actionable_snapshot(snapshot):
            return False
        observed[key] = (item.get('id'), item.get('decision_run_id'))
    return observed == identities


def _no_remote_original_buy_authority(plans):
    for plan in plans:
        if plan.get('origin_type') == 'rule':
            continue
        states = plan.get('item_states')
        if not isinstance(states, list) or not states:
            return False
        quantities = [item['core_quantity_state'] for item in states if isinstance(item.get('core_quantity_state'), dict)]
        if not quantities:
            return False
        for item in quantities:
            if item.get('submission_uncertain') is not False or type(item.get('entry_closed')) is not bool:
                return False
            if any(type(item.get(name)) is not int or item[name] < 0 for name in ('target_quantity', 'revision', 'filled_quantity', 'open_quantity', 'remaining_quantity')):
                return False
            if item['entry_closed'] is not True and item['remaining_quantity'] != 0:
                return False
    return True


class RuleEvidenceAdapter:
    def __init__(self, *, kis_client, exec_client: httpx.AsyncClient | None, session_factory=None, account_target_reader=None, publication_reader=None):
        self.kis, self.exec = kis_client, exec_client
        self.session_factory, self.account_target_reader = session_factory, account_target_reader
        self.publication_reader = publication_reader or _original_publication

    def _client(self, mode, batch, max_pages):
        if mode not in ('paper', 'live'):
            raise ValueError('rule evidence mode must be explicit paper/live')
        if self.kis is None:
            raise _Unavailable('kis_read_client_unavailable')
        if type(self.kis.use_mock) is not bool or self.kis.use_mock != (mode == 'paper'):
            raise _Unavailable('kis_read_client_mode_mismatch')
        if mode == 'paper' and isinstance(self.kis, KISClient):
            # Native KISClient still reads the shared credentials; a mock URL is not paper provisioning.
            raise _Unavailable('dedicated_paper_client_unavailable')
        return _ReadKIS(self.kis, batch, max_pages)

    async def market(self, *, symbol, session: RuleSessionEvidence, mode, clock: Callable[[], datetime], max_pages: int, budget_seconds=90) -> EvidenceResult:
        try:
            if symbol not in ETF_NAMES:
                raise _Unavailable('rule_instrument_not_whitelisted')
            authoritative = resolve_rule_session(session.target_trade_date)
            if authoritative.value is None or authoritative.value != session:
                raise _Unavailable('calendar_evidence_binding_mismatch')
            batch = _Batch(session.target_trade_date, clock, budget_seconds)
            client = self._client(mode, batch, max_pages)
            days = KRX_EXCHANGE_CALENDAR.sessions_window(session.previous_trade_date.isoformat(), -15)
            start, end = days[0].date(), session.previous_trade_date
            payload = await client.fetch_domestic_daily_chart(
                symbol, start_date=start.strftime('%Y%m%d'), end_date=end.strftime('%Y%m%d'),
                period_code='D', adjusted_price='0',
            )
            if not isinstance(payload, dict) or payload.get('rt_cd') != '0':
                raise _Unavailable('etf_history_unavailable')
            try:
                bars = extract_kis_final_bars(payload, symbol=symbol, start_date=start, end_date=end)
            except ValueError:
                raise _Unavailable('etf_history_incomplete_or_malformed') from None
            if set(bars) != {day.date() for day in days}:
                raise _Unavailable('etf_history_not_contiguous')
            ordered = [bars[day.date()] for day in days]
            if any(min(bar.values) <= 0 for bar in ordered):
                raise _Unavailable('etf_history_not_tradeable')
            computed = DailyTechnicalSnapshotService().compute_snapshot(
                symbol=symbol, market_date=end.isoformat(), source='kis',
                bars=[{'trade_date': bar.trade_date.isoformat(), 'open_price': bar.open_price,
                       'high_price': bar.high_price, 'low_price': bar.low_price,
                       'close_price': bar.close_price, 'volume': bar.volume} for bar in ordered],
            )
            if computed.atr_pct_14 is None or computed.atr_pct_14 <= 0:
                raise _Unavailable('etf_volatility_unavailable')
            volatility = _decimal(computed.atr_pct_14, positive=True)
            batch.check()
            received = _aware(datetime.fromisoformat(batch.components[-1]['received_at']))
            proof = {'symbol': symbol, 'session': session.model_dump(mode='json'),
                     'source_environment': 'mock' if client.use_mock else 'real',
                     'components': batch.components}
            return EvidenceResult(RuleMarketEvidence(
                symbol=symbol, prior_close_date=end, prior_close=Decimal(ordered[-1].close_price),
                volatility_pct=volatility, daily_value_krw=Decimal(ordered[-1].turnover),
                observed_at=received, source='kis_daily_chart:15_contiguous_completed_krx_sessions',
                evidence_id=canonical_json_sha256(proof),
                source_environment='mock' if client.use_mock else 'real', status='verified',
            ), 'available', private_proof=proof)
        except Exception as exc:
            return _result_error(exc)

    async def account(self, *, setup, policy, clock: Callable[[], datetime], max_pages: int, budget_seconds=90) -> EvidenceResult:
        try:
            if self.exec is None:
                raise _Unavailable('exec_read_client_unavailable')
            if setup.policy_hash != policy.policy_hash:
                raise _Unavailable('evidence_policy_binding_mismatch')
            session = resolve_rule_session(setup.origin.target_trade_date)
            if session.value is None or session.value != setup.session:
                raise _Unavailable('calendar_evidence_binding_mismatch')
            batch = _Batch(setup.origin.target_trade_date, clock, budget_seconds)
            client = self._client(setup.mode, batch, max_pages)
            account = _route_from_original_configuration(self.account_target_reader, setup.account_reference)

            async def exec_get(path, **kwargs):
                async def call():
                    response = await self.exec.get(path, follow_redirects=False, **kwargs)
                    response.raise_for_status()
                    return response.json()
                return await batch.read(f'exec:{path}', call)

            async def binding():
                payload = await exec_get('/v4/execution-plans/capabilities', headers={'X-Core-Account-Reference': account.account_reference})
                bound = payload.get('runtime_binding', {})
                route = bound.get('configured_route_fingerprint')
                if 'rule_origin_krx_us_rebound_v1' not in payload.get('capabilities', []) or bound.get('mode') != setup.mode or bound.get('provider') != 'kis' or bound.get('account_reference_matches') is not True or not isinstance(route, str) or len(route) != 32 or any(c not in '0123456789abcdef' for c in route):
                    raise _Unavailable('exec_account_runtime_binding_unproven')
                return bound

            initial_binding = await binding()
            service = TradingAccountInquiryService(client=client)
            positions = await service.get_positions(account_reference=account.account_reference, include_zero=True)
            summary = await service.get_account_summary(account_reference=account.account_reference)
            requested_at = batch.check()
            power = await service.get_buying_power(
                account_reference=account.account_reference, symbol=setup.symbol, price=setup.chase_limit_price,
                order_type='limit', include_cma=False, include_overseas=False,
            )
            power_received = batch.check()
            balance = positions.get('raw', {}).get('balance')
            rows = _paged_rows(balance, 'output1', max_pages)
            if positions.get('completeness', {}).get('status') != 'complete' or positions.get('count') != len(rows):
                raise _Unavailable('positions_completeness_unproven')
            now = batch.check()
            for payload in (positions, summary, power):
                try:
                    as_of = _aware(datetime.fromisoformat(payload['as_of']))
                except (KeyError, TypeError, ValueError):
                    raise _Unavailable('account_read_scope_or_age_unproven') from None
                if payload.get('account_reference') != account.account_reference or payload.get('broker_name') != 'kis' or not timedelta(0) <= now - as_of <= timedelta(seconds=policy.max_evidence_age_seconds):
                    raise _Unavailable('account_read_scope_or_age_unproven')
            totals = balance.get('output2')
            if not isinstance(totals, dict):
                raise _Unavailable('strict_account_equity_unavailable')
            equity = _decimal(totals.get('tot_evlu_amt'), positive=True)
            exposures = {}
            holdings = {}
            for row in rows:
                symbol = _symbol(row)
                quantity = _decimal(row.get('hldg_qty'), whole=True)
                amount = _decimal(row.get('evlu_amt'))
                if symbol in exposures or quantity > 0 and amount <= 0 or quantity == 0 and amount != 0:
                    raise _Unavailable('position_exposure_contradictory')
                exposures[symbol], holdings[symbol] = amount, quantity
            output = power.get('raw', {}).get('response', {}).get('output')
            inputs = power.get('input', {})
            if not isinstance(output, dict) or power.get('raw', {}).get('response', {}).get('rt_cd') != '0' or power.get('raw', {}).get('source_api') != 'inquire-psbl-order' or power.get('raw', {}).get('order_division_code') != '00' or power.get('symbol') != setup.symbol or inputs.get('order_type') != 'limit' or inputs.get('side') != 'buy' or inputs.get('include_cma') is not False or inputs.get('include_overseas') is not False or _decimal(inputs.get('price'), positive=True) != setup.chase_limit_price:
                raise _Unavailable('strict_cash_only_response_unavailable')
            orderable = _decimal(output.get('ord_psbl_cash'))
            cash_only = _decimal(output.get('nrcvb_buy_amt'))
            quantity = _decimal(output.get('nrcvb_buy_qty'), whole=True)
            calculation_price = _decimal(output.get('psbl_qty_calc_unpr'), positive=True)
            cancelable = await client.fetch_domestic_cancelable_orders(
                account_number=account.account_number, account_product_code=account.account_product_code,
                inquiry_division_1='0', inquiry_division_2='2', max_pages=max_pages,
            )
            today = setup.origin.target_trade_date.strftime('%Y%m%d')
            daily = await client.fetch_domestic_daily_order_fills(
                account_number=account.account_number, account_product_code=account.account_product_code,
                period_division='inner', inquiry_start_date=today, inquiry_end_date=today,
                sell_buy_division_code='02', conclusion_division='00', inquiry_division='00', inquiry_division_3='00',
                symbol='', broker_order_id='', exchange_id_division_code='ALL', max_pages=max_pages,
            )
            broker_at = batch.check()
            reservations = _broker_reservations(daily, cancelable, day=setup.origin.target_trade_date, account=account, max_pages=max_pages)
            runtime = await exec_get('/runtime/status')
            qa = runtime.get('core_quantity_account', {})
            if runtime.get('mode') != setup.mode or qa.get('account_submission_uncertain') is not False:
                raise _Unavailable('exec_account_submission_uncertain')
            runtime_at = _aware(datetime.fromisoformat(qa['as_of']))
            local_reserved = _decimal(qa.get('account_local_unacknowledged_buy_notional_krw'))
            limits = {name: _decimal(qa.get('limits', {}).get(name), positive=True) for name in (
                'max_symbol_budget_krw', 'max_total_budget_krw', 'risk_fraction', 'max_symbol_fraction',
            )}
            if policy.loss_fraction > limits['risk_fraction'] or policy.max_symbol_fraction > limits['max_symbol_fraction'] or equity * policy.max_symbol_fraction > limits['max_symbol_budget_krw']:
                raise _Unavailable('policy_exceeds_existing_account_limits')
            plans = []
            for index in range(max_pages):
                page = await exec_get('/v4/execution-plans', params={'limit': 100, 'offset': index * 100})
                if not isinstance(page, list) or len(page) > 100:
                    raise _Unavailable('exec_plan_coverage_unproven')
                plans.extend(page)
                if len(page) < 100:
                    break
            else:
                raise _Unavailable('exec_plan_pagination_unproven')
            baseline_rows = await batch.read('core_original_plan_rows', lambda: asyncio.to_thread(
                _core_plan_rows, self.session_factory, account.account_reference, setup.origin.target_trade_date,
            ))
            publication = await batch.read('core_original_publication', lambda: asyncio.to_thread(
                self.publication_reader, source_market_date=setup.source_market_date,
                target_trade_date=setup.origin.target_trade_date,
            ))
            original_cap = limits['max_total_budget_krw']
            reasons = ['original_entitlement_full_shared_cap_bound']
            if not baseline_rows and _no_remote_original_buy_authority(plans) and _completed_empty_publication(
                publication, source_market_date=setup.source_market_date, target_trade_date=setup.origin.target_trade_date,
                now=batch.check(),
            ):
                original_cap = Decimal(0)
                reasons = ['original_completed_nonactionable_publication']
            rule_notional, rule_risk = Decimal(0), Decimal(0)
            for plan in plans:
                if plan.get('origin_type') != 'rule' or plan.get('rule_origin', {}).get('target_trade_date') != setup.origin.target_trade_date.isoformat():
                    continue
                contract = RuleExecutionPlan.model_validate(plan.get('rule_contract'))
                entry = next(item for item in contract.items if item.side == 'buy')
                entry_acks = [item for item in plan.get('item_acks', []) if item.get('core_item_key') == entry.core_item_key and item.get('rule_origin') == contract.rule_origin.model_dump(mode='json')]
                if len(entry_acks) != 1 or contract.mode != setup.mode or plan.get('contract_ack', {}).get('dispatch_material_fingerprint') != contract.metadata.dispatch_material_fingerprint or entry.risk_context.get('execution_account_route', {}).get('configured_route_fingerprint') != initial_binding['configured_route_fingerprint']:
                    raise _Unavailable('existing_rule_commitment_binding_unproven')
                matches = [item for item in plan.get('item_states', []) if item.get('core_item_key') == entry.core_item_key and item.get('execution_plan_item_id') == entry_acks[0].get('execution_plan_item_id')]
                if len(matches) != 1 or matches[0].get('core_quantity_state', {}).get('submission_uncertain') is not False or matches[0].get('rule_origin') != contract.rule_origin.model_dump(mode='json'):
                    raise _Unavailable('existing_rule_commitment_unproven')
                state = matches[0]['core_quantity_state']
                if state.get('revision') != entry.quantity_policy['revision']:
                    raise _Unavailable('existing_rule_revision_unproven')
                committed = max(_decimal(state.get('target_quantity'), whole=True),
                                _decimal(state.get('filled_quantity'), whole=True) + _decimal(state.get('open_quantity'), whole=True))
                inputs = entry.quantity_policy['sizing_inputs']
                rule_notional += committed * _decimal(inputs.get('cash_per_share'), positive=True)
                rule_risk += committed * _decimal(inputs.get('risk_per_share'), positive=True)
            closing_rows = await batch.read('core_original_plan_rows_close', lambda: asyncio.to_thread(
                _core_plan_rows, self.session_factory, account.account_reference, setup.origin.target_trade_date,
            ))
            closing_publication = await batch.read('core_original_publication_close', lambda: asyncio.to_thread(
                self.publication_reader, source_market_date=setup.source_market_date,
                target_trade_date=setup.origin.target_trade_date,
            ))
            if closing_rows != baseline_rows or closing_publication != publication:
                raise _Unavailable('original_authority_changed_during_read')
            final_binding = await binding()
            if final_binding != initial_binding:
                raise _Unavailable('exec_account_runtime_binding_changed')
            now = batch.check()
            stamps = [datetime.fromisoformat(item['requested_at']) for item in batch.components]
            if any(not timedelta(0) <= now - stamp <= timedelta(seconds=policy.max_evidence_age_seconds) for stamp in (*stamps, runtime_at)):
                raise _Unavailable('evidence_component_expired')
            proof = {
                'account_reference': account.account_reference, 'mode': setup.mode, 'symbol': setup.symbol,
                'requested_price': str(setup.chase_limit_price),
                'quantity_calculation_price': str(calculation_price), 'route': initial_binding,
                'target_trade_date': setup.origin.target_trade_date.isoformat(), 'components': batch.components,
            }
            read_group = canonical_json_sha256(proof)
            original_potential = {lane: any(row.get('symbol') == setup.symbol and row.get('planned_side') == 'buy' and row.get('decision_agent_name') == lane for row in baseline_rows)
                                  for lane in ('signal-decider', 'short-term-decider')}
            has_holding = holdings.get(setup.symbol, Decimal(0)) > 0
            has_open = reservations.get(setup.symbol, Decimal(0)) > 0
            pending = sum(reservations.values(), Decimal(0))
            held = sum(exposures.values(), Decimal(0))
            balance_at = max(datetime.fromisoformat(item['received_at']) for item in batch.components
                             if item['component'].endswith('/inquire-balance'))
            evidence = RuleAccountEvidence(
                account_reference=account.account_reference, provider='kis', runtime_mode=setup.mode,
                symbol=setup.symbol, order_possible_price=setup.chase_limit_price, read_group_id=read_group,
                observed_at=min(stamps), balance_observed_at=balance_at,
                order_possible_requested_at=requested_at, order_possible_observed_at=power_received,
                broker_orders_observed_at=broker_at, baseline_observed_at=now,
                account_equity_krw=equity, ord_psbl_cash=orderable, nrcvb_buy_amt=cash_only, nrcvb_buy_qty=int(quantity),
                unproven_broker_pending_reserve_krw=pending + local_reserved,
                baseline_entitlement_krw=original_cap, buying_power_krw=min(orderable, cash_only),
                remaining_account_buy_notional_krw=max(limits['max_total_budget_krw'] - original_cap - pending - rule_notional, Decimal(0)),
                remaining_account_risk_krw=max(equity * min(policy.loss_fraction, limits['risk_fraction'], Decimal('0.015')) - held - pending - original_cap - rule_risk, Decimal(0)),
                symbol_exposure_krw=exposures.get(setup.symbol, Decimal(0)), symbol_reserved_krw=reservations.get(setup.symbol, Decimal(0)),
                strategy_committed_notional_krw=rule_notional, liquidity_committed_notional_krw=rule_notional,
                other_rule_committed_notional_krw=rule_notional + local_reserved, other_rule_committed_risk_krw=rule_risk + local_reserved,
                baseline_entitlements_proven=True, reservations_complete=True, exposure_complete=True,
                original_signal_potential=original_potential['signal-decider'],
                original_short_term_potential=original_potential['short-term-decider'],
                original_open_order=None if has_open else False, original_holding=None if has_holding else False,
                ambiguous_exposure=has_open or has_holding,
            )
            grant = size_rule_entry(setup=setup, policy=policy, account=evidence, now=now)
            return EvidenceResult(evidence, 'available' if grant.target_quantity > 0 else 'no_capacity',
                                  tuple(reasons + list(grant.reasons)), private_proof=proof)
        except Exception as exc:
            return _result_error(exc)
