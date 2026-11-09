from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.agent_context import reuse_service
from app.agent_context.orchestrator_helpers import attach_timesfm_forecast_evidence
from app.core.settings import settings
from app.forecast_context import service as forecast_service


MARKET_DATE = date(2026, 11, 6)
CUTOFF = '2026-11-06T14:00:00+00:00'
SYMBOL = '005930'
DECIDERS = ('short-term-decider', 'signal-decider')


class Rows:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def all(self):
        return self.rows

    def scalars(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def scalar_one(self):
        return self.first()


@pytest.fixture
def persisted_forecasts(monkeypatch):
    monkeypatch.setattr(settings, 'timesfm_enabled', True)
    monkeypatch.setattr(settings, 'timesfm_context_length', 512)
    monkeypatch.setattr(settings, 'timesfm_min_context_length', 128)
    sessions = forecast_service.KRX_EXCHANGE_CALENDAR.sessions_in_range(
        '2025-01-01', MARKET_DATE.isoformat()
    )[-128:]
    bars = [
        {
            'trade_date': session.date().isoformat(),
            'close_price': 100,
            'captured_at': '2026-11-06T12:00:00+00:00',
            'source': 'pykrx',
        }
        for session in sessions
    ]
    session_state = {'open': False}

    def load_prices(**_):
        assert not session_state['open'], 'Forecast validation held the feedback DB session'
        return {SYMBOL: deepcopy(bars)}

    loader = Mock(side_effect=load_prices)
    worker = Mock(return_value={
        'items': [{
            'symbol': SYMBOL,
            'quantiles': [[98, 99, 99.5, 100, 101, 102, 103, 103.5, 104] for _ in range(20)],
        }],
    })
    monkeypatch.setattr(forecast_service, 'load_recent_price_bars_by_symbol', loader)
    monkeypatch.setattr(forecast_service, '_run_worker', worker)
    evidence = forecast_service.TimesFMForecastService().build_for_symbols(
        market_date=MARKET_DATE, symbols=[SYMBOL], knowledge_cutoff_at=CUTOFF,
    )[SYMBOL]
    assert evidence['status'] == 'available'
    loader.reset_mock()
    worker.reset_mock()
    worker.side_effect = AssertionError('Feedback reuse must never invoke the model')

    feedback_at = datetime(2026, 11, 6, 14, 5, tzinfo=timezone.utc)
    feedback = {
        role: SimpleNamespace(
            id=index, market_date=MARKET_DATE, symbol=SYMBOL,
            agent_name=role, run_id='forecast-origin-run',
            summary_payload={}, feedback_payload={'summary': 'prior decision'},
            context_payload=attach_timesfm_forecast_evidence(
                {'role': role, 'symbol': SYMBOL}, deepcopy(evidence)
            ),
            received_at=feedback_at,
        )
        for index, role in enumerate(DECIDERS, start=1)
    }
    run = SimpleNamespace(
        id=10, run_id='forecast-origin-run',
        started_at=datetime(2026, 11, 6, 14, 1, tzinfo=timezone.utc),
        request_payload={
            'admission_knowledge_cutoff_at': CUTOFF,
            'knowledge_cutoff_at': '2026-11-06T13:00:00+00:00',
        },
    )
    snapshots = [
        SimpleNamespace(
            id=100 + row.id, feedback_id=row.id,
            content_fingerprint=f'persisted-snapshot-{row.id}',
            decision_run_id=run.run_id, feedback_received_at=feedback_at,
            symbol=SYMBOL, agent_name=role,
        )
        for role, row in feedback.items()
    ]

    class FeedbackSession:
        def execute(self, statement):
            sql = str(statement)
            if 'FROM symbols' in sql:
                return Rows([SimpleNamespace(id=101, symbol=SYMBOL)])
            if 'FROM agent_context_feedback_runs' in sql:
                return Rows([run])
            if 'FROM agent_context_feedback_failures' in sql:
                return Rows()
            if 'FROM signal_decider_decision_snapshots' in sql:
                return Rows(snapshots)
            if 'FROM agent_context_feedback' in sql:
                if 'max(' in sql:
                    return Rows([datetime(2026, 11, 6, 14, 2, tzinfo=timezone.utc)])
                role = statement.compile().params['agent_name_1']
                return Rows([feedback[role]])
            raise AssertionError(f'Unexpected feedback query: {sql}')

    @contextmanager
    def feedback_session():
        session_state['open'] = True
        try:
            yield FeedbackSession()
        finally:
            session_state['open'] = False

    monkeypatch.setattr(reuse_service, 'session_scope', feedback_session)
    service = reuse_service.AgentContextReuseService()

    def build(cutoff=CUTOFF):
        return service.build_latest_decisions(
            market_date=MARKET_DATE, symbols=[SYMBOL], roles=DECIDERS,
            knowledge_cutoff_at=cutoff,
        )[SYMBOL]

    yield SimpleNamespace(
        build=build, bars=bars, feedback=feedback, run=run,
        loader=loader, worker=worker,
    )
    worker.assert_not_called()


@pytest.mark.parametrize('cutoff', [CUTOFF, '2026-11-06T23:00:00+09:00', None])
def test_unchanged_forecasts_reuse_at_requested_or_original_run_cutoff(persisted_forecasts, cutoff):
    case = persisted_forecasts
    for row in case.feedback.values():
        row.context_payload.update(role='technical-summarizer', symbol='WRONG')
    original_contexts = deepcopy([row.context_payload for row in case.feedback.values()])

    decisions = case.build(cutoff)

    assert all(not decision.should_dispatch for decision in decisions.values())
    assert {decision.reason for decision in decisions.values()} == {'reuse_latest_feedback'}
    assert all(decision.latest_decision_snapshot_id for decision in decisions.values())
    assert case.loader.called
    assert [row.context_payload for row in case.feedback.values()] == original_contexts


@pytest.mark.parametrize('enabled', [True, False])
def test_historical_price_change_redispatches_both_deciders_even_after_disable(
    persisted_forecasts, monkeypatch, enabled,
):
    case = persisted_forecasts
    monkeypatch.setattr(settings, 'timesfm_enabled', enabled)
    case.bars[30]['close_price'] = 101

    decisions = case.build()

    assert all(decision.should_dispatch for decision in decisions.values())
    assert {decision.reason for decision in decisions.values()} == {
        'forecast_evidence_invalid:forecast_source_changed'
    }
    bundle = reuse_service.AgentContextReuseService().build_latest_feedback_bundle(
        market_date=MARKET_DATE, symbols=[SYMBOL], roles=DECIDERS,
    )
    assert all(item['stale'] for item in bundle['items'][0]['roles'])


def test_post_cutoff_history_repair_redispatches_without_moving_boundary(persisted_forecasts):
    case = persisted_forecasts
    case.bars[30]['captured_at'] = '2026-11-06T14:30:00+00:00'

    decisions = case.build(None)

    assert all(decision.should_dispatch for decision in decisions.values())
    assert {decision.reason for decision in decisions.values()} == {
        'forecast_evidence_invalid:forecast_source_unavailable'
    }


@pytest.mark.parametrize('unavailable', [False, True])
def test_legacy_and_authentic_unavailable_keep_reuse_without_source_reads(
    persisted_forecasts, unavailable,
):
    case = persisted_forecasts
    for row in case.feedback.values():
        row.context_payload = (
            attach_timesfm_forecast_evidence(
                {}, forecast_service.unavailable_evidence(SYMBOL, MARKET_DATE, 'forecast_worker_timeout')
            )
            if unavailable else {}
        )

    decisions = case.build()

    assert all(not decision.should_dispatch for decision in decisions.values())
    case.loader.assert_not_called()


@pytest.mark.parametrize('mutation,reason', [
    (lambda context: context.pop('forecast_evidence'), 'missing_forecast_evidence'),
    (lambda context: context.update(forecast_evidence={'timesfm3': None}), 'malformed_forecast_evidence'),
    (
        lambda context: context['forecast_evidence_generation'].update(evidence_fingerprint='replacement'),
        'forecast_evidence_generation_mismatch',
    ),
    (
        lambda context: context['forecast_evidence']['timesfm3']['horizons'][0].update(price_q50=999),
        'forecast_evidence_fingerprint_mismatch',
    ),
])
def test_invalid_saved_evidence_redispatches_only_its_decider_lane(persisted_forecasts, mutation, reason):
    case = persisted_forecasts
    mutation(case.feedback[DECIDERS[0]].context_payload)

    decisions = case.build()

    assert decisions[DECIDERS[0]].should_dispatch
    assert decisions[DECIDERS[0]].reason == f'forecast_evidence_invalid:{reason}'
    assert not decisions[DECIDERS[1]].should_dispatch


def test_expected_cutoff_mismatch_keeps_existing_reason_and_skips_forecast_reads(persisted_forecasts):
    case = persisted_forecasts

    decisions = case.build('2026-11-06T14:01:00+00:00')

    assert all(decision.should_dispatch for decision in decisions.values())
    assert {decision.reason for decision in decisions.values()} == {'knowledge_cutoff_mismatch'}
    case.loader.assert_not_called()


def test_missing_run_cutoff_cannot_trust_forecasts_own_cutoff(persisted_forecasts):
    case = persisted_forecasts
    case.run.request_payload = {}

    decisions = case.build(None)

    assert all(decision.should_dispatch for decision in decisions.values())
    assert {decision.reason for decision in decisions.values()} == {
        'forecast_evidence_invalid:timezone_aware_cutoff_required'
    }
    case.loader.assert_not_called()
    case.worker.assert_not_called()
