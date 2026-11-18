"""Persisted admission transfer is an explicit, internal, one-shot opt-in."""

import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timezone
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.db import engine
from app.db_models import (
    AgentContextFeedbackRun, DailyPipelineRun, DailyPipelineStageRun,
    EODCollectionRun, Symbol, DailyPriceBar,
)
from app.eod.daily_pipeline_state import DailyPipelineRunStateService

MODULE = 'app.eod.daily_pipeline_state.'
ROLES = ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer']
FULL = ['000001', '000002', '000003']
SELECTED = FULL[:2]
SUFFIX = ['signal_decider', 'next_open_execution_plan']
CUTOFF = '2040-01-09T12:39:40+00:00'
ERROR = ('required feedback reuse parent does not match the requested cutoff, '
         'policy context, fs_div, or exact scope contract')


class PreopenSummaryContinuationTests(unittest.TestCase):
    def setUp(self):
        self.schema = 'preopen_continuation_' + uuid4().hex
        with engine.begin() as connection:
            connection.execute(CreateSchema(self.schema))
        self.addCleanup(self.drop_schema)
        test_engine = engine.execution_options(schema_translate_map={None: self.schema})
        for model in (DailyPipelineRun, DailyPipelineStageRun, EODCollectionRun,
                      AgentContextFeedbackRun, Symbol, DailyPriceBar):
            model.__table__.create(test_engine)
        self.sessions = sessionmaker(bind=test_engine, expire_on_commit=False)

        @contextmanager
        def scope(**_kwargs):
            with self.sessions.begin() as session:
                yield session

        self.now = datetime(2040, 1, 10, 1, tzinfo=timezone.utc)
        self.market_date = date(2040, 1, 9)
        self.service = DailyPipelineRunStateService()
        self.proof = {
            'source_run_id': 'summary-retry', 'knowledge_cutoff_at': CUTOFF,
            'requested_symbols': FULL, 'expected_completed_symbols': SELECTED,
            'feedback_scope_mode': 'decider_scope_fast_path', 'fs_div': 'CFS',
        }
        self.metadata = {
            'entrypoint': 'daily_pipeline_stage_service.run', 'symbols': FULL,
            'force': False, 'reuse_existing_feedback': True,
            'bypass_prerequisite_gate': False, 'allow_expired_analysis_replay': False,
            'point_in_time_recovery': True, 'fs_div': 'CFS',
            'resume_from_stage': SUFFIX[0], 'reused_eod_collection_run_id': 598,
        }
        request = {
            'market_date': str(self.market_date),
            'policy_context_as_of_date': str(self.market_date),
            'requested_symbols': FULL, 'symbols': SELECTED,
            'roles': ROLES, 'role_symbol_scopes': {r: SELECTED for r in ROLES},
            'admission_scope_by_role': {r: FULL for r in ROLES},
            'prepared_decider_role_symbol_scopes': {},
            'fs_div': 'CFS', 'knowledge_cutoff_at': CUTOFF,
            'admission_knowledge_cutoff_at': CUTOFF,
            'requested_knowledge_cutoff_at': CUTOFF,
            'point_in_time_recovery': False, 'reuse_existing': True,
            'force_recompute': False, 'feedback_scope_mode': 'decider_scope_fast_path',
            'mode': 'daily_retry_cycle_role_sequenced',
        }
        source_metadata = {
            **deepcopy(self.metadata),
            'requested_stages': ['agent_feedback_summaries', *SUFFIX],
            'resolved_stages': ['agent_feedback_summaries', *SUFFIX],
            'resume_from_stage': 'agent_feedback_summaries',
            'previous_session_preopen_recovery_admitted': True,
            'inherited_feedback_prerequisite_admission': True,
            'inherited_feedback_source_run_id': 'summary-origin',
            'inherited_feedback_knowledge_cutoff_at': CUTOFF,
        }
        with self.sessions.begin() as session:
            session.add(EODCollectionRun(
                id=598, market='KRX', source='pykrx', trade_date=self.market_date,
                status='completed', symbols_total=3, symbols_succeeded=3,
                symbols_failed=0, completed_at=datetime(2040, 1, 9, 21, tzinfo=timezone.utc),
            ))
            session.flush()
            for code in FULL:
                symbol = Symbol(symbol=code, name_ko=code, market='KOSPI')
                session.add(symbol)
                session.flush()
                session.add(DailyPriceBar(
                    symbol_id=symbol.id, eod_run_id=598, trade_date=self.market_date,
                    open_price=100, high_price=100, low_price=100, close_price=100,
                    volume=1, turnover=100, source='pykrx',
                ))
            session.add(DailyPipelineRun(
                id=664, market_date=self.market_date, trigger='auto_eod_followup_resume',
                status='failed', current_stage='agent_feedback_summaries',
                started_at=datetime(2040, 1, 9, 22, 54, tzinfo=timezone.utc),
                completed_at=datetime(2040, 1, 9, 22, 55, tzinfo=timezone.utc),
                error_message=ERROR, run_metadata=source_metadata,
            ))
            session.flush()
            session.add(DailyPipelineStageRun(
                pipeline_run_id=664, market_date=self.market_date,
                stage_id='agent_feedback_summaries', status='failed',
                error_message=ERROR, processed=0, total=0, succeeded=0, failed=0,
                source_run_ids={}, details={},
            ))
            for run_id, started, status in (
                ('summary-origin', datetime(2040, 1, 9, 13, tzinfo=timezone.utc), 'partial'),
                ('summary-retry', datetime(2040, 1, 9, 22, 58, tzinfo=timezone.utc), 'completed'),
            ):
                session.add(AgentContextFeedbackRun(
                    run_id=run_id, market_date=self.market_date, status=status,
                    request_payload={**deepcopy(request), **({'resumed_from_run_id': 'summary-origin'}
                                     if run_id == 'summary-retry' else {})},
                    summary_payload={'resumed_from_run_id': 'summary-origin'}
                    if run_id == 'summary-retry' else {},
                    started_at=started, completed_at=(self.now if run_id == 'summary-retry'
                                                   else datetime(2040, 1, 9, 14, tzinfo=timezone.utc)),
                    symbols_total=6, completed_count=6, failed_count=0,
                ))
        for target, kwargs in (
            ('session_scope', {'new': scope}),
            ('_utcnow', {'return_value': self.now}),
            ('eod_collection_advisory_lock_is_held', {'return_value': True, 'create': True}),
            ('_preopen_continuation_target_session', {'return_value': date(2040, 1, 10), 'create': True}),
            ('_resolve_terminal_summary_knowledge_cutoff', {'return_value': {
                'ready': True, 'source_run_id': 'summary-retry',
                'knowledge_cutoff_at': datetime.fromisoformat(CUTOFF),
                'requested_symbols': SELECTED,
                'completed_scope_proof': {'completed_symbols': SELECTED},
            }}),
        ):
            active = patch(MODULE + target, **kwargs)
            active.start()
            self.addCleanup(active.stop)

    def drop_schema(self):
        with engine.begin() as connection:
            connection.execute(DropSchema(self.schema, cascade=True))

    def claim(self):
        return self.service.start_run(
            market_date=self.market_date, trigger='auto_eod_followup_resume',
            requested_stages=SUFFIX, resolved_stages=SUFFIX,
            run_metadata=self.metadata, provider_admission_fence_owned=True,
            preopen_continuation_source_pipeline_run_id=664,
            preopen_continuation_summary_proof=self.proof,
        )

    def test_explicit_claim_creates_one_audited_cohort_without_rewriting_source(self):
        result = self.claim()
        audit = result['run_metadata']['preopen_summary_continuation']
        self.assertEqual(audit['source_pipeline_run_id'], 664)
        self.assertEqual(audit['summary_feedback_run_id'], 'summary-retry')
        self.assertTrue(result['run_metadata']['same_day_intraday_watch_continuation_admitted'])
        with self.sessions.begin() as session:
            source = session.get(DailyPipelineRun, 664)
            self.assertEqual(source.status, 'failed')
            self.assertEqual(source.error_message, ERROR)
            self.assertNotIn('preopen_summary_continuation', source.run_metadata)
            new_run = session.get(DailyPipelineRun, result['id'])
            new_run.status = 'failed'
        again = self.claim()
        self.assertEqual(again['id'], result['id'])
        self.assertTrue(again['deduplicated'])
        self.assertEqual(again['deduplication_reason'], 'preopen_continuation_already_claimed')

    def test_missing_actual_fence_does_not_accept_callers_boolean(self):
        with patch(MODULE + 'eod_collection_advisory_lock_is_held', return_value=False):
            with self.assertRaisesRegex(ValueError, 'owned.*fence'):
                self.claim()

    def test_bad_persisted_evidence_is_rejected_without_creating_cohort(self):
        cases = [
            ('different EOD', DailyPipelineRun, 664, 'run_metadata',
             lambda m: {**m, 'reused_eod_collection_run_id': 599}),
            ('source late', DailyPipelineRun, 664, 'started_at',
             lambda _: datetime(2040, 1, 10, 0, tzinfo=timezone.utc)),
            ('source unknown failure', DailyPipelineRun, 664, 'error_message', lambda _: 'other'),
            ('source future finish', DailyPipelineRun, 664, 'completed_at', lambda _: self.now),
            ('EOD incomplete', EODCollectionRun, 598, 'symbols_failed', lambda _: 1),
            ('price foreign ownership', DailyPriceBar, 1, 'eod_run_id', lambda _: None),
            ('admission', DailyPipelineRun, 664, 'run_metadata',
             lambda m: {**m, 'previous_session_preopen_recovery_admitted': False}),
            ('source output', DailyPipelineStageRun, 1, 'source_run_ids', lambda _: {'run_id': 'already-ran'}),
            ('summary not complete', AgentContextFeedbackRun, 2, 'status', lambda _: 'running'),
            ('late summary', AgentContextFeedbackRun, 2, 'started_at',
             lambda _: datetime(2040, 1, 10, 0, tzinfo=timezone.utc)),
            ('different parent', AgentContextFeedbackRun, 2, 'summary_payload',
             lambda _: {'resumed_from_run_id': 'unrelated'}),
            ('scope changed', AgentContextFeedbackRun, 2, 'request_payload',
             lambda q: {**q, 'requested_symbols': FULL[:1]}),
            ('future summary completion', AgentContextFeedbackRun, 2, 'completed_at',
             lambda _: datetime(2040, 1, 10, 2, tzinfo=timezone.utc)),
            ('CFS changed', AgentContextFeedbackRun, 2, 'request_payload',
             lambda q: {**q, 'fs_div': 'OFS'}),
            ('role scope changed', AgentContextFeedbackRun, 2, 'request_payload',
             lambda q: {**q, 'role_symbol_scopes': {r: FULL[:1] for r in ROLES}}),
            ('policy changed', AgentContextFeedbackRun, 2, 'request_payload',
             lambda q: {**q, 'policy_context_as_of_date': '2040-01-10'}),
            ('cutoff changed', AgentContextFeedbackRun, 2, 'request_payload',
             lambda q: {**q, 'knowledge_cutoff_at': '2040-01-09T15:00:00+00:00'}),
        ]
        for label, model, key, field, change in cases:
            with self.subTest(label=label):
                with self.sessions.begin() as session:
                    row = session.get(model, key)
                    old = deepcopy(getattr(row, field))
                    setattr(row, field, change(old))
                try:
                    with self.assertRaises(ValueError):
                        self.claim()
                    with self.sessions.begin() as session:
                        self.assertEqual(len(list(session.scalars(select(DailyPipelineRun)))), 1)
                finally:
                    with self.sessions.begin() as session:
                        setattr(session.get(model, key), field, old)

    def test_missing_native_completed_receipts_are_not_replaced_by_callers_proof(self):
        with patch(MODULE + '_resolve_terminal_summary_knowledge_cutoff',
                   return_value={'ready': False}):
            with self.assertRaisesRegex(ValueError, 'receipt proof'):
                self.claim()

    def test_prior_decider_stage_or_newer_feedback_prevents_adoption(self):
        with self.sessions.begin() as session:
            session.add(DailyPipelineStageRun(
                pipeline_run_id=664, market_date=self.market_date,
                stage_id='signal_decider', status='pending',
            ))
        with self.assertRaisesRegex(ValueError, 'stage history'):
            self.claim()
        with self.sessions.begin() as session:
            stage = session.scalar(select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.stage_id == 'signal_decider'))
            session.delete(stage)
            session.add(AgentContextFeedbackRun(
                run_id='newer-summary', market_date=self.market_date, status='running',
                request_payload={'roles': ROLES},
            ))
        with self.assertRaisesRegex(ValueError, 'newer feedback'):
            self.claim()

    def test_no_opt_in_does_not_transfer_preopen_authority(self):
        result = self.service.start_run(
            market_date=self.market_date, trigger='auto_eod_followup_resume',
            requested_stages=SUFFIX, resolved_stages=SUFFIX,
            run_metadata=self.metadata, provider_admission_fence_owned=True,
        )
        self.assertNotIn('preopen_summary_continuation', result['run_metadata'])
        self.assertIsNot(result['run_metadata'].get('same_day_intraday_watch_continuation_admitted'), True)

    def test_audit_metadata_cannot_be_supplied_as_permission(self):
        self.metadata['preopen_summary_continuation'] = {'source_pipeline_run_id': 664}
        with self.assertRaisesRegex(ValueError, 'native-only'):
            self.claim()

    def test_concurrent_explicit_calls_spend_the_source_once(self):
        with ThreadPoolExecutor(max_workers=2) as workers:
            first, second = list(workers.map(lambda _: self.claim(), range(2)))
        self.assertEqual(first['id'], second['id'])
        self.assertEqual(sum(bool(r.get('deduplicated')) for r in (first, second)), 1)
        with self.sessions.begin() as session:
            self.assertEqual(len(list(session.scalars(select(DailyPipelineRun)))), 2)

    def test_claim_uses_the_real_completed_summary_resolver_in_its_transaction(self):
        from app.agent_context.retry_service import resolve_latest_completed_summary_knowledge_cutoff

        def native_resolve(**kwargs):
            return resolve_latest_completed_summary_knowledge_cutoff(now=self.now, **kwargs)

        with patch(MODULE + '_resolve_terminal_summary_knowledge_cutoff', side_effect=native_resolve):
            result = self.claim()
        self.assertEqual(result['run_metadata']['preopen_summary_continuation'][
            'summary_feedback_run_id'], 'summary-retry')


    def test_consumed_source_deduplicates_after_its_decider_has_written_feedback(self):
        first = self.claim()
        with self.sessions.begin() as session:
            session.add(AgentContextFeedbackRun(
                run_id='claimed-child-decider', market_date=self.market_date,
                status='completed', request_payload={'roles': ['signal-decider']},
            ))
        again = self.claim()
        self.assertEqual(again['id'], first['id'])
        self.assertTrue(again['deduplicated'])


class PreopenTargetSessionTests(unittest.TestCase):
    def test_only_the_exact_known_next_krx_open_session_is_accepted(self):
        from types import SimpleNamespace
        from app.eod.daily_pipeline_state import _preopen_continuation_target_session

        now = datetime(2040, 1, 10, 1, tzinfo=timezone.utc)
        for target, state, is_open, current_date in (
            (None, 'OPEN', True, '2040-01-10'),
            (date(2040, 1, 11), 'OPEN', True, '2040-01-10'),
            (date(2040, 1, 10), 'CLOSED', False, '2040-01-10'),
            (date(2040, 1, 10), 'PREOPEN', False, '2040-01-10'),
        ):
            with self.subTest(target=target, state=state), patch(
                'app.market.session.next_krx_trading_date', return_value=target,
            ) as calendar, patch('app.market.clock.get_market_state', return_value=SimpleNamespace(
                state=state, is_open=is_open, current_calendar_date=current_date,
            )):
                with self.assertRaisesRegex(ValueError, 'target OPEN'):
                    _preopen_continuation_target_session(date(2040, 1, 9), now)
                calendar.assert_called_once_with(date(2040, 1, 9), allow_fallback=False)
