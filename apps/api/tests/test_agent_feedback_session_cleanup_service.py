import json
import sqlite3
import subprocess
import tempfile
import unittest
import uuid
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

from sqlalchemy import delete, select
from sqlalchemy.dialects import postgresql

from app.agent_context import session_cleanup_service as session_cleanup_module
from app.agent_context.session_cleanup_service import AgentFeedbackSessionCleanupService, SessionCleanupTarget
from app.db import session_scope
from app.db_models import AgentFeedbackSessionCleanupFailure


class AgentFeedbackSessionCleanupServiceTests(unittest.TestCase):
    @staticmethod
    def _native_identity(
        *,
        session_id: str = '07651bdc-b2c4-45c7-a74b-06a68ab441fe',
        thread_id: str = '01a1578b-9067-70c0-b6f2-7df93723ba82',
    ) -> dict:
        return {
            'version': 1,
            'agent_name': 'eod-summarizer',
            'session_key': 'agent:eod-summarizer:feedback/eod-summarizer/slot-01',
            'openclaw_session_id': session_id,
            'openclaw_lifecycle_revision': 'lifecycle-revision-01',
            'openclaw_session_updated_at': 1792360031022,
            'binding_entry_key': 'session-key:eod-summarizer:test-binding',
            'codex_thread_id': thread_id,
            'agent_home': '/tmp/openclaw/agents/eod-summarizer/agent',
            'codex_home': '/tmp/openclaw/agents/eod-summarizer/agent/codex-home',
            'codex_state_db': (
                '/tmp/openclaw/agents/eod-summarizer/agent/codex-home/state_5.sqlite'
            ),
            'codex_rollout_path': (
                '/tmp/openclaw/agents/eod-summarizer/agent/codex-home/'
                f'sessions/2026/10/20/rollout-{thread_id}.jsonl'
            ),
            'codex_app_server_version': '0.160.0',
        }

    def _create_native_fixture(self, root: session_cleanup_module.Path) -> tuple[SessionCleanupTarget, dict]:
        agent_name = 'eod-summarizer'
        session_key = 'agent:eod-summarizer:feedback/eod-summarizer/slot-01'
        session_id = '07651bdc-b2c4-45c7-a74b-06a68ab441fe'
        thread_id = '01a1578b-9067-70c0-b6f2-7df93723ba82'
        agent_home = root / 'agents' / agent_name / 'agent'
        codex_home = agent_home / 'codex-home'
        rollout_path = (
            codex_home
            / 'sessions'
            / '2026'
            / '10'
            / '20'
            / f'rollout-{thread_id}.jsonl'
        )
        rollout_path.parent.mkdir(parents=True)
        rollout_path.write_text('{}\n', encoding='utf-8')
        (root / 'state').mkdir(parents=True)
        (root / 'openclaw.json').write_text(
            json.dumps(
                {
                    'agents': {
                        'entries': {
                            agent_name: {
                                'agentDir': str(agent_home),
                            }
                        }
                    }
                }
            ),
            encoding='utf-8',
        )

        with sqlite3.connect(agent_home / 'openclaw-agent.sqlite') as connection:
            connection.executescript(
                '''
                CREATE TABLE session_nodes (
                  session_key TEXT PRIMARY KEY,
                  current_session_id TEXT NOT NULL,
                  entry_json TEXT NOT NULL,
                  status TEXT,
                  updated_at INTEGER NOT NULL
                );
                CREATE TABLE session_windows (
                  session_id TEXT PRIMARY KEY,
                  status TEXT
                );
                CREATE TABLE session_pending_inputs (
                  session_id TEXT NOT NULL,
                  state TEXT NOT NULL
                );
                '''
            )
            connection.execute(
                'INSERT INTO session_nodes VALUES (?, ?, ?, ?, ?)',
                (
                    session_key,
                    session_id,
                    json.dumps(
                        {
                            'sessionId': session_id,
                            'lifecycleRevision': 'lifecycle-revision-01',
                            'updatedAt': 1792360031022,
                            'status': 'done',
                        }
                    ),
                    'done',
                    1792360031022,
                ),
            )
            connection.execute(
                'INSERT INTO session_windows VALUES (?, ?)',
                (session_id, 'done'),
            )

        codex_state_db = codex_home / 'state_5.sqlite'
        with sqlite3.connect(codex_state_db) as connection:
            connection.execute(
                'CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT NOT NULL)'
            )
            connection.execute(
                'INSERT INTO threads VALUES (?, ?)',
                (thread_id, str(rollout_path)),
            )

        service = AgentFeedbackSessionCleanupService()
        binding_entry_key = service._codex_binding_entry_key(
            agent_name=agent_name,
            session_key=session_key,
        )
        fingerprint = json.dumps(
            {
                'runtimeIdentity': {
                    'codexHome': str(codex_home),
                    'serverVersion': '0.160.0',
                }
            }
        )
        with sqlite3.connect(root / 'state' / 'openclaw.sqlite') as connection:
            connection.execute(
                '''
                CREATE TABLE plugin_state_entries (
                  plugin_id TEXT NOT NULL,
                  namespace TEXT NOT NULL,
                  entry_key TEXT NOT NULL,
                  value_json TEXT NOT NULL,
                  PRIMARY KEY (plugin_id, namespace, entry_key)
                )
                '''
            )
            connection.execute(
                'INSERT INTO plugin_state_entries VALUES (?, ?, ?, ?)',
                (
                    'codex',
                    'app-server-thread-bindings',
                    binding_entry_key,
                    json.dumps(
                        {
                            'version': 1,
                            'state': 'active',
                            'sessionId': session_id,
                            'binding': {
                                'threadId': thread_id,
                                'rolloutPath': str(rollout_path),
                                'appServerRuntimeFingerprint': fingerprint,
                            },
                        }
                    ),
                ),
            )
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name=agent_name,
            run_id='feedback-run-01',
            session_key=session_key,
        )
        return target, {
            'agent_home': agent_home,
            'codex_home': codex_home,
            'codex_state_db': codex_state_db,
            'rollout_path': rollout_path,
            'session_id': session_id,
            'thread_id': thread_id,
        }

    def test_record_failure_normalizes_structured_error_before_text_upsert(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 10, 30),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-structured-error',
            session_key=f'feedback/eod-summarizer/structured-error-{uuid.uuid4().hex}',
        )
        result = {
            'ok': False,
            'deleted': False,
            'error': ['gateway', 'still unavailable'],
        }
        session = MagicMock()
        existing_result = MagicMock()
        existing_result.scalar_one_or_none.return_value = None
        insert_result = MagicMock()
        insert_result.scalar_one.return_value = 1
        session.execute.side_effect = [existing_result, insert_result]
        scoped_session = MagicMock()
        scoped_session.__enter__.return_value = session
        scoped_session.__exit__.return_value = False

        with (
            patch.object(session_cleanup_module, 'session_scope', return_value=scoped_session),
            patch.object(service, '_serialize_failure', return_value={'id': 1}),
        ):
            service._record_failure(
                target=target,
                result=result,
                deadline_monotonic=1_000_000_000.0,
            )

        statement = session.execute.call_args_list[1].args[0]
        compiled = statement.compile(dialect=postgresql.dialect())
        normalized_values = [
            value
            for value in compiled.params.values()
            if value == '["gateway", "still unavailable"]'
        ]
        self.assertGreaterEqual(len(normalized_values), 2)
        self.assertNotIn(result['error'], compiled.params.values())

    def test_normalize_last_error_prefers_structured_message(self) -> None:
        service = AgentFeedbackSessionCleanupService()

        self.assertEqual(
            service._normalize_last_error(
                {
                    'type': 'gateway_transport_error',
                    'message': ' gateway connection closed ',
                }
            ),
            'gateway connection closed',
        )

    def test_record_failure_prefers_stderr_over_empty_structured_error(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 10, 30),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-empty-structured-error',
            session_key=f'feedback/eod-summarizer/empty-error-{uuid.uuid4().hex}',
        )
        session = MagicMock()
        existing_result = MagicMock()
        existing_result.scalar_one_or_none.return_value = None
        insert_result = MagicMock()
        insert_result.scalar_one.return_value = 1
        session.execute.side_effect = [existing_result, insert_result]
        scoped_session = MagicMock()
        scoped_session.__enter__.return_value = session
        scoped_session.__exit__.return_value = False

        with (
            patch.object(session_cleanup_module, 'session_scope', return_value=scoped_session),
            patch.object(service, '_serialize_failure', return_value={'id': 1}),
        ):
            service._record_failure(
                target=target,
                result={
                    'ok': False,
                    'deleted': False,
                    'error': {'message': '  '},
                    'stderr': 'gateway stderr detail',
                },
                deadline_monotonic=1_000_000_000.0,
            )

        statement = session.execute.call_args_list[1].args[0]
        compiled = statement.compile(dialect=postgresql.dialect())
        self.assertGreaterEqual(
            sum(
                value == 'gateway stderr detail'
                for value in compiled.params.values()
            ),
            2,
        )

    def test_enqueue_deferred_bypasses_shared_pool_wait_for_deadline_write(
        self,
    ) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-isolated-deadline',
            session_key='feedback/eod-summarizer/isolated-deadline',
        )
        session = MagicMock()
        insert_result = MagicMock()
        insert_result.scalars.return_value.all.return_value = [1]
        stored_row = MagicMock()
        stored_row.session_key = target.session_key
        stored_row.cleanup_result = {
            'native_cleanup_identity': self._native_identity()
        }
        select_result = MagicMock()
        select_result.scalars.return_value.all.return_value = [stored_row]
        session.execute.side_effect = [insert_result, select_result]

        with (
            patch.object(session_cleanup_module.time, 'monotonic', return_value=100.0),
            patch.object(session_cleanup_module.settings, 'db_pool_timeout_seconds', 120),
            patch.object(session_cleanup_module.settings, 'db_connect_timeout_seconds', 5),
            patch.object(
                service,
                '_capture_native_cleanup_identity',
                return_value={
                    'ok': True,
                    'identity': self._native_identity(),
                },
            ),
            patch.object(session_cleanup_module, 'session_scope') as scoped_session,
        ):
            scoped_session.return_value.__enter__.return_value = session
            result = service.enqueue_deferred(
                targets=[target],
                reason='cleanup_time_budget_exhausted',
                deadline_monotonic=106.0,
            )

        self.assertTrue(result['persistence_confirmed'])
        scoped_session.assert_called_once_with(
            statement_timeout_ms=(
                session_cleanup_module.settings.agent_feedback_db_statement_timeout_ms
            ),
            lock_timeout_ms=(
                session_cleanup_module.settings.agent_feedback_db_lock_timeout_ms
            ),
            deadline_monotonic=106.0,
            isolated_connection=True,
        )

    def test_enqueue_deferred_rejects_budget_that_cannot_bound_isolated_connect(
        self,
    ) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-deadline',
            session_key='feedback/eod-summarizer/deadline',
        )

        with (
            patch.object(session_cleanup_module.time, 'monotonic', return_value=100.0),
            patch.object(session_cleanup_module.settings, 'db_pool_timeout_seconds', 120),
            patch.object(session_cleanup_module.settings, 'db_connect_timeout_seconds', 5),
            patch.object(session_cleanup_module, 'session_scope') as scoped_session,
        ):
            with self.assertRaises(
                session_cleanup_module.CleanupPersistenceDeadlineExceeded
            ):
                service.enqueue_deferred(
                    targets=[target],
                    reason='cleanup_time_budget_exhausted',
                    deadline_monotonic=104.0,
                )

        scoped_session.assert_not_called()

    def test_enqueue_deferred_rechecks_connect_budget_after_identity_capture(
        self,
    ) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-capture-deadline',
            session_key='feedback/eod-summarizer/capture-deadline',
        )

        with (
            patch.object(
                session_cleanup_module.time,
                'monotonic',
                side_effect=[100.0, 100.0, 105.5],
            ),
            patch.object(session_cleanup_module.settings, 'db_connect_timeout_seconds', 5),
            patch.object(
                service,
                '_capture_native_cleanup_identity',
                return_value={
                    'ok': True,
                    'identity': self._native_identity(),
                },
            ),
            patch.object(session_cleanup_module, 'session_scope') as scoped_session,
        ):
            with self.assertRaises(
                session_cleanup_module.CleanupPersistenceDeadlineExceeded
            ):
                service.enqueue_deferred(
                    targets=[target],
                    reason='cleanup_time_budget_exhausted',
                    deadline_monotonic=106.0,
                )

        scoped_session.assert_not_called()

    def test_extract_json_parses_multiline_final_object_after_doctor_notice(self) -> None:
        stdout = '''
Doctor notices --------------------------------------------------------------
- plugins.entries.legacy-auth: plugin not found (stale config entry ignored)
Run "openclaw doctor --fix" to remove stale entries.
{
  "ok": true,
  "deleted": true,
  "session": {
    "key": "feedback/eod-summarizer/slot-01"
  }
}
'''

        result = AgentFeedbackSessionCleanupService._extract_json(stdout)

        self.assertEqual(
            result,
            {
                'ok': True,
                'deleted': True,
                'session': {'key': 'feedback/eod-summarizer/slot-01'},
            },
        )

    def test_read_only_sqlite_bounds_busy_timeout_by_cleanup_deadline(self) -> None:
        connection = MagicMock()

        with (
            patch.object(session_cleanup_module.time, 'monotonic', return_value=100.0),
            patch.object(
                session_cleanup_module.sqlite3,
                'connect',
                return_value=connection,
            ) as connect,
        ):
            result = AgentFeedbackSessionCleanupService._read_only_sqlite(
                session_cleanup_module.Path('/tmp/openclaw.sqlite'),
                deadline_monotonic=102.5,
            )

        self.assertIs(result, connection)
        self.assertEqual(connect.call_args.kwargs['timeout'], 2.5)
        connection.execute.assert_called_once_with('PRAGMA query_only = ON')

    def test_invoke_delete_requests_documented_json_output(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"ok": true, "deleted": true}',
            stderr='',
        )

        with (
            patch('app.agent_context.session_cleanup_service.settings.openclaw_gateway_token', 'token'),
            patch('app.agent_context.session_cleanup_service.subprocess.run', return_value=completed) as run,
        ):
            result = service._invoke_delete(
                session_key='feedback/eod-summarizer/slot-01',
                agent_name='eod-summarizer',
                expected_session_id='07651bdc-b2c4-45c7-a74b-06a68ab441fe',
                expected_lifecycle_revision='lifecycle-revision-01',
                expected_session_updated_at=1792360031022,
                delete_transcript=True,
                emit_lifecycle_hooks=False,
                deadline_monotonic=float('inf'),
            )

        self.assertTrue(result['ok'])
        self.assertIn('--json', run.call_args.args[0])
        params = json.loads(run.call_args.args[0][run.call_args.args[0].index('--params') + 1])
        self.assertEqual(params['agentId'], 'eod-summarizer')
        self.assertEqual(
            params['expectedSessionId'],
            '07651bdc-b2c4-45c7-a74b-06a68ab441fe',
        )
        self.assertEqual(
            params['expectedLifecycleRevision'],
            'lifecycle-revision-01',
        )
        self.assertEqual(params['expectedSessionUpdatedAt'], 1792360031022)

    def test_invoke_delete_keeps_gateway_token_out_of_process_arguments(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"ok": true, "deleted": true}',
            stderr='',
        )

        with (
            patch(
                'app.agent_context.session_cleanup_service.settings.openclaw_gateway_token',
                'secret-token',
            ),
            patch(
                'app.agent_context.session_cleanup_service.subprocess.run',
                return_value=completed,
            ) as run,
        ):
            result = service._invoke_delete(
                session_key='feedback/eod-summarizer/slot-01',
                agent_name='eod-summarizer',
                expected_session_id='07651bdc-b2c4-45c7-a74b-06a68ab441fe',
                expected_lifecycle_revision='lifecycle-revision-01',
                expected_session_updated_at=1792360031022,
                delete_transcript=True,
                emit_lifecycle_hooks=False,
                deadline_monotonic=float('inf'),
            )

        self.assertTrue(result['ok'])
        command = run.call_args.args[0]
        self.assertNotIn('--token', command)
        self.assertNotIn('secret-token', command)
        self.assertEqual(
            run.call_args.kwargs['env']['OPENCLAW_GATEWAY_TOKEN'],
            'secret-token',
        )

    def test_pending_cleanup_does_not_start_an_item_that_cannot_fit_run_budget(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        rows = [MagicMock(), MagicMock(), MagicMock()]
        targets = [
            SessionCleanupTarget(
                market_date=date(2026, 10, 19),
                symbol=f'00593{index}',
                agent_name='eod-summarizer',
                run_id='feedback-run-budgeted-cleanup',
                session_key=f'feedback/eod-summarizer/budget-{index}',
            )
            for index in range(3)
        ]
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = rows

        with (
            patch.object(session_cleanup_module, 'session_scope') as scoped_session,
            patch.object(
                service,
                '_target_from_failure',
                side_effect=targets,
            ),
            patch.object(
                service,
                'delete_now',
                return_value={'status': 'deleted'},
            ) as delete_now,
            patch.object(
                session_cleanup_module.settings,
                'openclaw_session_cleanup_timeout_seconds',
                30,
            ),
            patch.object(
                session_cleanup_module.settings,
                'db_connect_timeout_seconds',
                5,
            ),
            patch.object(
                session_cleanup_module.time,
                'monotonic',
                side_effect=[100.0, 100.0, 100.0, 106.0],
            ),
        ):
            scoped_session.return_value.__enter__.return_value = session
            result = service.run_pending_cleanup(
                limit=3,
                max_runtime_seconds=35,
            )

        delete_now.assert_called_once_with(
            target=targets[0],
            deadline_monotonic=135.0,
        )
        self.assertEqual(result['summary']['selected'], 3)
        self.assertEqual(result['summary']['attempted'], 1)
        self.assertEqual(result['summary']['deferred_for_budget'], 2)
        self.assertTrue(result['summary']['budget_exhausted'])

    def test_pending_cleanup_does_not_exempt_first_item_from_run_budget(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        row = MagicMock()
        target = SessionCleanupTarget(
            market_date=date(2026, 10, 19),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-short-cleanup-budget',
            session_key='feedback/eod-summarizer/short-budget',
        )
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [
            row
        ]

        with (
            patch.object(
                session_cleanup_module,
                'session_scope',
            ) as scoped_session,
            patch.object(
                service,
                '_target_from_failure',
                return_value=target,
            ),
            patch.object(service, 'delete_now') as delete_now,
            patch.object(
                session_cleanup_module.settings,
                'openclaw_session_cleanup_timeout_seconds',
                30,
            ),
            patch.object(
                session_cleanup_module.settings,
                'db_connect_timeout_seconds',
                1,
            ),
            patch.object(
                session_cleanup_module.time,
                'monotonic',
                side_effect=[100.0, 100.0, 100.0],
            ),
        ):
            scoped_session.return_value.__enter__.return_value = session
            result = service.run_pending_cleanup(
                limit=1,
                max_runtime_seconds=5,
            )

        delete_now.assert_not_called()
        self.assertEqual(result['summary']['attempted'], 0)
        self.assertEqual(result['summary']['deferred_for_budget'], 1)
        self.assertTrue(result['summary']['budget_exhausted'])

    def test_pending_cleanup_defers_selection_when_database_connect_cannot_fit_budget(self) -> None:
        service = AgentFeedbackSessionCleanupService()

        with (
            patch.object(session_cleanup_module, 'session_scope') as scoped_session,
            patch.object(
                service,
                '_require_database_budget',
                side_effect=session_cleanup_module.CleanupSafetyError(
                    'cleanup_deadline_exceeded',
                    'database connect cannot fit cleanup budget',
                ),
            ) as require_database_budget,
            patch.object(session_cleanup_module.time, 'monotonic', return_value=100.0),
        ):
            result = service.run_pending_cleanup(
                limit=1,
                max_runtime_seconds=5,
            )

        require_database_budget.assert_called_once_with(105.0)
        scoped_session.assert_not_called()
        self.assertEqual(result['summary']['selected'], 0)
        self.assertEqual(result['summary']['attempted'], 0)
        self.assertTrue(result['summary']['budget_exhausted'])
        self.assertTrue(result['summary']['selection_deferred_for_budget'])

    def test_pending_cleanup_defers_selection_when_database_checkout_exhausts_deadline(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        scoped_session = MagicMock()
        scoped_session.return_value.__enter__.side_effect = TimeoutError(
            'database session deadline exhausted during checkout'
        )

        with (
            patch.object(
                session_cleanup_module,
                'session_scope',
                scoped_session,
            ),
            patch.object(service, '_require_database_budget'),
            patch.object(session_cleanup_module.time, 'monotonic', return_value=100.0),
        ):
            result = service.run_pending_cleanup(
                limit=1,
                max_runtime_seconds=5,
            )

        self.assertEqual(result['summary']['selected'], 0)
        self.assertEqual(result['summary']['attempted'], 0)
        self.assertTrue(result['summary']['budget_exhausted'])
        self.assertTrue(result['summary']['selection_deferred_for_budget'])

    def test_pending_cleanup_skips_manual_blocker_without_starving_retry(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        retry_session_key = 'feedback/eod-summarizer/retry-after-manual-blocker'
        row = MagicMock()
        row.market_date = date(2026, 10, 19)
        row.symbol = '000660'
        row.agent_name = 'eod-summarizer'
        row.run_id = 'feedback-run-valid-retry'
        row.session_key = retry_session_key
        row.delete_transcript = True
        row.emit_lifecycle_hooks = False
        row.cleanup_result = {
            'ok': False,
            'retryable': True,
            'native_cleanup_identity': self._native_identity(),
        }
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [row]

        with (
            patch.object(session_cleanup_module, 'session_scope') as scoped_session,
            patch.object(
                service,
                'delete_now',
                return_value={'status': 'failed'},
            ) as delete_now,
        ):
            scoped_session.return_value.__enter__.return_value = session
            result = service.run_pending_cleanup(limit=1)

        pending_stmt = session.execute.call_args.args[0]
        compiled_sql = str(
            pending_stmt.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={'literal_binds': True},
            )
        ).lower()
        self.assertIn('manual_reconciliation_required', compiled_sql)
        self.assertIn('coalesce', compiled_sql)
        self.assertIn('is false', compiled_sql)
        attempted_target = delete_now.call_args.kwargs['target']
        self.assertEqual(attempted_target.session_key, retry_session_key)
        self.assertEqual(result['summary']['selected'], 1)
        self.assertEqual(result['summary']['attempted'], 1)

    def test_delete_now_finalizes_after_openclaw_and_native_state_are_absent(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='feedback/eod-summarizer/slot-01',
        )

        identity = self._native_identity()
        with (
            patch.object(
                service,
                '_prepare_cleanup',
                return_value={
                    'ok': True,
                    'identity': identity,
                    'openclaw_session_present': False,
                },
            ),
            patch.object(service, '_invoke_delete') as invoke_delete,
            patch.object(service, '_binding_owner_entry_keys', return_value=[]),
            patch.object(
                service,
                '_assess_native_cleanup',
                return_value={'ok': True, 'outcome': 'already_absent'},
            ) as assess_native,
            patch.object(service, '_clear_failure') as clear_failure,
            patch.object(service, '_record_failure') as record_failure,
        ):
            result = service.delete_now(target=target)

        self.assertEqual(result['status'], 'deleted')
        self.assertEqual(result['outcome'], 'already_absent')
        invoke_delete.assert_not_called()
        assess_native.assert_called_once()
        clear_failure.assert_called_once()
        self.assertEqual(
            clear_failure.call_args.kwargs['session_key'],
            target.session_key,
        )
        record_failure.assert_not_called()

    def test_delete_now_never_assesses_native_state_with_another_binding_owner(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='agent:eod-summarizer:feedback/eod-summarizer/slot-01',
        )

        identity = self._native_identity()
        with (
            patch.object(
                service,
                '_prepare_cleanup',
                return_value={
                    'ok': True,
                    'identity': identity,
                    'openclaw_session_present': True,
                },
            ),
            patch.object(
                service,
                '_invoke_delete',
                return_value={'ok': True, 'deleted': True},
            ),
            patch.object(
                service,
                '_assert_openclaw_generation_absent',
                return_value={'ok': True},
            ),
            patch.object(
                service,
                '_binding_owner_entry_keys',
                return_value=['conversation:still-owned'],
            ),
            patch.object(service, '_assess_native_cleanup') as assess_native,
            patch.object(service, '_record_failure', return_value={'id': 1}),
        ):
            result = service.delete_now(target=target)

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['result']['reason'], 'codex_thread_still_owned')
        assess_native.assert_not_called()

    def test_delete_now_refuses_generation_mismatch_before_gateway_delete(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='agent:eod-summarizer:feedback/eod-summarizer/slot-01',
        )
        with (
            patch.object(
                service,
                '_prepare_cleanup',
                return_value={
                    'ok': False,
                    'reason': 'openclaw_session_generation_mismatch',
                    'error': 'session generation changed before cleanup',
                },
            ),
            patch.object(service, '_invoke_delete') as invoke_delete,
            patch.object(service, '_assess_native_cleanup') as assess_native,
            patch.object(service, '_record_failure', return_value={'id': 1}),
        ):
            result = service.delete_now(target=target)

        self.assertEqual(result['status'], 'failed')
        invoke_delete.assert_not_called()
        assess_native.assert_not_called()

    def test_delete_now_keeps_failure_persistence_inside_resolved_deadline(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='feedback/eod-summarizer/slot-01',
        )

        with (
            patch.object(service, '_resolve_deadline', return_value=123.0),
            patch.object(
                service,
                '_prepare_cleanup',
                return_value={
                    'ok': False,
                    'reason': 'openclaw_session_generation_mismatch',
                    'error': 'session generation changed before cleanup',
                },
            ),
            patch.object(
                service,
                '_record_failure',
                return_value={'id': 1},
            ) as record_failure,
        ):
            service.delete_now(target=target, deadline_monotonic=200.0)

        self.assertEqual(record_failure.call_args.kwargs['deadline_monotonic'], 123.0)

    def test_delete_now_refuses_active_openclaw_session(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-active',
            session_key='agent:eod-summarizer:feedback/eod-summarizer/active',
        )
        with (
            patch.object(
                service,
                '_prepare_cleanup',
                return_value={
                    'ok': False,
                    'reason': 'openclaw_session_active',
                    'error': 'refusing to delete an active OpenClaw session',
                },
            ),
            patch.object(service, '_invoke_delete') as invoke_delete,
            patch.object(service, '_record_failure', return_value={'id': 1}),
        ):
            result = service.delete_now(target=target)

        self.assertEqual(result['result']['reason'], 'openclaw_session_active')
        invoke_delete.assert_not_called()

    def test_delete_now_finalizes_native_absence_only_after_zero_owner_recheck(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='agent:eod-summarizer:feedback/eod-summarizer/slot-01',
        )
        identity = self._native_identity()
        with (
            patch.object(
                service,
                '_prepare_cleanup',
                return_value={
                    'ok': True,
                    'identity': identity,
                    'openclaw_session_present': True,
                },
            ),
            patch.object(
                service,
                '_invoke_delete',
                return_value={'ok': True, 'deleted': True},
            ) as invoke_delete,
            patch.object(
                service,
                '_assert_openclaw_generation_absent',
                return_value={'ok': True},
            ),
            patch.object(service, '_binding_owner_entry_keys', return_value=[]),
            patch.object(
                service,
                '_assess_native_cleanup',
                return_value={'ok': True, 'outcome': 'already_absent'},
            ) as assess_native,
            patch.object(service, '_clear_failure'),
        ):
            result = service.delete_now(target=target)

        self.assertEqual(result['status'], 'deleted')
        self.assertEqual(result['native_cleanup']['outcome'], 'already_absent')
        self.assertEqual(
            invoke_delete.call_args.kwargs['expected_session_id'],
            identity['openclaw_session_id'],
        )
        self.assertEqual(
            invoke_delete.call_args.kwargs['expected_lifecycle_revision'],
            identity['openclaw_lifecycle_revision'],
        )
        self.assertEqual(
            invoke_delete.call_args.kwargs['expected_session_updated_at'],
            identity['openclaw_session_updated_at'],
        )
        assess_native.assert_called_once()

    def test_delete_now_uses_canonical_agent_scope_from_identity(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name=' EOD-SUMMARIZER ',
            run_id='feedback-run-01',
            session_key='feedback/eod-summarizer/slot-01',
        )
        identity = self._native_identity()

        with (
            patch.object(
                service,
                '_prepare_cleanup',
                return_value={
                    'ok': True,
                    'identity': identity,
                    'openclaw_session_present': True,
                },
            ),
            patch.object(
                service,
                '_invoke_delete',
                return_value={'ok': True, 'deleted': True},
            ) as invoke_delete,
            patch.object(
                service,
                '_assert_openclaw_generation_absent',
                return_value={'ok': True},
            ),
            patch.object(service, '_binding_owner_entry_keys', return_value=[]),
            patch.object(
                service,
                '_assess_native_cleanup',
                return_value={'ok': True, 'outcome': 'already_absent'},
            ),
            patch.object(service, '_clear_failure'),
        ):
            service.delete_now(target=target)

        self.assertEqual(invoke_delete.call_args.kwargs['agent_name'], 'eod-summarizer')

    def test_validate_persisted_identity_rejects_different_target_key(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='feedback/eod-summarizer/different-slot',
        )
        identity = self._native_identity()
        identity['codex_state_db'] = (
            '/tmp/openclaw/agents/eod-summarizer/agent/codex-home/state_5.sqlite'
        )
        identity['identity_sha256'] = service._identity_sha256(identity)

        with patch.object(service, '_resolve_agent_runtime_paths') as resolve_paths:
            with self.assertRaises(session_cleanup_module.CleanupSafetyError) as raised:
                service._validate_persisted_identity(
                    target=target,
                    identity=identity,
                )

        self.assertEqual(raised.exception.reason, 'session_key_identity_mismatch')
        resolve_paths.assert_not_called()

    def test_validate_absent_session_rejects_rebound_binding_generation(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='feedback/eod-summarizer/slot-01',
        )
        identity = self._native_identity()
        identity['codex_state_db'] = (
            '/tmp/openclaw/agents/eod-summarizer/agent/codex-home/state_5.sqlite'
        )
        identity['identity_sha256'] = service._identity_sha256(identity)
        paths = {
            'agent_home': session_cleanup_module.Path(identity['agent_home']).resolve(),
            'codex_home': session_cleanup_module.Path(identity['codex_home']).resolve(),
            'codex_state_db': session_cleanup_module.Path(identity['codex_state_db']).resolve(),
            'shared_state_db': session_cleanup_module.Path(
                '/tmp/openclaw/state/openclaw.sqlite'
            ).resolve(),
        }

        with (
            patch.object(service, '_resolve_agent_runtime_paths', return_value=paths),
            patch.object(service, '_read_session_snapshot', return_value={'present': False}),
            patch.object(
                service,
                '_read_binding',
                return_value={
                    'state': 'active',
                    'sessionId': '4b9188a5-54d4-466b-a5d1-f8435586c047',
                    'binding': {
                        'threadId': '019bf4d8-6e7d-70b0-80d8-75b9b12957ea',
                    },
                },
            ),
            patch.object(service, '_native_thread_record') as native_record,
        ):
            with self.assertRaises(session_cleanup_module.CleanupSafetyError) as raised:
                service._validate_persisted_identity(
                    target=target,
                    identity=identity,
                )

        self.assertEqual(raised.exception.reason, 'codex_binding_identity_mismatch')
        native_record.assert_not_called()

    def test_retry_without_immutable_native_identity_fails_closed(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        row = MagicMock()
        row.market_date = date(2026, 9, 11)
        row.symbol = '005930'
        row.agent_name = 'eod-summarizer'
        row.run_id = 'legacy-retry'
        row.session_key = 'agent:eod-summarizer:feedback/eod-summarizer/legacy'
        row.delete_transcript = True
        row.emit_lifecycle_hooks = False
        row.cleanup_result = {'ok': False, 'error': 'legacy failure'}
        target = service._target_from_failure(row)

        with (
            patch.object(service, '_invoke_delete') as invoke_delete,
            patch.object(service, '_record_failure', return_value={'id': 1}),
        ):
            result = service.delete_now(target=target)

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(
            result['result']['reason'],
            'legacy_retry_missing_native_cleanup_identity',
        )
        self.assertTrue(result['result']['blocked'])
        self.assertTrue(result['result']['manual_reconciliation_required'])
        self.assertFalse(result['result']['retryable'])
        invoke_delete.assert_not_called()

    def test_persisted_generation_conflict_is_quarantined_from_automatic_retry(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        identity = self._native_identity()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='generation-conflict',
            session_key=identity['session_key'],
            retry_cleanup_result={'native_cleanup_identity': identity},
        )

        with patch.object(
            service,
            '_validate_persisted_identity',
            side_effect=session_cleanup_module.CleanupSafetyError(
                'openclaw_session_generation_mismatch',
                'session generation changed before cleanup',
            ),
        ):
            result = service._prepare_cleanup(
                target=target,
                deadline_monotonic=float('inf'),
            )

        self.assertFalse(result['ok'])
        self.assertTrue(result['blocked'])
        self.assertTrue(result['manual_reconciliation_required'])
        self.assertFalse(result['retryable'])
        self.assertEqual(result['native_cleanup_identity'], identity)

    def test_online_core_never_invokes_native_delete_when_native_state_remains(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        identity = self._native_identity()

        with (
            patch.object(
                service,
                '_native_thread_record',
                return_value={
                    'id': identity['codex_thread_id'],
                    'rollout_path': identity['codex_rollout_path'],
                },
            ),
            patch.object(session_cleanup_module.subprocess, 'run') as run,
        ):
            result = service._assess_native_cleanup(
                identity=identity,
                deadline_monotonic=float('inf'),
            )

        self.assertFalse(result['ok'])
        self.assertEqual(
            result['reason'],
            'native_delete_requires_offline_maintenance',
        )
        self.assertTrue(result['retryable'])
        self.assertTrue(result['offline_maintenance_required'])
        run.assert_not_called()

    def test_native_row_absent_with_rollout_present_requires_offline_maintenance(
        self,
    ) -> None:
        service = AgentFeedbackSessionCleanupService()
        with tempfile.TemporaryDirectory() as temp_dir:
            rollout_path = session_cleanup_module.Path(temp_dir) / 'rollout.jsonl'
            rollout_path.write_text('{}\n', encoding='utf-8')
            identity = {
                **self._native_identity(),
                'codex_rollout_path': str(rollout_path),
            }
            with patch.object(service, '_native_thread_record', return_value=None):
                result = service._assess_native_cleanup(
                    identity=identity,
                    deadline_monotonic=float('inf'),
                )

        self.assertFalse(result['ok'])
        self.assertEqual(
            result['reason'],
            'native_delete_requires_offline_maintenance',
        )
        self.assertEqual(result['maintenance_reason'], 'native_row_missing_rollout_present')

    def test_native_row_and_rollout_both_absent_is_idempotent_success(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        with tempfile.TemporaryDirectory() as temp_dir:
            identity = {
                **self._native_identity(),
                'codex_rollout_path': str(
                    session_cleanup_module.Path(temp_dir) / 'absent-rollout.jsonl'
                ),
            }
            with patch.object(service, '_native_thread_record', return_value=None):
                result = service._assess_native_cleanup(
                    identity=identity,
                    deadline_monotonic=float('inf'),
                )

        self.assertTrue(result['ok'])
        self.assertEqual(result['outcome'], 'already_absent')

    def test_delete_now_persists_offline_maintenance_state_at_result_root(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='feedback/eod-summarizer/slot-01',
        )
        identity = self._native_identity()
        native_result = {
            'ok': False,
            'outcome': 'failed',
            'reason': 'native_delete_requires_offline_maintenance',
            'retryable': True,
            'offline_maintenance_required': True,
            'error': 'failed to delete session',
        }

        with (
            patch.object(
                service,
                '_prepare_cleanup',
                return_value={
                    'ok': True,
                    'identity': identity,
                    'openclaw_session_present': False,
                },
            ),
            patch.object(service, '_binding_owner_entry_keys', return_value=[]),
            patch.object(service, '_assess_native_cleanup', return_value=native_result),
            patch.object(
                service,
                '_record_failure',
                return_value={'id': 1},
            ) as record_failure,
        ):
            result = service.delete_now(target=target)

        self.assertEqual(result['status'], 'failed')
        self.assertTrue(result['result']['retryable'])
        self.assertTrue(result['result']['offline_maintenance_required'])
        persisted_result = record_failure.call_args.kwargs['result']
        self.assertTrue(persisted_result['retryable'])
        self.assertTrue(persisted_result['offline_maintenance_required'])

    def test_capture_native_identity_reads_exact_generation_and_binding(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        with tempfile.TemporaryDirectory() as state_dir:
            root = session_cleanup_module.Path(state_dir)
            target, expected = self._create_native_fixture(root)
            with patch.object(
                session_cleanup_module.settings,
                'openclaw_state_dir',
                state_dir,
            ):
                result = service._capture_native_cleanup_identity(target=target)

        self.assertTrue(result['ok'])
        identity = result['identity']
        self.assertEqual(identity['openclaw_session_id'], expected['session_id'])
        self.assertEqual(
            identity['openclaw_lifecycle_revision'],
            'lifecycle-revision-01',
        )
        self.assertEqual(identity['openclaw_session_updated_at'], 1792360031022)
        self.assertEqual(identity['codex_thread_id'], expected['thread_id'])
        self.assertEqual(identity['agent_home'], str(expected['agent_home'].resolve()))
        self.assertEqual(identity['codex_home'], str(expected['codex_home'].resolve()))
        self.assertEqual(identity['codex_state_db'], str(expected['codex_state_db'].resolve()))
        self.assertEqual(
            identity['identity_sha256'],
            service._identity_sha256(identity),
        )

    def test_capture_native_identity_refuses_active_session(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        with tempfile.TemporaryDirectory() as state_dir:
            root = session_cleanup_module.Path(state_dir)
            target, expected = self._create_native_fixture(root)
            with sqlite3.connect(expected['agent_home'] / 'openclaw-agent.sqlite') as connection:
                connection.execute(
                    "UPDATE session_nodes SET status = 'running' WHERE current_session_id = ?",
                    (expected['session_id'],),
                )
            with patch.object(
                session_cleanup_module.settings,
                'openclaw_state_dir',
                state_dir,
            ):
                result = service._capture_native_cleanup_identity(target=target)

        self.assertFalse(result['ok'])
        self.assertEqual(result['reason'], 'openclaw_session_active')

    def test_resolve_agent_runtime_paths_rejects_codex_home_symlink_escape(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        with tempfile.TemporaryDirectory() as state_dir:
            root = session_cleanup_module.Path(state_dir)
            agent_home = root / 'agents' / 'eod-summarizer' / 'agent'
            external_codex_home = root / 'outside-agent-home'
            agent_home.mkdir(parents=True)
            external_codex_home.mkdir()
            (external_codex_home / 'state_5.sqlite').touch()
            (agent_home / 'openclaw-agent.sqlite').touch()
            (agent_home / 'codex-home').symlink_to(
                external_codex_home,
                target_is_directory=True,
            )
            (root / 'state').mkdir()
            (root / 'state' / 'openclaw.sqlite').touch()
            (root / 'openclaw.json').write_text(
                json.dumps(
                    {
                        'agents': {
                            'entries': {
                                'eod-summarizer': {
                                    'agentDir': str(agent_home),
                                }
                            }
                        }
                    }
                ),
                encoding='utf-8',
            )

            with patch.object(
                session_cleanup_module.settings,
                'openclaw_state_dir',
                state_dir,
            ):
                with self.assertRaises(
                    session_cleanup_module.CleanupSafetyError
                ) as raised:
                    service._resolve_agent_runtime_paths(
                        agent_name='eod-summarizer'
                    )

        self.assertEqual(raised.exception.reason, 'invalid_agent_home')

    def test_invoke_delete_keeps_already_absent_success_error_free(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='''Doctor notice: stale config entry ignored
{
  "ok": true,
  "deleted": false
}
''',
            stderr='Doctor notice: run openclaw doctor --fix',
        )

        with (
            patch('app.agent_context.session_cleanup_service.settings.openclaw_gateway_token', 'token'),
            patch('app.agent_context.session_cleanup_service.subprocess.run', return_value=completed),
        ):
            result = service._invoke_delete(
                session_key='feedback/eod-summarizer/already-absent',
                agent_name='eod-summarizer',
                expected_session_id='07651bdc-b2c4-45c7-a74b-06a68ab441fe',
                expected_lifecycle_revision='lifecycle-revision-01',
                expected_session_updated_at=1792360031022,
                delete_transcript=True,
                emit_lifecycle_hooks=False,
                deadline_monotonic=float('inf'),
            )

        self.assertTrue(result['ok'])
        self.assertFalse(result['deleted'])
        self.assertIsNone(result['error'])
        self.assertIn('openclaw doctor --fix', result['stderr'])

    def test_invoke_delete_rejects_missing_or_non_boolean_deleted_field(self) -> None:
        service = AgentFeedbackSessionCleanupService()

        for stdout in (
            '{"ok": true}',
            '{"ok": true, "deleted": "false"}',
        ):
            with self.subTest(stdout=stdout):
                completed = subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout=stdout,
                    stderr='',
                )
                with (
                    patch(
                        'app.agent_context.session_cleanup_service.settings.openclaw_gateway_token',
                        'token',
                    ),
                    patch(
                        'app.agent_context.session_cleanup_service.subprocess.run',
                        return_value=completed,
                    ),
                ):
                    result = service._invoke_delete(
                        session_key='feedback/eod-summarizer/invalid-response',
                        agent_name='eod-summarizer',
                        expected_session_id='07651bdc-b2c4-45c7-a74b-06a68ab441fe',
                        expected_lifecycle_revision='lifecycle-revision-01',
                        expected_session_updated_at=1792360031022,
                        delete_transcript=True,
                        emit_lifecycle_hooks=False,
                        deadline_monotonic=float('inf'),
                    )

                self.assertFalse(result['ok'])
                self.assertFalse(result['deleted'])
                self.assertEqual(result['error'], 'sessions.delete returned an invalid response')

    def test_enqueue_deferred_persists_each_target_without_invoking_delete(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        suffix = uuid.uuid4().hex
        session_keys = [
            f'feedback/eod-summarizer/deferred-{suffix}-01',
            f'feedback/eod-summarizer/deferred-{suffix}-02',
        ]
        targets = [
            SessionCleanupTarget(
                market_date=date(2026, 9, 11),
                symbol=symbol,
                agent_name='eod-summarizer',
                run_id='feedback-run-deferred',
                session_key=session_key,
            )
            for symbol, session_key in zip(('005930', '000660'), session_keys, strict=True)
        ]

        try:
            with (
                patch.object(service, '_invoke_delete') as invoke_delete,
                patch.object(
                    service,
                    '_capture_native_cleanup_identity',
                    return_value={
                        'ok': True,
                        'identity': self._native_identity(),
                    },
                ) as capture_identity,
            ):
                result = service.enqueue_deferred(
                    targets=targets,
                    reason='cleanup_time_budget_exhausted',
                )

            with session_scope() as session:
                rows = session.execute(
                    select(AgentFeedbackSessionCleanupFailure)
                    .where(AgentFeedbackSessionCleanupFailure.session_key.in_(session_keys))
                    .order_by(AgentFeedbackSessionCleanupFailure.session_key)
                ).scalars().all()
                persisted = [
                    {
                        'session_key': row.session_key,
                        'cleanup_attempts': row.cleanup_attempts,
                        'last_attempt_at': row.last_attempt_at,
                        'next_attempt_at': row.next_attempt_at,
                        'last_error': row.last_error,
                        'cleanup_result': row.cleanup_result,
                    }
                    for row in rows
                ]

            self.assertEqual(result['status'], 'queued')
            self.assertEqual(result['queued'], 2)
            self.assertEqual(result['inserted'], 2)
            self.assertEqual([row['session_key'] for row in persisted], sorted(session_keys))
            self.assertTrue(all(row['cleanup_attempts'] == 0 for row in persisted))
            self.assertTrue(all(row['last_attempt_at'] is None for row in persisted))
            self.assertTrue(all(row['next_attempt_at'] is not None for row in persisted))
            self.assertTrue(all(row['last_error'] == 'cleanup_time_budget_exhausted' for row in persisted))
            self.assertEqual(result['identity_captured'], 2)
            self.assertEqual(result['identity_missing'], 0)
            self.assertTrue(
                all(
                    row['cleanup_result']['native_cleanup_identity']
                    == self._native_identity()
                    for row in persisted
                )
            )
            self.assertEqual(capture_identity.call_count, 2)
            invoke_delete.assert_not_called()
        finally:
            with session_scope() as session:
                session.execute(
                    delete(AgentFeedbackSessionCleanupFailure).where(
                        AgentFeedbackSessionCleanupFailure.session_key.in_(session_keys)
                    )
                )

    def test_enqueue_deferred_reports_missing_identity_as_manual_blocker(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        session_key = f'feedback/eod-summarizer/blocked-{uuid.uuid4().hex}'
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-blocked',
            session_key=session_key,
        )
        capture_failure = {
            'ok': False,
            'reason': 'openclaw_session_identity_unavailable',
            'error': 'cannot capture immutable identity',
        }

        try:
            with patch.object(
                service,
                '_capture_native_cleanup_identity',
                return_value=capture_failure,
            ):
                result = service.enqueue_deferred(
                    targets=[target],
                    reason='cleanup_time_budget_exhausted',
                )

            with session_scope() as session:
                row = session.execute(
                    select(AgentFeedbackSessionCleanupFailure).where(
                        AgentFeedbackSessionCleanupFailure.session_key == session_key
                    )
                ).scalar_one()
                cleanup_result = dict(row.cleanup_result)

            self.assertEqual(result['status'], 'blocked')
            self.assertEqual(result['queued'], 0)
            self.assertEqual(result['blocked'], 1)
            self.assertFalse(result['persistence_confirmed'])
            self.assertTrue(result['audit_persistence_confirmed'])
            self.assertTrue(result['manual_reconciliation_required'])
            self.assertTrue(cleanup_result['blocked'])
            self.assertTrue(cleanup_result['manual_reconciliation_required'])
            self.assertEqual(cleanup_result['identity_capture'], capture_failure)
        finally:
            with session_scope() as session:
                session.execute(
                    delete(AgentFeedbackSessionCleanupFailure).where(
                        AgentFeedbackSessionCleanupFailure.session_key == session_key
                    )
                )

    def test_enqueue_deferred_preserves_existing_retry_backoff(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        session_key = f'feedback/eod-summarizer/already-queued-{uuid.uuid4().hex}'
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-existing-retry',
            session_key=session_key,
        )
        retry_at = datetime.now(UTC) + timedelta(minutes=15)

        try:
            service.enqueue_deferred(
                targets=[target],
                reason='cleanup_time_budget_exhausted',
            )
            with session_scope() as session:
                row = session.execute(
                    select(AgentFeedbackSessionCleanupFailure).where(
                        AgentFeedbackSessionCleanupFailure.session_key == session_key
                    )
                ).scalar_one()
                row.cleanup_attempts = 4
                row.next_attempt_at = retry_at
                row.last_error = 'sessions.delete timed out after 30s'

            result = service.enqueue_deferred(
                targets=[target],
                reason='cleanup_time_budget_exhausted',
            )
            with session_scope() as session:
                row = session.execute(
                    select(AgentFeedbackSessionCleanupFailure).where(
                        AgentFeedbackSessionCleanupFailure.session_key == session_key
                    )
                ).scalar_one()
                persisted = {
                    'cleanup_attempts': row.cleanup_attempts,
                    'next_attempt_at': row.next_attempt_at,
                    'last_error': row.last_error,
                }

            self.assertEqual(result['inserted'], 0)
            self.assertEqual(result['already_queued'], 1)
            self.assertEqual(persisted['cleanup_attempts'], 4)
            self.assertEqual(persisted['next_attempt_at'], retry_at)
            self.assertEqual(persisted['last_error'], 'sessions.delete timed out after 30s')
        finally:
            with session_scope() as session:
                session.execute(
                    delete(AgentFeedbackSessionCleanupFailure).where(
                        AgentFeedbackSessionCleanupFailure.session_key == session_key
                    )
                )

    def test_invoke_delete_returns_failure_payload_on_timeout(self) -> None:
        service = AgentFeedbackSessionCleanupService()

        with (
            patch('app.agent_context.session_cleanup_service.settings.openclaw_gateway_token', 'token'),
            patch('app.agent_context.session_cleanup_service.settings.openclaw_session_cleanup_timeout_seconds', 30),
            patch(
                'app.agent_context.session_cleanup_service.subprocess.run',
                side_effect=subprocess.TimeoutExpired(
                    cmd=['openclaw', 'gateway', 'call', 'sessions.delete'],
                    timeout=30,
                    output='',
                    stderr='gateway timeout',
                ),
            ),
        ):
            result = service._invoke_delete(
                session_key='feedback/eod-summarizer/slot-01',
                agent_name='eod-summarizer',
                expected_session_id='07651bdc-b2c4-45c7-a74b-06a68ab441fe',
                expected_lifecycle_revision='lifecycle-revision-01',
                expected_session_updated_at=1792360031022,
                delete_transcript=True,
                emit_lifecycle_hooks=False,
                deadline_monotonic=float('inf'),
            )

        self.assertFalse(result['ok'])
        self.assertFalse(result['deleted'])
        self.assertEqual(result['timeout_seconds'], 30.0)
        self.assertIn('timed out', result['error'])


if __name__ == '__main__':
    unittest.main()
