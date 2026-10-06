import subprocess
import sqlite3
import tempfile
import unittest
import uuid
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

from sqlalchemy import delete, select

from app.agent_context import session_cleanup_service as session_cleanup_module
from app.agent_context.session_cleanup_service import AgentFeedbackSessionCleanupService, SessionCleanupTarget
from app.db import session_scope
from app.db_models import AgentFeedbackSessionCleanupFailure


class AgentFeedbackSessionCleanupServiceTests(unittest.TestCase):
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
        session.execute.return_value.scalars.return_value.all.return_value = []

        with (
            patch.object(session_cleanup_module.time, 'monotonic', return_value=100.0),
            patch.object(session_cleanup_module.settings, 'db_pool_timeout_seconds', 120),
            patch.object(session_cleanup_module.settings, 'db_connect_timeout_seconds', 5),
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
                delete_transcript=True,
                emit_lifecycle_hooks=False,
            )

        self.assertTrue(result['ok'])
        self.assertIn('--json', run.call_args.args[0])

    def test_delete_now_treats_already_absent_session_as_idempotent_success(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='feedback/eod-summarizer/slot-01',
        )

        with (
            patch.object(service, '_invoke_delete', return_value={'ok': True, 'deleted': False}),
            patch.object(
                service,
                '_prune_codex_binding',
                return_value={'status': 'already_absent', 'deleted': 0},
            ),
            patch.object(service, '_clear_failure') as clear_failure,
            patch.object(service, '_record_failure') as record_failure,
        ):
            result = service.delete_now(target=target)

        self.assertEqual(result['status'], 'deleted')
        self.assertEqual(result['outcome'], 'already_absent')
        clear_failure.assert_called_once_with(session_key=target.session_key)
        record_failure.assert_not_called()

    def test_delete_now_prunes_only_matching_codex_binding(self) -> None:
        service = AgentFeedbackSessionCleanupService()
        target = SessionCleanupTarget(
            market_date=date(2026, 9, 11),
            symbol='005930',
            agent_name='eod-summarizer',
            run_id='feedback-run-01',
            session_key='agent:eod-summarizer:feedback/eod-summarizer/slot-01',
        )

        with tempfile.TemporaryDirectory() as state_dir:
            db_path = session_cleanup_module.Path(state_dir) / 'state' / 'openclaw.sqlite'
            db_path.parent.mkdir(parents=True)
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    '''
                    CREATE TABLE plugin_state_entries (
                      plugin_id TEXT NOT NULL,
                      namespace TEXT NOT NULL,
                      entry_key TEXT NOT NULL,
                      value_json TEXT NOT NULL,
                      created_at INTEGER NOT NULL,
                      expires_at INTEGER,
                      PRIMARY KEY (plugin_id, namespace, entry_key)
                    )
                    '''
                )
                matching_key = service._codex_binding_entry_key(
                    agent_name=target.agent_name,
                    session_key=target.session_key,
                )
                connection.executemany(
                    '''
                    INSERT INTO plugin_state_entries
                      (plugin_id, namespace, entry_key, value_json, created_at, expires_at)
                    VALUES (?, ?, ?, ?, 1, NULL)
                    ''',
                    [
                        (
                            'codex',
                            'app-server-thread-bindings',
                            matching_key,
                            '{"version":1,"state":"active","binding":{"threadId":"deleted"}}',
                        ),
                        (
                            'codex',
                            'app-server-thread-bindings',
                            'session-key:eod-summarizer:unrelated',
                            '{"version":1,"state":"active","binding":{"threadId":"keep"}}',
                        ),
                    ],
                )

            with (
                patch.object(service, '_invoke_delete', return_value={'ok': True, 'deleted': True}),
                patch.object(service, '_clear_failure'),
                patch.object(session_cleanup_module.settings, 'openclaw_state_dir', state_dir),
            ):
                result = service.delete_now(target=target)

            self.assertEqual(result['binding_cleanup']['status'], 'deleted')
            with sqlite3.connect(db_path) as connection:
                rows = connection.execute(
                    'SELECT entry_key FROM plugin_state_entries ORDER BY entry_key'
                ).fetchall()

        self.assertEqual(rows, [('session-key:eod-summarizer:unrelated',)])

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
                delete_transcript=True,
                emit_lifecycle_hooks=False,
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
                        delete_transcript=True,
                        emit_lifecycle_hooks=False,
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
                    '_prune_codex_bindings',
                    return_value={'status': 'deleted', 'deleted': 2},
                ) as prune_bindings,
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
            self.assertEqual(result['binding_cleanup'], {'status': 'deleted', 'deleted': 2})
            prune_bindings.assert_called_once_with(targets=targets)
            invoke_delete.assert_not_called()
        finally:
            with session_scope() as session:
                session.execute(
                    delete(AgentFeedbackSessionCleanupFailure).where(
                        AgentFeedbackSessionCleanupFailure.session_key.in_(session_keys)
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
                delete_transcript=True,
                emit_lifecycle_hooks=False,
            )

        self.assertFalse(result['ok'])
        self.assertFalse(result['deleted'])
        self.assertEqual(result['timeout_seconds'], 30.0)
        self.assertIn('timed out', result['error'])


if __name__ == '__main__':
    unittest.main()
