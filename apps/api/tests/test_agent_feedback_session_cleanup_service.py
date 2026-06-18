import subprocess
import unittest
from unittest.mock import patch

from app.agent_context.session_cleanup_service import AgentFeedbackSessionCleanupService


class AgentFeedbackSessionCleanupServiceTests(unittest.TestCase):
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
