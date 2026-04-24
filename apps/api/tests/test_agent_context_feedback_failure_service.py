import unittest

from app.agent_context.feedback_failure_service import AgentContextFeedbackFailureService


class AgentContextFeedbackFailureServiceTests(unittest.TestCase):
    def test_normalize_failure_kind_truncates_long_values(self):
        service = AgentContextFeedbackFailureService()

        result = service._normalize_failure_kind('x' * 80)

        self.assertEqual(len(result), 64)
        self.assertEqual(result, 'x' * 64)


if __name__ == '__main__':
    unittest.main()
