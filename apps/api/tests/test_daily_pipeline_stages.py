import unittest
from unittest.mock import patch

from app.agent_context.roles import SIGNAL_DECIDER_AGENT_NAME, SUMMARIZER_AGENT_NAMES
from app.eod.daily_pipeline_stages import DailyPipelineStageService


class _FakePykrxStageRunner:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def run_daily_pipeline_stages(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'run_id': 901,
            'status': 'completed',
            'latest_available_trade_date': kwargs['market_date'],
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'bars_upserted': 2,
            'technical_snapshots': {
                'market_date': kwargs['market_date'],
                'snapshots_persisted': 2,
                'snapshots_failed': 0,
            },
        }


class _FakeFeedbackRetryService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def run_retry_cycles(self, *, config):
        self.calls.append({'config': config})
        return {
            'completed': True,
            'final_pending': 0,
            'roles': list(config.roles),
            'requested_symbols': list(config.symbols) if config.symbols is not None else None,
        }


class _FakeSignalDeciderEvaluationService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def compute_recent_evaluations(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'request': {
                'as_of_market_date': kwargs['as_of_market_date'],
                'lookback_days': kwargs['lookback_days'],
            },
            'summary': {
                'outcomes_upserted': 4,
                'evaluations_upserted': 4,
            },
            'items': [],
        }


class _FakeSkippedPykrxStageRunner:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def run_daily_pipeline_stages(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'triggered': False,
            'reason': 'already_running',
            'market_date': kwargs['market_date'],
            'latest_available_trade_date': kwargs['market_date'],
            'source': 'pykrx',
        }


class DailyPipelineStageServiceTests(unittest.TestCase):
    def test_describe_returns_canonical_stage_order_and_default_sequence(self) -> None:
        service = DailyPipelineStageService()

        payload = service.describe()

        self.assertEqual(
            payload['stage_order'][:6],
            [
                'master_refresh',
                'pykrx_universe_refresh',
                'price_collection',
                'technical_snapshots',
                'action_summaries',
                'trending_ranks',
            ],
        )
        self.assertIn('fundamentals_daily_refresh', payload['default_daily_stage_order'])
        self.assertIn('signal_decider', payload['default_daily_stage_order'])

    def test_run_reorders_requested_subset_and_splits_feedback_stages(self) -> None:
        pykrx_runner = _FakePykrxStageRunner()
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        service = DailyPipelineStageService(
            pykrx_service=pykrx_runner,
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )

        with (
            patch.object(service, '_execute_stage', side_effect=service._execute_stage) as execute_stage,
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'signal_decider_evaluations',
                    'signal_decider',
                    'technical_snapshots',
                    'agent_feedback_summaries',
                    'price_collection',
                ],
                symbols=['005930', '000660'],
            )

        self.assertEqual(
            result['resolved_stages'],
            [
                'price_collection',
                'technical_snapshots',
                'agent_feedback_summaries',
                'signal_decider',
                'signal_decider_evaluations',
            ],
        )
        self.assertEqual(pykrx_runner.calls[0]['stages'], ['price_collection', 'technical_snapshots'])
        self.assertEqual(pykrx_runner.calls[0]['symbols'], ['005930', '000660'])
        self.assertEqual(feedback_retry_service.calls[0]['config'].roles, SUMMARIZER_AGENT_NAMES)
        self.assertEqual(feedback_retry_service.calls[1]['config'].roles, (SIGNAL_DECIDER_AGENT_NAME,))
        self.assertEqual(evaluation_service.calls[0]['as_of_market_date'], '2026-04-14')
        self.assertEqual(evaluation_service.calls[0]['symbols'], ['005930', '000660'])
        self.assertGreaterEqual(execute_stage.call_count, 3)

    def test_run_rejects_unknown_stage(self) -> None:
        service = DailyPipelineStageService()

        with self.assertRaisesRegex(ValueError, 'Unknown daily stage'):
            service.run(market_date='2026-04-14', stages=['not-a-stage'])

    def test_run_marks_grouped_pykrx_skip_payloads_as_skipped(self) -> None:
        pykrx_runner = _FakeSkippedPykrxStageRunner()
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        result = service.run(
            market_date='2026-04-14',
            stages=['price_collection', 'technical_snapshots'],
        )

        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['stages']['price_collection']['status'], 'skipped')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'skipped')
        self.assertFalse(result['stages']['price_collection']['result']['triggered'])
        self.assertEqual(result['stages']['technical_snapshots']['result']['reason'], 'already_running')

    def test_resolve_result_status_marks_error_only_payload_failed(self) -> None:
        status = DailyPipelineStageService._resolve_result_status({'error': 'boom'})

        self.assertEqual(status, 'failed')

    def test_resolve_result_status_marks_error_payload_with_progress_partial(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'error': 'boom',
                'trending_ranks_persisted': 2,
            }
        )

        self.assertEqual(status, 'partial')


if __name__ == '__main__':
    unittest.main()
