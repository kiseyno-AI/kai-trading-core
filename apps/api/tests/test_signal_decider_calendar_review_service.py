from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Any
import unittest
from unittest.mock import patch

from app.agent_context.signal_decider_calendar_review_service import SignalDeciderCalendarReviewService
from app.agent_context.signal_decider_loop_service import SignalDeciderLoopRepository, SignalDeciderLoopService


def _build_detail_item(*, primary_horizon_days: int) -> dict[str, Any]:
    return {
        'snapshot_id': 84,
        'feedback_id': 11,
        'decision_run_id': 'signal-run-84',
        'market_date': '2026-04-23',
        'symbol': '005930',
        'policy_version': 'signal_decider_policy_v1',
        'decision': {
            'action': 'buy',
            'target_price': 105,
            'stop_loss_price': 96,
        },
        'summary': 'Breakout quality stayed constructive.',
        'supporting_evidence': ['Breadth improved into the close.'],
        'decision_risks': ['Leadership remained narrow.'],
        'received_at': '2026-04-23T06:00:00+00:00',
        'created_at': '2026-04-23T06:00:00+00:00',
        'review': {
            'status': 'complete',
            'primary_horizon_days': primary_horizon_days,
            'evaluation_version': 'signal_decider_eval_test',
            'insight': {
                'code': 'buy_should_have_taken',
                'priority': 'low',
                'label': 'Buy held up',
                'label_key': 'signal_decider.review.buy_should_have_taken',
                'description': 'The buy decision kept a constructive replay path on the requested horizon.',
                'outcome_tone': 'positive',
            },
            'narrative': {
                'version': 'signal_decider_replay_review_v1',
                'insight_code': 'buy_should_have_taken',
                'insight_priority': 'low',
                'headline': 'Buy held up on replay.',
                'headline_key': 'signal_decider.review.headline.buy_should_have_taken',
                'supporting_reasons': [
                    {
                        'code': 'target_hit_without_stop',
                        'message': 'Target was touched and the stop-loss was never touched inside the review window.',
                        'message_key': 'signal_decider.review.reason.target_hit_without_stop',
                    }
                ],
                'counterfactual_summary': None,
                'summary_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
            },
            'narrative_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
            'path_analysis': {
                'horizon_days': primary_horizon_days,
                'path_marker': 'target_hit',
                'first_trigger': 'target_hit',
                'trigger_sequence': 'target_only',
                'thresholds_configured': True,
                'bars_observed': primary_horizon_days,
                'checkpoints': {
                    'entry_trade_date': '2026-04-24',
                    'exit_trade_date': '2026-04-28',
                    'best_exit_trade_date': '2026-04-28',
                    'worst_drawdown_trade_date': '2026-04-24',
                    'first_target_hit_trade_date': '2026-04-24',
                    'first_stop_loss_hit_trade_date': None,
                    'observed_trade_dates': ['2026-04-24', '2026-04-25', '2026-04-28'],
                },
                'returns': {
                    'close_return_pct': 4.0,
                    'max_upside_return_pct': 5.0,
                    'max_drawdown_return_pct': -1.0,
                },
                'target_price': 105,
                'stop_loss_price': 96,
                'path_explanation': 'Target was touched and the stop-loss was never touched.',
            },
            'rule_evaluation': {
                'decision_action': 'buy',
                'evaluation_status': 'graded',
                'grade_label': 'good_buy',
                'grade_score': 0.91,
                'missed_opportunity': False,
                'counterfactual_action': None,
                'path_marker': 'target_hit',
                'trigger_sequence': 'target_only',
                'thresholds_configured': True,
                'review_rule': 'buy_should_have_taken',
                'generated_reason_codes': ['target_hit_without_stop'],
            },
            'horizons': [
                {
                    'horizon_days': 1,
                    'outcome_status': 'complete',
                    'evaluation_status': 'graded',
                    'grade_label': 'good_buy',
                    'path_analysis': {'path_marker': 'target_hit'},
                },
                {
                    'horizon_days': primary_horizon_days,
                    'outcome_status': 'complete',
                    'evaluation_status': 'graded',
                    'grade_label': 'good_buy',
                    'path_analysis': {'path_marker': 'target_hit'},
                },
            ],
            'outcome_horizons': [
                {
                    'horizon_days': 1,
                    'outcome_status': 'complete',
                    'evaluation_status': 'graded',
                    'grade_label': 'good_buy',
                    'path_analysis': {'path_marker': 'target_hit'},
                },
                {
                    'horizon_days': primary_horizon_days,
                    'outcome_status': 'complete',
                    'evaluation_status': 'graded',
                    'grade_label': 'good_buy',
                    'path_analysis': {'path_marker': 'target_hit'},
                },
            ],
            'quality_verdict': 'buy_should_have_taken',
            'quality_reason_code': 'target_hit_without_stop',
            'quality_reason': 'Target was touched and the stop-loss was never touched inside the review window.',
            'review_conclusion': 'Buy held up on replay.',
            'review_conclusion_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
            'path_outcome': 'target_hit',
            'intraday_path_result': 'target_only',
        },
        'observed_price_path': [{'trade_date': '2026-04-24', 'open_price': 100, 'high_price': 106, 'low_price': 99, 'close_price': 104}],
        'outcomes': [{'horizon_days': primary_horizon_days, 'outcome_status': 'complete'}],
        'evaluations': [{'horizon_days': primary_horizon_days, 'evaluation_status': 'graded'}],
    }


class FakeCalendarReviewRepository:
    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []
        self.items: dict[tuple[int, str, int], dict[str, Any]] = {}

    def start_run(self, **kwargs) -> dict[str, Any]:
        run = {
            'id': len(self.runs) + 1,
            'market_date_from': kwargs['market_date_from'].isoformat(),
            'market_date_to': kwargs['market_date_to'].isoformat(),
            'as_of_market_date': kwargs['as_of_market_date'].isoformat() if kwargs['as_of_market_date'] else None,
            'lookback_days': kwargs['lookback_days'],
            'evaluation_version': kwargs['evaluation_version'],
            'horizons': list(kwargs['horizons']),
            'primary_horizons': list(kwargs['primary_horizons']),
            'symbols': list(kwargs['symbols']) if kwargs['symbols'] else None,
            'trigger': kwargs['trigger'],
            'status': 'running',
            'force_recompute': kwargs['force_recompute'],
            'summary': None,
            'error_message': None,
            'created_at': '2026-04-30T09:00:00+00:00',
            'updated_at': '2026-04-30T09:00:00+00:00',
            'completed_at': None,
        }
        self.runs.append(run)
        return dict(run)

    def finish_run(self, *, run_id: int, status: str, summary_payload: dict[str, Any] | None, error_message: str | None = None) -> dict[str, Any]:
        run = self.runs[run_id - 1]
        run['status'] = status
        run['summary'] = summary_payload
        run['error_message'] = error_message
        run['completed_at'] = '2026-04-30T09:05:00+00:00'
        return dict(run)

    def list_recent_runs(self, *, evaluation_version: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        rows = list(reversed(self.runs))
        if evaluation_version:
            rows = [row for row in rows if row['evaluation_version'] == evaluation_version]
        return [dict(row) for row in rows[:limit]]

    def bulk_upsert_review_items(self, *, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        stored: list[dict[str, Any]] = []
        for index, item in enumerate(items, start=1):
            key = (int(item['decision_snapshot_id']), str(item['evaluation_version']), int(item['primary_horizon_days']))
            stored_item = {
                'id': index,
                'decision_snapshot_id': key[0],
                'review_run_id': int(item['review_run_id']),
                'market_date': item['market_date'].isoformat(),
                'symbol': item['symbol'],
                'policy_version': item['policy_version'],
                'decision_action': item['decision_action'],
                'evaluation_version': item['evaluation_version'],
                'primary_horizon_days': item['primary_horizon_days'],
                'review_status': item['review_status'],
                'insight_code': item['insight_code'],
                'insight_priority': item['insight_priority'],
                'quality_verdict': item['quality_verdict'],
                'quality_reason_code': item['quality_reason_code'],
                'quality_reason_text': item['quality_reason_text'],
                'review_conclusion': item['review_conclusion'],
                'review_summary_text': item['review_summary_text'],
                'path_outcome': item['path_outcome'],
                'intraday_path_result': item['intraday_path_result'],
                'item_payload': deepcopy(item['item_payload']),
                'detail_payload': deepcopy(item['detail_payload']),
                'refreshed_at': '2026-04-30T09:05:00+00:00',
                'created_at': '2026-04-30T09:05:00+00:00',
                'updated_at': '2026-04-30T09:05:00+00:00',
            }
            self.items[key] = stored_item
            stored.append(deepcopy(stored_item))
        return stored

    def list_review_items(
        self,
        *,
        market_date: date | None = None,
        market_date_from: date | None = None,
        market_date_to: date | None = None,
        evaluation_version: str,
        primary_horizon_days: int,
        symbols: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            deepcopy(item)
            for (_, version, primary), item in self.items.items()
            if version == evaluation_version and primary == primary_horizon_days
        ]
        if market_date is not None:
            rows = [row for row in rows if row['market_date'] == market_date.isoformat()]
        if market_date_from is not None:
            rows = [row for row in rows if row['market_date'] >= market_date_from.isoformat()]
        if market_date_to is not None:
            rows = [row for row in rows if row['market_date'] <= market_date_to.isoformat()]
        if symbols:
            rows = [row for row in rows if row['symbol'] in symbols]
        return rows

    def get_review_item(self, *, snapshot_id: int, evaluation_version: str, primary_horizon_days: int) -> dict[str, Any] | None:
        item = self.items.get((int(snapshot_id), evaluation_version, int(primary_horizon_days)))
        return deepcopy(item) if item is not None else None


class FakeLoopRepository:
    def list_decision_snapshots(self, **kwargs):
        return [
            type(
                'Row',
                (),
                {
                    'id': 84,
                    'market_date': date.fromisoformat('2026-04-23'),
                    'symbol': '005930',
                    'feedback_received_at': datetime(
                        2026,
                        4,
                        23,
                        6,
                        tzinfo=timezone.utc,
                    ),
                    'created_at': datetime(
                        2026,
                        4,
                        23,
                        6,
                        tzinfo=timezone.utc,
                    ),
                },
            )()
        ]


class FakeLoopService:
    def __init__(self) -> None:
        self.repository = FakeLoopRepository()
        self.compute_args = None
        self.build_args: list[dict[str, Any]] = []
        self._delegate = SignalDeciderLoopService(repository=SignalDeciderLoopRepository())

    def compute_evaluations(self, **kwargs):
        self.compute_args = kwargs
        return {'summary': {'evaluations_upserted': 6}}

    def build_review_items(self, **kwargs):
        self.build_args.append(kwargs)
        return [deepcopy(_build_detail_item(primary_horizon_days=int(kwargs['primary_horizon_days'])))]

    def summarize_review_items(self, **kwargs):
        return self._delegate.summarize_review_items(**kwargs)

    def build_review_scorecard(self, items):
        return self._delegate.build_review_scorecard(items)

    def build_calendar_day_status(self, **kwargs):
        return self._delegate.build_calendar_day_status(**kwargs)

    def build_replay_read_model_descriptor(self):
        return self._delegate.build_replay_read_model_descriptor()

    def build_replay_review_taxonomy(self):
        return self._delegate.build_replay_review_taxonomy()


class SignalDeciderCalendarReviewServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = FakeCalendarReviewRepository()
        self.loop_service = FakeLoopService()
        self.service = SignalDeciderCalendarReviewService(
            repository=self.repository,
            loop_service=self.loop_service,
        )

    def test_refresh_recent_reviews_persists_rows_for_each_primary_horizon(self) -> None:
        result = self.service.refresh_recent_reviews(
            as_of_market_date='2026-04-30',
            lookback_days=7,
            horizons=[1, 3, 5],
            primary_horizons=[3, 5],
            evaluation_version='signal_decider_eval_test',
            force_recompute=True,
            trigger='test_runner',
        )

        self.assertEqual(result['summary']['review_rows_upserted'], 2)
        self.assertEqual(self.loop_service.compute_args['market_date_to'], date.fromisoformat('2026-04-30'))
        self.assertEqual(self.loop_service.compute_args['horizons'], [1, 3, 5])
        self.assertEqual(sorted(item['primary_horizon_days'] for item in self.repository.items.values()), [3, 5])
        self.assertEqual(result['run']['status'], 'completed')

    def test_refresh_reviews_excludes_snapshots_invalid_for_source_date(self) -> None:
        valid_row = self.loop_service.repository.list_decision_snapshots()[0]
        invalid_row = type(
            'Row',
            (),
            {
                'id': 85,
                'market_date': date.fromisoformat('2026-04-23'),
                'symbol': '000660',
                'feedback_received_at': datetime(
                    2026,
                    4,
                    24,
                    1,
                    tzinfo=timezone.utc,
                ),
                'created_at': datetime(
                    2026,
                    4,
                    24,
                    1,
                    tzinfo=timezone.utc,
                ),
            },
        )()
        self.loop_service.repository.list_decision_snapshots = lambda **kwargs: [
            valid_row,
            invalid_row,
        ]

        self.service.refresh_reviews(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            primary_horizons=[5],
            evaluation_version='signal_decider_eval_test',
            trigger='test_runner',
            refresh_evaluations=False,
            evaluation_refresh_summary={'computed_count': 1},
        )

        self.assertEqual(
            [row.id for row in self.loop_service.build_args[0]['rows']],
            [84],
        )

    def test_refresh_recent_reviews_forwards_suppressed_evaluation_refresh(self) -> None:
        evaluation_summary = {'computed_count': 3, 'not_computed_count': 0}

        with patch.object(self.service, 'refresh_reviews', return_value={'summary': {'review_rows_upserted': 1}}) as refresh_reviews:
            result = self.service.refresh_recent_reviews(
                as_of_market_date='2026-04-30',
                lookback_days=7,
                horizons=[1, 3, 5],
                primary_horizons=[5],
                evaluation_version='signal_decider_eval_test',
                force_recompute=True,
                trigger='test_runner',
                refresh_evaluations=False,
                evaluation_refresh_summary=evaluation_summary,
            )

        self.assertEqual(result, {'summary': {'review_rows_upserted': 1}})
        self.assertEqual(refresh_reviews.call_args.kwargs['market_date_from'], date.fromisoformat('2026-04-23'))
        self.assertEqual(refresh_reviews.call_args.kwargs['market_date_to'], date.fromisoformat('2026-04-30'))
        self.assertFalse(refresh_reviews.call_args.kwargs['refresh_evaluations'])
        self.assertEqual(refresh_reviews.call_args.kwargs['evaluation_refresh_summary'], evaluation_summary)

    def test_get_review_calendar_reads_persisted_rows(self) -> None:
        self.service.refresh_recent_reviews(
            as_of_market_date='2026-04-30',
            primary_horizons=[5],
            evaluation_version='signal_decider_eval_test',
            trigger='test_runner',
        )

        result = self.service.get_review_calendar(
            month='2026-04',
            horizons=[1, 5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        self.assertEqual(result['read_model']['artifact_source'], 'signal_decider_calendar_review_items')
        self.assertEqual(result['summary']['day_count'], 1)
        self.assertEqual(result['items'][0]['calendar_status'], 'reviewed')
        self.assertEqual(result['items'][0]['scorecard']['target_hit_count'], 1)

    def test_get_review_date_reads_quality_alias_fields_from_persisted_payload(self) -> None:
        self.service.refresh_recent_reviews(
            as_of_market_date='2026-04-30',
            primary_horizons=[5],
            evaluation_version='signal_decider_eval_test',
            trigger='test_runner',
        )

        result = self.service.get_review_date(
            market_date='2026-04-23',
            horizons=[1, 5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        review = result['items'][0]['review']
        self.assertEqual(review['quality_verdict'], 'buy_should_have_taken')
        self.assertEqual(review['review_conclusion'], 'Buy held up on replay.')
        self.assertEqual(review['path_outcome'], 'target_hit')
        self.assertEqual([item['horizon_days'] for item in review['outcome_horizons']], [1, 5])

    def test_get_review_detail_reads_persisted_detail_payload(self) -> None:
        self.service.refresh_recent_reviews(
            as_of_market_date='2026-04-30',
            primary_horizons=[5],
            evaluation_version='signal_decider_eval_test',
            trigger='test_runner',
        )

        result = self.service.get_review_detail(
            market_date='2026-04-23',
            snapshot_id=84,
            horizons=[1, 5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        self.assertEqual(result['summary']['insight_code'], 'buy_should_have_taken')
        self.assertEqual(result['item']['review']['intraday_path_result'], 'target_only')
        self.assertEqual(result['item']['observed_price_path'][0]['trade_date'], '2026-04-24')

    def test_inspect_maintenance_returns_latest_runs(self) -> None:
        self.service.refresh_recent_reviews(
            as_of_market_date='2026-04-30',
            primary_horizons=[5],
            evaluation_version='signal_decider_eval_test',
            trigger='test_runner',
        )

        result = self.service.inspect_maintenance(evaluation_version='signal_decider_eval_test', limit=5)

        self.assertEqual(result['summary']['count'], 1)
        self.assertEqual(result['summary']['latest_status'], 'completed')
        self.assertEqual(result['items'][0]['evaluation_version'], 'signal_decider_eval_test')
