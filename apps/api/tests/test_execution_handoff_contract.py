import copy
import json
import unittest

from pydantic import ValidationError

from app.orders.exec_integration import SignalDeciderExecIntegrationService
from app.orders.execution_handoff_contract import (
    EXEC_AGENT_DECIDER_HANDOFF_SCHEMA_VERSION,
    EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
    LEGACY_EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
    EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
    EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION,
    PORTFOLIO_SUMMARY_READ_MODEL_SOURCE,
    serialize_exec_agent_decider_handoff_payload,
    serialize_execution_daily_board_payload,
    serialize_execution_dashboard_refresh_payload,
    serialize_execution_monitored_symbols_payload,
    serialize_execution_plan_dispatch_payload,
    serialize_execution_runtime_status_payload,
    serialize_execution_symbol_detail_payload,
    serialize_portfolio_risk_equivalent_payload,
)


class ExecutionHandoffContractTests(unittest.TestCase):
    maxDiff = None

    def _dispatch_payload(self, *, schema_version: str, item: dict) -> dict:
        return {
            'plan_id': 'test-plan',
            'source': 'test',
            'mode': 'mock',
            'market_date': '2026-08-19',
            'strategy_run_id': 'test-run',
            'requested_at': '2026-08-19T00:00:00+00:00',
            'items': [{
                'core_item_key': 'item-1',
                'agent_name': 'signal-decider',
                'decision_agent_name': 'signal-decider',
                'symbol': '005930',
                'side': 'buy',
                'intent': 'next_open_buy',
                'quantity_policy': {'type': 'fixed_quantity', 'quantity': '1'},
                'risk_context': {},
                'decision_snapshot_id': '1',
                **item,
            }],
            'metadata': {'schema_version': schema_version},
        }

    def test_v2_krx_dispatch_is_backfilled_for_compatibility(self) -> None:
        payload = serialize_execution_plan_dispatch_payload(
            self._dispatch_payload(schema_version=LEGACY_EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION, item={})
        )

        item = payload['items'][0]
        self.assertEqual((item['market'], item['exchange'], item['currency']), ('KRX', 'KRX', 'KRW'))

    def test_v3_us_dispatch_rejects_missing_routing_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, 'missing routing fields'):
            serialize_execution_plan_dispatch_payload(
                self._dispatch_payload(
                    schema_version=EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
                    item={'symbol': 'AAPL', 'market': 'US', 'currency': 'USD'},
                )
            )

    def _runtime(self, *, status: str = 'ready') -> dict:
        return {
            'status': status,
            'mode': 'mock',
            'exec_connected': status not in {'offline'},
            'automation_enabled': status not in {'paused', 'kill_switched', 'offline'},
            'paused': status == 'paused',
            'kill_switch_enabled': status == 'kill_switched',
            'monitoring_alive': status in {'ready', 'degraded', 'paused', 'kill_switched'},
            'last_heartbeat_at': '2026-04-30T00:05:00+00:00',
            'open_order_count': 1,
            'active_execution_job_count': 1,
            'pnl_coverage': {
                period: {
                    'status': 'available',
                    'status_reason': 'provided_by_exec',
                    'source': 'exec_runtime_status',
                }
                for period in ('daily', 'weekly', 'monthly')
            },
            'issues': [] if status == 'ready' else [{'code': status, 'message': status, 'retryable': False}],
            'core_integration_mode': 'dispatch',
            'delegate_execution': True,
        }

    def _freshness(self, *, endpoint: str = 'daily_board', status: str = 'fresh') -> dict:
        return {
            'schema_version': EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
            'endpoint': endpoint,
            'market_date': '2026-04-30',
            'status': status,
            'served_from_cache': False,
            'read_model_source': 'persisted_exec_read_model',
            'generated_at': '2026-04-30T00:15:35+00:00',
            'cached_at': '2026-04-30T00:15:35+00:00',
            'cache_age_seconds': 0,
            'cache_ttl_seconds': 5,
            'is_stale': status in {'stale', 'refreshing', 'unavailable'},
            'is_partial': status == 'partial',
            'refreshing': status == 'refreshing',
            'last_readback_synced_at': '2026-04-30T00:07:00+00:00',
            'readback_age_seconds': 515,
            'readback_synced_count': 1,
            'readback_missing_count': 0,
            'overview_item_count': 1,
            'output_item_count': 1,
        }

    def _refresh(self, *, status: str = 'not_requested') -> dict:
        return {
            'requested': status != 'not_requested',
            'status': status,
            'background': status != 'not_requested',
            'in_flight': status in {'scheduled', 'running'},
            'debounced': status == 'debounced',
            'timeout_seconds': 2.0,
            'debounce_seconds': 15.0,
            'last_trigger': 'test',
            'last_attempted_at': '2026-04-30T00:15:34+00:00',
            'started_at': '2026-04-30T00:15:34+00:00',
            'finished_at': None,
            'last_succeeded_at': '2026-04-30T00:07:00+00:00',
            'last_error': None,
        }

    def _today_activity(self) -> dict:
        return {
            'schema_version': EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION,
            'market_date': '2026-04-30',
            'buy_activity_status': 'available',
            'buy_activity_source': 'executions',
            'buy_filled_quantity_today': 5,
            'buy_avg_fill_price_today': 50100,
            'buy_last_filled_at': '2026-04-30T00:06:00+00:00',
            'buy_broker_order_ids': ['order-1'],
            'buy_broker_order_ids_omitted_count': 0,
            'sell_activity_status': 'none',
            'sell_activity_source': None,
            'sell_filled_quantity_today': 0,
            'sell_avg_fill_price_today': None,
            'sell_last_filled_at': None,
            'sell_broker_order_ids': [],
            'sell_broker_order_ids_omitted_count': 0,
            'net_open_quantity': 5,
        }

    def _daily_board_row(self) -> dict:
        return {
            'plan_item_id': 81,
            'account_reference': 'KIS:12345678-01',
            'account_label': 'Main',
            'symbol': '005930',
            'name': 'Samsung Electronics',
            'market': 'KOSPI',
            'agent_name': 'short-term-decider',
            'decision_agent_name': 'short-term-decider',
            'intent': 'intraday_watch_buy',
            'raw_action': 'watch_for_trigger',
            'tactical_action': 'watch_for_trigger',
            'generic_action': 'buy',
            'handoff_intent': 'intraday_watch_buy',
            'handoff_active': True,
            'approval_state': 'approved',
            'status': 'filled',
            'monitoring_state': 'watching_target_stop',
            'exec_monitoring_active': True,
            'exec_runtime_showable': True,
            'exec_runtime_state': 'held_position',
            'exec_runtime_exclusion_reason': None,
            'exec_agent_approved': True,
            'exec_agent_action': 'approve',
            'exec_agent_reason_code': 'small_tactical_upside',
            'exec_agent_target_profile': 'tactical_2pct',
            'exec_agent_target_tag': 'target_profile:tactical_2pct',
            'exec_agent_special_tactical_scalp': True,
            'held_position': True,
            'position_quantity': 5,
            'position_average_price': 50100,
            'approved_quantity': 5,
            'approved_notional': '250000.0000',
            'monitoring_statuses': ['stop_sell_monitoring', 'target_sell_monitoring'],
            'position_side': 'long',
            'planned_intents': ['stop_sell', 'target_sell'],
            'planned_quantity': 5,
            'filled_quantity': 5,
            'exit_filled_quantity': 0,
            'open_quantity': 5,
            'planned_price': 51500,
            'avg_fill_price': 50100,
            'today_activity': self._today_activity(),
            'decision_source': 'short-term-decider',
            'decision_snapshot_id': 145,
            'source_feedback_id': 702,
            'decision_run_id': 'short-run-145',
            'policy_version': 'short_term_decider_contract_v1',
            'execution_plan_id': 'core-short_term_decider-20260430-KIS_12345678_01',
            'broker_order_id': 'order-1',
            'last_updated_at': '2026-04-30T00:07:00+00:00',
            'failure_reason': None,
            'target_price': 54000,
            'stop_loss_price': 47000,
            'trigger_price': 50500,
            'chase_limit_price': 51500,
            'expected_holding_period': '1-2 trading days',
            'entry_execution_mode': 'pending_trigger_limit',
            'target_trade_date': '2026-04-30',
            'source_market_date': '2026-04-29',
            'current_price': 50200,
            'price_as_of': '2026-04-30T00:07:30+00:00',
            'price_received_at': '2026-04-30T00:07:31+00:00',
            'price_source': 'kis_realtime_ws',
            'price_status': 'available',
            'display_price_available': True,
            'display_price_fallback_reason': None,
            'realtime_subscribed': True,
            'realtime_subscription_reason': 'subscribed',
        }

    def _monitored_symbol_item(self) -> dict:
        return {
            'symbol': '005930',
            'name': 'Samsung Electronics',
            'market': 'KOSPI',
            'current_price': 50200,
            'price_as_of': '2026-04-30T00:07:30+00:00',
            'price_received_at': '2026-04-30T00:07:31+00:00',
            'price_source': 'kis_realtime_ws',
            'price_status': 'available',
            'display_price_available': True,
            'display_price_fallback_reason': None,
            'realtime_subscribed': True,
            'realtime_subscription_reason': 'subscribed',
            'monitoring_state': 'watching_target_stop',
            'exec_monitoring_active': True,
            'exec_runtime_showable': True,
            'exec_runtime_state': 'held_position',
            'exec_runtime_exclusion_reason': None,
            'exec_runtime_exclusion_reasons': [],
            'exec_agent_approved': True,
            'exec_agent_action': 'approve',
            'exec_agent_reason_code': 'small_tactical_upside',
            'exec_agent_target_profile': 'tactical_2pct',
            'exec_agent_target_tag': 'target_profile:tactical_2pct',
            'exec_agent_special_tactical_scalp': True,
            'held_position': True,
            'position_quantity': 5,
            'position_average_price': 50100,
            'approved_quantity': 5,
            'approved_notional': '250000.0000',
            'monitoring_statuses': ['stop_sell_monitoring', 'target_sell_monitoring'],
            'position_side': 'long',
            'planned_intents': ['stop_sell', 'target_sell'],
            'tactical_actions': ['watch_for_trigger'],
            'handoff_intents': ['intraday_watch_buy'],
            'target_price': 54000,
            'stop_loss_price': 47000,
            'trigger_price': 50500,
            'chase_limit_price': 51500,
            'expected_holding_period': '1-2 trading days',
            'decision_agent_names': ['short-term-decider'],
            'last_event_at': '2026-04-30T00:07:00+00:00',
            'account_references': ['KIS:12345678-01'],
            'status_counts': {'filled': 1},
        }

    def test_runtime_status_contract_locks_ready_degraded_offline_paused_and_kill_switch(self) -> None:
        service = SignalDeciderExecIntegrationService()
        integration = {
            'mode': 'dispatch',
            'enabled': True,
            'delegate_execution': True,
            'runtime_mode': 'mock',
            'issues': [],
        }

        ready = service._build_dashboard_runtime_status(
            integration=integration,
            runtime={
                'status': 'ok',
                'payload': {
                    'mode': 'mock',
                    'ready': True,
                    'safety': {'paused': False, 'kill_switch_enabled': False, 'automation_enabled': True},
                    'monitoring': {'alive': True, 'last_heartbeat_at': '2026-04-30T00:05:00+00:00'},
                    'counts': {'open_orders': 1, 'active_execution_jobs': 1},
                },
                'error': None,
            },
        )
        degraded = service._build_dashboard_runtime_status(
            integration=integration,
            runtime={'status': 'ok', 'payload': {'mode': 'mock', 'ready': True, 'monitoring': {'alive': False}}, 'error': None},
        )
        offline = service._build_dashboard_runtime_status(
            integration=integration,
            runtime={'status': 'unavailable', 'payload': None, 'error': {'code': 'unavailable', 'message': 'down'}},
        )
        paused = service._build_dashboard_runtime_status(
            integration=integration,
            runtime={'status': 'ok', 'payload': {'mode': 'mock', 'ready': True, 'safety': {'paused': True}}, 'error': None},
        )
        kill_switched = service._build_dashboard_runtime_status(
            integration=integration,
            runtime={
                'status': 'ok',
                'payload': {'mode': 'mock', 'ready': True, 'safety': {'kill_switch_enabled': True}},
                'error': None,
            },
        )

        self.assertEqual(ready['status'], 'ready')
        self.assertEqual(degraded['status'], 'degraded')
        self.assertEqual(offline['status'], 'offline')
        self.assertEqual(paused['status'], 'paused')
        self.assertEqual(kill_switched['status'], 'kill_switched')

        with self.assertRaises(ValidationError):
            serialize_execution_runtime_status_payload({**ready, 'status': 'sleeping'})

    def test_dispatch_payload_and_decider_handoff_contracts_lock_serialized_keys(self) -> None:
        handoff = serialize_exec_agent_decider_handoff_payload(
            {
                'schema_version': EXEC_AGENT_DECIDER_HANDOFF_SCHEMA_VERSION,
                'classification': 'delegated_for_exec_agent_sizing',
                'symbol': '005930',
                'target_trade_date': '2026-04-30',
                'source_market_date': '2026-04-29',
                'traceability': {'core_plan_item_id': 81, 'decision_agent_name': 'signal-decider'},
                'decider_input_context': {'request_payload': {}, 'input_feature_snapshot_payload': {}},
                'market_regime_context': {'schema_version': 'market_regime_context_v1', 'posture': 'normal'},
                'decision_input_quality_context': {'schema_version': 'decision_input_quality_v1'},
                'decider_output': {'decision_action': 'buy'},
                'eod_pre_decider_enrichment_metadata': {'schema_version': 'pre_decider_enrichment_v1'},
            }
        )
        dispatch = serialize_execution_plan_dispatch_payload(
            {
                'plan_id': 'core-signal_decider-20260430-KIS_12345678_01',
                'source': 'kai-trading-core',
                'mode': 'mock',
                'market_date': '2026-04-30',
                'strategy_run_id': 'signal-decider:2026-04-30:KIS_12345678_01',
                'requested_at': '2026-04-30T00:00:00+00:00',
                'items': [
                    {
                        'core_item_key': 'core-plan-81:next_open_buy',
                        'agent_name': 'signal-decider',
                        'decision_agent_name': 'signal-decider',
                        'symbol': '005930',
                        'asset_class': 'stock',
                        'market': 'KRX',
                        'exchange': 'KRX',
                        'currency': 'KRW',
                        'broker': 'kis',
                        'session_policy': {'session': 'regular', 'timezone': 'Asia/Seoul', 'allow_extended_hours': False},
                        'side': 'buy',
                        'intent': 'next_open_buy',
                        'quantity_policy': {'type': 'exec_agent_sized', 'budget_krw': '1500000'},
                        'limit_price': None,
                        'trigger_price': None,
                        'time_in_force': 'day',
                        'risk_context': {
                            'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
                            'classification': 'delegated_for_exec_agent_sizing',
                            'decider_handoff_context': handoff,
                        },
                        'decision_snapshot_id': '145',
                    },
                    {
                        'core_item_key': 'core-plan-81:target_sell',
                        'agent_name': 'signal-decider',
                        'decision_agent_name': 'signal-decider',
                        'symbol': '005930',
                        'asset_class': 'stock',
                        'market': 'KRX',
                        'exchange': 'KRX',
                        'currency': 'KRW',
                        'broker': 'kis',
                        'session_policy': {'session': 'regular', 'timezone': 'Asia/Seoul', 'allow_extended_hours': False},
                        'side': 'sell',
                        'intent': 'target_sell',
                        'quantity_policy': {
                            'type': 'position_exit_from_entry',
                            'linked_entry_item_key': 'core-plan-81:next_open_buy',
                        },
                        'limit_price': None,
                        'trigger_price': 54000,
                        'time_in_force': 'day',
                        'risk_context': {'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION},
                        'decision_snapshot_id': '145',
                    },
                    {
                        'core_item_key': 'core-plan-81:stop_sell',
                        'agent_name': 'signal-decider',
                        'decision_agent_name': 'signal-decider',
                        'symbol': '005930',
                        'asset_class': 'stock',
                        'market': 'KRX',
                        'exchange': 'KRX',
                        'currency': 'KRW',
                        'broker': 'kis',
                        'session_policy': {'session': 'regular', 'timezone': 'Asia/Seoul', 'allow_extended_hours': False},
                        'side': 'sell',
                        'intent': 'stop_sell',
                        'quantity_policy': {
                            'type': 'position_exit_from_entry',
                            'linked_entry_item_key': 'core-plan-81:next_open_buy',
                        },
                        'limit_price': None,
                        'trigger_price': 47000,
                        'time_in_force': 'day',
                        'risk_context': {'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION},
                        'decision_snapshot_id': '145',
                    },
                ],
                'metadata': {
                    'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
                    'agent_name': 'signal-decider',
                    'decision_agent_name': 'signal-decider',
                    'target_trade_date': '2026-04-30',
                },
            }
        )

        self.assertEqual(
            set(handoff),
            {
                'schema_version',
                'classification',
                'symbol',
                'target_trade_date',
                'source_market_date',
                'traceability',
                'decider_input_context',
                'market_regime_context',
                'decision_input_quality_context',
                'decider_output',
                'eod_pre_decider_enrichment_metadata',
            },
        )
        self.assertEqual(set(dispatch), {'plan_id', 'source', 'mode', 'market_date', 'strategy_run_id', 'requested_at', 'items', 'metadata'})
        self.assertEqual(dispatch['metadata']['schema_version'], EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION)
        self.assertEqual([item['intent'] for item in dispatch['items']], ['next_open_buy', 'target_sell', 'stop_sell'])
        self.assertEqual(dispatch['items'][0]['risk_context']['decider_handoff_context']['schema_version'], handoff['schema_version'])
        json.dumps(dispatch, ensure_ascii=False, sort_keys=True)

    def test_dashboard_read_model_contracts_lock_representative_shapes(self) -> None:
        row = self._daily_board_row()
        board = serialize_execution_daily_board_payload(
            {
                'request': {'market_date': '2026-04-30', 'refresh': False},
                'runtime': self._runtime(),
                'market_date': '2026-04-30',
                'summary': {
                    'items_total': 1,
                    'planned': 0,
                    'accepted': 0,
                    'scheduled': 0,
                    'submitted': 0,
                    'partial': 0,
                    'filled': 1,
                    'cancelled': 0,
                    'failed': 0,
                },
                'activity_coverage': {
                    'status': 'available',
                    'status_reason': 'core_read_model_items_available',
                    'exec_activity_detected': True,
                    'source': 'exec_runtime_status',
                },
                'freshness': self._freshness(),
                'refresh': self._refresh(),
                'items': [row],
            }
        )
        self.assertEqual(board['activity_coverage']['status'], 'available')
        self.assertEqual(board['runtime']['pnl_coverage']['daily']['status'], 'available')
        monitored = serialize_execution_monitored_symbols_payload(
            {
                'request': {'market_date': '2026-04-30', 'refresh': False},
                'runtime': self._runtime(status='degraded'),
                'market_date': '2026-04-30',
                'summary': {'items_total': 1, 'watching_target_stop': 1},
                'realtime_subscription': {'desired_symbols': ['005930'], 'desired_symbol_count': 1, 'max_subscriptions': 40},
                'freshness': self._freshness(endpoint='monitored_symbols', status='partial'),
                'refresh': self._refresh(status='scheduled'),
                'items': [self._monitored_symbol_item()],
            }
        )
        detail = serialize_execution_symbol_detail_payload(
            {
                'request': {'market_date': '2026-04-30', 'symbol': '005930', 'refresh': False},
                'runtime': self._runtime(status='offline'),
                'market_date': '2026-04-30',
                'symbol': '005930',
                'name': 'Samsung Electronics',
                'market': 'KOSPI',
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'display_price_available': True,
                'display_price_fallback_reason': None,
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'planning': {
                    'decision_source': 'short-term-decider',
                    'decision_agent_names': ['short-term-decider'],
                    'decision_snapshot_ids': [145],
                    'decision_run_ids': ['short-run-145'],
                    'source_feedback_ids': [702],
                    'policy_versions': ['short_term_decider_contract_v1'],
                    'account_references': ['KIS:12345678-01'],
                },
                'monitoring': {
                    'state': 'reconciling',
                    'position_side': 'long',
                    'planned_intents': ['stop_sell', 'target_sell'],
                    'last_event_at': '2026-04-30T00:07:00+00:00',
                    'runtime_status': 'offline',
                },
                'summary': {'items_total': 1, 'status_counts': {'filled': 1}},
                'freshness': self._freshness(endpoint='symbol_detail', status='unavailable'),
                'refresh': self._refresh(status='timeout'),
                'items': [{**row, 'dispatch': {'status': 'accepted'}, 'remote_state': {'derived_status': 'open'}}],
            }
        )

        self.assertEqual(
            set(board),
            {'request', 'runtime', 'market_date', 'summary', 'activity_coverage', 'freshness', 'refresh', 'items'},
        )
        self.assertEqual(board['freshness']['schema_version'], EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION)
        self.assertEqual(board['items'][0]['today_activity']['schema_version'], EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(monitored['runtime']['status'], 'degraded')
        self.assertEqual(monitored['freshness']['status'], 'partial')
        self.assertEqual(detail['runtime']['status'], 'offline')
        self.assertEqual(detail['monitoring']['state'], 'reconciling')
        json.dumps({'board': board, 'monitored': monitored, 'detail': detail}, ensure_ascii=False, sort_keys=True)

        bad_board = copy.deepcopy(board)
        bad_board['items'][0]['status'] = 'mystery'
        with self.assertRaises(ValidationError):
            serialize_execution_daily_board_payload(bad_board)

    def test_dashboard_payload_serializers_accept_disabled_refresh_states(self) -> None:
        for refresh_status in ('disabled', 'not_configured'):
            refresh = self._refresh(status=refresh_status)
            self.assertEqual(
                serialize_execution_dashboard_refresh_payload(refresh)['status'],
                refresh_status,
            )

            board = serialize_execution_daily_board_payload(
                {
                    'request': {'market_date': '2026-04-30', 'refresh': True},
                    'runtime': self._runtime(status='offline'),
                    'market_date': '2026-04-30',
                    'summary': {'items_total': 0},
                    'freshness': self._freshness(status='unavailable'),
                    'refresh': refresh,
                    'items': [],
                }
            )
            monitored = serialize_execution_monitored_symbols_payload(
                {
                    'request': {'market_date': '2026-04-30', 'refresh': True},
                    'runtime': self._runtime(status='offline'),
                    'market_date': '2026-04-30',
                    'summary': {'items_total': 0},
                    'realtime_subscription': {
                        'desired_symbols': [],
                        'desired_symbol_count': 0,
                        'max_subscriptions': 40,
                    },
                    'freshness': self._freshness(endpoint='monitored_symbols', status='unavailable'),
                    'refresh': refresh,
                    'items': [],
                }
            )
            detail = serialize_execution_symbol_detail_payload(
                {
                    'request': {'market_date': '2026-04-30', 'symbol': '005930', 'refresh': True},
                    'runtime': self._runtime(status='offline'),
                    'market_date': '2026-04-30',
                    'symbol': '005930',
                    'name': 'Samsung Electronics',
                    'market': 'KOSPI',
                    'display_price_available': False,
                    'realtime_subscribed': False,
                    'planning': {
                        'decision_agent_names': [],
                        'decision_snapshot_ids': [],
                        'account_references': [],
                    },
                    'monitoring': {'state': 'idle', 'planned_intents': []},
                    'summary': {'items_total': 0},
                    'freshness': self._freshness(endpoint='symbol_detail', status='unavailable'),
                    'refresh': refresh,
                    'items': [],
                }
            )

            self.assertEqual(board['refresh']['status'], refresh_status)
            self.assertEqual(monitored['refresh']['status'], refresh_status)
            self.assertEqual(detail['refresh']['status'], refresh_status)

    def test_portfolio_risk_equivalent_contract_documents_current_core_surface(self) -> None:
        payload = serialize_portfolio_risk_equivalent_payload(
            {
                'asOf': '2026-04-30T00:07:30+00:00',
                'source': PORTFOLIO_SUMMARY_READ_MODEL_SOURCE,
                'sync': {
                    'status': 'stale',
                    'lastSyncedAt': '2026-04-30T00:07:00+00:00',
                    'source': 'kis_account_inquiry_v1',
                    'message': 'Portfolio summary is serving cached data.',
                },
                'accounts': [
                    {
                        'id': 'KIS:12345678-01',
                        'summary': {'pnl': {'day': 22000, 'total': 22000, 'totalPercent': 4.0}},
                        'holdings': [{'symbol': '005930', 'quantity': 5, 'marketValue': 251000}],
                        'allocation': [{'label': 'Samsung Electronics', 'value': 251000, 'percent': 18.62}],
                    }
                ],
                'notices': ['Portfolio summary is serving cached data.'],
            }
        )

        self.assertEqual(payload['source'], PORTFOLIO_SUMMARY_READ_MODEL_SOURCE)
        self.assertEqual(payload['sync']['status'], 'stale')
        self.assertNotIn('contract_version', payload)

        bad_payload = copy.deepcopy(payload)
        bad_payload['sync']['status'] = 'unknown'
        with self.assertRaises(ValidationError):
            serialize_portfolio_risk_equivalent_payload(bad_payload)


if __name__ == '__main__':
    unittest.main()
