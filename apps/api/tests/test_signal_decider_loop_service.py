from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, event
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from app.agent_context.signal_decider_calibration_service import SignalDeciderCalibrationService
from app.agent_context.signal_decider_loop_service import (
    DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
    DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
    SIGNAL_DECIDER_POLICY_ACTIVATION_EVIDENCE_SCHEMA_VERSION,
    SignalDeciderLoopService,
    SignalDeciderLoopRepository,
    _dedupe_decision_snapshot_rows,
    _merge_policy_metadata_preserving_audit,
    build_signal_decider_feature_snapshot,
)
from app.db_models import (
    AgentContextFeedback,
    Base,
    SignalDeciderDecisionSnapshot,
    SignalDeciderEvaluation,
    SignalDeciderPolicyCandidate,
    SignalDeciderPolicyVersion,
    SignalDeciderRealizedOutcome,
)


def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.close()


def _feedback_row(
    *,
    feedback_id: int,
    market_date: str,
    symbol: str,
    action: str,
    snapshot_id: int | None = None,
    signal_score: int = 78,
    breakout_20d: bool = True,
    policy_version: str = DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
    target_price: int | None = None,
    stop_loss_price: int | None = None,
    expected_return_pct: float = 4.0,
    market_context: dict[str, object] | None = None,
    thesis_summary: str | None = None,
    supporting_evidence: list[str] | None = None,
    decision_risks: list[str] | None = None,
    invalidation_conditions: list[str] | None = None,
    data_quality_warnings: list[str] | None = None,
    wait_reason: dict[str, object] | None = None,
) -> SimpleNamespace:
    resolved_target_price = 105 if action == 'buy' and target_price is None else target_price
    resolved_stop_loss_price = 96 if action == 'buy' and stop_loss_price is None else stop_loss_price
    request_payload = {
        'symbol_day_context': {
            'technical_snapshot': {
                'trend_state': 'uptrend',
                'ma_alignment': 'bullish',
                'breakout_20d': breakout_20d,
                'breakdown_20d': False,
                'return_5d': 2.4,
                'return_20d': 8.1,
                'volume_ratio_20d': 1.6,
                'atr_pct_14': 2.2,
            },
            'action_summary': {
                'action_label': 'long_breakout',
                'bias_direction': 'bullish',
                'signal_score': signal_score,
                'confidence_tier': 'high',
                'risk_tier': 'low',
            },
            'signal_features': {
                'signal_count': 3,
                'volume_power_rank': 2,
                'overtime_volume_rank': 3,
                'overtime_fluctuation_rank': 1,
            },
            'price_context': {
                'close_location_ratio': 0.82,
                'gap_vs_prev_close_pct': 0.3,
                'range_pct': 4.1,
            },
        },
        'scores': {
            'unified_value': {
                'status': 'available',
                'score': 0.84,
                'higher_is_better': True,
                'meaning': 'Composite value attractiveness score across stored cross-sectional value percentiles.',
                'method': 'Equal-weight mean of PER, PBR, dividend yield, FCF yield, and EV/EBITDA percentiles.',
                'components_used': 5,
                'components_possible': 5,
                'coverage_notes': [],
                'as_of_market_date': market_date,
            },
            'quality': {
                'status': 'partial',
                'score': 0.74,
                'higher_is_better': True,
                'meaning': 'Business quality score across profitability, cash-flow quality, and balance-sheet quality inputs.',
                'method': 'Equal-weight mean of the available quality component percentiles.',
                'components_used': 5,
                'components_possible': 6,
                'coverage_notes': ['Missing quality components: accruals_ratio.'],
                'as_of_market_date': market_date,
            },
            'momentum': {
                'status': 'available',
                'score': 0.91,
                'higher_is_better': True,
                'meaning': 'Medium-term price-strength score plus trend and overheat gating context from stored daily bars.',
                'method': 'Ranks 12-1 momentum from stored daily closes, then records trend confirmation, a top-percentile eligibility gate, and the configured recent-overheat guard.',
                'components_used': 3,
                'components_possible': 3,
                'coverage_notes': [],
                'as_of_market_date': market_date,
            },
        },
    }
    if market_context is not None:
        request_payload['market_context'] = market_context
    response_payload = {
        'role': 'signal-decider',
        'agent_name': 'signal-decider',
        'symbol': symbol,
        'market_date': market_date,
        'policy_version': policy_version,
        'preferred_action': action,
        'next_day_open_action': action,
        'entry_price_basis': 'next_open',
        'wait_reason': wait_reason,
        'target_price': resolved_target_price,
        'expected_return_pct': expected_return_pct,
        'stop_loss_price': resolved_stop_loss_price,
        'suggested_holding_period': '3-10 trading days',
        'confidence_score_10': 7,
        'conviction_score_10': 6,
        'thesis_summary': thesis_summary or f'{action} decision for {symbol}',
        'supporting_evidence': supporting_evidence or ['Breadth improved into the close.'],
        'decision_risks': decision_risks or ['Leadership remained narrow.'],
        'invalidation_conditions': invalidation_conditions or ['이탈 종가가 반복되면 보수적으로 본다.'],
        'data_quality_warnings': data_quality_warnings or [],
        'specialist_disagreement': [],
        'traceability': {'agent_name': 'signal-decider', 'symbol': symbol, 'market_date': market_date},
    }
    traceability_payload = {
        'schema_version': 'signal_decider_trace_v2',
        'agent': {
            'agent_name': 'signal-decider',
            'agent_version': 'openclaw-v1',
            'feedback_id': feedback_id,
            'decision_run_id': f'signal-run-{snapshot_id or feedback_id}',
        },
        'policy': {'version': policy_version, 'activation_mode': 'manual_only'},
        'upstream_references': {},
        'gate_checks': {'required_upstream_roles': [], 'available_upstream_roles': []},
        'factor_contributions': {'feature_flags': [{'key': 'technical.breakout_20d', 'value': str(breakout_20d).lower()}]},
        'final_action_rationale': {
            'decision_action': action,
            'summary': thesis_summary or f'{action} decision for {symbol}',
            'wait_reason': wait_reason,
        },
        'raw_traceability': response_payload['traceability'],
    }
    snapshot_key = snapshot_id or feedback_id
    return SimpleNamespace(
        id=snapshot_key,
        feedback_id=feedback_id,
        market_date=date.fromisoformat(market_date),
        symbol=symbol,
        agent_name='signal-decider',
        agent_version='openclaw-v1',
        decision_run_id=f'signal-run-{snapshot_key}',
        feedback_received_at=datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
        policy_version=policy_version,
        decision_action=action,
        preferred_action=action,
        next_day_open_action=action,
        entry_price_basis='next_open',
        target_price=resolved_target_price,
        expected_return_pct=expected_return_pct,
        stop_loss_price=resolved_stop_loss_price,
        suggested_holding_period='3-10 trading days',
        confidence_score_10=7,
        conviction_score_10=6,
        thesis_summary=thesis_summary or f'{action} decision for {symbol}',
        supporting_evidence_payload=supporting_evidence or ['Breadth improved into the close.'],
        decision_risks_payload=decision_risks or ['Leadership remained narrow.'],
        invalidation_conditions_payload=invalidation_conditions or ['이탈 종가가 반복되면 보수적으로 본다.'],
        data_quality_warnings_payload=data_quality_warnings or [],
        specialist_disagreement_payload=[],
        traceability_payload=traceability_payload,
        input_feature_snapshot_payload={
            'technical': {
                'trend_state': 'uptrend',
                'ma_alignment': 'bullish',
                'breakout_20d': breakout_20d,
                'breakdown_20d': False,
                'return_5d': 2.4,
                'return_20d': 8.1,
                'volume_ratio_20d': 1.6,
                'atr_pct_14': 2.2,
            },
            'action_summary': {
                'action_label': 'long_breakout',
                'bias_direction': 'bullish',
                'signal_score': signal_score,
                'signal_score_bucket': 'high',
                'confidence_tier': 'high',
                'risk_tier': 'low',
            },
            'signal_features': {
                'signal_count': 3,
                'signal_count_bucket': '3plus',
                'volume_power_rank': 2,
                'overtime_volume_rank': 3,
                'overtime_fluctuation_rank': 1,
            },
            'price_context': {
                'close_location_ratio': 0.82,
                'gap_vs_prev_close_pct': 0.3,
                'range_pct': 4.1,
            },
            'scores': request_payload['scores'],
            'meta': {
                'supporting_evidence_count': 1,
                'decision_risk_count': 1,
                'data_warning_count': 0,
                'disagreement_count': 0,
                'has_data_warnings': False,
                'has_disagreement': False,
                'has_score_context': True,
            },
        },
        request_payload=request_payload,
        response_payload=response_payload,
        response_summary_payload={'model': 'openclaw/signal-decider', 'policy_version': policy_version},
        created_at=datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
    )


def _policy_version_record(
    *,
    policy_version: str = DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
    status: str = 'active',
    parent_policy_version: str | None = None,
    source_candidate_version: str | None = None,
    change_summary: str | None = None,
    created_by: str = 'system:bootstrap',
    activated_by: str | None = 'system:bootstrap',
    created_at: str = '2026-04-23T06:00:00+00:00',
    activated_at: str | None = '2026-04-23T06:00:00+00:00',
    retired_at: str | None = None,
) -> dict:
    return {
        'id': None,
        'policy_version': policy_version,
        'status': status,
        'parent_policy_version': parent_policy_version,
        'source_candidate_version': source_candidate_version,
        'change_summary': change_summary or 'Baseline manual-only signal-decider policy.',
        'rule_bundle': {
            'schema_version': 'signal_decider_policy_bundle_v1',
            'policy_version': policy_version,
            'review_mode': 'manual_only',
            'human_activation_required': True,
        },
        'metadata': {
            'schema_version': 'signal_decider_policy_metadata_v1',
            'activation_mode': 'manual_only',
            'candidate_activation_required': True,
        },
        'created_by': created_by,
        'activated_by': activated_by if status == 'active' else None,
        'created_at': created_at,
        'activated_at': activated_at if status == 'active' else None,
        'retired_at': retired_at,
    }


def _promotion_replay_evidence(
    *,
    policy_version: str = 'signal_decider_policy_v2',
    fee_bps: float = 10,
    slippage_bps: float = 5,
    estimated_net_delta_krw: int = 200,
    symbols: list[str] | None = None,
    policy_versions: list[str] | None = None,
) -> dict:
    baseline_net_pnl = -100
    candidate_net_pnl = baseline_net_pnl + estimated_net_delta_krw
    return {
        'schema_version': SIGNAL_DECIDER_POLICY_ACTIVATION_EVIDENCE_SCHEMA_VERSION,
        'candidate_policy_version': policy_version,
        'replay_artifact': {
            'schema_version': 'decider_replay_v1',
            'metadata': {
                'profitability_basis': 'estimated_net',
                'promotion_metrics_basis': 'estimated_net',
                'promotion_cohort_basis': 'fixed_horizon_counterfactual',
                'promotion_cost_gate': {
                    'eligible': True,
                    'status': 'passed',
                    'reason': None,
                    'minimum_assumptions': {
                        'fee_bps': 10,
                        'slippage_bps': 5,
                    },
                    'effective_assumptions': {
                        'fee_bps': fee_bps,
                        'slippage_bps': slippage_bps,
                    },
                },
            },
            'request': {
                'market_date_from': '2026-04-20',
                'market_date_to': '2026-04-30',
                'agent_name': 'signal-decider',
                'symbols': list(symbols or ['005930']),
                'policy_versions': list(policy_versions or ['signal_decider_policy_v1']),
                'horizon_days': 5,
                'rule_config': {
                    'schema_version': 'decider_replay_rule_config_v1',
                    'candidate_policy_version': policy_version,
                    'rules': [
                        {
                            'name': 'candidate_policy_rule',
                            'predicates': [
                                {
                                    'type': 'field',
                                    'field': 'technical.breakout_20d',
                                    'op': '==',
                                    'value': True,
                                }
                            ],
                        }
                    ],
                },
                'cost_assumptions': {
                    'fee_bps': fee_bps,
                    'slippage_bps': slippage_bps,
                    'notional_krw': 1_000_000,
                },
                'top_examples_limit': 10,
            },
            'summary': {
                'baseline_trade_count': 2,
                'candidate_trade_count': 1,
                'skipped_buys': 1,
                'estimated_net_pnl_delta_krw': estimated_net_delta_krw,
                'baseline': {'estimated_net_pnl_krw': baseline_net_pnl},
                'candidate': {'estimated_net_pnl_krw': candidate_net_pnl},
                'coverage': {
                    'source_decision_count': 3,
                    'economic_cohort_count': 2,
                    'censored_or_excluded_count': 1,
                    'cohort_basis': 'fixed_horizon_counterfactual',
                },
                'promotion_metrics': {
                    'basis': 'estimated_net',
                    'estimated_net_pnl_delta_krw': estimated_net_delta_krw,
                    'estimated_net_win_rate_delta': 0.25,
                    'estimated_net_average_return_delta': 0.01,
                },
            },
            'examples': [],
            'markdown_summary': '# replay',
        },
    }


def _bind_candidate_replay_contract(
    policy: dict,
    replay_evidence: dict,
    *,
    candidate_kind: str = 'tighten_buy_gate',
) -> dict:
    replay_request = replay_evidence['replay_artifact']['request']
    policy['rule_bundle'].update(
        {
            'base_policy_version': policy['parent_policy_version'],
            'candidate_kind': candidate_kind,
            'market_date_window': {
                'from': replay_request['market_date_from'],
                'to': replay_request['market_date_to'],
            },
            'primary_horizon_days': replay_request['horizon_days'],
            'proposed_changes': [
                {
                    'change_type': candidate_kind,
                    'feature_gate': {
                        'key': 'technical.breakout_20d',
                        'value': 'true',
                    },
                }
            ],
            'replay_contract': deepcopy(replay_request),
        }
    )
    policy['metadata']['evaluation_summary'] = {
        'market_date_from': replay_request['market_date_from'],
        'market_date_to': replay_request['market_date_to'],
        'horizon_days': replay_request['horizon_days'],
        'symbols': deepcopy(replay_request['symbols']),
    }
    return policy


class FakeDeciderReplayService:
    def __init__(self, replay_artifact: dict) -> None:
        self.replay_artifact = deepcopy(replay_artifact)
        self.calls: list[dict] = []

    def replay(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return deepcopy(self.replay_artifact)


class FakeSignalDeciderLoopRepository:
    def __init__(self, snapshot_rows):
        self.snapshot_rows = list(snapshot_rows)
        self.outcomes = {}
        self.evaluations = {}
        self.candidates = {}
        self.policy_versions = {
            DEFAULT_SIGNAL_DECIDER_POLICY_VERSION: {
                **_policy_version_record(),
                'id': 1,
            }
        }
        self.next_outcome_id = 1
        self.next_evaluation_id = 1
        self.next_candidate_id = 1
        self.next_policy_version_id = 2
        self.list_decision_snapshots_calls = []
        self.list_realized_outcomes_calls = []
        self.list_evaluations_calls = []

    def reset_calls(self):
        self.list_decision_snapshots_calls = []
        self.list_realized_outcomes_calls = []
        self.list_evaluations_calls = []

    def list_decision_snapshots(
        self,
        *,
        market_date=None,
        market_date_from=None,
        market_date_to=None,
        symbols=None,
        decision_actions=None,
        policy_versions=None,
        dedupe_feedback_results=False,
        lightweight_results=False,
        limit=None,
        offset=0,
    ):
        self.list_decision_snapshots_calls.append(
            {
                'market_date': market_date,
                'market_date_from': market_date_from,
                'market_date_to': market_date_to,
                'symbols': list(symbols or []),
                'decision_actions': list(decision_actions or []),
                'policy_versions': list(policy_versions or []),
                'dedupe_feedback_results': dedupe_feedback_results,
                'lightweight_results': lightweight_results,
                'limit': limit,
                'offset': offset,
            }
        )
        rows = list(self.snapshot_rows)
        if market_date is not None:
            rows = [row for row in rows if row.market_date == market_date]
        if market_date_from is not None:
            rows = [row for row in rows if row.market_date >= market_date_from]
        if market_date_to is not None:
            rows = [row for row in rows if row.market_date <= market_date_to]
        normalized_symbols = set(symbols or [])
        if normalized_symbols:
            rows = [row for row in rows if row.symbol in normalized_symbols]
        normalized_actions = set(decision_actions or [])
        if normalized_actions:
            rows = [row for row in rows if row.decision_action in normalized_actions]
        normalized_policy_versions = set(policy_versions or [])
        if normalized_policy_versions:
            rows = [row for row in rows if row.policy_version in normalized_policy_versions]
        rows.sort(key=lambda row: (row.market_date, row.created_at, row.id), reverse=True)
        if dedupe_feedback_results:
            rows = _dedupe_decision_snapshot_rows(rows)
        if offset:
            rows = rows[offset:]
        return rows[:limit] if limit is not None else rows

    def get_decision_snapshot(self, *, snapshot_id):
        for row in self.snapshot_rows:
            if row.id == snapshot_id:
                return row
        return None

    def list_realized_outcomes(self, *, decision_snapshot_ids, horizons=None):
        self.list_realized_outcomes_calls.append(
            {
                'decision_snapshot_ids': list(decision_snapshot_ids),
                'horizons': None if horizons is None else list(horizons),
            }
        )
        rows = [
            item
            for item in self.outcomes.values()
            if item['decision_snapshot_id'] in set(decision_snapshot_ids)
            and (horizons is None or item['horizon_days'] in set(horizons))
        ]
        rows.sort(key=lambda item: (item['decision_snapshot_id'], item['horizon_days']))
        return rows

    def list_evaluations(self, *, decision_snapshot_ids, horizons=None, evaluation_version=None):
        self.list_evaluations_calls.append(
            {
                'decision_snapshot_ids': list(decision_snapshot_ids),
                'horizons': None if horizons is None else list(horizons),
                'evaluation_version': evaluation_version,
            }
        )
        rows = [
            item
            for item in self.evaluations.values()
            if item['decision_snapshot_id'] in set(decision_snapshot_ids)
            and (horizons is None or item['horizon_days'] in set(horizons))
            and (evaluation_version is None or item['evaluation_version'] == evaluation_version)
        ]
        rows.sort(key=lambda item: (item['decision_snapshot_id'], item['horizon_days']))
        return rows

    def list_evaluations_by_range(
        self,
        *,
        market_date_from,
        market_date_to,
        symbols=None,
        horizons=None,
        evaluation_version=None,
    ):
        rows = [
            item
            for item in self.evaluations.values()
            if market_date_from <= date.fromisoformat(item['market_date']) <= market_date_to
            and (not symbols or item['symbol'] in set(symbols))
            and (horizons is None or item['horizon_days'] in set(horizons))
            and (evaluation_version is None or item['evaluation_version'] == evaluation_version)
        ]
        rows.sort(key=lambda item: (item['market_date'], item['symbol'], item['horizon_days']))
        return rows

    def list_policy_candidates(self, *, status=None, evaluation_version=None, base_policy_version=None, limit=50):
        rows = list(self.candidates.values())
        if status is not None:
            rows = [row for row in rows if row['status'] == status]
        if evaluation_version is not None:
            rows = [row for row in rows if row['evaluation_version'] == evaluation_version]
        if base_policy_version is not None:
            rows = [row for row in rows if row['base_policy_version'] == base_policy_version]
        rows.sort(key=lambda row: row['candidate_version'])
        return rows[:limit]

    def get_policy_candidate(self, *, candidate_version):
        return self.candidates.get(candidate_version)

    def list_policy_versions(self, *, status=None, policy_versions=None, limit=50):
        rows = list(self.policy_versions.values())
        if status is not None:
            rows = [row for row in rows if row['status'] == status]
        normalized_policy_versions = set(policy_versions or [])
        if normalized_policy_versions:
            rows = [row for row in rows if row['policy_version'] in normalized_policy_versions]
        rows.sort(
            key=lambda row: (
                0 if row['status'] == 'active' else 1,
                -(1 if row['activated_at'] else 0),
                row['activated_at'] or '',
                row['created_at'] or '',
                row['policy_version'],
            ),
        )
        return rows[:limit]

    def get_policy_version(self, *, policy_version):
        return self.policy_versions.get(policy_version)

    def get_active_policy_version(self):
        active_rows = [row for row in self.policy_versions.values() if row['status'] == 'active']
        if not active_rows:
            return None
        if len(active_rows) > 1:
            raise RuntimeError(
                'Signal-decider policy registry invariant violated: multiple active policies exist.'
            )
        active_rows.sort(key=lambda row: (row['activated_at'] or '', row['created_at'] or '', row['policy_version']), reverse=True)
        return active_rows[0]

    def bulk_upsert_realized_outcomes(self, *, items):
        persisted = []
        for item in items:
            key = (item['decision_snapshot_id'], item['horizon_days'])
            stored = dict(item)
            stored.setdefault('id', self.outcomes.get(key, {}).get('id', self.next_outcome_id))
            if key not in self.outcomes:
                self.next_outcome_id += 1
            stored['market_date'] = stored['market_date'].isoformat()
            stored['entry_trade_date'] = stored['entry_trade_date'].isoformat() if stored['entry_trade_date'] else None
            stored['exit_trade_date'] = stored['exit_trade_date'].isoformat() if stored['exit_trade_date'] else None
            stored['computed_at'] = stored['computed_at'].isoformat()
            diagnostics = dict(stored.get('diagnostics_payload') or {})
            stored['best_exit_trade_date'] = diagnostics.get('best_exit_trade_date')
            stored['worst_drawdown_trade_date'] = diagnostics.get('worst_drawdown_trade_date')
            stored['first_target_hit_trade_date'] = diagnostics.get('first_target_hit_trade_date')
            stored['first_stop_loss_hit_trade_date'] = diagnostics.get('first_stop_loss_hit_trade_date')
            stored['price_path'] = stored.get('price_path_payload')
            stored['coverage'] = dict(diagnostics.get('coverage') or {})
            stored['status_reason'] = stored['coverage'].get('status_reason')
            stored['diagnostics'] = diagnostics
            self.outcomes[key] = stored
            persisted.append(stored)
        return persisted

    def bulk_upsert_evaluations(self, *, items):
        persisted = []
        for item in items:
            key = (item['decision_snapshot_id'], item['horizon_days'], item['evaluation_version'])
            stored = dict(item)
            stored.setdefault('id', self.evaluations.get(key, {}).get('id', self.next_evaluation_id))
            if key not in self.evaluations:
                self.next_evaluation_id += 1
            stored['market_date'] = stored['market_date'].isoformat()
            stored['evaluated_at'] = stored['evaluated_at'].isoformat()
            stored['rationale'] = dict(stored.get('rationale_payload') or {})
            stored['diagnostics'] = dict(stored.get('diagnostics_payload') or {})
            stored['coverage'] = dict(stored['rationale'].get('coverage') or stored['diagnostics'].get('coverage') or {})
            stored['status_reason'] = stored['coverage'].get('status_reason')
            self.evaluations[key] = stored
            persisted.append(stored)
        return persisted

    def bulk_upsert_policy_candidates(self, *, items):
        persisted = []
        for item in items:
            key = item['candidate_version']
            stored = dict(item)
            stored.setdefault('id', self.candidates.get(key, {}).get('id', self.next_candidate_id))
            if key not in self.candidates:
                self.next_candidate_id += 1
            stored['market_date_from'] = stored['market_date_from'].isoformat()
            stored['market_date_to'] = stored['market_date_to'].isoformat()
            stored['generated_at'] = stored['generated_at'].isoformat()
            existing = self.candidates.get(key)
            if existing and existing.get('activated_at') is not None:
                stored['status'] = existing['status']
                stored['activated_at'] = existing['activated_at']
            self.candidates[key] = stored
            persisted.append(stored)
        return persisted

    def bulk_upsert_policy_versions(self, *, items):
        persisted = []
        for item in items:
            key = item['policy_version']
            existing = self.policy_versions.get(key)
            stored = {
                'id': existing.get('id') if existing else self.next_policy_version_id,
                'policy_version': key,
                'status': item['status'],
                'parent_policy_version': item['parent_policy_version'],
                'source_candidate_version': item['source_candidate_version'],
                'change_summary': item['change_summary'],
                'rule_bundle': item['rule_bundle_payload'],
                'metadata': item['metadata_payload'],
                'created_by': existing.get('created_by') if existing else item['created_by'],
                'activated_by': item['activated_by'],
                'created_at': existing.get('created_at') if existing else item['created_at'].isoformat(),
                'activated_at': item['activated_at'].isoformat() if item['activated_at'] else None,
                'retired_at': item['retired_at'].isoformat() if item['retired_at'] else None,
            }
            if existing and existing['status'] in {'active', 'retired'}:
                stored['status'] = existing['status']
                stored['metadata'] = _merge_policy_metadata_preserving_audit(
                    metadata_payload=stored['metadata'],
                    existing_metadata_payload=existing.get('metadata'),
                )
                stored['activated_by'] = existing['activated_by']
                stored['activated_at'] = existing['activated_at']
                stored['retired_at'] = existing['retired_at']
            if not existing:
                self.next_policy_version_id += 1
            self.policy_versions[key] = stored
            persisted.append(stored)
        return persisted

    def activate_policy_version(
        self,
        *,
        policy_version,
        activated_by,
        activation_note,
        promotion_evidence,
        activated_at,
    ):
        target = self.policy_versions.get(policy_version)
        if target is None:
            return None
        activated_at_text = activated_at.isoformat()
        for row in self.policy_versions.values():
            if row['status'] == 'active' and row['policy_version'] != policy_version:
                row['status'] = 'retired'
                row['retired_at'] = activated_at_text
        target['status'] = 'active'
        target['activated_by'] = activated_by
        target['activated_at'] = activated_at_text
        target['retired_at'] = None
        metadata = dict(target.get('metadata') or {})
        metadata['last_activation_note'] = activation_note
        metadata['promotion_evidence'] = dict(promotion_evidence)
        target['metadata'] = metadata
        candidate_version = target.get('source_candidate_version') or policy_version
        if candidate_version in self.candidates:
            evidence = dict(self.candidates[candidate_version].get('evidence') or {})
            evidence['promotion_evidence'] = dict(promotion_evidence)
            self.candidates[candidate_version]['evidence'] = evidence
            self.candidates[candidate_version]['status'] = 'activated'
            self.candidates[candidate_version]['activated_at'] = activated_at_text
        return target


def _apply_calibration_context(
    row,
    *,
    confidence_score_10: float,
    signal_score: float,
    close_location_ratio: float,
    unified_value_score: float,
    momentum_score: float,
    quality_score: float,
    liquidity_percentile: float,
    passes_liquidity_floor: bool,
    signal_feature_row_kind: str = 'direct_plus_fallback',
) -> None:
    signal_score_bucket = 'high' if signal_score >= 70 else 'medium' if signal_score >= 40 else 'low'
    row.confidence_score_10 = confidence_score_10
    row.response_payload['confidence_score_10'] = confidence_score_10
    row.input_feature_snapshot_payload['action_summary']['signal_score'] = signal_score
    row.input_feature_snapshot_payload['action_summary']['signal_score_bucket'] = signal_score_bucket
    row.input_feature_snapshot_payload['price_context']['close_location_ratio'] = close_location_ratio
    row.request_payload['scores'] = {
        'unified_value': {
            'status': 'available',
            'score': unified_value_score,
            'components_used': 5,
            'components_possible': 5,
            'coverage_notes': [],
            'as_of_market_date': row.market_date.isoformat(),
        },
        'momentum': {
            'status': 'available',
            'score': momentum_score,
            'components_used': 3,
            'components_possible': 3,
            'coverage_notes': [],
            'as_of_market_date': row.market_date.isoformat(),
        },
        'quality': {
            'status': 'available',
            'score': quality_score,
            'components_used': 6,
            'components_possible': 6,
            'coverage_notes': [],
            'as_of_market_date': row.market_date.isoformat(),
        },
    }
    row.request_payload['factor_summary'] = {
        'requested_market_date': row.market_date.isoformat(),
        'factor_scores': {
            'unified_value': {'status': 'available', 'score': unified_value_score},
            'momentum': {'status': 'available', 'score': momentum_score},
            'quality': {'status': 'available', 'score': quality_score},
        },
        'signal_features': {
            'status': 'available',
            'row_kind': signal_feature_row_kind,
            'fallback_groups_present': ['price_action', 'factor_scores'],
        },
    }
    row.request_payload['liquidity_context'] = {
        'status': 'available',
        'turnover': {
            'status': 'available',
            'turnover_source': 'provider',
            'turnover_krw': 8_800_000_000,
            'trade_date': row.market_date.isoformat(),
        },
        'ranking': {
            'status': 'available',
            'liquidity_percentile': liquidity_percentile,
            'passes_liquidity_floor': passes_liquidity_floor,
            'as_of_market_date': row.market_date.isoformat(),
        },
        'intraday': {
            'status': 'partial',
            'quote_spread_points': 100,
            'quote_balance_rank': 5,
        },
    }
    row.input_feature_snapshot_payload['scores'] = row.request_payload['scores']


def _build_signal_decider_calibration_fixture():
    rows = [
        _feedback_row(
            feedback_id=1,
            snapshot_id=101,
            market_date='2026-04-20',
            symbol='005930',
            action='buy',
        ),
        _feedback_row(
            feedback_id=2,
            snapshot_id=102,
            market_date='2026-04-20',
            symbol='000660',
            action='buy',
        ),
        _feedback_row(
            feedback_id=3,
            snapshot_id=103,
            market_date='2026-04-20',
            symbol='035420',
            action='wait',
            target_price=55,
            stop_loss_price=47,
            wait_reason={
                'code': 'stale_quote_orderbook',
                'label': 'Stale quote/orderbook freshness',
                'bucket': 'data_gap',
                'blocking_by_default': True,
                'summary': 'Realtime orderbook freshness is stale.',
                'source': 'provided',
            },
        ),
        _feedback_row(
            feedback_id=4,
            snapshot_id=104,
            market_date='2026-04-20',
            symbol='051910',
            action='wait',
            target_price=55,
            stop_loss_price=47,
            wait_reason={
                'code': 'stale_quote_orderbook',
                'label': 'Stale quote/orderbook freshness',
                'bucket': 'data_gap',
                'blocking_by_default': True,
                'summary': 'Realtime orderbook freshness is stale.',
                'source': 'provided',
            },
        ),
        _feedback_row(
            feedback_id=5,
            snapshot_id=105,
            market_date='2026-04-21',
            symbol='068270',
            action='buy',
        ),
        _feedback_row(
            feedback_id=6,
            snapshot_id=106,
            market_date='2026-04-21',
            symbol='105560',
            action='buy',
        ),
        _feedback_row(
            feedback_id=7,
            snapshot_id=107,
            market_date='2026-04-29',
            symbol='207940',
            action='buy',
        ),
    ]

    _apply_calibration_context(
        rows[0],
        confidence_score_10=9.0,
        signal_score=88.0,
        close_location_ratio=0.46,
        unified_value_score=0.91,
        momentum_score=0.92,
        quality_score=0.82,
        liquidity_percentile=0.93,
        passes_liquidity_floor=True,
    )
    _apply_calibration_context(
        rows[1],
        confidence_score_10=8.5,
        signal_score=84.0,
        close_location_ratio=0.52,
        unified_value_score=0.87,
        momentum_score=0.89,
        quality_score=0.79,
        liquidity_percentile=0.88,
        passes_liquidity_floor=True,
    )
    _apply_calibration_context(
        rows[2],
        confidence_score_10=4.0,
        signal_score=62.0,
        close_location_ratio=0.83,
        unified_value_score=0.74,
        momentum_score=0.81,
        quality_score=0.69,
        liquidity_percentile=0.86,
        passes_liquidity_floor=True,
    )
    _apply_calibration_context(
        rows[3],
        confidence_score_10=4.5,
        signal_score=66.0,
        close_location_ratio=0.79,
        unified_value_score=0.71,
        momentum_score=0.78,
        quality_score=0.67,
        liquidity_percentile=0.81,
        passes_liquidity_floor=True,
    )
    _apply_calibration_context(
        rows[4],
        confidence_score_10=7.2,
        signal_score=73.0,
        close_location_ratio=0.61,
        unified_value_score=0.76,
        momentum_score=0.74,
        quality_score=0.72,
        liquidity_percentile=0.67,
        passes_liquidity_floor=True,
    )
    _apply_calibration_context(
        rows[5],
        confidence_score_10=6.8,
        signal_score=58.0,
        close_location_ratio=0.55,
        unified_value_score=0.63,
        momentum_score=0.61,
        quality_score=0.64,
        liquidity_percentile=0.41,
        passes_liquidity_floor=False,
    )
    _apply_calibration_context(
        rows[6],
        confidence_score_10=7.5,
        signal_score=71.0,
        close_location_ratio=0.72,
        unified_value_score=0.79,
        momentum_score=0.77,
        quality_score=0.74,
        liquidity_percentile=0.72,
        passes_liquidity_floor=True,
    )

    bars_by_symbol = {
        '005930': [
            {'trade_date': '2026-04-20', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 1000},
            {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 106, 'low_price': 99, 'close_price': 104, 'volume': 1000},
            {'trade_date': '2026-04-22', 'open_price': 104, 'high_price': 107, 'low_price': 103, 'close_price': 105, 'volume': 1000},
            {'trade_date': '2026-04-23', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 111, 'low_price': 107, 'close_price': 110, 'volume': 1000},
        ],
        '000660': [
            {'trade_date': '2026-04-20', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
            {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 107, 'low_price': 99, 'close_price': 105, 'volume': 1000},
            {'trade_date': '2026-04-22', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 106, 'volume': 1000},
            {'trade_date': '2026-04-23', 'open_price': 106, 'high_price': 109, 'low_price': 105, 'close_price': 108, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 108, 'high_price': 110, 'low_price': 107, 'close_price': 109, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 109, 'high_price': 112, 'low_price': 108, 'close_price': 111, 'volume': 1000},
        ],
        '035420': [
            {'trade_date': '2026-04-20', 'open_price': 49, 'high_price': 50, 'low_price': 48, 'close_price': 49, 'volume': 900},
            {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 56, 'low_price': 49, 'close_price': 55, 'volume': 900},
            {'trade_date': '2026-04-22', 'open_price': 55, 'high_price': 57, 'low_price': 54, 'close_price': 56, 'volume': 900},
            {'trade_date': '2026-04-23', 'open_price': 56, 'high_price': 58, 'low_price': 55, 'close_price': 57, 'volume': 900},
            {'trade_date': '2026-04-24', 'open_price': 57, 'high_price': 59, 'low_price': 56, 'close_price': 58, 'volume': 900},
            {'trade_date': '2026-04-27', 'open_price': 58, 'high_price': 60, 'low_price': 57, 'close_price': 59, 'volume': 900},
        ],
        '051910': [
            {'trade_date': '2026-04-20', 'open_price': 49, 'high_price': 50, 'low_price': 48, 'close_price': 49, 'volume': 900},
            {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 56, 'low_price': 49, 'close_price': 55, 'volume': 900},
            {'trade_date': '2026-04-22', 'open_price': 55, 'high_price': 57, 'low_price': 54, 'close_price': 56, 'volume': 900},
            {'trade_date': '2026-04-23', 'open_price': 56, 'high_price': 58, 'low_price': 55, 'close_price': 57, 'volume': 900},
            {'trade_date': '2026-04-24', 'open_price': 57, 'high_price': 59, 'low_price': 56, 'close_price': 58, 'volume': 900},
            {'trade_date': '2026-04-27', 'open_price': 58, 'high_price': 60, 'low_price': 57, 'close_price': 59, 'volume': 900},
        ],
        '068270': [
            {'trade_date': '2026-04-21', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 800},
            {'trade_date': '2026-04-22', 'open_price': 100, 'high_price': 104, 'low_price': 99, 'close_price': 103, 'volume': 800},
            {'trade_date': '2026-04-23', 'open_price': 103, 'high_price': 106, 'low_price': 102, 'close_price': 105, 'volume': 800},
            {'trade_date': '2026-04-24', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 800},
            {'trade_date': '2026-04-27', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 800},
            {'trade_date': '2026-04-28', 'open_price': 108, 'high_price': 111, 'low_price': 107, 'close_price': 110, 'volume': 800},
        ],
        '105560': [
            {'trade_date': '2026-04-21', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 800},
            {'trade_date': '2026-04-22', 'open_price': 100, 'high_price': 103, 'low_price': 99, 'close_price': 102, 'volume': 800},
            {'trade_date': '2026-04-23', 'open_price': 102, 'high_price': 105, 'low_price': 101, 'close_price': 104, 'volume': 800},
            {'trade_date': '2026-04-24', 'open_price': 104, 'high_price': 106, 'low_price': 103, 'close_price': None, 'volume': 800},
            {'trade_date': '2026-04-27', 'open_price': 106, 'high_price': 107, 'low_price': 105, 'close_price': 106, 'volume': 800},
            {'trade_date': '2026-04-28', 'open_price': 106, 'high_price': 108, 'low_price': 105, 'close_price': 107, 'volume': 800},
        ],
        '207940': [
            {'trade_date': '2026-04-29', 'open_price': 200, 'high_price': 201, 'low_price': 198, 'close_price': 200, 'volume': 700},
            {'trade_date': '2026-04-30', 'open_price': 201, 'high_price': 204, 'low_price': 199, 'close_price': 203, 'volume': 700},
        ],
    }
    repository = FakeSignalDeciderLoopRepository(rows)
    loop_service = SignalDeciderLoopService(repository=repository)
    calibration_service = SignalDeciderCalibrationService(loop_service=loop_service)
    return loop_service, calibration_service, bars_by_symbol


def _build_rule_candidate_calibration_fixture(
    *,
    outcome_kind: str,
    sample_count: int = 3,
):
    if outcome_kind not in {'bad_buy', 'missed_opportunity'}:
        raise ValueError('outcome_kind must be bad_buy or missed_opportunity')

    symbols = [f'90{index:04d}' for index in range(1, sample_count + 1)]
    rows = []
    bars_by_symbol = {}
    for index, symbol in enumerate(symbols, start=1):
        action = 'buy' if outcome_kind == 'bad_buy' else 'wait'
        row = _feedback_row(
            feedback_id=400 + index,
            snapshot_id=900 + index,
            market_date='2026-04-20',
            symbol=symbol,
            action=action,
            target_price=105 if outcome_kind == 'bad_buy' else 55,
            stop_loss_price=96 if outcome_kind == 'bad_buy' else 47,
        )
        _apply_calibration_context(
            row,
            confidence_score_10=8.4 if outcome_kind == 'bad_buy' else 5.2,
            signal_score=82.0,
            close_location_ratio=0.86 if outcome_kind == 'bad_buy' else 0.48,
            unified_value_score=0.72,
            momentum_score=0.88,
            quality_score=0.73,
            liquidity_percentile=0.84,
            passes_liquidity_floor=True,
        )
        rows.append(row)
        if outcome_kind == 'bad_buy':
            bars_by_symbol[symbol] = [
                {'trade_date': '2026-04-20', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 1000},
                {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 101, 'low_price': 95, 'close_price': 96, 'volume': 1000},
                {'trade_date': '2026-04-22', 'open_price': 96, 'high_price': 98, 'low_price': 94, 'close_price': 95, 'volume': 1000},
                {'trade_date': '2026-04-23', 'open_price': 95, 'high_price': 97, 'low_price': 93, 'close_price': 94, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 94, 'high_price': 96, 'low_price': 92, 'close_price': 93, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 93, 'high_price': 95, 'low_price': 91, 'close_price': 92, 'volume': 1000},
            ]
        else:
            bars_by_symbol[symbol] = [
                {'trade_date': '2026-04-20', 'open_price': 49, 'high_price': 50, 'low_price': 48, 'close_price': 49, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 56, 'low_price': 49, 'close_price': 55, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 55, 'high_price': 57, 'low_price': 54, 'close_price': 56, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 56, 'high_price': 58, 'low_price': 55, 'close_price': 57, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 57, 'high_price': 59, 'low_price': 56, 'close_price': 58, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 58, 'high_price': 60, 'low_price': 57, 'close_price': 59, 'volume': 900},
            ]

    repository = FakeSignalDeciderLoopRepository(rows)
    loop_service = SignalDeciderLoopService(repository=repository)
    calibration_service = SignalDeciderCalibrationService(loop_service=loop_service)
    return loop_service, calibration_service, bars_by_symbol


def _build_rule_candidate_report(
    *,
    outcome_kind: str,
    sample_count: int = 3,
    min_sample_size: int = 3,
) -> dict:
    loop_service, calibration_service, bars_by_symbol = _build_rule_candidate_calibration_fixture(
        outcome_kind=outcome_kind,
        sample_count=sample_count,
    )
    with patch(
        'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
        side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
    ):
        loop_service.compute_evaluations(
            market_date_from='2026-04-20',
            market_date_to='2026-04-20',
            horizons=[5],
            evaluation_version='signal_decider_eval_test',
        )
        return calibration_service.build_report(
            market_date_from='2026-04-20',
            market_date_to='2026-04-20',
            horizon_days=5,
            evaluation_version='signal_decider_eval_test',
            min_sample_size=min_sample_size,
            cluster_limit=10,
        )


class SignalDeciderLoopServiceTests(unittest.TestCase):
    def test_get_active_policy_context_fails_closed_when_registry_is_unavailable(self):
        repository = FakeSignalDeciderLoopRepository([])
        service = SignalDeciderLoopService(repository=repository)

        with patch.object(
            repository,
            'get_active_policy_version',
            side_effect=SQLAlchemyError('policy registry unavailable'),
        ):
            with self.assertRaisesRegex(RuntimeError, 'active policy registry is unavailable'):
                service.get_active_policy_context()

    def test_get_active_policy_context_bootstraps_empty_registry_and_requeries_it(self):
        repository = FakeSignalDeciderLoopRepository([])
        repository.policy_versions.clear()
        service = SignalDeciderLoopService(repository=repository)

        result = service.get_active_policy_context()

        persisted = repository.policy_versions[DEFAULT_SIGNAL_DECIDER_POLICY_VERSION]
        self.assertEqual(result['current_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(result['status'], 'active')
        self.assertIsNotNone(persisted['id'])
        self.assertEqual(persisted['status'], 'active')

    def test_get_active_policy_context_fails_closed_when_existing_registry_has_no_active_policy(self):
        repository = FakeSignalDeciderLoopRepository([])
        repository.policy_versions[DEFAULT_SIGNAL_DECIDER_POLICY_VERSION]['status'] = 'retired'
        service = SignalDeciderLoopService(repository=repository)

        with self.assertRaisesRegex(RuntimeError, 'no active policy exists'):
            service.get_active_policy_context()

    def test_get_active_policy_context_fails_closed_when_multiple_policies_are_active(self):
        repository = FakeSignalDeciderLoopRepository([])
        repository.policy_versions['signal_decider_policy_v2'] = {
            **_policy_version_record(
                policy_version='signal_decider_policy_v2',
                status='active',
                parent_policy_version=DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
                activated_by='human.reviewer',
                activated_at='2026-05-01T06:00:00+00:00',
            ),
            'id': 2,
        }
        service = SignalDeciderLoopService(repository=repository)

        with self.assertRaisesRegex(RuntimeError, 'multiple active policies exist'):
            service.get_active_policy_context()

    def test_policy_model_rejects_a_second_active_policy(self):
        engine = create_engine('sqlite+pysqlite:///:memory:')
        Base.metadata.create_all(engine, tables=[SignalDeciderPolicyVersion.__table__])
        TestSessionLocal = sessionmaker(bind=engine, future=True)

        try:
            with TestSessionLocal() as session:
                session.add(
                    SignalDeciderPolicyVersion(
                        policy_version='signal_decider_policy_v1',
                        status='active',
                        change_summary='Baseline policy.',
                        rule_bundle_payload={},
                        metadata_payload={},
                    )
                )
                session.commit()

            with TestSessionLocal() as session:
                session.add(
                    SignalDeciderPolicyVersion(
                        policy_version='signal_decider_policy_v2',
                        status='active',
                        change_summary='Conflicting active policy.',
                        rule_bundle_payload={},
                        metadata_payload={},
                    )
                )
                with self.assertRaises(IntegrityError):
                    session.commit()
        finally:
            Base.metadata.drop_all(engine, tables=[SignalDeciderPolicyVersion.__table__])

    def test_build_signal_decider_feature_snapshot_marks_owned_fallback_signal_rows(self):
        snapshot = build_signal_decider_feature_snapshot(
            symbol_day_context={
                'signal_features': {
                    'signal_count': 0,
                    'row_kind': 'fallback_only',
                    'provenance': {
                        'row_kind': 'fallback_only',
                        'fallback_groups_present': ['price_action', 'factor_scores'],
                    },
                }
            }
        )

        self.assertEqual(snapshot['signal_features']['signal_count'], 0)
        self.assertEqual(snapshot['signal_features']['row_kind'], 'fallback_only')
        self.assertTrue(snapshot['signal_features']['has_owned_fallback_features'])

    def test_prepare_feedback_persistence_adds_policy_and_feature_snapshot(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))

        summary, feedback, context = service.prepare_feedback_persistence(
            agent_name='signal-decider',
            summary_payload={'model': 'openclaw/signal-decider'},
            feedback_payload={
                'next_day_open_action': 'wait',
                'traceability': {'symbol': '005930', 'market_date': '2026-04-23'},
            },
            context_payload={
                'symbol_day_context': {
                    'technical_snapshot': {'breakout_20d': True},
                    'action_summary': {'signal_score': 78, 'confidence_tier': 'high'},
                    'signal_features': {'signal_count': 3},
                    'price_context': {'close_location_ratio': 0.8},
                },
                'scores': {
                    'unified_value': {
                        'status': 'available',
                        'score': 0.83,
                        'components_used': 5,
                        'components_possible': 5,
                        'coverage_notes': [],
                        'as_of_market_date': '2026-04-23',
                    }
                },
            },
        )

        self.assertEqual(summary['policy_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(feedback['policy_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(context['signal_decider_policy']['activation_mode'], 'manual_only')
        self.assertTrue(context['signal_decider_trace']['feature_snapshot']['technical']['breakout_20d'])
        self.assertEqual(
            context['signal_decider_trace']['feature_snapshot']['action_summary']['signal_score_bucket'],
            'high',
        )
        self.assertEqual(
            context['signal_decider_trace']['feature_snapshot']['scores']['unified_value']['score'],
            0.83,
        )
        self.assertTrue(context['signal_decider_trace']['feature_snapshot']['meta']['has_score_context'])
        self.assertIn('decision_delta', feedback)
        self.assertFalse(feedback['decision_delta']['has_previous_decision'])

    def test_prepare_snapshot_persistence_item_builds_structured_traceability(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        snapshot_item = service.prepare_snapshot_persistence_item(
            feedback_id=11,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
            agent_version='openclaw-v1',
            run_id='signal-run-11',
            summary_payload={'model': 'openclaw/signal-decider'},
            feedback_payload={
                'preferred_action': 'wait',
                'next_day_open_action': 'wait',
                'thesis_summary': '보수적으로 대기한다.',
                'supporting_evidence': ['장 마감 수급이 개선됐다.'],
                'decision_risks': ['주도주 폭이 좁다.'],
                'invalidation_conditions': ['지지선 이탈 종가가 반복되면 무효화한다.'],
                'traceability': {'symbol': '005930', 'market_date': '2026-04-23'},
            },
            context_payload={
                'symbol_day_context': {
                    'technical_snapshot': {'breakout_20d': True, 'trend_state': 'uptrend'},
                    'action_summary': {'signal_score': 78, 'confidence_tier': 'high'},
                    'signal_features': {'signal_count': 3},
                    'price_context': {'close_location_ratio': 0.8},
                },
                'scores': {
                    'quality': {
                        'status': 'partial',
                        'score': 0.74,
                        'components_used': 5,
                        'components_possible': 6,
                        'coverage_notes': ['Missing quality components: accruals_ratio.'],
                        'as_of_market_date': '2026-04-23',
                    }
                },
                'upstream_traceability': {
                    'technical-summarizer': {'feedback_id': 7, 'run_id': 'upstream-7', 'source': 'persisted_feedback'}
                },
                'orchestration': {'required_upstream_roles': ['technical-summarizer']},
                'market_context': {
                    'status': 'partial',
                    'market_date': '2026-04-23',
                    'summary': {'market_bias': 'mixed', 'line': 'KOSPI +0.11%; USD/KRW 1444.5.', 'breadth_summary': None},
                    'indices': {'kospi': {'level': 2642.07, 'change': 2.78, 'change_rate': 0.11}},
                    'fx': {'usdkrw': {'value': 1444.5, 'effective_date': '2026-04-23', 'fallback_applied': False}},
                    'flows': {},
                    'missing_fields': ['indices.kosdaq', 'flows.foreign', 'flows.institutional'],
                    'stale_reason': None,
                },
                'signal_envelopes': [
                    {
                        'schema_version': 'signal_envelope_v1',
                        'source_name': 'factor:momentum',
                        'source_type': 'factor',
                        'symbol': '005930',
                        'market_date': '2026-04-23',
                        'horizon': '5d',
                        'value': 0.82,
                        'confidence': 1.0,
                        'evidence_count': 3,
                        'risk_flags': [],
                        'evidence_refs': [],
                        'summary': 'momentum factor status=available score=0.9100',
                        'raw_ref': {'type': 'factor_summary', 'path': 'factor_summary.factor_scores.momentum'},
                    }
                ],
            },
            received_at='2026-04-23T06:00:00+00:00',
        )

        self.assertEqual(snapshot_item['policy_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(snapshot_item['traceability_payload']['schema_version'], 'signal_decider_trace_v2')
        self.assertEqual(
            snapshot_item['traceability_payload']['upstream_references']['technical-summarizer']['feedback_id'],
            7,
        )
        self.assertTrue(snapshot_item['traceability_payload']['gate_checks']['traceability_symbol_matches'])
        self.assertEqual(snapshot_item['input_feature_snapshot_payload']['technical']['breakout_20d'], True)
        self.assertEqual(snapshot_item['request_payload']['scores']['quality']['status'], 'partial')
        self.assertEqual(snapshot_item['request_payload']['market_context']['status'], 'partial')
        self.assertEqual(
            snapshot_item['request_payload']['signal_envelopes'][0]['schema_version'],
            'signal_envelope_v1',
        )
        self.assertEqual(snapshot_item['input_feature_snapshot_payload']['scores']['quality']['components_possible'], 6)
        self.assertIn('decision_delta', snapshot_item['response_payload'])
        self.assertEqual(
            snapshot_item['traceability_payload']['final_action_rationale']['decision_delta']['current_recommendation'],
            'wait',
        )

    def test_prepare_snapshot_persistence_item_supports_short_term_decider(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        snapshot_item = service.prepare_snapshot_persistence_item(
            feedback_id=12,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='short-term-decider',
            agent_version='openclaw-v1',
            run_id='short-run-12',
            summary_payload={'model': 'openclaw/short-term-decider'},
            feedback_payload={
                'tactical_action': 'watch_for_trigger',
                'preferred_action': 'watch_for_trigger',
                'trigger_price': 81200,
                'chase_limit_price': 82400,
                'target_price': 84800,
                'stop_loss_price': 79200,
                'expected_holding_period': '1-2 trading days',
                'confidence_score_10': 7.2,
                'conviction_score_10': 6.4,
                'thesis_summary': '돌파 확인 뒤 추격 범위를 제한한다.',
                'supporting_evidence': ['거래대금이 재유입되고 있다.'],
                'tactical_risks': ['개장 직후 변동성이 확대될 수 있다.'],
                'invalidation_conditions': ['트리거 재돌파 실패 시 무효화한다.'],
                'data_quality_warnings': [],
                'traceability': {
                    'agent_name': 'short-term-decider',
                    'symbol': '005930',
                    'market_date': '2026-04-23',
                },
            },
            context_payload={
                'decision_support_constraints': {
                    'decision_support_only': True,
                    'allow_execution_logic': False,
                },
                'orchestration': {'required_upstream_roles': ['technical-summarizer']},
                'upstream_traceability': {
                    'technical-summarizer': {'feedback_id': 8, 'run_id': 'upstream-8', 'source': 'persisted_feedback'}
                },
            },
            received_at='2026-04-23T06:00:00+00:00',
        )

        self.assertEqual(snapshot_item['agent_name'], 'short-term-decider')
        self.assertEqual(snapshot_item['policy_version'], 'short_term_decider_contract_v1')
        self.assertEqual(snapshot_item['decision_action'], 'buy')
        self.assertEqual(snapshot_item['entry_price_basis'], 'trigger_limit')
        self.assertEqual(snapshot_item['traceability_payload']['schema_version'], 'short_term_decider_trace_v1')
        self.assertEqual(snapshot_item['traceability_payload']['tactical_context']['trigger_price'], 81200)
        self.assertEqual(snapshot_item['traceability_payload']['upstream_references']['technical-summarizer']['feedback_id'], 8)

    def test_prepare_snapshot_persistence_item_filters_resolved_pre_decider_warnings(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        snapshot_item = service.prepare_snapshot_persistence_item(
            feedback_id=120,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='short-term-decider',
            agent_version='openclaw-v1',
            run_id='short-run-120',
            summary_payload={'model': 'openclaw/short-term-decider'},
            feedback_payload={
                'tactical_action': 'watch_for_trigger',
                'preferred_action': 'watch_for_trigger',
                'trigger_price': 81200,
                'chase_limit_price': 82400,
                'target_price': 84800,
                'stop_loss_price': 79200,
                'expected_holding_period': '1-2 trading days',
                'confidence_score_10': 7.2,
                'conviction_score_10': 6.4,
                'thesis_summary': '돌파 확인 뒤 추격 범위를 제한한다.',
                'supporting_evidence': ['거래대금이 재유입되고 있다.'],
                'tactical_risks': ['개장 직후 변동성이 확대될 수 있다.'],
                'invalidation_conditions': ['트리거 재돌파 실패 시 무효화한다.'],
                'data_quality_warnings': [
                    'Realtime orderbook freshness is stale.',
                    'Raw trading value is missing; using price x volume estimate.',
                    'Investor flow data is missing.',
                    'Short-selling data is missing.',
                    'Sector peer data is missing.',
                    'No recent news/disclosures were captured.',
                ],
            },
            context_payload={
                'pre_decider_enrichment_context': {
                    'schema_version': 'pre_decider_enrichment_v1',
                    'quote_orderbook_snapshot': {
                        'status': 'stale_eod',
                        'snapshot_type': 'eod_stale',
                        'reason': 'closed_market_eod_snapshot',
                    },
                    'trading_value': {
                        'status': 'available',
                        'source': 'raw',
                        'source_detail': 'pykrx_valuation_snapshot.trading_value',
                    },
                    'flow_short_sector_context': {
                        'investor_flow': {'status': 'available'},
                        'short_selling': {'status': 'available'},
                        'sector_peers': {'status': 'partial'},
                    },
                },
                'orchestration': {'required_upstream_roles': ['technical-summarizer']},
            },
            received_at='2026-04-23T06:00:00+00:00',
        )

        self.assertEqual(snapshot_item['data_quality_warnings_payload'], ['No recent news/disclosures were captured.'])
        self.assertEqual(snapshot_item['response_payload']['data_quality_warnings'], ['No recent news/disclosures were captured.'])
        resolved_warnings = snapshot_item['request_payload']['pre_decider_warning_resolution']['resolved_warnings']
        self.assertEqual(len(resolved_warnings), 5)
        self.assertEqual({item['category'] for item in resolved_warnings}, {
            'quote_orderbook_snapshot',
            'trading_value',
            'investor_flow',
            'short_selling',
            'sector_peer',
        })

    def test_prepare_snapshot_persistence_item_builds_wait_to_wait_decision_delta(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        snapshot_item = service.prepare_snapshot_persistence_item(
            feedback_id=21,
            market_date='2026-04-24',
            symbol='005930',
            agent_name='signal-decider',
            agent_version='openclaw-v1',
            run_id='signal-run-21',
            summary_payload={'model': 'openclaw/signal-decider'},
            feedback_payload={
                'preferred_action': 'wait',
                'next_day_open_action': 'wait',
                'wait_reason': {
                    'code': 'stale_quote_orderbook',
                    'summary': '호가 신선도가 아직 부족합니다.',
                },
                'thesis_summary': '전일 대비 시도는 유지되지만 호가 신선도 부족으로 대기합니다.',
                'supporting_evidence': ['장 마감 수급은 유지됐다.'],
                'decision_risks': ['실시간 호가 반영이 늦다.'],
                'invalidation_conditions': ['호가 공백이 해소되지 않으면 보수적으로 유지한다.'],
                'data_quality_warnings': ['Realtime orderbook freshness is stale.'],
                'traceability': {'symbol': '005930', 'market_date': '2026-04-24'},
            },
            context_payload={
                'previous_decision_context': {
                    'available': True,
                    'feedback_id': 20,
                    'market_date': '2026-04-23',
                    'received_at': '2026-04-23T06:00:00+00:00',
                    'symbol': '005930',
                    'recommendation': 'wait',
                    'recommendation_class': 'wait',
                    'wait_reason': {
                        'code': 'stale_quote_orderbook',
                        'summary': '호가 신선도가 부족합니다.',
                    },
                    'summary': '호가 신선도 부족으로 대기합니다.',
                    'confidence_score_10': 5.0,
                    'conviction_score_10': 4.0,
                    'supporting_evidence': ['장 마감 수급은 중립이었다.'],
                    'decision_risks': ['실시간 호가가 늦다.'],
                    'invalidation_conditions': ['호가 공백 지속'],
                    'data_quality_warnings': ['Realtime orderbook freshness is stale.'],
                    'upgrade_trigger': None,
                    'source': 'persisted_feedback',
                }
            },
            received_at='2026-04-24T06:00:00+00:00',
        )

        decision_delta = snapshot_item['response_payload']['decision_delta']
        self.assertTrue(decision_delta['has_previous_decision'])
        self.assertEqual(decision_delta['transition'], 'wait_to_wait')
        self.assertTrue(decision_delta['repeat_wait'])
        self.assertEqual(decision_delta['previous_market_date'], '2026-04-23')
        self.assertEqual(decision_delta['previous_wait_reason']['code'], 'stale_quote_orderbook')
        self.assertEqual(decision_delta['current_wait_reason']['code'], 'stale_quote_orderbook')
        self.assertEqual(decision_delta['persistence_trend'], 'persistent')
        self.assertIn('wait_reason', decision_delta['stable_fields'])
        self.assertEqual(decision_delta['upgrade_trigger']['target_action'], 'buy')
        self.assertEqual(
            snapshot_item['traceability_payload']['final_action_rationale']['decision_delta']['transition'],
            'wait_to_wait',
        )

    def test_prepare_snapshot_persistence_item_builds_wait_to_buy_decision_delta(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        snapshot_item = service.prepare_snapshot_persistence_item(
            feedback_id=22,
            market_date='2026-04-24',
            symbol='005930',
            agent_name='signal-decider',
            agent_version='openclaw-v1',
            run_id='signal-run-22',
            summary_payload={'model': 'openclaw/signal-decider'},
            feedback_payload={
                'preferred_action': 'buy',
                'next_day_open_action': 'buy',
                'entry_price_basis': 'next_open',
                'target_price': 84000,
                'expected_return_pct': 4.8,
                'stop_loss_price': 79800,
                'suggested_holding_period': '3-10 trading days',
                'confidence_score_10': 7.0,
                'conviction_score_10': 6.0,
                'thesis_summary': '호가 신선도가 회복돼 보수적 매수로 상향한다.',
                'supporting_evidence': ['실시간 호가와 수급이 회복됐다.'],
                'decision_risks': ['갭 시초가 변동성'],
                'invalidation_conditions': ['79800원 이탈 종가'],
                'data_quality_warnings': [],
                'traceability': {'symbol': '005930', 'market_date': '2026-04-24'},
            },
            context_payload={
                'previous_decision_context': {
                    'available': True,
                    'feedback_id': 20,
                    'market_date': '2026-04-23',
                    'received_at': '2026-04-23T06:00:00+00:00',
                    'symbol': '005930',
                    'recommendation': 'wait',
                    'recommendation_class': 'wait',
                    'wait_reason': {
                        'code': 'stale_quote_orderbook',
                        'summary': '호가 신선도가 부족합니다.',
                    },
                    'summary': '호가 신선도 부족으로 대기합니다.',
                    'confidence_score_10': 5.0,
                    'conviction_score_10': 4.0,
                    'supporting_evidence': ['장 마감 수급은 중립이었다.'],
                    'decision_risks': ['실시간 호가가 늦다.'],
                    'invalidation_conditions': ['호가 공백 지속'],
                    'data_quality_warnings': ['Realtime orderbook freshness is stale.'],
                    'upgrade_trigger': None,
                    'source': 'persisted_feedback',
                }
            },
            received_at='2026-04-24T06:00:00+00:00',
        )

        decision_delta = snapshot_item['response_payload']['decision_delta']
        self.assertEqual(decision_delta['transition'], 'wait_to_buy')
        self.assertEqual(decision_delta['persistence_trend'], 'improving')
        self.assertEqual(decision_delta['previous_recommendation'], 'wait')
        self.assertEqual(decision_delta['current_recommendation'], 'buy')
        self.assertIsNone(decision_delta['upgrade_trigger'])

    def test_prepare_snapshot_persistence_item_builds_watch_to_wait_decision_delta_for_short_term(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        snapshot_item = service.prepare_snapshot_persistence_item(
            feedback_id=23,
            market_date='2026-04-24',
            symbol='005930',
            agent_name='short-term-decider',
            agent_version='openclaw-v1',
            run_id='short-run-23',
            summary_payload={'model': 'openclaw/short-term-decider'},
            feedback_payload={
                'tactical_action': 'wait',
                'preferred_action': 'wait',
                'target_price': 84800,
                'stop_loss_price': 79200,
                'expected_holding_period': '1-2 trading days',
                'confidence_score_10': 5.5,
                'conviction_score_10': 4.5,
                'wait_reason': {
                    'code': 'unsuitable_execution_timing',
                    'summary': '돌파 재시도가 약해져 대기로 낮춘다.',
                },
                'thesis_summary': '추격 트리거가 약화돼 일단 대기한다.',
                'supporting_evidence': ['체결 강도가 둔화됐다.'],
                'tactical_risks': ['개장 직후 변동성'],
                'invalidation_conditions': ['장 초반 추세 이탈'],
                'data_quality_warnings': [],
                'traceability': {
                    'agent_name': 'short-term-decider',
                    'symbol': '005930',
                    'market_date': '2026-04-24',
                },
            },
            context_payload={
                'previous_decision_context': {
                    'available': True,
                    'feedback_id': 22,
                    'market_date': '2026-04-23',
                    'received_at': '2026-04-23T06:00:00+00:00',
                    'symbol': '005930',
                    'recommendation': 'watch_for_trigger',
                    'recommendation_class': 'watch',
                    'wait_reason': {
                        'code': 'unsuitable_execution_timing',
                        'summary': '돌파 확인이 더 필요합니다.',
                    },
                    'summary': '돌파 확인 뒤 추격 범위를 제한한다.',
                    'confidence_score_10': 7.2,
                    'conviction_score_10': 6.4,
                    'target_price': 84800,
                    'stop_loss_price': 79200,
                    'trigger_price': 81200,
                    'chase_limit_price': 82400,
                    'supporting_evidence': ['거래대금이 재유입되고 있다.'],
                    'decision_risks': ['개장 직후 변동성이 확대될 수 있다.'],
                    'invalidation_conditions': ['트리거 재돌파 실패 시 무효화한다.'],
                    'data_quality_warnings': [],
                    'trigger_conditions': ['81200원 돌파와 거래량 동반'],
                    'upgrade_trigger': {
                        'target_action': 'watch_for_trigger',
                        'summary': '81200원 돌파와 거래량 동반',
                        'conditions': ['81200원 돌파와 거래량 동반'],
                        'trigger_price': 81200,
                        'chase_limit_price': 82400,
                    },
                    'source': 'persisted_feedback',
                }
            },
            received_at='2026-04-24T06:00:00+00:00',
        )

        decision_delta = snapshot_item['response_payload']['decision_delta']
        self.assertEqual(decision_delta['transition'], 'watch_to_wait')
        self.assertEqual(decision_delta['persistence_trend'], 'worsening')
        self.assertEqual(decision_delta['previous_recommendation'], 'watch_for_trigger')
        self.assertEqual(decision_delta['current_recommendation'], 'wait')
        self.assertEqual(decision_delta['upgrade_trigger']['target_action'], 'watch_for_trigger')

    def test_prepare_snapshot_persistence_item_infers_specific_wait_reason_from_core_blocker(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        snapshot_item = service.prepare_snapshot_persistence_item(
            feedback_id=13,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
            agent_version='openclaw-v1',
            run_id='signal-run-13',
            summary_payload={'model': 'openclaw/signal-decider'},
            feedback_payload={
                'preferred_action': 'wait',
                'next_day_open_action': 'wait',
                'thesis_summary': 'Need more data before entry.',
                'supporting_evidence': ['Momentum is still constructive.'],
                'decision_risks': ['Bid/ask spread is stale into the open.'],
                'data_quality_warnings': [
                    'No recent news/disclosures were captured.',
                    'Realtime orderbook freshness is stale.',
                ],
                'traceability': {'symbol': '005930', 'market_date': '2026-04-23'},
            },
            context_payload={'symbol_day_context': {}, 'scores': {}},
            received_at='2026-04-23T06:00:00+00:00',
        )

        self.assertEqual(snapshot_item['response_payload']['wait_reason']['code'], 'stale_quote_orderbook')
        self.assertEqual(
            snapshot_item['traceability_payload']['final_action_rationale']['wait_reason']['code'],
            'stale_quote_orderbook',
        )

    def test_prepare_snapshot_persistence_item_uses_actual_blocker_when_liquidity_support_is_positive(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        snapshot_item = service.prepare_snapshot_persistence_item(
            feedback_id=14,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
            agent_version='openclaw-v1',
            run_id='signal-run-14',
            summary_payload={'model': 'openclaw/signal-decider'},
            feedback_payload={
                'preferred_action': 'wait',
                'next_day_open_action': 'wait',
                'thesis_summary': 'Need more confirmation before entry.',
                'supporting_evidence': ['Liquidity is strong enough for execution.'],
                'decision_risks': ['Weak trend remains the main blocker.'],
                'traceability': {'symbol': '005930', 'market_date': '2026-04-23'},
            },
            context_payload={'symbol_day_context': {}, 'scores': {}},
            received_at='2026-04-23T06:00:00+00:00',
        )

        self.assertEqual(snapshot_item['response_payload']['wait_reason']['code'], 'weak_trend_relative_strength')
        self.assertEqual(
            snapshot_item['traceability_payload']['final_action_rationale']['wait_reason']['code'],
            'weak_trend_relative_strength',
        )

    def test_list_decisions_preserves_multiple_snapshots_for_one_symbol_date(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=41,
                    snapshot_id=101,
                    market_date='2026-04-23',
                    symbol='005930',
                    action='wait',
                ),
                _feedback_row(
                    feedback_id=41,
                    snapshot_id=102,
                    market_date='2026-04-23',
                    symbol='005930',
                    action='buy',
                    policy_version='signal_decider_policy_v2',
                ),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        result = service.list_decisions(market_date='2026-04-23', symbol='005930', limit=20)

        self.assertEqual(result['summary']['count'], 2)
        self.assertEqual([item['snapshot_id'] for item in result['items']], [102, 101])
        self.assertEqual([item['decision']['action'] for item in result['items']], ['buy', 'wait'])
        self.assertEqual(
            {item['policy_version'] for item in result['items']},
            {'signal_decider_policy_v1', 'signal_decider_policy_v2'},
        )

    def test_list_decisions_materializes_snapshot_rows_before_session_close(self):
        engine = create_engine('sqlite+pysqlite:///:memory:')
        event.listen(engine, 'connect', _enable_sqlite_foreign_keys)
        Base.metadata.create_all(
            engine,
            tables=[
                AgentContextFeedback.__table__,
                SignalDeciderDecisionSnapshot.__table__,
                SignalDeciderEvaluation.__table__,
            ],
        )
        TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

        @contextmanager
        def test_session_scope():
            session = TestSessionLocal()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            with TestSessionLocal() as session:
                snapshot = SignalDeciderDecisionSnapshot(
                    feedback_id=None,
                    market_date=date(2026, 4, 29),
                    symbol='005930',
                    agent_name='signal-decider',
                    agent_version='openclaw-v1',
                    decision_run_id='signal-run-274',
                    feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                    policy_version='signal_decider_policy_v1',
                    decision_action='buy',
                    preferred_action='buy',
                    next_day_open_action='buy',
                    entry_price_basis='next_open',
                    target_price=105000,
                    expected_return_pct=4.0,
                    stop_loss_price=99000,
                    suggested_holding_period='3-10 trading days',
                    confidence_score_10=7.0,
                    conviction_score_10=6.0,
                    thesis_summary='Breakout continuation with manageable risk.',
                    supporting_evidence_payload=['Breadth improved.'],
                    decision_risks_payload=['Leadership remained narrow.'],
                    invalidation_conditions_payload=['Close back below breakout level.'],
                    data_quality_warnings_payload=[],
                    specialist_disagreement_payload=[],
                    traceability_payload={'agent_name': 'signal-decider'},
                    input_feature_snapshot_payload={'technical': {'breakout_20d': True}},
                    request_payload={'symbol_day_context': {}},
                    response_payload={'preferred_action': 'buy', 'next_day_open_action': 'buy'},
                    response_summary_payload={'summary': 'buy setup'},
                )
                session.add(snapshot)
                session.commit()
                snapshot_id = snapshot.id

            with patch('app.agent_context.signal_decider_loop_service.session_scope', test_session_scope):
                service = SignalDeciderLoopService(repository=SignalDeciderLoopRepository())
                result = service.list_decisions(market_date='2026-04-29', limit=1)

            self.assertEqual(result['summary']['count'], 1)
            self.assertEqual(result['summary']['evaluated_count'], 0)
            self.assertEqual(result['summary']['verdict_counts'], {})
            self.assertEqual(result['items'][0]['snapshot_id'], snapshot_id)
            self.assertEqual(result['items'][0]['market_date'], '2026-04-29')
            self.assertEqual(
                result['items'][0]['evaluation_summary'],
                {
                    'evaluation_version': DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
                    'evaluated': False,
                    'stored_horizon_count': 0,
                    'graded_horizon_count': 0,
                    'pending_horizon_count': 0,
                    'status_reason_counts': {},
                    'has_missed_opportunity': False,
                    'verdict_counts': {},
                    'latest_graded': None,
                    'horizons': [],
                },
            )
            self.assertIsNone(result['items'][0]['explanation']['latest_evaluation'])
        finally:
            Base.metadata.drop_all(
                engine,
                tables=[
                    SignalDeciderEvaluation.__table__,
                    SignalDeciderDecisionSnapshot.__table__,
                    AgentContextFeedback.__table__,
                ],
            )
            engine.dispose()

    def test_activate_policy_flushes_higher_pk_active_before_lower_pk_candidate(self):
        engine = create_engine('sqlite+pysqlite:///:memory:')
        tables = [
            SignalDeciderPolicyVersion.__table__,
            SignalDeciderPolicyCandidate.__table__,
        ]
        Base.metadata.create_all(engine, tables=tables)
        TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

        @contextmanager
        def test_session_scope():
            session = TestSessionLocal()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            with TestSessionLocal() as session:
                session.add_all(
                    [
                        SignalDeciderPolicyVersion(
                            id=1,
                            policy_version='signal_decider_policy_v2',
                            status='candidate',
                            parent_policy_version=DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
                            source_candidate_version='signal_decider_policy_v2',
                            change_summary='Lower-PK candidate.',
                            rule_bundle_payload={},
                            metadata_payload={},
                            created_by='system:test',
                        ),
                        SignalDeciderPolicyVersion(
                            id=2,
                            policy_version=DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
                            status='active',
                            change_summary='Higher-PK active policy.',
                            rule_bundle_payload={},
                            metadata_payload={},
                            created_by='system:test',
                            activated_by='system:test',
                            activated_at=datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
                        ),
                    ]
                )
                session.commit()

            with patch('app.agent_context.signal_decider_loop_service.session_scope', test_session_scope):
                activated = SignalDeciderLoopRepository().activate_policy_version(
                    policy_version='signal_decider_policy_v2',
                    activated_by='human.reviewer',
                    activation_note=None,
                    promotion_evidence={'replay_artifact_sha256': 'a' * 64},
                    activated_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(activated['status'], 'active')
            with TestSessionLocal() as session:
                statuses = {
                    row.policy_version: row.status
                    for row in session.query(SignalDeciderPolicyVersion).all()
                }
            self.assertEqual(
                statuses,
                {
                    'signal_decider_policy_v2': 'active',
                    DEFAULT_SIGNAL_DECIDER_POLICY_VERSION: 'retired',
                },
            )
        finally:
            Base.metadata.drop_all(engine, tables=reversed(tables))
            engine.dispose()

    def test_insert_decision_snapshots_preserves_history_and_idempotent_feedback_retries(self):
        engine = create_engine('sqlite+pysqlite:///:memory:')
        event.listen(engine, 'connect', _enable_sqlite_foreign_keys)
        Base.metadata.create_all(
            engine,
            tables=[
                AgentContextFeedback.__table__,
                SignalDeciderDecisionSnapshot.__table__,
                SignalDeciderRealizedOutcome.__table__,
            ],
        )
        TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        repository = SignalDeciderLoopRepository()

        @contextmanager
        def test_session_scope():
            session = TestSessionLocal()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            with TestSessionLocal() as session:
                feedback = AgentContextFeedback(
                    market_date=date(2026, 4, 23),
                    symbol='005930',
                    agent_name='signal-decider',
                    agent_version='openclaw-v1',
                    run_id='signal-run-feedback-1',
                    summary_payload={'summary': 'wait setup'},
                    feedback_payload={'preferred_action': 'wait'},
                    normalized_feedback_payload={'preferred_action': 'wait'},
                    context_payload={'signal_decider_trace': {'policy_version': DEFAULT_SIGNAL_DECIDER_POLICY_VERSION}},
                    received_at=datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
                )
                session.add(feedback)
                session.commit()
                feedback_id = feedback.id

            base_row = _feedback_row(
                feedback_id=feedback_id,
                snapshot_id=101,
                market_date='2026-04-23',
                symbol='005930',
                action='wait',
            )
            snapshot_preparer = SignalDeciderLoopService(
                repository=FakeSignalDeciderLoopRepository([])
            )
            first_item = snapshot_preparer.prepare_snapshot_persistence_item(
                feedback_id=feedback_id,
                market_date=base_row.market_date,
                symbol=base_row.symbol,
                agent_name=base_row.agent_name,
                agent_version=base_row.agent_version,
                run_id=base_row.decision_run_id,
                summary_payload=base_row.response_summary_payload,
                feedback_payload=base_row.response_payload,
                normalized_feedback_payload=base_row.response_payload,
                context_payload=base_row.request_payload,
                received_at=datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
            )
            self.assertIsNotNone(first_item)
            first_item['created_at'] = datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc)
            first_item['updated_at'] = first_item['created_at']

            with TestSessionLocal() as session:
                first_rows = repository.insert_decision_snapshots(session=session, items=[first_item])
                first_snapshot_id = first_rows[0].id
                session.add(
                    SignalDeciderRealizedOutcome(
                        decision_snapshot_id=first_snapshot_id,
                        feedback_id=feedback_id,
                        market_date=date(2026, 4, 23),
                        symbol='005930',
                        policy_version=DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
                        decision_action='wait',
                        horizon_days=5,
                        outcome_status='complete',
                        bars_observed=5,
                    )
                )
                session.commit()

            retry_item = snapshot_preparer.prepare_snapshot_persistence_item(
                feedback_id=feedback_id,
                market_date=base_row.market_date,
                symbol=base_row.symbol,
                agent_name=base_row.agent_name,
                agent_version=base_row.agent_version,
                run_id=base_row.decision_run_id,
                summary_payload=base_row.response_summary_payload,
                feedback_payload=base_row.response_payload,
                normalized_feedback_payload=base_row.response_payload,
                context_payload=base_row.request_payload,
                received_at=datetime(2026, 4, 23, 6, 30, tzinfo=timezone.utc),
            )
            self.assertIsNotNone(retry_item)
            retry_item['created_at'] = datetime(2026, 4, 23, 6, 30, tzinfo=timezone.utc)
            retry_item['updated_at'] = retry_item['created_at']
            with TestSessionLocal() as session:
                retry_rows = repository.insert_decision_snapshots(session=session, items=[retry_item])
                retry_snapshot_id = retry_rows[0].id
                session.commit()

            second_item = dict(first_item)
            second_item.update(
                {
                    'decision_run_id': 'signal-run-101-replayed',
                    'decision_action': 'buy',
                    'preferred_action': 'buy',
                    'next_day_open_action': 'buy',
                    'thesis_summary': 'buy decision for 005930 after replay',
                    'created_at': datetime(2026, 4, 23, 7, 0, tzinfo=timezone.utc),
                    'updated_at': datetime(2026, 4, 23, 7, 0, tzinfo=timezone.utc),
                }
            )

            with TestSessionLocal() as session:
                second_rows = repository.insert_decision_snapshots(session=session, items=[second_item])
                second_snapshot_id = second_rows[0].id
                session.commit()

            with TestSessionLocal() as session:
                stored_rows = session.query(SignalDeciderDecisionSnapshot).order_by(
                    SignalDeciderDecisionSnapshot.id.asc()
                ).all()
                stored_outcome = session.query(SignalDeciderRealizedOutcome).one()

            with patch('app.agent_context.signal_decider_loop_service.session_scope', test_session_scope):
                latest_rows = repository.list_decision_snapshots(
                    market_date=date(2026, 4, 23),
                    dedupe_feedback_results=True,
                )

            self.assertEqual(first_snapshot_id, retry_snapshot_id)
            self.assertNotEqual(first_snapshot_id, second_snapshot_id)
            self.assertEqual(len(stored_rows), 2)
            self.assertEqual(stored_rows[0].decision_action, 'wait')
            self.assertEqual(stored_rows[0].decision_run_id, 'signal-run-101')
            self.assertEqual(stored_rows[1].decision_run_id, 'signal-run-101-replayed')
            self.assertEqual(stored_rows[1].decision_action, 'buy')
            self.assertEqual(stored_rows[1].thesis_summary, 'buy decision for 005930 after replay')
            self.assertEqual(stored_outcome.decision_snapshot_id, first_snapshot_id)
            self.assertEqual([row.id for row in latest_rows], [second_snapshot_id])
        finally:
            Base.metadata.drop_all(
                engine,
                tables=[
                    SignalDeciderRealizedOutcome.__table__,
                    SignalDeciderDecisionSnapshot.__table__,
                    AgentContextFeedback.__table__,
                ],
            )
            engine.dispose()

    def test_insert_decision_snapshots_dedupes_exact_null_feedback_retry_only(self):
        engine = create_engine('sqlite+pysqlite:///:memory:')
        event.listen(engine, 'connect', _enable_sqlite_foreign_keys)
        Base.metadata.create_all(
            engine,
            tables=[
                AgentContextFeedback.__table__,
                SignalDeciderDecisionSnapshot.__table__,
            ],
        )
        TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        repository = SignalDeciderLoopRepository()

        try:
            first_row = _feedback_row(
                feedback_id=None,
                snapshot_id=201,
                market_date='2026-04-23',
                symbol='005930',
                action='wait',
            )
            second_row = _feedback_row(
                feedback_id=None,
                snapshot_id=202,
                market_date='2026-04-23',
                symbol='005930',
                action='buy',
            )
            first_item = {key: value for key, value in vars(first_row).items() if key != 'id'}
            first_item['updated_at'] = first_item['created_at']
            second_item = {key: value for key, value in vars(second_row).items() if key != 'id'}
            second_item['updated_at'] = second_item['created_at']

            with TestSessionLocal() as session:
                inserted_rows = repository.insert_decision_snapshots(
                    session=session,
                    items=[first_item, second_item],
                )
                inserted_ids = [row.id for row in inserted_rows]
                session.commit()

            retry_item = dict(first_item)
            retry_item['created_at'] = datetime(2026, 4, 23, 7, 0, tzinfo=timezone.utc)
            retry_item['updated_at'] = retry_item['created_at']
            with TestSessionLocal() as session:
                retry_rows = repository.insert_decision_snapshots(
                    session=session,
                    items=[retry_item],
                )
                retry_id = retry_rows[0].id
                session.commit()

            with TestSessionLocal() as session:
                stored_rows = session.query(SignalDeciderDecisionSnapshot).order_by(
                    SignalDeciderDecisionSnapshot.id.asc()
                ).all()

            self.assertEqual(len(inserted_rows), 2)
            self.assertEqual(retry_id, inserted_ids[0])
            self.assertEqual(len(stored_rows), 2)
            self.assertNotEqual(stored_rows[0].id, stored_rows[1].id)
            self.assertEqual([row.decision_action for row in stored_rows], ['wait', 'buy'])
        finally:
            Base.metadata.drop_all(
                engine,
                tables=[
                    SignalDeciderDecisionSnapshot.__table__,
                    AgentContextFeedback.__table__,
                ],
            )
            engine.dispose()

    def test_dedupe_decision_snapshot_rows_prefers_latest_feedback_and_symbol_fallback(self):
        duplicate_feedback_old = _feedback_row(
            feedback_id=41,
            snapshot_id=101,
            market_date='2026-05-08',
            symbol='005930',
            action='wait',
        )
        duplicate_feedback_new = _feedback_row(
            feedback_id=41,
            snapshot_id=102,
            market_date='2026-05-08',
            symbol='005930',
            action='buy',
            policy_version='signal_decider_policy_v2',
        )
        null_feedback_old = _feedback_row(
            feedback_id=None,
            snapshot_id=201,
            market_date='2026-05-08',
            symbol='000660',
            action='wait',
        )
        null_feedback_new = _feedback_row(
            feedback_id=None,
            snapshot_id=202,
            market_date='2026-05-08',
            symbol='000660',
            action='buy',
        )
        distinct_row = _feedback_row(
            feedback_id=55,
            snapshot_id=301,
            market_date='2026-05-08',
            symbol='035420',
            action='reject',
        )

        deduped_rows = _dedupe_decision_snapshot_rows(
            [
                duplicate_feedback_new,
                duplicate_feedback_old,
                null_feedback_new,
                null_feedback_old,
                distinct_row,
            ]
        )

        self.assertEqual([row.id for row in deduped_rows], [102, 202, 301])

    def test_get_review_date_counts_distinct_feedback_snapshots(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=41,
                    snapshot_id=101,
                    market_date='2026-05-08',
                    symbol='005930',
                    action='wait',
                ),
                _feedback_row(
                    feedback_id=41,
                    snapshot_id=102,
                    market_date='2026-05-08',
                    symbol='005930',
                    action='buy',
                ),
                _feedback_row(
                    feedback_id=55,
                    snapshot_id=103,
                    market_date='2026-05-08',
                    symbol='000660',
                    action='reject',
                ),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)
        service._build_review_items = lambda **kwargs: [  # type: ignore[method-assign]
            {
                'snapshot_id': row.id,
                'market_date': row.market_date.isoformat(),
                'symbol': row.symbol,
                'decision': {'action': row.decision_action},
                'review': {'status': 'pending', 'insight': {}, 'path_analysis': {}},
            }
            for row in kwargs['rows']
        ]
        service._build_review_items_summary = lambda **kwargs: {  # type: ignore[method-assign]
            'count': len(kwargs['items']),
            'symbol_count': len({item['symbol'] for item in kwargs['items']}),
            'decision_actions': {},
            'review_status_counts': {},
            'evaluation_coverage': {},
        }

        result = service.get_review_date(market_date='2026-05-08')

        self.assertEqual(result['summary']['count'], 2)
        self.assertTrue(repository.list_decision_snapshots_calls[0]['dedupe_feedback_results'])

    def test_list_decisions_filters_by_verdict_and_policy_version(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-23',
                    symbol='005930',
                    action='wait',
                    policy_version='signal_decider_policy_v1',
                ),
                _feedback_row(
                    feedback_id=2,
                    snapshot_id=102,
                    market_date='2026-04-23',
                    symbol='000660',
                    action='buy',
                    policy_version='signal_decider_policy_v1',
                ),
                _feedback_row(
                    feedback_id=3,
                    snapshot_id=103,
                    market_date='2026-04-22',
                    symbol='035420',
                    action='wait',
                    policy_version='signal_decider_policy_v2',
                ),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            '005930': [
                {'trade_date': '2026-04-23', 'open_price': 100, 'high_price': 101, 'low_price': 99, 'close_price': 100, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 100, 'high_price': 103, 'low_price': 99, 'close_price': 102, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 102, 'high_price': 106, 'low_price': 101, 'close_price': 105, 'volume': 1000},
                {'trade_date': '2026-04-28', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
                {'trade_date': '2026-04-29', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
                {'trade_date': '2026-04-30', 'open_price': 108, 'high_price': 111, 'low_price': 107, 'close_price': 110, 'volume': 1000},
            ],
            '000660': [
                {'trade_date': '2026-04-23', 'open_price': 100, 'high_price': 101, 'low_price': 99, 'close_price': 100, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 100, 'high_price': 101, 'low_price': 97, 'close_price': 98, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 98, 'high_price': 99, 'low_price': 94, 'close_price': 95, 'volume': 1000},
                {'trade_date': '2026-04-28', 'open_price': 95, 'high_price': 96, 'low_price': 93, 'close_price': 94, 'volume': 1000},
                {'trade_date': '2026-04-29', 'open_price': 94, 'high_price': 95, 'low_price': 92, 'close_price': 93, 'volume': 1000},
                {'trade_date': '2026-04-30', 'open_price': 93, 'high_price': 94, 'low_price': 91, 'close_price': 92, 'volume': 1000},
            ],
            '035420': [
                {'trade_date': '2026-04-22', 'open_price': 80, 'high_price': 81, 'low_price': 79, 'close_price': 80, 'volume': 1000},
                {'trade_date': '2026-04-23', 'open_price': 80, 'high_price': 83, 'low_price': 79, 'close_price': 82, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 82, 'high_price': 86, 'low_price': 81, 'close_price': 85, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 85, 'high_price': 87, 'low_price': 84, 'close_price': 86, 'volume': 1000},
                {'trade_date': '2026-04-28', 'open_price': 86, 'high_price': 89, 'low_price': 85, 'close_price': 88, 'volume': 1000},
                {'trade_date': '2026-04-29', 'open_price': 88, 'high_price': 91, 'low_price': 87, 'close_price': 90, 'volume': 1000},
            ],
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            service.compute_evaluations(
                market_date_from='2026-04-22',
                market_date_to='2026-04-23',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.list_decisions(
            market_date_from='2026-04-22',
            market_date_to='2026-04-23',
            action='wait',
            policy_version='signal_decider_policy_v1',
            verdict='missed_opportunity_after_wait',
            has_missed_opportunity=True,
            horizons=[5],
            evaluation_version='signal_decider_eval_test',
            limit=20,
        )

        self.assertEqual(result['summary']['count'], 1)
        self.assertEqual(result['items'][0]['snapshot_id'], 101)
        self.assertEqual(result['items'][0]['evaluation_summary']['latest_graded']['grade_label'], 'missed_opportunity_after_wait')
        self.assertTrue(result['items'][0]['evaluation_summary']['has_missed_opportunity'])
        self.assertEqual(result['request']['policy_version'], 'signal_decider_policy_v1')
        self.assertEqual(result['summary']['verdict_counts'], {'missed_opportunity_after_wait': 1})

    def test_list_decisions_applies_limit_before_loading_evaluations(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(feedback_id=1, snapshot_id=101, market_date='2026-04-21', symbol='005930', action='wait'),
                _feedback_row(feedback_id=2, snapshot_id=102, market_date='2026-04-22', symbol='000660', action='buy'),
                _feedback_row(feedback_id=3, snapshot_id=103, market_date='2026-04-23', symbol='035420', action='reject'),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        result = service.list_decisions(
            market_date_from='2026-04-21',
            market_date_to='2026-04-23',
            limit=1,
        )

        self.assertEqual(result['summary']['count'], 1)
        self.assertEqual([item['snapshot_id'] for item in result['items']], [103])
        self.assertEqual(len(repository.list_decision_snapshots_calls), 1)
        self.assertEqual(repository.list_decision_snapshots_calls[0]['limit'], 1)
        self.assertEqual(repository.list_decision_snapshots_calls[0]['offset'], 0)
        self.assertEqual(len(repository.list_evaluations_calls), 1)
        self.assertEqual(repository.list_evaluations_calls[0]['decision_snapshot_ids'], [103])

    def test_list_decisions_pages_snapshot_batches_when_filtered_matches_are_later(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-23',
                    symbol='005930',
                    action='wait',
                    policy_version='signal_decider_policy_v1',
                ),
                _feedback_row(
                    feedback_id=2,
                    snapshot_id=102,
                    market_date='2026-04-23',
                    symbol='000660',
                    action='buy',
                    policy_version='signal_decider_policy_v1',
                ),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            '005930': [
                {'trade_date': '2026-04-23', 'open_price': 100, 'high_price': 101, 'low_price': 99, 'close_price': 100, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 100, 'high_price': 103, 'low_price': 99, 'close_price': 102, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 102, 'high_price': 106, 'low_price': 101, 'close_price': 105, 'volume': 1000},
                {'trade_date': '2026-04-28', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
                {'trade_date': '2026-04-29', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
                {'trade_date': '2026-04-30', 'open_price': 108, 'high_price': 111, 'low_price': 107, 'close_price': 110, 'volume': 1000},
            ],
            '000660': [
                {'trade_date': '2026-04-23', 'open_price': 100, 'high_price': 101, 'low_price': 99, 'close_price': 100, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 100, 'high_price': 101, 'low_price': 97, 'close_price': 98, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 98, 'high_price': 99, 'low_price': 94, 'close_price': 95, 'volume': 1000},
                {'trade_date': '2026-04-28', 'open_price': 95, 'high_price': 96, 'low_price': 93, 'close_price': 94, 'volume': 1000},
                {'trade_date': '2026-04-29', 'open_price': 94, 'high_price': 95, 'low_price': 92, 'close_price': 93, 'volume': 1000},
                {'trade_date': '2026-04-30', 'open_price': 93, 'high_price': 94, 'low_price': 91, 'close_price': 92, 'volume': 1000},
            ],
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            service.compute_evaluations(
                market_date_from='2026-04-23',
                market_date_to='2026-04-23',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        repository.reset_calls()

        result = service.list_decisions(
            market_date='2026-04-23',
            verdict='missed_opportunity_after_wait',
            has_missed_opportunity=True,
            horizons=[5],
            evaluation_version='signal_decider_eval_test',
            limit=1,
        )

        self.assertEqual(result['summary']['count'], 1)
        self.assertEqual(result['items'][0]['snapshot_id'], 101)
        self.assertEqual(
            [call['offset'] for call in repository.list_decision_snapshots_calls],
            [0, 1],
        )
        self.assertEqual(
            [call['decision_snapshot_ids'] for call in repository.list_evaluations_calls],
            [[102], [101]],
        )

    def test_get_decision_detail_returns_explanation_and_evaluations(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=41,
                    snapshot_id=101,
                    market_date='2026-04-23',
                    symbol='005930',
                    action='buy',
                    market_context={
                        'status': 'ready',
                        'market_date': '2026-04-23',
                        'summary': {
                            'market_bias': 'risk_on',
                            'line': 'KOSPI +0.45%; KOSDAQ +0.54%; USD/KRW 1444.5.',
                            'breadth_summary': 'KOSPI breadth 702/81/198; KOSDAQ breadth 901/92/501',
                        },
                        'indices': {
                            'kospi': {'level': 2642.07, 'change': 11.78, 'change_rate': 0.45},
                            'kosdaq': {'level': 857.6, 'change': 4.61, 'change_rate': 0.54},
                        },
                        'fx': {'usdkrw': {'value': 1444.5, 'effective_date': '2026-04-23', 'fallback_applied': False}},
                        'flows': {
                            'foreign': {'value': 746146, 'unit': 'KIS_NET_VALUE_RAW'},
                            'institutional': {'value': 206217, 'unit': 'KIS_NET_VALUE_RAW'},
                        },
                        'missing_fields': [],
                        'stale_reason': None,
                    },
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars = [
            {'trade_date': '2026-04-23', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 100, 'high_price': 104, 'low_price': 99, 'close_price': 103, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 103, 'high_price': 106, 'low_price': 102, 'close_price': 105, 'volume': 1000},
            {'trade_date': '2026-04-28', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
            {'trade_date': '2026-04-29', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
            {'trade_date': '2026-04-30', 'open_price': 108, 'high_price': 112, 'low_price': 107, 'close_price': 111, 'volume': 1000},
        ]

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            return_value=bars,
        ):
            service.compute_evaluations(
                market_date_from='2026-04-23',
                market_date_to='2026-04-23',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.get_decision_detail(
            snapshot_id=101,
            horizons=[5],
            evaluation_version='signal_decider_eval_test',
        )

        self.assertEqual(result['item']['snapshot_id'], 101)
        self.assertEqual(result['item']['explanation']['decision_action'], 'buy')
        self.assertEqual(result['item']['explanation']['feature_flags'][0]['key'], 'technical.breakout_20d')
        self.assertEqual(result['item']['evaluation_summary']['latest_graded']['grade_label'], 'good_buy')
        self.assertEqual(result['item']['market_context']['status'], 'ready')
        self.assertEqual(result['item']['market_context']['indices']['kospi']['level'], 2642.07)
        self.assertEqual(result['item']['request_payload']['market_context']['flows']['foreign']['value'], 746146)
        self.assertEqual(result['item']['outcomes'][0]['horizon_days'], 5)
        self.assertEqual(result['item']['evaluations'][0]['horizon_days'], 5)

    def test_get_decision_detail_surfaces_normalized_wait_reason(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=42,
                    snapshot_id=102,
                    market_date='2026-04-23',
                    symbol='005930',
                    action='wait',
                    thesis_summary='Need more data before entry.',
                    decision_risks=['Bid/ask spread is stale into the open.'],
                    data_quality_warnings=[
                        'No recent news/disclosures were captured.',
                        'Realtime orderbook freshness is stale.',
                    ],
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        result = service.get_decision_detail(
            snapshot_id=102,
            horizons=[5],
            evaluation_version='signal_decider_eval_test',
        )

        self.assertEqual(result['item']['wait_reason']['code'], 'stale_quote_orderbook')
        self.assertEqual(result['item']['decision']['wait_reason']['code'], 'stale_quote_orderbook')
        self.assertEqual(result['item']['explanation']['wait_reason']['code'], 'stale_quote_orderbook')
        self.assertEqual(result['item']['decision_delta']['current_wait_reason']['code'], 'stale_quote_orderbook')
        self.assertEqual(result['item']['explanation']['decision_delta']['current_wait_reason']['code'], 'stale_quote_orderbook')

    def test_get_decision_detail_includes_actual_execution_outcomes_separately(self):
        repository = FakeSignalDeciderLoopRepository(
            [_feedback_row(feedback_id=41, snapshot_id=101, market_date='2026-04-23', symbol='005930', action='buy')]
        )
        service = SignalDeciderLoopService(repository=repository)

        actual_execution_outcomes = [
            {
                'outcome_id': 7,
                'decision_snapshot_id': 101,
                'symbol': '005930',
                'lifecycle_status': 'closed',
                'exit_reason': 'target_hit',
                'realized_pnl_krw': '20000.00',
            }
        ]
        actual_execution_summary = {
            'count': 1,
            'closed_count': 1,
            'open_count': 0,
            'unresolved_count': 0,
            'rejected_count': 0,
            'lifecycle_status_counts': {'closed': 1},
            'exit_reason_counts': {'target_hit': 1},
            'total_closed_quantity': '5',
            'total_open_quantity': '0',
            'total_realized_pnl_krw': '20000.00',
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.signal_decider_execution_outcome_service.list_snapshot_outcomes',
            return_value=actual_execution_outcomes,
        ) as list_mock, patch(
            'app.agent_context.signal_decider_loop_service.signal_decider_execution_outcome_service.build_summary',
            return_value=actual_execution_summary,
        ) as summary_mock:
            result = service.get_decision_detail(
                snapshot_id=101,
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        list_mock.assert_called_once_with(decision_snapshot_ids=[101], refresh=True)
        summary_mock.assert_called_once_with(items=actual_execution_outcomes)
        self.assertEqual(result['summary']['actual_execution_outcome_count'], 1)
        self.assertEqual(result['item']['actual_execution_summary'], actual_execution_summary)
        self.assertEqual(result['item']['actual_execution_outcomes'], actual_execution_outcomes)

    def test_get_review_calendar_summarizes_days_and_pending_coverage(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-20',
                    symbol='005930',
                    action='buy',
                    target_price=105,
                    stop_loss_price=96,
                ),
                _feedback_row(
                    feedback_id=2,
                    snapshot_id=102,
                    market_date='2026-04-20',
                    symbol='000660',
                    action='wait',
                    target_price=55,
                    stop_loss_price=47,
                ),
                _feedback_row(
                    feedback_id=3,
                    snapshot_id=103,
                    market_date='2026-04-20',
                    symbol='035420',
                    action='reject',
                    target_price=72,
                    stop_loss_price=59,
                ),
                _feedback_row(
                    feedback_id=4,
                    snapshot_id=104,
                    market_date='2026-04-29',
                    symbol='051910',
                    action='buy',
                    target_price=210,
                    stop_loss_price=190,
                ),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            '005930': [
                {'trade_date': '2026-04-20', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 1000},
                {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 106, 'low_price': 99, 'close_price': 104, 'volume': 1000},
                {'trade_date': '2026-04-22', 'open_price': 104, 'high_price': 107, 'low_price': 103, 'close_price': 105, 'volume': 1000},
                {'trade_date': '2026-04-23', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 110, 'low_price': 107, 'close_price': 109, 'volume': 1000},
            ],
            '000660': [
                {'trade_date': '2026-04-20', 'open_price': 50, 'high_price': 50, 'low_price': 49, 'close_price': 50, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 56, 'low_price': 49, 'close_price': 55, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 55, 'high_price': 57, 'low_price': 54, 'close_price': 56, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 56, 'high_price': 58, 'low_price': 55, 'close_price': 57, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 57, 'high_price': 59, 'low_price': 56, 'close_price': 58, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 58, 'high_price': 60, 'low_price': 57, 'close_price': 59, 'volume': 900},
            ],
            '035420': [
                {'trade_date': '2026-04-20', 'open_price': 60, 'high_price': 61, 'low_price': 59, 'close_price': 60, 'volume': 850},
                {'trade_date': '2026-04-21', 'open_price': 60, 'high_price': 61, 'low_price': 58, 'close_price': 59, 'volume': 850},
                {'trade_date': '2026-04-22', 'open_price': 59, 'high_price': 60, 'low_price': 57, 'close_price': 58, 'volume': 850},
                {'trade_date': '2026-04-23', 'open_price': 58, 'high_price': 59, 'low_price': 56, 'close_price': 57, 'volume': 850},
                {'trade_date': '2026-04-24', 'open_price': 57, 'high_price': 58, 'low_price': 55, 'close_price': 56, 'volume': 850},
                {'trade_date': '2026-04-27', 'open_price': 56, 'high_price': 57, 'low_price': 54, 'close_price': 55, 'volume': 850},
            ],
            '051910': [
                {'trade_date': '2026-04-29', 'open_price': 200, 'high_price': 201, 'low_price': 198, 'close_price': 200, 'volume': 700},
                {'trade_date': '2026-04-30', 'open_price': 201, 'high_price': 204, 'low_price': 199, 'close_price': 203, 'volume': 700},
            ],
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-29',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.get_review_calendar(
            month='2026-04',
            horizons=[1, 3, 5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        day_items = {item['market_date']: item for item in result['items']}
        self.assertEqual(result['summary']['day_count'], 2)
        self.assertEqual(result['summary']['decision_count'], 4)
        self.assertEqual(result['summary']['scorecard']['target_hit_count'], 2)
        self.assertEqual(result['summary']['scorecard']['stop_hit_count'], 1)
        self.assertEqual(result['summary']['scorecard']['pending_count'], 1)
        self.assertEqual(result['summary']['calendar_status'], 'partial')
        self.assertEqual(day_items['2026-04-20']['decision_count'], 3)
        self.assertEqual(day_items['2026-04-20']['calendar_status'], 'reviewed')
        self.assertEqual(day_items['2026-04-20']['scorecard']['target_hit_count'], 2)
        self.assertEqual(day_items['2026-04-29']['calendar_status'], 'stored')
        self.assertEqual(day_items['2026-04-29']['evaluation_coverage']['status'], 'pending')
        self.assertEqual(day_items['2026-04-29']['scorecard']['pending_count'], 1)

    def test_get_review_calendar_rejects_zero_primary_horizon(self):
        repository = FakeSignalDeciderLoopRepository([])
        service = SignalDeciderLoopService(repository=repository)

        with self.assertRaisesRegex(
            ValueError,
            r'primary_horizon_days must be drawn from \[1, 3, 5, 10\]',
        ):
            service.get_review_calendar(month='2026-04', primary_horizon_days=0)

        self.assertEqual(repository.list_decision_snapshots_calls, [])

    def test_get_review_date_returns_replay_summaries_with_stable_insights(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-20',
                    symbol='005930',
                    action='buy',
                    target_price=105,
                    stop_loss_price=96,
                ),
                _feedback_row(
                    feedback_id=2,
                    snapshot_id=102,
                    market_date='2026-04-20',
                    symbol='000660',
                    action='wait',
                    target_price=55,
                    stop_loss_price=47,
                ),
                _feedback_row(
                    feedback_id=3,
                    snapshot_id=103,
                    market_date='2026-04-20',
                    symbol='035420',
                    action='reject',
                    target_price=72,
                    stop_loss_price=59,
                ),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            '005930': [
                {'trade_date': '2026-04-20', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 1000},
                {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 106, 'low_price': 99, 'close_price': 104, 'volume': 1000},
                {'trade_date': '2026-04-22', 'open_price': 104, 'high_price': 107, 'low_price': 103, 'close_price': 105, 'volume': 1000},
                {'trade_date': '2026-04-23', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 110, 'low_price': 107, 'close_price': 109, 'volume': 1000},
            ],
            '000660': [
                {'trade_date': '2026-04-20', 'open_price': 50, 'high_price': 50, 'low_price': 49, 'close_price': 50, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 56, 'low_price': 49, 'close_price': 55, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 55, 'high_price': 57, 'low_price': 54, 'close_price': 56, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 56, 'high_price': 58, 'low_price': 55, 'close_price': 57, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 57, 'high_price': 59, 'low_price': 56, 'close_price': 58, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 58, 'high_price': 60, 'low_price': 57, 'close_price': 59, 'volume': 900},
            ],
            '035420': [
                {'trade_date': '2026-04-20', 'open_price': 60, 'high_price': 61, 'low_price': 59, 'close_price': 60, 'volume': 850},
                {'trade_date': '2026-04-21', 'open_price': 60, 'high_price': 61, 'low_price': 58, 'close_price': 59, 'volume': 850},
                {'trade_date': '2026-04-22', 'open_price': 59, 'high_price': 60, 'low_price': 57, 'close_price': 58, 'volume': 850},
                {'trade_date': '2026-04-23', 'open_price': 58, 'high_price': 59, 'low_price': 56, 'close_price': 57, 'volume': 850},
                {'trade_date': '2026-04-24', 'open_price': 57, 'high_price': 58, 'low_price': 55, 'close_price': 56, 'volume': 850},
                {'trade_date': '2026-04-27', 'open_price': 56, 'high_price': 57, 'low_price': 54, 'close_price': 55, 'volume': 850},
            ],
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[1, 3, 5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.get_review_date(
            market_date='2026-04-20',
            horizons=[1, 3, 5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        items_by_symbol = {item['symbol']: item for item in result['items']}
        self.assertEqual(result['summary']['evaluation_coverage']['status'], 'complete')
        self.assertEqual(items_by_symbol['005930']['review']['insight']['code'], 'buy_should_have_taken')
        self.assertEqual(items_by_symbol['005930']['review']['insight']['priority'], 'low')
        self.assertEqual(items_by_symbol['005930']['review']['quality_verdict'], 'buy_should_have_taken')
        self.assertEqual(items_by_symbol['005930']['review']['review_conclusion'], 'Buy held up on replay.')
        self.assertEqual(items_by_symbol['005930']['review']['path_outcome'], 'target_hit')
        self.assertEqual(items_by_symbol['005930']['review']['intraday_path_result'], 'target_only')
        self.assertEqual(items_by_symbol['005930']['review']['path_analysis']['path_marker'], 'target_hit')
        self.assertEqual(
            items_by_symbol['005930']['review']['narrative']['supporting_reasons'][0]['code'],
            'target_hit_without_stop',
        )
        self.assertEqual(
            items_by_symbol['005930']['review']['narrative_text'],
            items_by_symbol['005930']['review']['narrative']['summary_text'],
        )
        self.assertEqual(items_by_symbol['000660']['review']['insight']['code'], 'wait_missed_opportunity')
        self.assertEqual(items_by_symbol['000660']['review']['path_analysis']['path_marker'], 'target_hit')
        self.assertEqual(
            items_by_symbol['000660']['review']['narrative']['counterfactual_summary']['action'],
            'buy',
        )
        self.assertEqual(items_by_symbol['035420']['review']['insight']['code'], 'reject_correct')
        self.assertEqual(items_by_symbol['035420']['review']['path_analysis']['path_marker'], 'stop_hit')

    def test_get_review_detail_returns_collision_insight_and_price_path(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-20',
                    symbol='005930',
                    action='buy',
                    target_price=105,
                    stop_loss_price=96,
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars = [
            {'trade_date': '2026-04-20', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 1000},
            {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 106, 'low_price': 95, 'close_price': 101, 'volume': 1000},
            {'trade_date': '2026-04-22', 'open_price': 101, 'high_price': 104, 'low_price': 99, 'close_price': 102, 'volume': 1000},
            {'trade_date': '2026-04-23', 'open_price': 102, 'high_price': 103, 'low_price': 100, 'close_price': 101, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 101, 'high_price': 102, 'low_price': 99, 'close_price': 100, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 100, 'high_price': 101, 'low_price': 98, 'close_price': 99, 'volume': 1000},
        ]

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            return_value=bars,
        ):
            service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.get_review_detail(
            market_date='2026-04-20',
            snapshot_id=101,
            horizons=[5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        self.assertEqual(result['summary']['insight_code'], 'ambiguous_target_stop_collision')
        self.assertEqual(result['summary']['insight_priority'], 'high')
        self.assertEqual(result['summary']['headline'], 'Replay path is operationally ambiguous.')
        self.assertEqual(result['item']['review']['path_analysis']['path_marker'], 'collision')
        self.assertEqual(result['item']['review']['insight']['code'], 'ambiguous_target_stop_collision')
        self.assertEqual(result['item']['review']['quality_verdict'], 'ambiguous_target_stop_collision')
        self.assertEqual(result['item']['review']['path_outcome'], 'collision')
        self.assertEqual(result['item']['review']['intraday_path_result'], 'collision')
        self.assertEqual(
            result['item']['review']['narrative']['supporting_reasons'][0]['code'],
            'target_stop_collision_same_session',
        )
        self.assertEqual(result['item']['observed_price_path'][0]['trade_date'], '2026-04-21')
        self.assertEqual(
            result['item']['review']['path_analysis']['checkpoints']['first_target_hit_trade_date'],
            '2026-04-21',
        )
        self.assertEqual(
            result['item']['review']['path_analysis']['checkpoints']['first_stop_loss_hit_trade_date'],
            '2026-04-21',
        )

    def test_get_review_detail_structures_stop_before_target_buy_narrative(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-20',
                    symbol='005930',
                    action='buy',
                    target_price=110,
                    stop_loss_price=95,
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars = [
            {'trade_date': '2026-04-20', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 1000},
            {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 103, 'low_price': 94, 'close_price': 97, 'volume': 1000},
            {'trade_date': '2026-04-22', 'open_price': 97, 'high_price': 111, 'low_price': 96, 'close_price': 109, 'volume': 1000},
            {'trade_date': '2026-04-23', 'open_price': 109, 'high_price': 112, 'low_price': 107, 'close_price': 111, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 111, 'high_price': 113, 'low_price': 109, 'close_price': 112, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 112, 'high_price': 114, 'low_price': 110, 'close_price': 113, 'volume': 1000},
        ]

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            return_value=bars,
        ):
            service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.get_review_detail(
            market_date='2026-04-20',
            snapshot_id=101,
            horizons=[5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        self.assertEqual(result['item']['review']['insight']['code'], 'buy_stopped_out')
        self.assertEqual(result['item']['review']['insight']['priority'], 'high')
        self.assertEqual(result['item']['review']['path_analysis']['path_marker'], 'stop_hit')
        self.assertEqual(result['item']['review']['path_analysis']['trigger_sequence'], 'stop_before_target')
        self.assertEqual(
            result['item']['review']['narrative']['supporting_reasons'][0]['code'],
            'stop_hit_before_target',
        )
        self.assertEqual(
            result['item']['review']['narrative']['headline'],
            'Buy would have stopped out first.',
        )

    def test_get_review_detail_structures_no_threshold_positive_close_wait_narrative(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-20',
                    symbol='000660',
                    action='wait',
                    target_price=60,
                    stop_loss_price=45,
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars = [
            {'trade_date': '2026-04-20', 'open_price': 49, 'high_price': 50, 'low_price': 48, 'close_price': 49, 'volume': 900},
            {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 52, 'low_price': 49, 'close_price': 51, 'volume': 900},
            {'trade_date': '2026-04-22', 'open_price': 51, 'high_price': 53, 'low_price': 50, 'close_price': 52, 'volume': 900},
            {'trade_date': '2026-04-23', 'open_price': 52, 'high_price': 53, 'low_price': 51, 'close_price': 52, 'volume': 900},
            {'trade_date': '2026-04-24', 'open_price': 52, 'high_price': 54, 'low_price': 51, 'close_price': 53, 'volume': 900},
            {'trade_date': '2026-04-27', 'open_price': 53, 'high_price': 54, 'low_price': 52, 'close_price': 53, 'volume': 900},
        ]

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            return_value=bars,
        ):
            service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.get_review_detail(
            market_date='2026-04-20',
            snapshot_id=101,
            horizons=[5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        self.assertEqual(result['item']['review']['insight']['code'], 'wait_missed_opportunity')
        self.assertEqual(result['item']['review']['path_analysis']['path_marker'], 'neither')
        self.assertEqual(
            result['item']['review']['narrative']['supporting_reasons'][0]['code'],
            'no_threshold_hit_positive_close',
        )
        self.assertEqual(
            result['item']['review']['narrative']['counterfactual_summary']['action'],
            'buy',
        )

    def test_get_review_detail_structures_no_threshold_negative_close_buy_narrative(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-20',
                    symbol='005930',
                    action='buy',
                    target_price=120,
                    stop_loss_price=80,
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars = [
            {'trade_date': '2026-04-20', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 1000},
            {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 102, 'low_price': 98, 'close_price': 99, 'volume': 1000},
            {'trade_date': '2026-04-22', 'open_price': 99, 'high_price': 101, 'low_price': 98, 'close_price': 98, 'volume': 1000},
            {'trade_date': '2026-04-23', 'open_price': 98, 'high_price': 100, 'low_price': 97, 'close_price': 98, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 97, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 97, 'high_price': 98, 'low_price': 97, 'close_price': 97, 'volume': 1000},
        ]

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            return_value=bars,
        ):
            service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.get_review_detail(
            market_date='2026-04-20',
            snapshot_id=101,
            horizons=[5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        self.assertEqual(result['item']['review']['insight']['code'], 'buy_no_edge')
        self.assertEqual(result['item']['review']['path_analysis']['path_marker'], 'neither')
        self.assertEqual(
            result['item']['review']['narrative']['supporting_reasons'][0]['code'],
            'no_threshold_hit_negative_close',
        )
        self.assertIsNone(result['item']['review']['narrative']['counterfactual_summary'])

    def test_get_review_date_returns_pending_structured_narrative_for_incomplete_replay(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-29',
                    symbol='051910',
                    action='buy',
                    target_price=210,
                    stop_loss_price=190,
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars = [
            {'trade_date': '2026-04-29', 'open_price': 200, 'high_price': 201, 'low_price': 198, 'close_price': 200, 'volume': 700},
            {'trade_date': '2026-04-30', 'open_price': 201, 'high_price': 204, 'low_price': 199, 'close_price': 203, 'volume': 700},
        ]

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            return_value=bars,
        ):
            service.compute_evaluations(
                market_date_from='2026-04-29',
                market_date_to='2026-04-29',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.get_review_date(
            market_date='2026-04-29',
            horizons=[5],
            primary_horizon_days=5,
            evaluation_version='signal_decider_eval_test',
        )

        item = result['items'][0]
        self.assertEqual(item['review']['status'], 'pending')
        self.assertEqual(item['review']['insight']['code'], 'evaluation_pending')
        self.assertEqual(item['review']['status_reason'], 'pending_maturity')
        self.assertEqual(item['review']['coverage']['earliest_computable_date'], '2026-05-08')
        self.assertEqual(item['review']['narrative']['supporting_reasons'][0]['code'], 'pending_maturity')
        self.assertEqual(
            result['summary']['evaluation_coverage']['status_reason_counts'],
            {'pending_maturity': 1},
        )

    def test_get_diagnostics_batches_outcomes_and_evaluations(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(feedback_id=1, snapshot_id=101, market_date='2026-04-20', symbol='005930', action='buy'),
                _feedback_row(feedback_id=2, snapshot_id=102, market_date='2026-04-20', symbol='000660', action='wait'),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            '005930': [
                {'trade_date': '2026-04-20', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
                {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 104, 'low_price': 99, 'close_price': 103, 'volume': 1000},
                {'trade_date': '2026-04-22', 'open_price': 103, 'high_price': 106, 'low_price': 102, 'close_price': 105, 'volume': 1000},
                {'trade_date': '2026-04-23', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 112, 'low_price': 107, 'close_price': 111, 'volume': 1000},
            ],
            '000660': [
                {'trade_date': '2026-04-20', 'open_price': 48, 'high_price': 49, 'low_price': 47, 'close_price': 48, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 53, 'low_price': 49, 'close_price': 52, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 52, 'high_price': 54, 'low_price': 51, 'close_price': 53, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 53, 'high_price': 55, 'low_price': 52, 'close_price': 54, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 54, 'high_price': 56, 'low_price': 53, 'close_price': 55, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 55, 'high_price': 58, 'low_price': 54, 'close_price': 57, 'volume': 900},
            ],
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        repository.reset_calls()

        result = service.get_diagnostics(
            market_date='2026-04-20',
            evaluation_version='signal_decider_eval_test',
            limit=20,
        )

        self.assertEqual(result['summary']['count'], 2)
        self.assertEqual(len(repository.list_realized_outcomes_calls), 1)
        self.assertEqual(len(repository.list_evaluations_calls), 1)
        self.assertEqual(
            repository.list_realized_outcomes_calls[0]['decision_snapshot_ids'],
            [102, 101],
        )
        self.assertEqual(
            repository.list_evaluations_calls[0]['decision_snapshot_ids'],
            [102, 101],
        )

    def test_compute_evaluations_grades_buy_and_wait_across_horizons(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(feedback_id=1, market_date='2026-04-20', symbol='005930', action='buy'),
                _feedback_row(feedback_id=2, market_date='2026-04-20', symbol='000660', action='wait'),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            '005930': [
                {'trade_date': '2026-04-20', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
                {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 104, 'low_price': 99, 'close_price': 103, 'volume': 1000},
                {'trade_date': '2026-04-22', 'open_price': 103, 'high_price': 106, 'low_price': 102, 'close_price': 105, 'volume': 1000},
                {'trade_date': '2026-04-23', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 112, 'low_price': 107, 'close_price': 111, 'volume': 1000},
            ],
            '000660': [
                {'trade_date': '2026-04-20', 'open_price': 48, 'high_price': 49, 'low_price': 47, 'close_price': 48, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 53, 'low_price': 49, 'close_price': 52, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 52, 'high_price': 54, 'low_price': 51, 'close_price': 53, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 53, 'high_price': 55, 'low_price': 52, 'close_price': 54, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 54, 'high_price': 56, 'low_price': 53, 'close_price': 55, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 55, 'high_price': 58, 'low_price': 54, 'close_price': 57, 'volume': 900},
            ],
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            result = service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[1, 5],
                evaluation_version='signal_decider_eval_test',
            )

        self.assertEqual(result['summary']['decisions_considered'], 2)
        self.assertEqual(result['summary']['outcomes_upserted'], 4)
        self.assertEqual(result['summary']['evaluations_upserted'], 4)
        items_by_symbol = {item['symbol']: item for item in result['items']}
        buy_evaluations = {item['horizon_days']: item for item in items_by_symbol['005930']['evaluations']}
        wait_evaluations = {item['horizon_days']: item for item in items_by_symbol['000660']['evaluations']}
        self.assertEqual(buy_evaluations[1]['grade_label'], 'good_buy')
        self.assertEqual(buy_evaluations[5]['grade_label'], 'good_buy')
        self.assertEqual(wait_evaluations[5]['grade_label'], 'missed_opportunity_after_wait')
        buy_outcomes = {item['horizon_days']: item for item in items_by_symbol['005930']['outcomes']}
        self.assertEqual(buy_outcomes[1]['entry_trade_date'], '2026-04-21')
        self.assertEqual(buy_outcomes[5]['exit_trade_date'], '2026-04-27')

    def test_compute_evaluations_uses_earliest_future_bars_for_realized_outcomes(self):
        repository = FakeSignalDeciderLoopRepository(
            [_feedback_row(feedback_id=1, market_date='2026-04-20', symbol='005930', action='buy')]
        )
        service = SignalDeciderLoopService(repository=repository)

        future_bars = [
            {'trade_date': '2026-04-20', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
            {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 104, 'low_price': 99, 'close_price': 103, 'volume': 1000},
            {'trade_date': '2026-04-22', 'open_price': 103, 'high_price': 106, 'low_price': 102, 'close_price': 105, 'volume': 1000},
            {'trade_date': '2026-04-23', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 112, 'low_price': 107, 'close_price': 111, 'volume': 1000},
            {'trade_date': '2026-04-28', 'open_price': 111, 'high_price': 114, 'low_price': 110, 'close_price': 113, 'volume': 1000},
            {'trade_date': '2026-04-29', 'open_price': 113, 'high_price': 116, 'low_price': 112, 'close_price': 115, 'volume': 1000},
            {'trade_date': '2026-04-30', 'open_price': 115, 'high_price': 118, 'low_price': 114, 'close_price': 117, 'volume': 1000},
        ]
        captured_kwargs = []

        def fake_load(symbol, **kwargs):
            self.assertEqual(symbol, '005930')
            captured_kwargs.append(kwargs)
            if kwargs.get('limit') == 1 and kwargs.get('prefer_recent_limit', True):
                return future_bars[-1:]
            return future_bars

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=fake_load,
        ):
            result = service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        self.assertEqual(len(captured_kwargs), 2)
        self.assertFalse(captured_kwargs[0]['prefer_recent_limit'])
        self.assertEqual(captured_kwargs[0]['start_date'], date.fromisoformat('2026-04-20'))
        self.assertEqual(captured_kwargs[0]['end_date'], date.fromisoformat('2026-04-27'))
        self.assertEqual(captured_kwargs[1]['start_date'], date.fromisoformat('2026-04-20'))
        self.assertEqual(captured_kwargs[1]['limit'], 1)
        self.assertTrue(captured_kwargs[1]['prefer_recent_limit'])
        outcome = result['items'][0]['outcomes'][0]
        evaluation = result['items'][0]['evaluations'][0]
        self.assertEqual(outcome['entry_trade_date'], '2026-04-21')
        self.assertEqual(outcome['exit_trade_date'], '2026-04-27')
        self.assertEqual(outcome['bars_observed'], 5)
        self.assertEqual(outcome['status_reason'], 'computed')
        self.assertEqual(outcome['coverage']['status_reason'], 'computed')
        self.assertEqual(evaluation['status_reason'], 'computed')
        self.assertEqual(result['summary']['coverage']['computed_count'], 1)

    def test_list_evaluations_exposes_pending_maturity_coverage_for_latest_immature_horizon(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-29',
                    symbol='051910',
                    action='buy',
                    target_price=210,
                    stop_loss_price=190,
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars = [
            {'trade_date': '2026-04-29', 'open_price': 200, 'high_price': 201, 'low_price': 198, 'close_price': 200, 'volume': 700},
            {'trade_date': '2026-04-30', 'open_price': 201, 'high_price': 204, 'low_price': 199, 'close_price': 203, 'volume': 700},
        ]

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            return_value=bars,
        ):
            service.compute_evaluations(
                market_date_from='2026-04-29',
                market_date_to='2026-04-29',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )
            result = service.list_evaluations(
                market_date='2026-04-29',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        self.assertEqual(result['summary']['count'], 1)
        self.assertEqual(result['summary']['evaluated_count'], 0)
        self.assertEqual(result['summary']['stored_evaluation_count'], 1)
        self.assertEqual(result['summary']['coverage']['pending_maturity_count'], 1)
        self.assertEqual(result['summary']['coverage']['status_reason_counts'], {'pending_maturity': 1})
        item = result['items'][0]
        self.assertEqual(item['coverage_summary']['computed_count'], 0)
        coverage = item['coverage'][0]
        self.assertEqual(coverage['status_reason'], 'pending_maturity')
        self.assertEqual(coverage['decision_market_date'], '2026-04-29')
        self.assertEqual(coverage['entry_trade_date'], '2026-04-30')
        self.assertEqual(coverage['observed_bars_count'], 1)
        self.assertEqual(coverage['required_bars_count'], 5)
        self.assertEqual(coverage['earliest_computable_date'], '2026-05-08')
        self.assertEqual(item['evaluations'][0]['status_reason'], 'pending_maturity')
        self.assertEqual(item['evaluations'][0]['evaluation_status'], 'pending')

    def test_compute_evaluations_marks_mature_gap_as_insufficient_price_data(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-20',
                    symbol='005930',
                    action='buy',
                    target_price=110,
                    stop_loss_price=95,
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        future_bars = [
            {'trade_date': '2026-04-20', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
            {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 104, 'low_price': 99, 'close_price': 103, 'volume': 1000},
            {'trade_date': '2026-04-22', 'open_price': 103, 'high_price': 106, 'low_price': 102, 'close_price': 105, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 112, 'low_price': 107, 'close_price': 111, 'volume': 1000},
        ]
        latest_bar = [
            {'trade_date': '2026-04-28', 'open_price': 111, 'high_price': 114, 'low_price': 110, 'close_price': 113, 'volume': 1000}
        ]

        def fake_load(symbol, **kwargs):
            self.assertEqual(symbol, '005930')
            if kwargs.get('limit') == 1 and kwargs.get('prefer_recent_limit', True):
                return latest_bar
            return future_bars

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=fake_load,
        ):
            result = service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )
            review_result = service.get_review_date(
                market_date='2026-04-20',
                horizons=[5],
                primary_horizon_days=5,
                evaluation_version='signal_decider_eval_test',
            )

        outcome = result['items'][0]['outcomes'][0]
        evaluation = result['items'][0]['evaluations'][0]
        self.assertEqual(outcome['status_reason'], 'insufficient_price_data')
        self.assertTrue(outcome['coverage']['matured'])
        self.assertEqual(outcome['coverage']['missing_trade_dates'], ['2026-04-23'])
        self.assertEqual(evaluation['status_reason'], 'insufficient_price_data')
        self.assertEqual(evaluation['evaluation_status'], 'pending')
        self.assertEqual(result['summary']['coverage']['insufficient_price_data_count'], 1)
        self.assertEqual(result['summary']['coverage']['status_reason_counts'], {'insufficient_price_data': 1})
        review_item = review_result['items'][0]
        self.assertEqual(review_result['summary']['evaluation_coverage']['pending_count'], 1)
        self.assertEqual(review_result['summary']['evaluation_coverage']['insufficient_price_data_count'], 1)
        self.assertEqual(review_result['summary']['horizon_coverage'][0]['insufficient_price_data_count'], 1)
        self.assertEqual(review_item['review']['status'], 'pending')
        self.assertEqual(review_item['review']['status_reason'], 'insufficient_price_data')
        self.assertEqual(review_item['review']['coverage']['status_reason'], 'insufficient_price_data')
        self.assertEqual(
            review_item['review']['narrative']['supporting_reasons'][0]['code'],
            'insufficient_price_data',
        )

    def test_compute_evaluations_marks_severe_drawdown_recovery_buy_as_bad_buy(self):
        repository = FakeSignalDeciderLoopRepository(
            [_feedback_row(feedback_id=1, market_date='2026-04-20', symbol='005930', action='buy')]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars = [
            {'trade_date': '2026-04-20', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
            {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 103, 'low_price': 96, 'close_price': 101, 'volume': 1000},
            {'trade_date': '2026-04-22', 'open_price': 101, 'high_price': 107, 'low_price': 100, 'close_price': 106, 'volume': 1000},
            {'trade_date': '2026-04-23', 'open_price': 106, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 105, 'close_price': 108, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 110, 'low_price': 107, 'close_price': 109, 'volume': 1000},
        ]

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            return_value=bars,
        ):
            result = service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        evaluation = result['items'][0]['evaluations'][0]
        outcome = result['items'][0]['outcomes'][0]
        self.assertEqual(evaluation['grade_label'], 'bad_buy')
        self.assertTrue(evaluation['rationale']['severe_drawdown_before_target'])
        self.assertEqual(outcome['worst_drawdown_trade_date'], '2026-04-21')
        self.assertEqual(outcome['first_target_hit_trade_date'], '2026-04-22')

    def test_compute_evaluations_detects_pre_target_severe_drawdown_when_worst_low_is_later(self):
        repository = FakeSignalDeciderLoopRepository(
            [_feedback_row(feedback_id=1, market_date='2026-04-20', symbol='005930', action='buy')]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars = [
            {'trade_date': '2026-04-20', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
            {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 103, 'low_price': 96, 'close_price': 100, 'volume': 1000},
            {'trade_date': '2026-04-22', 'open_price': 100, 'high_price': 106, 'low_price': 99, 'close_price': 105, 'volume': 1000},
            {'trade_date': '2026-04-23', 'open_price': 105, 'high_price': 106, 'low_price': 93, 'close_price': 95, 'volume': 1000},
            {'trade_date': '2026-04-24', 'open_price': 95, 'high_price': 98, 'low_price': 94, 'close_price': 96, 'volume': 1000},
            {'trade_date': '2026-04-27', 'open_price': 96, 'high_price': 99, 'low_price': 95, 'close_price': 97, 'volume': 1000},
        ]

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            return_value=bars,
        ):
            result = service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        evaluation = result['items'][0]['evaluations'][0]
        outcome = result['items'][0]['outcomes'][0]
        self.assertEqual(evaluation['grade_label'], 'bad_buy')
        self.assertTrue(evaluation['rationale']['severe_drawdown_before_target'])
        self.assertEqual(evaluation['diagnostics']['first_severe_drawdown_trade_date'], '2026-04-21')
        self.assertEqual(outcome['worst_drawdown_trade_date'], '2026-04-23')
        self.assertEqual(outcome['first_target_hit_trade_date'], '2026-04-22')

    def test_compute_recent_evaluations_uses_trailing_calendar_window(self):
        repository = FakeSignalDeciderLoopRepository([])
        service = SignalDeciderLoopService(repository=repository)

        with patch.object(service, 'compute_evaluations', return_value={'request': {}, 'summary': {}, 'items': []}) as compute:
            result = service.compute_recent_evaluations(
                as_of_market_date='2026-04-30',
                symbols=['005930'],
            )

        self.assertEqual(
            compute.call_args.kwargs['market_date_from'].isoformat(),
            '2026-03-31',
        )
        self.assertEqual(
            compute.call_args.kwargs['market_date_to'].isoformat(),
            '2026-04-30',
        )
        self.assertEqual(compute.call_args.kwargs['symbols'], ['005930'])
        self.assertEqual(result['request']['lookback_days'], DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS)
        self.assertEqual(result['request']['as_of_market_date'], '2026-04-30')

    def test_compute_recent_evaluations_backfills_mature_short_horizons_while_keeping_5d_pending(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=1,
                    snapshot_id=101,
                    market_date='2026-04-30',
                    symbol='005930',
                    action='buy',
                    target_price=110,
                    stop_loss_price=95,
                )
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        future_bars = [
            {'trade_date': '2026-04-30', 'open_price': 99, 'high_price': 100, 'low_price': 98, 'close_price': 99, 'volume': 1000},
            {'trade_date': '2026-05-04', 'open_price': 100, 'high_price': 103, 'low_price': 99, 'close_price': 102, 'volume': 1000},
            {'trade_date': '2026-05-06', 'open_price': 102, 'high_price': 106, 'low_price': 101, 'close_price': 105, 'volume': 1000},
            {'trade_date': '2026-05-07', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
            {'trade_date': '2026-05-08', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
        ]
        latest_bar = [future_bars[-1]]

        def fake_load(symbol, **kwargs):
            self.assertEqual(symbol, '005930')
            if kwargs.get('limit') == 1 and kwargs.get('prefer_recent_limit', True):
                return latest_bar
            return future_bars

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=fake_load,
        ):
            result = service.compute_recent_evaluations(
                as_of_market_date='2026-05-08',
                lookback_days=14,
                horizons=[1, 3, 5],
                evaluation_version='signal_decider_eval_test',
            )

        self.assertEqual(result['summary']['status'], 'completed')
        self.assertEqual(result['summary']['computed_count'], 2)
        self.assertEqual(result['summary']['pending_maturity_count'], 1)
        self.assertEqual(result['summary']['insufficient_price_data_count'], 0)
        self.assertEqual(result['summary']['not_computed_count'], 0)
        self.assertEqual(result['summary']['warning_count'], 0)
        item = result['items'][0]
        evaluations = {entry['horizon_days']: entry for entry in item['evaluations']}
        self.assertEqual(evaluations[1]['status_reason'], 'computed')
        self.assertEqual(evaluations[3]['status_reason'], 'computed')
        self.assertEqual(evaluations[5]['status_reason'], 'pending_maturity')
        self.assertEqual(evaluations[5]['evaluation_status'], 'pending')

    def test_get_aggregate_diagnostics_summarizes_horizon_metrics_and_contributors(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(feedback_id=1, snapshot_id=101, market_date='2026-04-20', symbol='005930', action='buy'),
                _feedback_row(feedback_id=2, snapshot_id=102, market_date='2026-04-20', symbol='000660', action='wait'),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            '005930': [
                {'trade_date': '2026-04-20', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
                {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 104, 'low_price': 99, 'close_price': 103, 'volume': 1000},
                {'trade_date': '2026-04-22', 'open_price': 103, 'high_price': 106, 'low_price': 102, 'close_price': 105, 'volume': 1000},
                {'trade_date': '2026-04-23', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 112, 'low_price': 107, 'close_price': 111, 'volume': 1000},
            ],
            '000660': [
                {'trade_date': '2026-04-20', 'open_price': 48, 'high_price': 49, 'low_price': 47, 'close_price': 48, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 53, 'low_price': 49, 'close_price': 52, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 52, 'high_price': 54, 'low_price': 51, 'close_price': 53, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 53, 'high_price': 55, 'low_price': 52, 'close_price': 54, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 54, 'high_price': 56, 'low_price': 53, 'close_price': 55, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 55, 'high_price': 58, 'low_price': 54, 'close_price': 57, 'volume': 900},
            ],
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )

        result = service.get_aggregate_diagnostics(
            market_date='2026-04-20',
            horizons=[5],
            evaluation_version='signal_decider_eval_test',
            contributor_limit=10,
        )

        self.assertEqual(result['summary']['decision_count'], 2)
        self.assertEqual(result['summary']['evaluation_count'], 2)
        self.assertEqual(result['aggregates']['by_horizon'][0]['horizon_days'], 5)
        self.assertEqual(result['aggregates']['by_horizon'][0]['buy_precision'], 1.0)
        self.assertEqual(result['aggregates']['by_horizon'][0]['wait_missed_opportunity_rate'], 1.0)
        self.assertEqual(result['contributors']['factors'][0]['key'], 'technical.breakout_20d')
        self.assertGreaterEqual(result['contributors']['factors'][0]['negative_outcome_count'], 1)

    def test_generate_policy_candidates_builds_manual_relax_wait_candidates(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(feedback_id=2, market_date='2026-04-20', symbol='000660', action='wait'),
                _feedback_row(feedback_id=3, market_date='2026-04-20', symbol='035420', action='wait'),
                _feedback_row(feedback_id=4, market_date='2026-04-20', symbol='051910', action='wait'),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            symbol: [
                {'trade_date': '2026-04-20', 'open_price': 48, 'high_price': 49, 'low_price': 47, 'close_price': 48, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 53, 'low_price': 49, 'close_price': 52, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 52, 'high_price': 55, 'low_price': 51, 'close_price': 54, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 54, 'high_price': 57, 'low_price': 53, 'close_price': 56, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 56, 'high_price': 60, 'low_price': 55, 'close_price': 59, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 59, 'high_price': 63, 'low_price': 58, 'close_price': 62, 'volume': 900},
            ]
            for symbol in ('000660', '035420', '051910')
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            result = service.generate_policy_candidates(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                primary_horizon_days=5,
                evaluation_version='signal_decider_eval_test',
                min_sample_size=2,
            )

        self.assertGreaterEqual(result['summary']['candidates_generated'], 1)
        self.assertTrue(result['summary']['manual_activation_required'])
        self.assertGreaterEqual(result['summary']['policy_versions_registered'], 1)
        self.assertTrue(any(item['candidate_kind'] == 'relax_wait_gate' for item in result['items']))
        self.assertTrue(all(item['activation_mode'] == 'manual_only' for item in result['items']))
        self.assertTrue(all(item['policy_status'] == 'candidate' for item in result['items']))
        self.assertTrue(all(item['policy_version_record']['status'] == 'candidate' for item in result['items']))
        self.assertTrue(
            all(
                item['policy_version_record']['rule_bundle']['activation_eligibility']['eligible']
                is False
                for item in result['items']
                if item['candidate_kind'] == 'relax_wait_gate'
            )
        )

    def test_candidate_rule_bundle_builds_executable_boolean_replay_gate(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        bundle = service._candidate_rule_bundle(
            {
                'candidate_version': 'signal_decider_policy_v2',
                'base_policy_version': DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
                'evaluation_version': DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
                'market_date_from': date(2026, 4, 20),
                'market_date_to': date(2026, 4, 30),
                'horizon_days': 5,
                'focus_action': 'buy',
                'candidate_kind': 'tighten_buy_gate',
                'feature_key': 'technical.breakout_20d',
                'feature_value': 'true',
                'support_ratio': 1.0,
                'support_count': 3,
                'contradiction_count': 0,
                'evidence_payload': {'symbols': ['005930']},
            }
        )

        predicate = bundle['replay_contract']['rule_config']['rules'][0]['predicates'][0]
        self.assertIs(predicate['value'], True)

    def test_candidate_versions_are_bounded_and_do_not_recursively_embed_the_base(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))
        version_kwargs = {
            'evaluation_version': DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
            'horizon_days': 5,
            'candidate_kind': 'tighten_buy_gate',
            'feature_key': 'technical.breakout_20d',
            'feature_value': 'true',
            'market_date_from': date(2026, 4, 20),
            'market_date_to': date(2026, 4, 30),
            'symbols': ['005930'],
        }

        first_generation = service._candidate_version(
            base_policy_version=DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
            **version_kwargs,
        )
        second_generation = service._candidate_version(
            base_policy_version=first_generation,
            **version_kwargs,
        )

        self.assertLessEqual(len(first_generation), 64)
        self.assertLessEqual(len(second_generation), 64)
        self.assertTrue(first_generation.startswith('signal_decider_candidate_v1_'))
        self.assertTrue(second_generation.startswith('signal_decider_candidate_v1_'))
        self.assertNotIn(first_generation, second_generation)
        self.assertNotEqual(first_generation, second_generation)

    def test_generate_policy_candidates_keeps_base_policy_versions_separate(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(
                    feedback_id=2,
                    market_date='2026-04-20',
                    symbol='000660',
                    action='wait',
                    policy_version='signal_decider_policy_v1',
                ),
                _feedback_row(
                    feedback_id=3,
                    market_date='2026-04-20',
                    symbol='035420',
                    action='wait',
                    policy_version='signal_decider_policy_v1',
                ),
                _feedback_row(
                    feedback_id=4,
                    market_date='2026-04-20',
                    symbol='051910',
                    action='wait',
                    policy_version='signal_decider_policy_v2',
                ),
                _feedback_row(
                    feedback_id=5,
                    market_date='2026-04-20',
                    symbol='068270',
                    action='wait',
                    policy_version='signal_decider_policy_v2',
                ),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            symbol: [
                {'trade_date': '2026-04-20', 'open_price': 48, 'high_price': 49, 'low_price': 47, 'close_price': 48, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 53, 'low_price': 49, 'close_price': 52, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 52, 'high_price': 55, 'low_price': 51, 'close_price': 54, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 54, 'high_price': 57, 'low_price': 53, 'close_price': 56, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 56, 'high_price': 60, 'low_price': 55, 'close_price': 59, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 59, 'high_price': 63, 'low_price': 58, 'close_price': 62, 'volume': 900},
            ]
            for symbol in ('000660', '035420', '051910', '068270')
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            result = service.generate_policy_candidates(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                primary_horizon_days=5,
                evaluation_version='signal_decider_eval_test',
                min_sample_size=2,
            )

        grouped_candidates = [
            item
            for item in result['items']
            if item['candidate_kind'] == 'relax_wait_gate'
            and item['feature_key'] == 'technical.trend_state'
            and item['feature_value'] == 'uptrend'
        ]
        self.assertEqual(len(grouped_candidates), 2)
        self.assertEqual(
            {item['base_policy_version'] for item in grouped_candidates},
            {'signal_decider_policy_v1', 'signal_decider_policy_v2'},
        )
        self.assertTrue(all(item['support_count'] == 2 for item in grouped_candidates))
        self.assertTrue(all(item['affected_decisions_count'] == 2 for item in grouped_candidates))

    def test_list_policy_versions_returns_registry_summary(self):
        repository = FakeSignalDeciderLoopRepository([])
        repository.policy_versions['signal_decider_policy_v2'] = {
            **_policy_version_record(
                policy_version='signal_decider_policy_v2',
                status='candidate',
                parent_policy_version='signal_decider_policy_v1',
                source_candidate_version='signal_decider_policy_v2',
                change_summary='Relax wait decisions under repeated breakout evidence.',
                activated_by=None,
                activated_at=None,
            ),
            'id': 2,
        }
        service = SignalDeciderLoopService(repository=repository)

        result = service.list_policy_versions(limit=10)

        self.assertEqual(result['summary']['count'], 2)
        self.assertEqual(result['summary']['active_policy_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(result['summary']['status_counts']['active'], 1)
        self.assertEqual(result['summary']['status_counts']['candidate'], 1)
        self.assertEqual(result['items'][0]['policy_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(result['items'][1]['activation_mode'], 'manual_only')

    def test_list_policy_versions_status_filter_preserves_active_policy_summary(self):
        repository = FakeSignalDeciderLoopRepository([])
        repository.policy_versions['signal_decider_policy_v2'] = {
            **_policy_version_record(
                policy_version='signal_decider_policy_v2',
                status='candidate',
                parent_policy_version='signal_decider_policy_v1',
                source_candidate_version='signal_decider_policy_v2',
                change_summary='Relax wait decisions under repeated breakout evidence.',
                activated_by=None,
                activated_at=None,
            ),
            'id': 2,
        }
        service = SignalDeciderLoopService(repository=repository)

        result = service.list_policy_versions(status='candidate', limit=10)

        self.assertEqual(result['summary']['count'], 1)
        self.assertEqual(result['summary']['active_policy_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(result['summary']['status_counts'], {'candidate': 1})
        self.assertEqual([item['policy_version'] for item in result['items']], ['signal_decider_policy_v2'])

    def test_bulk_upsert_policy_versions_preserves_audit_metadata_for_active_and_retired_rows(self):
        for existing_status in ('active', 'retired'):
            with self.subTest(existing_status=existing_status):
                repository = FakeSignalDeciderLoopRepository([])
                repository.policy_versions['signal_decider_policy_v2'] = {
                    **_policy_version_record(
                        policy_version='signal_decider_policy_v2',
                        status=existing_status,
                        parent_policy_version='signal_decider_policy_v1',
                        source_candidate_version='signal_decider_policy_v2',
                        change_summary='Previously approved candidate.',
                        activated_by='human.reviewer',
                        activated_at='2026-04-29T06:30:00+00:00',
                        retired_at='2026-04-30T06:30:00+00:00' if existing_status == 'retired' else None,
                    ),
                    'id': 2,
                    'metadata': {
                        'schema_version': 'signal_decider_policy_metadata_v1',
                        'policy_origin': 'candidate_generation',
                        'activation_mode': 'manual_only',
                        'candidate_activation_required': True,
                        'activation_history': [
                            {
                                'event': 'activated',
                                'activated_at': '2026-04-29T06:30:00+00:00',
                                'activated_by': 'human.reviewer',
                            }
                        ],
                        'last_activation_note': 'Approved after review.',
                        'retired_by_activation_of': (
                            'signal_decider_policy_v3' if existing_status == 'retired' else None
                        ),
                    },
                }

                repository.bulk_upsert_policy_versions(
                    items=[
                        {
                            'policy_version': 'signal_decider_policy_v2',
                            'status': 'candidate',
                            'parent_policy_version': 'signal_decider_policy_v1',
                            'source_candidate_version': 'signal_decider_policy_v2',
                            'change_summary': 'Regenerated candidate payload.',
                            'rule_bundle_payload': {
                                'schema_version': 'signal_decider_policy_bundle_v1',
                                'policy_version': 'signal_decider_policy_v2',
                            },
                            'metadata_payload': {
                                'schema_version': 'signal_decider_policy_metadata_v1',
                                'policy_origin': 'candidate_generation',
                                'activation_mode': 'manual_only',
                                'candidate_activation_required': True,
                                'evidence': {'support_ratio': 1.0},
                            },
                            'created_by': 'system:auto_candidate_generation',
                            'activated_by': None,
                            'activated_at': None,
                            'retired_at': None,
                            'created_at': datetime(2026, 5, 1, 6, 0, tzinfo=timezone.utc),
                            'updated_at': datetime(2026, 5, 1, 6, 0, tzinfo=timezone.utc),
                        }
                    ]
                )

                persisted = repository.policy_versions['signal_decider_policy_v2']
                self.assertEqual(persisted['status'], existing_status)
                self.assertEqual(persisted['metadata']['policy_origin'], 'candidate_generation')
                self.assertEqual(persisted['metadata']['evidence']['support_ratio'], 1.0)
                self.assertEqual(
                    persisted['metadata']['activation_history'][0]['activated_by'],
                    'human.reviewer',
                )
                self.assertEqual(persisted['metadata']['last_activation_note'], 'Approved after review.')
                self.assertEqual(
                    persisted['metadata'].get('retired_by_activation_of'),
                    'signal_decider_policy_v3' if existing_status == 'retired' else None,
                )

    def test_get_policy_version_detail_returns_candidate_evidence(self):
        repository = FakeSignalDeciderLoopRepository([])
        repository.policy_versions['signal_decider_policy_v2'] = {
            **_policy_version_record(
                policy_version='signal_decider_policy_v2',
                status='candidate',
                parent_policy_version='signal_decider_policy_v1',
                source_candidate_version='signal_decider_policy_v2',
                change_summary='Relax wait decisions under repeated breakout evidence.',
                activated_by=None,
                activated_at=None,
            ),
            'id': 2,
        }
        repository.candidates['signal_decider_policy_v2'] = {
            'id': 1,
            'candidate_version': 'signal_decider_policy_v2',
            'base_policy_version': 'signal_decider_policy_v1',
            'evaluation_version': 'signal_decider_eval_v1',
            'market_date_from': '2026-04-20',
            'market_date_to': '2026-04-30',
            'horizon_days': 5,
            'focus_action': 'wait',
            'candidate_kind': 'relax_wait_gate',
            'feature_key': 'technical.breakout_20d',
            'feature_value': 'true',
            'affected_decisions_count': 3,
            'support_count': 3,
            'contradiction_count': 0,
            'support_ratio': 1.0,
            'status': 'generated',
            'activation_mode': 'manual_only',
            'summary': 'Relax wait decisions when breakout evidence repeats.',
            'hypothesis': 'Repeated breakout evidence suggests wait is too conservative.',
            'evidence': {'support_ratio': 1.0},
            'recommendation': {'proposed_policy_version': 'signal_decider_policy_v2'},
            'generated_at': '2026-04-30T06:00:00+00:00',
            'activated_at': None,
        }
        service = SignalDeciderLoopService(repository=repository)

        result = service.get_policy_version_detail(policy_version='signal_decider_policy_v2')

        self.assertEqual(result['item']['policy_version'], 'signal_decider_policy_v2')
        self.assertEqual(result['item']['status'], 'candidate')
        self.assertEqual(result['item']['candidate']['candidate_kind'], 'relax_wait_gate')
        self.assertEqual(result['item']['candidate_activation_required'], True)

    def test_activate_policy_version_retires_previous_active_and_marks_candidate_activated(self):
        repository = FakeSignalDeciderLoopRepository([])
        replay_evidence = _promotion_replay_evidence()
        candidate_policy = _bind_candidate_replay_contract({
            **_policy_version_record(
                policy_version='signal_decider_policy_v2',
                status='candidate',
                parent_policy_version='signal_decider_policy_v1',
                source_candidate_version='signal_decider_policy_v2',
                change_summary='Tighten buy decisions under repeated breakout evidence.',
                activated_by=None,
                activated_at=None,
            ),
            'id': 2,
        }, replay_evidence)
        repository.policy_versions['signal_decider_policy_v2'] = candidate_policy
        repository.candidates['signal_decider_policy_v2'] = {
            'id': 1,
            'candidate_version': 'signal_decider_policy_v2',
            'base_policy_version': 'signal_decider_policy_v1',
            'evaluation_version': 'signal_decider_eval_v1',
            'market_date_from': '2026-04-20',
            'market_date_to': '2026-04-30',
            'horizon_days': 5,
            'focus_action': 'buy',
            'candidate_kind': 'tighten_buy_gate',
            'feature_key': 'technical.breakout_20d',
            'feature_value': 'true',
            'affected_decisions_count': 3,
            'support_count': 3,
            'contradiction_count': 0,
            'support_ratio': 1.0,
            'status': 'generated',
            'activation_mode': 'manual_only',
            'summary': 'Tighten buy decisions when breakout evidence repeats.',
            'hypothesis': 'Repeated breakout evidence suggests buy is too aggressive.',
            'evidence': {'support_ratio': 1.0},
            'recommendation': {'proposed_policy_version': 'signal_decider_policy_v2'},
            'generated_at': '2026-04-30T06:00:00+00:00',
            'activated_at': None,
        }
        replay_service = FakeDeciderReplayService(replay_evidence['replay_artifact'])
        service = SignalDeciderLoopService(
            repository=repository,
            replay_service=replay_service,
        )

        result = service.activate_policy_version(
            policy_version='signal_decider_policy_v2',
            activated_by='human.reviewer',
            activation_note='Reviewed diagnostics and approved the candidate.',
            replay_evidence=replay_evidence,
        )

        self.assertTrue(result['summary']['activated'])
        self.assertEqual(result['summary']['previous_active_policy_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(result['item']['status'], 'active')
        self.assertEqual(repository.policy_versions[DEFAULT_SIGNAL_DECIDER_POLICY_VERSION]['status'], 'retired')
        self.assertEqual(repository.candidates['signal_decider_policy_v2']['status'], 'activated')
        promotion_evidence = repository.policy_versions['signal_decider_policy_v2']['metadata'][
            'promotion_evidence'
        ]
        self.assertEqual(promotion_evidence['validation_status'], 'passed')
        self.assertEqual(len(promotion_evidence['replay_artifact_sha256']), 64)
        self.assertEqual(
            repository.candidates['signal_decider_policy_v2']['evidence']['promotion_evidence'],
            promotion_evidence,
        )
        self.assertEqual(len(replay_service.calls), 1)
        self.assertEqual(replay_service.calls[0]['symbols'], ['005930'])
        self.assertEqual(replay_service.calls[0]['policy_versions'], ['signal_decider_policy_v1'])
        self.assertEqual(
            replay_service.calls[0]['rule_config'],
            candidate_policy['rule_bundle']['replay_contract']['rule_config'],
        )

    def test_candidate_activation_fails_closed_without_positive_net_replay_evidence(self):
        repository = FakeSignalDeciderLoopRepository([])
        valid_replay_evidence = _promotion_replay_evidence()
        candidate_policy = _bind_candidate_replay_contract({
            **_policy_version_record(
                policy_version='signal_decider_policy_v2',
                status='candidate',
                parent_policy_version='signal_decider_policy_v1',
                source_candidate_version='signal_decider_policy_v2',
                activated_by=None,
                activated_at=None,
            ),
            'id': 2,
        }, valid_replay_evidence)
        repository.policy_versions['signal_decider_policy_v2'] = candidate_policy
        service = SignalDeciderLoopService(
            repository=repository,
            replay_service=FakeDeciderReplayService(valid_replay_evidence['replay_artifact']),
        )

        invalid_evidence_cases = [
            None,
            _promotion_replay_evidence(fee_bps=0),
            _promotion_replay_evidence(fee_bps=9.999999),
            _promotion_replay_evidence(slippage_bps=4.999999),
            _promotion_replay_evidence(estimated_net_delta_krw=-100),
        ]
        wrong_cohort_evidence = _promotion_replay_evidence()
        wrong_cohort_evidence['replay_artifact']['metadata'][
            'promotion_cohort_basis'
        ] = 'actual_execution_lifecycle'
        invalid_evidence_cases.append(wrong_cohort_evidence)

        for replay_evidence in invalid_evidence_cases:
            with self.subTest(replay_evidence=replay_evidence):
                with self.assertRaises(ValueError):
                    service.activate_policy_version(
                        policy_version='signal_decider_policy_v2',
                        activated_by='human.reviewer',
                        replay_evidence=replay_evidence,
                    )

        self.assertEqual(
            repository.policy_versions[DEFAULT_SIGNAL_DECIDER_POLICY_VERSION]['status'],
            'active',
        )
        self.assertEqual(repository.policy_versions['signal_decider_policy_v2']['status'], 'candidate')

    def test_candidate_activation_rejects_tampered_artifact_or_different_contract_inputs(self):
        valid_replay_evidence = _promotion_replay_evidence()
        invalid_cases = []

        tampered_artifact = deepcopy(valid_replay_evidence)
        tampered_artifact['replay_artifact']['summary']['candidate']['estimated_net_pnl_krw'] += 1
        invalid_cases.append(('tampered_artifact', tampered_artifact))

        different_rule = deepcopy(valid_replay_evidence)
        different_rule['replay_artifact']['request']['rule_config']['rules'][0]['predicates'][0][
            'field'
        ] = 'technical.breakdown_20d'
        invalid_cases.append(('different_rule', different_rule))

        different_symbols = deepcopy(valid_replay_evidence)
        different_symbols['replay_artifact']['request']['symbols'] = ['000660']
        invalid_cases.append(('different_symbols', different_symbols))

        different_policy = deepcopy(valid_replay_evidence)
        different_policy['replay_artifact']['request']['policy_versions'] = [
            'unrelated_policy_v9'
        ]
        invalid_cases.append(('different_policy', different_policy))

        different_window = deepcopy(valid_replay_evidence)
        different_window['replay_artifact']['request']['market_date_from'] = '2026-04-21'
        invalid_cases.append(('different_window', different_window))

        different_costs = _promotion_replay_evidence(fee_bps=11)
        invalid_cases.append(('different_costs', different_costs))

        for case_name, replay_evidence in invalid_cases:
            with self.subTest(case_name=case_name):
                repository = FakeSignalDeciderLoopRepository([])
                candidate_policy = _bind_candidate_replay_contract(
                    {
                        **_policy_version_record(
                            policy_version='signal_decider_policy_v2',
                            status='candidate',
                            parent_policy_version='signal_decider_policy_v1',
                            source_candidate_version='signal_decider_policy_v2',
                            activated_by=None,
                            activated_at=None,
                        ),
                        'id': 2,
                    },
                    valid_replay_evidence,
                )
                repository.policy_versions['signal_decider_policy_v2'] = candidate_policy
                replay_service = FakeDeciderReplayService(
                    valid_replay_evidence['replay_artifact']
                )
                service = SignalDeciderLoopService(
                    repository=repository,
                    replay_service=replay_service,
                )

                with self.assertRaises(ValueError):
                    service.activate_policy_version(
                        policy_version='signal_decider_policy_v2',
                        activated_by='human.reviewer',
                        replay_evidence=replay_evidence,
                    )

                self.assertEqual(
                    repository.policy_versions['signal_decider_policy_v2']['status'],
                    'candidate',
                )

    def test_relax_wait_and_reject_candidates_fail_closed_before_replay(self):
        for candidate_kind in ('relax_wait_gate', 'relax_reject_gate'):
            with self.subTest(candidate_kind=candidate_kind):
                repository = FakeSignalDeciderLoopRepository([])
                replay_evidence = _promotion_replay_evidence()
                repository.policy_versions['signal_decider_policy_v2'] = (
                    _bind_candidate_replay_contract(
                        {
                            **_policy_version_record(
                                policy_version='signal_decider_policy_v2',
                                status='candidate',
                                parent_policy_version='signal_decider_policy_v1',
                                source_candidate_version='signal_decider_policy_v2',
                                activated_by=None,
                                activated_at=None,
                            ),
                            'id': 2,
                        },
                        replay_evidence,
                        candidate_kind=candidate_kind,
                    )
                )
                replay_service = FakeDeciderReplayService(replay_evidence['replay_artifact'])
                service = SignalDeciderLoopService(
                    repository=repository,
                    replay_service=replay_service,
                )

                with self.assertRaisesRegex(ValueError, 'counterfactual replay'):
                    service.activate_policy_version(
                        policy_version='signal_decider_policy_v2',
                        activated_by='human.reviewer',
                        replay_evidence=replay_evidence,
                    )

                self.assertEqual(replay_service.calls, [])

    def test_already_active_bootstrap_policy_is_idempotent_without_replay_evidence(self):
        repository = FakeSignalDeciderLoopRepository([])
        service = SignalDeciderLoopService(repository=repository)

        result = service.activate_policy_version(
            policy_version=DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
            activated_by='system:bootstrap',
        )

        self.assertFalse(result['summary']['activated'])
        self.assertTrue(result['summary']['already_active'])
        self.assertFalse(result['summary']['state_changed'])
        self.assertEqual(
            result['summary']['promotion_evidence_validation'],
            'not_run_already_active',
        )
        self.assertEqual(result['item']['status'], 'active')
        self.assertNotIn(
            'promotion_evidence',
            repository.policy_versions[DEFAULT_SIGNAL_DECIDER_POLICY_VERSION]['metadata'],
        )

    def test_signal_decider_calibration_report_groups_computed_rows_and_reports_exclusions(self):
        loop_service, calibration_service, bars_by_symbol = _build_signal_decider_calibration_fixture()

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            loop_service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )
            result = calibration_service.build_report(
                market_date_from='2026-04-20',
                market_date_to='2026-04-29',
                horizon_days=5,
                evaluation_version='signal_decider_eval_test',
                min_sample_size=2,
                cluster_limit=5,
            )

        self.assertEqual(result['summary']['status'], 'ready')
        self.assertEqual(result['summary']['snapshots_considered'], 7)
        self.assertEqual(result['summary']['mature_computed_count'], 4)
        self.assertEqual(
            result['exclusions']['status_reason_counts'],
            {
                'insufficient_price_data': 1,
                'not_computed': 1,
                'pending_maturity': 1,
            },
        )
        decision_groups = {
            item['bucket_value']: item
            for item in result['group_summaries']['decision_actions']['items']
        }
        self.assertEqual(decision_groups['buy']['sample_count'], 2)
        self.assertEqual(decision_groups['wait']['sample_count'], 2)
        confidence_groups = {
            item['bucket_value']: item
            for item in result['group_summaries']['confidence_buckets']['items']
        }
        self.assertEqual(confidence_groups['high']['sample_count'], 2)
        self.assertEqual(confidence_groups['low']['sample_count'], 2)
        wait_reason_groups = {
            item['bucket_value']: item
            for item in result['group_summaries']['wait_reason_taxonomy']['items']
        }
        self.assertEqual(wait_reason_groups['stale_quote_orderbook']['sample_count'], 2)
        self.assertEqual(wait_reason_groups['stale_quote_orderbook']['negative_rate'], 1.0)
        self.assertTrue(result['summary']['ready_for_operator_review'])
        self.assertEqual(result['review_boundary']['manual_review_only'], True)
        self.assertEqual(result['review_boundary']['auto_apply_prompt_changes'], False)

    def test_signal_decider_calibration_report_identifies_condition_clusters_and_hints(self):
        loop_service, calibration_service, bars_by_symbol = _build_signal_decider_calibration_fixture()

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            loop_service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )
            result = calibration_service.build_report(
                market_date_from='2026-04-20',
                market_date_to='2026-04-29',
                horizon_days=5,
                evaluation_version='signal_decider_eval_test',
                min_sample_size=2,
                cluster_limit=5,
            )

        positive_clusters = list(result['condition_clusters']['positive'])
        negative_clusters = list(result['condition_clusters']['negative'])
        self.assertTrue(
            any(
                any(
                    token['key'] == 'decision.confidence_bucket' and token['value'] == 'high'
                    for token in cluster['tokens']
                )
                for cluster in positive_clusters
            )
        )
        self.assertTrue(
            any(
                any(
                    token['key'] == 'wait_reason.code' and token['value'] == 'stale_quote_orderbook'
                    for token in cluster['tokens']
                )
                for cluster in negative_clusters
            )
        )
        self.assertGreaterEqual(len(result['calibration_hints']['scoring_guidance']), 1)
        self.assertGreaterEqual(len(result['calibration_hints']['operator_review']), 1)
        all_hints = (
            list(result['calibration_hints']['prompt_guidance'])
            + list(result['calibration_hints']['scoring_guidance'])
            + list(result['calibration_hints']['operator_review'])
        )
        self.assertTrue(all_hints)
        self.assertTrue(all(hint['manual_review_only'] is True for hint in all_hints))
        self.assertTrue(all(hint['auto_apply'] is False for hint in all_hints))

    def test_signal_decider_calibration_rule_candidates_skip_low_sample_clusters(self):
        result = _build_rule_candidate_report(
            outcome_kind='bad_buy',
            sample_count=2,
            min_sample_size=2,
        )

        self.assertEqual(result['rule_candidates']['summary']['candidate_count'], 0)
        self.assertEqual(result['rule_candidates']['status'], 'empty')
        self.assertEqual(result['rule_candidates']['thresholds']['min_sample_size'], 3)

    def test_signal_decider_calibration_rule_candidates_include_bad_buy_veto_or_downsize(self):
        result = _build_rule_candidate_report(outcome_kind='bad_buy')

        candidates = list(result['rule_candidates']['items'])
        outcome_categories = {'outcome_path_buckets', 'reward_risk_buckets'}
        outcome_predicate_fields = {
            'outcome.path_bucket',
            'outcome.drawdown_bucket',
            'decision.reward_risk_bucket',
        }
        outcome_path_buckets = result['group_summaries']['outcome_path_buckets']['items']
        self.assertTrue(
            any(
                item['bucket_key'] == 'outcome.path_bucket'
                and item['bucket_value'] == 'severe_drawdown_before_target'
                and item['bad_buy_count'] == 3
                for item in outcome_path_buckets
            )
        )
        reward_risk_buckets = result['group_summaries']['reward_risk_buckets']['items']
        self.assertTrue(
            any(
                item['bucket_key'] == 'decision.reward_risk_bucket'
                for item in reward_risk_buckets
            )
        )
        for direction in ('positive', 'negative'):
            for cluster in result['condition_clusters'][direction]:
                self.assertFalse(
                    any(token['category'] in outcome_categories for token in cluster['tokens'])
                )
        risk_candidates = [
            candidate
            for candidate in candidates
            if candidate['candidate_type'] in {'veto', 'downsize'}
        ]
        self.assertTrue(risk_candidates)
        candidate = risk_candidates[0]
        self.assertEqual(candidate['target_decider'], 'signal-decider')
        self.assertEqual(candidate['support_sample_count'], 3)
        self.assertEqual(candidate['negative_rate'], 1.0)
        self.assertEqual(candidate['bad_buy_rate'], 1.0)
        self.assertEqual(candidate['activation_status'], 'needs_replay')
        self.assertEqual(candidate['recommended_replay_config']['candidate_id'], candidate['candidate_id'])
        self.assertEqual(candidate['recommended_replay_config']['candidate_type'], candidate['candidate_type'])
        self.assertEqual(candidate['recommended_replay_config']['primary_metric'], 'bad_buy_rate')
        self.assertFalse(candidate['active_policy'])
        self.assertTrue(
            any(
                condition['field'] == 'decision.action'
                and condition['operator'] == 'eq'
                and condition['value'] == 'buy'
                for condition in candidate['predicate']['conditions']
            )
        )
        non_action_conditions = [
            condition
            for condition in candidate['predicate']['conditions']
            if condition['field'] != 'decision.action'
        ]
        self.assertTrue(non_action_conditions)
        for risk_candidate in risk_candidates:
            predicate_conditions = list(risk_candidate['predicate']['conditions'])
            replay_conditions = list(
                risk_candidate['recommended_replay_config']['predicate']['conditions']
            )
            for condition in predicate_conditions + replay_conditions:
                self.assertNotIn(condition['field'], outcome_predicate_fields)
                self.assertFalse(str(condition['field']).startswith('outcome.'))

    def test_signal_decider_calibration_rule_candidates_warn_on_missed_opportunity_cluster(self):
        result = _build_rule_candidate_report(outcome_kind='missed_opportunity')

        warning_candidates = [
            candidate
            for candidate in result['rule_candidates']['items']
            if candidate['candidate_type'] == 'prompt_warning'
            and candidate.get('missed_opportunity_rate') == 1.0
        ]
        self.assertTrue(warning_candidates)
        candidate = warning_candidates[0]
        self.assertEqual(candidate['support_sample_count'], 3)
        self.assertEqual(candidate['negative_rate'], 1.0)
        self.assertEqual(candidate['recommended_replay_config']['primary_metric'], 'missed_opportunity_rate')
        self.assertNotIn(candidate['candidate_type'], {'veto', 'downsize'})
        self.assertTrue(
            any(
                condition['field'] == 'decision.action'
                and condition['operator'] == 'eq'
                and condition['value'] == 'wait'
                for condition in candidate['predicate']['conditions']
            )
        )

    def test_signal_decider_calibration_rule_candidate_ids_are_deterministic(self):
        loop_service, calibration_service, bars_by_symbol = _build_rule_candidate_calibration_fixture(
            outcome_kind='bad_buy',
            sample_count=3,
        )

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            loop_service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                evaluation_version='signal_decider_eval_test',
            )
            first = calibration_service.build_report(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizon_days=5,
                evaluation_version='signal_decider_eval_test',
                min_sample_size=3,
                cluster_limit=10,
            )
            second = calibration_service.build_report(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizon_days=5,
                evaluation_version='signal_decider_eval_test',
                min_sample_size=3,
                cluster_limit=10,
            )

        first_ids = [candidate['candidate_id'] for candidate in first['rule_candidates']['items']]
        second_ids = [candidate['candidate_id'] for candidate in second['rule_candidates']['items']]
        self.assertTrue(first_ids)
        self.assertEqual(first_ids, second_ids)
        self.assertEqual(len(first_ids), len(set(first_ids)))


if __name__ == '__main__':
    unittest.main()
