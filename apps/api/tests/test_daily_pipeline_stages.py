import hashlib
import json
import threading
import unittest
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.exc import IntegrityError

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import (
    FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION,
    FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)
from app.core.settings import settings
from app.eod.daily_pipeline_outcomes import daily_pipeline_outcome_is_satisfied
from app.eod.daily_pipeline_state import _build_stage_duration_details
from app.eod.daily_pipeline_stages import (
    CURRENT_FEEDBACK_PREREQUISITE_STAGE_ORDER,
    FACTOR_STAGE_ORDER,
    STAGE_DEFINITIONS,
    DailyPipelineScopeFinalizationError,
    DailyPipelineStageCheckpointError,
    DailyPipelineStageService,
)
from app.eod.pykrx_pipeline_runner import (
    DailyPipelineTrackingWriteError,
    _build_exact_scope_outcome,
    _build_pykrx_invocation_scope_proof,
)


def _feedback_prerequisites(*, status: str = 'ready', missing_stage_ids: list[str] | None = None) -> dict:
    missing_stage_ids = list(missing_stage_ids or [])
    ready = status == 'ready'
    return {
        'market_date': '2026-04-14',
        'fs_div': 'CFS',
        'status': status,
        'ready': ready,
        'required_stage_ids': ['fundamentals_daily_refresh', 'market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER, 'signal_features'],
        'ready_stage_ids': [] if not ready else ['fundamentals_daily_refresh', 'market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER, 'signal_features'],
        'waiting_stage_ids': missing_stage_ids if status == 'waiting' else [],
        'blocked_stage_ids': missing_stage_ids if status == 'blocked' else [],
        'missing_prerequisites': [
            {
                'stage': stage_id,
                'label': stage_id.replace('_', ' '),
                'status': 'running' if status == 'waiting' else 'partial',
                'gate_status': status,
                'current_activity': f'{status} {stage_id}',
                'details': {},
            }
            for stage_id in missing_stage_ids
        ],
        'stages': {},
    }


def _decision_identity(
    *,
    symbol: str,
    symbol_index: int,
    role: str,
) -> dict:
    role_offset = 1 if role == 'short-term-decider' else 2
    return {
        'symbol': symbol,
        'decision_agent_name': role,
        'decision_snapshot_id': (symbol_index * 100) + role_offset,
        'decision_run_id': f'decision-run-{role}-{symbol}',
    }


def _decision_identities(symbols: list[str]) -> list[dict]:
    return sorted(
        [
            _decision_identity(
                symbol=symbol,
                symbol_index=symbol_index,
                role=role,
            )
            for symbol_index, symbol in enumerate(symbols, start=1)
            for role in DECISION_SUPPORT_AGENT_NAMES
        ],
        key=lambda identity: (
            identity['symbol'],
            identity['decision_agent_name'],
        ),
    )


def _complete_pykrx_universe_snapshot(
    *,
    market_date: str,
    markets: list[str],
    symbols: list[str],
) -> dict:
    symbols_by_market = {market: [] for market in markets}
    symbols_by_market[markets[0]] = list(symbols)
    membership_markets = {
        market: {
            'symbols': market_symbols,
            'symbol_count': len(market_symbols),
            'membership_sha256': hashlib.sha256(
                '\n'.join(sorted(market_symbols)).encode('utf-8')
            ).hexdigest(),
        }
        for market, market_symbols in symbols_by_market.items()
    }
    return {
        'source': 'pykrx',
        'markets': list(markets),
        'symbol_count': len(symbols),
        'persisted_count': len(symbols),
        'latest_trade_date': market_date,
        'universe_source': 'pykrx',
        'membership_basis': 'end_date_snapshot',
        'complete': True,
        'unavailable_markets': [],
        'market_membership': {
            'schema_version': 'pykrx_market_membership_v1',
            'membership_basis': 'end_date_snapshot',
            'symbol_count': len(symbols),
            'markets': membership_markets,
        },
    }


def _completed_decider_result(
    *,
    symbols: list[str],
    cutoff_at: datetime,
    source_summary_run_id: str,
    source_feedback_scope_mode: str = 'broad_feedback_universe',
    completed_scope_proof: dict | None = None,
    run_id: str = 'signal-decider-completed',
) -> dict:
    roles = list(DECISION_SUPPORT_AGENT_NAMES)
    completed_scope_proof = completed_scope_proof or {
        'schema_version': 'completed_summarizer_intersection_v1',
        'completed_symbols': list(symbols),
        'admitted_symbols': list(symbols),
    }
    role_progress = {
        role: {
            'role': role,
            'status': 'completed',
            'boundary_status': 'completed',
            'total_symbols': len(symbols),
            'processed_symbols': len(symbols),
            'completed_symbols': len(symbols),
            'terminal_symbols': len(symbols),
            'failed_items': 0,
            'in_progress_symbols': 0,
            'pending_symbols': 0,
            'retry_pending_symbols': 0,
        }
        for role in roles
    }
    return {
        'completed': True,
        'status': 'completed',
        'run_id': run_id,
        'roles': roles,
        'requested_symbols': list(symbols),
        'feedback_scope_mode': (
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
        ),
        'scope_authority': {
            'schema_version': FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION,
            'kind': FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION,
            'source_summary_run_id': source_summary_run_id,
            'source_feedback_scope_mode': source_feedback_scope_mode,
            'decision_feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            'knowledge_cutoff_at': cutoff_at.isoformat(),
            'completed_scope_proof': completed_scope_proof,
        },
        'decision_identities': _decision_identities(symbols),
        'progress': {
            'summary': {
                'run_id': run_id,
                'run_status': 'completed',
                'run_table_status': 'completed',
                'run_completed_at': cutoff_at.isoformat(),
                'is_live': False,
                'expected_feedback_count': len(symbols) * len(roles),
                'completed_feedback_count': len(symbols) * len(roles),
                'failed_items': 0,
                'role_symbol_scopes': {
                    role: list(symbols) for role in roles
                },
                'role_symbol_counts': {
                    role: len(symbols) for role in roles
                },
                'failed_symbols_by_role': {},
                'role_progress': role_progress,
            }
        },
    }


def _completed_decider_result_for_call(
    kwargs: dict,
    *,
    run_id: str = 'signal-decider-completed',
) -> dict:
    resolution = kwargs['feedback_cutoff_resolution']
    return _completed_decider_result(
        symbols=list(kwargs['symbols'] or []),
        cutoff_at=kwargs['feedback_knowledge_cutoff_at'],
        source_summary_run_id=resolution['source_run_id'],
        source_feedback_scope_mode=resolution['feedback_scope_mode'],
        completed_scope_proof=resolution['completed_scope_proof'],
        run_id=run_id,
    )


def _completed_feedback_result(
    *,
    symbols: list[str],
    cutoff_at: datetime,
    run_id: str = 'summary-run',
) -> dict:
    roles = list(SUMMARIZER_AGENT_NAMES)
    role_progress = {
        role: {
            'role': role,
            'status': 'completed',
            'boundary_status': 'completed',
            'total_symbols': len(symbols),
            'processed_symbols': len(symbols),
            'completed_symbols': len(symbols),
            'terminal_symbols': len(symbols),
            'failed_items': 0,
            'in_progress_symbols': 0,
            'pending_symbols': 0,
            'retry_pending_symbols': 0,
        }
        for role in roles
    }
    return {
        'completed': True,
        'status': 'completed',
        'run_id': run_id,
        'roles': roles,
        'requested_symbols': list(symbols),
        'knowledge_cutoff_at': cutoff_at.isoformat(),
        'feedback_scope_mode': 'broad_feedback_universe',
        'progress': {
            'summary': {
                'run_id': run_id,
                'run_status': 'completed',
                'run_table_status': 'completed',
                'run_completed_at': cutoff_at.isoformat(),
                'is_live': False,
                'expected_feedback_count': len(symbols) * len(roles),
                'completed_feedback_count': len(symbols) * len(roles),
                'failed_items': 0,
                'role_symbol_scopes': {
                    role: list(symbols) for role in roles
                },
                'role_symbol_counts': {
                    role: len(symbols) for role in roles
                },
                'failed_symbols_by_role': {},
                'role_progress': role_progress,
            }
        },
    }


def _terminal_decider_completion_proof(
    *,
    symbols: list[str],
    cutoff_at: datetime,
    source_summary_run_id: str = 'summary-run',
    run_id: str = 'signal-decider-completed',
) -> dict:
    completed_scope_proof = {
        'schema_version': 'completed_summarizer_intersection_v1',
        'completed_symbols': list(symbols),
        'admitted_symbols': list(symbols),
    }
    proof = DailyPipelineStageService._terminal_decider_progression_proof(
        {
            'status': 'completed',
            'result': _completed_decider_result(
                symbols=symbols,
                cutoff_at=cutoff_at,
                source_summary_run_id=source_summary_run_id,
                completed_scope_proof=completed_scope_proof,
                run_id=run_id,
            ),
        },
        expected_decider_symbols=symbols,
        feedback_cutoff_resolution={
            'ready': True,
            'knowledge_cutoff_at': cutoff_at,
            'source_run_id': source_summary_run_id,
            'feedback_scope_mode': 'broad_feedback_universe',
            'requested_symbols': list(symbols),
            'completed_scope_proof': completed_scope_proof,
        },
    )
    assert proof is not None
    return proof


def _completed_evaluation_result(
    *,
    symbols: list[str],
    horizons: list[int] | None = None,
) -> dict:
    horizons = list(horizons or [1, 3])
    items = []
    for symbol_index, symbol in enumerate(symbols, start=1):
        signal_identity = _decision_identity(
            symbol=symbol,
            symbol_index=symbol_index,
            role='signal-decider',
        )
        snapshot_id = signal_identity['decision_snapshot_id']
        decision_action = 'buy' if symbol_index == 1 else 'hold'
        items.append(
            {
                'snapshot_id': snapshot_id,
                'feedback_id': 200 + symbol_index,
                'decision_run_id': signal_identity['decision_run_id'],
                'agent_name': 'signal-decider',
                'symbol': symbol,
                'market_date': '2026-04-14',
                'policy_version': 'policy-v1',
                'decision': {'action': decision_action},
                'outcomes': [
                    {
                        'id': 1_000 + symbol_index * 10 + horizon,
                        'decision_snapshot_id': snapshot_id,
                        'horizon_days': horizon,
                        'decision_action': decision_action,
                    }
                    for horizon in horizons
                ],
                'evaluations': [
                    {
                        'id': 2_000 + symbol_index * 10 + horizon,
                        'decision_snapshot_id': snapshot_id,
                        'horizon_days': horizon,
                        'decision_action': decision_action,
                        'evaluation_version': 'evaluation-v1',
                        'evaluation_status': 'computed',
                    }
                    for horizon in horizons
                ],
            }
        )
    item_count = len(items) * len(horizons)
    return {
        'status': 'completed',
        'request': {
            'symbols': list(symbols),
            'horizons': horizons,
            'evaluation_version': 'evaluation-v1',
            'evaluation_rule_set_version': 'rule-set-v1',
        },
        'summary': {
            'status': 'completed',
            'decisions_considered': len(items),
            'outcomes_upserted': item_count,
            'evaluations_upserted': item_count,
            'computed_count': item_count,
            'pending_maturity_count': 0,
            'insufficient_price_data_count': 0,
            'not_computed_count': 0,
            'graded_count': item_count,
            'pending_count': 0,
            'warning_count': 0,
            'warnings': [],
            'coverage': {
                'count': item_count,
                'computed_count': item_count,
                'pending_maturity_count': 0,
                'insufficient_price_data_count': 0,
                'not_computed_count': 0,
            },
        },
        'items': items,
    }


def _next_open_completion_result(
    *,
    parent_run_id: str,
    completed_symbols: list[str],
    manifest_symbols: list[str] | None = None,
) -> dict:
    manifest_symbols = list(manifest_symbols or completed_symbols)
    completed_scope = set(completed_symbols)
    items = []
    partition = []
    for index, symbol in enumerate(manifest_symbols, start=1):
        if symbol not in completed_scope:
            for role in DECISION_SUPPORT_AGENT_NAMES:
                partition.append(
                    {
                        'symbol': symbol,
                        'decision_agent_name': role,
                        'decision_run_id': f'{parent_run_id}-{role}-{index}',
                        'decision_snapshot_id': None,
                        'canonical_action': None,
                        'authority_fingerprint': None,
                        'generation_fingerprint': None,
                        'disposition': 'incomplete',
                        'included_items': [],
                        'exclusion': None,
                    }
                )
            continue
        plan_item_id = index
        short_identity = _decision_identity(
            symbol=symbol,
            symbol_index=index,
            role='short-term-decider',
        )
        signal_identity = _decision_identity(
            symbol=symbol,
            symbol_index=index,
            role='signal-decider',
        )
        signal_snapshot_id = signal_identity['decision_snapshot_id']
        signal_run_id = signal_identity['decision_run_id']
        items.append(
            {
                'plan_item_id': plan_item_id,
                'account_reference': 'paper-primary',
                'symbol': symbol,
                'decision_agent_name': 'signal-decider',
                'decision_run_id': signal_run_id,
                'decision_snapshot_id': signal_snapshot_id,
                'eligibility_status': 'eligible',
                'handoff_active': True,
                'exec_monitoring_active': True,
            }
        )
        partition.extend(
            [
                {
                    'symbol': symbol,
                    'decision_agent_name': 'short-term-decider',
                    'decision_run_id': short_identity['decision_run_id'],
                    'decision_snapshot_id': short_identity[
                        'decision_snapshot_id'
                    ],
                    'canonical_action': 'wait',
                    'authority_fingerprint': f'authority-short-{index}',
                    'generation_fingerprint': f'generation-short-{index}',
                    'disposition': 'non_actionable',
                    'included_items': [],
                    'exclusion': {
                        'kind': 'non_actionable',
                        'reason_code': 'latest_snapshot_not_actionable',
                    },
                },
                {
                    'symbol': symbol,
                    'decision_agent_name': 'signal-decider',
                    'decision_run_id': signal_run_id,
                    'decision_snapshot_id': signal_snapshot_id,
                    'canonical_action': 'buy',
                    'authority_fingerprint': f'authority-signal-{index}',
                    'generation_fingerprint': f'generation-signal-{index}',
                    'disposition': 'included',
                    'included_items': [
                        {
                            'plan_item_id': plan_item_id,
                            'account_reference': 'paper-primary',
                            'eligibility_status': 'eligible',
                            'handoff_active': True,
                            'exec_monitoring_active': True,
                            'row_authority_fingerprint': (
                                f'row-authority-{index}'
                            ),
                        }
                    ],
                    'exclusion': None,
                },
            ]
        )
    partition.sort(
        key=lambda entry: (
            entry['symbol'],
            entry['decision_agent_name'],
        )
    )
    manifest = {
        'schema_version': 'next_open_completion_manifest_v1',
        'parent_run_id': parent_run_id,
        'authority_fingerprint': f'authority-{parent_run_id}',
        'generation_fingerprint': f'generation-{parent_run_id}',
        'role_symbol_scopes': {
            role: list(manifest_symbols)
            for role in DECISION_SUPPORT_AGENT_NAMES
        },
        'decision_partition': partition,
        'decision_count': len(partition),
        'included_decision_count': len(completed_symbols),
        'excluded_decision_count': len(completed_symbols),
        'incomplete_decision_count': (
            (len(manifest_symbols) - len(completed_symbols))
            * len(DECISION_SUPPORT_AGENT_NAMES)
        ),
        'included_item_count': len(items),
    }
    manifest['manifest_digest'] = hashlib.sha256(
        json.dumps(
            manifest,
            ensure_ascii=True,
            sort_keys=True,
            separators=(',', ':'),
        ).encode('utf-8')
    ).hexdigest()
    return {
        'status': 'completed',
        'completion_manifest': manifest,
        'items': items,
    }


class _FakePykrxStageRunner:
    def __init__(self, *, events: list[str] | None = None) -> None:
        self.calls: list[dict] = []
        self.events = events

    def run_daily_pipeline_stages(self, **kwargs):
        self.calls.append(kwargs)
        if self.events is not None:
            self.events.extend(
                ['pykrx_universe_acquired', 'price_collection_started']
            )
        symbols = ['005930', '000660']
        return {
            'run_id': 901,
            'status': 'completed',
            'latest_available_trade_date': kwargs['market_date'],
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'bars_upserted': 2,
            'universe_snapshot': _complete_pykrx_universe_snapshot(
                market_date=kwargs['market_date'],
                markets=list(kwargs['markets']),
                symbols=symbols,
            ),
            **_build_pykrx_invocation_scope_proof(symbols),
            **_build_exact_scope_outcome(
                selected_symbols=symbols,
                successful_symbols=symbols,
                retry_debt_symbols=[],
            ),
            'technical_snapshots': {
                'market_date': kwargs['market_date'],
                'snapshots_persisted': 2,
                'snapshots_failed': 0,
            },
        }


class _FakeFeedbackRetryService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def run_retry_cycles(
        self,
        *,
        config,
        force: bool = False,
        bypass_prerequisite_gate: bool = False,
    ):
        self.calls.append(
            {
                'config': config,
                'force': force,
                'bypass_prerequisite_gate': bypass_prerequisite_gate,
            }
        )
        if config.progress_callback is not None:
            config.progress_callback(
                status='running',
                current_step='role_started',
                current_activity='Agent feedback role started',
                processed=1,
                total=4,
                succeeded=1,
                failed=0,
                current_period=config.roles[0],
                source_run_ids={'agent_feedback_run_id': 'fake-feedback-run'},
            )
        symbols = list(config.symbols or [])
        roles = list(config.roles)
        role_scopes = {
            role: list(
                (config.role_symbol_scopes or {}).get(role, symbols)
            )
            for role in roles
        }
        role_progress = {
            role: {
                'role': role,
                'status': 'completed',
                'boundary_status': 'completed',
                'total_symbols': len(role_scopes[role]),
                'processed_symbols': len(role_scopes[role]),
                'completed_symbols': len(role_scopes[role]),
                'terminal_symbols': len(role_scopes[role]),
                'failed_items': 0,
                'in_progress_symbols': 0,
                'pending_symbols': 0,
                'retry_pending_symbols': 0,
            }
            for role in roles
        }
        return {
            'completed': True,
            'status': 'completed',
            'final_pending': 0,
            'run_id': 'fake-feedback-run',
            'roles': roles,
            'requested_symbols': symbols,
            'knowledge_cutoff_at': config.knowledge_cutoff_at.isoformat(),
            'feedback_scope_mode': config.feedback_scope_mode,
            'scope_authority': (
                dict(config.scope_authority)
                if config.scope_authority is not None
                else None
            ),
            **(
                {'decision_identities': _decision_identities(symbols)}
                if set(roles) == set(DECISION_SUPPORT_AGENT_NAMES)
                else {}
            ),
            'progress': {
                'summary': {
                    'run_id': 'fake-feedback-run',
                    'run_status': 'completed',
                    'run_table_status': 'completed',
                    'run_completed_at': (
                        config.knowledge_cutoff_at.isoformat()
                    ),
                    'is_live': False,
                    'expected_feedback_count': sum(
                        len(scope) for scope in role_scopes.values()
                    ),
                    'completed_feedback_count': sum(
                        len(scope) for scope in role_scopes.values()
                    ),
                    'failed_items': 0,
                    'role_symbol_scopes': role_scopes,
                    'role_symbol_counts': {
                        role: len(scope)
                        for role, scope in role_scopes.items()
                    },
                    'failed_symbols_by_role': {},
                    'role_progress': role_progress,
                }
            },
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


class _FakePipelineRunStateService:
    def __init__(self) -> None:
        self.next_run_id = 9000
        self.runs: list[dict] = []
        self.stage_events: list[dict] = []
        self.heartbeat_results: list[bool] = []
        self.heartbeat_event = threading.Event()
        self.ownership_lost_event = threading.Event()

    def start_run(self, **kwargs):
        run_id = self.next_run_id
        self.next_run_id += 1
        payload = {
            'id': run_id,
            **kwargs,
            'status': 'running',
            'stages': {},
        }
        self.runs.append(payload)
        return payload

    def start_stage(self, **kwargs):
        self.stage_events.append({'event': 'start_stage', **kwargs})
        return {'id': len(self.stage_events), **kwargs, 'status': 'running'}

    def update_stage(self, **kwargs):
        self.stage_events.append({'event': 'update_stage', **kwargs})
        return {'id': len(self.stage_events), **kwargs}

    def finish_stage(self, **kwargs):
        self.stage_events.append({'event': 'finish_stage', **kwargs})
        return {'id': len(self.stage_events), **kwargs}

    def finish_run(self, **kwargs):
        self.runs[-1]['status'] = kwargs.get('status')
        self.runs[-1]['finish'] = kwargs
        return {**self.runs[-1], **kwargs}

    def heartbeat_stage(self, **kwargs):
        self.stage_events.append({'event': 'heartbeat_stage', **kwargs})
        self.heartbeat_event.set()
        if self.heartbeat_results:
            result = self.heartbeat_results.pop(0)
            if result is False:
                self.ownership_lost_event.set()
            return result
        return True


class _FakeDeduplicatedRecoveryRunStateService(_FakePipelineRunStateService):
    def start_run(self, **kwargs):
        return {
            'id': 315,
            'market_date': kwargs['market_date'],
            'trigger': 'monitor_stale_continuation',
            'status': 'completed',
            'stages': {'signal_decider_evaluations': {'status': 'completed'}},
            'deduplicated': True,
            'deduplication_reason': 'recovery_already_completed',
        }


class _FakeTerminalContinuationRunStateService(_FakePipelineRunStateService):
    def __init__(
        self,
        *,
        claimed: bool = True,
        claim_error: Exception | None = None,
        persisted_stage_outcomes: dict | None = None,
    ) -> None:
        super().__init__()
        self.claimed = claimed
        self.claim_error = claim_error
        self.persisted_stage_outcomes = dict(
            persisted_stage_outcomes or {}
        )
        self.claim_calls: list[dict] = []
        self.start_run_calls: list[dict] = []
        self.finish_calls: list[dict] = []

    def start_run(self, **kwargs):
        self.start_run_calls.append(kwargs)
        raise AssertionError('terminal continuation must not create a new run')

    def claim_terminal_continuation(self, **kwargs):
        self.claim_calls.append(kwargs)
        if self.claim_error is not None:
            raise self.claim_error
        if not self.claimed:
            return {
                'claimed': False,
                'reason': 'terminal_continuation_already_claimed',
                'pipeline_run_id': kwargs['pipeline_run_id'],
            }
        return {
            'claimed': True,
            'reason': kwargs['reason'],
            'pipeline_run_id': kwargs['pipeline_run_id'],
            'persisted_stage_outcomes': self.persisted_stage_outcomes,
            'knowledge_cutoff_resolution': {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': datetime(
                    2026,
                    4,
                    14,
                    8,
                    30,
                    tzinfo=timezone.utc,
                ),
                'source_run_id': 'feedback-cycle-summary',
                'feedback_scope_mode': 'broad_feedback_universe',
                'requested_symbols': ['005930'],
                'completed_scope_proof': {
                    'schema_version': (
                        'completed_summarizer_intersection_v1'
                    ),
                    'completed_symbols': ['005930'],
                    'admitted_symbols': ['005930'],
                },
            },
            'pipeline_run': {
                'id': kwargs['pipeline_run_id'],
                'status': 'running',
                'stages': {},
                'run_metadata': {
                    'symbols': ['005930'],
                    'markets': ['KOSPI'],
                    'force': False,
                    'fs_div': 'CFS',
                    'symbol_limit': None,
                    'symbol_offset': 0,
                    'capture_limit': 500,
                    'retry_cycles': None,
                    'feedback_max_cycles': 3,
                    'feedback_outer_batch_size': 20,
                    'reuse_existing_feedback': True,
                    'bypass_prerequisite_gate': False,
                    'point_in_time_recovery': True,
                    'stage_statuses': {
                        'price_collection': 'partial',
                        'macro_context_refresh': 'completed',
                        'agent_feedback_summaries': 'pending',
                        'signal_decider': 'pending',
                        'next_open_execution_plan': 'pending',
                        'signal_decider_evaluations': 'pending',
                    },
                    'terminal_continuation_evidence': {
                        'parent': {
                            'stage_statuses': {
                                'price_collection': 'partial',
                                'macro_context_refresh': 'completed',
                                'agent_feedback_summaries': 'completed',
                                'signal_decider': 'failed',
                                'next_open_execution_plan': 'blocked',
                                'signal_decider_evaluations': 'blocked',
                            },
                        },
                        'suffix_stages': {
                            'agent_feedback_summaries': {
                                'source_run_ids': {
                                    'run_id': 'feedback-cycle-summary'
                                },
                            },
                        },
                    },
                },
            },
        }

    def finish_run(self, **kwargs):
        self.finish_calls.append(kwargs)
        return {
            'id': kwargs['pipeline_run_id'],
            'status': kwargs['status'],
            'run_metadata': kwargs.get('run_metadata') or {},
        }


class _FakeSameDayRearmRunStateService(_FakePipelineRunStateService):
    def __init__(self, *, proof: dict) -> None:
        super().__init__()
        self.proof = dict(proof)
        self.claim_calls: list[dict] = []
        self.start_run_calls: list[dict] = []
        self.finish_calls: list[dict] = []

    def start_run(self, **kwargs):
        self.start_run_calls.append(kwargs)
        raise AssertionError('same-day rearm must resume the existing run')

    def claim_same_day_intraday_watch_rearm_resume(self, **kwargs):
        self.claim_calls.append(kwargs)
        return {
            'claimed': True,
            'reason': 'same_day_intraday_watch_rearm_resumed',
            'pipeline_run_id': kwargs['pipeline_run_id'],
            'summary_cutoff_proof': self.proof,
            'persisted_stage_outcomes': {
                'signal_decider': {
                    'stage': 'signal_decider',
                    'status': 'completed',
                    'processed': 160,
                    'total': 160,
                    'succeeded': 160,
                    'failed': 0,
                    'remaining': 0,
                    'source_run_ids': {
                        'agent_feedback_run_id': 'decider-completed'
                    },
                    'details': {
                        'result_summary': {'status': 'completed'}
                    },
                }
            },
            'pipeline_run': {
                'id': kwargs['pipeline_run_id'],
                'status': 'running',
                'stages': {},
                'run_metadata': {
                    'symbols': None,
                    'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                    'force': False,
                    'fs_div': 'CFS',
                    'reuse_existing_feedback': True,
                    'point_in_time_recovery': True,
                    'same_day_intraday_watch_continuation_admitted': True,
                },
            },
        }

    def finish_run(self, **kwargs):
        self.finish_calls.append(kwargs)
        return {
            'id': kwargs['pipeline_run_id'],
            'status': kwargs['status'],
            'run_metadata': kwargs.get('run_metadata') or {},
        }


class _FakeCutoffContractRepairRunStateService(
    _FakePipelineRunStateService
):
    def __init__(self) -> None:
        super().__init__()
        self.claim_calls: list[dict] = []
        self.start_run_calls: list[dict] = []
        self.finish_calls: list[dict] = []

    def start_run(self, **kwargs):
        self.start_run_calls.append(kwargs)
        raise AssertionError('cutoff repair resume must not create a new run')

    def claim_summary_cutoff_contract_repair_resume(self, **kwargs):
        self.claim_calls.append(kwargs)
        proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
        }
        return {
            'claimed': True,
            'resumed': True,
            'pipeline_run_id': kwargs['pipeline_run_id'],
            'summary_cutoff_proof': proof,
            'knowledge_cutoff_resolution': {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': datetime(
                    2026,
                    4,
                    14,
                    8,
                    30,
                    tzinfo=timezone.utc,
                ),
                'source_run_id': 'feedback-cycle-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['005930'],
                'completed_scope_proof': {
                    'schema_version': (
                        'completed_summarizer_intersection_v1'
                    ),
                    'completed_symbols': ['005930'],
                    'admitted_symbols': ['005930'],
                },
            },
            'pipeline_run': {
                'id': kwargs['pipeline_run_id'],
                'status': 'running',
                'stages': {},
                'run_metadata': {
                    'symbols': None,
                    'markets': ['KOSPI'],
                    'force': False,
                    'fs_div': 'CFS',
                    'symbol_limit': None,
                    'symbol_offset': 0,
                    'capture_limit': 500,
                    'retry_cycles': None,
                    'feedback_max_cycles': 3,
                    'feedback_outer_batch_size': 20,
                    'reuse_existing_feedback': True,
                    'bypass_prerequisite_gate': False,
                    'point_in_time_recovery': False,
                    'stage_statuses': {
                        'signal_decider': 'pending',
                        'next_open_execution_plan': 'pending',
                        'signal_decider_evaluations': 'pending',
                    },
                    'summary_cutoff_contract_repair_version': 1,
                    'summary_cutoff_contract_repair_proof': proof,
                },
            },
        }

    def finish_run(self, **kwargs):
        self.finish_calls.append(kwargs)
        return {
            'id': kwargs['pipeline_run_id'],
            'status': kwargs['status'],
            'run_metadata': kwargs.get('run_metadata') or {},
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


class _FakePartiallySuccessfulPykrxStageRunner:
    def run_daily_pipeline_stages(self, **kwargs):
        symbols = ['005930', '000660']
        return {
            'run_id': 902,
            'status': 'completed_with_errors',
            'latest_available_trade_date': kwargs['market_date'],
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'bars_upserted': 2,
            **_build_pykrx_invocation_scope_proof(symbols),
            **_build_exact_scope_outcome(
                selected_symbols=symbols,
                successful_symbols=symbols,
                retry_debt_symbols=[],
            ),
            'technical_snapshots': {
                'market_date': kwargs['market_date'],
                'status': 'partial',
                'snapshots_persisted': 1,
                'snapshots_failed': 1,
            },
        }


class _FakeExactScopePykrxStageRunner:
    def __init__(
        self,
        *,
        invocation_proof: str,
        selected_symbols: list[str] | None = None,
        successful_symbols: list[str] | None = None,
        retry_debt_symbols: list[str] | None = None,
    ) -> None:
        self.invocation_proof = invocation_proof
        self.selected_symbols = list(
            selected_symbols or ['005930', '000660']
        )
        self.successful_symbols = list(
            successful_symbols or ['005930']
        )
        self.retry_debt_symbols = list(
            retry_debt_symbols or ['000660']
        )
        self.calls: list[dict] = []

    def run_daily_pipeline_stages(self, **kwargs):
        self.calls.append(kwargs)
        result = {
            'run_id': 903,
            'status': 'completed_with_errors',
            'latest_available_trade_date': kwargs['market_date'],
            'symbols_total': len(self.selected_symbols),
            'symbols_succeeded': len(self.successful_symbols),
            'symbols_failed': len(self.retry_debt_symbols),
            'failed_symbols': list(self.retry_debt_symbols),
            'bars_upserted': len(self.successful_symbols),
            **_build_exact_scope_outcome(
                selected_symbols=self.selected_symbols,
                successful_symbols=self.successful_symbols,
                retry_debt_symbols=self.retry_debt_symbols,
            ),
        }
        if self.invocation_proof != 'missing':
            result.update(
                _build_pykrx_invocation_scope_proof(
                    self.selected_symbols
                )
            )
        if self.invocation_proof == 'mismatched':
            result['invocation_scope_sha256'] = '0' * 64
        return result


class DailyPipelineStageServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.run_state_service = _FakePipelineRunStateService()
        self.run_state_patcher = patch(
            'app.eod.daily_pipeline_stages.daily_pipeline_run_state_service',
            self.run_state_service,
        )
        self.run_state_patcher.start()
        self.addCleanup(self.run_state_patcher.stop)
        self.prerequisite_patcher = patch(
            'app.eod.daily_pipeline_stages.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            side_effect=lambda **_: _feedback_prerequisites(),
        )
        self.prerequisite_patcher.start()
        self.addCleanup(self.prerequisite_patcher.stop)
        self.market_state_patcher = patch(
            'app.eod.daily_pipeline_stages.get_market_state',
            return_value=MagicMock(
                state='CLOSED',
                server_time='2026-04-14T16:00:00+09:00',
            ),
        )
        self.market_state_patcher.start()
        self.addCleanup(self.market_state_patcher.stop)

    def _execute_pykrx_enrichment(
        self,
        *,
        collection_result: dict,
        preflight: dict,
        symbols: list[str] | None = None,
    ) -> tuple[DailyPipelineStageService, dict]:
        invocation_symbols = symbols or ['005930']
        prepared_result = dict(collection_result)
        coverage = dict(prepared_result.get('coverage') or {})
        coverage_categories = dict(coverage.get('categories') or {})
        for category in preflight.get('required_categories') or []:
            coverage_categories.setdefault(
                category,
                {
                    'expected_scope_keys': list(invocation_symbols),
                    'complete_scope_keys': list(invocation_symbols),
                },
            )
        coverage['categories'] = coverage_categories
        prepared_result['coverage'] = coverage
        enrichment_service = MagicMock()
        enrichment_service.collect_for_market_date.return_value = (
            prepared_result
        )
        service = DailyPipelineStageService(
            pykrx_enrichment_service_ref=enrichment_service,
        )
        with patch(
            'app.eod.daily_pipeline_stages.build_pykrx_enrichment_preflight',
            return_value=preflight,
        ):
            result = service._execute_stage(
                stage_id='pykrx_enrichment',
                market_date='2026-04-14',
                symbols=invocation_symbols,
                markets=['KOSPI'],
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )
        return service, result

    def test_describe_returns_canonical_stage_order_and_default_sequence(self) -> None:
        service = DailyPipelineStageService()

        payload = service.describe()

        self.assertEqual(
            payload['stage_order'][:7],
            [
                'master_refresh',
                'pykrx_universe_refresh',
                'price_collection',
                'pykrx_enrichment',
                'technical_snapshots',
                'action_summaries',
                'trending_ranks',
            ],
        )

        self.assertIn('fundamentals_daily_refresh', payload['default_daily_stage_order'])
        self.assertIn('momentum_factors', payload['default_daily_stage_order'])
        self.assertIn('composite_factors', payload['default_daily_stage_order'])
        self.assertIn('external_news_collect', payload['default_daily_stage_order'])
        self.assertIn('external_news_enrich', payload['default_daily_stage_order'])
        self.assertIn('macro_context_refresh', payload['default_daily_stage_order'])
        self.assertLess(
            payload['default_daily_stage_order'].index('composite_factors'),
            payload['default_daily_stage_order'].index('signal_features'),
        )
        self.assertLess(
            payload['default_daily_stage_order'].index('signal_features'),
            payload['default_daily_stage_order'].index('external_news_collect'),
        )
        self.assertLess(
            payload['default_daily_stage_order'].index('external_news_collect'),
            payload['default_daily_stage_order'].index('external_news_enrich'),
        )
        self.assertLess(
            payload['default_daily_stage_order'].index('external_news_enrich'),
            payload['default_daily_stage_order'].index('macro_context_refresh'),
        )
        self.assertLess(
            payload['default_daily_stage_order'].index('macro_context_refresh'),
            payload['default_daily_stage_order'].index('signal_decider'),
        )
        self.assertIn('signal_decider', payload['default_daily_stage_order'])
        self.assertLess(
            payload['default_daily_stage_order'].index('signal_decider'),
            payload['default_daily_stage_order'].index('next_open_execution_plan'),
        )
        self.assertLess(
            payload['default_daily_stage_order'].index('next_open_execution_plan'),
            payload['default_daily_stage_order'].index('signal_decider_evaluations'),
        )
        self.assertIn('unified_value_factors', payload['stage_order'])
        self.assertIn('quality_factors', payload['stage_order'])

        unified_value_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'unified_value_factors'
        )
        self.assertEqual(
            unified_value_stage['depends_on'],
            ['price_collection', 'pykrx_enrichment', 'fundamentals_daily_refresh'],
        )
        quality_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'quality_factors'
        )
        self.assertEqual(
            quality_stage['depends_on'],
            ['price_collection', 'fundamentals_daily_refresh'],
        )

        advanced_value_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'advanced_value_factors'
        )
        self.assertEqual(
            advanced_value_stage['depends_on'],
            ['price_collection', 'pykrx_enrichment', 'fundamentals_daily_refresh'],
        )
        piotroski_stage = next(
            stage for stage in payload['stages'] if stage['id'] == 'piotroski_filter'
        )
        self.assertEqual(
            piotroski_stage['depends_on'],
            ['fundamentals_daily_refresh', 'pykrx_enrichment'],
        )
        value_stage = next(
            stage for stage in payload['stages'] if stage['id'] == 'value_factors'
        )
        self.assertEqual(
            value_stage['depends_on'],
            ['price_collection', 'pykrx_enrichment'],
        )
        composite_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'composite_factors'
        )
        self.assertEqual(
            composite_stage['depends_on'],
            [
                'momentum_factors',
                'value_factors',
                'low_risk_factors',
                'beta_factors',
                'advanced_value_factors',
            ],
        )
        self.assertTrue(composite_stage['default_selected'])
        signal_feature_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'signal_features'
        )
        self.assertEqual(
            signal_feature_stage['depends_on'],
            ['market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER],
        )
        external_news_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'external_news_collect'
        )
        self.assertEqual(
            external_news_stage['depends_on'],
            ['market_risk_filter', 'piotroski_filter', *FACTOR_STAGE_ORDER],
        )
        external_news_enrich_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'external_news_enrich'
        )
        self.assertEqual(external_news_enrich_stage['depends_on'], ['external_news_collect'])
        macro_context_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'macro_context_refresh'
        )
        self.assertEqual(macro_context_stage['depends_on'], ['external_news_enrich'])
        agent_feedback_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'agent_feedback_summaries'
        )
        self.assertEqual(
            agent_feedback_stage['depends_on'],
            [
                'market_risk_filter',
                'piotroski_filter',
                *FACTOR_STAGE_ORDER,
                'signal_features',
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
            ],
        )
        signal_decider_stage = next(
            stage
            for stage in payload['stages']
            if stage['id'] == 'signal_decider'
        )
        self.assertIn(
            'signal_decider_decision_snapshots',
            signal_decider_stage['output_tables'],
        )
        self.assertTrue(external_news_stage['default_selected'])
        self.assertTrue(external_news_enrich_stage['default_selected'])
        self.assertTrue(macro_context_stage['default_selected'])

    def test_action_summaries_declares_technical_snapshot_dependency(
        self,
    ) -> None:
        self.assertEqual(
            STAGE_DEFINITIONS['action_summaries'].depends_on,
            ('technical_snapshots',),
        )

    def test_stage_duration_details_marks_slow_stage(self) -> None:
        started_at = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)
        completed_at = datetime(2026, 7, 21, 9, 6, 1, 123000, tzinfo=timezone.utc)

        details = _build_stage_duration_details(started_at=started_at, completed_at=completed_at)

        self.assertEqual(details['duration_ms'], 361123)
        self.assertEqual(details['duration_seconds'], 361.123)
        self.assertTrue(details['slow_stage'])
        self.assertEqual(details['slow_stage_threshold_ms'], 300000)

    def test_monitor_recovery_returns_existing_run_without_executing_stages(self) -> None:
        pykrx_runner = _FakePykrxStageRunner()
        service = DailyPipelineStageService(
            pykrx_service=pykrx_runner,
            run_state_service=_FakeDeduplicatedRecoveryRunStateService(),
        )

        result = service.run(
            market_date='2026-08-21',
            stages=['price_collection'],
            trigger='monitor_stale_continuation',
            force=True,
        )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'recovery_already_completed')
        self.assertEqual(result['pipeline_run_id'], 315)
        self.assertEqual(pykrx_runner.calls, [])

    def test_recovery_threads_run_id_but_blocks_global_trending_without_full_scope(self) -> None:
        service = DailyPipelineStageService()

        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 2,
                    'contexts_failed': 0,
                    'items': [],
                    'scope_finalization': {'ready': True},
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930', '000660'],
                        successful_symbols=['005930', '000660'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as rebuild_contexts,
            patch(
                'app.eod.daily_pipeline_stages.daily_technical_snapshot_service.'
                'rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 2,
                    'snapshots_failed': 0,
                    'scope_finalization': {'ready': True},
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930', '000660'],
                        successful_symbols=['005930', '000660'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as rebuild_snapshots,
            patch(
                'app.eod.daily_pipeline_stages.daily_action_summary_service.'
                'rebuild_for_market_date',
                return_value={
                    'action_summaries_persisted': 2,
                    'action_summaries_failed': 0,
                    'scope_finalization': {'ready': True},
                },
            ) as rebuild_action_summaries,
            patch(
                'app.eod.daily_pipeline_stages.'
                'compute_and_persist_daily_trending_ranks',
                return_value={
                    'trending_ranks_persisted': 6,
                    'periods_persisted': 3,
                },
            ) as rebuild_trending_ranks,
            patch(
                'app.eod.daily_pipeline_stages._finalize_daily_stage_scope',
                return_value={
                    'status': 'verified',
                    'stage_id': 'technical_snapshots',
                    'remaining_symbols': ['005930', '000660'],
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-24',
                stages=[
                    'technical_snapshots',
                    'action_summaries',
                    'trending_ranks',
                ],
                symbols=['005930', '000660'],
                reused_eod_collection_run_id=7,
                trigger='auto_followup_recovery',
            )

        self.assertEqual(result['status'], 'partial')
        expected_symbol_scope = {
            'market_date': '2026-04-24',
            'symbols': ['005930', '000660'],
            'exact_scope_symbols': ['005930', '000660'],
            'source': 'stage_orchestrator',
            'run_id': 7,
        }
        rebuild_contexts.assert_called_once_with(**expected_symbol_scope)
        rebuild_snapshots.assert_called_once_with(**expected_symbol_scope)
        rebuild_action_summaries.assert_called_once_with(
            **expected_symbol_scope
        )
        rebuild_trending_ranks.assert_not_called()

    def test_mixed_failed_blocked_and_completed_status_is_partial(
        self,
    ) -> None:
        self.assertEqual(
            DailyPipelineStageService._resolve_overall_status(
                [
                    {'status': 'failed'},
                    {'status': 'blocked'},
                    {'status': 'completed'},
                ]
            ),
            'partial',
        )

    def test_pure_failed_status_remains_failed(self) -> None:
        self.assertEqual(
            DailyPipelineStageService._resolve_overall_status(
                [{'status': 'failed'}]
            ),
            'failed',
        )

    def test_mixed_terminal_incomplete_with_completed_status_is_partial(
        self,
    ) -> None:
        for incomplete_status in (
            'blocked',
            'waiting',
            'interrupted',
            'abandoned',
        ):
            with self.subTest(incomplete_status=incomplete_status):
                self.assertEqual(
                    DailyPipelineStageService._resolve_overall_status(
                        [
                            {'status': incomplete_status},
                            {'status': 'completed'},
                        ]
                    ),
                    'partial',
                )

    def test_pure_terminal_incomplete_statuses_remain_specific(self) -> None:
        for status in ('blocked', 'waiting', 'interrupted', 'abandoned'):
            with self.subTest(status=status):
                self.assertEqual(
                    DailyPipelineStageService._resolve_overall_status(
                        [{'status': status}, {'status': status}]
                    ),
                    status,
                )

    def test_all_pending_status_remains_pending(self) -> None:
        self.assertEqual(
            DailyPipelineStageService._resolve_overall_status(
                [{'status': 'pending'}, {'status': 'pending'}]
            ),
            'pending',
        )

    def test_malformed_or_generic_skipped_outcomes_are_not_completed(
        self,
    ) -> None:
        for outcomes in (
            [{'status': 'completed'}, {}],
            [{'status': 'completed'}, {'status': 'skipped'}],
            [{'status': 'skipped'}],
        ):
            with self.subTest(outcomes=outcomes):
                self.assertEqual(
                    DailyPipelineStageService._resolve_overall_status(
                        outcomes
                    ),
                    'partial',
                )

    def test_explicit_safe_skip_is_terminally_satisfied(self) -> None:
        self.assertEqual(
            DailyPipelineStageService._resolve_overall_status(
                [
                    {'stage': 'value_factors', 'status': 'completed'},
                    {
                        'stage': 'signal_decider',
                        'status': 'skipped',
                        'safe_skip': True,
                    },
                ]
            ),
            'completed',
        )

    def test_trending_fails_closed_for_narrowed_or_unproven_price_scope(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        exact_membership = ['005930', '000660']
        base_kwargs = {
            'stage_id': 'trending_ranks',
            'market_date': '2026-04-14',
            'markets': ['KOSPI', 'KOSDAQ'],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
            'exact_scope_symbols': exact_membership,
        }

        with patch(
            'app.eod.daily_pipeline_stages.'
            'compute_and_persist_daily_trending_ranks'
        ) as persist_trending:
            narrowed = service._execute_stage(
                **base_kwargs,
                symbols=['005930'],
                complete_price_scope_proven=True,
            )
            unproven = service._execute_stage(
                **base_kwargs,
                symbols=exact_membership,
                complete_price_scope_proven=False,
            )

        persist_trending.assert_not_called()
        for result in (narrowed, unproven):
            self.assertEqual(result['status'], 'blocked')
            self.assertEqual(
                result['reason'],
                'complete_price_scope_authority_unavailable',
            )
            self.assertEqual(
                result['retry_debt_stock_codes'],
                exact_membership,
            )
            self.assertEqual(
                result['_retry_metadata']['failed_symbols'],
                exact_membership,
            )
        self.assertEqual(narrowed['admitted_stock_codes'], ['005930'])

    def test_complete_price_scope_authority_requires_exact_exhaustive_proof(
        self,
    ) -> None:
        symbols = ['005930', '000660']
        complete_price_result = {
            'status': 'completed',
            **_build_exact_scope_outcome(
                selected_symbols=symbols,
                successful_symbols=symbols,
                retry_debt_symbols=[],
            ),
        }
        price_outcome = {
            'status': 'completed',
            'result': complete_price_result,
        }

        self.assertTrue(
            DailyPipelineStageService._complete_price_scope_is_proven(
                exact_scope_symbols=symbols,
                admitted_symbols=symbols,
                price_outcome=price_outcome,
                price_invocation_symbols=symbols,
            )
        )
        self.assertFalse(
            DailyPipelineStageService._complete_price_scope_is_proven(
                exact_scope_symbols=symbols,
                admitted_symbols=['005930'],
                price_outcome=price_outcome,
                price_invocation_symbols=symbols,
            )
        )

    def test_prior_date_recovery_skips_current_only_stages_but_runs_as_of_stages(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        call_kwargs = {
            'market_date': '2026-09-11',
            'symbols': ['099999'],
            'markets': ['KOSPI'],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
            'point_in_time_recovery': True,
        }
        current_only_stage_ids = (
            'master_refresh',
            'pykrx_universe_refresh',
            'signal_features',
            'market_risk_snapshots',
            'market_risk_filter',
            'fundamentals_daily_refresh',
            'external_news_collect',
            'external_news_enrich',
            'macro_context_refresh',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_piotroski_filter_service.'
                'rebuild_for_market_date',
                return_value={
                    'results_persisted': 1,
                    'results_failed': 0,
                    'scope_finalization': {'ready': True},
                },
            ) as rebuild_piotroski,
            patch(
                'app.eod.daily_pipeline_stages.daily_momentum_factor_service.'
                'rebuild_for_market_date',
                return_value={
                    'results_persisted': 1,
                    'results_failed': 0,
                    'scope_finalization': {'ready': True},
                },
            ) as rebuild_momentum,
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['099999'],
                        successful_symbols=['099999'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as rebuild_context,
            patch(
                'app.eod.daily_pipeline_stages.daily_technical_snapshot_service.'
                'rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 1,
                    'snapshots_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['099999'],
                        successful_symbols=['099999'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as rebuild_technical,
            patch.object(
                service.pykrx_enrichment_service,
                'collect_for_market_date',
                return_value={'status': 'completed', 'coverage': {}},
            ) as collect_enrichment,
            patch(
                'app.eod.daily_pipeline_stages._finalize_daily_stage_scope',
                return_value={
                    'status': 'verified',
                    'stage_id': 'technical_snapshots',
                    'remaining_symbols': ['099999'],
                },
            ),
        ):
            skipped_results = {
                stage_id: service._execute_stage(
                    stage_id=stage_id,
                    **call_kwargs,
                )
                for stage_id in current_only_stage_ids
            }
            piotroski_result = service._execute_stage(
                stage_id='piotroski_filter',
                **call_kwargs,
            )
            service._execute_stage(
                stage_id='pykrx_enrichment',
                **call_kwargs,
            )
            price_derived_result = service._execute_stage(
                stage_id='technical_snapshots',
                **call_kwargs,
            )

        for stage_id, result in skipped_results.items():
            with self.subTest(stage_id=stage_id):
                self.assertFalse(result['triggered'])
                self.assertTrue(result['safe_skip'])
                self.assertEqual(result['status'], 'skipped')
                self.assertEqual(
                    result['reason'],
                    'point_in_time_recovery_current_only_stage',
                )
        self.assertEqual(piotroski_result['results_persisted'], 1)
        rebuild_piotroski.assert_called_once_with(
            market_date='2026-09-11',
            symbols=['099999'],
        )
        collect_enrichment.assert_called_once_with(
            market_date='2026-09-11',
            markets=['KOSPI'],
            symbols=['099999'],
            use_full_market_coverage=True,
            point_in_time_recovery=True,
        )
        self.assertEqual(price_derived_result['snapshots_persisted'], 1)
        rebuild_context.assert_called_once_with(
            market_date='2026-09-11',
            symbols=['099999'],
            exact_scope_symbols=None,
            source='stage_orchestrator',
            run_id=None,
        )
        rebuild_technical.assert_called_once_with(
            market_date='2026-09-11',
            symbols=['099999'],
            exact_scope_symbols=None,
            source='stage_orchestrator',
            run_id=None,
        )

    def test_prior_date_recovery_runs_as_of_piotroski_before_factor_chain(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_piotroski_filter_service.'
                'rebuild_for_market_date',
                return_value={
                    'results_persisted': 1,
                    'results_failed': 0,
                    'scope_finalization': {'ready': True},
                },
            ) as rebuild_piotroski,
            patch(
                'app.eod.daily_pipeline_stages.daily_momentum_factor_service.'
                'rebuild_for_market_date',
                return_value={
                    'results_persisted': 1,
                    'results_failed': 0,
                    'scope_finalization': {'ready': True},
                },
            ) as rebuild_momentum,
        ):
            result = service.run(
                market_date='2026-09-11',
                stages=['piotroski_filter', 'momentum_factors'],
                symbols=['099999'],
                trigger='manual_api',
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'completed', result)
        self.assertEqual(
            result['stages']['piotroski_filter']['status'],
            'completed',
        )
        self.assertEqual(
            result['stages']['momentum_factors']['status'],
            'completed',
        )
        rebuild_piotroski.assert_called_once_with(
            market_date='2026-09-11',
            symbols=['099999'],
            ranking_symbols=['099999'],
            exact_scope_symbols=['099999'],
        )
        rebuild_momentum.assert_called_once_with(
            market_date='2026-09-11',
            symbols=['099999'],
            ranking_symbols=['099999'],
            exact_scope_symbols=['099999'],
        )

    def test_preopen_recovery_runs_previous_session_market_risk_stages(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        market = MagicMock(
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            current_calendar_date=None,
            server_time='2026-09-29T08:30:00+09:00',
        )
        call_kwargs = {
            'market_date': '2026-09-28',
            'symbols': ['005930'],
            'markets': ['KOSPI'],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
            'point_in_time_recovery': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.pykrx_pipeline_runner.previous_krx_trading_date',
                return_value=datetime(2026, 9, 28).date(),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_market_risk_snapshot_service.collect_for_market_date',
                new=AsyncMock(
                    return_value={
                        'snapshots_persisted': 1,
                        'snapshots_failed': 0,
                    }
                ),
            ) as collect_snapshots,
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_market_risk_filter_service.rebuild_for_market_date',
                new=AsyncMock(
                    return_value={
                        'results_persisted': 1,
                        'results_failed': 0,
                    }
                ),
            ) as rebuild_filters,
        ):
            snapshot_result = service._execute_stage(
                stage_id='market_risk_snapshots',
                **call_kwargs,
            )
            filter_result = service._execute_stage(
                stage_id='market_risk_filter',
                **call_kwargs,
            )

        self.assertEqual(snapshot_result['snapshots_persisted'], 1)
        self.assertEqual(filter_result['results_persisted'], 1)
        collect_snapshots.assert_awaited_once()
        rebuild_filters.assert_awaited_once()

    def test_production_boundaries_auto_promote_prior_date_current_only_skip(
        self,
    ) -> None:
        for trigger in (
            'manual_api',
            'manual_api_stage',
            'internal_ops_cli',
            'internal_ops_cli_stage',
        ):
            with self.subTest(trigger=trigger):
                service = DailyPipelineStageService()
                with (
                    patch(
                        'app.eod.daily_pipeline_stages.get_market_state',
                        return_value=MagicMock(
                            state='OPEN',
                            server_time='2026-09-14T10:00:00+09:00',
                        ),
                    ),
                    patch(
                        'app.eod.daily_pipeline_stages.'
                        'daily_market_risk_snapshot_service.'
                        'collect_for_market_date',
                    ) as collect_current_risk,
                ):
                    result = service.run(
                        market_date='2026-09-11',
                        stages=['market_risk_snapshots'],
                        symbols=['099999'],
                        trigger=trigger,
                    )

                self.assertEqual(result['status'], 'completed')
                stage = result['stages']['market_risk_snapshots']
                self.assertEqual(stage['status'], 'completed')
                self.assertTrue(stage['result']['safe_skip'])
                collect_current_risk.assert_not_called()

    def test_run_reorders_requested_subset_and_splits_feedback_stages(self) -> None:
        pykrx_runner = _FakePykrxStageRunner()
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        feedback_cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            pykrx_service=pykrx_runner,
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )

        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return (
                    feedback_cutoff
                    if tz is not None
                    else feedback_cutoff.replace(tzinfo=None)
                )

        original_execute_stage = service._execute_stage

        def execute_stage(stage_id, **kwargs):
            if stage_id == 'master_refresh':
                return {'status': 'completed'}
            return original_execute_stage(stage_id, **kwargs)

        with (
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute_stage,
            ) as execute_stage_mock,
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch(
                'app.eod.daily_pipeline_stages.datetime',
                new=FixedDatetime,
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': feedback_cutoff,
                    'source_run_id': 'fake-feedback-run',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['000660', '005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['000660', '005930'],
                        'admitted_symbols': ['000660', '005930'],
                    },
                },
            ),
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
                fs_div='OFS',
            )

        self.assertEqual(
            result['resolved_stages'],
            [
                'master_refresh',
                'pykrx_universe_refresh',
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
        self.assertEqual(feedback_retry_service.calls[1]['config'].roles, DECISION_SUPPORT_AGENT_NAMES)
        self.assertTrue(
            all(
                feedback_call['config'].fs_div == 'OFS'
                for feedback_call in feedback_retry_service.calls
            )
        )
        self.assertEqual(
            [
                feedback_call['config'].knowledge_cutoff_at
                for feedback_call in feedback_retry_service.calls
            ],
            [feedback_cutoff, feedback_cutoff],
        )
        self.assertTrue(evaluation_service.calls, result)
        self.assertEqual(evaluation_service.calls[0]['as_of_market_date'], '2026-04-14')
        self.assertEqual(evaluation_service.calls[0]['symbols'], ['000660', '005930'])
        self.assertGreaterEqual(execute_stage_mock.call_count, 4)

    def test_same_run_decider_uses_completed_summary_intersection(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        parent_symbols = ['000660', '005930', '035420']
        completed_intersection = ['000660', '005930']
        decider_roles = list(DECISION_SUPPORT_AGENT_NAMES)
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        def feedback_result(*, config, **_kwargs):
            feedback_retry_service.calls.append({'config': config})
            if config.roles == SUMMARIZER_AGENT_NAMES:
                return _completed_feedback_result(
                    symbols=parent_symbols,
                    cutoff_at=cutoff,
                    run_id='feedback-cycle-summary-same-run',
                )
            role_progress = {
                role: {
                    'role': role,
                    'status': 'completed',
                    'boundary_status': 'completed',
                    'total_symbols': len(completed_intersection),
                    'processed_symbols': len(completed_intersection),
                    'completed_symbols': len(completed_intersection),
                    'terminal_symbols': len(completed_intersection),
                    'failed_items': 0,
                    'in_progress_symbols': 0,
                    'pending_symbols': 0,
                    'retry_pending_symbols': 0,
                }
                for role in decider_roles
            }
            return {
                'completed': True,
                'status': 'completed',
                'final_pending': 0,
                'roles': list(config.roles),
                'requested_symbols': list(config.symbols or ()),
                'feedback_scope_mode': config.feedback_scope_mode,
                'scope_authority': config.scope_authority,
                'run_id': 'signal-decider-completed',
                'decision_identities': _decision_identities(
                    completed_intersection
                ),
                'progress': {
                    'summary': {
                        'run_id': 'signal-decider-completed',
                        'run_status': 'completed',
                        'run_table_status': 'completed',
                        'run_completed_at': cutoff.isoformat(),
                        'is_live': False,
                        'expected_feedback_count': (
                            len(completed_intersection)
                            * len(decider_roles)
                        ),
                        'completed_feedback_count': (
                            len(completed_intersection)
                            * len(decider_roles)
                        ),
                        'failed_items': 0,
                        'role_symbol_scopes': {
                            role: list(completed_intersection)
                            for role in decider_roles
                        },
                        'role_symbol_counts': {
                            role: len(completed_intersection)
                            for role in decider_roles
                        },
                        'failed_symbols_by_role': {
                            role: [] for role in decider_roles
                        },
                        'role_progress': role_progress,
                    }
                },
            }

        original_execute_stage = service._execute_stage

        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cutoff if tz is not None else cutoff.replace(tzinfo=None)

        def execute_stage(stage_id, **kwargs):
            if stage_id == 'next_open_execution_plan':
                return _next_open_completion_result(
                    parent_run_id='signal-decider-completed',
                    completed_symbols=list(kwargs['symbols']),
                )
            if stage_id == 'signal_decider_evaluations':
                return _completed_evaluation_result(
                    symbols=list(kwargs['symbols'])
                )
            return original_execute_stage(stage_id, **kwargs)

        with (
            patch.object(
                feedback_retry_service,
                'run_retry_cycles',
                side_effect=feedback_result,
            ),
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute_stage,
            ) as execute_stage_mock,
            patch(
                'app.eod.daily_pipeline_stages.datetime',
                new=FixedDatetime,
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value=_feedback_prerequisites(),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary-same-run',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': completed_intersection,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': completed_intersection,
                        'admitted_symbols': parent_symbols,
                    },
                },
            ) as resolve_cutoff,
            patch(
                'app.eod.daily_pipeline_stages.'
                'build_expired_decision_replay_result',
                return_value=None,
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                symbols=parent_symbols,
                fs_div='CFS',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(len(feedback_retry_service.calls), 2)
        self.assertEqual(
            feedback_retry_service.calls[1]['config'].symbols,
            tuple(completed_intersection),
        )
        self.assertEqual(
            feedback_retry_service.calls[1]['config'].scope_authority[
                'kind'
            ],
            'completed_summary_intersection_authoritative_cohort',
        )
        self.assertEqual(
            feedback_retry_service.calls[1]['config'].scope_authority[
                'source_summary_run_id'
            ],
            'feedback-cycle-summary-same-run',
        )
        resolve_cutoff.assert_called_once_with(
            market_date=datetime(2026, 4, 14).date(),
            feedback_scope_mode='broad_feedback_universe',
            fs_div='CFS',
            source_run_id='feedback-cycle-summary-same-run',
            requested_symbols=parent_symbols,
        )
        completed_evaluation_call = next(
            call
            for call in execute_stage_mock.call_args_list
            if call.args[0] == 'signal_decider_evaluations'
        )
        self.assertEqual(
            completed_evaluation_call.kwargs['symbols'],
            completed_intersection,
        )

    def test_same_run_partial_summary_advances_only_terminal_success_intersection(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        parent_symbols = ['000660', '005930', '035420']
        completed_intersection = ['000660', '005930']
        decider_roles = list(DECISION_SUPPORT_AGENT_NAMES)
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        def feedback_result(*, config, **_kwargs):
            feedback_retry_service.calls.append({'config': config})
            if config.roles == SUMMARIZER_AGENT_NAMES:
                return {
                    'completed': False,
                    'status': 'partial',
                    'final_pending': 0,
                    'roles': list(config.roles),
                    'requested_symbols': parent_symbols,
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'run_id': 'feedback-cycle-summary-partial',
                }
            role_progress = {
                role: {
                    'role': role,
                    'status': 'completed',
                    'boundary_status': 'completed',
                    'total_symbols': len(completed_intersection),
                    'processed_symbols': len(completed_intersection),
                    'completed_symbols': len(completed_intersection),
                    'terminal_symbols': len(completed_intersection),
                    'failed_items': 0,
                    'in_progress_symbols': 0,
                    'pending_symbols': 0,
                    'retry_pending_symbols': 0,
                }
                for role in decider_roles
            }
            return {
                'completed': True,
                'status': 'completed',
                'final_pending': 0,
                'roles': list(config.roles),
                'requested_symbols': list(config.symbols or ()),
                'feedback_scope_mode': config.feedback_scope_mode,
                'scope_authority': config.scope_authority,
                'run_id': 'signal-decider-completed-after-partial-summary',
                'decision_identities': _decision_identities(
                    completed_intersection
                ),
                'progress': {
                    'summary': {
                        'run_id': (
                            'signal-decider-completed-after-partial-summary'
                        ),
                        'run_status': 'completed',
                        'run_table_status': 'completed',
                        'run_completed_at': cutoff.isoformat(),
                        'is_live': False,
                        'expected_feedback_count': (
                            len(completed_intersection)
                            * len(decider_roles)
                        ),
                        'completed_feedback_count': (
                            len(completed_intersection)
                            * len(decider_roles)
                        ),
                        'failed_items': 0,
                        'role_symbol_scopes': {
                            role: list(completed_intersection)
                            for role in decider_roles
                        },
                        'role_symbol_counts': {
                            role: len(completed_intersection)
                            for role in decider_roles
                        },
                        'failed_symbols_by_role': {
                            role: [] for role in decider_roles
                        },
                        'role_progress': role_progress,
                    }
                },
            }

        original_execute_stage = service._execute_stage

        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cutoff if tz is not None else cutoff.replace(tzinfo=None)

        def execute_stage(stage_id, **kwargs):
            if stage_id == 'next_open_execution_plan':
                return _next_open_completion_result(
                    parent_run_id=(
                        'signal-decider-completed-after-partial-summary'
                    ),
                    completed_symbols=list(kwargs['symbols']),
                )
            if stage_id == 'signal_decider_evaluations':
                return _completed_evaluation_result(
                    symbols=list(kwargs['symbols'])
                )
            return original_execute_stage(stage_id, **kwargs)

        with (
            patch.object(
                feedback_retry_service,
                'run_retry_cycles',
                side_effect=feedback_result,
            ),
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute_stage,
            ) as execute_stage_mock,
            patch(
                'app.eod.daily_pipeline_stages.datetime',
                new=FixedDatetime,
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value=_feedback_prerequisites(),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'terminal_role_success_intersection',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary-partial',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': completed_intersection,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': completed_intersection,
                        'admitted_symbols': parent_symbols,
                    },
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'build_expired_decision_replay_result',
                return_value=None,
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                symbols=parent_symbols,
                fs_div='CFS',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            result['stages']['agent_feedback_summaries']['status'],
            'partial',
        )
        self.assertEqual(
            result['stages']['signal_decider']['status'],
            'completed',
        )
        self.assertEqual(
            result['stages']['next_open_execution_plan']['status'],
            'completed',
        )
        self.assertEqual(
            result['stages']['signal_decider_evaluations']['status'],
            'completed',
        )
        self.assertEqual(len(feedback_retry_service.calls), 2)
        self.assertEqual(
            feedback_retry_service.calls[1]['config'].symbols,
            tuple(completed_intersection),
        )
        self.assertEqual(
            feedback_retry_service.calls[1]['config'].scope_authority[
                'source_summary_run_id'
            ],
            'feedback-cycle-summary-partial',
        )
        evaluation_call = next(
            call
            for call in execute_stage_mock.call_args_list
            if call.args[0] == 'signal_decider_evaluations'
        )
        self.assertEqual(
            evaluation_call.kwargs['symbols'],
            completed_intersection,
        )

    def test_exact_previous_session_recovery_runs_only_during_preopen(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        symbols = ['000660', '005930']
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        market = type(
            'Market',
            (),
            {
                'state': 'PREOPEN',
                'server_time': '2026-04-15T08:00:00+09:00',
            },
        )()

        def execute(stage_id, **stage_kwargs):
            if stage_id == 'agent_feedback_summaries':
                return _completed_feedback_result(
                    symbols=symbols,
                    cutoff_at=cutoff,
                )
            if stage_id == 'signal_decider':
                return _completed_decider_result(
                    symbols=symbols,
                    cutoff_at=cutoff,
                    source_summary_run_id='summary-run',
                )
            if stage_id == 'next_open_execution_plan':
                return _next_open_completion_result(
                    parent_run_id='signal-decider-completed',
                    completed_symbols=list(stage_kwargs['symbols']),
                )
            if stage_id == 'signal_decider_evaluations':
                return _completed_evaluation_result(
                    symbols=list(stage_kwargs['symbols'])
                )
            raise AssertionError(stage_id)

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 4, 15),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value=_feedback_prerequisites(),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'summary-run',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': symbols,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': symbols,
                        'admitted_symbols': symbols,
                    },
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                symbols=symbols,
                resume_from_stage='agent_feedback_summaries',
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=73,
                point_in_time_recovery=True,
                reuse_existing_feedback=True,
                provider_admission_fence_owned=True,
                inherit_feedback_prerequisite_admission=True,
                inherited_feedback_knowledge_cutoff_at=cutoff,
                inherited_feedback_source_run_id='summary-run',
                inherited_feedback_retry_config={
                    'source_run_id': 'summary-run',
                    'roles': list(SUMMARIZER_AGENT_NAMES),
                    'requested_symbols': list(symbols),
                    'role_symbol_scopes': {
                        role: list(symbols)
                        for role in SUMMARIZER_AGENT_NAMES
                    },
                    'knowledge_cutoff_at': cutoff,
                    'feedback_scope_mode': (
                        'broad_feedback_universe'
                    ),
                    'fs_div': 'CFS',
                    'policy_context_as_of_date': date(2026, 4, 15),
                    'scope_authority': None,
                    'idempotency_key': None,
                    'outer_batch_size': 25,
                },
            )

        self.assertEqual(result['status'], 'completed', result)
        self.assertTrue(
            self.run_state_service.runs[-1]['run_metadata'][
                'previous_session_preopen_recovery_admitted'
            ]
        )
        self.assertTrue(execute_stage.call_args_list)
        self.assertTrue(
            all(
                call.kwargs['allow_previous_session_preopen_recovery']
                is True
                for call in execute_stage.call_args_list
            )
        )

    def test_decider_only_recovery_inherits_completed_summary_cutoff(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': ['000660', '005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['000660', '005930'],
                        'admitted_symbols': ['000660', '005930'],
                    },
                },
            ) as resolve_cutoff,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                trigger='auto_eod_followup_resume',
                summary_cutoff_proof={
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': None,
                },
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            feedback_retry_service.calls[0]['config'].knowledge_cutoff_at,
            cutoff,
        )
        self.assertEqual(
            feedback_retry_service.calls[0]['config'].symbols,
            ('000660', '005930'),
        )
        self.assertEqual(
            result['pipeline_run']['run_metadata'][
                'feedback_knowledge_cutoff'
            ]['source_run_id'],
            'feedback-cycle-summary',
        )
        self.assertEqual(
            result['pipeline_run']['run_metadata'][
                'feedback_knowledge_cutoff'
            ]['requested_symbols'],
            ['000660', '005930'],
        )
        resolve_cutoff.assert_called_once_with(
            market_date=datetime(2026, 4, 14).date(),
            feedback_scope_mode=(
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            fs_div='CFS',
            source_run_id='feedback-cycle-summary',
            requested_symbols=None,
        )

    def test_decider_only_persisted_cutoff_revalidates_broad_intersection(
        self,
    ) -> None:
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        completed_symbols = ['005930']
        admitted_symbols = ['000660', '005930']
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        resolution = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': cutoff,
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'broad_feedback_universe',
            'requested_symbols': completed_symbols,
            'completed_scope_proof': {
                'schema_version': (
                    'completed_summarizer_intersection_v1'
                ),
                'completed_symbols': completed_symbols,
                'admitted_symbols': admitted_symbols,
            },
        }
        proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'broad_feedback_universe',
            'requested_symbols': None,
            'expected_completed_symbols': completed_symbols,
            'knowledge_cutoff_at': cutoff.isoformat(),
            'fs_div': 'CFS',
        }

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value=resolution,
            ) as resolve_cutoff,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                trigger='auto_eod_followup_resume',
                summary_cutoff_proof=proof,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            feedback_retry_service.calls[0]['config'].symbols,
            tuple(completed_symbols),
        )
        resolve_cutoff.assert_called_once_with(
            market_date=date(2026, 4, 14),
            feedback_scope_mode='broad_feedback_universe',
            fs_div='CFS',
            source_run_id='feedback-cycle-summary',
            requested_symbols=None,
        )

        for mismatch, reason in (
            (
                {
                    **resolution,
                    'knowledge_cutoff_at': cutoff + timedelta(seconds=1),
                },
                'summary_knowledge_cutoff_mismatch',
            ),
            (
                {**resolution, 'requested_symbols': ['000660']},
                'summary_completed_scope_mismatch',
            ),
        ):
            mismatched_retry_service = _FakeFeedbackRetryService()
            mismatched_service = DailyPipelineStageService(
                feedback_retry_service=mismatched_retry_service,
            )
            with (
                self.subTest(reason=reason),
                patch.object(
                    mismatched_service,
                    '_normalize_market_date',
                    return_value='2026-04-14',
                ),
                patch(
                    'app.eod.daily_pipeline_stages.'
                    'resolve_latest_completed_summary_knowledge_cutoff',
                    return_value=mismatch,
                ),
            ):
                blocked = mismatched_service.run(
                    market_date='2026-04-14',
                    stages=['signal_decider'],
                    trigger='auto_eod_followup_resume',
                    summary_cutoff_proof=proof,
                )
            self.assertEqual(blocked['status'], 'blocked')
            self.assertEqual(
                blocked['stages']['signal_decider']['result'][
                    'knowledge_cutoff_resolution'
                ]['reason'],
                reason,
            )
            self.assertEqual(mismatched_retry_service.calls, [])

    def test_decider_only_latest_summary_is_pinned_to_exact_completed_scope(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        parent_symbols = ['000660', '005930']
        completed_intersection = ['005930']
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        latest_resolution = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': cutoff,
            'source_run_id': 'feedback-cycle-summary-latest',
            'feedback_scope_mode': 'broad_feedback_universe',
            'requested_symbols': parent_symbols,
        }
        exact_resolution = {
            **latest_resolution,
            'requested_symbols': completed_intersection,
            'completed_scope_proof': {
                'schema_version': 'completed_summarizer_intersection_v1',
                'completed_symbols': completed_intersection,
                'admitted_symbols': parent_symbols,
            },
        }

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                side_effect=[latest_resolution, exact_resolution],
            ) as resolve_cutoff,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                symbols=parent_symbols,
                trigger='manual_api',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            feedback_retry_service.calls[0]['config'].symbols,
            tuple(completed_intersection),
        )
        self.assertEqual(
            feedback_retry_service.calls[0]['config'].scope_authority[
                'source_summary_run_id'
            ],
            'feedback-cycle-summary-latest',
        )
        self.assertEqual(resolve_cutoff.call_count, 2)
        self.assertEqual(
            resolve_cutoff.call_args_list[1].kwargs,
            {
                'market_date': datetime(2026, 4, 14).date(),
                'feedback_scope_mode': 'broad_feedback_universe',
                'fs_div': 'CFS',
                'source_run_id': 'feedback-cycle-summary-latest',
                'requested_symbols': parent_symbols,
            },
        )

    def test_decider_only_latest_summary_blocks_when_exact_pin_fails(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        parent_symbols = ['000660', '005930']
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                side_effect=[
                    {
                        'ready': True,
                        'reason': 'latest_completed_summary_cutoff',
                        'knowledge_cutoff_at': cutoff,
                        'source_run_id': 'feedback-cycle-summary-latest',
                        'feedback_scope_mode': 'broad_feedback_universe',
                        'requested_symbols': parent_symbols,
                    },
                    {
                        'ready': False,
                        'reason': 'summary_run_lineage_mismatch',
                        'knowledge_cutoff_at': None,
                        'source_run_id': 'feedback-cycle-summary-latest',
                        'feedback_scope_mode': 'broad_feedback_universe',
                        'requested_symbols': parent_symbols,
                    },
                ],
            ) as resolve_cutoff,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                symbols=parent_symbols,
                trigger='internal_ops_cli',
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['stages']['signal_decider']['result']['reason'],
            'summary_knowledge_cutoff_unavailable',
        )
        self.assertEqual(resolve_cutoff.call_count, 2)
        self.assertEqual(feedback_retry_service.calls, [])

    def test_decider_only_recovery_rejects_proof_outside_explicit_scope(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': ['000660', '005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['000660', '005930'],
                        'admitted_symbols': ['000660', '005930'],
                    },
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                symbols=['005930'],
                trigger='auto_eod_followup_resume',
                summary_cutoff_proof={
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': None,
                },
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['stages']['signal_decider']['result'][
                'knowledge_cutoff_resolution'
            ]['reason'],
            'summary_run_symbol_scope_mismatch',
        )
        self.assertEqual(feedback_retry_service.calls, [])

    def test_decider_only_force_bypass_blocks_without_proven_summary_cutoff(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': False,
                    'reason': 'summary_run_incomplete',
                    'knowledge_cutoff_at': None,
                    'source_run_id': 'feedback-cycle-partial',
                    'feedback_scope_mode': 'broad_feedback_universe',
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['signal_decider'],
                symbols=['005930'],
                trigger='auto_eod_followup_resume',
                force=True,
                bypass_prerequisite_gate=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['stages']['signal_decider']['result']['reason'],
            'summary_knowledge_cutoff_unavailable',
        )
        self.assertEqual(feedback_retry_service.calls, [])

    def test_terminal_continuation_reuses_same_run_and_original_summary_cutoff(self) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed: list[tuple[str, datetime | None]] = []

        def execute(stage_id, **kwargs):
            executed.append((stage_id, kwargs['feedback_knowledge_cutoff_at']))
            if stage_id == 'agent_feedback_summaries':
                return {
                    'status': 'completed',
                    'run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['005930'],
                }
            if stage_id == 'signal_decider':
                return _completed_decider_result_for_call(
                    kwargs,
                    run_id='signal-decider-terminal-continuation',
                )
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930'],
                    },
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                symbols=['999999'],
                markets=['KOSDAQ'],
                force=True,
                bypass_prerequisite_gate=True,
                fs_div='OFS',
                feedback_max_cycles=9,
                feedback_outer_batch_size=99,
                reuse_existing_feedback=False,
                point_in_time_recovery=True,
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(result['pipeline_run_id'], 456)
        self.assertEqual(run_state_service.start_run_calls, [])
        self.assertEqual(
            run_state_service.claim_calls[0]['reused_eod_collection_run_id'],
            546,
        )
        self.assertEqual(
            [stage_id for stage_id, _ in executed],
            [
                'agent_feedback_summaries',
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )
        self.assertTrue(all(stage_cutoff == cutoff for _, stage_cutoff in executed))
        for call in execute_stage.call_args_list:
            self.assertEqual(call.kwargs['symbols'], ['005930'])
            self.assertEqual(call.kwargs['markets'], ['KOSPI'])
            self.assertFalse(call.kwargs['force'])
            self.assertFalse(call.kwargs['bypass_prerequisite_gate'])
            self.assertEqual(call.kwargs['fs_div'], 'CFS')
            self.assertEqual(call.kwargs['feedback_max_cycles'], 3)
            self.assertEqual(call.kwargs['feedback_outer_batch_size'], 20)
            self.assertTrue(call.kwargs['reuse_existing_feedback'])
            self.assertTrue(call.kwargs['point_in_time_recovery'])
        final_statuses = run_state_service.finish_calls[0]['run_metadata'][
            'stage_statuses'
        ]
        self.assertEqual(final_statuses['price_collection'], 'partial')
        self.assertEqual(final_statuses['macro_context_refresh'], 'completed')
        self.assertEqual(final_statuses['signal_decider'], 'completed')
        self.assertEqual(result['status'], 'partial')

    def test_terminal_continuation_limits_decider_to_fast_summary_scope(
        self,
    ) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService()
        original_claim = run_state_service.claim_terminal_continuation

        def claim_with_fast_subset(**kwargs):
            claim = original_claim(**kwargs)
            claim['knowledge_cutoff_resolution'].update(
                {
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': ['005930'],
                }
            )
            claim['pipeline_run']['run_metadata']['symbols'] = [
                '005930',
                '000660',
            ]
            return claim

        run_state_service.claim_terminal_continuation = (
            claim_with_fast_subset
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed_symbols: dict[str, list[str] | None] = {}

        def execute(stage_id, **kwargs):
            executed_symbols[stage_id] = kwargs['symbols']
            if stage_id == 'agent_feedback_summaries':
                return {
                    'status': 'completed',
                    'run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['005930', '000660'],
                }
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': datetime(
                        2026,
                        4,
                        14,
                        8,
                        30,
                        tzinfo=timezone.utc,
                    ),
                    'source_run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930', '000660'],
                    },
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(result['pipeline_run_id'], 456)
        self.assertEqual(
            executed_symbols['signal_decider'],
            ['005930'],
        )

    def test_terminal_continuation_refreshes_decider_from_rerun_summary(
        self,
    ) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService()
        original_claim = run_state_service.claim_terminal_continuation
        parent_symbols = ['005930', '000660']
        original_intersection = ['005930']
        rerun_intersection = ['000660']
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)

        def claim_with_prior_intersection(**kwargs):
            claim = original_claim(**kwargs)
            claim['pipeline_run']['run_metadata']['symbols'] = (
                parent_symbols
            )
            claim['knowledge_cutoff_resolution'].update(
                {
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': original_intersection,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': original_intersection,
                        'admitted_symbols': parent_symbols,
                    },
                }
            )
            return claim

        run_state_service.claim_terminal_continuation = (
            claim_with_prior_intersection
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed_symbols: dict[str, list[str] | None] = {}
        decider_cutoff_resolution: dict | None = None

        def execute(stage_id, **kwargs):
            nonlocal decider_cutoff_resolution
            executed_symbols[stage_id] = kwargs['symbols']
            if stage_id == 'agent_feedback_summaries':
                return {
                    'status': 'completed',
                    'run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': parent_symbols,
                }
            if stage_id == 'signal_decider':
                decider_cutoff_resolution = kwargs[
                    'feedback_cutoff_resolution'
                ]
                return _completed_decider_result_for_call(
                    kwargs,
                    run_id='signal-decider-rerun-summary',
                )
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary-rerun',
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': rerun_intersection,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': rerun_intersection,
                        'admitted_symbols': parent_symbols,
                    },
                },
            ) as resolve_cutoff,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            executed_symbols['agent_feedback_summaries'],
            parent_symbols,
        )
        self.assertEqual(
            executed_symbols['signal_decider'],
            rerun_intersection,
        )
        self.assertEqual(
            decider_cutoff_resolution['source_run_id'],
            'feedback-cycle-summary-rerun',
        )
        resolve_cutoff.assert_called_once_with(
            market_date=datetime(2026, 4, 14).date(),
            feedback_scope_mode='broad_feedback_universe',
            fs_div='CFS',
            source_run_id='feedback-cycle-summary-rerun',
            requested_symbols=parent_symbols,
        )

    def test_cutoff_contract_repair_resume_reuses_same_run(self) -> None:
        run_state_service = _FakeCutoffContractRepairRunStateService()
        cutoff = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed: list[tuple[str, list[str] | None]] = []

        def execute(stage_id, **kwargs):
            executed.append((stage_id, kwargs['symbols']))
            if stage_id == 'signal_decider':
                return _completed_decider_result_for_call(
                    kwargs,
                    run_id='signal-decider-cutoff-repair',
                )
            if stage_id == 'next_open_execution_plan':
                return _next_open_completion_result(
                    parent_run_id='signal-decider-cutoff-repair',
                    completed_symbols=list(kwargs['symbols']),
                )
            if stage_id == 'signal_decider_evaluations':
                return _completed_evaluation_result(
                    symbols=list(kwargs['symbols'])
                )
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930'],
                    },
                },
            ) as resolve_cutoff,
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                resume_from_stage='signal_decider',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
                summary_cutoff_contract_repair=True,
                summary_cutoff_proof={
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': None,
                },
            )

        self.assertEqual(result['pipeline_run_id'], 456)
        self.assertEqual(run_state_service.start_run_calls, [])
        self.assertEqual(
            run_state_service.claim_calls,
            [
                {
                    'pipeline_run_id': 456,
                    'market_date': '2026-04-14',
                    'reused_eod_collection_run_id': 546,
                }
            ],
        )
        self.assertEqual(
            executed,
            [
                ('signal_decider', ['005930']),
                ('next_open_execution_plan', ['005930']),
                ('signal_decider_evaluations', ['005930']),
            ],
        )
        resolve_cutoff.assert_not_called()
        self.assertEqual(result['status'], 'completed')

    def test_cutoff_contract_repair_resume_rejects_invalid_persisted_fs_div(
        self,
    ) -> None:
        class InvalidContextRunStateService(
            _FakeCutoffContractRepairRunStateService
        ):
            def claim_summary_cutoff_contract_repair_resume(self, **kwargs):
                result = super().claim_summary_cutoff_contract_repair_resume(
                    **kwargs
                )
                result['pipeline_run']['run_metadata']['fs_div'] = 'INVALID'
                return result

        run_state_service = InvalidContextRunStateService()
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                return_value={'status': 'completed'},
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'cutoff contract repair fs_div is invalid',
            ):
                service.run(
                    market_date='2026-04-14',
                    stages=[
                        'signal_decider',
                        'next_open_execution_plan',
                        'signal_decider_evaluations',
                    ],
                    trigger='auto_eod_followup_resume',
                    resume_from_stage='signal_decider',
                    reused_eod_collection_run_id=546,
                    existing_pipeline_run_id=456,
                    summary_cutoff_contract_repair=True,
                    summary_cutoff_proof={
                        'source_run_id': 'feedback-cycle-summary',
                        'feedback_scope_mode': 'decider_scope_fast_path',
                        'requested_symbols': None,
                    },
                )

        self.assertEqual(len(run_state_service.finish_calls), 1)
        self.assertEqual(
            run_state_service.finish_calls[0]['pipeline_run_id'],
            456,
        )
        self.assertEqual(run_state_service.finish_calls[0]['status'], 'failed')

    def test_terminal_continuation_context_recovers_summary_result_lineage(
        self,
    ) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService()
        pipeline_run = run_state_service.claim_terminal_continuation(
            pipeline_run_id=456,
            reason='summary_knowledge_cutoff_repair',
        )['pipeline_run']
        summary_evidence = pipeline_run['run_metadata'][
            'terminal_continuation_evidence'
        ]['suffix_stages']['agent_feedback_summaries']
        summary_evidence['source_run_ids'] = {'eod_run_id': 546}
        summary_evidence['details'] = {
            'result_summary': {'run_id': 'feedback-cycle-summary'}
        }

        context = DailyPipelineStageService(
            run_state_service=run_state_service,
        )._terminal_continuation_execution_context(pipeline_run)

        self.assertEqual(
            context['summary_source_run_id'],
            'feedback-cycle-summary',
        )

        summary_evidence['source_run_ids'] = {
            'run_id': 'another-feedback-cycle'
        }
        with self.assertRaisesRegex(
            RuntimeError,
            'terminal continuation summary source run is unavailable',
        ):
            DailyPipelineStageService(
                run_state_service=run_state_service,
            )._terminal_continuation_execution_context(pipeline_run)

    def test_terminal_continuation_skips_persisted_completed_prefix(self) -> None:
        suffix_ids = [
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        for completed_prefix_size in range(1, len(suffix_ids)):
            with self.subTest(completed_prefix_size=completed_prefix_size):
                persisted_stage_outcomes = {
                    stage_id: {
                        'stage': stage_id,
                        'stage_id': stage_id,
                        'status': 'completed',
                        'processed': 1,
                        'total': 1,
                        'succeeded': 1,
                        'failed': 0,
                        'error_message': None,
                        'details': {
                            'result_summary': {
                                'status': 'completed',
                                'run_id': f'{stage_id}-continued',
                            }
                        },
                    }
                    for stage_id in suffix_ids[:completed_prefix_size]
                }
                persisted_stage_outcomes['agent_feedback_summaries'][
                    'source_run_ids'
                ] = {'run_id': 'feedback-cycle-summary'}
                persisted_stage_outcomes['agent_feedback_summaries'][
                    'details'
                ]['result_summary'].update(
                    {
                        'run_id': 'feedback-cycle-summary',
                        'feedback_scope_mode': (
                            'broad_feedback_universe'
                        ),
                    }
                )
                run_state_service = (
                    _FakeTerminalContinuationRunStateService(
                        persisted_stage_outcomes=(
                            persisted_stage_outcomes
                        ),
                    )
                )
                service = DailyPipelineStageService(
                    run_state_service=run_state_service,
                )
                executed: list[str] = []

                def execute(stage_id, **_kwargs):
                    executed.append(stage_id)
                    if stage_id == 'signal_decider':
                        return _completed_decider_result_for_call(
                            _kwargs,
                            run_id='signal-decider-prefix-recovery',
                        )
                    return {
                        'status': 'completed',
                        'stage_id': stage_id,
                    }

                with (
                    patch.object(
                        service,
                        '_normalize_market_date',
                        return_value='2026-04-14',
                    ),
                    patch.object(
                        service,
                        '_execute_stage',
                        side_effect=execute,
                    ),
                ):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=suffix_ids,
                        trigger='auto_eod_followup_resume',
                        reused_eod_collection_run_id=546,
                        existing_pipeline_run_id=456,
                    )

                self.assertEqual(
                    executed,
                    (
                        suffix_ids[completed_prefix_size:]
                        if completed_prefix_size == 1
                        else []
                    ),
                )
                started_stage_ids = [
                    event['stage_id']
                    for event in run_state_service.stage_events
                    if event['event'] == 'start_stage'
                ]
                for stage_id in suffix_ids[:completed_prefix_size]:
                    self.assertNotIn(stage_id, started_stage_ids)
                    self.assertEqual(
                        result['stages'][stage_id]['status'],
                        'completed',
                    )
                if completed_prefix_size >= 2:
                    for stage_id in suffix_ids[completed_prefix_size:]:
                        self.assertEqual(
                            result['stages'][stage_id]['status'],
                            'blocked',
                        )
                self.assertEqual(result['status'], 'partial')

    def test_terminal_continuation_preserves_original_scope_for_skipped_summary(
        self,
    ) -> None:
        skipped_summary = {
            'stage': 'agent_feedback_summaries',
            'stage_id': 'agent_feedback_summaries',
            'status': 'skipped',
            'processed': 0,
            'total': 0,
            'succeeded': 0,
            'failed': 0,
            'remaining': 0,
            'error_message': None,
            'source_run_ids': {},
            'details': {
                'result_summary': {
                    'triggered': False,
                    'status': 'skipped',
                    'reason': 'next_open_window_expired',
                    'market_date': '2026-04-14',
                    'target_trade_date': '2026-04-15',
                    'current_market_date': '2026-04-15',
                    'market_state': 'OPEN',
                    'stage': 'agent_feedback_summaries',
                }
            },
        }
        run_state_service = _FakeTerminalContinuationRunStateService(
            persisted_stage_outcomes={
                'agent_feedback_summaries': skipped_summary,
            },
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed: list[str] = []

        def execute(stage_id, **_kwargs):
            executed.append(stage_id)
            if stage_id == 'signal_decider':
                return _completed_decider_result_for_call(
                    _kwargs,
                    run_id='signal-decider-skipped-summary',
                )
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(
            result['stages']['agent_feedback_summaries']['status'],
            'skipped',
        )
        self.assertEqual(
            executed,
            [
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )

    def test_terminal_continuation_uses_persisted_rerun_summary_scope(
        self,
    ) -> None:
        parent_symbols = ['005930', '000660']
        rerun_symbols = ['000660']
        rerun_source_run_id = 'agent_feedback_summaries-rerun'
        persisted_summary_outcome = {
            'stage': 'agent_feedback_summaries',
            'stage_id': 'agent_feedback_summaries',
            'status': 'completed',
            'processed': 1,
            'total': 1,
            'succeeded': 1,
            'failed': 0,
            'error_message': None,
            'details': {
                'result_summary': {
                    'status': 'completed',
                    'run_id': rerun_source_run_id,
                    'feedback_scope_mode': 'broad_feedback_universe',
                }
            },
        }
        run_state_service = _FakeTerminalContinuationRunStateService(
            persisted_stage_outcomes={
                'agent_feedback_summaries': persisted_summary_outcome,
            },
        )
        original_claim = run_state_service.claim_terminal_continuation

        def claim_with_rerun_summary(**kwargs):
            claim = original_claim(**kwargs)
            claim['pipeline_run']['run_metadata']['symbols'] = (
                parent_symbols
            )
            claim['knowledge_cutoff_resolution'].update(
                {
                    'source_run_id': rerun_source_run_id,
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'requested_symbols': rerun_symbols,
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': rerun_symbols,
                        'admitted_symbols': parent_symbols,
                    },
                }
            )
            return claim

        run_state_service.claim_terminal_continuation = (
            claim_with_rerun_summary
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )
        executed_symbols: dict[str, list[str] | None] = {}
        decider_source_run_id: str | None = None

        def execute(stage_id, **kwargs):
            nonlocal decider_source_run_id
            executed_symbols[stage_id] = kwargs['symbols']
            if stage_id == 'signal_decider':
                decider_source_run_id = kwargs[
                    'feedback_cutoff_resolution'
                ]['source_run_id']
                return _completed_decider_result_for_call(
                    kwargs,
                    run_id='signal-decider-persisted-rerun',
                )
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertNotIn(
            'agent_feedback_summaries',
            executed_symbols,
        )
        self.assertEqual(
            executed_symbols['signal_decider'],
            rerun_symbols,
        )
        self.assertEqual(decider_source_run_id, rerun_source_run_id)

    def test_terminal_continuation_does_not_finalize_when_claim_proof_raises(
        self,
    ) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService(
            claim_error=RuntimeError('cutoff lookup failed'),
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )

        with patch.object(
            service,
            '_normalize_market_date',
            return_value='2026-04-14',
        ):
            with self.assertRaisesRegex(RuntimeError, 'cutoff lookup failed'):
                service.run(
                    market_date='2026-04-14',
                    stages=[
                        'agent_feedback_summaries',
                        'signal_decider',
                        'next_open_execution_plan',
                        'signal_decider_evaluations',
                    ],
                    trigger='auto_eod_followup_resume',
                    point_in_time_recovery=True,
                    reused_eod_collection_run_id=546,
                    existing_pipeline_run_id=456,
                )

        self.assertEqual(len(run_state_service.claim_calls), 1)
        self.assertEqual(run_state_service.finish_calls, [])
        self.assertEqual(run_state_service.stage_events, [])

    def test_terminal_continuation_stops_when_claim_is_rejected(self) -> None:
        run_state_service = _FakeTerminalContinuationRunStateService(
            claimed=False,
        )
        service = DailyPipelineStageService(
            run_state_service=run_state_service,
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(service, '_execute_stage') as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=546,
                existing_pipeline_run_id=456,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(
            result['reason'],
            'terminal_continuation_already_claimed',
        )
        execute_stage.assert_not_called()

    def test_run_uses_default_daily_stage_order_including_factor_stages(self) -> None:
        call_order: list[str] = []
        pykrx_runner = _FakePykrxStageRunner(events=call_order)
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        def execute(stage_id, **_kwargs):
            call_order.append(stage_id)
            return {'stage_id': stage_id}

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': False,
                    'reason': 'no_completed_summary',
                    'knowledge_cutoff_at': None,
                    'source_run_id': None,
                    'feedback_scope_mode': None,
                },
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(market_date='2026-04-14')

        self.assertEqual(
            result['resolved_stages'],
            service.describe()['default_daily_stage_order'],
        )
        self.assertEqual(len(pykrx_runner.calls), 1)
        self.assertEqual(
            pykrx_runner.calls[0]['stages'],
            [
                'price_collection',
                'technical_snapshots',
                'action_summaries',
                'trending_ranks',
                'market_risk_filter',
            ],
        )
        self.assertEqual(
            pykrx_runner.calls[0]['owning_pipeline_run_id'],
            self.run_state_service.runs[0]['id'],
        )
        self.assertTrue(pykrx_runner.calls[0]['refresh_universe'])
        self.assertEqual(
            call_order[:4],
            [
                'master_refresh',
                'pykrx_universe_acquired',
                'price_collection_started',
                'pykrx_enrichment',
            ],
        )
        self.assertEqual(
            result['stages']['pykrx_universe_refresh']['result'][
                'membership_sha256'
            ],
            hashlib.sha256(b'000660\n005930').hexdigest(),
        )
        self.assertEqual(
            result['stages']['pykrx_universe_refresh']['result'][
                'persisted_count'
            ],
            2,
        )
        self.assertEqual(
            result['stages']['price_collection']['result'][
                'source_run_ids'
            ]['eod_run_id'],
            901,
        )
        price_finish = next(
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'finish_stage'
            and event['stage_id'] == 'price_collection'
        )
        self.assertEqual(
            price_finish['source_run_ids']['eod_run_id'],
            901,
        )
        executed_stage_ids = [call.args[0] for call in execute_stage.call_args_list]
        enrichment_call = next(
            call
            for call in execute_stage.call_args_list
            if call.args[0] == 'pykrx_enrichment'
        )
        self.assertEqual(
            enrichment_call.kwargs['exact_scope_symbols'],
            ['000660', '005930'],
        )
        self.assertEqual(
            executed_stage_ids[:3],
            [
                'master_refresh',
                'pykrx_enrichment',
                'fundamentals_daily_refresh',
            ],
        )
        self.assertEqual(
            executed_stage_ids[3:12],
            [
                'piotroski_filter',
                'momentum_factors',
                'value_factors',
                'low_risk_factors',
                'beta_factors',
                'advanced_value_factors',
                'unified_value_factors',
                'quality_factors',
                'composite_factors',
            ],
        )
        self.assertEqual(executed_stage_ids[12], 'signal_features')
        self.assertEqual(executed_stage_ids[13], 'external_news_collect')
        self.assertEqual(executed_stage_ids[14], 'external_news_enrich')
        self.assertEqual(executed_stage_ids[15], 'macro_context_refresh')

    def test_master_failure_short_circuits_universe_price_and_derived_writes(
        self,
    ) -> None:
        pykrx_runner = MagicMock()
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                return_value={
                    'status': 'failed',
                    'reason': 'master_provider_unavailable',
                },
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'master_refresh',
                    'pykrx_universe_refresh',
                    'price_collection',
                    'technical_snapshots',
                ],
            )

        execute_stage.assert_called_once()
        self.assertEqual(execute_stage.call_args.args[0], 'master_refresh')
        pykrx_runner.run_daily_pipeline_stages.assert_not_called()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(
            self.run_state_service.runs[-1]['finish']['status'],
            'failed',
        )
        self.assertEqual(result['stages']['master_refresh']['status'], 'failed')
        self.assertEqual(
            result['stages']['pykrx_universe_refresh']['status'],
            'blocked',
        )
        self.assertEqual(
            result['stages']['price_collection']['status'],
            'blocked',
        )
        self.assertEqual(
            result['stages']['technical_snapshots']['status'],
            'blocked',
        )

    def test_universe_acquisition_failure_stops_before_downstream_stages(
        self,
    ) -> None:
        pykrx_runner = MagicMock()
        pykrx_runner.run_daily_pipeline_stages.side_effect = RuntimeError(
            'universe unavailable'
        )
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        def execute(stage_id, **_kwargs):
            self.assertEqual(stage_id, 'master_refresh')
            return {'status': 'completed', 'symbols_refreshed': 2}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'universe unavailable',
            ):
                service.run(
                    market_date='2026-04-14',
                    stages=[
                        'master_refresh',
                        'pykrx_universe_refresh',
                        'price_collection',
                        'technical_snapshots',
                    ],
                )

        execute_stage.assert_called_once()
        pykrx_runner.run_daily_pipeline_stages.assert_called_once()
        finished_stage_ids = [
            event['stage_id']
            for event in self.run_state_service.stage_events
            if event['event'] == 'finish_stage'
        ]
        self.assertEqual(
            finished_stage_ids,
            ['master_refresh', 'pykrx_universe_refresh'],
        )
        universe_finish = self.run_state_service.stage_events[-1]
        self.assertEqual(universe_finish['status'], 'failed')
        self.assertEqual(
            universe_finish['details']['result_summary']['reason'],
            'authoritative_universe_acquisition_failed',
        )
        self.assertEqual(
            self.run_state_service.runs[-1]['status'],
            'failed',
        )

    def test_run_treats_already_completed_skip_as_satisfied_dependency(self) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'fundamentals_daily_refresh':
                return {
                    'triggered': False,
                    'reason': 'already_completed',
                    'market_date': '2026-04-14',
                }
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=execute) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['fundamentals_daily_refresh', 'advanced_value_factors'],
            )

        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'skipped')
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'completed')
        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['fundamentals_daily_refresh', 'advanced_value_factors'],
        )

    def test_run_keeps_non_satisfied_skip_dependency_blocked(self) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'fundamentals_daily_refresh':
                return {
                    'triggered': False,
                    'reason': 'already_running',
                    'market_date': '2026-04-14',
                }
            raise AssertionError('dependent stage must not run after an unsafe skip')

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=execute) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['fundamentals_daily_refresh', 'advanced_value_factors'],
            )

        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'skipped')
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'blocked')
        self.assertEqual(
            result['stages']['advanced_value_factors']['result']['incomplete_dependencies'],
            ['fundamentals_daily_refresh'],
        )
        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['fundamentals_daily_refresh'],
        )

    def test_technical_counts_union_disjoint_context_and_snapshot_debt(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='technical_snapshots',
            result={
                'contexts_persisted': 2,
                'contexts_failed': 1,
                'snapshots_persisted': 2,
                'snapshots_failed': 1,
                'scope_outcome_manifest_version': (
                    'technical_snapshots_scope_outcomes_v1'
                ),
                'selected_stock_codes': [
                    '005930',
                    '000660',
                    '035420',
                ],
                'successful_stock_codes': ['005930'],
                'retry_debt_stock_codes': ['000660', '035420'],
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 3,
                'total': 3,
                'succeeded': 1,
                'failed': 2,
            },
        )

    def test_market_risk_terminal_counts_ignore_screening_rejections(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='market_risk_filter',
            result={
                'selected_stock_codes': ['005930', '000660'],
                'results_persisted': 2,
                'results_failed': 0,
                'summary': {
                    'passed_count': 1,
                    'screening_rejected_count': 1,
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 2,
                'succeeded': 2,
                'failed': 0,
            },
        )

    def test_market_risk_snapshot_counts_use_exact_requested_scope_debt(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='market_risk_snapshots',
            result={
                'requested_symbols': ['005930', '000660', '999999'],
                'matched_symbols': ['005930'],
                'snapshots_persisted': 1,
                # Missing requested identities are still retry debt even if
                # an older producer omitted them from its failure count.
                'snapshots_failed': 0,
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 3,
                'total': 3,
                'succeeded': 1,
                'failed': 2,
            },
        )

    def test_market_risk_snapshot_counts_broad_mixed_missing_scope(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='market_risk_snapshots',
            result={
                'requested_symbols': ['999999', '005930'],
                'matched_symbols': ['005930'],
                'missing_symbols': ['999999'],
                'snapshots_persisted': 1,
                'snapshots_failed': 1,
                'failed_symbols': ['999999'],
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 2,
                'succeeded': 1,
                'failed': 1,
            },
        )

    def test_partial_market_risk_advances_only_exact_success_cohort(
        self,
    ) -> None:
        selected_symbols = [f'{index:06d}' for index in range(1, 21)]
        retry_symbol = selected_symbols[-1]
        successful_symbols = selected_symbols[:-1]

        class PartialMarketRiskPykrxRunner:
            def run_daily_pipeline_stages(_self, **kwargs):
                return {
                    'run_id': 904,
                    'status': 'completed_with_errors',
                    'latest_available_trade_date': kwargs['market_date'],
                    'symbols_total': len(selected_symbols),
                    'symbols_succeeded': len(selected_symbols),
                    'symbols_failed': 0,
                    'bars_upserted': len(selected_symbols),
                    **_build_exact_scope_outcome(
                        selected_symbols=selected_symbols,
                        successful_symbols=selected_symbols,
                        retry_debt_symbols=[],
                    ),
                    **_build_pykrx_invocation_scope_proof(
                        selected_symbols
                    ),
                    'market_risk_filter': {
                        'status': 'partial',
                        'results_persisted': len(successful_symbols),
                        'results_failed': 1,
                        'failed_symbols': [retry_symbol],
                        'matched_symbols': selected_symbols,
                        **_build_exact_scope_outcome(
                            manifest_version=(
                                'market_risk_filter_scope_outcomes_v1'
                            ),
                            selected_symbols=selected_symbols,
                            successful_symbols=successful_symbols,
                            retry_debt_symbols=[retry_symbol],
                        ),
                    },
                }

        service = DailyPipelineStageService(
            pykrx_service=PartialMarketRiskPykrxRunner()
        )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                return_value={'status': 'completed'},
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'price_collection',
                    'market_risk_filter',
                    'external_news_collect',
                ],
                symbols=selected_symbols,
            )

        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['master_refresh', 'external_news_collect'],
        )
        external_news_call = execute_stage.call_args_list[-1]
        self.assertEqual(
            external_news_call.args[0],
            'external_news_collect',
        )
        self.assertEqual(
            external_news_call.kwargs['symbols'],
            successful_symbols,
        )
        self.assertEqual(
            result['stages']['market_risk_filter']['status'],
            'partial',
        )
        self.assertEqual(
            result['stages']['external_news_collect']['result'][
                'requested_symbols'
            ],
            successful_symbols,
        )

    def test_partial_factor_persistence_advances_ninety_nine_of_one_hundred(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        selected_symbols = [f'{index:06d}' for index in range(1, 101)]
        successful_symbols = selected_symbols[:-1]
        retry_symbol = selected_symbols[-1]

        def execute(stage_id, **kwargs):
            if stage_id == 'momentum_factors':
                self.assertEqual(kwargs['symbols'], selected_symbols)
                return {
                    'status': 'partial',
                    'results_persisted': len(successful_symbols),
                    'results_failed': 1,
                    'failed_symbols': [retry_symbol],
                    **_build_exact_scope_outcome(
                        manifest_version='factor_scope_outcomes_v1',
                        selected_symbols=selected_symbols,
                        successful_symbols=successful_symbols,
                        retry_debt_symbols=[retry_symbol],
                    ),
                }
            if stage_id == 'composite_factors':
                self.assertEqual(kwargs['symbols'], successful_symbols)
                return {
                    'status': 'completed',
                    'results_persisted': len(successful_symbols),
                    'results_failed': 0,
                }
            raise AssertionError(f'unexpected stage: {stage_id}')

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors', 'composite_factors'],
                symbols=selected_symbols,
            )

        self.assertEqual(
            result['stages']['momentum_factors']['status'],
            'partial',
        )
        self.assertEqual(
            result['stages']['composite_factors']['status'],
            'completed',
        )
        composite_result = result['stages']['composite_factors']['result']
        self.assertEqual(
            composite_result['requested_symbols'],
            successful_symbols,
        )
        self.assertEqual(
            composite_result['exact_partial_dependency_proofs'],
            [
                {
                    'dependency': 'momentum_factors',
                    'manifest_version': 'factor_scope_outcomes_v1',
                    'selected_stock_codes': selected_symbols,
                    'successful_stock_codes': successful_symbols,
                    'retry_debt_stock_codes': [retry_symbol],
                    'terminal_not_applicable_stock_codes': [],
                    'admitted_stock_codes': successful_symbols,
                }
            ],
        )
        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['momentum_factors', 'composite_factors'],
        )

    def test_full_universe_pykrx_proof_advances_only_exact_price_successes(
        self,
    ) -> None:
        pykrx_runner = _FakeExactScopePykrxStageRunner(
            invocation_proof='valid'
        )
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                return_value={'status': 'completed'},
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['price_collection', 'momentum_factors'],
                symbols=None,
            )

        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['master_refresh', 'momentum_factors'],
        )
        self.assertEqual(
            execute_stage.call_args_list[-1].args[0],
            'momentum_factors',
        )
        self.assertEqual(
            execute_stage.call_args_list[-1].kwargs['symbols'],
            ['005930'],
        )
        price_result = result['stages']['price_collection']['result']
        self.assertEqual(
            price_result['retry_debt_stock_codes'],
            ['000660'],
        )
        self.assertEqual(
            result['stages']['momentum_factors']['result'][
                'exact_partial_dependency_proofs'
            ],
            [
                {
                    'dependency': 'price_collection',
                    'manifest_version': (
                        'pykrx_enrichment_scope_outcomes_v1'
                    ),
                    'selected_stock_codes': ['005930', '000660'],
                    'successful_stock_codes': ['005930'],
                    'retry_debt_stock_codes': ['000660'],
                    'terminal_not_applicable_stock_codes': [],
                    'admitted_stock_codes': ['005930'],
                }
            ],
        )

    def test_full_universe_pykrx_scope_blocks_without_matching_proof(
        self,
    ) -> None:
        for proof_mode in ('missing', 'mismatched'):
            with self.subTest(proof_mode=proof_mode):
                pykrx_runner = _FakeExactScopePykrxStageRunner(
                    invocation_proof=proof_mode
                )
                service = DailyPipelineStageService(
                    pykrx_service=pykrx_runner
                )

                with (
                    patch.object(
                        service,
                        '_normalize_market_date',
                        return_value='2026-04-14',
                    ),
                    patch.object(
                        service,
                        '_execute_stage',
                        return_value={'status': 'completed'},
                    ) as execute_stage,
                ):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=['price_collection', 'momentum_factors'],
                        symbols=None,
                    )

                execute_stage.assert_called_once()
                self.assertEqual(
                    execute_stage.call_args.args[0],
                    'master_refresh',
                )
                momentum_stage = result['stages']['momentum_factors']
                self.assertEqual(momentum_stage['status'], 'blocked')
                self.assertEqual(
                    momentum_stage['result']['incomplete_dependencies'],
                    ['price_collection'],
                )

    def test_price_scope_manifest_cannot_narrow_explicit_invocation(
        self,
    ) -> None:
        requested_symbols = ['005930', '000660', '035420']

        class NarrowingPriceRunner:
            def run_daily_pipeline_stages(_self, **kwargs):
                reported_symbols = requested_symbols[:2]
                return {
                    'run_id': 904,
                    'status': 'completed_with_errors',
                    'latest_available_trade_date': kwargs['market_date'],
                    'symbols_total': len(requested_symbols),
                    'symbols_succeeded': len(reported_symbols),
                    'symbols_failed': 1,
                    'failed_symbols': [requested_symbols[-1]],
                    'bars_upserted': len(reported_symbols),
                    **_build_exact_scope_outcome(
                        selected_symbols=reported_symbols,
                        successful_symbols=reported_symbols,
                        retry_debt_symbols=[],
                    ),
                    **_build_pykrx_invocation_scope_proof(
                        requested_symbols
                    ),
                }

        service = DailyPipelineStageService(
            pykrx_service=NarrowingPriceRunner()
        )
        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                return_value={'status': 'completed'},
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['price_collection', 'momentum_factors'],
                symbols=requested_symbols,
            )

        execute_stage.assert_called_once()
        self.assertEqual(
            execute_stage.call_args.args[0],
            'master_refresh',
        )
        price_stage = result['stages']['price_collection']
        self.assertEqual(price_stage['status'], 'partial')
        self.assertEqual(
            price_stage['result']['failed_symbols'],
            ['035420'],
        )
        momentum_stage = result['stages']['momentum_factors']
        self.assertEqual(momentum_stage['status'], 'blocked')
        self.assertEqual(
            momentum_stage['result']['incomplete_dependencies'],
            ['price_collection'],
        )

    def test_limited_pitr_price_retry_preserves_frozen_finalization_scope(
        self,
    ) -> None:
        exact_membership = ['005930', '000660', '035420']
        retry_subset = ['035420']

        class LimitedRetryRunner:
            def run_daily_pipeline_stages(_self, **kwargs):
                return {
                    'run_id': 905,
                    'status': 'completed',
                    'latest_available_trade_date': kwargs['market_date'],
                    'symbols_total': 1,
                    'symbols_succeeded': 1,
                    'symbols_failed': 0,
                    'failed_symbols': [],
                    'bars_upserted': 1,
                    **_build_exact_scope_outcome(
                        selected_symbols=retry_subset,
                        successful_symbols=retry_subset,
                        retry_debt_symbols=[],
                    ),
                    **_build_pykrx_invocation_scope_proof(retry_subset),
                }

        service = DailyPipelineStageService(
            pykrx_service=LimitedRetryRunner()
        )

        def execute(stage_id, **kwargs):
            self.assertEqual(stage_id, 'momentum_factors')
            self.assertEqual(kwargs['symbols'], exact_membership)
            self.assertEqual(
                kwargs['exact_scope_symbols'],
                exact_membership,
            )
            return {'status': 'completed'}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['price_collection', 'momentum_factors'],
                symbols=retry_subset,
                exact_membership_symbols=exact_membership,
                markets=['KOSPI', 'KOSDAQ'],
                reused_eod_collection_run_id=905,
                point_in_time_recovery=True,
                trigger='exact_price_bar_repair',
                recovery_parent_scope_outcome=(
                    _build_exact_scope_outcome(
                        manifest_version=(
                            'auto_eod_followup_admission_scope_outcomes_v1'
                        ),
                        selected_symbols=exact_membership,
                        successful_symbols=['005930', '000660'],
                        retry_debt_symbols=retry_subset,
                    )
                ),
            )

        execute_stage.assert_called_once()
        self.assertEqual(
            result['stages']['momentum_factors']['result'][
                'requested_symbols'
            ],
            exact_membership,
        )

    def test_pitr_rejects_conflicting_complete_membership_snapshot(
        self,
    ) -> None:
        exact_membership = ['005930', '000660', '035420']
        retry_subset = ['035420']

        class LimitedRetryRunner:
            def run_daily_pipeline_stages(_self, **kwargs):
                return {
                    'run_id': 906,
                    'status': 'completed',
                    'latest_available_trade_date': kwargs['market_date'],
                    'symbols_total': 1,
                    'symbols_succeeded': 1,
                    'symbols_failed': 0,
                    'bars_upserted': 1,
                    'universe_snapshot': {'complete': True},
                    **_build_exact_scope_outcome(
                        selected_symbols=retry_subset,
                        successful_symbols=retry_subset,
                        retry_debt_symbols=[],
                    ),
                    **_build_pykrx_invocation_scope_proof(retry_subset),
                }

        service = DailyPipelineStageService(
            pykrx_service=LimitedRetryRunner()
        )
        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                '_validated_exact_piotroski_membership_symbols',
                return_value=['005930', '000660'],
            ),
            patch.object(service, '_execute_stage') as execute_stage,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'conflicts with the frozen point-in-time recovery membership',
            ):
                service.run(
                    market_date='2026-04-14',
                    stages=['price_collection', 'momentum_factors'],
                    symbols=retry_subset,
                    exact_membership_symbols=exact_membership,
                    markets=['KOSPI', 'KOSDAQ'],
                    reused_eod_collection_run_id=906,
                    point_in_time_recovery=True,
                    trigger='exact_price_bar_repair',
                )

        execute_stage.assert_not_called()
        self.assertEqual(
            self.run_state_service.runs[-1]['status'],
            'failed',
        )

    def test_partial_dart_scope_advances_only_exact_success_partition_into_piotroski(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        dart_result = {
            'status': 'partial',
            'scope_outcome_manifest_version': (
                'dart_daily_data_scope_outcomes_v1'
            ),
            'selected_stock_codes': ['005930', '000660', '035420'],
            'successful_stock_codes': ['005930'],
            'retry_debt_stock_codes': ['000660'],
            'terminal_not_applicable_stock_codes': ['035420'],
            'scope_outcome_manifest': [
                {'stock_code': '005930', 'outcome': 'success'},
                {'stock_code': '000660', 'outcome': 'retry_debt'},
                {
                    'stock_code': '035420',
                    'outcome': 'terminal_not_applicable',
                },
            ],
        }

        def execute(stage_id, **kwargs):
            if stage_id == 'fundamentals_daily_refresh':
                return deepcopy(dart_result)
            if stage_id == 'pykrx_enrichment':
                return {'status': 'completed'}
            if stage_id == 'piotroski_filter':
                return {
                    'status': 'completed',
                    'selected_stock_codes': list(kwargs['symbols']),
                    'successful_stock_codes': list(kwargs['symbols']),
                    'retry_debt_stock_codes': [],
                }
            raise AssertionError(f'unexpected stage: {stage_id}')

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'pykrx_enrichment',
                    'fundamentals_daily_refresh',
                    'piotroski_filter',
                ],
                symbols=['005930', '000660', '035420'],
            )

        piotroski_call = next(
            call
            for call in execute_stage.call_args_list
            if call.args[0] == 'piotroski_filter'
        )
        self.assertEqual(piotroski_call.kwargs['symbols'], ['005930'])
        self.assertEqual(result['stages']['piotroski_filter']['status'], 'completed')
        self.assertEqual(
            result['stages']['piotroski_filter']['result']['requested_symbols'],
            ['005930'],
        )
        self.assertEqual(
            result['stages']['piotroski_filter']['result'][
                'exact_partial_dependency_proofs'
            ],
            [
                {
                    'dependency': 'fundamentals_daily_refresh',
                    'manifest_version': 'dart_daily_data_scope_outcomes_v1',
                    'selected_stock_codes': ['005930', '000660', '035420'],
                    'successful_stock_codes': ['005930'],
                    'retry_debt_stock_codes': ['000660'],
                    'terminal_not_applicable_stock_codes': ['035420'],
                    'admitted_stock_codes': ['005930'],
                }
            ],
        )

    def test_pitr_retry_subset_rebuilds_full_exact_finalization_scope(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        exact_membership = ['005930', '000660', '035420']
        retry_subset = ['035420']

        def execute(stage_id, **kwargs):
            if stage_id == 'fundamentals_daily_refresh':
                self.assertEqual(kwargs['symbols'], retry_subset)
                return {
                    'status': 'partial',
                    'scope_outcome_manifest_version': (
                        'dart_daily_data_scope_outcomes_v1'
                    ),
                    'selected_stock_codes': retry_subset,
                    'successful_stock_codes': retry_subset,
                    'retry_debt_stock_codes': [],
                    'terminal_not_applicable_stock_codes': [],
                    'scope_outcome_manifest': [
                        {
                            'stock_code': '035420',
                            'outcome': 'success',
                        },
                    ],
                }
            if stage_id == 'piotroski_filter':
                return {
                    'status': 'completed',
                    'selected_stock_codes': list(kwargs['symbols']),
                    'successful_stock_codes': list(kwargs['symbols']),
                    'retry_debt_stock_codes': [],
                }
            raise AssertionError(f'unexpected stage: {stage_id}')

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'fundamentals_daily_refresh',
                    'piotroski_filter',
                ],
                symbols=retry_subset,
                exact_membership_symbols=exact_membership,
                markets=['KOSPI', 'KOSDAQ'],
                reused_eod_collection_run_id=904,
                point_in_time_recovery=True,
                trigger='exact_price_bar_repair',
                recovery_parent_scope_outcome=(
                    _build_exact_scope_outcome(
                        manifest_version=(
                            'auto_eod_followup_admission_scope_outcomes_v1'
                        ),
                        selected_symbols=exact_membership,
                        successful_symbols=['005930', '000660'],
                        retry_debt_symbols=retry_subset,
                    )
                ),
            )

        piotroski_call = next(
            call
            for call in execute_stage.call_args_list
            if call.args[0] == 'piotroski_filter'
        )
        self.assertEqual(
            piotroski_call.kwargs['symbols'],
            exact_membership,
        )
        self.assertEqual(
            piotroski_call.kwargs['exact_scope_symbols'],
            exact_membership,
        )
        self.assertEqual(
            result['stages']['piotroski_filter']['result'][
                'requested_symbols'
            ],
            exact_membership,
        )
        self.assertEqual(
            result['stages']['piotroski_filter']['result'][
                'exact_partial_dependency_proofs'
            ][0]['admitted_stock_codes'],
            retry_subset,
        )

    def test_pitr_excludes_current_retry_debt_but_preserves_prior_successes(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        exact_membership = ['005930', '000660', '035420', '051910']
        retry_subset = ['035420', '051910']
        current_success = ['035420']
        current_retry_debt = ['051910']

        def execute(stage_id, **kwargs):
            if stage_id == 'fundamentals_daily_refresh':
                self.assertEqual(kwargs['symbols'], retry_subset)
                return {
                    'status': 'partial',
                    'scope_outcome_manifest_version': (
                        'dart_daily_data_scope_outcomes_v1'
                    ),
                    'selected_stock_codes': retry_subset,
                    'successful_stock_codes': current_success,
                    'retry_debt_stock_codes': current_retry_debt,
                    'terminal_not_applicable_stock_codes': [],
                    'scope_outcome_manifest': [
                        {
                            'stock_code': '035420',
                            'outcome': 'success',
                        },
                        {
                            'stock_code': '051910',
                            'outcome': 'retry_debt',
                        },
                    ],
                }
            if stage_id == 'piotroski_filter':
                self.assertEqual(
                    kwargs['symbols'],
                    ['005930', '000660', '035420'],
                )
                self.assertEqual(
                    kwargs['exact_scope_symbols'],
                    exact_membership,
                )
                return {
                    'status': 'completed',
                    'selected_stock_codes': list(kwargs['symbols']),
                    'successful_stock_codes': list(kwargs['symbols']),
                    'retry_debt_stock_codes': [],
                }
            raise AssertionError(f'unexpected stage: {stage_id}')

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'fundamentals_daily_refresh',
                    'piotroski_filter',
                ],
                symbols=retry_subset,
                exact_membership_symbols=exact_membership,
                markets=['KOSPI', 'KOSDAQ'],
                reused_eod_collection_run_id=904,
                point_in_time_recovery=True,
                trigger='exact_price_bar_repair',
                recovery_parent_scope_outcome=(
                    _build_exact_scope_outcome(
                        manifest_version=(
                            'auto_eod_followup_admission_scope_outcomes_v1'
                        ),
                        selected_symbols=exact_membership,
                        successful_symbols=['005930', '000660'],
                        retry_debt_symbols=retry_subset,
                    )
                ),
            )

        piotroski_call = next(
            call
            for call in execute_stage.call_args_list
            if call.args[0] == 'piotroski_filter'
        )
        self.assertNotIn(
            current_retry_debt[0],
            piotroski_call.kwargs['symbols'],
        )
        self.assertEqual(
            result['stages']['piotroski_filter']['result'][
                'requested_symbols'
            ],
            ['005930', '000660', '035420'],
        )

    def test_pitr_narrowed_finalization_blocks_without_parent_scope_proof(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        exact_membership = ['005930', '000660']
        retry_subset = ['000660']

        def execute(stage_id, **kwargs):
            if stage_id == 'fundamentals_daily_refresh':
                return {
                    'status': 'completed',
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'dart_daily_data_scope_outcomes_v1'
                        ),
                        selected_symbols=retry_subset,
                        successful_symbols=retry_subset,
                        retry_debt_symbols=[],
                    ),
                }
            raise AssertionError(
                'exact finalization must not run without parent proof'
            )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'fundamentals_daily_refresh',
                    'piotroski_filter',
                ],
                symbols=retry_subset,
                exact_membership_symbols=exact_membership,
                markets=['KOSPI'],
                reused_eod_collection_run_id=904,
                point_in_time_recovery=True,
                trigger='exact_price_bar_repair',
            )

        self.assertEqual(execute_stage.call_count, 1)
        blocked = result['stages']['piotroski_filter']
        self.assertEqual(blocked['status'], 'blocked')
        self.assertIn(
            'recovery_parent_scope_outcome',
            blocked['result']['incomplete_dependencies'],
        )

    def test_pitr_full_retry_scope_does_not_reintroduce_current_debt(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        exact_membership = ['005930', '000660']

        def execute(stage_id, **kwargs):
            if stage_id == 'fundamentals_daily_refresh':
                self.assertEqual(kwargs['symbols'], exact_membership)
                return {
                    'status': 'partial',
                    'scope_outcome_manifest_version': (
                        'dart_daily_data_scope_outcomes_v1'
                    ),
                    'selected_stock_codes': exact_membership,
                    'successful_stock_codes': ['005930'],
                    'retry_debt_stock_codes': ['000660'],
                    'terminal_not_applicable_stock_codes': [],
                    'scope_outcome_manifest': [
                        {
                            'stock_code': '005930',
                            'outcome': 'success',
                        },
                        {
                            'stock_code': '000660',
                            'outcome': 'retry_debt',
                        },
                    ],
                }
            if stage_id == 'piotroski_filter':
                self.assertEqual(kwargs['symbols'], ['005930'])
                self.assertEqual(
                    kwargs['exact_scope_symbols'],
                    exact_membership,
                )
                return {
                    'status': 'completed',
                    'selected_stock_codes': ['005930'],
                    'successful_stock_codes': ['005930'],
                    'retry_debt_stock_codes': [],
                }
            raise AssertionError(f'unexpected stage: {stage_id}')

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'fundamentals_daily_refresh',
                    'piotroski_filter',
                ],
                symbols=exact_membership,
                exact_membership_symbols=exact_membership,
                markets=['KOSPI', 'KOSDAQ'],
                reused_eod_collection_run_id=904,
                point_in_time_recovery=True,
                trigger='exact_price_bar_repair',
            )

        piotroski_call = next(
            call
            for call in execute_stage.call_args_list
            if call.args[0] == 'piotroski_filter'
        )
        self.assertEqual(
            piotroski_call.kwargs['symbols'],
            ['005930'],
        )
        self.assertEqual(
            piotroski_call.kwargs['exact_scope_symbols'],
            exact_membership,
        )
        self.assertEqual(
            result['stages']['piotroski_filter']['result'][
                'requested_symbols'
            ],
            ['005930'],
        )

    def test_multiple_partial_dependencies_share_the_final_intersection_proof(
        self,
    ) -> None:
        selected_symbols = ['005930', '000660', '035420']
        pykrx_runner = _FakeExactScopePykrxStageRunner(
            invocation_proof='valid',
            selected_symbols=selected_symbols,
            successful_symbols=['005930', '000660'],
            retry_debt_symbols=['035420'],
        )
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)
        fundamentals_result = {
            'status': 'partial',
            'scope_outcome_manifest_version': (
                'dart_daily_data_scope_outcomes_v1'
            ),
            'selected_stock_codes': selected_symbols,
            'successful_stock_codes': ['000660', '035420'],
            'retry_debt_stock_codes': ['005930'],
            'terminal_not_applicable_stock_codes': [],
            'scope_outcome_manifest': [
                {'stock_code': '000660', 'outcome': 'success'},
                {'stock_code': '035420', 'outcome': 'success'},
                {'stock_code': '005930', 'outcome': 'retry_debt'},
            ],
        }

        def execute(stage_id, **kwargs):
            if stage_id == 'master_refresh':
                return {'status': 'completed'}
            if stage_id == 'fundamentals_daily_refresh':
                return deepcopy(fundamentals_result)
            if stage_id == 'quality_factors':
                return {
                    'status': 'completed',
                    'selected_stock_codes': list(kwargs['symbols']),
                }
            raise AssertionError(f'unexpected stage: {stage_id}')

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'price_collection',
                    'fundamentals_daily_refresh',
                    'quality_factors',
                ],
                symbols=selected_symbols,
            )

        quality_call = next(
            call
            for call in execute_stage.call_args_list
            if call.args[0] == 'quality_factors'
        )
        self.assertEqual(quality_call.kwargs['symbols'], ['000660'])
        proofs = result['stages']['quality_factors']['result'][
            'exact_partial_dependency_proofs'
        ]
        self.assertEqual(
            [proof['dependency'] for proof in proofs],
            ['price_collection', 'fundamentals_daily_refresh'],
        )
        self.assertEqual(
            [proof['admitted_stock_codes'] for proof in proofs],
            [['000660'], ['000660']],
        )

    def test_malformed_dart_scope_partitions_block_piotroski_continuation(
        self,
    ) -> None:
        valid_result = {
            'status': 'partial',
            'scope_outcome_manifest_version': (
                'dart_daily_data_scope_outcomes_v1'
            ),
            'selected_stock_codes': ['005930', '000660', '035420'],
            'successful_stock_codes': ['005930'],
            'retry_debt_stock_codes': ['000660'],
            'terminal_not_applicable_stock_codes': ['035420'],
            'scope_outcome_manifest': [
                {'stock_code': '005930', 'outcome': 'success'},
                {'stock_code': '000660', 'outcome': 'retry_debt'},
                {
                    'stock_code': '035420',
                    'outcome': 'terminal_not_applicable',
                },
            ],
        }
        malformed_results: dict[str, dict] = {}

        overlapping_partition = deepcopy(valid_result)
        overlapping_partition['successful_stock_codes'].append('000660')
        malformed_results['overlapping_partition'] = overlapping_partition

        duplicate_partition_symbol = deepcopy(valid_result)
        duplicate_partition_symbol['selected_stock_codes'].append('005930')
        malformed_results['duplicate_partition_symbol'] = (
            duplicate_partition_symbol
        )

        duplicate_manifest_symbol = deepcopy(valid_result)
        duplicate_manifest_symbol['scope_outcome_manifest'].append(
            {'stock_code': '005930', 'outcome': 'success'}
        )
        malformed_results['duplicate_manifest_symbol'] = (
            duplicate_manifest_symbol
        )

        mismatched_manifest = deepcopy(valid_result)
        mismatched_manifest['scope_outcome_manifest'][1]['outcome'] = 'success'
        malformed_results['mismatched_manifest'] = mismatched_manifest

        incomplete_invocation_scope = deepcopy(valid_result)
        incomplete_invocation_scope['selected_stock_codes'] = [
            '005930',
            '000660',
        ]
        incomplete_invocation_scope[
            'terminal_not_applicable_stock_codes'
        ] = []
        incomplete_invocation_scope['scope_outcome_manifest'] = [
            {'stock_code': '005930', 'outcome': 'success'},
            {'stock_code': '000660', 'outcome': 'retry_debt'},
        ]
        malformed_results['incomplete_invocation_scope'] = (
            incomplete_invocation_scope
        )

        for case_name, dart_result in malformed_results.items():
            with self.subTest(case=case_name):
                service = DailyPipelineStageService()

                def execute(stage_id, **_kwargs):
                    if stage_id == 'fundamentals_daily_refresh':
                        return deepcopy(dart_result)
                    if stage_id == 'pykrx_enrichment':
                        return {'status': 'completed'}
                    raise AssertionError(
                        'piotroski must not run from malformed scope evidence'
                    )

                with (
                    patch.object(
                        service,
                        '_normalize_market_date',
                        return_value='2026-04-14',
                    ),
                    patch.object(
                        service,
                        '_execute_stage',
                        side_effect=execute,
                    ) as execute_stage,
                ):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=[
                            'pykrx_enrichment',
                            'fundamentals_daily_refresh',
                            'piotroski_filter',
                        ],
                        symbols=['005930', '000660', '035420'],
                    )

                piotroski_stage = result['stages']['piotroski_filter']
                self.assertEqual(piotroski_stage['status'], 'blocked')
                self.assertEqual(
                    piotroski_stage['result']['incomplete_dependencies'],
                    ['fundamentals_daily_refresh'],
                )
                self.assertNotIn(
                    'piotroski_filter',
                    [
                        call.args[0]
                        for call in execute_stage.call_args_list
                    ],
                )

    def test_exact_partial_scope_is_bound_to_narrowed_invocation(self) -> None:
        result = {
            'scope_outcome_manifest_version': (
                'dart_daily_data_scope_outcomes_v1'
            ),
            'selected_stock_codes': ['005930', '000660'],
            'successful_stock_codes': ['005930'],
            'retry_debt_stock_codes': ['000660'],
            'terminal_not_applicable_stock_codes': [],
            'scope_outcome_manifest': [
                {'stock_code': '005930', 'outcome': 'success'},
                {'stock_code': '000660', 'outcome': 'retry_debt'},
            ],
        }
        outcome = {'status': 'partial', 'result': result}

        exact_scope = DailyPipelineStageService._exact_partial_success_scope(
            outcome=outcome,
            invocation_symbols=['005930', '000660'],
        )

        self.assertIsNotNone(exact_scope)
        self.assertEqual(
            exact_scope['successful_stock_codes'],
            ['005930'],
        )
        self.assertIsNone(
            DailyPipelineStageService._exact_partial_success_scope(
                outcome=outcome,
                invocation_symbols=['005930', '000660', '035420'],
            )
        )
        self.assertIsNone(
            DailyPipelineStageService._exact_partial_success_scope(
                outcome=outcome,
                invocation_symbols=None,
            )
        )

    def test_recovery_treats_terminal_optional_context_gaps_as_ordering_only(
        self,
    ) -> None:
        optional_context_results = (
            (
                'external_news_collect',
                {
                    'status': 'disabled',
                    'reason': 'provider_disabled',
                    'summary': {
                        'requested_symbol_count': 2,
                        'eligible_symbol_count': 0,
                        'collected_symbol_count': 0,
                    },
                },
                'skipped',
            ),
            (
                'external_news_enrich',
                {
                    'status': 'partial',
                    'items_inspected': 2,
                    'items_missing_classification': 1,
                    'items_remaining_at_least': 1,
                    'missing_item_ids': [101],
                },
                'partial',
            ),
            (
                'macro_context_refresh',
                {
                    'status': 'unavailable',
                    'observations_upserted': 0,
                    'observations_failed': 3,
                },
                'partial',
            ),
            (
                'macro_context_refresh',
                {
                    'status': 'partial',
                    'observations_upserted': 2,
                    'observations_failed': 1,
                },
                'partial',
            ),
        )

        for stage_id, optional_result, expected_status in optional_context_results:
            with self.subTest(stage_id=stage_id, optional_result=optional_result):
                run_state_service = _FakePipelineRunStateService()
                service = DailyPipelineStageService(
                    run_state_service=run_state_service,
                )

                def execute(current_stage_id, **_):
                    if current_stage_id == stage_id:
                        return dict(optional_result)
                    return {'status': 'completed', 'stage_id': current_stage_id}

                with (
                    patch.object(
                        service,
                        '_normalize_market_date',
                        return_value='2026-04-14',
                    ),
                    patch.object(
                        service,
                        '_execute_stage',
                        side_effect=execute,
                    ) as execute_stage,
                ):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=[stage_id, 'agent_feedback_summaries'],
                        trigger='auto_eod_followup_resume',
                    )

                self.assertEqual(
                    result['stages'][stage_id]['status'],
                    expected_status,
                )
                self.assertEqual(
                    result['stages']['agent_feedback_summaries']['status'],
                    'completed',
                )
                self.assertEqual(
                    [call.args[0] for call in execute_stage.call_args_list],
                    [stage_id, 'agent_feedback_summaries'],
                )
                self.assertTrue(daily_pipeline_outcome_is_satisfied(result))

    def test_run_blocks_high_coverage_factor_partial_without_identity_manifest(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'momentum_factors':
                return {
                    'status': 'partial',
                    'results_persisted': 99,
                    'results_failed': 1,
                    'remaining_retryable_events': 0,
                }
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors', 'composite_factors'],
                trigger='auto_eod_followup_resume',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'partial')
        self.assertEqual(result['stages']['composite_factors']['status'], 'blocked')
        self.assertFalse(
            result['pipeline_run']['run_metadata'].get('resumable', False)
        )
        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['momentum_factors'],
        )

    def test_run_blocks_explicit_nonblocking_low_coverage_without_identity_manifest(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'advanced_value_factors':
                return {
                    'status': 'partial',
                    'reason': 'partial_usable_coverage',
                    'gate_status': 'ready',
                    'blocking_status': 'non_blocking',
                    'results_persisted': 2_675,
                    'results_failed': 0,
                    'remaining_retryable_events': 0,
                    'summary': {
                        'universe_count': 2_675,
                        'persisted_count': 2_675,
                        'ready_count': 300,
                        'complete_count': 300,
                        'partial_count': 0,
                        'unusable_count': 2_375,
                    },
                }
            return {'status': 'completed', 'stage_id': stage_id}

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['advanced_value_factors', 'composite_factors'],
                trigger='auto_eod_followup_resume',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            result['stages']['advanced_value_factors']['status'],
            'partial',
        )
        self.assertEqual(
            result['stages']['composite_factors']['status'],
            'blocked',
        )
        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            ['advanced_value_factors'],
        )

    def test_run_blocks_unproven_factor_partial_dependency(self) -> None:
        service = DailyPipelineStageService()

        for partial_result in (
            {
                'status': 'partial',
                'results_persisted': 98,
                'results_failed': 2,
                'remaining_retryable_events': 0,
            },
            {
                'status': 'partial',
                'results_persisted': 99,
                'results_failed': 1,
                'retry_cycles_requested': 3,
                'retry_cycles_completed': 1,
                'remaining_retryable_events': 1,
            },
        ):
            with self.subTest(partial_result=partial_result):
                with (
                    patch.object(
                        service,
                        '_normalize_market_date',
                        return_value='2026-04-14',
                    ),
                    patch.object(
                        service,
                        '_execute_stage',
                        side_effect=[partial_result],
                    ) as execute_stage,
                ):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=['momentum_factors', 'composite_factors'],
                    )

                self.assertEqual(
                    result['stages']['composite_factors']['status'],
                    'blocked',
                )
                self.assertEqual(execute_stage.call_count, 1)

    def test_run_resume_from_market_risk_filter_preserves_upstream_outputs_by_not_rerunning_them(self) -> None:
        service = DailyPipelineStageService()

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=lambda stage_id, **_: {'stage_id': stage_id}) as execute_stage,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': False,
                    'reason': 'summary_cutoff_proof_unavailable',
                    'knowledge_cutoff_at': None,
                    'source_run_id': None,
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                resume_from_stage='market_risk_filter',
            )

        self.assertEqual(result['resolved_stages'][0], 'market_risk_filter')
        self.assertNotIn('price_collection', result['resolved_stages'])
        self.assertNotIn('technical_snapshots', result['resolved_stages'])
        self.assertEqual(execute_stage.call_args_list[0].args[0], 'market_risk_filter')
        self.assertEqual(self.run_state_service.runs[0]['run_metadata']['resume_from_stage'], 'market_risk_filter')

    def test_run_resume_from_market_risk_snapshots_includes_requested_resume_stage(self) -> None:
        service = DailyPipelineStageService()

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=lambda stage_id, **_: {'stage_id': stage_id}) as execute_stage,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': False,
                    'reason': 'summary_cutoff_proof_unavailable',
                    'knowledge_cutoff_at': None,
                    'source_run_id': None,
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                resume_from_stage='market_risk_snapshots',
            )

        self.assertEqual(result['resolved_stages'][0], 'market_risk_snapshots')
        self.assertIn('market_risk_snapshots', result['requested_stages'])
        self.assertEqual(execute_stage.call_args_list[0].args[0], 'market_risk_snapshots')
        self.assertEqual(self.run_state_service.runs[0]['run_metadata']['resume_from_stage'], 'market_risk_snapshots')

    def test_run_starting_at_market_risk_filter_checkpoints_progress_before_rows_exist(self) -> None:
        service = DailyPipelineStageService()

        def _execute(stage_id, **kwargs):
            kwargs['progress_callback'](
                status='running',
                current_step='fetching_kis_market_risk_snapshots',
                current_activity='Fetching KIS market-risk snapshot for 005930',
                processed=1,
                total=2,
                succeeded=1,
                failed=0,
                current_symbol='005930',
            )
            return {
                'market_date': kwargs['market_date'],
                'matched_symbols': ['005930', '000660'],
                'results_persisted': 0,
                'results_failed': 0,
                'items': [],
            }

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=_execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['market_risk_filter'],
                symbols=['005930', '000660'],
            )

        self.assertEqual(result['status'], 'completed')
        progress_events = [
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'update_stage' and event['stage_id'] == 'market_risk_filter'
        ]
        self.assertEqual(progress_events[0]['status'], 'running')
        self.assertEqual(progress_events[0]['processed'], 1)
        self.assertEqual(progress_events[0]['total'], 2)
        self.assertEqual(progress_events[0]['current_symbol'], '005930')

    def test_run_gates_feedback_stage_until_prerequisites_are_ready(self) -> None:
        pykrx_runner = _FakePykrxStageRunner()
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            pykrx_service=pykrx_runner,
            feedback_retry_service=feedback_retry_service,
        )

        with patch(
            'app.eod.daily_pipeline_stages.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            side_effect=lambda **_: _feedback_prerequisites(
                status='waiting',
                missing_stage_ids=['fundamentals_daily_refresh', 'quality_factors'],
            ),
        ) as inspect_prerequisites:
            result = service.run(
                market_date='2026-04-14',
                stages=['agent_feedback_summaries'],
                symbols=['005930', '000660'],
            )

        self.assertEqual(result['status'], 'waiting')
        self.assertEqual(result['stages']['agent_feedback_summaries']['status'], 'waiting')
        self.assertEqual(
            result['stages']['agent_feedback_summaries']['result']['reason'],
            'feedback_prerequisites_not_ready',
        )
        self.assertEqual(feedback_retry_service.calls, [])
        self.assertEqual(
            inspect_prerequisites.call_args.kwargs['symbols'],
            ['005930', '000660'],
        )

    def test_suffix_recovery_cannot_bypass_missing_persisted_prerequisites(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with patch(
            'app.eod.daily_pipeline_stages.'
            'agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            return_value=_feedback_prerequisites(
                status='waiting',
                missing_stage_ids=[
                    'momentum_factors',
                    'beta_factors',
                    'signal_features',
                ],
            ),
        ) as inspect_prerequisites:
            for force in (False, True):
                with self.subTest(force=force):
                    result = service.run(
                        market_date='2026-04-14',
                        stages=[
                            'agent_feedback_summaries',
                            'signal_decider',
                            'next_open_execution_plan',
                        ],
                        force=force,
                        bypass_prerequisite_gate=True,
                        trigger='stage_orchestrator',
                    )

                    self.assertEqual(result['status'], 'waiting')
                    self.assertEqual(
                        result['reason'],
                        'feedback_prerequisites_not_ready',
                    )
                    self.assertIsNone(result['pipeline_run_id'])
        self.assertEqual(inspect_prerequisites.call_count, 2)
        self.assertTrue(
            all(
                call.kwargs['required_stage_ids']
                == CURRENT_FEEDBACK_PREREQUISITE_STAGE_ORDER
                for call in inspect_prerequisites.call_args_list
            )
        )
        self.assertEqual(self.run_state_service.runs, [])
        self.assertEqual(feedback_retry_service.calls, [])

    def test_expired_analysis_replay_can_explicitly_bypass_preflight(self) -> None:
        service = DailyPipelineStageService()

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
            ) as inspect_prerequisites,
            patch.object(
                service,
                '_execute_stage',
                return_value={'status': 'completed'},
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['agent_feedback_summaries'],
                bypass_prerequisite_gate=True,
                allow_expired_analysis_replay=True,
            )

        self.assertEqual(result['status'], 'completed')
        inspect_prerequisites.assert_not_called()
        self.assertTrue(
            execute_stage.call_args.kwargs['bypass_prerequisite_gate']
        )
        self.assertTrue(
            execute_stage.call_args.kwargs[
                'allow_expired_analysis_replay'
            ]
        )

    def test_point_in_time_suffix_keeps_date_bounded_numeric_prerequisites(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        with patch(
            'app.eod.daily_pipeline_stages.'
            'agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            return_value=_feedback_prerequisites(),
        ) as inspect_prerequisites:
            result = service.run(
                market_date='2026-04-14',
                stages=['agent_feedback_summaries'],
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertNotIn(
            'signal_features',
            inspect_prerequisites.call_args.kwargs['required_stage_ids'],
        )
        self.assertEqual(len(feedback_retry_service.calls), 1)

    def test_suffix_recovery_with_ready_persisted_prerequisites_remains_allowed(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )

        result = service.run(
            market_date='2026-04-14',
            stages=['agent_feedback_summaries'],
            force=True,
            bypass_prerequisite_gate=True,
            reuse_existing_feedback=True,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(len(self.run_state_service.runs), 1)
        self.assertEqual(len(feedback_retry_service.calls), 1)

    def test_factor_suffix_preflight_excludes_prerequisites_planned_in_run(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        planned_stages = [
            *FACTOR_STAGE_ORDER,
            'signal_features',
            'agent_feedback_summaries',
        ]

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value=_feedback_prerequisites(),
            ) as inspect_prerequisites,
            patch.object(
                service,
                '_execute_stage',
                return_value={
                    'triggered': True,
                    'status': 'completed',
                    'results_persisted': 1,
                    'results_failed': 0,
                },
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=planned_stages,
                resume_from_stage='momentum_factors',
            )

        self.assertEqual(result['status'], 'completed')
        required_stage_ids = (
            inspect_prerequisites.call_args.kwargs['required_stage_ids']
        )
        self.assertNotIn('momentum_factors', required_stage_ids)
        self.assertNotIn('signal_features', required_stage_ids)
        self.assertIn('fundamentals_daily_refresh', required_stage_ids)
        self.assertEqual(
            [
                call.kwargs.get('stage_id')
                or (call.args[0] if call.args else None)
                for call in execute_stage.call_args_list
            ],
            planned_stages,
        )
        self.assertEqual(len(self.run_state_service.runs), 1)

    def test_factor_stage_heartbeats_while_synchronous_work_is_running(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            self.assertEqual(stage_id, 'momentum_factors')
            self.assertTrue(
                self.run_state_service.heartbeat_event.wait(timeout=0.5),
                'factor stage did not emit a heartbeat while work was active',
            )
            return {
                'status': 'completed',
                'results_persisted': 2,
                'results_failed': 0,
            }

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS',
                0.01,
                create=True,
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors'],
            )

        self.assertEqual(result['status'], 'completed')
        heartbeat_events = [
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'heartbeat_stage'
        ]
        self.assertTrue(heartbeat_events)
        self.assertEqual(heartbeat_events[0]['stage_id'], 'momentum_factors')

    def test_factor_pipeline_stops_before_next_stage_after_ownership_loss(
        self,
    ) -> None:
        self.run_state_service.heartbeat_results = [True, False]
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_):
            executed_stages.append(stage_id)
            if stage_id != 'momentum_factors':
                raise AssertionError(
                    'pipeline must stop before the next factor after ownership loss'
                )
            self.assertTrue(
                self.run_state_service.ownership_lost_event.wait(timeout=0.5),
                'ownership loss was not observed during the active factor',
            )
            return {
                'status': 'completed',
                'results_persisted': 2,
                'results_failed': 0,
            }

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS',
                0.01,
                create=True,
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors', 'value_factors'],
            )

        self.assertEqual(executed_stages, ['momentum_factors'])
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')

    def test_factor_stage_is_not_started_when_initial_ownership_check_fails(
        self,
    ) -> None:
        self.run_state_service.heartbeat_results = [False]
        service = DailyPipelineStageService()

        with patch.object(service, '_execute_stage') as execute_stage:
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors'],
            )

        execute_stage.assert_not_called()
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')
        self.assertNotIn('finish', self.run_state_service.runs[-1])

    def test_factor_stage_is_not_started_when_initial_heartbeat_write_fails(
        self,
    ) -> None:
        self.run_state_service.heartbeat_stage = MagicMock(
            side_effect=RuntimeError('heartbeat storage unavailable')
        )
        service = DailyPipelineStageService()

        with (
            patch.object(service, '_execute_stage') as execute_stage,
            self.assertRaisesRegex(
                DailyPipelineStageCheckpointError,
                'heartbeat storage unavailable',
            ),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['momentum_factors'],
            )

        execute_stage.assert_not_called()
        self.assertEqual(
            self.run_state_service.runs[-1]['status'],
            'failed',
        )

    def test_factor_pipeline_stops_after_background_heartbeat_write_fails(
        self,
    ) -> None:
        heartbeat_failed = threading.Event()
        heartbeat_calls = 0

        def heartbeat(**_kwargs):
            nonlocal heartbeat_calls
            heartbeat_calls += 1
            if heartbeat_calls == 1:
                return True
            heartbeat_failed.set()
            raise RuntimeError('background heartbeat storage unavailable')

        self.run_state_service.heartbeat_stage = MagicMock(
            side_effect=heartbeat
        )
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_kwargs):
            executed_stages.append(stage_id)
            self.assertEqual(stage_id, 'momentum_factors')
            self.assertTrue(heartbeat_failed.wait(timeout=0.5))
            return {
                'status': 'completed',
                'results_persisted': 2,
                'results_failed': 0,
            }

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS',
                0.01,
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
            self.assertRaisesRegex(
                DailyPipelineStageCheckpointError,
                'background heartbeat storage unavailable',
            ),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['momentum_factors', 'value_factors'],
            )

        self.assertEqual(executed_stages, ['momentum_factors'])
        self.assertEqual(
            self.run_state_service.runs[-1]['status'],
            'failed',
        )

    def test_stage_start_failure_retries_run_terminalization_and_preserves_error(
        self,
    ) -> None:
        self.run_state_service.start_stage = MagicMock(
            side_effect=RuntimeError('stage checkpoint unavailable')
        )
        self.run_state_service.finish_run = MagicMock(
            side_effect=[
                RuntimeError('finish timeout 1'),
                RuntimeError('finish timeout 2'),
                {'id': 9000, 'status': 'failed'},
            ]
        )
        service = DailyPipelineStageService()

        with self.assertRaisesRegex(
            RuntimeError,
            'stage checkpoint unavailable',
        ):
            service.run(
                market_date='2026-04-14',
                stages=['momentum_factors'],
            )

        self.assertEqual(self.run_state_service.finish_run.call_count, 3)
        for finish_call in self.run_state_service.finish_run.call_args_list:
            self.assertEqual(finish_call.kwargs['status'], 'failed')
            self.assertEqual(
                finish_call.kwargs['error_message'],
                'stage checkpoint unavailable',
            )

    def test_final_run_checkpoint_retries_transient_failures(self) -> None:
        self.run_state_service.finish_run = MagicMock(
            side_effect=[
                RuntimeError('finish timeout 1'),
                RuntimeError('finish timeout 2'),
                {'id': 9000, 'status': 'completed'},
            ]
        )
        service = DailyPipelineStageService()

        with patch.object(
            service,
            '_execute_stage',
            return_value={
                'triggered': True,
                'status': 'completed',
                'symbols_refreshed': 1,
                'symbols_failed': 0,
            },
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['master_refresh'],
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(self.run_state_service.finish_run.call_count, 3)

    def test_optional_context_partial_completes_run_with_degraded_quality(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        with patch.object(
            service,
            '_execute_stage',
            return_value={
                'triggered': True,
                'status': 'partial',
                'context_status': 'stale_cache',
                'remaining_retryable_events': 0,
            },
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['macro_context_refresh'],
            )

        self.assertEqual(
            result['stages']['macro_context_refresh']['status'],
            'partial',
        )
        self.assertEqual(result['status'], 'completed')
        finish = self.run_state_service.runs[-1]['finish']
        self.assertEqual(finish['status'], 'completed')
        self.assertEqual(
            finish['run_metadata']['quality_status'],
            'degraded',
        )
        self.assertEqual(
            finish['run_metadata']['warning_stage_ids'],
            ['macro_context_refresh'],
        )
        self.assertEqual(finish['run_metadata']['warning_count'], 1)

    def test_stage_failure_preserves_error_when_stage_cleanup_fails(self) -> None:
        self.run_state_service.finish_stage = MagicMock(
            side_effect=RuntimeError('stage cleanup unavailable')
        )
        self.run_state_service.finish_run = MagicMock(
            return_value={'id': 9000, 'status': 'failed'}
        )
        service = DailyPipelineStageService()

        with (
            patch.object(
                service,
                '_execute_stage',
                side_effect=ValueError('stage body failed'),
            ),
            self.assertRaisesRegex(ValueError, 'stage body failed'),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['master_refresh'],
            )

        self.run_state_service.finish_run.assert_called_once()
        self.assertEqual(
            self.run_state_service.finish_run.call_args.kwargs['status'],
            'failed',
        )

    def test_recoverable_factor_stage_failure_continues_independent_branches(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_kwargs):
            executed_stages.append(stage_id)
            if stage_id == 'value_factors':
                return {
                    'triggered': True,
                    'terminal': True,
                    'retry_pending': True,
                    'retryable': True,
                    'status': 'failed',
                    'stage': stage_id,
                    'reason': 'valuation_provider_unavailable',
                    'failure_reason': 'valuation_provider_unavailable',
                    'error_type': 'ValuationProviderError',
                    'error_message': 'valuation provider unavailable',
                    'market_date': '2026-04-14',
                    'results_failed': 0,
                    'total': 0,
                }
            return {
                'status': 'completed',
                'results_persisted': 2,
                'results_failed': 0,
            }

        with patch.object(service, '_execute_stage', side_effect=execute):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'value_factors',
                    'low_risk_factors',
                    'beta_factors',
                    'composite_factors',
                ],
            )

        self.assertEqual(
            executed_stages,
            ['value_factors', 'low_risk_factors', 'beta_factors'],
        )
        self.assertEqual(
            result['stages']['value_factors']['status'],
            'failed',
        )
        self.assertEqual(
            result['stages']['low_risk_factors']['status'],
            'completed',
        )
        self.assertEqual(
            result['stages']['beta_factors']['status'],
            'completed',
        )
        self.assertEqual(
            result['stages']['composite_factors']['status'],
            'blocked',
        )
        self.assertEqual(
            result['stages']['composite_factors']['result'][
                'incomplete_dependencies'
            ],
            ['value_factors'],
        )
        failed_checkpoint = next(
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'finish_stage'
            and event['stage_id'] == 'value_factors'
        )
        self.assertEqual(failed_checkpoint['status'], 'failed')
        self.assertEqual(
            failed_checkpoint['details']['result_summary']['error_type'],
            'ValuationProviderError',
        )
        self.assertEqual(failed_checkpoint['processed'], 0)
        self.assertEqual(failed_checkpoint['total'], 0)
        self.assertEqual(failed_checkpoint['failed'], 0)
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            self.run_state_service.runs[-1]['finish']['status'],
            'partial',
        )

    def test_factor_stage_integrity_error_remains_fatal(self) -> None:
        service = DailyPipelineStageService()
        executed_stages: list[str] = []
        integrity_error = IntegrityError(
            'insert into daily_value_factor_results',
            {},
            RuntimeError('constraint violation'),
        )

        def execute(stage_id, **_kwargs):
            executed_stages.append(stage_id)
            raise integrity_error

        with (
            patch.object(service, '_execute_stage', side_effect=execute),
            self.assertRaises(IntegrityError),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['value_factors', 'low_risk_factors'],
            )

        self.assertEqual(executed_stages, ['value_factors'])
        self.assertEqual(
            self.run_state_service.runs[-1]['finish']['status'],
            'failed',
        )

    def test_unknown_factor_stage_exception_remains_fatal(self) -> None:
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_kwargs):
            executed_stages.append(stage_id)
            raise KeyError('unsafe factor invariant')

        with (
            patch.object(service, '_execute_stage', side_effect=execute),
            self.assertRaisesRegex(KeyError, 'unsafe factor invariant'),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['value_factors', 'low_risk_factors'],
            )

        self.assertEqual(executed_stages, ['value_factors'])
        self.assertEqual(
            self.run_state_service.runs[-1]['finish']['status'],
            'failed',
        )

    def test_builtin_factor_stage_errors_remain_fatal(self) -> None:
        cases = (
            RuntimeError('internal invariant violated'),
            ValueError('malformed persisted payload'),
            OSError('disk full'),
        )
        for exc in cases:
            with self.subTest(error_type=type(exc).__name__):
                service = DailyPipelineStageService()
                with (
                    patch.object(
                        service,
                        '_execute_stage',
                        side_effect=exc,
                    ),
                    self.assertRaisesRegex(type(exc), str(exc)),
                ):
                    service.run(
                        market_date='2026-04-14',
                        stages=['value_factors', 'low_risk_factors'],
                    )

                self.assertEqual(
                    self.run_state_service.runs[-1]['finish']['status'],
                    'failed',
                )

    def test_factor_progress_checkpoint_failure_remains_fatal(self) -> None:
        service = DailyPipelineStageService()
        self.run_state_service.update_stage = MagicMock(
            side_effect=RuntimeError('checkpoint write unavailable')
        )

        def execute(_stage_id, **kwargs):
            kwargs['progress_callback'](
                status='running',
                current_step='factor_batch',
            )
            return {'status': 'completed'}

        with (
            patch.object(service, '_execute_stage', side_effect=execute),
            self.assertRaisesRegex(
                DailyPipelineStageCheckpointError,
                'checkpoint write unavailable',
            ),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['value_factors', 'low_risk_factors'],
            )

        self.assertEqual(
            self.run_state_service.runs[-1]['finish']['status'],
            'failed',
        )

    def test_pipeline_tracking_write_error_remains_fatal(self) -> None:
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_kwargs):
            executed_stages.append(stage_id)
            raise DailyPipelineTrackingWriteError(
                'official pipeline checkpoint unavailable'
            )

        with (
            patch.object(service, '_execute_stage', side_effect=execute),
            self.assertRaisesRegex(
                DailyPipelineTrackingWriteError,
                'official pipeline checkpoint unavailable',
            ),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['value_factors', 'low_risk_factors'],
            )

        self.assertEqual(executed_stages, ['value_factors'])
        self.assertEqual(
            self.run_state_service.runs[-1]['finish']['status'],
            'failed',
        )

    def test_failed_stage_checkpoint_failure_closes_parent_run(
        self,
    ) -> None:
        self.run_state_service.finish_stage = MagicMock(
            side_effect=RuntimeError(
                'failed stage checkpoint unavailable'
            )
        )
        self.run_state_service.finish_run = MagicMock(
            return_value={'id': 9000, 'status': 'failed'}
        )
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_kwargs):
            executed_stages.append(stage_id)
            return {
                'status': 'failed',
                'reason': 'valuation_provider_unavailable',
                'results_failed': 0,
                'total': 0,
            }

        with (
            patch.object(service, '_execute_stage', side_effect=execute),
            self.assertRaisesRegex(
                RuntimeError,
                'failed stage checkpoint unavailable',
            ),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['value_factors', 'low_risk_factors'],
            )

        self.assertEqual(executed_stages, ['value_factors'])
        self.assertEqual(self.run_state_service.finish_stage.call_count, 4)
        self.run_state_service.finish_run.assert_called_once()
        self.assertEqual(
            self.run_state_service.finish_run.call_args.kwargs['status'],
            'failed',
        )
        self.assertEqual(
            self.run_state_service.finish_run.call_args.kwargs[
                'error_message'
            ],
            'failed stage checkpoint unavailable',
        )

    def test_post_execute_stage_exception_is_not_isolated(self) -> None:
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_kwargs):
            executed_stages.append(stage_id)
            return {
                'status': 'completed',
                'results_persisted': 2,
                'results_failed': 0,
            }

        with (
            patch.object(service, '_execute_stage', side_effect=execute),
            patch.object(
                service,
                '_build_stage_outcome',
                side_effect=RuntimeError(
                    'post-execute outcome processing failed'
                ),
            ),
            self.assertRaisesRegex(
                RuntimeError,
                'post-execute outcome processing failed',
            ),
        ):
            service.run(
                market_date='2026-04-14',
                stages=['value_factors', 'low_risk_factors'],
            )

        self.assertEqual(executed_stages, ['value_factors'])
        self.assertEqual(
            self.run_state_service.runs[-1]['finish']['status'],
            'failed',
        )

    def test_stage_terminal_checkpoint_failure_defers_parent_close_to_run_handler(
        self,
    ) -> None:
        self.run_state_service.finish_stage = MagicMock(
            side_effect=RuntimeError('stage terminal checkpoint unavailable')
        )
        self.run_state_service.finish_run = MagicMock(
            return_value={'id': 9000, 'status': 'failed'}
        )
        service = DailyPipelineStageService()

        with self.assertRaisesRegex(
            RuntimeError,
            'stage terminal checkpoint unavailable',
        ):
            service._finish_tracked_stage(
                pipeline_run_id=9000,
                stage_id='master_refresh',
                market_date='2026-04-14',
                outcome={'status': 'completed'},
                result={
                    'status': 'completed',
                    'symbols_refreshed': 1,
                    'symbols_failed': 0,
                },
            )

        self.assertEqual(self.run_state_service.finish_stage.call_count, 3)
        self.run_state_service.finish_run.assert_not_called()

    def test_missing_strict_completion_authority_mutates_stage_outcome(self) -> None:
        service = DailyPipelineStageService()
        outcome = {'status': 'completed'}

        service._finish_tracked_stage(
            pipeline_run_id=9000,
            stage_id='next_open_execution_plan',
            market_date='2026-04-14',
            outcome=outcome,
            result={
                'status': 'completed',
                'items': [],
            },
        )

        self.assertEqual(outcome['status'], 'partial')
        finish = self.run_state_service.stage_events[-1]
        self.assertEqual(finish['event'], 'finish_stage')
        self.assertEqual(finish['status'], 'partial')
        self.assertEqual(
            finish['details']['completion_authority_status'],
            'invalid',
        )
        self.assertEqual(
            finish['details']['completion_authority_reason'],
            'terminal_progression_proof_unavailable',
        )

    def test_factor_exception_after_ownership_loss_avoids_late_state_writes(
        self,
    ) -> None:
        self.run_state_service.heartbeat_results = [True, False]
        service = DailyPipelineStageService()
        executed_stages: list[str] = []

        def execute(stage_id, **_):
            executed_stages.append(stage_id)
            self.assertEqual(stage_id, 'momentum_factors')
            self.assertTrue(
                self.run_state_service.ownership_lost_event.wait(timeout=0.5),
            )
            raise RuntimeError('factor failed after ownership loss')

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS',
                0.01,
            ),
            patch.object(service, '_execute_stage', side_effect=execute),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['momentum_factors', 'value_factors'],
            )

        self.assertEqual(executed_stages, ['momentum_factors'])
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')
        self.assertNotIn('finish', self.run_state_service.runs[-1])
        self.assertFalse(
            any(
                event['event'] == 'finish_stage'
                for event in self.run_state_service.stage_events
            )
        )

    def test_fundamentals_stage_checkpoints_dart_progress(self) -> None:
        service = DailyPipelineStageService()

        async def _fake_refresh(**kwargs):
            kwargs['progress_callback'](
                status='running',
                current_step='syncing_disclosures',
                current_activity='DART fundamentals daily refresh: syncing disclosures',
                processed=1,
                total=3,
                succeeded=1,
                failed=0,
                current_symbol='005930',
                current_period='CFS',
                source_run_ids={'dart_refresh_run_id': 77},
            )
            return {
                'triggered': True,
                'run_id': 77,
                'status': 'completed',
                'symbols_refreshed': 3,
                'symbols_failed': 0,
                'events_captured': 3,
                'events_failed': 0,
            }

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch(
                'app.eod.daily_pipeline_stages.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                side_effect=_fake_refresh,
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['fundamentals_daily_refresh'],
            )

        self.assertEqual(result['status'], 'completed')
        progress_events = [
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'update_stage' and event['stage_id'] == 'fundamentals_daily_refresh'
        ]
        self.assertEqual(progress_events[0]['current_step'], 'syncing_disclosures')
        self.assertEqual(progress_events[0]['processed'], 1)
        self.assertEqual(progress_events[0]['source_run_ids']['dart_refresh_run_id'], 77)

    def test_agent_feedback_stage_checkpoints_retry_progress(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(feedback_retry_service=feedback_retry_service)

        result = service.run(
            market_date='2026-04-14',
            stages=['agent_feedback_summaries'],
            symbols=['005930', '000660'],
        )

        self.assertEqual(result['status'], 'completed')
        self.assertIsNotNone(feedback_retry_service.calls[0]['config'].progress_callback)
        progress_events = [
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'update_stage' and event['stage_id'] == 'agent_feedback_summaries'
        ]
        self.assertEqual(progress_events[0]['current_step'], 'role_started')
        self.assertEqual(progress_events[0]['processed'], 1)
        self.assertEqual(progress_events[0]['source_run_ids']['agent_feedback_run_id'], 'fake-feedback-run')

    def test_forced_feedback_recovery_reuses_existing_rows(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(feedback_retry_service=feedback_retry_service)

        result = service.run(
            market_date='2026-04-14',
            stages=['agent_feedback_summaries'],
            force=True,
            bypass_prerequisite_gate=True,
            reuse_existing_feedback=True,
        )

        self.assertEqual(result['status'], 'completed')
        call = feedback_retry_service.calls[0]
        self.assertFalse(call['force'])
        self.assertTrue(call['bypass_prerequisite_gate'])
        self.assertTrue(call['config'].reuse_existing)

    def test_point_in_time_feedback_recovery_enables_durable_revalidation(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service
        )

        with patch(
            'app.eod.daily_pipeline_stages.build_expired_decision_replay_result',
            return_value=None,
        ):
            result = service._execute_stage(
                stage_id='agent_feedback_summaries',
                market_date='2026-04-14',
                symbols=['005930'],
                markets=['KOSPI'],
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
                point_in_time_recovery=True,
            )

        self.assertTrue(result['completed'])
        self.assertTrue(
            feedback_retry_service.calls[0][
                'config'
            ].point_in_time_recovery
        )

    def test_feedback_rearm_keeps_parent_prerequisite_admission(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )
        cutoff = datetime(
            2026,
            4,
            14,
            8,
            30,
            tzinfo=timezone.utc,
        )
        symbols = ['005930', '000660']
        role_symbol_scopes = {
            role: list(symbols)
            for role in SUMMARIZER_AGENT_NAMES
        }
        inherited_retry_config = {
            'source_run_id': 'feedback-cycle-parent',
            'roles': list(SUMMARIZER_AGENT_NAMES),
            'requested_symbols': list(symbols),
            'role_symbol_scopes': role_symbol_scopes,
            'knowledge_cutoff_at': cutoff,
            'feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            'fs_div': 'OFS',
            'policy_context_as_of_date': date(2026, 4, 15),
            'scope_authority': None,
            'idempotency_key': None,
            'outer_batch_size': 17,
        }
        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value=_feedback_prerequisites(
                    status='blocked',
                    missing_stage_ids=['fundamentals_daily_refresh'],
                ),
            ) as inspect_prerequisites,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'fake-feedback-run',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': list(symbols),
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': list(symbols),
                        'admitted_symbols': list(symbols),
                    },
                },
            ),
            patch(
                'app.orders.next_open_execution.'
                'SignalDeciderNextOpenExecutionService.'
                '_refresh_plan_items_once',
                return_value={
                    'status': 'completed',
                    'items_refreshed': len(symbols),
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                resume_from_stage='agent_feedback_summaries',
                trigger='auto_eod_followup_resume',
                point_in_time_recovery=True,
                symbols=symbols,
                fs_div='OFS',
                inherit_feedback_prerequisite_admission=True,
                inherited_feedback_knowledge_cutoff_at=cutoff,
                inherited_feedback_source_run_id=(
                    'feedback-cycle-parent'
                ),
                inherited_feedback_retry_config=(
                    inherited_retry_config
                ),
                feedback_max_cycles=4,
                feedback_outer_batch_size=17,
                reuse_existing_feedback=True,
                reused_eod_collection_run_id=7,
            )

        self.assertEqual(
            result['stages']['agent_feedback_summaries']['status'],
            'completed',
        )
        self.assertGreaterEqual(inspect_prerequisites.call_count, 2)
        self.assertGreaterEqual(len(feedback_retry_service.calls), 1)
        call = feedback_retry_service.calls[0]
        self.assertFalse(call['force'])
        self.assertTrue(call['bypass_prerequisite_gate'])
        self.assertTrue(call['config'].reuse_existing)
        self.assertEqual(
            call['config'].reuse_parent_run_id,
            'feedback-cycle-parent',
        )
        self.assertEqual(
            call['config'].knowledge_cutoff_at,
            cutoff,
        )
        self.assertEqual(call['config'].symbols, tuple(symbols))
        self.assertEqual(
            call['config'].role_symbol_scopes,
            role_symbol_scopes,
        )
        self.assertEqual(
            call['config'].feedback_scope_mode,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        )
        self.assertEqual(call['config'].fs_div, 'OFS')
        self.assertEqual(
            call['config'].policy_context_as_of_date,
            date(2026, 4, 15),
        )
        self.assertIsNone(call['config'].scope_authority)
        self.assertEqual(call['config'].outer_batch_size, 17)
        self.assertEqual(call['config'].max_cycles, 4)
        historical_metadata = self.run_state_service.runs[-1][
            'run_metadata'
        ]
        self.assertEqual(
            historical_metadata['inherited_feedback_source_run_id'],
            'feedback-cycle-parent',
        )
        self.assertEqual(
            historical_metadata[
                'inherited_feedback_knowledge_cutoff_at'
            ],
            cutoff.isoformat(),
        )
        self.assertEqual(
            historical_metadata['feedback_retry_parent_contract'],
            {
                'schema_version': 'feedback_retry_parent_v1',
                'source_run_id': 'feedback-cycle-parent',
                'knowledge_cutoff_at': cutoff.isoformat(),
                'feedback_scope_mode': (
                    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                ),
            },
        )

        same_day_retry_service = _FakeFeedbackRetryService()
        same_day_service = DailyPipelineStageService(
            feedback_retry_service=same_day_retry_service,
        )
        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value=_feedback_prerequisites(),
            ) as inspect_same_day_prerequisites,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'fake-feedback-run',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': list(symbols),
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': list(symbols),
                        'admitted_symbols': list(symbols),
                    },
                },
            ),
            patch(
                'app.orders.next_open_execution.'
                'SignalDeciderNextOpenExecutionService.'
                '_refresh_plan_items_once',
                return_value={
                    'status': 'completed',
                    'items_refreshed': len(symbols),
                },
            ),
        ):
            same_day_result = same_day_service.run(
                market_date='2026-04-14',
                stages=[
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                resume_from_stage='agent_feedback_summaries',
                trigger='auto_eod_followup_resume',
                symbols=symbols,
                fs_div='OFS',
                inherited_feedback_retry_config=inherited_retry_config,
                feedback_max_cycles=4,
                feedback_outer_batch_size=17,
                reuse_existing_feedback=True,
                reused_eod_collection_run_id=7,
            )

        self.assertEqual(
            same_day_result['stages']['agent_feedback_summaries'][
                'status'
            ],
            'completed',
        )
        self.assertGreaterEqual(
            inspect_same_day_prerequisites.call_count,
            1,
        )
        same_day_call = same_day_retry_service.calls[0]
        self.assertFalse(same_day_call['bypass_prerequisite_gate'])
        self.assertEqual(
            same_day_call['config'].reuse_parent_run_id,
            'feedback-cycle-parent',
        )
        self.assertEqual(same_day_call['config'].knowledge_cutoff_at, cutoff)
        self.assertEqual(same_day_call['config'].symbols, tuple(symbols))
        self.assertEqual(
            same_day_call['config'].feedback_scope_mode,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        )
        same_day_metadata = self.run_state_service.runs[-1][
            'run_metadata'
        ]
        self.assertEqual(
            same_day_metadata['inherited_feedback_source_run_id'],
            'feedback-cycle-parent',
        )
        self.assertEqual(
            same_day_metadata['feedback_retry_parent_contract'][
                'feedback_scope_mode'
            ],
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        )

    def test_decider_only_suffix_rejects_summary_retry_config(self) -> None:
        service = DailyPipelineStageService()
        cutoff = datetime(
            2026,
            4,
            14,
            8,
            30,
            tzinfo=timezone.utc,
        )

        with self.assertRaisesRegex(
            ValueError,
            'inherited feedback retry config requires the exact automatic '
            'feedback suffix',
        ):
            service.run(
                market_date='2026-04-14',
                stages=[
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                resume_from_stage='signal_decider',
                trigger='auto_eod_followup_resume',
                point_in_time_recovery=True,
                reuse_existing_feedback=True,
                reused_eod_collection_run_id=7,
                inherited_feedback_retry_config={
                    'source_run_id': 'feedback-cycle-parent',
                    'knowledge_cutoff_at': cutoff,
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                },
            )

    def test_signal_decider_does_not_reuse_summary_parent_lineage(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        cutoff = datetime(
            2026,
            4,
            14,
            8,
            30,
            tzinfo=timezone.utc,
        )

        with patch(
            'app.eod.daily_pipeline_stages.'
            'build_expired_decision_replay_result',
            return_value=None,
        ):
            result = service._execute_stage(
                stage_id='signal_decider',
                market_date='2026-04-14',
                symbols=['005930'],
                markets=['KOSPI'],
                force=False,
                bypass_prerequisite_gate=True,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
                feedback_knowledge_cutoff_at=cutoff,
                feedback_reuse_parent_run_id=(
                    'feedback-cycle-summary-parent'
                ),
                feedback_cutoff_resolution={
                    'ready': True,
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary-parent',
                    'feedback_scope_mode': (
                        'broad_feedback_universe'
                    ),
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930'],
                    },
                },
            )

        self.assertTrue(result['completed'])
        self.assertEqual(len(feedback_retry_service.calls), 1)
        config = feedback_retry_service.calls[0]['config']
        self.assertEqual(
            config.feedback_scope_mode,
            'decider_scope_fast_path',
        )
        self.assertIsNone(config.reuse_parent_run_id)

    def test_run_rejects_unknown_stage(self) -> None:
        service = DailyPipelineStageService()

        with self.assertRaisesRegex(ValueError, 'Unknown daily stage'):
            service.run(market_date='2026-04-14', stages=['not-a-stage'])

    def test_next_open_only_request_keeps_persisted_decider_boundary(self) -> None:
        service = DailyPipelineStageService()

        validated = service.validate_run_request(
            market_date='2026-04-14',
            stages=['next_open_execution_plan'],
            resume_from_stage=None,
        )

        self.assertEqual(
            validated['resolved_stages'],
            ['next_open_execution_plan'],
        )

    def test_direct_provider_selection_includes_only_provider_control_prefix(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        cases = {
            'price_collection': [
                'master_refresh',
                'pykrx_universe_refresh',
                'price_collection',
            ],
            'technical_snapshots': ['technical_snapshots'],
            'action_summaries': ['action_summaries'],
        }

        for requested_stage, expected in cases.items():
            with self.subTest(requested_stage=requested_stage):
                validated = service.validate_run_request(
                    market_date='2026-04-14',
                    stages=[requested_stage],
                    resume_from_stage=None,
                )
                self.assertEqual(validated['resolved_stages'], expected)

    def test_direct_terminal_suffix_never_expands_to_provider_stages(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        validated = service.validate_run_request(
            market_date='2026-04-14',
            stages=[
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
            resume_from_stage=None,
        )

        self.assertEqual(
            validated['resolved_stages'],
            [
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )
        self.assertNotIn('price_collection', validated['resolved_stages'])
        self.assertNotIn(
            'pykrx_universe_refresh',
            validated['resolved_stages'],
        )

    def test_resume_stage_keeps_validated_persisted_prefix_boundary(self) -> None:
        service = DailyPipelineStageService()

        validated = service.validate_run_request(
            market_date='2026-04-14',
            stages=['technical_snapshots', 'action_summaries'],
            resume_from_stage='technical_snapshots',
        )

        self.assertEqual(
            validated['resolved_stages'],
            ['technical_snapshots', 'action_summaries'],
        )

    def test_next_open_resume_keeps_persisted_dependency_boundary(self) -> None:
        service = DailyPipelineStageService()

        validated = service.validate_run_request(
            market_date='2026-04-14',
            stages=['next_open_execution_plan'],
            resume_from_stage='next_open_execution_plan',
        )

        self.assertEqual(
            validated['resolved_stages'],
            ['next_open_execution_plan'],
        )

    def test_run_marks_grouped_pykrx_skip_payloads_as_skipped(self) -> None:
        pykrx_runner = _FakeSkippedPykrxStageRunner()
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        with patch.object(
            service,
            '_execute_stage',
            return_value={'status': 'completed'},
        ) as execute_stage:
            result = service.run(
                market_date='2026-04-14',
                stages=['price_collection', 'technical_snapshots'],
            )

        self.assertEqual(execute_stage.call_args.args[0], 'master_refresh')
        # A generic already-running skip is not terminally satisfied evidence,
        # so the completed control step cannot promote the aggregate run.
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            result['stages']['pykrx_universe_refresh']['status'],
            'skipped',
        )
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

    def test_resolve_result_status_downgrades_completed_feedback_with_failures(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'status': 'completed',
                'completed': True,
                'final_pending': 0,
                'progress': {
                    'summary': {
                        'expected_feedback_count': 160,
                        'completed_feedback_count': 60,
                        'failed_items': 100,
                    }
                },
            }
        )

        self.assertEqual(status, 'partial')

    def test_resolve_result_status_downgrades_completed_feedback_with_pending_role(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'status': 'completed',
                'completed': True,
                'final_pending': 0,
                'progress': {
                    'summary': {
                        'expected_feedback_count': 239,
                        'completed_feedback_count': 239,
                        'failed_items': 0,
                        'role_progress': {
                            'fundamental-summarizer': {
                                'status': 'partial',
                                'total_symbols': 80,
                                'processed_symbols': 79,
                                'completed_symbols': 79,
                                'pending_symbols': 1,
                                'failed_items': 0,
                            }
                        },
                    }
                },
            }
        )

        self.assertEqual(status, 'partial')

    def test_stage_result_details_omit_high_volume_nested_evidence(self) -> None:
        details = DailyPipelineStageService._build_stage_run_details(
            stage_id='signal_decider_evaluations',
            result={
                'status': 'completed',
                'summary': {'evaluations_upserted': 200},
                'evaluation_refresh': {
                    'summary': {'evaluations_upserted': 200},
                    'items': [{'symbol': f'{index:06d}'} for index in range(200)],
                },
                'cycles': [{'items': list(range(200))}],
                'matched_symbols': [f'{index:06d}' for index in range(200)],
                'progress': {
                    'summary': {
                        'completed_feedback_count': 200,
                        'funnel_diagnostics': {'symbols': list(range(200))},
                    }
                },
            },
        )

        result_summary = details['result_summary']
        self.assertEqual(
            result_summary['evaluation_refresh'],
            {'summary': {'evaluations_upserted': 200}},
        )
        self.assertNotIn('cycles', result_summary)
        self.assertNotIn('matched_symbols', result_summary)
        self.assertNotIn(
            'funnel_diagnostics',
            result_summary['progress']['summary'],
        )
        self.assertEqual(
            details['result_summary_omissions']['evaluation_refresh.items'],
            {'type': 'list', 'item_count': 200},
        )
        self.assertEqual(
            details['result_summary_omissions']['cycles'],
            {'type': 'list', 'item_count': 1},
        )
        self.assertLess(len(str(details)), 5_000)

    def test_stage_result_details_preserve_scalar_aggregates_with_bulk_field_names(self) -> None:
        details = DailyPipelineStageService._build_stage_run_details(
            stage_id='signal_decider_evaluations',
            result={
                'status': 'completed',
                'summary': {
                    'items': 201,
                    'rows': 200,
                    'stages': 3,
                    'request_payload': None,
                },
            },
        )

        self.assertEqual(
            details['result_summary']['summary'],
            {
                'items': 201,
                'rows': 200,
                'stages': 3,
                'request_payload': None,
            },
        )
        self.assertNotIn('result_summary_omissions', details)

    def test_stage_result_details_preserve_exact_evaluation_failure_evidence(self) -> None:
        failure_evidence = {
            str(snapshot_id): {
                'decision_snapshot_id': snapshot_id,
                'symbol': f'{snapshot_id:06d}',
                'horizon_days': [1],
                'reason': 'signal_decider_evaluation_row_persistence_failed',
            }
            for snapshot_id in range(1, 102)
        }

        details = DailyPipelineStageService._build_stage_run_details(
            stage_id='signal_decider_evaluations',
            result={
                'status': 'failed',
                'evaluation_failures': list(failure_evidence.values()),
                'evaluation_failure_evidence': failure_evidence,
                'retry_debt_snapshot_ids': list(range(1, 102)),
                'summary': {
                    'failed_count': 101,
                    'failed_snapshot_count': 101,
                    'retry_debt_count': 101,
                },
            },
        )

        self.assertEqual(
            details['result_summary']['evaluation_failure_evidence'],
            failure_evidence,
        )
        self.assertEqual(
            details['result_summary_omissions']['evaluation_failures'],
            {'type': 'list', 'item_count': 101},
        )
        self.assertEqual(
            details['result_summary_omissions']['retry_debt_snapshot_ids'],
            {'type': 'list', 'item_count': 101},
        )

    def test_persisted_large_exact_scope_proof_restores_partial_cohort(
        self,
    ) -> None:
        selected_symbols = [f'{index:06d}' for index in range(1, 1_001)]
        retry_symbol = selected_symbols[-1]
        result = {
            'status': 'partial',
            'results_persisted': 999,
            'results_failed': 1,
            'remaining_retryable_events': 0,
            **_build_exact_scope_outcome(
                manifest_version='factor_scope_outcomes_v1',
                selected_symbols=selected_symbols,
                successful_symbols=selected_symbols[:-1],
                retry_debt_symbols=[retry_symbol],
            ),
        }
        details = DailyPipelineStageService._build_stage_run_details(
            stage_id='momentum_factors',
            result=result,
            invocation_symbols=selected_symbols,
        )

        self.assertNotIn(
            'selected_stock_codes',
            details['result_summary'],
        )
        proof = details['exact_scope_outcome_proof']
        self.assertEqual(proof['selected_symbol_count'], 1_000)
        self.assertEqual(proof['selected_stock_codes'], selected_symbols)
        self.assertTrue(
            daily_pipeline_outcome_is_satisfied(
                {
                    'stage': 'momentum_factors',
                    'status': 'partial',
                    'details': details,
                }
            )
        )

        service = DailyPipelineStageService()
        restored_outcome, restored_invocation = (
            service._restore_persisted_stage_outcome(
                stage_id='momentum_factors',
                persisted_outcome={
                    'status': 'partial',
                    'details': details,
                },
            )
        )
        restored_scope = service._exact_partial_success_scope(
            outcome=restored_outcome,
            invocation_symbols=restored_invocation,
        )

        self.assertIsNotNone(restored_scope)
        self.assertEqual(
            restored_scope['successful_stock_codes'],
            selected_symbols[:-1],
        )
        self.assertEqual(
            restored_scope['retry_debt_stock_codes'],
            [retry_symbol],
        )

        tampered_details = deepcopy(details)
        tampered_details['exact_scope_outcome_proof'][
            'selected_symbol_sha256'
        ] = '0' * 64
        with self.assertRaisesRegex(
            RuntimeError,
            'persisted exact scope outcome proof is invalid',
        ):
            service._restore_persisted_stage_outcome(
                stage_id='momentum_factors',
                persisted_outcome={
                    'status': 'partial',
                    'details': tampered_details,
                },
            )

    def test_exact_scope_proof_canonicalizes_valid_order_variance(
        self,
    ) -> None:
        invocation_symbols = ['005930', '000660', '035420']
        result_symbols = list(reversed(invocation_symbols))
        result = {
            'status': 'partial',
            **_build_exact_scope_outcome(
                manifest_version='factor_scope_outcomes_v1',
                selected_symbols=result_symbols,
                successful_symbols=['005930', '000660'],
                retry_debt_symbols=['035420'],
            ),
        }

        details = DailyPipelineStageService._build_stage_run_details(
            stage_id='momentum_factors',
            result=result,
            invocation_symbols=invocation_symbols,
        )

        self.assertEqual(
            details['exact_scope_outcome_proof'][
                'selected_stock_codes'
            ],
            sorted(invocation_symbols),
        )

    def test_resolve_result_status_uses_role_boundary_and_completed_counts(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'status': 'completed',
                'progress': {
                    'summary': {
                        'role_progress': {
                            'fundamental-summarizer': {
                                'status': 'completed',
                                'boundary_status': 'incomplete',
                                'total_symbols': 80,
                                'processed_symbols': 80,
                                'completed_symbols': 79,
                                'pending_symbols': 0,
                                'retry_pending_symbols': 1,
                                'failed_items': 0,
                            }
                        }
                    }
                },
            }
        )

        self.assertEqual(status, 'partial')

    def test_resolve_result_status_normalizes_ok_to_completed(self) -> None:
        status = DailyPipelineStageService._resolve_result_status({'status': 'ok'})

        self.assertEqual(status, 'completed')

    def test_resolve_result_status_downgrades_ok_with_failures(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {'status': 'ok', 'summary': {'failed_count': 1}}
        )

        self.assertEqual(status, 'partial')

    def test_resolve_result_status_rejects_persisted_factor_rows_without_ready_content(
        self,
    ) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        )

        self.assertEqual(status, 'partial')

    def test_resolve_result_status_preserves_explicit_low_coverage_factor_partial(
        self,
    ) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'status': 'partial',
                'reason': 'partial_usable_coverage',
                'gate_status': 'ready',
                'blocking_status': 'non_blocking',
                'results_persisted': 2_675,
                'summary': {
                    'universe_count': 2_675,
                    'ready_count': 300,
                    'complete_count': 300,
                    'partial_count': 0,
                    'unusable_count': 2_375,
                },
            }
        )

        self.assertEqual(status, 'partial')

    def test_resolve_result_status_blocks_zero_scope_with_incomplete_context(
        self,
    ) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {
                'summary': {
                    'items_total': 0,
                    'symbol_count': 0,
                    'agent_context_completeness_status': 'incomplete',
                    'agent_context_completeness': {
                        'status': 'incomplete',
                        'candidate_set_definitive': False,
                        'pending_scope_count': 157,
                    },
                },
                'items': [],
            }
        )

        self.assertEqual(status, 'blocked')

    def test_resolve_result_status_accepts_failed_item_collections(self) -> None:
        status = DailyPipelineStageService._resolve_result_status(
            {'error': 'some items failed', 'failed_items': ['005930']}
        )

        self.assertEqual(status, 'partial')

    def test_extract_stage_counts_uses_external_news_collection_summary(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_collect',
            result={
                'status': 'ok',
                'summary': {
                    'eligible_symbol_count': 201,
                    'collected_symbol_count': 201,
                    'items_inserted': 252,
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 201,
                'total': 201,
                'succeeded': 201,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_preserves_collected_news_when_scope_count_is_stale(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_collect',
            result={
                'status': 'ok',
                'summary': {
                    'eligible_symbol_count': 0,
                    'collected_symbol_count': 2,
                    'items_inserted': 3,
                },
            },
        )

    def test_extract_stage_counts_recovers_legacy_news_collection_evidence(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_collect',
            result={
                'status': 'ok',
                'summary': {
                    'eligible_symbol_count': 2,
                    'items_inserted': 3,
                    'failed_symbol_count': 0,
                    'skipped_symbol_count': 0,
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 2,
                'succeeded': 2,
                'failed': 0,
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 2,
                'succeeded': 2,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_keeps_fundamentals_retry_debt_remaining(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='fundamentals_daily_refresh',
            result={
                'status': 'partial',
                'symbols_checked': 4,
                'symbols_fresh': 2,
                'symbols_refreshed': 0,
                'symbols_failed': 1,
                'successful_stock_codes': ['000660', '005930'],
                'retry_debt_stock_codes': ['035420', '035720'],
                'retry_debt_count': 2,
                'partial_reasons': ['scope_retry_debt'],
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 4,
                'succeeded': 2,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_keeps_trending_progress_in_period_units(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='trending_ranks',
            result={
                'trending_ranks_persisted': 6,
                'periods_persisted': 3,
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 3,
                'total': 3,
                'succeeded': 3,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_uses_signal_decider_terminal_work(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='signal_decider',
            result={
                'status': 'completed',
                'completed': True,
                'final_pending': 0,
                'progress': {
                    'result': {
                        'roles': {
                            'short-term-decider': {
                                'requested_count': 80,
                                'completed_count': 80,
                                'error_count': 0,
                            },
                            'signal-decider': {
                                'requested_count': 80,
                                'completed_count': 80,
                                'error_count': 0,
                            },
                        },
                    },
                    'summary': {
                        'run_status': 'completed',
                        'run_table_status': 'completed',
                        'expected_feedback_count': 160,
                        'completed_feedback_count': 160,
                        'failed_items': 0,
                    },
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 160,
                'total': 160,
                'succeeded': 160,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_uses_signal_decider_evaluation_writes(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='signal_decider_evaluations',
            result={
                'request': {
                    'horizons': [1, 3, 5, 10],
                },
                'summary': {
                    'status': 'completed',
                    'decisions_considered': 960,
                    'outcomes_upserted': 3_840,
                    'evaluations_upserted': 3_840,
                    'not_computed_count': 0,
                    'warning_count': 0,
                    'coverage': {
                        'count': 3_840,
                        'computed_count': 2_284,
                        'pending_maturity_count': 1_550,
                        'insufficient_price_data_count': 6,
                        'not_computed_count': 0,
                    },
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 3_840,
                'total': 3_840,
                'succeeded': 3_840,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_preserves_evaluation_retry_debt(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='signal_decider_evaluations',
            result={
                'status': 'partial',
                'request': {
                    'horizons': [1],
                },
                'summary': {
                    'status': 'partial',
                    'decisions_considered': 2,
                    'outcomes_upserted': 2,
                    'evaluations_upserted': 1,
                    'failed_count': 1,
                    'failed_snapshot_count': 1,
                    'retry_debt_count': 1,
                    'retry_debt_snapshot_ids': [102],
                    'coverage': {
                        'count': 2,
                        'computed_count': 1,
                        'pending_maturity_count': 0,
                        'insufficient_price_data_count': 0,
                        'not_computed_count': 1,
                    },
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 2,
                'succeeded': 1,
                'failed': 1,
            },
        )

    def test_extract_stage_counts_keeps_decider_request_denominator_when_summary_is_truncated(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='signal_decider',
            result={
                'progress': {
                    'result': {
                        'roles': {
                            'short-term-decider': {
                                'requested_count': 80,
                                'completed_count': 80,
                                'error_count': 0,
                            },
                            'signal-decider': {
                                'requested_count': 80,
                                'completed_count': 80,
                                'error_count': 0,
                            },
                        },
                    },
                    'summary': {
                        'expected_feedback_count': 65,
                        'completed_feedback_count': 65,
                        'failed_items': 0,
                    },
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 65,
                'total': 160,
                'succeeded': 65,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_keeps_evaluation_request_denominator_when_writes_are_truncated(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='signal_decider_evaluations',
            result={
                'request': {
                    'horizons': [1, 3, 5, 10],
                },
                'summary': {
                    'decisions_considered': 960,
                    'outcomes_upserted': 65,
                    'evaluations_upserted': 65,
                    'failed_count': 0,
                    'coverage': {
                        'count': 3_840,
                        'computed_count': 2_284,
                        'pending_maturity_count': 1_550,
                        'insufficient_price_data_count': 6,
                        'not_computed_count': 0,
                    },
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 65,
                'total': 3_840,
                'succeeded': 65,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_preserves_explicit_zero_eligible_news_scope(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_collect',
            result={
                'status': 'disabled',
                'summary': {
                    'requested_symbol_count': 201,
                    'eligible_symbol_count': 0,
                    'collected_symbol_count': 0,
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_uses_external_news_enrichment_result(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_enrich',
            result={
                'status': 'partial',
                'items_inspected': 703,
                'items_updated': 701,
                'items_missing_classification': 3,
                'items_remaining_at_least': 1,
                'missing_item_ids': [11, 12],
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 703,
                'total': 704,
                'succeeded': 701,
                'failed': 2,
            },
        )

    def test_extract_stage_counts_keeps_news_failures_beyond_id_preview(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='external_news_enrich',
            result={
                'status': 'partial',
                'items_inspected': 120,
                'items_missing_classification': 101,
                'items_remaining_at_least': 1,
                'missing_item_ids': list(range(50)),
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 120,
                'total': 121,
                'succeeded': 20,
                'failed': 100,
            },
        )

    def test_extract_stage_counts_treats_ready_enrichment_warning_as_success(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='pykrx_enrichment',
            result={
                'categories_requested': [
                    'sector_classification',
                    'short_selling',
                ],
                'categories': {
                    'sector_classification': {
                        'status': 'completed',
                        'rows_persisted': 10,
                        'errors': [],
                    },
                    'short_selling': {
                        'status': 'partial',
                        'rows_persisted': 10,
                        'errors': [
                            {
                                'operation': 'short_selling_balance',
                                'error': 'empty provider frame',
                            }
                        ],
                    },
                },
                'downstream_preflight': {
                    'allowed': True,
                    'status': 'ready',
                    'required_categories': [
                        'sector_classification',
                        'investor_flow_symbol',
                        'short_selling',
                        'valuation_snapshot',
                    ],
                    'category_row_counts': {
                        'sector_classification': 10,
                        'investor_flow_symbol': 10,
                        'short_selling': 10,
                        'valuation_snapshot': 10,
                    },
                    'failed_categories': [],
                    'blocked_categories': [],
                    'waiting_categories': [],
                    'zero_coverage_categories': [],
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 2,
                'succeeded': 2,
                'failed': 0,
            },
        )

    def test_extract_stage_counts_keeps_preflight_failed_enrichment_failed(
        self,
    ) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='pykrx_enrichment',
            result={
                'categories_requested': [
                    'sector_classification',
                    'short_selling',
                ],
                'categories': {
                    'sector_classification': {
                        'status': 'completed',
                        'rows_persisted': 10,
                        'errors': [],
                    },
                    'short_selling': {
                        'status': 'partial',
                        'rows_persisted': 10,
                        'errors': ['coverage incomplete'],
                    },
                },
                'downstream_preflight': {
                    'allowed': False,
                    'status': 'blocked',
                    'failed_categories': ['short_selling'],
                },
            },
        )

        self.assertEqual(
            counts,
            {
                'processed': 2,
                'total': 2,
                'succeeded': 1,
                'failed': 1,
            },
        )

    def test_build_stage_run_details_marks_external_news_total_as_lower_bound(self) -> None:
        details = DailyPipelineStageService._build_stage_run_details(
            stage_id='external_news_enrich',
            result={
                'status': 'partial',
                'items_inspected': 703,
                'items_remaining_at_least': 1,
            },
        )

        self.assertEqual(
            details['count_semantics'],
            {
                'total': 'lower_bound',
                'total_at_least': 704,
                'remaining': 'lower_bound',
                'remaining_at_least': 1,
            },
        )

    def test_explicit_zero_stage_scope_rejects_lossy_or_boolean_counts(self) -> None:
        self.assertFalse(
            DailyPipelineStageService._has_explicit_zero_stage_scope(
                stage_id='external_news_collect',
                result={'summary': {'eligible_symbol_count': 0.5}},
            )
        )
        self.assertFalse(
            DailyPipelineStageService._has_explicit_zero_stage_scope(
                stage_id='external_news_enrich',
                result={
                    'items_inspected': False,
                    'items_remaining_at_least': 0,
                },
            )
        )

    def test_execute_stage_routes_macro_context_refresh_to_service(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.eod.daily_pipeline_stages.macro_context_service.refresh_context',
            return_value={'status': 'completed', 'observations_upserted': 3, 'observations_failed': 0},
        ) as refresh_context:
            result = service._execute_stage(
                stage_id='macro_context_refresh',
                market_date='2026-04-24',
                symbols=['005930', '000660'],
                markets=None,
                force=True,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result['observations_upserted'], 3)
        self.assertEqual(
            refresh_context.call_args.kwargs,
            {
                'force': True,
                'trigger': 'daily_pipeline_stage',
            },
        )

    def test_optional_context_unknown_service_exceptions_remain_fatal(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        execute_kwargs = {
            'market_date': '2026-04-24',
            'symbols': ['005930', '000660'],
            'markets': [],
            'force': True,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
        }
        cases = (
            (
                'external_news_collect',
                'app.eod.daily_pipeline_stages.external_news_service.collect_for_agent_feedback_candidates',
            ),
            (
                'external_news_enrich',
                'app.eod.daily_pipeline_stages.external_news_service.enrich_recent_classification',
            ),
            (
                'macro_context_refresh',
                'app.eod.daily_pipeline_stages.macro_context_service.refresh_context',
            ),
        )

        for stage_id, target in cases:
            with (
                self.subTest(stage_id=stage_id),
                patch(
                    target,
                    side_effect=RuntimeError(
                        'unexpected service invariant'
                    ),
                ),
                self.assertRaisesRegex(
                    RuntimeError,
                    'unexpected service invariant',
                ),
            ):
                service._execute_stage(
                    stage_id=stage_id,
                    **execute_kwargs,
                )

    def test_optional_context_integrity_errors_remain_fatal(self) -> None:
        service = DailyPipelineStageService()
        integrity_error = IntegrityError(
            'insert into context',
            {},
            RuntimeError('constraint violation'),
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.macro_context_service.refresh_context',
                side_effect=integrity_error,
            ),
            self.assertRaises(IntegrityError),
        ):
            service._execute_stage(
                stage_id='macro_context_refresh',
                market_date='2026-04-24',
                symbols=['005930'],
                markets=[],
                force=True,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

    def test_finish_tracked_stage_promotes_nested_failure_reason(self) -> None:
        service = DailyPipelineStageService()

        service._finish_tracked_stage(
            pipeline_run_id=9000,
            stage_id='macro_context_refresh',
            market_date='2026-04-24',
            outcome={'status': 'partial'},
            result={
                'status': 'partial',
                'error_message': 'provider call failed',
                'result_summary': {
                    'failure_reason': 'macro_provider_unavailable',
                },
                'observations_upserted': 0,
                'observations_failed': 0,
            },
        )

        finish = self.run_state_service.stage_events[-1]
        self.assertEqual(finish['event'], 'finish_stage')
        self.assertEqual(finish['status'], 'partial')
        self.assertEqual(
            finish['details']['reason'],
            'macro_provider_unavailable',
        )
        self.assertEqual(
            finish['details']['failure_reason'],
            'macro_provider_unavailable',
        )
        self.assertEqual(
            finish['error_message'],
            'provider call failed',
        )
        self.assertIn(
            'macro_provider_unavailable',
            finish['current_activity'],
        )

    def test_optional_service_degradation_does_not_fail_parent_run(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.eod.daily_pipeline_stages.macro_context_service.refresh_context',
            return_value={
                'triggered': True,
                'terminal': True,
                'retry_pending': False,
                'retryable': False,
                'status': 'partial',
                'reason': 'fred_credentials_missing',
                'failure_reason': 'fred_credentials_missing',
                'observations_upserted': 2,
                'observations_failed': 1,
            },
        ):
            result = service.run(
                market_date='2026-04-24',
                stages=['macro_context_refresh'],
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            result['stages']['macro_context_refresh']['status'],
            'partial',
        )
        finish = self.run_state_service.runs[-1]['finish']
        self.assertEqual(finish['status'], 'completed')
        self.assertNotIn('error_message', finish)
        self.assertEqual(finish['run_metadata']['quality_status'], 'degraded')

    def test_execute_stage_forwards_market_date_to_external_news_enrichment(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.eod.daily_pipeline_stages.external_news_service.enrich_recent_classification',
            return_value={'status': 'completed', 'items_inspected': 2},
        ) as enrich:
            result = service._execute_stage(
                stage_id='external_news_enrich',
                market_date='2026-10-07',
                symbols=['005930', '000660'],
                markets=[],
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=100,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result['status'], 'completed')
        enrich.assert_called_once_with(
            symbols=['005930', '000660'],
            market_date='2026-10-07',
            lookback_days=settings.external_news_context_lookback_days,
            limit=100,
        )

    def test_technical_stage_repairs_price_contexts_for_the_same_scope(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        call_kwargs = {
            'stage_id': 'technical_snapshots',
            'market_date': '2026-04-24',
            'symbols': ['005930', '000660'],
            'markets': ['KOSPI'],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 2,
                    'contexts_failed': 0,
                    'items': [{'symbol': '005930'}],
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930', '000660'],
                        successful_symbols=['005930', '000660'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as rebuild_contexts,
            patch(
                'app.eod.daily_pipeline_stages.daily_technical_snapshot_service.'
                'rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 2,
                    'snapshots_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930', '000660'],
                        successful_symbols=['005930', '000660'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as rebuild_snapshots,
            patch(
                'app.eod.daily_pipeline_stages.'
                '_finalize_daily_stage_scope',
                return_value={
                    'ready': True,
                    'deleted_stale_row_count': 0,
                },
            ) as finalize_scope,
        ):
            result = service._execute_stage(**call_kwargs)

        expected_scope = {
            'market_date': '2026-04-24',
            'symbols': ['005930', '000660'],
            'exact_scope_symbols': None,
            'source': 'stage_orchestrator',
            'run_id': None,
        }
        rebuild_contexts.assert_called_once_with(**expected_scope)
        rebuild_snapshots.assert_called_once_with(**expected_scope)
        self.assertEqual(result['contexts_persisted'], 2)
        self.assertEqual(result['contexts_failed'], 0)
        self.assertNotIn('items', result['price_contexts'])
        finalize_scope.assert_not_called()

    def test_resumed_technical_stage_advances_only_combined_exact_successes(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        symbols = ['005930', '000660', '035420']
        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 2,
                    'contexts_failed': 1,
                    'scope_finalization': {'ready': True},
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=symbols,
                        successful_symbols=['005930', '035420'],
                        retry_debt_symbols=['000660'],
                    ),
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 1,
                    'snapshots_failed': 2,
                    'scope_finalization': {'ready': True},
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=symbols,
                        successful_symbols=['005930'],
                        retry_debt_symbols=['000660', '035420'],
                    ),
                },
            ) as rebuild_technical_snapshots,
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'action_summaries_persisted': 1,
                    'action_summaries_failed': 2,
                    'scope_finalization': {'ready': True},
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=symbols,
                        successful_symbols=['005930'],
                        retry_debt_symbols=['000660', '035420'],
                    ),
                },
            ) as rebuild_action_summaries,
            patch(
                'app.eod.daily_pipeline_stages.'
                '_finalize_daily_stage_scope',
                return_value={
                    'ready': True,
                    'deleted_stale_row_count': 1,
                },
            ) as finalize_scope,
        ):
            result = service.run(
                market_date='2026-04-24',
                stages=['technical_snapshots', 'action_summaries'],
                symbols=symbols,
                resume_from_stage='technical_snapshots',
            )

        technical = result['stages']['technical_snapshots']
        self.assertEqual(technical['status'], 'partial')
        self.assertEqual(
            technical['result']['successful_stock_codes'],
            ['005930'],
        )
        self.assertEqual(
            technical['result']['retry_debt_stock_codes'],
            ['000660', '035420'],
        )
        self.assertEqual(
            rebuild_action_summaries.call_args.kwargs['symbols'],
            ['005930'],
        )
        self.assertEqual(
            rebuild_technical_snapshots.call_args.kwargs['symbols'],
            ['005930', '035420'],
        )
        self.assertEqual(
            rebuild_technical_snapshots.call_args.kwargs[
                'exact_scope_symbols'
            ],
            symbols,
        )
        self.assertEqual(
            rebuild_action_summaries.call_args.kwargs[
                'exact_scope_symbols'
            ],
            symbols,
        )
        finalize_scope.assert_not_called()

    def test_action_stage_finalizes_full_scope_around_current_admitted_subset(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        action_result = {
            'action_summaries_persisted': 1,
            'action_summaries_failed': 1,
            'scope_finalization': {
                'ready': True,
                'deleted_stale_row_count': 1,
            },
            **_build_exact_scope_outcome(
                manifest_version='action_summaries_scope_outcomes_v1',
                selected_symbols=['005930', '000660'],
                successful_symbols=['005930'],
                retry_debt_symbols=['000660'],
            ),
        }
        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_action_summary_service.rebuild_for_market_date',
                return_value=action_result,
            ) as rebuild_action_summaries,
            patch(
                'app.eod.daily_pipeline_stages.'
                '_finalize_daily_stage_scope',
                return_value={
                    'ready': True,
                    'deleted_stale_row_count': 1,
                },
            ) as finalize_scope,
        ):
            result = service._execute_stage(
                'action_summaries',
                market_date='2026-04-24',
                symbols=['005930'],
                exact_scope_symbols=['005930', '000660'],
                markets=['KOSPI'],
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        rebuild_action_summaries.assert_called_once_with(
            market_date='2026-04-24',
            symbols=['005930'],
            exact_scope_symbols=['005930', '000660'],
            source='stage_orchestrator',
            run_id=None,
        )
        finalize_scope.assert_not_called()
        self.assertEqual(
            result['scope_finalization']['deleted_stale_row_count'],
            1,
        )

    def test_technical_stage_omits_exact_proof_for_incomplete_child_identities(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        symbols = ['005930', '000660', '035420']
        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 2,
                    'contexts_failed': 1,
                    'scope_outcome_manifest_version': (
                        'price_contexts_scope_outcomes_v1'
                    ),
                    'selected_stock_codes': symbols,
                    'successful_stock_codes': ['005930', '000660'],
                    'retry_debt_stock_codes': [],
                    'terminal_not_applicable_stock_codes': [],
                    'scope_outcome_manifest': [
                        {
                            'stock_code': symbol,
                            'outcome': 'success',
                        }
                        for symbol in ['005930', '000660']
                    ],
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'status': 'completed',
                    'snapshots_persisted': 3,
                    'snapshots_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=symbols,
                        successful_symbols=symbols,
                        retry_debt_symbols=[],
                    ),
                },
            ),
        ):
            result = service._execute_stage(
                stage_id='technical_snapshots',
                market_date='2026-04-24',
                symbols=symbols,
                markets=['KOSPI'],
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        for key in (
            'scope_outcome_manifest_version',
            'selected_stock_codes',
            'successful_stock_codes',
            'retry_debt_stock_codes',
            'terminal_not_applicable_stock_codes',
            'scope_outcome_manifest',
        ):
            self.assertNotIn(key, result)
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['gate_status'], 'blocked')
        self.assertEqual(
            result['scope_outcome_contract_reason'],
            'price_context_scope_outcome_invalid',
        )
        self.assertEqual(result['retryable_symbols'], symbols)
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            symbols,
        )

    def test_missing_technical_child_manifests_block_downstream_scope(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        symbols = ['005930', '000660']
        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 2,
                    'contexts_failed': 0,
                    'scope_finalization': {'ready': True},
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'status': 'completed',
                    'snapshots_persisted': 2,
                    'snapshots_failed': 0,
                    'scope_finalization': {'ready': True},
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_action_summary_service.rebuild_for_market_date',
            ) as rebuild_action_summaries,
        ):
            result = service.run(
                market_date='2026-04-24',
                stages=['technical_snapshots', 'action_summaries'],
                symbols=symbols,
                resume_from_stage='technical_snapshots',
            )

        technical = result['stages']['technical_snapshots']
        self.assertEqual(technical['status'], 'partial')
        self.assertEqual(
            technical['result']['reason'],
            'technical_scope_outcome_contract_invalid',
        )
        self.assertEqual(
            technical['result']['retryable_symbols'],
            symbols,
        )
        self.assertEqual(
            technical['result']['_retry_metadata']['failed_symbols'],
            symbols,
        )
        action_summaries = result['stages']['action_summaries']
        self.assertEqual(action_summaries['status'], 'blocked')
        self.assertEqual(
            action_summaries['result']['incomplete_dependencies'],
            ['technical_snapshots'],
        )
        rebuild_action_summaries.assert_not_called()
        technical_finish = next(
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'finish_stage'
            and event['stage_id'] == 'technical_snapshots'
        )
        self.assertEqual(technical_finish['succeeded'], 0)
        self.assertEqual(technical_finish['failed'], 2)

    def test_underreported_context_count_blocks_full_invocation(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        symbols = ['005930', '000660']
        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'status': 'partial',
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=symbols,
                        successful_symbols=['005930'],
                        retry_debt_symbols=['000660'],
                    ),
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'status': 'completed',
                    'snapshots_persisted': 2,
                    'snapshots_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=symbols,
                        successful_symbols=symbols,
                        retry_debt_symbols=[],
                    ),
                },
            ),
        ):
            result = service._execute_stage(
                stage_id='technical_snapshots',
                market_date='2026-04-24',
                symbols=symbols,
                markets=['KOSPI'],
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['gate_status'], 'blocked')
        self.assertEqual(
            result['scope_outcome_contract_reason'],
            'price_context_scope_count_mismatch',
        )
        self.assertEqual(result['retryable_symbols'], symbols)
        self.assertNotIn('scope_outcome_manifest_version', result)

    def test_technical_child_requested_count_must_match_exact_scope(
        self,
    ) -> None:
        cases = (
            (2, [], 2),
            (3, ['005930', '000660'], 3),
        )
        for requested_count, selected_symbols, retry_count in cases:
            with self.subTest(
                requested_count=requested_count,
                selected_symbols=selected_symbols,
            ):
                price_context_result = {
                    'status': 'completed',
                    'requested_symbols': requested_count,
                    'contexts_persisted': len(selected_symbols),
                    'contexts_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=selected_symbols,
                        successful_symbols=selected_symbols,
                        retry_debt_symbols=[],
                    ),
                }
                technical_result = {
                    'status': 'completed',
                    'requested_symbols': requested_count,
                    'snapshots_persisted': len(selected_symbols),
                    'snapshots_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=selected_symbols,
                        successful_symbols=selected_symbols,
                        retry_debt_symbols=[],
                    ),
                }

                result = (
                    DailyPipelineStageService
                    ._combine_technical_scope_outcomes(
                        price_context_result=price_context_result,
                        technical_result=technical_result,
                        invocation_symbols=None,
                    )
                )

                self.assertEqual(result['status'], 'partial')
                self.assertEqual(result['gate_status'], 'blocked')
                self.assertEqual(
                    result['scope_outcome_contract_reason'],
                    'price_context_requested_scope_count_mismatch',
                )
                self.assertEqual(
                    result['retryable_symbols'],
                    selected_symbols,
                )
                self.assertEqual(
                    result['retryable_symbol_count'],
                    retry_count,
                )
                self.assertNotIn(
                    'scope_outcome_manifest_version',
                    result,
                )

    def test_derived_empty_technical_scope_requires_explicit_authority(
        self,
    ) -> None:
        price_context_result = {
            'status': 'completed',
            'requested_symbols': 0,
            'contexts_persisted': 0,
            'contexts_failed': 0,
            **_build_exact_scope_outcome(
                manifest_version='price_contexts_scope_outcomes_v1',
                selected_symbols=[],
                successful_symbols=[],
                retry_debt_symbols=[],
            ),
        }
        technical_result = {
            'status': 'completed',
            'requested_symbols': 0,
            'snapshots_persisted': 0,
            'snapshots_failed': 0,
            **_build_exact_scope_outcome(
                manifest_version=(
                    'technical_snapshots_scope_outcomes_v1'
                ),
                selected_symbols=[],
                successful_symbols=[],
                retry_debt_symbols=[],
            ),
        }

        derived_result = (
            DailyPipelineStageService
            ._combine_technical_scope_outcomes(
                price_context_result=price_context_result,
                technical_result=technical_result,
                invocation_symbols=None,
            )
        )

        self.assertEqual(derived_result['status'], 'partial')
        self.assertEqual(derived_result['gate_status'], 'blocked')
        self.assertEqual(
            derived_result['scope_outcome_contract_reason'],
            'derived_empty_scope_not_authoritative',
        )
        self.assertTrue(derived_result['retry_required'])
        self.assertTrue(
            derived_result['_retry_metadata']['retry_required']
        )
        self.assertEqual(derived_result['retryable_symbols'], [])
        self.assertEqual(derived_result['retryable_symbol_count'], 0)
        self.assertNotIn(
            'scope_outcome_manifest_version',
            derived_result,
        )

        explicit_empty_result = (
            DailyPipelineStageService
            ._combine_technical_scope_outcomes(
                price_context_result=price_context_result,
                technical_result=technical_result,
                invocation_symbols=[],
            )
        )
        self.assertEqual(explicit_empty_result['status'], 'completed')
        self.assertEqual(
            explicit_empty_result['selected_stock_codes'],
            [],
        )
        self.assertNotIn('scope_outcome_contract_status', explicit_empty_result)

    def test_swapped_technical_child_manifest_versions_fail_closed(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        symbols = ['005930', '000660']
        with (
            patch(
                'app.eod.daily_pipeline_stages.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 2,
                    'contexts_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=symbols,
                        successful_symbols=symbols,
                        retry_debt_symbols=[],
                    ),
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'status': 'completed',
                    'snapshots_persisted': 2,
                    'snapshots_failed': 0,
                    **_build_exact_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=symbols,
                        successful_symbols=symbols,
                        retry_debt_symbols=[],
                    ),
                },
            ),
        ):
            result = service._execute_stage(
                stage_id='technical_snapshots',
                market_date='2026-04-24',
                symbols=symbols,
                markets=['KOSPI'],
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['gate_status'], 'blocked')
        self.assertEqual(
            result['scope_outcome_contract_reason'],
            'price_context_scope_manifest_version_invalid',
        )
        self.assertEqual(result['retryable_symbols'], symbols)
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            symbols,
        )
        self.assertNotIn('scope_outcome_manifest_version', result)

    def test_execute_stage_routes_unified_value_factor_stage_to_service(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.eod.daily_pipeline_stages.daily_unified_value_factor_service.rebuild_for_market_date',
            return_value={'results_persisted': 2},
        ) as compute_unified_value:
            result = service._execute_stage(
                stage_id='unified_value_factors',
                market_date='2026-04-24',
                symbols=['005930', '000660'],
                markets=None,
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_unified_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_unified_value.call_args.kwargs['symbols'], ['005930', '000660'])

    def test_execute_stage_routes_every_atomic_derived_stage_through_exact_contract(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        exact_scope = ['005930', '000660']
        stage_targets = (
            (
                'market_risk_snapshots',
                'app.eod.daily_pipeline_stages.'
                'daily_market_risk_snapshot_service.collect_for_market_date',
                True,
                False,
            ),
            (
                'market_risk_filter',
                'app.eod.daily_pipeline_stages.'
                'daily_market_risk_filter_service.rebuild_for_market_date',
                True,
                False,
            ),
            (
                'piotroski_filter',
                'app.eod.daily_pipeline_stages.'
                'daily_piotroski_filter_service.rebuild_for_market_date',
                False,
                True,
            ),
            (
                'momentum_factors',
                'app.eod.daily_pipeline_stages.'
                'daily_momentum_factor_service.rebuild_for_market_date',
                False,
                True,
            ),
            (
                'value_factors',
                'app.eod.daily_pipeline_stages.'
                'daily_value_factor_service.rebuild_for_market_date',
                False,
                True,
            ),
            (
                'low_risk_factors',
                'app.eod.daily_pipeline_stages.'
                'daily_low_risk_factor_service.rebuild_for_market_date',
                False,
                True,
            ),
            (
                'beta_factors',
                'app.eod.daily_pipeline_stages.'
                'daily_beta_factor_service.rebuild_for_market_date',
                False,
                True,
            ),
            (
                'advanced_value_factors',
                'app.eod.daily_pipeline_stages.'
                'daily_advanced_value_factor_service.rebuild_for_market_date',
                False,
                True,
            ),
            (
                'unified_value_factors',
                'app.eod.daily_pipeline_stages.'
                'daily_unified_value_factor_service.rebuild_for_market_date',
                False,
                True,
            ),
            (
                'quality_factors',
                'app.eod.daily_pipeline_stages.'
                'daily_quality_factor_service.rebuild_for_market_date',
                False,
                True,
            ),
            (
                'composite_factors',
                'app.eod.daily_pipeline_stages.'
                'daily_composite_factor_service.rebuild_for_market_date',
                False,
                True,
            ),
        )
        call_kwargs = {
            'market_date': '2026-04-24',
            'symbols': exact_scope,
            'exact_scope_symbols': exact_scope,
            'markets': ['KOSPI'],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
        }

        for stage_id, target, is_async, uses_ranking_scope in stage_targets:
            stage_mock = (
                AsyncMock(
                    return_value={
                        'scope_finalization': {'ready': True},
                    }
                )
                if is_async
                else MagicMock(
                    return_value={
                        'scope_finalization': {'ready': True},
                    }
                )
            )
            with self.subTest(stage_id=stage_id), patch(
                target,
                new=stage_mock,
            ), patch(
                'app.eod.daily_pipeline_stages.'
                '_finalize_daily_stage_scope',
            ) as legacy_finalize:
                result = service._execute_stage(
                    stage_id=stage_id,
                    **call_kwargs,
                )

            self.assertTrue(result['scope_finalization']['ready'])
            self.assertEqual(
                stage_mock.call_args.kwargs['exact_scope_symbols'],
                exact_scope,
            )
            if uses_ranking_scope:
                self.assertEqual(
                    stage_mock.call_args.kwargs['ranking_symbols'],
                    exact_scope,
                )
            legacy_finalize.assert_not_called()

    def test_exact_scope_finalization_write_error_is_dedicated_fatal(
        self,
    ) -> None:
        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                '_finalize_daily_stage_scope',
                side_effect=RuntimeError('scope checkpoint unavailable'),
            ),
            self.assertRaisesRegex(
                DailyPipelineScopeFinalizationError,
                'value_factors exact-scope finalization failed',
            ) as raised,
        ):
            DailyPipelineStageService._execute_exact_scoped_derived_stage(
                stage_id='value_factors',
                market_date='2026-04-24',
                exact_scope_symbols=['005930', '000660'],
                call=lambda: {
                    'status': 'completed',
                    'results_persisted': 2,
                },
            )

        self.assertIsInstance(raised.exception.__cause__, RuntimeError)

    def test_pipeline_missing_atomic_scope_finalization_proof_is_fatal(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        symbols = ['005930', '000660']

        with (
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_value_factor_service.rebuild_for_market_date',
                return_value={
                    'status': 'completed',
                    'results_persisted': 2,
                    'results_failed': 0,
                },
            ) as rebuild_value,
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_low_risk_factor_service.rebuild_for_market_date',
                return_value={
                    'status': 'completed',
                    'scope_finalization': {'ready': True},
                },
            ) as rebuild_low_risk,
            self.assertRaisesRegex(
                DailyPipelineScopeFinalizationError,
                'value_factors atomic exact-scope finalization proof '
                'is unavailable',
            ),
        ):
            service.run(
                market_date='2026-04-24',
                stages=['value_factors', 'low_risk_factors'],
                symbols=symbols,
            )

        self.assertEqual(
            rebuild_value.call_args.kwargs['exact_scope_symbols'],
            symbols,
        )
        rebuild_low_risk.assert_not_called()
        self.assertEqual(
            self.run_state_service.runs[-1]['finish']['status'],
            'failed',
        )

    def test_execute_stage_routes_quality_factor_stage_to_service(self) -> None:
        service = DailyPipelineStageService()

        with patch(
            'app.eod.daily_pipeline_stages.daily_quality_factor_service.rebuild_for_market_date',
            return_value={'results_persisted': 2},
        ) as compute_quality:
            result = service._execute_stage(
                stage_id='quality_factors',
                market_date='2026-04-24',
                symbols=['005930', '000660'],
                markets=None,
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_quality.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_quality.call_args.kwargs['symbols'], ['005930', '000660'])

    def test_execute_stage_refreshes_next_open_plan_after_decider(self) -> None:
        service = DailyPipelineStageService()

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=MagicMock(state='PREOPEN'),
            ),
            patch(
                'app.orders.next_open_execution.signal_decider_next_open_execution_service.refresh_plan_items',
                return_value={'summary': {'items_total': 2, 'eligible_count': 1}},
            ) as refresh_plan,
        ):
            result = service._execute_stage(
                stage_id='next_open_execution_plan',
                market_date='2026-04-14',
                symbols=None,
                markets=None,
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
                point_in_time_recovery=True,
            )

        self.assertEqual(result['summary']['items_total'], 2)
        refresh_plan.assert_called_once_with(
            target_trade_date='2026-04-15',
            trigger='daily_pipeline_post_decider',
            force=False,
            completion_parent_proof=None,
        )

    def test_execute_stage_skips_expired_prior_date_open_plan(self) -> None:
        service = DailyPipelineStageService()
        market = MagicMock(
            state='OPEN',
            server_time='2026-09-14T10:00:00+09:00',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=market,
            ),
            patch(
                'app.orders.next_open_execution.normalize_next_open_target_trade_date',
                return_value=datetime(2026, 9, 14).date(),
            ),
            patch(
                'app.orders.next_open_execution.signal_decider_next_open_execution_service.refresh_plan_items',
            ) as refresh_plan,
        ):
            result = service._execute_stage(
                stage_id='next_open_execution_plan',
                market_date='2026-09-11',
                symbols=None,
                markets=None,
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'next_open_window_expired')
        self.assertEqual(result['stage'], 'next_open_execution_plan')
        refresh_plan.assert_not_called()

    def test_execute_stage_hands_preopen_admitted_recovery_to_open_plan(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        market = MagicMock(
            state='OPEN',
            server_time='2026-09-14T09:01:00+09:00',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 9, 14),
            ),
            patch(
                'app.orders.next_open_execution.'
                'signal_decider_next_open_execution_service.'
                'refresh_plan_items',
                return_value={'status': 'completed'},
            ) as refresh_plan,
        ):
            result = service._execute_stage(
                stage_id='next_open_execution_plan',
                market_date='2026-09-11',
                symbols=None,
                markets=[],
                force=False,
                bypass_prerequisite_gate=False,
                fs_div='CFS',
                symbol_limit=None,
                symbol_offset=0,
                capture_limit=500,
                retry_cycles=None,
                feedback_max_cycles=3,
                feedback_outer_batch_size=20,
                reuse_existing_feedback=True,
                point_in_time_recovery=True,
                allow_same_day_intraday_watch_continuation=True,
            )

        self.assertEqual(result['status'], 'completed')
        refresh_plan.assert_called_once_with(
            target_trade_date='2026-09-12',
            trigger='daily_pipeline_post_decider',
            force=False,
            completion_parent_proof=None,
        )

    def test_execute_stage_skips_expired_prior_date_decision_replay(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )
        market = MagicMock(
            state='OPEN',
            server_time='2026-09-14T10:00:00+09:00',
        )
        call_kwargs = {
            'market_date': '2026-09-11',
            'symbols': ['005930'],
            'markets': [],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=datetime(2026, 9, 14).date(),
            ),
        ):
            results = [
                service._execute_stage(stage_id=stage_id, **call_kwargs)
                for stage_id in (
                    'agent_feedback_summaries',
                    'signal_decider',
                    'signal_decider_evaluations',
                )
            ]

        self.assertTrue(
            all(result['status'] == 'skipped' for result in results)
        )
        self.assertTrue(
            all(
                result['reason'] == 'next_open_window_expired'
                for result in results
            )
        )
        self.assertEqual(feedback_retry_service.calls, [])
        self.assertEqual(evaluation_service.calls, [])

    def test_execute_stage_allows_expired_analysis_replay_without_plan(self) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )
        market = MagicMock(
            state='OPEN',
            server_time='2026-09-14T10:00:00+09:00',
        )
        call_kwargs = {
            'market_date': '2026-09-11',
            'symbols': ['005930'],
            'markets': [],
            'force': False,
            'bypass_prerequisite_gate': True,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
            'allow_expired_analysis_replay': True,
        }

        with patch(
            'app.eod.daily_pipeline_stages.get_market_state',
            return_value=market,
        ):
            feedback_result = service._execute_stage(
                stage_id='agent_feedback_summaries',
                **call_kwargs,
            )
            evaluation_result = service._execute_stage(
                stage_id='signal_decider_evaluations',
                **call_kwargs,
            )

        self.assertTrue(feedback_result['completed'])
        self.assertEqual(
            evaluation_result['summary']['evaluations_upserted'],
            4,
        )
        self.assertEqual(len(feedback_retry_service.calls), 1)
        self.assertEqual(len(evaluation_service.calls), 1)

    def test_execute_decider_does_not_start_during_preopen(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        call_kwargs = {
            'stage_id': 'signal_decider',
            'market_date': '2026-09-11',
            'symbols': ['005930'],
            'markets': [],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                side_effect=[
                    MagicMock(
                        state='PREOPEN',
                        server_time='2026-09-14T08:59:00+09:00',
                    ),
                    MagicMock(
                        state='OPEN',
                        server_time='2026-09-14T09:01:00+09:00',
                    ),
                ],
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=datetime(2026, 9, 14).date(),
            ),
        ):
            result = service._execute_stage(**call_kwargs)

        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'next_open_window_expired')
        self.assertEqual(feedback_retry_service.calls, [])

    def test_execute_decider_keeps_preopen_admission_after_market_open(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
        )
        call_kwargs = {
            'stage_id': 'signal_decider',
            'market_date': '2026-09-11',
            'symbols': ['005930'],
            'markets': [],
            'force': False,
            'bypass_prerequisite_gate': False,
            'fs_div': 'CFS',
            'symbol_limit': None,
            'symbol_offset': 0,
            'capture_limit': 500,
            'retry_cycles': None,
            'feedback_max_cycles': 3,
            'feedback_outer_batch_size': 20,
            'reuse_existing_feedback': True,
            'allow_same_day_intraday_watch_continuation': True,
        }

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                side_effect=[
                    MagicMock(
                        state='PREOPEN',
                        server_time='2026-09-14T08:59:00+09:00',
                    ),
                    MagicMock(
                        state='OPEN',
                        server_time='2026-09-14T09:01:00+09:00',
                    ),
                ],
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 9, 14),
            ),
        ):
            result = service._execute_stage(**call_kwargs)

        self.assertTrue(result['completed'])
        self.assertEqual(len(feedback_retry_service.calls), 1)

    def test_auto_pitr_preopen_plan_only_does_not_persist_admission(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        preopen = MagicMock(
            state='PREOPEN',
            server_time='2026-09-14T08:59:00+09:00',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=preopen,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 9, 14),
            ),
            patch.object(
                service,
                '_execute_stage',
                return_value={'status': 'completed'},
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-09-11',
                stages=['next_open_execution_plan'],
                resume_from_stage='next_open_execution_plan',
                trigger='auto_eod_followup_resume',
                point_in_time_recovery=True,
            )

        self.assertFalse(
            execute_stage.call_args.kwargs[
                'allow_same_day_intraday_watch_continuation'
            ]
        )
        self.assertFalse(
            self.run_state_service.runs[-1]['run_metadata'][
                'same_day_intraday_watch_continuation_admitted'
            ]
        )

    def test_exact_auto_pitr_recovery_cannot_start_during_open(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )
        cutoff = datetime(2026, 10, 27, 0, 5, tzinfo=timezone.utc)
        open_market = MagicMock(
            state='OPEN',
            server_time='2026-10-27T10:01:00+09:00',
        )
        stages = [
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=open_market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 10, 27),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930'],
                    },
                },
            ),
            patch(
                'app.orders.next_open_execution.'
                'signal_decider_next_open_execution_service.'
                'refresh_plan_items',
                return_value={'status': 'completed'},
            ) as refresh_plan,
        ):
            result = service.run(
                market_date='2026-10-26',
                stages=stages,
                resume_from_stage='signal_decider',
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=581,
                point_in_time_recovery=True,
                force=False,
                reuse_existing_feedback=True,
                summary_cutoff_proof={
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': None,
                    'expected_completed_symbols': ['005930'],
                    'knowledge_cutoff_at': cutoff.isoformat(),
                    'fs_div': 'CFS',
                },
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(feedback_retry_service.calls, [])
        refresh_plan.assert_not_called()
        self.assertEqual(evaluation_service.calls, [])
        self.assertTrue(
            all(
                outcome['status'] == 'skipped'
                and outcome['result']['reason']
                == 'next_open_window_expired'
                for outcome in result['stages'].values()
            )
        )
        self.assertFalse(
            self.run_state_service.runs[-1]['run_metadata'][
                'same_day_intraday_watch_continuation_admitted'
            ]
        )

    def test_exact_auto_pitr_recovery_cannot_start_during_preopen(
        self,
    ) -> None:
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
        )
        cutoff = datetime(2026, 10, 27, 0, 5, tzinfo=timezone.utc)
        preopen_market = MagicMock(
            state='PREOPEN',
            server_time='2026-10-27T08:01:00+09:00',
        )
        stages = [
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=preopen_market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 10, 27),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': cutoff,
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': ['005930'],
                    'completed_scope_proof': {
                        'schema_version': (
                            'completed_summarizer_intersection_v1'
                        ),
                        'completed_symbols': ['005930'],
                        'admitted_symbols': ['005930'],
                    },
                },
            ),
            patch(
                'app.orders.next_open_execution.'
                'signal_decider_next_open_execution_service.'
                'refresh_plan_items',
                return_value={'status': 'completed'},
            ) as refresh_plan,
        ):
            result = service.run(
                market_date='2026-10-26',
                stages=stages,
                resume_from_stage='signal_decider',
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=581,
                point_in_time_recovery=True,
                force=False,
                reuse_existing_feedback=True,
                summary_cutoff_proof={
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': None,
                    'expected_completed_symbols': ['005930'],
                    'knowledge_cutoff_at': cutoff.isoformat(),
                    'fs_div': 'CFS',
                },
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(feedback_retry_service.calls, [])
        refresh_plan.assert_not_called()
        self.assertEqual(evaluation_service.calls, [])
        self.assertTrue(
            all(
                outcome['status'] == 'skipped'
                and outcome['result']['reason']
                == 'next_open_window_expired'
                for outcome in result['stages'].values()
            )
        )
        self.assertFalse(
            self.run_state_service.runs[-1]['run_metadata'][
                'same_day_intraday_watch_continuation_admitted'
            ]
        )

    def test_same_day_rearm_blocks_proofless_persisted_signal_completion(
        self,
    ) -> None:
        cutoff = datetime(2026, 10, 27, 0, 5, tzinfo=timezone.utc)
        proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            'requested_symbols': None,
            'expected_completed_symbols': ['005930'],
            'knowledge_cutoff_at': cutoff.isoformat(),
            'fs_div': 'CFS',
        }
        run_state_service = _FakeSameDayRearmRunStateService(proof=proof)
        feedback_retry_service = _FakeFeedbackRetryService()
        evaluation_service = _FakeSignalDeciderEvaluationService()
        service = DailyPipelineStageService(
            feedback_retry_service=feedback_retry_service,
            decision_loop_service=evaluation_service,
            run_state_service=run_state_service,
        )
        open_market = MagicMock(
            state='OPEN',
            server_time='2026-10-28T10:01:00+09:00',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=open_market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 10, 28),
            ),
            patch(
                'app.orders.next_open_execution.'
                'signal_decider_next_open_execution_service.'
                'refresh_plan_items',
                return_value={'status': 'completed'},
            ) as refresh_plan,
        ):
            result = service.run(
                market_date='2026-10-27',
                stages=[
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                resume_from_stage='signal_decider',
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=581,
                point_in_time_recovery=True,
                force=False,
                reuse_existing_feedback=True,
                fs_div='CFS',
                summary_cutoff_proof=proof,
                existing_pipeline_run_id=593,
                same_day_intraday_watch_rearm_resume=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(len(feedback_retry_service.calls), 0)
        refresh_plan.assert_not_called()
        self.assertEqual(evaluation_service.calls, [])
        self.assertEqual(
            [event['stage_id'] for event in run_state_service.stage_events
             if event['event'] == 'start_stage'],
            ['next_open_execution_plan', 'signal_decider_evaluations'],
        )
        self.assertEqual(len(run_state_service.claim_calls), 1)
        self.assertEqual(
            run_state_service.claim_calls[0]['target_trade_date'],
            '2026-10-28',
        )
        self.assertEqual(
            run_state_service.claim_calls[0][
                'expected_summary_cutoff_proof'
            ],
            proof,
        )
        self.assertEqual(run_state_service.start_run_calls, [])

    def test_open_auto_pitr_exact_suffix_without_full_proof_is_not_admitted(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        open_market = MagicMock(
            state='OPEN',
            server_time='2026-10-27T10:01:00+09:00',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=open_market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 10, 27),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': False,
                    'reason': 'summary_cutoff_proof_unavailable',
                    'knowledge_cutoff_at': None,
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                },
            ),
        ):
            result = service.run(
                market_date='2026-10-26',
                stages=[
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                resume_from_stage='signal_decider',
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=581,
                point_in_time_recovery=True,
                force=False,
                reuse_existing_feedback=True,
                summary_cutoff_proof={
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': None,
                },
            )

        self.assertEqual(result['status'], 'completed')
        self.assertFalse(
            self.run_state_service.runs[-1]['run_metadata'][
                'same_day_intraday_watch_continuation_admitted'
            ]
        )

    def test_open_auto_pitr_plan_only_does_not_persist_admission(self) -> None:
        service = DailyPipelineStageService()
        open_market = MagicMock(
            state='OPEN',
            server_time='2026-10-27T10:01:00+09:00',
        )

        with (
            patch(
                'app.eod.daily_pipeline_stages.get_market_state',
                return_value=open_market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 10, 27),
            ),
            patch.object(
                service,
                '_execute_stage',
                return_value={'status': 'completed'},
            ) as execute_stage,
        ):
            result = service.run(
                market_date='2026-10-26',
                stages=['next_open_execution_plan'],
                resume_from_stage='next_open_execution_plan',
                trigger='auto_eod_followup_resume',
                reused_eod_collection_run_id=581,
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            result['stages']['next_open_execution_plan']['status'],
            'partial',
        )
        self.assertFalse(
            execute_stage.call_args.kwargs[
                'allow_same_day_intraday_watch_continuation'
            ]
        )
        self.assertFalse(
            self.run_state_service.runs[-1]['run_metadata'][
                'same_day_intraday_watch_continuation_admitted'
            ]
        )

    def test_next_open_plan_stage_counts_eligible_and_blocked_rows(self) -> None:
        counts = DailyPipelineStageService._extract_stage_counts(
            stage_id='next_open_execution_plan',
            result={
                'summary': {
                    'items_total': 18,
                    'eligible_count': 4,
                    'monitor_candidate_count': 10,
                    'blocked_count': 4,
                }
            },
        )

        self.assertEqual(
            counts,
            {'processed': 18, 'total': 18, 'succeeded': 18, 'failed': 0},
        )

    def test_price_stage_ignores_unrelated_grouped_stage_failure(self) -> None:
        service = DailyPipelineStageService()

        result = service._extract_pykrx_stage_result(
            'price_collection',
            {
                'run_id': 901,
                'status': 'completed_with_errors',
                'latest_available_trade_date': '2026-04-14',
                'symbols_total': 2,
                'symbols_succeeded': 2,
                'symbols_failed': 0,
                'technical_snapshots': {
                    'status': 'partial',
                    'snapshots_persisted': 1,
                    'snapshots_failed': 1,
                },
            },
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['source_run_ids']['eod_run_id'], 901)

    def test_grouped_non_price_stage_does_not_mislabel_eod_run_id(self) -> None:
        service = DailyPipelineStageService()

        result = service._extract_pykrx_stage_result(
            'technical_snapshots',
            {
                'run_id': 901,
                'latest_available_trade_date': '2026-04-14',
                'technical_snapshots': {
                    'status': 'completed',
                    'snapshots_persisted': 2,
                    'snapshots_failed': 0,
                },
            },
        )

        self.assertEqual(result['run_id'], 901)
        self.assertEqual(result['source_run_ids'], {})

    def test_grouped_technical_failure_does_not_block_enrichment_after_price_success(self) -> None:
        enrichment_service = MagicMock()
        enrichment_service.collect_for_market_date.return_value = {
            'status': 'completed',
            'categories_requested': [],
            'categories': {},
            'coverage': {
                'categories': {
                    category: {
                        'expected_scope_keys': [],
                        'complete_scope_keys': [],
                    }
                    for category in (
                        'sector_classification',
                        'investor_flow_symbol',
                        'short_selling',
                        'valuation_snapshot',
                    )
                }
            },
        }
        service = DailyPipelineStageService(
            pykrx_service=_FakePartiallySuccessfulPykrxStageRunner(),
            pykrx_enrichment_service_ref=enrichment_service,
        )
        original_execute_stage = service._execute_stage

        def execute_stage(stage_id, **kwargs):
            if stage_id == 'master_refresh':
                return {'status': 'completed'}
            return original_execute_stage(stage_id, **kwargs)

        with (
            patch(
                'app.eod.daily_pipeline_stages.build_pykrx_enrichment_preflight',
                return_value={'allowed': True, 'failed_categories': []},
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute_stage,
            ) as execute_stage_mock,
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'price_collection',
                    'pykrx_enrichment',
                    'technical_snapshots',
                ],
                provider_admission_fence_owned=True,
            )

        self.assertEqual(
            execute_stage_mock.call_args_list[0].args[0],
            'master_refresh',
        )
        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'partial')
        self.assertEqual(result['stages']['pykrx_enrichment']['status'], 'completed')
        enrichment_service.collect_for_market_date.assert_called_once()
        self.assertTrue(
            enrichment_service.collect_for_market_date.call_args.kwargs[
                'use_full_market_coverage'
            ]
        )
        self.assertTrue(
            enrichment_service.collect_for_market_date.call_args.kwargs[
                'daily_price_bar_lock_owned'
            ]
        )

    def test_grouped_price_context_failure_marks_technical_stage_partial(
        self,
    ) -> None:
        pykrx_runner = MagicMock()
        selected_symbols = ['005930', '000660']
        pykrx_runner.run_daily_pipeline_stages.return_value = {
            'run_id': 903,
            'status': 'completed_with_errors',
            'latest_available_trade_date': '2026-04-14',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'bars_upserted': 2,
            'universe_snapshot': _complete_pykrx_universe_snapshot(
                market_date='2026-04-14',
                markets=['KOSPI', 'KOSDAQ'],
                symbols=selected_symbols,
            ),
            **_build_pykrx_invocation_scope_proof(selected_symbols),
            **_build_exact_scope_outcome(
                selected_symbols=selected_symbols,
                successful_symbols=selected_symbols,
                retry_debt_symbols=[],
            ),
            'price_contexts': {
                'contexts_persisted': 1,
                'contexts_failed': 1,
                '_retry_metadata': {'failed_symbols': ['000660']},
            },
            'technical_snapshots': {
                'market_date': '2026-04-14',
                'snapshots_persisted': 2,
                'snapshots_failed': 0,
            },
        }
        service = DailyPipelineStageService(pykrx_service=pykrx_runner)

        with patch.object(
            service,
            '_execute_stage',
            return_value={'status': 'completed'},
        ) as execute_stage:
            result = service.run(
                market_date='2026-04-14',
                stages=['price_collection', 'technical_snapshots'],
            )

        self.assertEqual(execute_stage.call_args.args[0], 'master_refresh')
        technical = result['stages']['technical_snapshots']
        self.assertEqual(technical['status'], 'partial')
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(technical['result']['contexts_persisted'], 1)
        self.assertEqual(technical['result']['contexts_failed'], 1)
        self.assertEqual(
            technical['result']['price_contexts']['_retry_metadata'][
                'failed_symbols'
            ],
            ['000660'],
        )
        technical_finish = next(
            event
            for event in self.run_state_service.stage_events
            if event['event'] == 'finish_stage'
            and event['stage_id'] == 'technical_snapshots'
        )
        self.assertEqual(technical_finish['status'], 'partial')
        self.assertEqual(technical_finish['failed'], 1)

    def test_execute_pykrx_enrichment_blocks_zero_required_coverage(self) -> None:
        required_categories = (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
        collection_result = {
            'status': 'completed',
            'categories_requested': list(required_categories),
            'categories': {
                category: {'status': 'completed', 'rows_persisted': 0, 'errors': []}
                for category in required_categories
            },
            'coverage': {
                'categories': {
                    category: {
                        'scope_type': 'symbol',
                        'expected_rows': 1,
                        'present_rows': 0,
                        'coverage_ratio': 0.0,
                        'coverage_status': 'missing',
                    }
                    for category in required_categories
                }
            },
        }
        enrichment_service = MagicMock()
        enrichment_service.collect_for_market_date.return_value = collection_result
        service = DailyPipelineStageService(
            pykrx_enrichment_service_ref=enrichment_service,
        )

        result = service._execute_stage(
            stage_id='pykrx_enrichment',
            market_date='2026-04-14',
            symbols=['005930'],
            markets=['KOSPI'],
            force=False,
            bypass_prerequisite_gate=False,
            fs_div='CFS',
            symbol_limit=None,
            symbol_offset=0,
            capture_limit=500,
            retry_cycles=None,
            feedback_max_cycles=3,
            feedback_outer_batch_size=20,
            reuse_existing_feedback=True,
        )

        self.assertEqual(result['status'], 'partial')
        self.assertFalse(result['downstream_preflight']['allowed'])
        self.assertEqual(
            set(result['downstream_preflight']['failed_categories']),
            set(required_categories),
        )

    def test_execute_pykrx_enrichment_completes_ready_provider_warning(
        self,
    ) -> None:
        required_categories = (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
        collection_result = {
            'status': 'partial',
            'categories_requested': list(required_categories),
            'categories': {
                'sector_classification': {
                    'status': 'completed',
                    'rows_persisted': 10,
                    'errors': [],
                },
                'investor_flow_symbol': {
                    'status': 'completed',
                    'rows_persisted': 10,
                    'errors': [],
                },
                'short_selling': {
                    'status': 'partial',
                    'rows_persisted': 10,
                    'errors': ['empty provider frame'],
                },
                'valuation_snapshot': {
                    'status': 'completed',
                    'rows_persisted': 10,
                    'errors': [],
                },
            },
            'coverage': {},
        }
        ready_preflight = {
            'allowed': True,
            'status': 'ready',
            'required_categories': list(required_categories),
            'category_row_counts': {
                category: 10 for category in required_categories
            },
            'failed_categories': [],
            'blocked_categories': [],
            'waiting_categories': [],
            'zero_coverage_categories': [],
        }
        service, result = self._execute_pykrx_enrichment(
            collection_result=collection_result,
            preflight=ready_preflight,
        )

        self.assertEqual(result['status'], 'completed')
        outcome = service._build_stage_outcome(
            'pykrx_enrichment',
            result,
        )
        self.assertEqual(outcome['status'], 'completed')
        self.assertTrue(service._dependency_outcome_is_satisfied(outcome))
        self.assertEqual(
            service._extract_stage_counts(
                stage_id='pykrx_enrichment',
                result=result,
            ),
            {
                'processed': 4,
                'total': 4,
                'succeeded': 4,
                'failed': 0,
            },
        )

    def test_execute_pykrx_enrichment_partitions_exact_symbol_coverage(
        self,
    ) -> None:
        required_categories = (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
        symbols = ['005930', '000660']
        coverage_categories = {
            category: {
                'expected_scope_keys': list(symbols),
                'complete_scope_keys': list(symbols),
            }
            for category in required_categories
        }
        coverage_categories['short_selling']['complete_scope_keys'] = [
            '005930'
        ]
        collection_result = {
            'status': 'completed',
            'categories_requested': list(required_categories),
            'categories': {
                category: {
                    'status': 'completed',
                    'rows_persisted': 2,
                    'errors': [],
                }
                for category in required_categories
            },
            'coverage': {'categories': coverage_categories},
        }
        preflight = {
            'allowed': True,
            'status': 'ready',
            'required_categories': list(required_categories),
            'category_row_counts': {
                category: 2 for category in required_categories
            },
            'failed_categories': [],
            'blocked_categories': [],
            'waiting_categories': [],
            'zero_coverage_categories': [],
        }

        _, result = self._execute_pykrx_enrichment(
            collection_result=collection_result,
            preflight=preflight,
            symbols=symbols,
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['selected_stock_codes'], symbols)
        self.assertEqual(result['successful_stock_codes'], ['005930'])
        self.assertEqual(result['retry_debt_stock_codes'], ['000660'])
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            ['000660'],
        )
        self.assertEqual(
            result['exact_scope_retry_reasons']['000660'],
            ['short_selling:incomplete_required_coverage'],
        )

    def test_execute_pykrx_enrichment_keeps_incomplete_optional_work_partial(
        self,
    ) -> None:
        required_categories = (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        )
        ready_preflight = {
            'allowed': True,
            'status': 'ready',
            'required_categories': list(required_categories),
            'category_row_counts': {
                category: 10 for category in required_categories
            },
            'failed_categories': [],
            'blocked_categories': [],
            'waiting_categories': [],
            'zero_coverage_categories': [],
        }
        required_results = {
            category: {
                'status': 'completed',
                'rows_persisted': 10,
                'errors': [],
            }
            for category in required_categories
        }
        cases = {
            'failed_optional_category': (
                {
                    **required_results,
                    'foreign_ownership': {
                        'status': 'partial',
                        'rows_persisted': 0,
                        'errors': ['provider unavailable'],
                    },
                },
                {'processed': 4, 'total': 4, 'succeeded': 3, 'failed': 1},
            ),
            'missing_optional_category': (
                required_results,
                {'processed': 3, 'total': 4, 'succeeded': 3, 'failed': 0},
            ),
        }

        for case_name, (category_results, expected_counts) in cases.items():
            with self.subTest(case=case_name):
                collection_result = {
                    'status': 'partial',
                    'categories_requested': [
                        *required_categories,
                        'foreign_ownership',
                    ],
                    'categories': category_results,
                    'coverage': {},
                }
                service, result = self._execute_pykrx_enrichment(
                    collection_result=collection_result,
                    preflight=ready_preflight,
                )

                self.assertEqual(result['status'], 'partial')
                outcome = service._build_stage_outcome(
                    'pykrx_enrichment',
                    result,
                )
                self.assertEqual(outcome['status'], 'partial')
                self.assertFalse(
                    service._dependency_outcome_is_satisfied(outcome)
                )
                self.assertEqual(
                    service._extract_stage_counts(
                        stage_id='pykrx_enrichment',
                        result=result,
                    ),
                    expected_counts,
                )

    def test_execute_pykrx_enrichment_keeps_unproven_warning_partial(
        self,
    ) -> None:
        collection_result = {
            'status': 'partial',
            'categories_requested': ['short_selling'],
            'categories': {
                'short_selling': {
                    'status': 'partial',
                    'rows_persisted': 10,
                    'errors': ['empty provider frame'],
                },
            },
            'coverage': {},
        }
        service, result = self._execute_pykrx_enrichment(
            collection_result=collection_result,
            preflight={
                'allowed': True,
                'status': 'ready',
                'required_categories': ['short_selling'],
                'category_row_counts': {'short_selling': 10},
                'failed_categories': [],
                'blocked_categories': [],
                'waiting_categories': [],
                'zero_coverage_categories': [],
            },
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            service._build_stage_outcome(
                'pykrx_enrichment',
                result,
            )['status'],
            'partial',
        )

    def test_recovered_stage_status_is_terminal_completed(self) -> None:
        self.assertEqual(
            DailyPipelineStageService._resolve_result_status(
                {'status': 'recovered'}
            ),
            'completed',
        )

    def test_run_blocks_next_open_plan_when_signal_decider_is_incomplete(self) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'signal_decider':
                return {'status': 'blocked', 'reason': 'feedback_prerequisites_not_ready'}
            raise AssertionError('next-open plan refresh must not run after a blocked decider')

        with (
            patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
            patch.object(service, '_execute_stage', side_effect=execute) as execute_stage,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': datetime(
                        2026,
                        4,
                        14,
                        8,
                        30,
                        tzinfo=timezone.utc,
                    ),
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'broad_feedback_universe',
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=['next_open_execution_plan'],
            )

        self.assertNotIn('signal_decider', result['stages'])
        self.assertEqual(result['stages']['next_open_execution_plan']['status'], 'blocked')
        self.assertEqual(
            result['stages']['next_open_execution_plan']['result']['reason'],
            'signal_decider_stage_incomplete',
        )
        execute_stage.assert_not_called()

    def test_run_blocks_both_downstream_stages_without_terminal_decider_proof(
        self,
    ) -> None:
        service = DailyPipelineStageService()

        def execute(stage_id, **_):
            if stage_id == 'signal_decider':
                return {
                    'status': 'completed',
                    'run_id': 'decider-x',
                }
            raise AssertionError(
                'proofless decider must not admit downstream execution'
            )

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value={
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': datetime(
                        2026,
                        4,
                        14,
                        8,
                        30,
                        tzinfo=timezone.utc,
                    ),
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'broad_feedback_universe',
                },
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
            )

        self.assertEqual(
            [call.args[0] for call in execute_stage.call_args_list],
            [],
        )
        self.assertEqual(
            result['stages']['next_open_execution_plan']['status'],
            'blocked',
        )
        self.assertEqual(
            result['stages']['signal_decider_evaluations']['status'],
            'blocked',
        )

    def test_finish_downgrades_proofless_strict_completion(self) -> None:
        service = DailyPipelineStageService()
        for stage_id in (
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ):
            with self.subTest(stage_id=stage_id):
                self.run_state_service.stage_events.clear()
                service._finish_tracked_stage(
                    pipeline_run_id=9000,
                    stage_id=stage_id,
                    market_date='2026-04-14',
                    outcome={'status': 'completed'},
                    result={
                        'status': 'completed',
                        'run_id': f'{stage_id}-run',
                    },
                )

                finish = self.run_state_service.stage_events[-1]
                self.assertEqual(finish['status'], 'partial')
                self.assertEqual(
                    finish['details']['completion_authority_status'],
                    'invalid',
                )

    def test_evaluation_completion_proof_binds_exact_parent_scope(self) -> None:
        symbols = ['000660', '005930']
        parent_proof = _terminal_decider_completion_proof(
            symbols=symbols,
            cutoff_at=datetime(
                2026,
                4,
                14,
                8,
                30,
                tzinfo=timezone.utc,
            ),
        )
        result = _completed_evaluation_result(symbols=symbols)

        proof = (
            DailyPipelineStageService
            ._build_evaluations_completion_scope_proof(
                result,
                parent_scope_proof=parent_proof,
            )
        )

        self.assertIsNotNone(proof)
        self.assertEqual(proof['parent_completed_symbols'], symbols)
        self.assertEqual(proof['decision_count'], len(symbols))
        self.assertEqual(proof['item_count'], 4)
        self.assertEqual(
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider_evaluations',
                proof,
            ),
            proof,
        )

    def test_evaluation_completion_proof_rejects_subset_or_mutated_identity(
        self,
    ) -> None:
        symbols = ['000660', '005930']
        parent_proof = _terminal_decider_completion_proof(
            symbols=symbols,
            cutoff_at=datetime(
                2026,
                4,
                14,
                8,
                30,
                tzinfo=timezone.utc,
            ),
        )
        base_result = _completed_evaluation_result(symbols=symbols)
        cases = {}
        subset = deepcopy(base_result)
        subset['items'] = subset['items'][:1]
        subset['summary'].update(
            decisions_considered=1,
            outcomes_upserted=2,
            evaluations_upserted=2,
        )
        cases['subset'] = subset
        different_same_size = deepcopy(base_result)
        different_same_size['items'][1]['symbol'] = '999999'
        cases['different_same_size'] = different_same_size
        duplicate_same_symbol = deepcopy(base_result)
        duplicate_same_symbol['items'][1] = deepcopy(
            duplicate_same_symbol['items'][0]
        )
        cases['duplicate_same_symbol'] = duplicate_same_symbol
        mutated_snapshot = deepcopy(base_result)
        mutated_snapshot['items'][0]['evaluations'][0][
            'decision_snapshot_id'
        ] = 999
        cases['mutated_snapshot'] = mutated_snapshot
        mutated_action = deepcopy(base_result)
        mutated_action['items'][0]['outcomes'][0][
            'decision_action'
        ] = 'sell'
        cases['mutated_action'] = mutated_action

        for label, result in cases.items():
            with self.subTest(label=label):
                self.assertIsNone(
                    DailyPipelineStageService
                    ._build_evaluations_completion_scope_proof(
                        result,
                        parent_scope_proof=parent_proof,
                    )
                )

    def test_signal_terminal_proof_rejects_completed_scope_containing_retry_debt(
        self,
    ) -> None:
        proof = _terminal_decider_completion_proof(
            symbols=['000660', '005930'],
            cutoff_at=datetime(
                2026,
                4,
                14,
                8,
                30,
                tzinfo=timezone.utc,
            ),
        )
        proof['failed_symbols_by_role']['short-term-decider'] = [
            '005930'
        ]
        proof['completed_items'] = 3
        proof['failed_items'] = 1
        proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(proof)
        )

        self.assertIsNone(
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'signal_decider',
                proof,
            )
        )

    def test_next_open_completion_proof_partitions_partial_parent_intersection(
        self,
    ) -> None:
        symbols = ['000660', '005930']
        parent_proof = _terminal_decider_completion_proof(
            symbols=symbols,
            cutoff_at=datetime(
                2026,
                4,
                14,
                8,
                30,
                tzinfo=timezone.utc,
            ),
        )
        parent_proof['completed_symbols'] = ['000660']
        parent_proof['decision_identities'] = [
            identity
            for identity in parent_proof['decision_identities']
            if identity['symbol'] == '000660'
        ]
        parent_proof['failed_symbols_by_role'][
            'short-term-decider'
        ] = ['005930']
        parent_proof['completed_items'] = 3
        parent_proof['failed_items'] = 1
        parent_proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(parent_proof)
        )
        stale_result = _next_open_completion_result(
            parent_run_id=parent_proof['source_run_id'],
            completed_symbols=['000660'],
            manifest_symbols=symbols,
        )

        self.assertIsNone(
            DailyPipelineStageService
            ._build_next_open_completion_scope_proof(
                stale_result,
                parent_scope_proof=parent_proof,
            )
        )
        result = _next_open_completion_result(
            parent_run_id=parent_proof['source_run_id'],
            completed_symbols=['000660'],
        )
        proof = (
            DailyPipelineStageService
            ._build_next_open_completion_scope_proof(
                result,
                parent_scope_proof=parent_proof,
            )
        )

        self.assertIsNotNone(proof)
        self.assertEqual(proof['parent_completed_symbols'], ['000660'])
        self.assertEqual(proof['decision_count'], 2)
        self.assertEqual(proof['included_decision_count'], 1)
        self.assertEqual(proof['excluded_decision_count'], 1)
        self.assertEqual(proof['item_count'], 1)
        self.assertEqual(
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                'next_open_execution_plan',
                proof,
            ),
            proof,
        )

    def test_next_open_completion_proof_rejects_missing_or_incomplete_parent_decision(
        self,
    ) -> None:
        symbols = ['000660', '005930']
        parent_proof = _terminal_decider_completion_proof(
            symbols=symbols,
            cutoff_at=datetime(
                2026,
                4,
                14,
                8,
                30,
                tzinfo=timezone.utc,
            ),
        )
        base_result = _next_open_completion_result(
            parent_run_id=parent_proof['source_run_id'],
            completed_symbols=symbols,
        )
        cases = {}
        missing = deepcopy(base_result)
        missing_manifest = missing['completion_manifest']
        missing_manifest['decision_partition'].pop()
        missing_manifest['decision_count'] -= 1
        missing_manifest['manifest_digest'] = hashlib.sha256(
            json.dumps(
                {
                    key: value
                    for key, value in missing_manifest.items()
                    if key != 'manifest_digest'
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(',', ':'),
            ).encode('utf-8')
        ).hexdigest()
        cases['missing_parent_decision'] = missing

        incomplete = deepcopy(base_result)
        incomplete_manifest = incomplete['completion_manifest']
        signal_entry = next(
            entry
            for entry in incomplete_manifest['decision_partition']
            if entry['decision_agent_name'] == 'signal-decider'
        )
        removed_plan_item_id = signal_entry['included_items'][0][
            'plan_item_id'
        ]
        signal_entry['disposition'] = 'incomplete'
        signal_entry['included_items'] = []
        incomplete_manifest['included_decision_count'] -= 1
        incomplete_manifest['incomplete_decision_count'] += 1
        incomplete_manifest['included_item_count'] -= 1
        incomplete['items'] = [
            item
            for item in incomplete['items']
            if item['plan_item_id'] != removed_plan_item_id
        ]
        incomplete_manifest['manifest_digest'] = hashlib.sha256(
            json.dumps(
                {
                    key: value
                    for key, value in incomplete_manifest.items()
                    if key != 'manifest_digest'
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(',', ':'),
            ).encode('utf-8')
        ).hexdigest()
        cases['actionable_without_materialized_row'] = incomplete

        for label, result in cases.items():
            with self.subTest(label=label):
                self.assertIsNone(
                    DailyPipelineStageService
                    ._build_next_open_completion_scope_proof(
                        result,
                        parent_scope_proof=parent_proof,
                    )
                )

    def test_run_advances_plan_and_evaluation_for_terminal_partial_decider_intersection(
        self,
    ) -> None:
        service = DailyPipelineStageService()
        symbols = ['000660', '005930']
        roles = list(DECISION_SUPPORT_AGENT_NAMES)
        failed_role = 'short-term-decider'
        cutoff_at = datetime(
            2026,
            4,
            14,
            8,
            30,
            tzinfo=timezone.utc,
        )
        completed_scope_proof = {
            'schema_version': 'completed_summarizer_intersection_v1',
            'completed_symbols': symbols,
            'admitted_symbols': symbols,
        }
        cutoff_resolution = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': cutoff_at,
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'broad_feedback_universe',
            'requested_symbols': symbols,
            'completed_scope_proof': completed_scope_proof,
        }

        role_progress = {
            role: {
                'role': role,
                'status': (
                    'completed_with_failures'
                    if role == failed_role
                    else 'completed'
                ),
                'boundary_status': (
                    'completed_with_failures'
                    if role == failed_role
                    else 'completed'
                ),
                'total_symbols': 2,
                'processed_symbols': 2,
                'completed_symbols': 1 if role == failed_role else 2,
                'terminal_symbols': 2,
                'failed_items': 1 if role == failed_role else 0,
                'in_progress_symbols': 0,
                'pending_symbols': 0,
                'retry_pending_symbols': 1 if role == failed_role else 0,
            }
            for role in roles
        }
        progress_summary = {
            'run_id': 'signal-decider-partial',
            'run_status': 'partial',
            'run_table_status': 'partial',
            'run_completed_at': cutoff_at.isoformat(),
            'is_live': False,
            'expected_feedback_count': 4,
            'completed_feedback_count': 3,
            'failed_items': 1,
            'role_symbol_scopes': {
                role: list(symbols) for role in roles
            },
            'role_symbol_counts': {role: 2 for role in roles},
            'failed_symbols_by_role': {
                failed_role: ['005930'],
            },
            'role_progress': role_progress,
        }

        def execute(stage_id, **kwargs):
            if stage_id == 'signal_decider':
                return {
                    'status': 'partial',
                    'run_id': 'signal-decider-partial',
                    'roles': roles,
                    'feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'requested_symbols': symbols,
                    'scope_authority': {
                        'schema_version': (
                            FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION
                        ),
                        'kind': (
                            FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION
                        ),
                        'source_summary_run_id': (
                            'feedback-cycle-summary'
                        ),
                        'source_feedback_scope_mode': (
                            'broad_feedback_universe'
                        ),
                        'decision_feedback_scope_mode': (
                            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                        ),
                        'knowledge_cutoff_at': cutoff_at.isoformat(),
                        'completed_scope_proof': completed_scope_proof,
                    },
                    'decision_identities': _decision_identities(
                        ['000660']
                    ),
                    'progress': {'summary': progress_summary},
                }
            if stage_id == 'next_open_execution_plan':
                return _next_open_completion_result(
                    parent_run_id='signal-decider-partial',
                    completed_symbols=['000660'],
                )
            if stage_id == 'signal_decider_evaluations':
                return _completed_evaluation_result(
                    symbols=list(kwargs['symbols'])
                )
            raise AssertionError(f'unexpected stage: {stage_id}')

        with (
            patch.object(
                service,
                '_normalize_market_date',
                return_value='2026-04-14',
            ),
            patch.object(
                service,
                '_execute_stage',
                side_effect=execute,
            ) as execute_stage,
            patch(
                'app.eod.daily_pipeline_stages.'
                'resolve_latest_completed_summary_knowledge_cutoff',
                return_value=cutoff_resolution,
            ),
        ):
            result = service.run(
                market_date='2026-04-14',
                stages=[
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                symbols=symbols,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['signal_decider']['status'], 'partial')
        self.assertEqual(
            result['stages']['next_open_execution_plan']['status'],
            'completed',
        )
        self.assertEqual(
            result['stages']['signal_decider_evaluations']['status'],
            'completed',
        )
        evaluation_call = next(
            call
            for call in execute_stage.call_args_list
            if call.args[0] == 'signal_decider_evaluations'
        )
        self.assertEqual(evaluation_call.kwargs['symbols'], ['000660'])
        marker = service._build_nonblocking_partial_progression_marker(
            market_date='2026-04-14',
            pipeline_run_id=96,
            reused_eod_collection_run_id=7,
            trigger='auto_eod_followup_resume',
            stage_outcomes=result['stages'],
            feedback_cutoff_resolution=cutoff_resolution,
        )
        self.assertIsNotNone(marker)
        self.assertEqual(
            marker['decider']['completed_symbols'],
            ['000660'],
        )
        self.assertEqual(
            marker['retry_debt']['decider_failed_symbols_by_role'][
                'short-term-decider'
            ],
            ['005930'],
        )
        blocked_stages = deepcopy(result['stages'])
        blocked_stages['next_open_execution_plan']['status'] = 'blocked'
        self.assertIsNone(
            service._build_nonblocking_partial_progression_marker(
                market_date='2026-04-14',
                pipeline_run_id=96,
                reused_eod_collection_run_id=7,
                trigger='auto_eod_followup_resume',
                stage_outcomes=blocked_stages,
                feedback_cutoff_resolution=cutoff_resolution,
            )
        )

    def test_partial_decider_intersection_rejects_tampered_scope_and_lineage(
        self,
    ) -> None:
        roles = list(DECISION_SUPPORT_AGENT_NAMES)
        symbols = ['000660', '005930']
        cutoff_at = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
        scope_proof = {
            'schema_version': 'completed_summarizer_intersection_v1',
            'completed_symbols': symbols,
            'admitted_symbols': symbols,
        }
        cutoff_resolution = {
            'ready': True,
            'knowledge_cutoff_at': cutoff_at,
            'source_run_id': 'summary-run',
            'feedback_scope_mode': 'broad_feedback_universe',
            'requested_symbols': symbols,
            'completed_scope_proof': scope_proof,
        }
        progress_summary = {
            'run_id': 'decider-run',
            'run_status': 'partial',
            'run_table_status': 'partial',
            'run_completed_at': cutoff_at.isoformat(),
            'is_live': False,
            'expected_feedback_count': 4,
            'completed_feedback_count': 3,
            'failed_items': 1,
            'role_symbol_scopes': {
                role: list(symbols) for role in roles
            },
            'role_symbol_counts': {role: 2 for role in roles},
            'failed_symbols_by_role': {
                'short-term-decider': ['005930'],
            },
            'role_progress': {
                role: {
                    'role': role,
                    'status': (
                        'completed_with_failures'
                        if role == 'short-term-decider'
                        else 'completed'
                    ),
                    'boundary_status': (
                        'completed_with_failures'
                        if role == 'short-term-decider'
                        else 'completed'
                    ),
                    'total_symbols': 2,
                    'processed_symbols': 2,
                    'completed_symbols': (
                        1 if role == 'short-term-decider' else 2
                    ),
                    'terminal_symbols': 2,
                    'failed_items': (
                        1 if role == 'short-term-decider' else 0
                    ),
                    'in_progress_symbols': 0,
                    'pending_symbols': 0,
                    'retry_pending_symbols': (
                        1 if role == 'short-term-decider' else 0
                    ),
                }
                for role in roles
            },
        }
        outcome = {
            'status': 'partial',
            'result': {
                'status': 'partial',
                'run_id': 'decider-run',
                'roles': roles,
                'feedback_scope_mode': (
                    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                ),
                'requested_symbols': symbols,
                'scope_authority': {
                    'schema_version': (
                        FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION
                    ),
                    'kind': (
                        FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION
                    ),
                    'source_summary_run_id': 'summary-run',
                    'source_feedback_scope_mode': (
                        'broad_feedback_universe'
                    ),
                    'decision_feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    ),
                    'knowledge_cutoff_at': cutoff_at.isoformat(),
                    'completed_scope_proof': scope_proof,
                },
                'decision_identities': _decision_identities(
                    ['000660']
                ),
                'progress': {'summary': progress_summary},
            },
        }

        resolver = (
            DailyPipelineStageService
            ._terminal_decider_completed_symbols
        )
        self.assertEqual(
            resolver(
                outcome,
                expected_decider_symbols=symbols,
                feedback_cutoff_resolution=cutoff_resolution,
            ),
            ['000660'],
        )

        tampered_cases = []
        wrong_lineage = deepcopy(outcome)
        wrong_lineage['result']['scope_authority'][
            'source_summary_run_id'
        ] = 'other-summary-run'
        tampered_cases.append(('summary_lineage', wrong_lineage))
        wrong_requested_scope = deepcopy(outcome)
        wrong_requested_scope['result']['requested_symbols'] = [
            '000660',
            '035420',
        ]
        tampered_cases.append(('requested_scope', wrong_requested_scope))
        wrong_progress_scope = deepcopy(outcome)
        wrong_progress_scope['result']['progress']['summary'][
            'role_symbol_scopes'
        ]['signal-decider'] = ['000660', '035420']
        tampered_cases.append(('progress_scope', wrong_progress_scope))
        contradictory_status = deepcopy(outcome)
        contradictory_status['result']['progress']['summary'][
            'run_status'
        ] = 'completed'
        contradictory_status['result']['progress']['summary'][
            'run_table_status'
        ] = 'completed'
        tampered_cases.append(('status_counts', contradictory_status))

        for case_name, tampered_outcome in tampered_cases:
            with self.subTest(case=case_name):
                self.assertIsNone(
                    resolver(
                        tampered_outcome,
                        expected_decider_symbols=symbols,
                        feedback_cutoff_resolution=cutoff_resolution,
                    )
                )


if __name__ == '__main__':
    unittest.main()
