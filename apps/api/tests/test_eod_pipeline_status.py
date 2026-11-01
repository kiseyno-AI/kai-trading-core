import hashlib
import json
import unittest
from copy import deepcopy
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from app.agent_context.roles import (
    DECISION_SUPPORT_AGENT_NAMES,
    SUMMARIZER_AGENT_NAMES,
)
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.db_models import DailyMomentumFactorResult
from app.eod.daily_pipeline_stages import DailyPipelineStageService
from app.eod.pipeline_status import (
    DEFAULT_FACTOR_STAGE_ORDER,
    DEFAULT_PYKRX_ENRICHMENT_CATEGORIES,
    EODPipelineStatusService,
    _load_daily_pipeline_stage_history,
    _load_latest_feedback_run_summary,
    _load_latest_pipeline_pykrx_run,
    _load_pipeline_pykrx_run_rows,
    _load_row_count_missing_diagnostics_for_model,
    _signal_feature_row_is_semantically_ready,
)
from app.eod.market_membership import build_market_membership_snapshot


class _FakeMarketState:
    market = 'KRX'
    state = 'CLOSED'
    is_trading_day = True
    is_open = False
    reason = None
    server_time = '2026-04-21T18:15:00+09:00'
    current_calendar_date = '2026-04-21'
    latest_valid_trading_date = '2026-04-21'
    market_session = 'closed'


class _FakeBreakDayMarketState:
    market = 'KRX'
    state = 'HOLIDAY'
    is_trading_day = False
    is_open = False
    reason = 'holiday'
    server_time = '2026-05-05T12:15:00+09:00'
    current_calendar_date = '2026-05-05'
    latest_valid_trading_date = '2026-05-04'
    market_session = 'holiday'


def _full_factor_stage_counts(*, total: int, updated_at: str) -> dict[str, dict]:
    return {
        stage_id: {
            'persisted': total,
            'updated_at': updated_at,
        }
        for stage_id in DEFAULT_FACTOR_STAGE_ORDER
    }


def _full_feedback_prerequisite_stage_counts(*, total: int, updated_at: str) -> dict[str, dict]:
    return {
        'market_risk_filter': {
            'persisted': total,
            'updated_at': updated_at,
        },
        'piotroski_filter': {
            'persisted': total,
            'updated_at': updated_at,
        },
        **_full_factor_stage_counts(total=total, updated_at=updated_at),
    }


def _full_pykrx_enrichment_coverage(*, symbol_count: int, market_count: int, updated_at: str) -> dict[str, dict]:
    return {
        'requested_symbol_count': symbol_count,
        'requested_market_count': market_count,
        'categories': {
            category: {
                'present_rows': symbol_count if category not in {'market_breadth', 'investor_flow_market'} else market_count,
                'expected_rows': symbol_count if category not in {'market_breadth', 'investor_flow_market'} else market_count,
                'coverage_status': 'complete',
                'latest_captured_at': updated_at,
            }
            for category in DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
        },
    }


def _scope_gap_evidence(
    *,
    requested_count: int,
    present_count: int,
    missing_symbols_sample: list[str] | None = None,
    **extra,
) -> dict:
    return {
        'evidence_version': 'row_count_scope_gap_v1',
        'authoritative': True,
        'requested_count': requested_count,
        'present_count': present_count,
        'missing_count': requested_count - present_count,
        'missing_symbols_sample': list(missing_symbols_sample or []),
        **extra,
    }


def _blocked_pykrx_enrichment_coverage(*, expected_symbols: int, market_count: int, updated_at: str) -> dict[str, dict]:
    present_by_category = {
        'sector_classification': int(expected_symbols * 0.62),
        'investor_flow_symbol': int(expected_symbols * 0.60),
        'short_selling': int(expected_symbols * 0.63),
        'valuation_snapshot': int(expected_symbols * 0.61),
        'foreign_ownership': expected_symbols,
    }
    return {
        'requested_symbol_count': expected_symbols,
        'requested_market_count': market_count,
        'categories': {
            category: {
                'present_rows': (
                    market_count
                    if category in {'market_breadth', 'investor_flow_market'}
                    else present_by_category.get(category, expected_symbols)
                ),
                'expected_rows': (
                    market_count
                    if category in {'market_breadth', 'investor_flow_market'}
                    else expected_symbols
                ),
                'coverage_ratio': (
                    1.0
                    if category in {'market_breadth', 'investor_flow_market', 'foreign_ownership'}
                    else round(present_by_category.get(category, expected_symbols) / expected_symbols, 6)
                ),
                'coverage_status': (
                    'complete'
                    if category in {'market_breadth', 'investor_flow_market', 'foreign_ownership'}
                    else 'partial'
                ),
                'latest_captured_at': updated_at,
            }
            for category in DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
        },
    }


def _available_macro_context_counts(*, updated_at: str = '2026-06-20T06:00:00+00:00') -> dict:
    return {
        'persisted': 3,
        'required_series_count': 3,
        'updated_at': updated_at,
        'context_status': 'available',
        'source_statuses': {
            'world_bank': {'status': 'available', 'series_count': 3},
            'fred': {'status': 'disabled_missing_api_key'},
        },
        'macro_regime_summary': {
            'growth_label': 'growth_expansion',
            'inflation_label': 'inflation_stable',
            'llm_generated': False,
        },
        'usage_policy': {
            'contextual_input_only': True,
            'direct_buy_sell_signal': False,
            'llm_generated': False,
        },
    }


def _completed_official_stage(
    *,
    run_id: int,
    stage_id: str,
    market_date: str,
    processed: int = 1,
    total: int = 1,
    updated_at: str = '2026-05-28T08:30:00+00:00',
) -> dict[str, dict]:
    return {
        'run': {
            'id': run_id,
            'market_date': market_date,
            'trigger': 'manual_api',
            'status': 'completed',
            'current_stage': stage_id,
            'started_at': updated_at,
            'updated_at': updated_at,
            'completed_at': updated_at,
            'error_message': None,
            'run_metadata': {'requested_stages': [stage_id], 'resolved_stages': [stage_id]},
        },
        'stage': {
            'stage_id': stage_id,
            'status': 'completed',
            'market_date': market_date,
            'started_at': updated_at,
            'updated_at': updated_at,
            'completed_at': updated_at,
            'processed': processed,
            'total': total,
            'succeeded': processed,
            'failed': 0,
            'remaining': 0,
            'current_step': None,
            'current_activity': None,
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': {},
        },
    }


def _terminal_decider_completion_scope_proof(
    *,
    symbols: list[str],
    parent_run_id: str,
    source_summary_run_id: str,
) -> dict:
    decision_identities = sorted(
        (
            {
                'symbol': symbol,
                'decision_agent_name': role,
                'decision_snapshot_id': (index * 2) + role_index + 1,
                'decision_run_id': (
                    f'{parent_run_id}-'
                    f'{"short" if role == "short-term-decider" else "signal"}'
                    f'-{index}'
                ),
            }
            for index, symbol in enumerate(symbols)
            for role_index, role in enumerate(
                DECISION_SUPPORT_AGENT_NAMES
            )
        ),
        key=lambda identity: (
            identity['symbol'],
            identity['decision_agent_name'],
        ),
    )
    proof = {
        'schema_version': 'stage_completion_scope_v1',
        'kind': 'signal_decider_terminal_progression',
        'roles': list(DECISION_SUPPORT_AGENT_NAMES),
        'source_run_id': parent_run_id,
        'source_summary_run_id': source_summary_run_id,
        'knowledge_cutoff_at': '2026-10-07T08:00:00+00:00',
        'admitted_symbols': list(symbols),
        'completed_symbols': list(symbols),
        'failed_symbols_by_role': {
            role: [] for role in DECISION_SUPPORT_AGENT_NAMES
        },
        'decision_identities': decision_identities,
        'total_items': len(symbols) * len(DECISION_SUPPORT_AGENT_NAMES),
        'completed_items': (
            len(symbols) * len(DECISION_SUPPORT_AGENT_NAMES)
        ),
        'failed_items': 0,
    }
    proof['proof_digest'] = (
        DailyPipelineStageService._stage_completion_scope_proof_digest(
            proof
        )
    )
    return proof


def _completed_signal_decider_history_stage(
    *,
    run_id: int,
    market_date: str,
    symbol_count: int,
    updated_at: str = '2026-05-28T08:30:00+00:00',
) -> dict[str, dict]:
    symbols = [f'{index:06d}' for index in range(1, symbol_count + 1)]
    parent_run_id = f'feedback-cycle-{market_date}-{run_id}'
    source_summary_run_id = f'feedback-summary-{market_date}-{run_id}'
    proof = _terminal_decider_completion_scope_proof(
        symbols=symbols,
        parent_run_id=parent_run_id,
        source_summary_run_id=source_summary_run_id,
    )
    item = _completed_official_stage(
        run_id=run_id,
        stage_id='signal_decider',
        market_date=market_date,
        processed=proof['total_items'],
        total=proof['total_items'],
        updated_at=updated_at,
    )
    item['stage']['details'] = {'completion_scope_proof': proof}
    item['stage']['source_run_ids'] = {
        'signal_decider_run_id': parent_run_id,
        'source_summary_run_id': source_summary_run_id,
    }
    return item


def _completed_feedback_history_stage(
    *,
    run_id: int,
    market_date: str,
    total: int,
    role_counts: tuple[int, ...] | None = None,
    updated_at: str = '2026-05-28T08:30:00+00:00',
) -> dict[str, dict]:
    item = _completed_official_stage(
        run_id=run_id,
        stage_id='agent_feedback_summaries',
        market_date=market_date,
        processed=total,
        total=total,
        updated_at=updated_at,
    )
    feedback_run_id = f'feedback-cycle-{market_date}-{run_id}'
    if role_counts is None:
        base_count, remainder = divmod(
            total,
            len(SUMMARIZER_AGENT_NAMES),
        )
        role_counts = tuple(
            base_count + int(index < remainder)
            for index, _ in enumerate(SUMMARIZER_AGENT_NAMES)
        )
    if not (
        len(role_counts) == len(SUMMARIZER_AGENT_NAMES)
        and all(type(count) is int and count >= 0 for count in role_counts)
        and sum(role_counts) == total
    ):
        raise ValueError('role_counts must exactly partition total')
    next_symbol = 1
    role_symbol_scopes: dict[str, list[str]] = {}
    role_progress = {
        role: {
            'role': role,
            'status': 'completed',
            'boundary_status': 'completed',
            'total_symbols': role_counts[index],
            'processed_symbols': role_counts[index],
            'completed_symbols': role_counts[index],
            'in_progress_symbols': 0,
            'pending_symbols': 0,
            'failed_items': 0,
            'retry_pending_symbols': 0,
        }
        for index, role in enumerate(SUMMARIZER_AGENT_NAMES)
    }
    for role in SUMMARIZER_AGENT_NAMES:
        count = role_progress[role]['total_symbols']
        role_symbol_scopes[role] = [
            f'{symbol:06d}'
            for symbol in range(next_symbol, next_symbol + count)
        ]
        next_symbol += count
    result = {
        'status': 'completed',
        'run_id': feedback_run_id,
        'knowledge_cutoff_at': '2026-10-07T08:00:00+00:00',
        'feedback_scope_mode': 'broad_feedback_universe',
        'roles': list(SUMMARIZER_AGENT_NAMES),
        'progress': {
            'summary': {
                'run_id': feedback_run_id,
                'expected_feedback_count': total,
                'completed_feedback_count': total,
                'failed_items': 0,
                'role_symbol_scopes': role_symbol_scopes,
                'role_progress': role_progress,
            }
        },
    }
    item['stage']['source_run_ids'] = {'run_id': feedback_run_id}
    item['stage']['details'] = (
        DailyPipelineStageService._build_stage_run_details(
            stage_id='agent_feedback_summaries',
            result=result,
            invocation_symbols=sorted(
                {
                    symbol
                    for symbols in role_symbol_scopes.values()
                    for symbol in symbols
                }
            ),
        )
    )
    return item


def _completed_next_open_history_stage(
    *,
    run_id: int,
    market_date: str,
    total: int,
    updated_at: str = '2026-05-28T08:30:00+00:00',
) -> dict[str, dict]:
    item = _completed_official_stage(
        run_id=run_id,
        stage_id='next_open_execution_plan',
        market_date=market_date,
        processed=total,
        total=total,
        updated_at=updated_at,
    )
    parent_run_id = f'feedback-cycle-{market_date}-{run_id}'
    source_summary_run_id = f'feedback-summary-{market_date}-{run_id}'
    symbols = [f'{index:06d}' for index in range(1, total + 1)]
    plan_items = []
    decision_partition = []
    for index, symbol in enumerate(symbols):
        active = index == 0
        plan_item_id = index + 1
        snapshot_id = (index * 2) + 2
        decision_run_id = f'{parent_run_id}-signal-{index}'
        plan_items.append({
            'plan_item_id': plan_item_id,
            'account_reference': 'paper-primary',
            'symbol': symbol,
            'decision_agent_name': 'signal-decider',
            'decision_run_id': decision_run_id,
            'decision_snapshot_id': snapshot_id,
            'eligibility_status': 'eligible' if active else 'blocked',
            'handoff_active': active,
            'exec_monitoring_active': active,
        })
        decision_partition.extend(
            [
                {
                    'symbol': symbol,
                    'decision_agent_name': 'short-term-decider',
                    'decision_run_id': (
                        f'{parent_run_id}-short-{index}'
                    ),
                    'decision_snapshot_id': (index * 2) + 1,
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
                    'decision_run_id': decision_run_id,
                    'decision_snapshot_id': snapshot_id,
                    'canonical_action': 'buy',
                    'authority_fingerprint': f'authority-signal-{index}',
                    'generation_fingerprint': f'generation-signal-{index}',
                    'disposition': 'included',
                    'included_items': [
                        {
                            'plan_item_id': plan_item_id,
                            'account_reference': 'paper-primary',
                            'eligibility_status': (
                                'eligible' if active else 'blocked'
                            ),
                            'handoff_active': active,
                            'exec_monitoring_active': active,
                            'row_authority_fingerprint': (
                                f'row-authority-{index}'
                            ),
                        }
                    ],
                    'exclusion': None,
                },
            ]
        )
    decision_partition.sort(
        key=lambda entry: (
            entry['symbol'],
            entry['decision_agent_name'],
        )
    )
    completion_manifest = {
        'schema_version': 'next_open_completion_manifest_v1',
        'parent_run_id': parent_run_id,
        'authority_fingerprint': f'authority-{parent_run_id}',
        'generation_fingerprint': f'generation-{parent_run_id}',
        'role_symbol_scopes': {
            role: list(symbols) for role in DECISION_SUPPORT_AGENT_NAMES
        },
        'decision_partition': decision_partition,
        'decision_count': len(decision_partition),
        'included_decision_count': len(symbols),
        'excluded_decision_count': len(symbols),
        'incomplete_decision_count': 0,
        'included_item_count': len(plan_items),
    }
    completion_manifest['manifest_digest'] = hashlib.sha256(
        json.dumps(
            completion_manifest,
            ensure_ascii=True,
            sort_keys=True,
            separators=(',', ':'),
        ).encode('utf-8')
    ).hexdigest()
    item['stage']['details'] = (
        DailyPipelineStageService._build_stage_run_details(
            stage_id='next_open_execution_plan',
            result={
                'status': 'completed',
                'completion_manifest': completion_manifest,
                'items': plan_items,
            },
            completion_parent_proof=(
                _terminal_decider_completion_scope_proof(
                    symbols=symbols,
                    parent_run_id=parent_run_id,
                    source_summary_run_id=source_summary_run_id,
                )
            ),
        )
    )
    item['stage']['source_run_ids'] = {
        'parent_signal_decider_run_id': parent_run_id,
        'source_summary_run_id': source_summary_run_id,
    }
    return item


def _completed_zero_item_next_open_history_stage(
    *,
    run_id: int,
    market_date: str,
    updated_at: str = '2026-05-28T08:30:00+00:00',
) -> dict[str, dict]:
    item = _completed_official_stage(
        run_id=run_id,
        stage_id='next_open_execution_plan',
        market_date=market_date,
        processed=0,
        total=0,
        updated_at=updated_at,
    )
    symbol = '000001'
    parent_run_id = f'feedback-cycle-{market_date}-{run_id}'
    source_summary_run_id = f'feedback-summary-{market_date}-{run_id}'
    partition = [
        {
            'symbol': symbol,
            'decision_agent_name': role,
            'decision_run_id': (
                f'{parent_run_id}-'
                f'{"short" if role == "short-term-decider" else "signal"}'
                '-0'
            ),
            'decision_snapshot_id': index + 1,
            'canonical_action': 'wait',
            'authority_fingerprint': f'authority-{role}',
            'generation_fingerprint': f'generation-{role}',
            'disposition': 'non_actionable',
            'included_items': [],
            'exclusion': {
                'kind': 'non_actionable',
                'reason_code': 'latest_snapshot_not_actionable',
            },
        }
        for index, role in enumerate(DECISION_SUPPORT_AGENT_NAMES)
    ]
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
            role: [symbol] for role in DECISION_SUPPORT_AGENT_NAMES
        },
        'decision_partition': partition,
        'decision_count': len(partition),
        'included_decision_count': 0,
        'excluded_decision_count': len(partition),
        'incomplete_decision_count': 0,
        'included_item_count': 0,
    }
    manifest['manifest_digest'] = hashlib.sha256(
        json.dumps(
            manifest,
            ensure_ascii=True,
            sort_keys=True,
            separators=(',', ':'),
        ).encode('utf-8')
    ).hexdigest()
    item['stage']['details'] = (
        DailyPipelineStageService._build_stage_run_details(
            stage_id='next_open_execution_plan',
            result={
                'status': 'completed',
                'completion_manifest': manifest,
                'items': [],
            },
            completion_parent_proof=(
                _terminal_decider_completion_scope_proof(
                    symbols=[symbol],
                    parent_run_id=parent_run_id,
                    source_summary_run_id=source_summary_run_id,
                )
            ),
        )
    )
    item['stage']['source_run_ids'] = {
        'parent_signal_decider_run_id': parent_run_id,
        'source_summary_run_id': source_summary_run_id,
    }
    return item


def _incomplete_official_stage(
    *,
    run_id: int,
    stage_id: str,
    market_date: str,
    status: str,
    updated_at: str,
    succeeded: int,
    failed: int,
) -> dict[str, dict]:
    processed = succeeded + failed
    return {
        'run': {
            'id': run_id,
            'market_date': market_date,
            'trigger': 'manual_api',
            'status': status,
            'current_stage': stage_id,
            'started_at': updated_at,
            'updated_at': updated_at,
            'completed_at': updated_at,
            'error_message': 'Signal feature collection did not finish cleanly' if status == 'failed' else None,
            'run_metadata': {'requested_stages': [stage_id], 'resolved_stages': [stage_id]},
        },
        'stage': {
            'stage_id': stage_id,
            'status': status,
            'market_date': market_date,
            'started_at': updated_at,
            'updated_at': updated_at,
            'completed_at': updated_at,
            'processed': processed,
            'total': processed,
            'succeeded': succeeded,
            'failed': failed,
            'remaining': 0,
            'current_step': f'{stage_id}_{status}',
            'current_activity': f'{stage_id} finished {status}',
            'current_symbol': None,
            'current_period': None,
            'error_message': 'Signal feature collection did not finish cleanly' if status == 'failed' else None,
            'source_run_ids': {},
            'details': {
                'features_persisted': succeeded,
                'features_failed': failed,
            },
        },
    }


def _blocked_downstream_output_eod_run(
    *,
    market_date: str,
    updated_at: str,
) -> dict:
    blocked_progress = {
        'status': 'blocked',
        'processed': 0,
        'total': 0,
        'succeeded': 0,
        'failed': 0,
        'updated_at': updated_at,
        'result_summary': {
            'triggered': False,
            'status': 'blocked',
            'reason': 'inherited_downstream_recovery_pending',
            'market_date': market_date,
        },
    }
    return {
        'id': 561,
        'market': 'KRX',
        'trade_date': market_date,
        'source': 'pykrx',
        'trigger': 'auto_close_scheduler',
        'status': 'completed_with_errors',
        'symbols_total': 1,
        'symbols_succeeded': 1,
        'symbols_failed': 0,
        'error_message': 'Inherited downstream recovery remains pending.',
        'started_at': updated_at,
        'completed_at': updated_at,
        'run_metadata': {
            'mode': 'daily_auto',
            'last_progress_at': updated_at,
            'stage_progress': {
                stage_id: dict(blocked_progress)
                for stage_id in (
                    'signal_features',
                    'external_news_collect',
                    'external_news_enrich',
                    'agent_feedback_summaries',
                )
            },
        },
    }


def _legacy_completed_signal_decider_stage(
    *,
    market_date: str,
    updated_at: str,
    expected: int = 160,
    completed: int = 160,
) -> dict[str, dict]:
    item = _completed_official_stage(
        run_id=516,
        stage_id='signal_decider',
        market_date=market_date,
        processed=0,
        total=0,
        updated_at=updated_at,
    )
    item['stage']['details'] = {
        'result_summary': {
            'status': 'completed',
            'completed': True,
            'final_pending': 0,
            'progress': {
                'result': {
                    'roles': {
                        'short-term-decider': {
                            'requested_count': expected // 2,
                            'completed_count': completed // 2,
                            'error_count': 0,
                        },
                        'signal-decider': {
                            'requested_count': expected - (expected // 2),
                            'completed_count': completed - (completed // 2),
                            'error_count': 0,
                        },
                    },
                },
                'summary': {
                    'run_status': 'completed',
                    'run_table_status': 'completed',
                    'cycle_status': 'completed',
                    'expected_feedback_count': expected,
                    'completed_feedback_count': completed,
                    'computed_feedback_count': completed,
                    'terminal_feedback_count': completed,
                    'failed_items': 0,
                    'pending_failure_count': 0,
                    'pending_feedback_count': 0,
                    'buffered_failure_count': 0,
                    'flushed_failure_count': 0,
                    'failure_groups': {},
                    'pending_symbols': 0,
                    'remaining_roles': [],
                    'completed_roles': [
                        'short-term-decider',
                        'signal-decider',
                    ],
                    'is_live': False,
                    'current_role': None,
                    'role_progress': {
                        'short-term-decider': {
                            'status': 'completed',
                            'boundary_status': 'completed',
                            'total_symbols': expected // 2,
                            'processed_symbols': completed // 2,
                            'completed_symbols': completed // 2,
                            'pending_symbols': 0,
                            'retry_pending_symbols': 0,
                            'failed_items': 0,
                        },
                        'signal-decider': {
                            'status': 'completed',
                            'boundary_status': 'completed',
                            'total_symbols': expected - (expected // 2),
                            'processed_symbols': completed - (completed // 2),
                            'completed_symbols': completed - (completed // 2),
                            'pending_symbols': 0,
                            'retry_pending_symbols': 0,
                            'failed_items': 0,
                        },
                    },
                },
            },
        },
    }
    return item


def _legacy_completed_signal_decider_evaluations_stage(
    *,
    market_date: str,
    updated_at: str,
    outcomes_upserted: int = 3_840,
    evaluations_upserted: int = 3_840,
) -> dict[str, dict]:
    item = _completed_official_stage(
        run_id=516,
        stage_id='signal_decider_evaluations',
        market_date=market_date,
        processed=0,
        total=0,
        updated_at=updated_at,
    )
    item['stage']['details'] = {
        'result_summary': {
            'request': {
                'horizons': [1, 3, 5, 10],
            },
            'summary': {
                'status': 'completed',
                'decisions_considered': 960,
                'outcomes_upserted': outcomes_upserted,
                'evaluations_upserted': evaluations_upserted,
                'coverage': {
                    'count': 3_840,
                    'computed_count': 2_284,
                    'pending_maturity_count': 1_550,
                    'insufficient_price_data_count': 6,
                    'not_computed_count': 0,
                },
                'computed_count': 2_284,
                'pending_maturity_count': 1_550,
                'insufficient_price_data_count': 6,
                'not_computed_count': 0,
                'graded_count': 2_284,
                'pending_count': 1_556,
                'warning_count': 0,
                'warnings': [],
            },
        },
    }
    return item


def _legacy_wrapped_signal_decider_evaluations_stage(
    *,
    market_date: str,
    updated_at: str,
) -> dict[str, dict]:
    nested_refresh_summary = {
        'status': 'completed',
        'decisions_considered': 930,
        'outcomes_upserted': 3_720,
        'evaluations_upserted': 3_720,
        'graded_count': 2_843,
        'pending_count': 877,
        'coverage': {
            'count': 3_720,
            'computed_count': 2_843,
            'pending_maturity_count': 869,
            'insufficient_price_data_count': 8,
            'not_computed_count': 0,
        },
        'computed_count': 2_843,
        'pending_maturity_count': 869,
        'insufficient_price_data_count': 8,
        'not_computed_count': 0,
        'warning_count': 0,
        'warnings': [],
    }
    evaluation_request = {
        'market_date_from': '2026-08-31',
        'market_date_to': market_date,
        'horizons': [1, 3, 5, 10],
    }
    calendar_refresh = {
        'request': {
            'market_date_from': '2026-09-09',
            'market_date_to': market_date,
            'horizons': [1, 3, 5, 10],
        },
        'summary': {
            'decision_count': 374,
            'review_rows_upserted': 1_496,
            'primary_horizon_count': 4,
            'evaluation_refresh': deepcopy(nested_refresh_summary),
        },
        'run': {
            'id': 122,
            'status': 'completed',
            'horizons': [1, 3, 5, 10],
            'summary': {
                'evaluation_refresh': deepcopy(nested_refresh_summary),
            },
            'error_message': None,
        },
    }
    item = _completed_official_stage(
        run_id=491,
        stage_id='signal_decider_evaluations',
        market_date=market_date,
        processed=2_843,
        total=2_843,
        updated_at=updated_at,
    )
    item['stage']['details'] = {
        'result_summary': {
            'status': 'completed',
            'triggered': True,
            'warnings': [],
            'request': {
                'as_of_market_date': market_date,
                'horizons': None,
                'refresh_calendar': True,
            },
            'evaluation_refresh': {
                'request': evaluation_request,
                'summary': deepcopy(nested_refresh_summary),
            },
            'calendar_refresh': calendar_refresh,
            'summary': {
                'status': 'completed',
                'computed_count': 2_843,
                'pending_maturity_count': 869,
                'insufficient_price_data_count': 8,
                'not_computed_count': 0,
                'evaluation_refresh': deepcopy(nested_refresh_summary),
                'warning_count': 0,
                'warnings': [],
                'calendar_refresh': deepcopy(calendar_refresh['summary']),
            },
        },
    }
    return item


class EODPipelineStatusServiceTests(unittest.TestCase):
    def test_row_count_membership_diagnostics_detect_equal_count_wrong_symbol(self):
        session = MagicMock()
        price_result = MagicMock()
        price_result.scalars.return_value.all.return_value = [1, 2]
        factor_result = MagicMock()
        factor_result.scalars.return_value.all.return_value = [1, 3]
        missing_detail_result = MagicMock()
        missing_detail_result.all.return_value = [
            SimpleNamespace(symbol='005930', name_ko='삼성전자', market='KOSPI')
        ]
        session.execute.side_effect = [
            price_result,
            factor_result,
            missing_detail_result,
        ]

        diagnostics = _load_row_count_missing_diagnostics_for_model(
            session=session,
            model=DailyMomentumFactorResult,
            market_date=date(2026, 4, 21),
            limit=10,
        )

        self.assertEqual(diagnostics['membership_requested_count'], 2)
        self.assertEqual(diagnostics['membership_present_count'], 1)
        self.assertEqual(diagnostics['membership_extra_count'], 1)
        self.assertEqual(diagnostics['missing_count'], 1)
        self.assertEqual(diagnostics['missing_symbols_sample'], ['005930'])

    def test_equal_count_wrong_symbol_factor_rows_are_partial_and_blocked(self):
        service = EODPipelineStatusService()
        price_stage = {
            'status': 'completed',
            'gate_status': 'ready',
            'blocking_status': 'non_blocking',
            'total': 2,
            'succeeded': 2,
            'details': {},
        }

        stage = service._build_row_count_stage(
            stage_id='momentum_factors',
            market_date='2026-04-21',
            price_stage=price_stage,
            price_rows_persisted=2,
            prior_stages={'price_collection': price_stage},
            aggregate={
                'persisted': 2,
                'membership_evidence_version': 'row_count_scope_gap_v1',
                'membership_authoritative': True,
                'membership_requested_count': 2,
                'membership_present_count': 1,
                'membership_extra_count': 1,
                'missing_count': 1,
                'missing_symbols_sample': ['005930'],
            },
            live_symbols_total=2,
            feedback_run=None,
            scope_symbols=[],
        )

        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertEqual(stage['total'], 2)
        self.assertEqual(stage['succeeded'], 1)
        self.assertEqual(stage['remaining'], 1)
        self.assertEqual(
            stage['details']['gate_reason'],
            'missing_rows_intersect_daily_price_membership',
        )
        self.assertEqual(stage['details']['off_scope_rows_persisted'], 1)

    def test_read_keeps_aggregate_only_price_coverage_unverified(self):
        pykrx_run = {
            'id': 900,
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {'markets': ['KOSPI']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 2,
            technical_snapshot_count_loader=lambda *_: 2,
            action_summary_count_loader=lambda *_: 2,
            row_count_stage_counts_loader=lambda **_: {},
        )

        result = service.read(market_date='2026-04-21')

        price_stage = result['stages']['price_collection']
        self.assertEqual(price_stage['status'], 'completed')
        self.assertEqual(price_stage['gate_status'], 'ready')
        self.assertEqual(price_stage['severity'], 'degraded')
        self.assertEqual(
            price_stage['details']['expected_rows_source'],
            'eod_run_symbol_total_unverified',
        )
        self.assertEqual(
            price_stage['details']['coverage_verification_status'],
            'unverified',
        )
        self.assertFalse(price_stage['details']['exact_universe_verified'])

    def test_read_uses_validated_run_membership_instead_of_self_certifying_price_count(self):
        requested_symbols = ['000660', '005930']
        membership = build_market_membership_snapshot(
            symbols=[
                SimpleNamespace(symbol='000660', market='KOSPI'),
                SimpleNamespace(symbol='005930', market='KOSPI'),
            ],
            markets=['KOSPI'],
        )
        pykrx_run = {
            'id': 901,
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': requested_symbols,
                'universe_snapshot': {
                    'markets': ['KOSPI'],
                    'market_membership': membership,
                },
            },
        }

        def scope_gap_loader(*, stage_id, market_date, symbols):
            assert market_date == '2026-04-21'
            assert symbols == requested_symbols
            if stage_id == 'price_collection':
                return _scope_gap_evidence(
                    requested_count=2,
                    present_count=1,
                    missing_symbols_sample=['005930'],
                )
            return _scope_gap_evidence(
                requested_count=2,
                present_count=2,
            )

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 2,
            technical_snapshot_count_loader=lambda *_: 2,
            action_summary_count_loader=lambda *_: 2,
            row_count_stage_counts_loader=lambda **_: {},
            row_count_scope_gap_loader=scope_gap_loader,
        )

        result = service.read(market_date='2026-04-21')

        price_stage = result['stages']['price_collection']
        self.assertEqual(price_stage['status'], 'partial')
        self.assertEqual(price_stage['succeeded'], 1)
        self.assertEqual(price_stage['failed'], 1)
        self.assertEqual(
            price_stage['details']['expected_rows_source'],
            'validated_eod_run_market_membership',
        )
        self.assertFalse(price_stage['details']['exact_universe_verified'])
        self.assertEqual(
            price_stage['details']['exact_universe_coverage'][
                'missing_symbols_sample'
            ],
            ['005930'],
        )

    def test_read_blocks_complete_factor_count_when_requested_symbol_is_missing(self):
        requested_symbols = ['000660', '005930']
        membership = build_market_membership_snapshot(
            symbols=[
                SimpleNamespace(symbol='000660', market='KOSPI'),
                SimpleNamespace(symbol='005930', market='KOSPI'),
            ],
            markets=['KOSPI'],
        )
        pykrx_run = {
            'id': 902,
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': requested_symbols,
                'universe_snapshot': {
                    'markets': ['KOSPI'],
                    'market_membership': membership,
                },
            },
        }

        def scope_gap_loader(*, stage_id, market_date, symbols):
            assert market_date == '2026-04-21'
            assert symbols == requested_symbols
            if stage_id == 'momentum_factors':
                return _scope_gap_evidence(
                    requested_count=2,
                    present_count=1,
                    missing_symbols_sample=['005930'],
                )
            return _scope_gap_evidence(
                requested_count=2,
                present_count=2,
            )

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 2,
            technical_snapshot_count_loader=lambda *_: 2,
            action_summary_count_loader=lambda *_: 2,
            row_count_stage_counts_loader=lambda **_: (
                _full_feedback_prerequisite_stage_counts(
                    total=2,
                    updated_at='2026-04-21T09:45:00+00:00',
                )
            ),
            row_count_scope_gap_loader=scope_gap_loader,
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2,
                'semantic_ready_count': 2,
            },
        )

        result = service.read(
            market_date='2026-04-21',
            scope_symbols=requested_symbols,
        )

        momentum_stage = result['stages']['momentum_factors']
        self.assertEqual(momentum_stage['status'], 'completed')
        self.assertEqual(momentum_stage['gate_status'], 'blocked')
        self.assertEqual(
            momentum_stage['details']['gate_reason'],
            'missing_rows_intersect_requested_scope',
        )
        self.assertEqual(
            momentum_stage['details']['feedback_scope_gap'][
                'missing_symbols_sample'
            ],
            ['005930'],
        )

    def test_read_rejects_equal_count_wrong_symbol_factor_coverage_and_keeps_official_partial(self):
        requested_symbols = ['000660', '005930']
        membership = build_market_membership_snapshot(
            symbols=[
                SimpleNamespace(symbol='000660', market='KOSPI'),
                SimpleNamespace(symbol='005930', market='KOSPI'),
            ],
            markets=['KOSPI'],
        )
        pykrx_run = {
            'id': 903,
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': requested_symbols,
                'universe_snapshot': {
                    'markets': ['KOSPI'],
                    'market_membership': membership,
                },
            },
        }
        official_item = _incomplete_official_stage(
            run_id=904,
            stage_id='momentum_factors',
            market_date='2026-04-21',
            status='partial',
            updated_at='2026-04-21T10:00:00+00:00',
            succeeded=1,
            failed=1,
        )
        official_run = {
            **official_item['run'],
            'stages': {'momentum_factors': official_item['stage']},
        }

        def scope_gap_loader(*, stage_id, market_date, symbols):
            assert market_date == '2026-04-21'
            assert symbols == requested_symbols
            if stage_id == 'momentum_factors':
                return _scope_gap_evidence(
                    requested_count=2,
                    present_count=1,
                    missing_symbols_sample=['005930'],
                )
            return _scope_gap_evidence(
                requested_count=2,
                present_count=2,
            )

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 2,
            technical_snapshot_count_loader=lambda *_: 2,
            action_summary_count_loader=lambda *_: 2,
            row_count_stage_counts_loader=lambda **_: (
                _full_feedback_prerequisite_stage_counts(
                    total=2,
                    updated_at='2026-04-21T09:45:00+00:00',
                )
            ),
            row_count_scope_gap_loader=scope_gap_loader,
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2,
                'semantic_ready_count': 2,
            },
        )

        result = service.read(market_date='2026-04-21')

        momentum_stage = result['stages']['momentum_factors']
        self.assertEqual(momentum_stage['status'], 'partial')
        self.assertEqual(momentum_stage['source_status'], 'partial')
        self.assertEqual(momentum_stage['gate_status'], 'blocked')
        self.assertEqual(
            momentum_stage['details']['mode'],
            'official_daily_pipeline_run_state',
        )
        self.assertFalse(
            momentum_stage['details']['exact_universe_verified']
        )
        self.assertEqual(
            momentum_stage['details']['exact_universe_coverage'][
                'missing_symbols_sample'
            ],
            ['005930'],
        )
        self.assertEqual(result['status'], 'blocked')

    def test_signal_feature_stage_requires_semantically_usable_rows_for_requested_scope(self):
        self.assertFalse(
            _signal_feature_row_is_semantically_ready(
                signal_count=0,
                feature_payload={'fallback_features': {}},
            )
        )
        self.assertTrue(
            _signal_feature_row_is_semantically_ready(
                signal_count=0,
                feature_payload={
                    'fallback_features': {
                        'price_action': {'close_return_1d_pct': 1.2},
                    },
                },
            )
        )
        service = EODPipelineStatusService(
            row_count_scope_gap_loader=lambda **_: _scope_gap_evidence(
                requested_count=2,
                present_count=1,
                missing_symbols_sample=['005930'],
                semantic_readiness_required=True,
            ),
        )

        stage = service._build_signal_features_stage(
            market_date='2026-04-21',
            aggregate={
                'persisted': 2,
                'semantic_ready_count': 1,
                'semantic_unready_count': 1,
                'direct_signal_rows': 0,
                'fallback_usable_rows': 1,
            },
            expected_total=2,
            scope_symbols=['000660', '005930'],
        )

        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertEqual(stage['details']['semantic_ready_rows'], 1)
        self.assertEqual(stage['details']['semantic_unready_rows'], 1)
        self.assertEqual(
            stage['details']['gate_reason'],
            'missing_rows_intersect_requested_scope',
        )

    def test_factor_partial_blocks_when_missing_row_intersects_requested_scope(self):
        service = EODPipelineStatusService(
            row_count_scope_gap_loader=lambda **kwargs: _scope_gap_evidence(
                requested_count=len(kwargs['symbols']),
                present_count=1,
                missing_symbols_sample=['005930'],
            ),
        )

        diagnostics = service._build_row_count_stage_diagnostics(
            stage_id='quality_factors',
            status='partial',
            market_date='2026-04-21',
            persisted=199,
            expected_total=200,
            blocking_stages=[],
            failed_blocking_stages=[],
            aggregate={'missing_count': 1},
            feedback_run=None,
            scope_symbols=['000660', '005930'],
        )

        self.assertEqual(diagnostics['gate_status'], 'blocked')
        self.assertEqual(diagnostics['blocking_status'], 'blocking')
        self.assertEqual(
            diagnostics['gate_reason'],
            'missing_rows_intersect_requested_scope',
        )
        self.assertEqual(
            diagnostics['feedback_scope_gap']['missing_symbols_sample'],
            ['005930'],
        )

    def test_requested_scope_gate_fails_closed_on_malformed_zero_gap_proof(self):
        service = EODPipelineStatusService(
            row_count_scope_gap_loader=lambda **_: {
                'missing_count': 0,
            },
        )

        stage = service._apply_scope_coverage_gate(
            stage_id='quality_factors',
            market_date='2026-04-21',
            stage={
                'status': 'completed',
                'gate_status': 'ready',
                'severity': 'ready',
                'blocking_status': 'non_blocking',
                'details': {},
            },
            scope_symbols=['005930'],
        )

        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertEqual(stage['blocking_status'], 'blocking')
        self.assertEqual(
            stage['details']['gate_reason'],
            'requested_scope_coverage_evidence_invalid',
        )

    def test_requested_scope_completion_does_not_bypass_dependency_or_terminal_gate(self):
        service = EODPipelineStatusService(
            row_count_scope_gap_loader=lambda **_: _scope_gap_evidence(
                requested_count=1,
                present_count=1,
            ),
        )

        diagnostics = service._build_row_count_stage_diagnostics(
            stage_id='quality_factors',
            status='pending',
            market_date='2026-04-21',
            persisted=1,
            expected_total=1,
            blocking_stages=['fundamentals_daily_refresh'],
            failed_blocking_stages=[],
            aggregate={'missing_count': 0},
            feedback_run=None,
            scope_symbols=['005930'],
        )
        self.assertEqual(diagnostics['gate_status'], 'waiting')
        self.assertEqual(
            diagnostics['gate_reason'],
            'waiting_on_dependency',
        )

        failed_stage = service._apply_scope_coverage_gate(
            stage_id='technical_snapshots',
            market_date='2026-04-21',
            stage={
                'status': 'failed',
                'gate_status': 'blocked',
                'severity': 'failed',
                'blocking_status': 'blocking',
                'details': {'gate_reason': 'official_stage_failed'},
            },
            scope_symbols=['005930'],
        )
        self.assertEqual(failed_stage['status'], 'failed')
        self.assertEqual(failed_stage['gate_status'], 'blocked')
        self.assertEqual(
            failed_stage['details']['gate_reason'],
            'official_stage_failed',
        )

    def test_stored_full_coverage_does_not_override_terminal_source_stage(self):
        service = EODPipelineStatusService(
            row_count_scope_gap_loader=lambda **_: _scope_gap_evidence(
                requested_count=1,
                present_count=1,
            ),
        )
        exact_scope_gap = _scope_gap_evidence(
            requested_count=1,
            present_count=1,
        )

        for status in ('failed', 'blocked', 'partial', 'interrupted'):
            with self.subTest(status=status):
                source_stage = {
                    'status': status,
                    'gate_status': 'blocked',
                    'severity': 'blocked',
                    'blocking_status': 'blocking',
                    'details': {'gate_reason': f'official_stage_{status}'},
                }
                normalized = service._normalize_stored_pykrx_coverage_stage(
                    source_stage,
                    persisted=1,
                    expected_total=1,
                    exact_scope_gap=exact_scope_gap,
                    expected_total_source=(
                        'validated_eod_run_market_membership'
                    ),
                )
                gated = service._apply_scope_coverage_gate(
                    stage_id='technical_snapshots',
                    market_date='2026-04-21',
                    stage=normalized,
                    scope_symbols=['005930'],
                )

                self.assertEqual(gated['status'], status)
                self.assertEqual(gated['gate_status'], 'blocked')
                self.assertEqual(gated['blocking_status'], 'blocking')
                self.assertEqual(
                    gated['details']['gate_reason'],
                    f'official_stage_{status}',
                )

    def test_requested_scope_completion_isolates_out_of_scope_partial_dependency(self):
        service = EODPipelineStatusService(
            row_count_scope_gap_loader=lambda **_: {
                'evidence_version': 'row_count_scope_gap_v1',
                'authoritative': True,
                'requested_count': 1,
                'present_count': 1,
                'missing_count': 0,
                'missing_symbols_sample': [],
            },
        )
        price_stage = service._apply_scope_coverage_gate(
            stage_id='price_collection',
            market_date='2026-04-21',
            stage={
                'status': 'partial',
                'gate_status': 'blocked',
                'severity': 'blocked',
                'blocking_status': 'blocking',
                'details': {
                    'gate_reason': 'missing_rows_intersect_exact_run_universe',
                    'exact_universe_coverage': {
                        'requested_count': 2,
                        'present_count': 1,
                        'missing_count': 1,
                        'missing_symbols_sample': ['005930'],
                    },
                },
            },
            scope_symbols=['000660'],
        )

        self.assertEqual(price_stage['status'], 'partial')
        self.assertEqual(price_stage['gate_status'], 'ready')
        self.assertEqual(price_stage['severity'], 'degraded')
        self.assertEqual(price_stage['blocking_status'], 'non_blocking')
        self.assertEqual(
            price_stage['details']['gate_reason'],
            'requested_scope_rows_complete',
        )
        self.assertTrue(
            service._dependency_is_ready_for_scope(
                stage=price_stage,
                scope_symbols=['000660'],
            )
        )

        momentum_stage = service._build_row_count_stage(
            stage_id='momentum_factors',
            market_date='2026-04-21',
            price_stage=price_stage,
            price_rows_persisted=1,
            prior_stages={'price_collection': price_stage},
            aggregate={'persisted': 1, 'missing_count': 1},
            live_symbols_total=2,
            feedback_run=None,
            scope_symbols=['000660'],
        )
        self.assertEqual(momentum_stage['status'], 'completed')
        self.assertEqual(momentum_stage['gate_status'], 'ready')
        self.assertEqual(momentum_stage['details']['blocking_stages'], [])
        self.assertEqual(
            momentum_stage['details']['gate_reason'],
            'requested_scope_rows_complete',
        )

    def test_requested_scope_never_promotes_non_partial_dependency_states(self):
        service = EODPipelineStatusService()
        for status, gate_status in (
            ('failed', 'blocked'),
            ('running', 'waiting'),
            ('pending', 'waiting'),
            ('blocked', 'blocked'),
            ('waiting', 'waiting'),
        ):
            with self.subTest(status=status):
                self.assertFalse(
                    service._dependency_is_ready_for_scope(
                        stage={
                            'status': status,
                            'gate_status': gate_status,
                            'blocking_status': 'blocking',
                        },
                        scope_symbols=['000660'],
                    )
                )

    def test_partial_dependency_requires_exact_requested_scope_evidence(self):
        service = EODPipelineStatusService()
        partial_ready_without_scope_evidence = {
            'status': 'partial',
            'gate_status': 'ready',
            'blocking_status': 'non_blocking',
            'details': {
                'gate_reason': 'global_coverage_above_minimum',
            },
        }
        self.assertFalse(
            service._dependency_is_ready_for_scope(
                stage=partial_ready_without_scope_evidence,
                scope_symbols=['000660'],
            )
        )

        partial_ready_with_incomplete_scope = {
            **partial_ready_without_scope_evidence,
            'details': {
                'requested_scope_coverage': {
                    'requested_count': 1,
                    'present_count': 0,
                    'missing_count': 1,
                    'missing_symbols_sample': ['000660'],
                },
            },
        }
        self.assertFalse(
            service._dependency_is_ready_for_scope(
                stage=partial_ready_with_incomplete_scope,
                scope_symbols=['000660'],
            )
        )

        authoritative_but_incomplete_row_count_evidence = {
            **partial_ready_without_scope_evidence,
            'details': {
                'requested_scope_coverage': {
                    'authoritative': True,
                    'evidence_version': 'row_count_scope_gap_v1',
                    'requested_count': 2,
                    'present_count': 1,
                    'missing_count': 1,
                    'missing_symbols_sample': ['000660'],
                },
            },
        }
        self.assertFalse(
            service._dependency_is_ready_for_scope(
                stage=(
                    authoritative_but_incomplete_row_count_evidence
                ),
                scope_symbols=['005930', '000660'],
            )
        )

    def test_waiting_partial_dependency_never_promotes_exact_scope_evidence(self):
        service = EODPipelineStatusService()

        self.assertFalse(
            service._dependency_is_ready_for_scope(
                stage={
                    'status': 'partial',
                    'gate_status': '',
                    'blocking_status': 'waiting',
                    'details': {
                        'requested_scope_coverage': {
                            'authoritative': True,
                            'evidence_version': (
                                'dart_fundamentals_scope_outcomes_v1'
                            ),
                            'requested_count': 1,
                            'missing_count': 0,
                        }
                    },
                },
                scope_symbols=['000660'],
            )
        )

    def test_fundamentals_partial_manifest_is_ready_only_for_successful_requested_symbols(self):
        run = {
            'id': 901,
            'market_date': '2026-10-28',
            'status': 'partial',
            'fs_div': 'CFS',
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_fresh': 1,
            'symbols_refreshed': 1,
            'symbols_failed': 1,
            'run_metadata': {
                'scope_outcome_manifest_version': (
                    'dart_fundamentals_scope_outcomes_v1'
                ),
                'selected_stock_codes': ['000660', '005930', '035420'],
                'successful_stock_codes': ['000660', '005930'],
                'retry_debt_stock_codes': ['035420'],
                'scope_outcome_manifest': [
                    {'stock_code': '000660', 'outcome': 'success'},
                    {'stock_code': '005930', 'outcome': 'success'},
                    {'stock_code': '035420', 'outcome': 'retry_debt'},
                ],
            },
        }
        service = EODPipelineStatusService(
            fundamentals_refresh_loader=lambda **_: run,
        )

        successful_scope = service._build_fundamentals_stage(
            market_date='2026-10-28',
            fs_div='CFS',
            scope_symbols=['000660', '005930'],
        )
        retry_debt_scope = service._build_fundamentals_stage(
            market_date='2026-10-28',
            fs_div='CFS',
            scope_symbols=['035420'],
        )

        self.assertEqual(successful_scope['gate_status'], 'ready')
        self.assertEqual(
            successful_scope['details']['requested_scope_coverage'][
                'missing_count'
            ],
            0,
        )
        self.assertTrue(
            service._dependency_is_ready_for_scope(
                stage=successful_scope,
                scope_symbols=['000660', '005930'],
            )
        )
        self.assertEqual(retry_debt_scope['gate_status'], 'blocked')
        self.assertEqual(
            retry_debt_scope['details']['requested_scope_coverage'][
                'missing_symbols_sample'
            ],
            ['035420'],
        )
        self.assertFalse(
            service._dependency_is_ready_for_scope(
                stage=retry_debt_scope,
                scope_symbols=['035420'],
            )
        )

    def test_completed_fundamentals_run_does_not_cover_symbol_outside_manifest(self):
        run = {
            'id': 902,
            'market_date': '2026-10-28',
            'status': 'completed',
            'fs_div': 'CFS',
            'symbols_total': 1,
            'symbols_checked': 1,
            'symbols_refreshed': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'scope_outcome_manifest_version': (
                    'dart_fundamentals_scope_outcomes_v1'
                ),
                'selected_stock_codes': ['005930'],
                'successful_stock_codes': ['005930'],
                'retry_debt_stock_codes': [],
                'scope_outcome_manifest': [
                    {'stock_code': '005930', 'outcome': 'success'},
                ],
            },
        }
        service = EODPipelineStatusService(
            fundamentals_refresh_loader=lambda **_: run,
        )

        stage = service._build_fundamentals_stage(
            market_date='2026-10-28',
            fs_div='CFS',
            scope_symbols=['000660'],
        )

        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertEqual(
            stage['details']['requested_scope_coverage'][
                'missing_symbols_sample'
            ],
            ['000660'],
        )
        self.assertFalse(
            service._dependency_is_ready_for_scope(
                stage=stage,
                scope_symbols=['000660'],
            )
        )

    def test_fundamentals_scope_resolution_uses_latest_attempt_per_symbol(self):
        older_full_run = {
            'id': 903,
            'status': 'completed',
            'run_metadata': {
                'scope_outcome_manifest_version': (
                    'dart_fundamentals_scope_outcomes_v1'
                ),
                'selected_stock_codes': ['000660', '005930'],
                'successful_stock_codes': ['000660', '005930'],
                'retry_debt_stock_codes': [],
                'scope_outcome_manifest': [
                    {'stock_code': '000660', 'outcome': 'success'},
                    {'stock_code': '005930', 'outcome': 'success'},
                ],
            },
        }
        newer_retry_run = {
            'id': 904,
            'market_date': '2026-10-28',
            'status': 'partial',
            'fs_div': 'CFS',
            'symbols_total': 1,
            'symbols_checked': 1,
            'symbols_failed': 1,
            'run_metadata': {
                'scope_outcome_manifest_version': (
                    'dart_fundamentals_scope_outcomes_v1'
                ),
                'selected_stock_codes': ['005930'],
                'successful_stock_codes': [],
                'retry_debt_stock_codes': ['005930'],
                'scope_outcome_manifest': [
                    {'stock_code': '005930', 'outcome': 'retry_debt'},
                ],
            },
            '_scope_evidence_runs': [],
        }
        newer_retry_run['_scope_evidence_runs'] = [
            newer_retry_run,
            older_full_run,
        ]
        service = EODPipelineStatusService(
            fundamentals_refresh_loader=lambda **_: newer_retry_run,
        )

        stage = service._build_fundamentals_stage(
            market_date='2026-10-28',
            fs_div='CFS',
            scope_symbols=['000660', '005930'],
        )
        coverage = stage['details']['requested_scope_coverage']

        self.assertEqual(coverage['present_count'], 1)
        self.assertEqual(coverage['missing_symbols_sample'], ['005930'])
        self.assertEqual(
            coverage['source_run_ids_by_symbol'],
            {'000660': 903, '005930': 904},
        )
        self.assertEqual(stage['gate_status'], 'blocked')

    def test_daily_data_scope_manifest_is_ready_only_for_success_partition(
        self,
    ):
        run = {
            'id': 905,
            'market_date': '2026-10-28',
            'status': 'partial',
            'fs_div': 'CFS',
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_refreshed': 1,
            'symbols_failed': 1,
            'run_metadata': {
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
            },
        }
        service = EODPipelineStatusService(
            fundamentals_refresh_loader=lambda **_: run,
        )

        successful_scope = service._build_fundamentals_stage(
            market_date='2026-10-28',
            fs_div='CFS',
            scope_symbols=['005930'],
        )
        excluded_scope = service._build_fundamentals_stage(
            market_date='2026-10-28',
            fs_div='CFS',
            scope_symbols=['000660', '035420'],
        )

        successful_coverage = successful_scope['details'][
            'requested_scope_coverage'
        ]
        self.assertEqual(
            successful_coverage['evidence_version'],
            'dart_daily_data_scope_outcomes_v1',
        )
        self.assertTrue(successful_coverage['authoritative'])
        self.assertEqual(successful_coverage['missing_count'], 0)
        self.assertEqual(successful_scope['gate_status'], 'ready')
        self.assertTrue(
            service._dependency_is_ready_for_scope(
                stage=successful_scope,
                scope_symbols=['005930'],
            )
        )
        self.assertEqual(excluded_scope['gate_status'], 'blocked')
        self.assertEqual(
            excluded_scope['details']['requested_scope_coverage'][
                'missing_symbols_sample'
            ],
            ['000660', '035420'],
        )

    def test_daily_data_scope_manifest_rejects_malformed_partitions(self):
        valid_metadata = {
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
        malformed_metadata: dict[str, dict] = {}

        overlapping_partition = deepcopy(valid_metadata)
        overlapping_partition['successful_stock_codes'].append('000660')
        malformed_metadata['overlapping_partition'] = overlapping_partition

        duplicate_partition_symbol = deepcopy(valid_metadata)
        duplicate_partition_symbol['selected_stock_codes'].append('005930')
        malformed_metadata['duplicate_partition_symbol'] = (
            duplicate_partition_symbol
        )

        duplicate_manifest_symbol = deepcopy(valid_metadata)
        duplicate_manifest_symbol['scope_outcome_manifest'].append(
            {'stock_code': '005930', 'outcome': 'success'}
        )
        malformed_metadata['duplicate_manifest_symbol'] = (
            duplicate_manifest_symbol
        )

        mismatched_manifest = deepcopy(valid_metadata)
        mismatched_manifest['scope_outcome_manifest'][1]['outcome'] = 'success'
        malformed_metadata['mismatched_manifest'] = mismatched_manifest

        service = EODPipelineStatusService()
        for case_name, run_metadata in malformed_metadata.items():
            with self.subTest(case=case_name):
                coverage = service._build_fundamentals_requested_scope_coverage(
                    run={
                        'id': 906,
                        'status': 'partial',
                        'run_metadata': run_metadata,
                    },
                    scope_symbols=['005930', '000660', '035420'],
                )

                self.assertFalse(coverage['authoritative'])
                self.assertEqual(coverage['present_count'], 0)
                self.assertEqual(coverage['missing_count'], 3)
                self.assertEqual(
                    coverage['missing_symbols_sample'],
                    ['005930', '000660', '035420'],
                )
                self.assertEqual(coverage['source_run_ids_by_symbol'], {})

    def test_pykrx_global_completion_does_not_cover_missing_requested_symbol(self):
        coverage = _full_pykrx_enrichment_coverage(
            symbol_count=1,
            market_count=2,
            updated_at='2026-10-28T16:30:00+09:00',
        )
        service = EODPipelineStatusService()

        stage = service._build_pykrx_enrichment_stage(
            market_date='2026-10-28',
            coverage=coverage,
            scope_symbols=['000660', '005930'],
        )

        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertFalse(
            stage['details']['requested_scope_coverage']['authoritative']
        )
        self.assertEqual(
            stage['details']['requested_scope_coverage']['missing_count'],
            2,
        )
        self.assertFalse(
            service._dependency_is_ready_for_scope(
                stage=stage,
                scope_symbols=['000660', '005930'],
            )
        )

    def test_legacy_optional_unavailable_stage_is_rendered_partial(self) -> None:
        normalized = EODPipelineStatusService._normalize_stage_contract(
            {
                'external_news_enrich': {
                    'task': 'external_news_enrich',
                    'status': 'unavailable',
                    'details': {
                        'result_summary': {
                            'triggered': True,
                            'status': 'unavailable',
                            'reason': 'external_news_storage_unavailable',
                        },
                    },
                },
            }
        )
        stage = normalized['external_news_enrich']

        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['source_status'], 'unavailable')
        self.assertEqual(
            stage['details']['status_canonicalized_from'],
            'unavailable',
        )
        self.assertEqual(
            EODPipelineStatusService._resolve_overall_status(
                {'external_news_enrich': stage}
            ),
            'completed',
        )

    def test_read_reports_completed_with_optional_context_warning_metadata(
        self,
    ) -> None:
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 0,
            fundamentals_refresh_loader=lambda **_: None,
            price_bar_count_loader=lambda **_: 0,
            pykrx_enrichment_loader=lambda **_: {},
        )
        normalized_stages = {
            'technical_snapshots': {
                'task': 'technical_snapshots',
                'stage_id': 'technical_snapshots',
                'label': 'Technical snapshots',
                'status': 'completed',
                'details': {},
            },
            'macro_context_refresh': {
                'task': 'macro_context_refresh',
                'stage_id': 'macro_context_refresh',
                'label': 'Macro context refresh',
                'status': 'partial',
                'details': {
                    'result_summary': {
                        'status': 'partial',
                        'remaining_retryable_events': 0,
                    },
                },
            },
        }

        with patch.object(
            service,
            '_normalize_stage_contract',
            return_value=normalized_stages,
        ):
            result = service.read(
                market_date='2026-04-21',
                include_feedback_context=False,
                include_feedback_stage=False,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            result['stages']['macro_context_refresh']['status'],
            'partial',
        )
        self.assertEqual(result['quality_status'], 'degraded')
        self.assertEqual(
            result['warning_stage_ids'],
            ['macro_context_refresh'],
        )
        self.assertEqual(result['warning_count'], 1)

    def test_required_partial_keeps_pipeline_status_partial(self) -> None:
        stages = {
            'technical_snapshots': {
                'stage_id': 'technical_snapshots',
                'status': 'partial',
                'details': {'remaining_retryable_events': 0},
            },
            'macro_context_refresh': {
                'stage_id': 'macro_context_refresh',
                'status': 'partial',
                'details': {'remaining_retryable_events': 0},
            },
        }

        self.assertEqual(
            EODPipelineStatusService._resolve_overall_status(stages),
            'partial',
        )

    def test_completed_required_stage_with_blocked_gate_keeps_pipeline_blocked(self) -> None:
        stages = {
            'technical_snapshots': {
                'stage_id': 'technical_snapshots',
                'status': 'completed',
                'gate_status': 'blocked',
                'blocking_status': 'blocking',
                'details': {
                    'gate_reason': 'missing_rows_intersect_exact_run_universe',
                    'missing_count': 1,
                },
            },
            'action_summaries': {
                'stage_id': 'action_summaries',
                'status': 'completed',
                'gate_status': 'ready',
                'blocking_status': 'non_blocking',
                'details': {},
            },
        }

        self.assertEqual(
            EODPipelineStatusService._resolve_overall_status(stages),
            'blocked',
        )
        self.assertEqual(
            stages['technical_snapshots']['details']['gate_reason'],
            'missing_rows_intersect_exact_run_universe',
        )

    def test_optional_context_warning_only_resolves_completed(self) -> None:
        stages = {
            'macro_context_refresh': {
                'stage_id': 'macro_context_refresh',
                'status': 'partial',
                'details': {'remaining_retryable_events': 0},
            },
        }

        self.assertEqual(
            EODPipelineStatusService._resolve_overall_status(stages),
            'completed',
        )

    def test_pykrx_stage_counts_not_applicable_categories_as_satisfied(self):
        coverage = _full_pykrx_enrichment_coverage(
            symbol_count=1,
            market_count=1,
            updated_at='2026-09-18T15:40:00+09:00',
        )
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        ):
            coverage['categories'][category] = {
                'scope_type': 'symbol',
                'present_rows': 0,
                'expected_rows': 0,
                'coverage_ratio': None,
                'coverage_status': 'not_applicable',
                'expectation_status': 'not_applicable',
                'latest_captured_at': None,
            }

        stage = EODPipelineStatusService()._build_pykrx_enrichment_stage(
            market_date='2026-09-18',
            coverage=coverage,
        )

        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['gate_status'], 'ready')
        self.assertEqual(stage['details']['missing_categories'], [])
        self.assertEqual(stage['details']['coverage_warning_categories'], [])
        gates = {
            item['category']: item
            for item in stage['details']['decider_category_gates']
        }
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        ):
            with self.subTest(category=category):
                self.assertEqual(
                    gates[category]['gate_status'],
                    'not_applicable',
                )

    def test_pykrx_stage_remains_partial_when_decision_usable_core_is_missing(self):
        coverage = _full_pykrx_enrichment_coverage(
            symbol_count=2767,
            market_count=2,
            updated_at='2026-10-30T15:40:00+09:00',
        )
        coverage['categories']['short_selling'].update(
            {
                'present_rows': 2767,
                'expected_rows': 2767,
                'coverage_ratio': 1.0,
                'coverage_status': 'complete',
                'decision_usable_present_rows': 0,
                'decision_usable_ratio': 0.0,
                'decision_usable_status': 'missing',
                'complete_present_rows': 0,
                'completeness_ratio': 0.0,
                'completeness_status': 'missing',
            }
        )

        stage = EODPipelineStatusService()._build_pykrx_enrichment_stage(
            market_date='2026-10-30',
            coverage=coverage,
        )

        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['source_status'], 'partial')
        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertGreater(stage['failed'], 0)
        self.assertIn(
            'short_selling',
            stage['details']['missing_categories'],
        )
        short_gate = next(
            item
            for item in stage['details']['decider_blocking_categories']
            if item['category'] == 'short_selling'
        )
        self.assertEqual(
            short_gate['reason'],
            'missing_required_pykrx_enrichment_decision_evidence',
        )
        self.assertEqual(short_gate['decision_usable_rows'], 0)

    def test_pykrx_stage_completes_with_warning_when_only_short_balance_is_missing(self):
        coverage = _full_pykrx_enrichment_coverage(
            symbol_count=2767,
            market_count=2,
            updated_at='2026-10-30T15:40:00+09:00',
        )
        coverage['categories']['short_selling'].update(
            {
                'present_rows': 2767,
                'expected_rows': 2767,
                'coverage_ratio': 1.0,
                'coverage_status': 'complete',
                'decision_usable_present_rows': 2767,
                'decision_usable_ratio': 1.0,
                'decision_usable_status': 'complete',
                'complete_present_rows': 0,
                'completeness_ratio': 0.0,
                'completeness_status': 'missing',
            }
        )

        stage = EODPipelineStatusService()._build_pykrx_enrichment_stage(
            market_date='2026-10-30',
            coverage=coverage,
        )

        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(
            stage['source_status'],
            'completed_with_coverage_warnings',
        )
        self.assertEqual(stage['gate_status'], 'ready')
        self.assertEqual(stage['failed'], 0)
        self.assertIn(
            'short_selling',
            stage['details']['coverage_warning_categories'],
        )

    def test_pykrx_partial_stage_counts_not_applicable_categories_as_processed(self):
        coverage = _full_pykrx_enrichment_coverage(
            symbol_count=1,
            market_count=1,
            updated_at='2026-09-18T15:40:00+09:00',
        )
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        ):
            coverage['categories'][category] = {
                'scope_type': 'symbol',
                'present_rows': 0,
                'expected_rows': 0,
                'coverage_ratio': None,
                'coverage_status': 'not_applicable',
                'expectation_status': 'not_applicable',
                'latest_captured_at': None,
            }
        coverage['categories']['valuation_snapshot'] = {
            'scope_type': 'symbol',
            'present_rows': 0,
            'expected_rows': 1,
            'coverage_ratio': 0.0,
            'coverage_status': 'missing',
            'expectation_status': 'available',
            'latest_captured_at': None,
        }

        stage = EODPipelineStatusService()._build_pykrx_enrichment_stage(
            market_date='2026-09-18',
            coverage=coverage,
        )

        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['total'], 7)
        self.assertEqual(stage['succeeded'], 6)
        self.assertEqual(stage['failed'], 1)
        self.assertEqual(stage['processed'], 7)
        self.assertEqual(stage['remaining'], 0)

    def test_official_completed_stage_with_failed_items_is_reported_partial(self):
        market_date = '2026-05-28'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='signal_decider',
            market_date=market_date,
            processed=160,
            total=160,
        )
        official_item['stage']['succeeded'] = 60
        official_item['stage']['failed'] = 100
        official_run = {
            **official_item['run'],
            'stages': {'signal_decider': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['signal_decider']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['source_status'], 'completed')
        self.assertEqual(stage['failed'], 100)
        self.assertEqual(stage['current_activity'], 'Completed with 100 failed items.')

    def test_strict_completion_stages_without_scope_proof_are_downgraded(
        self,
    ) -> None:
        service = EODPipelineStatusService()
        for stage_id in (
            'agent_feedback_summaries',
            'next_open_execution_plan',
        ):
            with self.subTest(stage_id=stage_id):
                item = _completed_official_stage(
                    run_id=386,
                    stage_id=stage_id,
                    market_date='2026-05-28',
                    processed=2,
                    total=2,
                )
                built = service._build_official_stage(
                    stage_id=stage_id,
                    market_date='2026-05-28',
                    official_run=item['run'],
                    official_stage=item['stage'],
                    fallback_stage={
                        'status': 'pending',
                        'details': {},
                        'source_run_ids': {},
                    },
                )

                self.assertEqual(
                    built['status'],
                    (
                        'blocked'
                        if stage_id == 'next_open_execution_plan'
                        else 'partial'
                    ),
                )
                self.assertEqual(
                    built['details']['status_repair_reason'],
                    'non_authoritative_terminal_result',
                )

    def test_next_open_completed_accepts_exact_partial_decider_terminal_scope(
        self,
    ) -> None:
        decider = _completed_signal_decider_history_stage(
            run_id=386,
            market_date='2026-05-28',
            symbol_count=2,
        )['stage']
        decider['status'] = 'partial'
        decider.update(
            processed=6,
            total=6,
            succeeded=4,
            failed=2,
            remaining=0,
        )
        decider_proof = decider['details']['completion_scope_proof']
        decider_proof['admitted_symbols'].append('000003')
        for role in DECISION_SUPPORT_AGENT_NAMES:
            decider_proof['failed_symbols_by_role'][role] = ['000003']
        decider_proof.update(
            total_items=6,
            completed_items=4,
            failed_items=2,
        )
        decider_proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(decider_proof)
        )

        next_open = _completed_next_open_history_stage(
            run_id=386,
            market_date='2026-05-28',
            total=2,
        )['stage']
        next_open_proof = next_open['details'][
            'completion_scope_proof'
        ]
        next_open_proof['parent_admitted_symbols'].append('000003')
        next_open_proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(next_open_proof)
        )

        normalized = EODPipelineStatusService._normalize_stage_contract(
            {
                'signal_decider': decider,
                'next_open_execution_plan': next_open,
            }
        )

        self.assertEqual(
            normalized['next_open_execution_plan']['status'],
            'completed',
        )

        mismatched = deepcopy(next_open)
        mismatched_proof = mismatched['details'][
            'completion_scope_proof'
        ]
        mismatched_proof['parent_completed_symbols'] = [
            '000001',
            '000003',
        ]
        mismatched_proof['role_symbol_scopes'] = {
            role: ['000001', '000003']
            for role in DECISION_SUPPORT_AGENT_NAMES
        }
        for entry in mismatched_proof['decision_partition']:
            if entry['symbol'] == '000002':
                entry['symbol'] = '000003'
        mismatched_proof['decision_partition'].sort(
            key=lambda entry: (
                entry['symbol'],
                entry['decision_agent_name'],
            )
        )
        mismatched_proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(mismatched_proof)
        )

        mismatched_normalized = (
            EODPipelineStatusService._normalize_stage_contract(
                {
                    'signal_decider': decider,
                    'next_open_execution_plan': mismatched,
                }
            )
        )

        self.assertEqual(
            mismatched_normalized['next_open_execution_plan']['status'],
            'blocked',
        )
        self.assertEqual(
            mismatched_normalized['next_open_execution_plan']['details'][
                'status_repair_reason'
            ],
            'signal_decider_completion_proof_unavailable_or_mismatched',
        )

    def test_completed_next_open_plan_is_blocked_when_decider_counts_are_incomplete(self):
        market_date = '2026-05-28'
        decider_item = _completed_official_stage(
            run_id=386,
            stage_id='signal_decider',
            market_date=market_date,
            processed=160,
            total=160,
        )
        decider_item['stage']['succeeded'] = 60
        decider_item['stage']['failed'] = 100
        plan_item = _completed_official_stage(
            run_id=386,
            stage_id='next_open_execution_plan',
            market_date=market_date,
            processed=0,
            total=0,
        )
        official_run = {
            **decider_item['run'],
            'stages': {
                'signal_decider': decider_item['stage'],
                'next_open_execution_plan': plan_item['stage'],
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['next_open_execution_plan']
        self.assertEqual(stage['status'], 'blocked')
        self.assertEqual(stage['source_status'], 'completed')
        self.assertEqual(
            stage['details']['status_repair_reason'],
            'signal_decider_completion_proof_unavailable_or_mismatched',
        )

    def test_official_external_news_results_repair_legacy_zero_counters(self):
        market_date = '2026-05-28'
        collect_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_collect',
            market_date=market_date,
            processed=0,
            total=201,
        )
        collect_item['stage']['succeeded'] = 0
        collect_item['stage']['details'] = {
            'result_summary': {
                'summary': {
                    'eligible_symbol_count': 201,
                    'collected_symbol_count': 201,
                    'items_inserted': 252,
                }
            }
        }
        enrich_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_enrich',
            market_date=market_date,
            processed=0,
            total=0,
        )
        enrich_item['stage']['succeeded'] = 0
        enrich_item['stage']['details'] = {
            'result_summary': {
                'items_inspected': 703,
                'items_updated': 703,
                'items_missing_classification': 0,
                'items_remaining_at_least': 0,
                'missing_item_ids': [],
            }
        }
        official_run = {
            **collect_item['run'],
            'stages': {
                'external_news_collect': collect_item['stage'],
                'external_news_enrich': enrich_item['stage'],
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        collect_stage = result['stages']['external_news_collect']
        self.assertEqual(collect_stage['processed'], 201)
        self.assertEqual(collect_stage['total'], 201)
        self.assertEqual(collect_stage['succeeded'], 201)
        self.assertEqual(collect_stage['failed'], 0)
        enrich_stage = result['stages']['external_news_enrich']
        self.assertEqual(enrich_stage['processed'], 703)
        self.assertEqual(enrich_stage['total'], 703)
        self.assertEqual(enrich_stage['succeeded'], 703)
        self.assertEqual(enrich_stage['failed'], 0)

    def test_official_external_news_enrichment_recovers_legacy_lower_bound_semantics(self):
        market_date = '2026-05-28'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_enrich',
            market_date=market_date,
            processed=703,
            total=704,
        )
        official_item['stage']['succeeded'] = 701
        official_item['stage']['failed'] = 2
        official_item['stage']['remaining'] = 1
        official_item['stage']['details'] = {
            'result_summary': {
                'items_inspected': 703,
                'items_missing_classification': 3,
                'items_remaining_at_least': 1,
                'missing_item_ids': [11, 12],
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'external_news_enrich': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        self.assertEqual(
            result['stages']['external_news_enrich']['details']['count_semantics'],
            {
                'total': 'lower_bound',
                'total_at_least': 704,
                'remaining': 'lower_bound',
                'remaining_at_least': 1,
            },
        )

    def test_official_external_news_explicit_zero_scope_clears_stale_legacy_total(self):
        market_date = '2026-05-28'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_collect',
            market_date=market_date,
            processed=0,
            total=201,
        )
        official_item['stage']['succeeded'] = 0
        official_item['stage']['details'] = {
            'result_summary': {
                'status': 'disabled',
                'summary': {
                    'requested_symbol_count': 201,
                    'eligible_symbol_count': 0,
                    'collected_symbol_count': 0,
                },
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'external_news_collect': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['external_news_collect']
        self.assertEqual(stage['processed'], 0)
        self.assertEqual(stage['total'], 0)
        self.assertEqual(stage['succeeded'], 0)
        self.assertEqual(stage['failed'], 0)
        self.assertEqual(stage['remaining'], 0)

    def test_official_external_news_failure_counters_are_not_overwritten(self):
        market_date = '2026-05-28'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_collect',
            market_date=market_date,
            processed=0,
            total=201,
        )
        official_item['stage']['succeeded'] = 0
        official_item['stage']['failed'] = 1
        official_item['stage']['details'] = {
            'result_summary': {
                'summary': {
                    'eligible_symbol_count': 201,
                    'collected_symbol_count': 201,
                }
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'external_news_collect': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['external_news_collect']
        self.assertEqual(stage['processed'], 0)
        self.assertEqual(stage['succeeded'], 0)
        self.assertEqual(stage['failed'], 1)

    def test_pykrx_enrichment_diagnostics_do_not_change_official_stage_status(self):
        market_date = '2026-05-28'
        official_item = _incomplete_official_stage(
            run_id=386,
            stage_id='pykrx_enrichment',
            market_date=market_date,
            status='partial',
            updated_at='2026-05-28T08:45:00+00:00',
            succeeded=2,
            failed=5,
        )
        official_run = {
            **official_item['run'],
            'stages': {'pykrx_enrichment': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=10,
                market_count=2,
                updated_at='2026-05-28T08:40:00+00:00',
            ),
        )

        without_diagnostics = service.read(
            market_date=market_date,
            include_pykrx_enrichment_coverage=False,
        )
        with_diagnostics = service.read(
            market_date=market_date,
            include_pykrx_enrichment_coverage=True,
        )

        self.assertEqual(
            without_diagnostics['stages']['pykrx_enrichment']['status'],
            'partial',
        )
        self.assertEqual(
            with_diagnostics['stages']['pykrx_enrichment']['status'],
            'partial',
        )
        self.assertEqual(
            with_diagnostics['stages']['pykrx_enrichment']['gate_status'],
            'ready',
        )
        self.assertTrue(
            with_diagnostics['stages']['pykrx_enrichment']['details']['categories']
        )
        self.assertEqual(
            with_diagnostics['stages']['pykrx_enrichment']['details'][
                'coverage_diagnostics_source'
            ],
            'stage_specific_fallback',
        )
        self.assertEqual(
            with_diagnostics['stages']['pykrx_enrichment']['details'][
                'coverage_diagnostics_updated_at'
            ],
            '2026-05-28T08:40:00+00:00',
        )

    def test_pykrx_live_coverage_overrides_stale_official_gate_details(self):
        market_date = '2026-05-28'
        coverage = _full_pykrx_enrichment_coverage(
            symbol_count=100,
            market_count=2,
            updated_at='2026-05-28T08:40:00+00:00',
        )
        coverage['categories']['valuation_snapshot'].update(
            {
                'present_rows': 96,
                'expected_rows': 100,
                'coverage_ratio': 0.96,
                'coverage_status': 'partial',
            }
        )
        official_item = _completed_official_stage(
            run_id=387,
            stage_id='pykrx_enrichment',
            market_date=market_date,
            processed=7,
            total=7,
            updated_at='2026-05-28T08:45:00+00:00',
        )
        official_item['stage']['details'] = {
            'requested_symbol_count': 1,
            'categories': {
                'valuation_snapshot': {
                    'present_rows': 0,
                    'expected_rows': 1,
                    'coverage_status': 'missing',
                }
            },
            'coverage_warning_categories': [],
            'decider_category_gate_status': 'waiting',
            'decider_waiting_categories': [{'category': 'valuation_snapshot'}],
            'decider_warning_categories': [],
        }
        official_run = {
            **official_item['run'],
            'stages': {'pykrx_enrichment': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 100,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            pykrx_enrichment_loader=lambda **_: coverage,
        )

        result = service.read(
            market_date=market_date,
            include_pykrx_enrichment_coverage=True,
        )

        stage = result['stages']['pykrx_enrichment']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['gate_status'], 'ready')
        self.assertEqual(stage['details']['requested_symbol_count'], 100)
        self.assertEqual(
            stage['details']['categories']['valuation_snapshot']['present_rows'],
            96,
        )
        self.assertEqual(stage['details']['decider_category_gate_status'], 'ready')
        self.assertEqual(stage['details']['decider_waiting_categories'], [])
        self.assertEqual(
            [item['category'] for item in stage['details']['decider_warning_categories']],
            ['valuation_snapshot'],
        )
        self.assertIn('pykrx_enrichment', result['gate_summary']['ready_stage_ids'])
        self.assertNotIn(
            'pykrx_enrichment',
            result['gate_summary']['waiting_stage_ids'],
        )

    def test_load_daily_pipeline_stage_history_filters_stage_market_date_for_history_index(self):
        class _CapturedResult:
            def all(self):
                return []

        class _CapturedSession:
            statement = None

            def execute(self, statement):
                self.statement = statement
                return _CapturedResult()

        captured_session = _CapturedSession()

        @contextmanager
        def _session_scope():
            yield captured_session

        with patch('app.eod.pipeline_status.session_scope', _session_scope):
            result = _load_daily_pipeline_stage_history(market_date='2026-05-28')

        self.assertEqual(result, {})
        compiled_sql = str(captured_session.statement.compile(compile_kwargs={'literal_binds': True}))
        where_sql = compiled_sql.split('\nWHERE ', 1)[1].split(' ORDER BY ', 1)[0]
        self.assertIn('daily_pipeline_runs.market_date =', where_sql)
        self.assertIn('daily_pipeline_stage_runs.market_date =', where_sql)
        self.assertIn('daily_pipeline_stage_runs.stage_id IN', where_sql)

    def test_load_latest_pipeline_pykrx_run_is_read_only_for_stale_looking_daily_run(self):
        historical_run = {
            'id': 900,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_backfill_api',
            'status': 'running',
            'symbols_total': 100,
            'symbols_succeeded': 20,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T10:00:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'historical_backfill',
            },
        }
        daily_running_run = {
            'id': 901,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'running',
            'symbols_total': 3,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:00:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'stage': 'technical_snapshots',
            },
        }
        with (
            patch(
                'app.eod.pipeline_status._load_pipeline_pykrx_run_rows',
                return_value=[historical_run, daily_running_run],
            ),
            patch(
                'app.eod.pykrx_backfill.PYKRX_BACKFILL_SERVICE.'
                'reconcile_latest_daily_run_status',
            ) as reconcile_run,
        ):
            result = _load_latest_pipeline_pykrx_run(trade_date='2026-04-21')

        self.assertEqual(result['id'], daily_running_run['id'])
        self.assertEqual(result['status'], 'running')
        reconcile_run.assert_not_called()

    def test_load_pipeline_pykrx_run_rows_filters_historical_runs_in_sql_before_limit(self):
        daily_run = {'id': 901, 'run_metadata': {'mode': 'daily_auto'}}

        with patch(
            'app.eod.pipeline_status.list_eod_collection_runs',
            return_value=[daily_run],
        ) as list_runs:
            result = _load_pipeline_pykrx_run_rows(trade_date='2026-04-21')

        self.assertEqual(result, [daily_run])
        list_runs.assert_called_once_with(
            market='KRX',
            trade_date='2026-04-21',
            source='pykrx',
            excluded_run_metadata_modes=('historical_backfill',),
            limit=10,
        )

    def test_load_pipeline_pykrx_run_rows_falls_back_to_latest_historical_run(self):
        historical_run = {
            'id': 900,
            'run_metadata': {'mode': 'historical_backfill'},
        }

        with patch(
            'app.eod.pipeline_status.list_eod_collection_runs',
            side_effect=[[], [historical_run]],
        ) as list_runs:
            result = _load_pipeline_pykrx_run_rows(trade_date='2026-04-21')

        self.assertEqual(result, [historical_run])
        self.assertEqual(
            list_runs.call_args_list,
            [
                call(
                    market='KRX',
                    trade_date='2026-04-21',
                    source='pykrx',
                    excluded_run_metadata_modes=('historical_backfill',),
                    limit=10,
                ),
                call(
                    market='KRX',
                    trade_date='2026-04-21',
                    source='pykrx',
                    limit=1,
                ),
            ],
        )

    def test_read_returns_pending_when_no_runs_exist(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            pykrx_enrichment_loader=lambda **_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['market_date'], '2026-04-21')
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['total_stages'], 26)
        self.assertEqual(result['completed_stages'], 0)
        self.assertEqual(result['pending_stages'], 26)
        self.assertEqual(result['current_stage'], 'master_refresh')
        self.assertEqual(result['stages']['price_collection']['status'], 'pending')
        self.assertEqual(result['stages']['price_collection']['order'], 3)
        self.assertEqual(result['stages']['price_collection']['group'], 'market_data')
        self.assertEqual(result['stages']['price_collection']['inspect_routes'], ['/eod/pipeline/status', '/eod/pykrx/daily/run-if-due'])
        self.assertEqual(result['stages']['price_collection']['total'], 3579)
        self.assertEqual(result['stages']['trending_ranks']['total'], 3)
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'pending')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'pending')
        self.assertEqual(result['stages']['composite_factors']['status'], 'pending')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['details']['fs_div'], 'CFS')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['details']['inspect_routes'],
            ['/disclosures/dart/fundamentals/daily/inspect', '/eod/pipeline/status'],
        )

    def test_read_keeps_current_trading_date_pending_before_official_run(self):
        official_run_dates = []

        def _load_official_run(*, market_date):
            official_run_dates.append(market_date)
            return None

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            pykrx_enrichment_loader=lambda **_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=_load_official_run,
            price_bar_count_loader=lambda **_: 1,
        )

        result = service.read(market_date=_FakeMarketState.current_calendar_date)

        self.assertEqual(official_run_dates, [_FakeMarketState.current_calendar_date])
        self.assertIsNone(result['official_run'])
        self.assertEqual(result['market_date'], _FakeMarketState.current_calendar_date)
        self.assertEqual(result['completed_stages'], 1)
        self.assertGreater(result['pending_stages'], 0)
        self.assertEqual(result['status'], 'pending')

    def test_read_without_market_date_still_selects_yesterday_official_run(self):
        latest_run = {
            'id': 487,
            'market_date': '2026-04-20',
            'trigger': 'scheduled',
            'status': 'partial',
            'current_stage': 'price_collection',
            'started_at': '2026-04-20T11:10:00+00:00',
            'updated_at': '2026-04-20T11:20:00+00:00',
            'completed_at': '2026-04-20T11:20:00+00:00',
            'error_message': None,
            'run_metadata': {},
            'stages': {},
        }

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda *, market_date: (
                latest_run if market_date is None else None
            ),
        )

        result = service.read()

        self.assertEqual(result['market_date'], '2026-04-20')
        self.assertEqual(result['official_run']['id'], 487)

    def test_read_keeps_historical_partial_evidence_partial_without_official_run(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            pykrx_enrichment_loader=lambda **_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            price_bar_count_loader=lambda **_: 1,
        )

        result = service.read(market_date='2026-04-20')

        self.assertIsNone(result['official_run'])
        self.assertEqual(result['completed_stages'], 1)
        self.assertGreater(result['pending_stages'], 0)
        self.assertEqual(result['status'], 'partial')

    def test_read_prefers_official_running_market_risk_filter_before_output_rows_exist(self):
        official_run = {
            'id': 501,
            'market_date': '2026-04-21',
            'trigger': 'internal_ops_cli_stage',
            'status': 'running',
            'current_stage': 'market_risk_filter',
            'started_at': '2026-04-21T09:10:00+00:00',
            'updated_at': '2026-04-21T09:11:00+00:00',
            'completed_at': None,
            'error_message': None,
            'run_metadata': {'resume_from_stage': 'market_risk_filter'},
            'stages': {
                'market_risk_filter': {
                    'stage_id': 'market_risk_filter',
                    'status': 'running',
                    'market_date': '2026-04-21',
                    'started_at': '2026-04-21T09:10:00+00:00',
                    'updated_at': '2026-04-21T09:11:00+00:00',
                    'completed_at': None,
                    'processed': 7,
                    'total': 20,
                    'succeeded': 7,
                    'failed': 0,
                    'remaining': 13,
                    'current_step': 'fetching_kis_market_risk_snapshots',
                    'current_activity': 'Fetching KIS market-risk snapshot for 005930',
                    'current_symbol': '005930',
                    'current_period': None,
                    'source_run_ids': {},
                    'details': {},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 20,
            fundamentals_refresh_loader=lambda **_: None,
            row_count_stage_counts_loader=lambda **_: {'market_risk_filter': {'persisted': 0}},
            official_run_loader=lambda **_: official_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['total_stages'], 26)
        self.assertEqual(result['running_stages'], 1)
        self.assertEqual(result['current_stage'], 'market_risk_filter')
        self.assertEqual(result['official_run']['id'], 501)
        stage = result['stages']['market_risk_filter']
        self.assertEqual(stage['status'], 'running')
        self.assertEqual(stage['order'], 8)
        self.assertEqual(stage['group'], 'market_data')
        self.assertEqual(stage['details']['progress_source'], 'official_run')
        self.assertEqual(stage['processed'], 7)
        self.assertEqual(stage['total'], 20)
        self.assertEqual(stage['current_symbol'], '005930')
        self.assertEqual(stage['details']['mode'], 'official_daily_pipeline_run_state')

    def test_terminal_parent_run_does_not_report_orphan_stage_as_running(self):
        official_item = _completed_official_stage(
            run_id=503,
            stage_id='market_risk_filter',
            market_date='2026-04-21',
            processed=7,
            total=20,
            updated_at='2026-04-21T09:30:00+00:00',
        )
        official_item['stage'].update(
            {
                'status': 'running',
                'completed_at': None,
                'remaining': 13,
                'current_step': 'fetching_kis_market_risk_snapshots',
                'current_activity': 'Fetching KIS market-risk snapshot for 005930',
            }
        )
        official_run = {
            **official_item['run'],
            'status': 'completed',
            'current_stage': 'market_risk_filter',
            'stages': {'market_risk_filter': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 20,
            fundamentals_refresh_loader=lambda **_: None,
            row_count_stage_counts_loader=lambda **_: {
                'market_risk_filter': {'persisted': 0}
            },
            official_run_loader=lambda **_: official_run,
        )

        result = service.read(market_date='2026-04-21')

        stage = result['stages']['market_risk_filter']
        self.assertEqual(stage['status'], 'interrupted')
        self.assertEqual(stage['source_status'], 'running')
        self.assertEqual(result['running_stages'], 0)
        self.assertEqual(result['stage_status_counts']['interrupted'], 1)
        self.assertTrue(stage['details']['orphaned_running_stage'])
        self.assertEqual(
            stage['details']['orphan_reason'],
            'terminal_parent_with_running_stage',
        )
        self.assertEqual(stage['details']['parent_run_status'], 'completed')

    def test_read_surfaces_interrupted_official_run_as_resumable_stage_state(self):
        official_run = {
            'id': 502,
            'market_date': '2026-04-21',
            'trigger': 'manual_api',
            'status': 'interrupted',
            'current_stage': 'signal_decider',
            'started_at': '2026-04-21T09:10:00+00:00',
            'updated_at': '2026-04-21T09:30:00+00:00',
            'completed_at': '2026-04-21T09:30:00+00:00',
            'error_message': 'Core process started while previous daily pipeline run was still marked running.',
            'run_metadata': {'resumable': True},
            'stages': {
                'signal_decider': {
                    'stage_id': 'signal_decider',
                    'status': 'interrupted',
                    'market_date': '2026-04-21',
                    'started_at': '2026-04-21T09:20:00+00:00',
                    'updated_at': '2026-04-21T09:30:00+00:00',
                    'completed_at': '2026-04-21T09:30:00+00:00',
                    'processed': 3,
                    'total': 10,
                    'succeeded': 3,
                    'failed': 0,
                    'remaining': 7,
                    'current_step': 'core_restart_interrupted',
                    'current_activity': 'Core process started while previous daily pipeline run was still marked running.',
                    'current_symbol': None,
                    'current_period': None,
                    'error_message': 'Core process started while previous daily pipeline run was still marked running.',
                    'source_run_ids': {},
                    'details': {'resumable': True},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['current_stage'], 'signal_decider')
        self.assertEqual(result['current_stage_status'], 'interrupted')
        self.assertEqual(result['stage_status_counts']['interrupted'], 1)
        self.assertEqual(result['stages']['signal_decider']['status'], 'interrupted')
        self.assertTrue(result['stages']['signal_decider']['details']['resumable'])
        self.assertIn('previous daily pipeline run', result['stages']['signal_decider']['current_activity'])

    def test_read_selects_abandoned_official_stage_before_pending_stage(self):
        official_run = {
            'id': 504,
            'market_date': '2026-04-21',
            'trigger': 'manual_api',
            'status': 'abandoned',
            'current_stage': 'signal_decider',
            'started_at': '2026-04-21T09:10:00+00:00',
            'updated_at': '2026-04-21T09:30:00+00:00',
            'completed_at': '2026-04-21T09:30:00+00:00',
            'error_message': 'Previous daily pipeline run was abandoned after restart.',
            'run_metadata': {'resumable': True},
            'stages': {
                'signal_decider': {
                    'stage_id': 'signal_decider',
                    'status': 'abandoned',
                    'market_date': '2026-04-21',
                    'started_at': '2026-04-21T09:20:00+00:00',
                    'updated_at': '2026-04-21T09:30:00+00:00',
                    'completed_at': '2026-04-21T09:30:00+00:00',
                    'processed': 3,
                    'total': 10,
                    'succeeded': 3,
                    'failed': 0,
                    'remaining': 7,
                    'current_step': 'core_restart_abandoned',
                    'current_activity': 'Previous daily pipeline run was abandoned after restart.',
                    'current_symbol': None,
                    'current_period': None,
                    'error_message': 'Previous daily pipeline run was abandoned after restart.',
                    'source_run_ids': {},
                    'details': {'resumable': True},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'abandoned')
        self.assertEqual(result['current_stage'], 'signal_decider')
        self.assertEqual(result['current_stage_status'], 'abandoned')
        self.assertEqual(result['stage_status_counts']['abandoned'], 1)
        self.assertEqual(result['stages']['signal_decider']['status'], 'abandoned')
        self.assertTrue(result['stages']['signal_decider']['details']['resumable'])

    def test_read_shows_downstream_signal_decider_running_with_zero_output_rows(self):
        official_run = {
            'id': 503,
            'market_date': '2026-04-21',
            'trigger': 'manual_api',
            'status': 'running',
            'current_stage': 'signal_decider',
            'started_at': '2026-04-21T09:10:00+00:00',
            'updated_at': '2026-04-21T09:20:00+00:00',
            'completed_at': None,
            'error_message': None,
            'run_metadata': {'requested_stages': ['signal_decider']},
            'stages': {
                'signal_decider': {
                    'stage_id': 'signal_decider',
                    'status': 'running',
                    'market_date': '2026-04-21',
                    'started_at': '2026-04-21T09:20:00+00:00',
                    'updated_at': '2026-04-21T09:20:00+00:00',
                    'completed_at': None,
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'current_step': 'signal_decider_started',
                    'current_activity': 'Running decision support decider dispatch',
                    'current_symbol': None,
                    'current_period': None,
                    'source_run_ids': {},
                    'details': {},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['stages']['signal_decider']['status'], 'running')
        self.assertEqual(result['stages']['signal_decider']['processed'], 0)
        self.assertEqual(
            result['stages']['signal_decider']['current_activity'],
            'Running decision support decider dispatch',
        )

    def test_read_summarizes_normalized_warning_followup_official_stage_as_partial(self):
        market_date = '2026-06-05'
        official_run = {
            'id': 505,
            'market_date': market_date,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'current_stage': 'signal_decider_evaluations',
            'started_at': '2026-06-05T06:41:47+00:00',
            'updated_at': '2026-06-05T07:08:00+00:00',
            'completed_at': '2026-06-05T07:08:00+00:00',
            'error_message': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'stage_statuses': {'signal_decider_evaluations': 'partial'},
            },
            'stages': {
                'signal_decider_evaluations': {
                    'stage_id': 'signal_decider_evaluations',
                    'status': 'partial',
                    'market_date': market_date,
                    'started_at': '2026-06-05T07:05:30+00:00',
                    'updated_at': '2026-06-05T07:08:00+00:00',
                    'completed_at': '2026-06-05T07:08:00+00:00',
                    'processed': 228,
                    'total': 228,
                    'succeeded': 227,
                    'failed': 1,
                    'remaining': 0,
                    'current_step': 'signal_decider_evaluations_partial',
                    'current_activity': 'Completed with errors signal decider evaluations',
                    'current_symbol': None,
                    'current_period': None,
                    'source_run_ids': {'eod_run_id': 311},
                    'details': {
                        'mode': 'auto_eod_pipeline_tracking',
                        'progress_source': 'auto_eod_stage_progress',
                        'result_summary': {
                            'status': 'warning',
                            'summary': {'not_computed_count': 1},
                        },
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 228,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read(market_date=market_date)

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['current_stage'], 'signal_decider_evaluations')
        self.assertEqual(result['current_stage_status'], 'partial')
        self.assertEqual(result['partial_stages'], 1)
        self.assertNotIn('warning', result['stage_status_counts'])
        stage = result['stages']['signal_decider_evaluations']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['completed_at'], '2026-06-05T07:08:00+00:00')
        self.assertEqual(stage['details']['progress_source'], 'auto_eod_stage_progress')
        self.assertEqual(
            result['official_run']['run_metadata']['stage_statuses'],
            {'signal_decider_evaluations': 'partial'},
        )

    def test_read_uses_break_day_calendar_date_when_no_official_run_exists(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeBreakDayMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['market_date'], '2026-05-05')
        self.assertEqual(
            result['market'],
            {
                'market': 'KRX',
                'state': 'HOLIDAY',
                'is_trading_day': False,
                'is_open': False,
                'reason': 'holiday',
                'server_time': '2026-05-05T12:15:00+09:00',
                'current_calendar_date': '2026-05-05',
                'latest_valid_trading_date': '2026-05-04',
                'market_session': 'holiday',
            },
        )
        self.assertEqual(result['market_session']['status'], 'holiday')
        self.assertEqual(result['market_session']['latest_valid_trading_date'], '2026-05-04')
        self.assertFalse(result['market_session']['intraday_data_expected'])
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['stages']['price_collection']['market_date'], '2026-05-05')
        self.assertEqual(result['stages']['price_collection']['status'], 'pending')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['market_date'], '2026-05-05')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'pending')

    def test_read_without_market_date_uses_latest_persisted_run_date(self):
        latest_run = {
            'id': 386,
            'market_date': '2026-05-04',
            'trigger': 'manual_daily_api',
            'status': 'partial',
            'current_stage': 'price_collection',
            'started_at': '2026-05-05T09:00:00+00:00',
            'updated_at': '2026-05-05T10:00:00+00:00',
            'completed_at': '2026-05-05T10:00:00+00:00',
            'error_message': None,
            'run_metadata': {},
            'stages': {},
        }
        official_run_dates = []
        pykrx_dates = []

        def _load_official_run(*, market_date):
            official_run_dates.append(market_date)
            return latest_run if market_date is None else None

        def _load_pykrx_run(*, trade_date):
            pykrx_dates.append(trade_date)
            return None

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeBreakDayMarketState(),
            pykrx_run_loader=_load_pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=_load_official_run,
        )

        result = service.read()

        self.assertEqual(official_run_dates, [None])
        self.assertEqual(pykrx_dates, ['2026-05-04'])
        self.assertEqual(result['market_date'], '2026-05-04')
        self.assertEqual(result['official_run']['id'], 386)

    def test_official_stage_result_summary_is_compacted_on_read(self):
        market_date = '2026-04-21'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='signal_decider_evaluations',
            market_date=market_date,
            processed=200,
            total=200,
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'status': 'completed',
                'summary': {'evaluations_upserted': 200},
                'evaluation_refresh': {
                    'summary': {'evaluations_upserted': 200},
                    'items': [{'symbol': f'{index:06d}'} for index in range(200)],
                },
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'signal_decider_evaluations': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read(market_date=market_date)

        details = result['stages']['signal_decider_evaluations']['details']
        self.assertEqual(
            details['result_summary']['evaluation_refresh'],
            {'summary': {'evaluations_upserted': 200}},
        )
        self.assertEqual(
            details['result_summary_omissions']['evaluation_refresh.items'],
            {'type': 'list', 'item_count': 200},
        )

    def test_full_factor_row_count_does_not_hide_zero_ready_content(self):
        market_date = '2026-09-23'
        official_item = _completed_official_stage(
            run_id=475,
            stage_id='low_risk_factors',
            market_date=market_date,
            processed=2_678,
            total=2_678,
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'low_risk_factors': official_item,
            },
            price_bar_count_loader=lambda **_: 2_678,
            row_count_stage_counts_loader=lambda **_: {
                'low_risk_factors': {
                    'persisted': 2_678,
                    'updated_at': '2026-09-23T12:00:00+00:00',
                },
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['low_risk_factors']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertEqual(
            stage['details']['gate_reason'],
            'no_decision_ready_rows',
        )

    def test_fresher_low_risk_rows_replace_stale_zero_ready_history(self):
        market_date = '2026-09-23'
        official_item = _completed_official_stage(
            run_id=475,
            stage_id='low_risk_factors',
            market_date=market_date,
            processed=2_678,
            total=2_678,
            updated_at='2026-09-24T00:00:00+00:00',
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'low_risk_factors': official_item,
            },
            price_bar_count_loader=lambda **_: 2_678,
            row_count_stage_counts_loader=lambda **_: {
                'low_risk_factors': {
                    'persisted': 2_678,
                    'updated_at': '2026-09-27T22:54:27+00:00',
                    'decision_ready_count': 1_587,
                    'insufficient_history_count': 1_091,
                    'latest_decision_ready_at': '2026-09-27T22:54:27+00:00',
                },
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['low_risk_factors']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['gate_status'], 'ready')
        self.assertEqual(stage['details']['decision_ready_count'], 1_587)
        self.assertEqual(stage['details']['insufficient_history_count'], 1_091)

    def test_newer_unready_row_does_not_make_older_ready_rows_fresh(self):
        market_date = '2026-09-23'
        official_item = _completed_official_stage(
            run_id=475,
            stage_id='low_risk_factors',
            market_date=market_date,
            processed=2_678,
            total=2_678,
            updated_at='2026-09-24T00:00:00+00:00',
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'low_risk_factors': official_item,
            },
            price_bar_count_loader=lambda **_: 2_678,
            row_count_stage_counts_loader=lambda **_: {
                'low_risk_factors': {
                    'persisted': 2_678,
                    'updated_at': '2026-09-27T22:54:27+00:00',
                    'decision_ready_count': 1_587,
                    'insufficient_history_count': 1_091,
                    'latest_decision_ready_at': '2026-09-23T22:54:27+00:00',
                },
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['low_risk_factors']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertEqual(stage['details']['gate_reason'], 'no_decision_ready_rows')

    def test_fresher_ready_rows_replace_zero_ready_current_official_run(self):
        market_date = '2026-09-23'
        official_item = _completed_official_stage(
            run_id=475,
            stage_id='low_risk_factors',
            market_date=market_date,
            processed=2_678,
            total=2_678,
            updated_at='2026-09-24T00:00:00+00:00',
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'low_risk_factors': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 2_678,
            row_count_stage_counts_loader=lambda **_: {
                'low_risk_factors': {
                    'persisted': 2_678,
                    'updated_at': '2026-09-27T22:54:27+00:00',
                    'decision_ready_count': 1_587,
                    'insufficient_history_count': 1_091,
                    'latest_decision_ready_at': '2026-09-27T22:54:27+00:00',
                },
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['low_risk_factors']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['gate_status'], 'ready')
        self.assertEqual(stage['details']['decision_ready_count'], 1_587)

    def test_completed_zero_scope_history_is_blocked_by_incomplete_context(self):
        market_date = '2026-09-22'
        signal_decider_item = _completed_official_stage(
            run_id=467,
            stage_id='signal_decider',
            market_date=market_date,
            processed=1,
            total=1,
        )
        official_item = _completed_official_stage(
            run_id=467,
            stage_id='next_open_execution_plan',
            market_date=market_date,
            processed=0,
            total=0,
        )
        official_item['stage']['details'] = {
            'result_summary': {
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
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'signal_decider': signal_decider_item,
                'next_open_execution_plan': official_item,
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['next_open_execution_plan']
        self.assertEqual(stage['status'], 'blocked')
        self.assertEqual(
            stage['details']['status_repair_reason'],
            'incomplete_result_summary',
        )

    def test_official_feedback_stage_repairs_counts_and_activity_from_role_progress(self):
        market_date = '2026-04-21'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='agent_feedback_summaries',
            market_date=market_date,
            processed=239,
            total=239,
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'status': 'completed',
                'progress': {
                    'summary': {
                        'role_progress': {
                            'eod-summarizer': {
                                'status': 'completed',
                                'total_symbols': 80,
                                'processed_symbols': 80,
                                'completed_symbols': 80,
                                'pending_symbols': 0,
                                'failed_items': 0,
                            },
                            'technical-summarizer': {
                                'status': 'completed',
                                'total_symbols': 80,
                                'processed_symbols': 80,
                                'completed_symbols': 80,
                                'pending_symbols': 0,
                                'failed_items': 0,
                            },
                            'fundamental-summarizer': {
                                'status': 'partial',
                                'total_symbols': 80,
                                'processed_symbols': 79,
                                'completed_symbols': 79,
                                'pending_symbols': 1,
                                'failed_items': 0,
                            },
                        }
                    }
                },
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'agent_feedback_summaries': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['agent_feedback_summaries']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['processed'], 239)
        self.assertEqual(stage['total'], 240)
        self.assertEqual(stage['succeeded'], 239)
        self.assertEqual(stage['failed'], 0)
        self.assertEqual(stage['remaining'], 1)
        self.assertEqual(stage['current_activity'], 'Incomplete with 1 item remaining.')

    def test_read_reports_live_stage_progress_for_pykrx_and_fundamentals(self):
        pykrx_run = {
            'id': 101,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'running',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'requested_start_date': '20260421',
                'requested_end_date': '20260421',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
                'stage': 'technical_snapshots',
                'current_step': 'computing_symbol_snapshot',
                'current_activity': 'Computing daily technical snapshots for 000660',
                'current_symbol': '000660',
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'started_at': '2026-04-21T09:10:00+00:00',
                        'updated_at': '2026-04-21T09:20:00+00:00',
                        'completed_at': '2026-04-21T09:20:00+00:00',
                        'processed': 3,
                        'total': 3,
                        'succeeded': 3,
                        'failed': 0,
                        'bars_upserted': 3,
                    },
                    'technical_snapshots': {
                        'status': 'running',
                        'started_at': '2026-04-21T09:20:10+00:00',
                        'updated_at': '2026-04-21T09:20:30+00:00',
                        'processed': 1,
                        'total': 3,
                        'succeeded': 1,
                        'failed': 0,
                        'current_symbol': '000660',
                        'current_step': 'computing_symbol_snapshot',
                        'current_activity': 'Computing daily technical snapshots for 000660',
                    },
                },
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 1, 'updated_at': '2026-04-21T09:20:30+00:00'},
            'action_summaries': {'persisted': 0, 'updated_at': None},
            'trending_ranks': {'persisted': 0, 'periods_persisted': 0, 'updated_at': None},
        }
        fundamentals_run = {
            'id': 55,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'running',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3579,
            'symbols_checked': 120,
            'symbols_missing': 10,
            'symbols_stale': 35,
            'symbols_fresh': 60,
            'symbols_refreshed': 15,
            'symbols_failed': 2,
            'events_detected': 440,
            'events_captured': 37,
            'events_failed': 1,
            'error_message': None,
            'started_at': '2026-04-21T09:21:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'symbols_selected': 200,
                'stage': 'capturing_fundamentals',
                'capture_event_count': 75,
                'retry_cycle': 2,
                'retry_cycles_requested': 3,
                'batch_index': 1,
                'batch_count': 2,
                'capture_run_id': 700,
                'capture_run_ids': [699, 700],
                'sync_runs': [{'run_id': 880}, {'run_id': 881}],
                'remaining_candidate_events': 403,
                'last_progress_at': '2026-04-21T09:22:10+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            official_run_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['price_collection']['processed'], 3)
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'running')
        self.assertEqual(result['stages']['technical_snapshots']['current_symbol'], '000660')
        self.assertEqual(result['stages']['technical_snapshots']['processed'], 1)
        self.assertEqual(result['stages']['action_summaries']['status'], 'pending')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'running')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['current_activity'],
            'Capturing DART fundamentals for 75 events, retry cycle 2/3, batch 1/2',
        )
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['source_run_ids']['capture_run_ids'], [699, 700])
        self.assertNotIn(
            'trigger_after_capture_enabled',
            result['stages']['fundamentals_daily_refresh']['details'],
        )

    def test_read_promotes_active_auto_followup_progress_when_official_rows_are_absent(self):
        market_date = '2026-06-05'
        pykrx_run = {
            'id': 311,
            'market': 'KRX',
            'trade_date': market_date,
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'running',
            'symbols_total': 2806,
            'symbols_succeeded': 2806,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-06-05T06:41:47+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': market_date,
                'markets': ['KOSPI', 'KOSDAQ'],
                'stage': 'signal_decider_evaluations',
                'current_step': 'signal_decider_evaluations_running',
                'current_activity': 'Running signal-decider evaluation follow-up',
                'last_progress_at': '2026-06-05T07:06:00+00:00',
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'processed': 2806,
                        'total': 2806,
                        'succeeded': 2806,
                        'failed': 0,
                        'updated_at': '2026-06-05T06:35:00+00:00',
                    },
                    'market_risk_filter': {
                        'status': 'completed',
                        'processed': 2806,
                        'total': 2806,
                        'succeeded': 2806,
                        'failed': 0,
                        'updated_at': '2026-06-05T06:55:00+00:00',
                    },
                    'signal_decider_evaluations': {
                        'status': 'running',
                        'started_at': '2026-06-05T07:05:30+00:00',
                        'updated_at': '2026-06-05T07:06:00+00:00',
                        'current_step': 'signal_decider_evaluations_running',
                        'current_activity': 'Running signal-decider evaluation follow-up',
                        'processed': 14,
                        'total': 228,
                        'succeeded': 14,
                        'failed': 0,
                        'source_run_ids': {'signal_decider_evaluation_run_id': 'recent-coverage'},
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {
                'price_collection': {'persisted': 2806, 'updated_at': '2026-06-05T06:35:00+00:00'},
                'technical_snapshots': {'persisted': 2806, 'updated_at': '2026-06-05T06:45:00+00:00'},
                'action_summaries': {'persisted': 2806, 'updated_at': '2026-06-05T06:50:00+00:00'},
                'trending_ranks': {'persisted': 8418, 'periods_persisted': 3, 'updated_at': '2026-06-05T06:52:00+00:00'},
            },
            live_symbol_count_loader=lambda: 4480,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 2806,
        )

        result = service.read(market_date=market_date)

        self.assertIsNone(result['official_run'])
        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['running_stages'], 1)
        self.assertEqual(result['current_stage'], 'signal_decider_evaluations')
        stage = result['stages']['signal_decider_evaluations']
        self.assertEqual(stage['status'], 'running')
        self.assertEqual(stage['processed'], 14)
        self.assertEqual(stage['total'], 228)
        self.assertEqual(stage['current_activity'], 'Running signal-decider evaluation follow-up')
        self.assertEqual(stage['source_run_ids']['eod_run_id'], 311)
        self.assertEqual(stage['source_run_ids']['signal_decider_evaluation_run_id'], 'recent-coverage')
        self.assertEqual(stage['details']['mode'], 'eod_collection_run_stage_progress')
        self.assertEqual(stage['details']['progress_source'], 'live_run_metadata')
        self.assertEqual(stage['details']['last_progress_at'], '2026-06-05T07:06:00+00:00')
        self.assertEqual(stage['details']['stale_running_threshold_minutes'], 30)

    def test_read_downgrades_failed_pykrx_stage_when_progress_row_was_left_running(self):
        pykrx_run = {
            'id': 202,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'failed',
            'symbols_total': 3,
            'symbols_succeeded': 1,
            'symbols_failed': 2,
            'error_message': 'Marked stale after 30 minutes without completion.',
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'stage': 'technical_snapshots',
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'processed': 3,
                        'total': 3,
                        'succeeded': 3,
                        'failed': 0,
                    },
                    'technical_snapshots': {
                        'status': 'running',
                        'processed': 1,
                        'total': 3,
                        'succeeded': 1,
                        'failed': 0,
                        'current_symbol': '000660',
                        'current_step': 'computing_symbol_snapshot',
                        'current_activity': 'Computing daily technical snapshots for 000660',
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'failed')
        self.assertEqual(
            result['stages']['technical_snapshots']['current_activity'],
            'Marked stale after 30 minutes without completion.',
        )

    def test_read_uses_failed_refresh_error_message_instead_of_running_activity(self):
        fundamentals_run = {
            'id': 77,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'failed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3579,
            'symbols_checked': 120,
            'symbols_missing': 10,
            'symbols_stale': 35,
            'symbols_fresh': 60,
            'symbols_refreshed': 15,
            'symbols_failed': 2,
            'events_detected': 440,
            'events_captured': 37,
            'events_failed': 1,
            'error_message': 'Marked failed after exceeding the stale running threshold before a daily refresh status read',
            'started_at': '2026-04-21T09:21:00+00:00',
            'completed_at': '2026-04-21T10:21:00+00:00',
            'run_metadata': {
                'symbols_selected': 200,
                'stage': 'capturing_fundamentals',
                'capture_event_count': 75,
                'retry_cycle': 2,
                'retry_cycles_requested': 3,
                'batch_index': 1,
                'batch_count': 2,
                'capture_run_id': 700,
                'capture_run_ids': [699, 700],
                'sync_runs': [{'run_id': 880}, {'run_id': 881}],
                'remaining_candidate_events': 403,
                'last_progress_at': '2026-04-21T09:22:10+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
        )

        result = service.read()

        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'failed')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['current_activity'],
            'Marked failed after exceeding the stale running threshold before a daily refresh status read',
        )

    def test_read_marks_completed_core_pipeline_partial_when_factor_rows_are_missing(self):
        pykrx_run = {
            'id': 303,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 88,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2,
            'symbols_refreshed': 1,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T10:01:00+00:00',
            'completed_at': '2026-04-21T10:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 3,
                'last_progress_at': '2026-04-21T10:05:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'completed')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'pending')
        self.assertEqual(result['stages']['value_factors']['status'], 'pending')
        self.assertEqual(result['stages']['composite_factors']['status'], 'pending')
        self.assertEqual(
            result['stage_order'],
            [
                'master_refresh',
                'pykrx_universe_refresh',
                'price_collection',
                'pykrx_enrichment',
                'technical_snapshots',
                'action_summaries',
                'trending_ranks',
                'market_risk_filter',
                'fundamentals_daily_refresh',
                'piotroski_filter',
                'momentum_factors',
                'value_factors',
                'low_risk_factors',
                'beta_factors',
                'advanced_value_factors',
                'unified_value_factors',
                'quality_factors',
                'composite_factors',
                'signal_features',
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
                'agent_feedback_summaries',
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )

    def test_read_keeps_blocked_factor_stage_pending_when_stored_rows_exist_before_fundamentals(self):
        pykrx_run = {
            'id': 304,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        factor_stage_counts = {
            'momentum_factors': {
                'persisted': 2,
                'updated_at': '2026-04-21T10:10:00+00:00',
            },
            'advanced_value_factors': {
                'persisted': 3,
                'updated_at': '2026-04-21T10:15:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            factor_stage_counts_loader=lambda **_: factor_stage_counts,
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=3,
                market_count=2,
                updated_at='2026-04-21T10:00:00+00:00',
            ),
        )

        result = service.read()

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'partial')
        self.assertEqual(
            result['stages']['momentum_factors']['current_activity'],
            'Stored 2/3 rows for momentum factor compute',
        )
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'pending')
        self.assertEqual(
            result['stages']['advanced_value_factors']['current_activity'],
            'Waiting on DART fundamentals daily refresh',
        )
        self.assertEqual(
            result['stages']['advanced_value_factors']['details']['blocking_stages'],
            ['fundamentals_daily_refresh'],
        )
        self.assertEqual(
            result['stages']['advanced_value_factors']['source_status'],
            'stored_rows_present',
        )
        self.assertIsNone(result['stages']['advanced_value_factors']['completed_at'])

    def test_read_keeps_blocked_factor_stage_pending_when_dependency_failed(self):
        pykrx_run = {
            'id': 305,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 89,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'failed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_missing': 1,
            'symbols_stale': 1,
            'symbols_fresh': 1,
            'symbols_refreshed': 0,
            'symbols_failed': 1,
            'events_detected': 5,
            'events_captured': 2,
            'events_failed': 1,
            'error_message': 'Fundamentals refresh failed',
            'started_at': '2026-04-21T10:01:00+00:00',
            'completed_at': '2026-04-21T10:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 3,
                'last_progress_at': '2026-04-21T10:05:00+00:00',
            },
        }
        factor_stage_counts = {
            'advanced_value_factors': {
                'persisted': 3,
                'updated_at': '2026-04-21T10:15:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            factor_stage_counts_loader=lambda **_: factor_stage_counts,
        )

        result = service.read()

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'failed')
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'pending')
        self.assertEqual(
            result['stages']['advanced_value_factors']['current_activity'],
            'Blocked by failed DART fundamentals daily refresh',
        )
        self.assertEqual(
            result['stages']['advanced_value_factors']['details']['failed_blocking_stages'],
            ['fundamentals_daily_refresh'],
        )
        self.assertIsNone(result['stages']['advanced_value_factors']['completed_at'])

    def test_completed_official_factor_is_not_replaced_by_stale_dependency_wait(self):
        market_date = '2026-09-28'
        updated_at = '2026-09-28T20:00:00+00:00'
        fundamentals_item = _completed_official_stage(
            run_id=480,
            stage_id='fundamentals_daily_refresh',
            market_date=market_date,
            processed=0,
            total=0,
            updated_at=updated_at,
        )
        fundamentals_item['stage']['details'] = {
            'result_summary': {
                'triggered': False,
                'skipped': True,
                'safe_skip': True,
                'reason': 'point_in_time_recovery_current_only_stage',
            }
        }
        affected_stage_ids = (
            'piotroski_filter',
            'advanced_value_factors',
            'unified_value_factors',
            'quality_factors',
            'composite_factors',
        )
        stage_history = {
            'fundamentals_daily_refresh': fundamentals_item,
            **{
                stage_id: _completed_official_stage(
                    run_id=480,
                    stage_id=stage_id,
                    market_date=market_date,
                    processed=3,
                    total=3,
                    updated_at=updated_at,
                )
                for stage_id in affected_stage_ids
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {
                'price_collection': {'persisted': 3, 'updated_at': updated_at},
            },
            live_symbol_count_loader=lambda: 3,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: stage_history,
            price_bar_count_loader=lambda **_: 3,
            row_count_stage_counts_loader=lambda **_: {
                stage_id: {'persisted': 3, 'updated_at': updated_at}
                for stage_id in (
                    'piotroski_filter',
                    *DEFAULT_FACTOR_STAGE_ORDER,
                )
            },
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=3,
                market_count=2,
                updated_at=updated_at,
            ),
        )

        result = service.read(market_date=market_date)

        for stage_id in affected_stage_ids:
            with self.subTest(stage_id=stage_id):
                factor_stage = result['stages'][stage_id]
                self.assertEqual(factor_stage['status'], 'completed')
                self.assertEqual(
                    factor_stage['details']['mode'],
                    'official_daily_pipeline_run_state',
                )
                self.assertEqual(
                    factor_stage['details'].get('blocking_stages', []),
                    [],
                )

    def test_completed_official_factor_supersedes_stale_dependency_failure(self):
        self.assertTrue(
            EODPipelineStatusService._stored_dependency_wait_is_stale(
                stored_stage={
                    'status': 'pending',
                    'gate_status': 'blocked',
                    'details': {
                        'blocking_stages': ['fundamentals_daily_refresh'],
                        'failed_blocking_stages': ['fundamentals_daily_refresh'],
                        'rows_persisted': 3,
                        'expected_rows': 3,
                    },
                },
                current_stage={
                    'status': 'completed',
                    'details': {
                        'mode': 'official_daily_pipeline_run_state',
                        'result_summary': {
                            'status': 'completed',
                            'results_persisted': 3,
                        },
                    },
                },
                current_stages={
                    'fundamentals_daily_refresh': {'status': 'completed'},
                },
            )
        )

    def test_completed_eod_progress_factor_is_not_replaced_by_stale_dependency_wait(self):
        market_date = '2026-09-28'
        updated_at = '2026-09-28T20:00:00+00:00'
        affected_stage_ids = (
            'piotroski_filter',
            'advanced_value_factors',
            'unified_value_factors',
            'quality_factors',
            'composite_factors',
        )
        completed_progress = {
            'status': 'completed',
            'processed': 3,
            'total': 3,
            'succeeded': 3,
            'failed': 0,
            'updated_at': updated_at,
            'result_summary': {'results_persisted': 3, 'results_failed': 0},
        }
        pykrx_run = {
            'id': 552,
            'market': 'KRX',
            'trade_date': market_date,
            'source': 'pykrx',
            'trigger': 'internal_ops_cli',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': updated_at,
            'completed_at': updated_at,
            'run_metadata': {
                'mode': 'daily_auto',
                'last_progress_at': updated_at,
                'stage_progress': {
                    'fundamentals_daily_refresh': {
                        'status': 'completed',
                        'processed': 0,
                        'total': 0,
                        'succeeded': 0,
                        'failed': 0,
                        'updated_at': updated_at,
                        'result_summary': {
                            'triggered': False,
                            'skipped': True,
                            'safe_skip': True,
                            'reason': 'point_in_time_recovery_current_only_stage',
                        },
                    },
                    **{
                        stage_id: dict(completed_progress)
                        for stage_id in affected_stage_ids
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {
                'price_collection': {'persisted': 3, 'updated_at': updated_at},
            },
            live_symbol_count_loader=lambda: 3,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 3,
            row_count_stage_counts_loader=lambda **_: {
                stage_id: {'persisted': 3, 'updated_at': updated_at}
                for stage_id in (
                    'piotroski_filter',
                    *DEFAULT_FACTOR_STAGE_ORDER,
                )
            },
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=3,
                market_count=2,
                updated_at=updated_at,
            ),
        )

        result = service.read(market_date=market_date)

        for stage_id in affected_stage_ids:
            with self.subTest(stage_id=stage_id):
                factor_stage = result['stages'][stage_id]
                self.assertEqual(factor_stage['status'], 'completed')
                self.assertEqual(
                    factor_stage['details']['mode'],
                    'eod_collection_run_stage_progress',
                )
                self.assertEqual(
                    factor_stage['details'].get('blocking_stages', []),
                    [],
                )

    def test_read_prefers_stored_price_bar_count_when_price_stage_metadata_is_stale(self):
        pykrx_run = {
            'id': 306,
            'market': 'KRX',
            'trade_date': '2026-05-06',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2811,
            'symbols_succeeded': 2811,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T06:10:00+00:00',
            'completed_at': '2026-05-06T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-05-06',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 2810, 'updated_at': '2026-05-06T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 2810, 'updated_at': '2026-05-06T06:35:00+00:00'},
            'action_summaries': {'persisted': 2810, 'updated_at': '2026-05-06T06:45:00+00:00'},
            'trending_ranks': {'persisted': 8430, 'periods_persisted': 3, 'updated_at': '2026-05-06T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 90,
            'market_date': '2026-05-06',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2810,
            'symbols_checked': 2810,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2750,
            'symbols_refreshed': 60,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T07:01:00+00:00',
            'completed_at': '2026-05-06T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2810,
                'last_progress_at': '2026-05-06T07:05:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            row_count_stage_counts_loader=lambda **_: _full_feedback_prerequisite_stage_counts(
                total=2810,
                updated_at='2026-05-06T07:15:00+00:00',
            ),
            price_bar_count_loader=lambda **_: 2810,
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=2810,
                market_count=2,
                updated_at='2026-05-06T07:15:00+00:00',
            ),
        )

        result = service.read(market_date='2026-05-06')

        self.assertEqual(result['stages']['momentum_factors']['status'], 'completed')
        self.assertEqual(result['stages']['unified_value_factors']['status'], 'completed')
        self.assertEqual(result['stages']['quality_factors']['status'], 'completed')
        self.assertEqual(result['stages']['momentum_factors']['details']['expected_rows'], 2810)
        self.assertEqual(
            result['stages']['momentum_factors']['details']['expected_rows_source'],
            'daily_price_bars',
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_bars_persisted'],
            2810,
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_stage_expected_rows'],
            2810,
        )

    def test_read_prefers_date_scoped_price_bars_over_limited_retry_run_counts_for_factor_progress(self):
        pykrx_run = {
            'id': 307,
            'market': 'KRX',
            'trade_date': '2026-05-06',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 100,
            'symbols_succeeded': 100,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T06:10:00+00:00',
            'completed_at': '2026-05-06T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-05-06',
                'requested_start_date': '20260506',
                'requested_end_date': '20260506',
                'limited_run': True,
                'retry': {
                    'retry_of_run_id': 300,
                    'retry_cycle': 2,
                    'symbol_count': 100,
                },
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 100, 'updated_at': '2026-05-06T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 100, 'updated_at': '2026-05-06T06:35:00+00:00'},
            'action_summaries': {'persisted': 100, 'updated_at': '2026-05-06T06:45:00+00:00'},
            'trending_ranks': {'persisted': 300, 'periods_persisted': 3, 'updated_at': '2026-05-06T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 91,
            'market_date': '2026-05-06',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2810,
            'symbols_checked': 2810,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2750,
            'symbols_refreshed': 60,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T07:01:00+00:00',
            'completed_at': '2026-05-06T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2810,
                'last_progress_at': '2026-05-06T07:05:00+00:00',
            },
        }
        factor_stage_counts = {
            'momentum_factors': {
                'persisted': 1000,
                'updated_at': '2026-05-06T07:15:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            factor_stage_counts_loader=lambda **_: factor_stage_counts,
            price_bar_count_loader=lambda **_: 2810,
        )

        result = service.read(market_date='2026-05-06')

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'partial')
        self.assertEqual(result['stages']['momentum_factors']['total'], 2810)
        self.assertEqual(
            result['stages']['momentum_factors']['current_activity'],
            'Stored 1000/2810 rows for momentum factor compute',
        )
        self.assertEqual(result['stages']['momentum_factors']['details']['expected_rows'], 2810)
        self.assertEqual(
            result['stages']['momentum_factors']['details']['expected_rows_source'],
            'daily_price_bars',
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_bars_persisted'],
            2810,
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_stage_expected_rows'],
            2810,
        )

    def test_read_marks_market_risk_gap_non_blocking_when_feedback_running_cleanly(self):
        pykrx_run = {
            'id': 390,
            'market': 'KRX',
            'trade_date': '2026-05-27',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2806,
            'symbols_succeeded': 2806,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-27T06:10:00+00:00',
            'completed_at': '2026-05-27T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-05-27',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 2806, 'updated_at': '2026-05-27T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 2806, 'updated_at': '2026-05-27T06:35:00+00:00'},
            'action_summaries': {'persisted': 2806, 'updated_at': '2026-05-27T06:45:00+00:00'},
            'trending_ranks': {'persisted': 8418, 'periods_persisted': 3, 'updated_at': '2026-05-27T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 3901,
            'market_date': '2026-05-27',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2806,
            'symbols_checked': 2806,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2800,
            'symbols_refreshed': 6,
            'symbols_failed': 0,
            'events_detected': 0,
            'events_captured': 0,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-27T07:01:00+00:00',
            'completed_at': '2026-05-27T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2806,
                'last_progress_at': '2026-05-27T07:05:00+00:00',
            },
        }
        row_counts = _full_feedback_prerequisite_stage_counts(
            total=2806,
            updated_at='2026-05-27T07:15:00+00:00',
        )
        row_counts['market_risk_filter'] = {
            'persisted': 2805,
            'updated_at': '2026-05-27T07:15:00+00:00',
            'missing_count': 1,
            'missing_symbols_sample': ['176750'],
            'missing_symbol_details_sample': [
                {'symbol': '176750', 'name_ko': 'DuChemBio', 'market': 'KOSDAQ'},
            ],
        }
        feedback_run = {
            'source': 'runtime_feedback_state',
            'run_id': 'feedback-cycle-2026-05-27-c21e78a409',
            'status': 'running',
            'requested_symbols': ['005930', '000660'],
            'summary': {
                'run_status': 'running',
                'run_table_status': 'running',
                'current_role': 'fundamental-summarizer',
                'failed_items': 0,
                'completed_feedback_count': 280,
                'expected_feedback_count': 565,
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            row_count_stage_counts_loader=lambda **_: row_counts,
            price_bar_count_loader=lambda **_: 2806,
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=2806,
                market_count=2,
                updated_at='2026-05-27T06:25:00+00:00',
            ),
            feedback_run_loader=lambda **_: feedback_run,
            row_count_scope_gap_loader=lambda **kwargs: _scope_gap_evidence(
                requested_count=len(kwargs['symbols']),
                present_count=len(kwargs['symbols']),
                missing_symbol_details_sample=[],
            ),
        )

        result = service.read(market_date='2026-05-27')

        self.assertEqual(result['status'], 'running')
        market_risk_stage = result['stages']['market_risk_filter']
        self.assertEqual(market_risk_stage['status'], 'partial')
        self.assertEqual(market_risk_stage['gate_status'], 'ready')
        self.assertEqual(market_risk_stage['severity'], 'degraded')
        self.assertEqual(market_risk_stage['blocking_status'], 'non_blocking')
        self.assertEqual(market_risk_stage['details']['missing_count'], 1)
        self.assertEqual(market_risk_stage['details']['missing_symbols_sample'], ['176750'])
        self.assertEqual(
            market_risk_stage['details']['gate_reason'],
            'missing_rows_outside_active_feedback_scope',
        )
        self.assertEqual(market_risk_stage['details']['feedback_scope_gap']['missing_count'], 0)
        feedback_stage = result['stages']['agent_feedback_summaries']
        self.assertEqual(feedback_stage['status'], 'running')
        self.assertEqual(feedback_stage['failed'], 0)
        self.assertEqual(feedback_stage['details']['feedback_failure_status'], 'no_failures')

    def test_read_reconciles_agent_feedback_stage_from_child_progress_when_run_snapshot_is_stale(self):
        roles = [
            'eod-summarizer',
            'technical-summarizer',
            'fundamental-summarizer',
            'short-term-decider',
            'signal-decider',
        ]
        stale_run_snapshot = {
            'run_id': 'symbol-run-2026-06-19-stale',
            'status': 'running',
            'symbols_total': 2,
            'completed_count': 0,
            'failed_count': 0,
            'started_at': '2026-06-19T07:53:00+00:00',
            'completed_at': None,
            'request_payload': {
                'mode': 'role_sequential_runtime_buffered',
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'roles': roles,
            },
            'summary_payload': {
                'progress_summary': {
                    'run_id': 'symbol-run-2026-06-19-stale',
                    'run_status': 'running',
                    'run_table_status': 'running',
                    'current_role': 'eod-summarizer',
                    'completed_feedback_count': 0,
                    'failed_items': 0,
                    'expected_feedback_count': 10,
                    'latest_feedback_at': '2026-06-19T07:53:00+00:00',
                },
            },
        }
        reconciled_progress = {
            'summary': {
                'run_id': 'symbol-run-2026-06-19-stale',
                'run_status': 'running',
                'run_table_status': 'running',
                'is_live': True,
                'run_started_at': '2026-06-19T07:53:00+00:00',
                'current_role': 'eod-summarizer',
                'completed_roles': [],
                'remaining_roles': roles,
                'total_symbols': 2,
                'processed_symbols': 2,
                'completed_symbols': 0,
                'partial_symbols': 2,
                'in_progress_symbols': 0,
                'pending_symbols': 0,
                'completed_feedback_count': 2,
                'failed_items': 1,
                'expected_feedback_count': 10,
                'latest_feedback_at': '2026-06-19T09:39:03+00:00',
                'latest_failed_feedback_at': '2026-06-19T09:38:00+00:00',
                'failure_groups': {
                    'transport/timeout': {
                        'failure_status': 'transport/timeout',
                        'count': 1,
                        'symbols_sample': ['000660'],
                    },
                },
                'role_progress': {
                    'eod-summarizer': {
                        'role': 'eod-summarizer',
                        'status': 'completed',
                        'total_symbols': 2,
                        'processed_symbols': 2,
                        'completed_symbols': 2,
                        'pending_symbols': 0,
                        'failed_items': 0,
                    },
                    'technical-summarizer': {
                        'role': 'technical-summarizer',
                        'status': 'partial',
                        'total_symbols': 2,
                        'processed_symbols': 1,
                        'completed_symbols': 0,
                        'pending_symbols': 1,
                        'failed_items': 1,
                    },
                    'fundamental-summarizer': {'role': 'fundamental-summarizer', 'status': 'pending', 'total_symbols': 2},
                    'short-term-decider': {'role': 'short-term-decider', 'status': 'pending', 'total_symbols': 2},
                    'signal-decider': {'role': 'signal-decider', 'status': 'pending', 'total_symbols': 2},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            pykrx_enrichment_loader=lambda **_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            row_count_stage_counts_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 0,
            signal_feature_counts_loader=lambda **_: {},
            external_news_counts_loader=lambda **_: {},
            signal_decider_counts_loader=lambda **_: {},
            feedback_run_loader=_load_latest_feedback_run_summary,
        )

        with patch(
            'app.agent_context.runtime_state.runtime_feedback_state_store.get_run',
            return_value=None,
        ), patch(
            'app.agent_context.run_state_service.agent_context_feedback_run_state_service.find_latest_run',
            return_value=stale_run_snapshot,
        ) as latest_run_loader, patch(
            'app.agent_context.progress_service.agent_context_progress_service.get_feedback_progress',
            return_value=reconciled_progress,
        ) as progress_loader:
            result = service.read(market_date='2026-06-19')

        self.assertEqual(
            latest_run_loader.call_args.kwargs['request_modes'],
            [DAILY_RETRY_CYCLE_MODE],
        )
        self.assertEqual(
            latest_run_loader.call_args.kwargs['requested_roles'],
            [
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
            ],
        )
        progress_loader.assert_called_once()
        self.assertIsNone(progress_loader.call_args.kwargs.get('symbols'))
        self.assertEqual(
            progress_loader.call_args.kwargs['run_id'],
            'symbol-run-2026-06-19-stale',
        )
        stage = result['stages']['agent_feedback_summaries']
        self.assertEqual(stage['status'], 'running')
        self.assertEqual(stage['updated_at'], '2026-06-19T09:39:03+00:00')
        self.assertEqual(stage['processed'], 3)
        self.assertEqual(stage['total'], 10)
        self.assertEqual(stage['succeeded'], 2)
        self.assertEqual(stage['failed'], 1)
        self.assertEqual(stage['details']['current_active_roles'], ['eod-summarizer'])
        self.assertEqual(stage['details']['target_symbols'], 2)
        self.assertEqual(stage['details']['completed_symbols'], 0)
        self.assertEqual(stage['details']['failed_symbols_count'], 1)
        self.assertEqual(stage['details']['latest_successful_feedback_at'], '2026-06-19T09:39:03+00:00')
        self.assertEqual(stage['details']['latest_failed_feedback_at'], '2026-06-19T09:38:00+00:00')
        self.assertEqual(stage['details']['role_progress']['eod-summarizer']['completed_symbols'], 2)
        self.assertIn('fundamental-summarizer', stage['details']['role_progress'])
        self.assertIn('short-term-decider', stage['details']['role_progress'])
        self.assertIn('signal-decider', stage['details']['role_progress'])

    def test_latest_feedback_run_summary_ignores_decider_runtime_state(self):
        class _DeciderRuntimeState:
            run_id = 'feedback-cycle-2026-10-07-decider'
            status = 'partial'
            requested_symbols = ['005930']
            requested_roles = ['signal-decider']

            @staticmethod
            def to_progress_summary():
                return {
                    'run_id': 'feedback-cycle-2026-10-07-decider',
                    'run_status': 'partial',
                    'completed_feedback_count': 143,
                    'failed_items': 17,
                    'expected_feedback_count': 160,
                }

        summarizer_run = {
            'run_id': 'feedback-cycle-2026-10-07-summarizers',
            'status': 'completed',
            'completed_at': '2026-10-07T05:01:00+00:00',
            'request_payload': {
                'mode': DAILY_RETRY_CYCLE_MODE,
                'roles': [
                    'eod-summarizer',
                    'technical-summarizer',
                    'fundamental-summarizer',
                ],
            },
        }
        summarizer_progress = {
            'summary': {
                'run_id': 'feedback-cycle-2026-10-07-summarizers',
                'run_status': 'completed',
                'completed_feedback_count': 470,
                'failed_items': 0,
                'expected_feedback_count': 470,
            },
        }

        with patch(
            'app.agent_context.runtime_state.runtime_feedback_state_store.get_run',
            return_value=_DeciderRuntimeState(),
        ), patch(
            'app.agent_context.run_state_service.agent_context_feedback_run_state_service.find_latest_run',
            return_value=summarizer_run,
        ) as latest_run_loader, patch(
            'app.agent_context.run_state_service.agent_context_feedback_run_state_service.resolve_run_requested_symbols',
            return_value=['005930', '000660'],
        ), patch(
            'app.agent_context.progress_service.agent_context_progress_service.get_feedback_progress',
            return_value=summarizer_progress,
        ) as progress_loader:
            result = _load_latest_feedback_run_summary(market_date='2026-10-07')

        self.assertEqual(result['run_id'], 'feedback-cycle-2026-10-07-summarizers')
        self.assertEqual(result['summary']['completed_feedback_count'], 470)
        self.assertEqual(
            latest_run_loader.call_args.kwargs['requested_roles'],
            [
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
            ],
        )
        progress_loader.assert_called_once_with(
            market_date=unittest.mock.ANY,
            run_id='feedback-cycle-2026-10-07-summarizers',
        )

    def test_read_preserves_partial_source_stage_despite_stored_full_coverage(self):
        pykrx_run = {
            'id': 401,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 877,
            'symbols_succeeded': 874,
            'symbols_failed': 3,
            'error_message': 'Failed symbols: 139050,217620,452980',
            'started_at': '2026-04-21T09:50:00+00:00',
            'completed_at': '2026-04-21T09:55:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'stage_progress': {
                    'price_collection': {'status': 'partial', 'processed': 3, 'total': 3, 'succeeded': 0, 'failed': 3},
                    'technical_snapshots': {'status': 'partial', 'processed': 3, 'total': 3, 'succeeded': 0, 'failed': 3},
                    'action_summaries': {'status': 'partial', 'processed': 3, 'total': 3, 'succeeded': 0, 'failed': 3},
                },
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 0, 'updated_at': None},
            'technical_snapshots': {'persisted': 0, 'updated_at': None},
            'action_summaries': {'persisted': 0, 'updated_at': None},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 4458,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            price_bar_count_loader=lambda **_: 3683,
            technical_snapshot_count_loader=lambda _: 3683,
            action_summary_count_loader=lambda _: 3683,
            row_count_stage_counts_loader=lambda **_: {
                'market_risk_filter': {'persisted': 3683, 'updated_at': '2026-04-21T10:15:00+00:00'},
                'piotroski_filter': {'persisted': 3683, 'updated_at': '2026-04-21T10:15:00+00:00'},
            },
        )

        result = service.read()

        for stage_id in ('price_collection', 'technical_snapshots', 'action_summaries'):
            self.assertEqual(result['stages'][stage_id]['status'], 'partial')
            self.assertEqual(
                result['stages'][stage_id]['source_run_ids'],
                {'eod_run_id': 401},
            )
            self.assertNotIn(
                'stored_rows_coverage_override',
                result['stages'][stage_id]['details'],
            )
        self.assertNotEqual(
            result['stages']['market_risk_filter']['status'],
            'completed',
        )

    def test_read_maps_completed_pipeline_stages_from_completed_run_and_aggregate_counts(self):
        pykrx_run = {
            'id': 303,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 88,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2,
            'symbols_refreshed': 1,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T10:01:00+00:00',
            'completed_at': '2026-04-21T10:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 3,
                'last_progress_at': '2026-04-21T10:05:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            official_run_loader=lambda **_: None,
            row_count_stage_counts_loader=lambda **_: _full_feedback_prerequisite_stage_counts(
                total=3,
                updated_at='2026-04-21T10:15:00+00:00',
            ),
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=3,
                market_count=2,
                updated_at='2026-04-21T10:12:00+00:00',
            ),
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['total_stages'], 26)
        self.assertEqual(result['stage_status_counts']['completed'], 16)
        self.assertEqual(result['skipped_stages'], 2)
        self.assertEqual(result['partial_stages'], 0)
        self.assertEqual(result['pending_stages'], 8)
        self.assertEqual(result['current_stage'], 'signal_features')
        self.assertEqual(result['stages']['master_refresh']['status'], 'skipped')
        self.assertEqual(result['stages']['pykrx_universe_refresh']['status'], 'skipped')
        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'completed')
        self.assertEqual(result['stages']['action_summaries']['status'], 'completed')
        self.assertEqual(result['stages']['trending_ranks']['status'], 'completed')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'completed')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'completed')
        self.assertEqual(result['stages']['composite_factors']['status'], 'completed')
        self.assertEqual(result['stages']['trending_ranks']['processed'], 3)
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['succeeded'], 3)

    def test_read_uses_persisted_outputs_but_keeps_blocked_dependencies_pending_for_single_stage_resume(self):
        market_date = '2026-05-28'
        pykrx_run = {
            'id': 400,
            'market': 'KRX',
            'trade_date': market_date,
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2806,
            'symbols_succeeded': 2806,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-28T06:10:00+00:00',
            'completed_at': '2026-05-28T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': market_date,
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 2806, 'updated_at': '2026-05-28T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 2806, 'updated_at': '2026-05-28T06:35:00+00:00'},
            'action_summaries': {'persisted': 2806, 'updated_at': '2026-05-28T06:45:00+00:00'},
            'trending_ranks': {'persisted': 8418, 'periods_persisted': 3, 'updated_at': '2026-05-28T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 4001,
            'market_date': market_date,
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2806,
            'symbols_checked': 2806,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2800,
            'symbols_refreshed': 6,
            'symbols_failed': 0,
            'events_detected': 0,
            'events_captured': 0,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-28T07:01:00+00:00',
            'completed_at': '2026-05-28T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2806,
                'last_progress_at': '2026-05-28T07:05:00+00:00',
            },
        }
        official_run = {
            'id': 9,
            'market_date': market_date,
            'trigger': 'manual_api',
            'status': 'completed',
            'current_stage': 'signal_decider_evaluations',
            'started_at': '2026-05-28T09:00:00+00:00',
            'updated_at': '2026-05-28T09:05:00+00:00',
            'completed_at': '2026-05-28T09:05:00+00:00',
            'error_message': None,
            'run_metadata': {
                'requested_stages': ['signal_decider_evaluations'],
                'resolved_stages': ['signal_decider_evaluations'],
            },
            'stages': {
                'signal_decider_evaluations': {
                    'stage_id': 'signal_decider_evaluations',
                    'status': 'completed',
                    'market_date': market_date,
                    'started_at': '2026-05-28T09:00:00+00:00',
                    'updated_at': '2026-05-28T09:05:00+00:00',
                    'completed_at': '2026-05-28T09:05:00+00:00',
                    'processed': 228,
                    'total': 228,
                    'succeeded': 228,
                    'failed': 0,
                    'remaining': 0,
                    'current_step': None,
                    'current_activity': None,
                    'current_symbol': None,
                    'current_period': None,
                    'source_run_ids': {},
                    'details': {},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 4480,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            row_count_stage_counts_loader=lambda **_: _full_feedback_prerequisite_stage_counts(
                total=2806,
                updated_at='2026-05-28T07:15:00+00:00',
            ),
            price_bar_count_loader=lambda **_: 2806,
            pykrx_enrichment_loader=lambda **_: _blocked_pykrx_enrichment_coverage(
                expected_symbols=4480,
                market_count=3,
                updated_at='2026-05-28T06:25:00+00:00',
            ),
            official_run_loader=lambda **_: official_run,
            official_stage_history_loader=lambda **_: {
                'agent_feedback_summaries': _completed_feedback_history_stage(
                    run_id=8,
                    market_date=market_date,
                    total=570,
                ),
                'next_open_execution_plan': _completed_next_open_history_stage(
                    run_id=8,
                    market_date=market_date,
                    total=114,
                ),
            },
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2823,
                'updated_at': '2026-05-28T06:30:00+00:00',
            },
            external_news_counts_loader=lambda **_: {
                'items': 1091,
                'symbols': 114,
                'classified_items': 1091,
                'updated_at': '2026-05-28T06:40:00+00:00',
            },
            macro_context_counts_loader=lambda **_: _available_macro_context_counts(
                updated_at='2026-05-28T06:50:00+00:00',
            ),
            signal_decider_counts_loader=lambda **_: {
                'feedback_rows': 114,
                'feedback_symbols': 114,
                'decision_snapshots': 228,
                'decision_snapshot_symbols': 114,
                'updated_at': '2026-05-28T08:50:00+00:00',
            },
        )

        result = service.read(market_date=market_date)

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['total_stages'], 26)
        self.assertEqual(result['completed_stages'], 17)
        self.assertEqual(result['skipped_stages'], 2)
        self.assertEqual(result['display_completed_stages'], 19)
        self.assertEqual(result['pending_stages'], 5)
        self.assertEqual(result['partial_stages'], 1)
        self.assertEqual(result['running_stages'], 0)
        self.assertEqual(result['current_stage'], 'signal_decider_evaluations')
        self.assertEqual(result['current_stage_status'], 'partial')
        self.assertEqual(result['stages']['master_refresh']['status'], 'skipped')
        self.assertEqual(
            result['stages']['master_refresh']['details']['reason'],
            'preopen_global_stage_not_required_for_resumed_market_date_status',
        )
        self.assertEqual(result['stages']['signal_features']['status'], 'completed')
        self.assertEqual(result['stages']['signal_features']['details']['rows_persisted'], 2823)
        self.assertEqual(result['stages']['external_news_collect']['status'], 'completed')
        self.assertEqual(result['stages']['external_news_collect']['details']['items_persisted'], 1091)
        self.assertEqual(result['stages']['external_news_enrich']['status'], 'completed')
        self.assertEqual(result['stages']['external_news_enrich']['details']['classified_items'], 1091)
        self.assertEqual(result['stages']['macro_context_refresh']['status'], 'completed')
        self.assertEqual(result['stages']['macro_context_refresh']['details']['persisted_series_count'], 3)
        self.assertEqual(result['stages']['signal_decider']['status'], 'completed')
        self.assertEqual(result['stages']['signal_decider']['details']['feedback_rows'], 114)
        self.assertEqual(result['stages']['signal_decider']['details']['decision_snapshots'], 228)
        self.assertEqual(
            result['stages']['next_open_execution_plan']['status'],
            'blocked',
        )
        self.assertEqual(
            result['stages']['next_open_execution_plan']['details'][
                'status_repair_reason'
            ],
            'signal_decider_completion_proof_unavailable_or_mismatched',
        )
        self.assertEqual(
            result['stages']['signal_decider_evaluations']['status'],
            'partial',
        )
        pykrx_stage = result['stages']['pykrx_enrichment']
        self.assertEqual(pykrx_stage['status'], 'completed')
        self.assertEqual(pykrx_stage['gate_status'], 'blocked')
        self.assertEqual(pykrx_stage['details']['decider_category_gate_status'], 'blocked')
        for stage_id in (
            'piotroski_filter',
            'value_factors',
            'advanced_value_factors',
            'unified_value_factors',
        ):
            with self.subTest(stage_id=stage_id):
                self.assertEqual(result['stages'][stage_id]['status'], 'pending')
                self.assertIn(
                    'pykrx_enrichment',
                    result['stages'][stage_id]['details']['blocking_stages'],
                )
        self.assertEqual(result['stages']['composite_factors']['status'], 'pending')
        self.assertEqual(
            result['stages']['composite_factors']['details']['blocking_stages'],
            ['value_factors', 'advanced_value_factors'],
        )
        self.assertEqual(result['gate_summary']['status'], 'blocked')
        self.assertEqual(result['gate_summary']['blocked_stage_ids'], ['pykrx_enrichment'])
        blocking_categories = pykrx_stage['details']['decider_blocking_categories']
        self.assertIn('sector_classification', {item['category'] for item in blocking_categories})
        self.assertEqual(
            blocking_categories[0]['reason'],
            'incomplete_required_pykrx_enrichment_category',
        )

    def test_read_preserves_incomplete_official_history_for_persisted_output_stage(self):
        market_date = '2026-05-28'
        resumed_stage = _completed_official_stage(
            run_id=22,
            stage_id='signal_decider_evaluations',
            market_date=market_date,
            processed=10,
            total=10,
            updated_at='2026-05-28T09:05:00+00:00',
        )['stage']
        latest_resume_run = {
            'id': 22,
            'market_date': market_date,
            'trigger': 'manual_api',
            'status': 'completed',
            'current_stage': 'signal_decider_evaluations',
            'started_at': '2026-05-28T09:00:00+00:00',
            'updated_at': '2026-05-28T09:05:00+00:00',
            'completed_at': '2026-05-28T09:05:00+00:00',
            'error_message': None,
            'run_metadata': {
                'requested_stages': ['signal_decider_evaluations'],
                'resolved_stages': ['signal_decider_evaluations'],
            },
            'stages': {'signal_decider_evaluations': resumed_stage},
        }

        for official_status in ('partial', 'failed'):
            with self.subTest(official_status=official_status):
                service = EODPipelineStatusService(
                    market_state_loader=lambda: _FakeMarketState(),
                    pykrx_run_loader=lambda **_: None,
                    pykrx_stage_counts_loader=lambda *_: {},
                    live_symbol_count_loader=lambda: 10,
                    fundamentals_refresh_loader=lambda **_: None,
                    official_run_loader=lambda **_: latest_resume_run,
                    official_stage_history_loader=lambda **_: {
                        'signal_features': _incomplete_official_stage(
                            run_id=21,
                            stage_id='signal_features',
                            market_date=market_date,
                            status=official_status,
                            updated_at='2026-05-28T08:45:00+00:00',
                            succeeded=3,
                            failed=2,
                        ),
                    },
                    signal_feature_counts_loader=lambda **_: {
                        'persisted': 3,
                        'updated_at': '2026-05-28T08:40:00+00:00',
                    },
                )

                result = service.read(market_date=market_date)

                stage = result['stages']['signal_features']
                self.assertEqual(stage['status'], official_status)
                self.assertEqual(stage['source_status'], official_status)
                self.assertEqual(stage['processed'], 5)
                self.assertEqual(stage['succeeded'], 3)
                self.assertEqual(stage['failed'], 2)
                self.assertEqual(stage['details']['features_persisted'], 3)
                self.assertEqual(stage['details']['features_failed'], 2)
                self.assertTrue(stage['details']['historical_stage_state'])
                self.assertEqual(stage['details']['progress_source'], 'official_stage_history')
                self.assertEqual(
                    result['current_stage'],
                    (
                        'signal_features'
                        if official_status == 'partial'
                        else 'signal_decider_evaluations'
                    ),
                )
                self.assertEqual(
                    result['stages']['signal_decider_evaluations']['status'],
                    'partial',
                )

    def test_read_prefers_newer_persisted_outputs_over_older_blocked_eod_progress(self):
        market_date = '2026-10-07'
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: _blocked_downstream_output_eod_run(
                market_date=market_date,
                updated_at='2026-10-07T12:34:45+00:00',
            ),
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2_703,
                'updated_at': '2026-10-07T13:46:33+00:00',
            },
            external_news_counts_loader=lambda **_: {
                'items': 259,
                'symbols': 77,
                'classified_items': 193,
                'updated_at': '2026-10-07T13:56:17+00:00',
            },
        )

        result = service.read(market_date=market_date)

        signal_features = result['stages']['signal_features']
        self.assertEqual(signal_features['status'], 'completed')
        self.assertEqual(signal_features['details']['rows_persisted'], 2_703)
        external_news_collect = result['stages']['external_news_collect']
        self.assertEqual(external_news_collect['status'], 'completed')
        self.assertEqual(
            external_news_collect['details']['items_persisted'],
            259,
        )
        external_news_enrich = result['stages']['external_news_enrich']
        self.assertEqual(external_news_enrich['status'], 'partial')
        self.assertEqual(
            external_news_enrich['details']['classified_items'],
            193,
        )

    def test_read_prefers_newer_recovery_history_over_older_blocked_eod_progress(self):
        market_date = '2026-10-07'
        recovered_at = '2026-10-07T13:56:20+00:00'
        enrich_history = _incomplete_official_stage(
            run_id=513,
            stage_id='external_news_enrich',
            market_date=market_date,
            status='partial',
            updated_at=recovered_at,
            succeeded=193,
            failed=66,
        )
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: _blocked_downstream_output_eod_run(
                market_date=market_date,
                updated_at='2026-10-07T12:34:45+00:00',
            ),
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'signal_features': _completed_official_stage(
                    run_id=512,
                    stage_id='signal_features',
                    market_date=market_date,
                    processed=2_703,
                    total=2_703,
                    updated_at='2026-10-07T13:46:36+00:00',
                ),
                'external_news_collect': _completed_official_stage(
                    run_id=513,
                    stage_id='external_news_collect',
                    market_date=market_date,
                    processed=201,
                    total=201,
                    updated_at=recovered_at,
                ),
                'external_news_enrich': enrich_history,
            },
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2_703,
                'updated_at': '2026-10-07T12:00:00+00:00',
            },
            external_news_counts_loader=lambda **_: {
                'items': 259,
                'symbols': 77,
                'classified_items': 193,
                'updated_at': '2026-10-07T12:00:00+00:00',
            },
        )

        result = service.read(market_date=market_date)

        self.assertEqual(result['stages']['signal_features']['status'], 'completed')
        self.assertEqual(
            result['stages']['external_news_collect']['status'],
            'completed',
        )
        self.assertEqual(
            result['stages']['external_news_enrich']['status'],
            'partial',
        )
        for stage_id in (
            'signal_features',
            'external_news_collect',
            'external_news_enrich',
        ):
            self.assertNotEqual(
                result['stages'][stage_id]['source_run_ids'],
                {'eod_run_id': 561},
            )

    def test_read_legacy_zero_semantics_do_not_authorize_next_open_without_parent_proof(
        self,
    ) -> None:
        market_date = '2026-10-07'
        blocked_at = '2026-10-07T13:00:00+00:00'
        recovered_at = '2026-10-07T21:07:57+00:00'
        unrelated_at = '2026-10-07T22:09:07+00:00'
        eod_run = _blocked_downstream_output_eod_run(
            market_date=market_date,
            updated_at=blocked_at,
        )
        for stage_id in (
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ):
            eod_run['run_metadata']['stage_progress'][stage_id] = {
                'status': 'blocked',
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'updated_at': blocked_at,
                'result_summary': {
                    'triggered': False,
                    'status': 'blocked',
                    'reason': 'summary_run_symbol_scope_mismatch',
                    'market_date': market_date,
                },
            }

        unrelated_history = _completed_official_stage(
            run_id=517,
            stage_id='external_news_enrich',
            market_date=market_date,
            processed=0,
            total=0,
            updated_at=unrelated_at,
        )
        unrelated_history['stage']['details'] = {
            'result_summary': {
                'triggered': False,
                'skipped': True,
                'safe_skip': True,
                'status': 'skipped',
                'reason': 'point_in_time_recovery_current_only_stage',
                'market_date': market_date,
                'stage': 'external_news_enrich',
            }
        }
        latest_official_run = {
            **unrelated_history['run'],
            'stages': {
                'external_news_enrich': unrelated_history['stage'],
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: eod_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: latest_official_run,
            official_stage_history_loader=lambda **_: {
                'signal_decider': _legacy_completed_signal_decider_stage(
                    market_date=market_date,
                    updated_at=recovered_at,
                ),
                'next_open_execution_plan': _completed_next_open_history_stage(
                    run_id=516,
                    market_date=market_date,
                    total=5,
                    updated_at=recovered_at,
                ),
                'signal_decider_evaluations': (
                    _legacy_completed_signal_decider_evaluations_stage(
                        market_date=market_date,
                        updated_at=recovered_at,
                    )
                ),
                'external_news_enrich': unrelated_history,
            },
        )

        result = service.read(market_date=market_date)

        expected_statuses = {
            'signal_decider': 'completed',
            'next_open_execution_plan': 'blocked',
            'signal_decider_evaluations': 'completed',
        }
        for stage_id, expected_status in expected_statuses.items():
            with self.subTest(stage_id=stage_id):
                stage = result['stages'][stage_id]
                self.assertEqual(stage['status'], expected_status)
        self.assertEqual(
            result['stages']['next_open_execution_plan']['details'][
                'status_repair_reason'
            ],
            'signal_decider_completion_proof_unavailable_or_mismatched',
        )

    def test_legacy_zero_count_decision_history_requires_stage_specific_semantic_proof(
        self,
    ) -> None:
        market_date = '2026-10-07'
        current = {
            'status': 'blocked',
            'processed': 0,
            'total': 0,
            'succeeded': 0,
            'failed': 0,
            'remaining': 0,
            'updated_at': '2026-10-07T13:00:00+00:00',
        }
        decider_factory = lambda: _legacy_completed_signal_decider_stage(
            market_date=market_date,
            updated_at='2026-10-07T14:00:00+00:00',
        )['stage']
        evaluation_factory = lambda: (
            _legacy_completed_signal_decider_evaluations_stage(
                market_date=market_date,
                updated_at='2026-10-07T14:00:00+00:00',
            )['stage']
        )

        cases: list[tuple[str, str, dict]] = []
        for marker, value in (
            ('safe_skip', True),
            ('skipped', True),
            ('triggered', False),
        ):
            decider = decider_factory()
            decider['details']['result_summary'][marker] = value
            cases.append((f'decider_{marker}', 'signal_decider', decider))
            evaluations = evaluation_factory()
            evaluations['details']['result_summary'][marker] = value
            cases.append(
                (
                    f'evaluations_{marker}',
                    'signal_decider_evaluations',
                    evaluations,
                )
            )

        empty_decider = decider_factory()
        empty_decider['details']['result_summary'] = {}
        cases.append(('decider_empty', 'signal_decider', empty_decider))
        malformed_evaluations = evaluation_factory()
        malformed_evaluations['details']['result_summary'] = {'summary': []}
        cases.append(
            (
                'evaluations_malformed',
                'signal_decider_evaluations',
                malformed_evaluations,
            )
        )
        for status in ('incomplete', 'partial', 'failed', 'pending'):
            decider = decider_factory()
            decider['details']['result_summary']['status'] = status
            cases.append((f'decider_{status}', 'signal_decider', decider))
            evaluations = evaluation_factory()
            evaluations['details']['result_summary']['summary']['status'] = status
            cases.append(
                (
                    f'evaluations_{status}',
                    'signal_decider_evaluations',
                    evaluations,
                )
            )

        pending_decider = decider_factory()
        pending_decider['details']['result_summary']['final_pending'] = 1
        cases.append(('decider_final_pending', 'signal_decider', pending_decider))
        failed_decider = decider_factory()
        failed_decider['details']['result_summary']['progress']['summary'][
            'failed_items'
        ] = 1
        cases.append(('decider_failed_items', 'signal_decider', failed_decider))
        role_pending_decider = decider_factory()
        role_pending_decider['details']['result_summary']['progress']['summary'][
            'role_progress'
        ]['signal-decider']['status'] = 'pending'
        cases.append(
            ('decider_role_pending', 'signal_decider', role_pending_decider)
        )
        raw_scope_mismatch = _legacy_completed_signal_decider_stage(
            market_date=market_date,
            updated_at='2026-10-07T14:00:00+00:00',
            expected=115,
            completed=65,
        )['stage']
        cases.append(
            ('decider_raw_65_of_115', 'signal_decider', raw_scope_mismatch)
        )
        evaluation_scope_mismatch = (
            _legacy_completed_signal_decider_evaluations_stage(
                market_date=market_date,
                updated_at='2026-10-07T14:00:00+00:00',
                outcomes_upserted=115,
                evaluations_upserted=65,
            )['stage']
        )
        cases.append(
            (
                'evaluations_raw_65_of_115',
                'signal_decider_evaluations',
                evaluation_scope_mismatch,
            )
        )

        for label, stage_id, candidate in cases:
            with self.subTest(label=label):
                self.assertFalse(
                    EODPipelineStatusService._recovery_state_is_newer(
                        stage_id=stage_id,
                        candidate=candidate,
                        current=current,
                        candidate_is_history=True,
                    )
                )

    def test_legacy_semantic_recovery_requires_strictly_newer_terminal_timestamp(
        self,
    ) -> None:
        market_date = '2026-10-07'
        timestamp_cases = (
            ('newer', '2026-10-07T14:00:00+00:00', True),
            ('equal', '2026-10-07T13:00:00+00:00', False),
            ('older', '2026-10-07T12:00:00+00:00', False),
            ('missing', None, False),
            ('invalid', 'not-a-timestamp', False),
        )
        current = {
            'status': 'blocked',
            'processed': 0,
            'total': 0,
            'succeeded': 0,
            'failed': 0,
            'remaining': 0,
            'updated_at': '2026-10-07T13:00:00+00:00',
        }
        for stage_id, factory in (
            ('signal_decider', _legacy_completed_signal_decider_stage),
            (
                'signal_decider_evaluations',
                _legacy_completed_signal_decider_evaluations_stage,
            ),
        ):
            for label, candidate_updated_at, expected in timestamp_cases:
                with self.subTest(stage_id=stage_id, label=label):
                    candidate = factory(
                        market_date=market_date,
                        updated_at=(
                            candidate_updated_at
                            or '2026-10-07T14:00:00+00:00'
                        ),
                    )['stage']
                    if candidate_updated_at is None:
                        candidate['updated_at'] = None
                        candidate['completed_at'] = None
                    self.assertEqual(
                        EODPipelineStatusService._recovery_state_is_newer(
                            stage_id=stage_id,
                            candidate=candidate,
                            current=current,
                            candidate_is_history=True,
                        ),
                        expected,
                    )

    def test_legacy_evaluation_proof_rejects_self_consistent_truncation_and_invalid_denominators(
        self,
    ) -> None:
        market_date = '2026-10-07'
        current = {
            'status': 'blocked',
            'processed': 0,
            'total': 3_840,
            'succeeded': 0,
            'failed': 0,
            'remaining': 3_840,
            'updated_at': '2026-10-07T13:00:00+00:00',
        }

        cases: list[tuple[str, dict]] = []
        truncated = _legacy_completed_signal_decider_evaluations_stage(
            market_date=market_date,
            updated_at='2026-10-07T14:00:00+00:00',
            outcomes_upserted=65,
            evaluations_upserted=65,
        )['stage']
        cases.append(('self_consistent_65_of_3840', truncated))

        for label, horizons in (
            ('missing_horizons', None),
            ('malformed_horizons', '1,3,5,10'),
            ('duplicate_horizons', [1, 1, 3, 5]),
        ):
            candidate = _legacy_completed_signal_decider_evaluations_stage(
                market_date=market_date,
                updated_at='2026-10-07T14:00:00+00:00',
            )['stage']
            request = candidate['details']['result_summary']['request']
            if horizons is None:
                request.pop('horizons')
            else:
                request['horizons'] = horizons
            cases.append((label, candidate))

        coverage_mismatch = (
            _legacy_completed_signal_decider_evaluations_stage(
                market_date=market_date,
                updated_at='2026-10-07T14:00:00+00:00',
            )['stage']
        )
        coverage_mismatch['details']['result_summary']['summary']['coverage'][
            'count'
        ] = 65
        cases.append(('coverage_count_mismatch', coverage_mismatch))

        coverage_components_mismatch = (
            _legacy_completed_signal_decider_evaluations_stage(
                market_date=market_date,
                updated_at='2026-10-07T14:00:00+00:00',
            )['stage']
        )
        coverage_components_mismatch['details']['result_summary']['summary'][
            'coverage'
        ]['insufficient_price_data_count'] = 5
        cases.append(
            ('coverage_components_mismatch', coverage_components_mismatch)
        )

        for label, candidate in cases:
            with self.subTest(label=label):
                self.assertFalse(
                    EODPipelineStatusService._recovery_state_is_newer(
                        stage_id='signal_decider_evaluations',
                        candidate=candidate,
                        current=current,
                        candidate_is_history=True,
                    )
                )

    def test_legacy_decider_proof_rejects_summary_shrunk_below_role_requests(
        self,
    ) -> None:
        candidate = _legacy_completed_signal_decider_stage(
            market_date='2026-10-07',
            updated_at='2026-10-07T14:00:00+00:00',
            expected=65,
            completed=65,
        )['stage']
        role_results = candidate['details']['result_summary']['progress'][
            'result'
        ]['roles']
        role_results['short-term-decider'].update(
            requested_count=80,
            completed_count=80,
        )
        role_results['signal-decider'].update(
            requested_count=80,
            completed_count=80,
        )

        self.assertFalse(
            EODPipelineStatusService._recovery_state_is_newer(
                stage_id='signal_decider',
                candidate=candidate,
                current={
                    'status': 'blocked',
                    'processed': 65,
                    'total': 160,
                    'succeeded': 65,
                    'failed': 0,
                    'remaining': 95,
                    'updated_at': '2026-10-07T13:00:00+00:00',
                },
                candidate_is_history=True,
            )
        )

    def test_feedback_history_counts_require_summary_identity_proof(
        self,
    ) -> None:
        candidate = _completed_official_stage(
            run_id=519,
            stage_id='agent_feedback_summaries',
            market_date='2026-10-07',
            processed=6,
            total=6,
        )['stage']

        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='agent_feedback_summaries',
                candidate=candidate,
            )
        )

        candidate = _completed_feedback_history_stage(
            run_id=519,
            market_date='2026-10-07',
            total=6,
        )['stage']

        self.assertTrue(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='agent_feedback_summaries',
                candidate=candidate,
            )
        )
        empty_role_scope_candidate = _completed_feedback_history_stage(
            run_id=520,
            market_date='2026-10-07',
            total=2,
            role_counts=(2, 0, 0),
        )['stage']
        empty_role_scope_proof = empty_role_scope_candidate['details'][
            'completion_scope_proof'
        ]
        self.assertEqual(
            empty_role_scope_proof['role_symbol_scopes'][
                SUMMARIZER_AGENT_NAMES[1]
            ],
            [],
        )
        self.assertTrue(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='agent_feedback_summaries',
                candidate=empty_role_scope_candidate,
            )
        )
        mismatched_identity = deepcopy(candidate)
        mismatched_identity['source_run_ids'] = {
            'run_id': 'feedback-cycle-2026-10-07-other'
        }
        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='agent_feedback_summaries',
                candidate=mismatched_identity,
            )
        )
        one_role_carries_total = deepcopy(candidate)
        role_progress = one_role_carries_total['details'][
            'result_summary'
        ]['progress']['summary']['role_progress']
        first_role = SUMMARIZER_AGENT_NAMES[0]
        role_progress.clear()
        role_progress[first_role] = {
            'role': first_role,
            'status': 'completed',
            'boundary_status': 'completed',
            'total_symbols': 6,
            'processed_symbols': 6,
            'completed_symbols': 6,
            'in_progress_symbols': 0,
            'pending_symbols': 0,
            'failed_items': 0,
            'retry_pending_symbols': 0,
        }
        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='agent_feedback_summaries',
                candidate=one_role_carries_total,
            )
        )
        self.assertFalse(
            EODPipelineStatusService._recovery_state_is_newer(
                stage_id='agent_feedback_summaries',
                candidate=candidate,
                current={
                    'status': 'blocked',
                    'processed': 6,
                    'total': 9,
                    'succeeded': 6,
                    'failed': 0,
                    'remaining': 3,
                    'updated_at': '2026-05-28T07:30:00+00:00',
                },
                candidate_is_history=True,
            )
        )
        same_scope_current = deepcopy(candidate)
        same_scope_current.update(
            status='blocked',
            updated_at='2026-05-28T07:30:00+00:00',
        )
        self.assertTrue(
            EODPipelineStatusService._recovery_state_is_newer(
                stage_id='agent_feedback_summaries',
                candidate=candidate,
                current=same_scope_current,
                candidate_is_history=True,
            )
        )
        proofless_blocked_current = deepcopy(same_scope_current)
        proofless_blocked_current['details'].pop(
            'completion_scope_proof'
        )
        self.assertFalse(
            EODPipelineStatusService._recovery_state_is_newer(
                stage_id='agent_feedback_summaries',
                candidate=candidate,
                current=proofless_blocked_current,
                candidate_is_history=True,
            )
        )
        for field, replacement in (
            ('source_run_id', 'feedback-cycle-2026-10-07-other'),
            ('knowledge_cutoff_at', '2026-10-07T09:00:00+00:00'),
        ):
            with self.subTest(feedback_authority_field=field):
                different_authority_current = deepcopy(
                    same_scope_current
                )
                authority_proof = different_authority_current[
                    'details'
                ]['completion_scope_proof']
                authority_proof[field] = replacement
                authority_proof['proof_digest'] = (
                    DailyPipelineStageService
                    ._stage_completion_scope_proof_digest(
                        authority_proof
                    )
                )
                self.assertFalse(
                    EODPipelineStatusService._recovery_state_is_newer(
                        stage_id='agent_feedback_summaries',
                        candidate=candidate,
                        current=different_authority_current,
                        candidate_is_history=True,
                    )
                )
        different_role_scope_current = deepcopy(same_scope_current)
        different_scope_proof = different_role_scope_current['details'][
            'completion_scope_proof'
        ]
        first_role, second_role = SUMMARIZER_AGENT_NAMES[:2]
        role_scopes = different_scope_proof['role_symbol_scopes']
        role_scopes[first_role], role_scopes[second_role] = (
            role_scopes[second_role],
            role_scopes[first_role],
        )
        different_scope_proof['role_scope_manifest_fingerprint'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(
                {
                    'requested_symbols': different_scope_proof[
                        'requested_symbols'
                    ],
                    'role_symbol_scopes': role_scopes,
                }
            )
        )
        different_scope_proof['proof_digest'] = (
            DailyPipelineStageService
            ._stage_completion_scope_proof_digest(different_scope_proof)
        )
        self.assertFalse(
            EODPipelineStatusService._recovery_state_is_newer(
                stage_id='agent_feedback_summaries',
                candidate=candidate,
                current=different_role_scope_current,
                candidate_is_history=True,
            )
        )

    def test_next_open_history_counts_require_bound_scope_proof(
        self,
    ) -> None:
        candidate = _completed_official_stage(
            run_id=519,
            stage_id='next_open_execution_plan',
            market_date='2026-10-07',
            processed=2,
            total=2,
        )['stage']

        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='next_open_execution_plan',
                candidate=candidate,
            )
        )

        candidate = _completed_next_open_history_stage(
            run_id=519,
            market_date='2026-10-07',
            total=2,
        )['stage']

        self.assertTrue(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='next_open_execution_plan',
                candidate=candidate,
            )
        )
        mismatched_scope = deepcopy(candidate)
        mismatched_scope['details']['nonblocking_scope_proof'].update(
            item_count=3,
            blocked_item_count=2,
        )
        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='next_open_execution_plan',
                candidate=mismatched_scope,
            )
        )
        unrelated_decision_run = deepcopy(candidate)
        unrelated_decision_run['details']['nonblocking_scope_proof'][
            'active_scope'
        ][0]['decision_run_id'] = 'unrelated-child-run'
        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='next_open_execution_plan',
                candidate=unrelated_decision_run,
            )
        )
        unrelated_parent = deepcopy(candidate)
        unrelated_parent['source_run_ids'][
            'parent_signal_decider_run_id'
        ] = 'unrelated-parent-run'
        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='next_open_execution_plan',
                candidate=unrelated_parent,
            )
        )
        unrelated_current = _completed_next_open_history_stage(
            run_id=520,
            market_date='2026-10-07',
            total=2,
            updated_at='2026-05-28T07:30:00+00:00',
        )['stage']
        unrelated_current['status'] = 'blocked'
        self.assertFalse(
            EODPipelineStatusService._recovery_state_is_newer(
                stage_id='next_open_execution_plan',
                candidate=candidate,
                current=unrelated_current,
                candidate_is_history=True,
            )
        )
        proofless_blocked_current = deepcopy(candidate)
        proofless_blocked_current.update(
            status='blocked',
            updated_at='2026-05-28T07:30:00+00:00',
        )
        proofless_blocked_current['details'].pop(
            'completion_scope_proof'
        )
        self.assertFalse(
            EODPipelineStatusService._recovery_state_is_newer(
                stage_id='next_open_execution_plan',
                candidate=candidate,
                current=proofless_blocked_current,
                candidate_is_history=True,
            )
        )
        for field, replacement in (
            ('decision_snapshot_id', 99),
            ('authority_fingerprint', 'regenerated-authority'),
            ('generation_fingerprint', 'regenerated-generation'),
            ('active', False),
        ):
            with self.subTest(next_open_authority_field=field):
                regenerated_current = deepcopy(candidate)
                regenerated_current.update(
                    status='blocked',
                    updated_at='2026-05-28T07:30:00+00:00',
                )
                regenerated_proof = regenerated_current['details'][
                    'completion_scope_proof'
                ]
                included_entry = next(
                    entry
                    for entry in regenerated_proof[
                        'decision_partition'
                    ]
                    if entry['disposition'] == 'included'
                )
                if field == 'active':
                    included_entry['included_items'][0][
                        'handoff_active'
                    ] = False
                    included_entry['included_items'][0][
                        'exec_monitoring_active'
                    ] = False
                    regenerated_proof['active_item_count'] -= 1
                    regenerated_proof['blocked_item_count'] += 1
                else:
                    included_entry[field] = replacement
                regenerated_proof['proof_digest'] = (
                    DailyPipelineStageService
                    ._stage_completion_scope_proof_digest(
                        regenerated_proof
                    )
                )
                self.assertFalse(
                    EODPipelineStatusService._recovery_state_is_newer(
                        stage_id='next_open_execution_plan',
                        candidate=candidate,
                        current=regenerated_current,
                        candidate_is_history=True,
                    )
                )

    def test_zero_item_next_open_history_requires_full_non_actionable_partition(
        self,
    ) -> None:
        item = _completed_zero_item_next_open_history_stage(
            run_id=521,
            market_date='2026-10-07',
        )
        candidate = item['stage']

        self.assertTrue(
            EODPipelineStatusService
            ._history_stage_has_authoritative_completion(
                stage_id='next_open_execution_plan',
                candidate=candidate,
            )
        )
        built = EODPipelineStatusService()._build_official_stage(
            stage_id='next_open_execution_plan',
            market_date='2026-10-07',
            official_run=item['run'],
            official_stage=candidate,
            fallback_stage={
                'status': 'pending',
                'details': {},
                'source_run_ids': {},
            },
        )
        self.assertEqual(built['status'], 'completed')

        missing_partition = deepcopy(candidate)
        missing_partition['details'].pop('completion_scope_proof')
        self.assertFalse(
            EODPipelineStatusService
            ._history_stage_has_authoritative_completion(
                stage_id='next_open_execution_plan',
                candidate=missing_partition,
            )
        )

    def test_decision_history_cannot_overlay_different_authoritative_denominator(
        self,
    ) -> None:
        cases = (
            (
                'signal_decider',
                _legacy_completed_signal_decider_stage(
                    market_date='2026-10-07',
                    updated_at='2026-10-07T14:00:00+00:00',
                    expected=130,
                    completed=130,
                )['stage'],
                230,
            ),
            (
                'signal_decider_evaluations',
                _legacy_completed_signal_decider_evaluations_stage(
                    market_date='2026-10-07',
                    updated_at='2026-10-07T14:00:00+00:00',
                )['stage'],
                8_000,
            ),
        )
        for stage_id, candidate, current_total in cases:
            with self.subTest(stage_id=stage_id):
                self.assertFalse(
                    EODPipelineStatusService._recovery_state_is_newer(
                        stage_id=stage_id,
                        candidate=candidate,
                        current={
                            'status': 'blocked',
                            'processed': 0,
                            'total': current_total,
                            'succeeded': 0,
                            'failed': 0,
                            'remaining': current_total,
                            'updated_at': (
                                '2026-10-07T13:00:00+00:00'
                            ),
                            'details': {},
                        },
                        candidate_is_history=True,
                    )
                )

    def test_legacy_decision_history_cannot_claim_same_size_current_cohort(
        self,
    ) -> None:
        cases = (
            (
                'signal_decider',
                _legacy_completed_signal_decider_stage(
                    market_date='2026-10-07',
                    updated_at='2026-10-07T14:00:00+00:00',
                    expected=130,
                    completed=130,
                )['stage'],
                130,
            ),
            (
                'signal_decider_evaluations',
                _legacy_completed_signal_decider_evaluations_stage(
                    market_date='2026-10-07',
                    updated_at='2026-10-07T14:00:00+00:00',
                )['stage'],
                3_840,
            ),
        )
        for stage_id, candidate, current_total in cases:
            with self.subTest(stage_id=stage_id):
                self.assertFalse(
                    EODPipelineStatusService._recovery_state_is_newer(
                        stage_id=stage_id,
                        candidate=candidate,
                        current={
                            'status': 'blocked',
                            'processed': 0,
                            'total': current_total,
                            'succeeded': 0,
                            'failed': 0,
                            'remaining': current_total,
                            'updated_at': (
                                '2026-10-07T13:00:00+00:00'
                            ),
                            'details': {
                                'requested_symbols': ['999999'],
                            },
                        },
                        candidate_is_history=True,
                    )
                )

    def test_real_shaped_legacy_evaluation_wrapper_fails_closed_without_proof(
        self,
    ) -> None:
        item = _legacy_wrapped_signal_decider_evaluations_stage(
            market_date='2026-09-30',
            updated_at='2026-10-01T16:55:34+00:00',
        )
        candidate = item['stage']

        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='signal_decider_evaluations',
                candidate=candidate,
            )
        )
        built_stage = EODPipelineStatusService()._build_official_stage(
            stage_id='signal_decider_evaluations',
            market_date='2026-09-30',
            official_run=item['run'],
            official_stage=candidate,
            fallback_stage={
                'status': 'pending',
                'details': {},
                'source_run_ids': {},
            },
        )

        self.assertEqual(built_stage['status'], 'partial')
        self.assertEqual(built_stage['processed'], 2_843)
        self.assertEqual(built_stage['total'], 2_843)
        self.assertEqual(built_stage['succeeded'], 2_843)
        self.assertEqual(built_stage['failed'], 0)

    def test_malformed_legacy_evaluation_wrapper_is_not_authoritative(
        self,
    ) -> None:
        item = _legacy_wrapped_signal_decider_evaluations_stage(
            market_date='2026-09-30',
            updated_at='2026-10-01T16:55:34+00:00',
        )
        candidate = item['stage']
        candidate['details']['result_summary']['summary'][
            'evaluation_refresh'
        ]['coverage']['computed_count'] = 2_842

        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='signal_decider_evaluations',
                candidate=candidate,
            )
        )
        built_stage = EODPipelineStatusService()._build_official_stage(
            stage_id='signal_decider_evaluations',
            market_date='2026-09-30',
            official_run=item['run'],
            official_stage=candidate,
            fallback_stage={
                'status': 'pending',
                'details': {},
                'source_run_ids': {},
            },
        )

        self.assertEqual(built_stage['status'], 'partial')

    def test_legacy_evaluation_wrapper_rejects_malformed_optional_controls(
        self,
    ) -> None:
        cases = (
            ('result_warnings', ('warnings',), 'warn'),
            ('summary_warning_count', ('summary', 'warning_count'), '0'),
            (
                'calendar_primary_horizon_count',
                ('calendar_refresh', 'summary', 'primary_horizon_count'),
                '4',
            ),
        )
        for label, path, value in cases:
            with self.subTest(label=label):
                item = _legacy_wrapped_signal_decider_evaluations_stage(
                    market_date='2026-09-30',
                    updated_at='2026-10-01T16:55:34+00:00',
                )
                candidate = item['stage']
                payload = candidate['details']['result_summary']
                cursor = payload
                for key in path[:-1]:
                    cursor = cursor[key]
                cursor[path[-1]] = value

                self.assertFalse(
                    EODPipelineStatusService._history_stage_has_authoritative_completion(
                        stage_id='signal_decider_evaluations',
                        candidate=candidate,
                    )
                )
                built_stage = EODPipelineStatusService()._build_official_stage(
                    stage_id='signal_decider_evaluations',
                    market_date='2026-09-30',
                    official_run=item['run'],
                    official_stage=candidate,
                    fallback_stage={
                        'status': 'pending',
                        'details': {},
                        'source_run_ids': {},
                    },
                )
                self.assertEqual(built_stage['status'], 'partial')

    def test_canonical_evaluation_proof_rejects_malformed_coverage_and_warnings(
        self,
    ) -> None:
        delete = object()
        cases = (
            ('coverage_removed', ('summary', 'coverage'), delete),
            (
                'coverage_components_removed',
                ('summary', 'coverage'),
                {'count': 3_840},
            ),
            (
                'coverage_component_partial',
                ('summary', 'coverage', 'computed_count'),
                delete,
            ),
            ('warnings_scalar', ('summary', 'warnings'), 'warn'),
            ('warning_count_string', ('summary', 'warning_count'), '0'),
        )
        for label, path, value in cases:
            with self.subTest(label=label):
                item = _legacy_completed_signal_decider_evaluations_stage(
                    market_date='2026-10-07',
                    updated_at='2026-10-07T14:00:00+00:00',
                )
                candidate = item['stage']
                payload = candidate['details']['result_summary']
                cursor = payload
                for key in path[:-1]:
                    cursor = cursor[key]
                if value is delete:
                    cursor.pop(path[-1])
                else:
                    cursor[path[-1]] = value

                self.assertFalse(
                    EODPipelineStatusService._history_stage_has_authoritative_completion(
                        stage_id='signal_decider_evaluations',
                        candidate=candidate,
                    )
                )
                built_stage = EODPipelineStatusService()._build_official_stage(
                    stage_id='signal_decider_evaluations',
                    market_date='2026-10-07',
                    official_run=item['run'],
                    official_stage=candidate,
                    fallback_stage={
                        'status': 'pending',
                        'details': {},
                        'source_run_ids': {},
                    },
                )
                self.assertEqual(built_stage['status'], 'partial')

    def test_canonical_evaluation_proof_requires_root_components_to_match_coverage(
        self,
    ) -> None:
        delete = object()
        cases = (
            ('root_component_missing', 'computed_count', delete),
            ('root_component_string', 'pending_maturity_count', '1550'),
            ('root_component_mismatch', 'insufficient_price_data_count', 5),
        )
        for label, key, value in cases:
            with self.subTest(label=label):
                item = _legacy_completed_signal_decider_evaluations_stage(
                    market_date='2026-10-07',
                    updated_at='2026-10-07T14:00:00+00:00',
                )
                candidate = item['stage']
                summary = candidate['details']['result_summary']['summary']
                if value is delete:
                    summary.pop(key)
                else:
                    summary[key] = value

                self.assertFalse(
                    EODPipelineStatusService._history_stage_has_authoritative_completion(
                        stage_id='signal_decider_evaluations',
                        candidate=candidate,
                    )
                )
                built_stage = EODPipelineStatusService()._build_official_stage(
                    stage_id='signal_decider_evaluations',
                    market_date='2026-10-07',
                    official_run=item['run'],
                    official_stage=candidate,
                    fallback_stage={
                        'status': 'pending',
                        'details': {},
                        'source_run_ids': {},
                    },
                )
                self.assertEqual(built_stage['status'], 'partial')

    def test_canonical_evaluation_proof_requires_consistent_grade_and_pending_counts(
        self,
    ) -> None:
        cases = (
            ('graded_count_mismatch', 'graded_count', 1),
            ('graded_count_string', 'graded_count', '2284'),
            ('pending_count_mismatch', 'pending_count', 1),
            ('pending_count_string', 'pending_count', '1556'),
        )
        for label, key, value in cases:
            with self.subTest(label=label):
                item = _legacy_completed_signal_decider_evaluations_stage(
                    market_date='2026-10-07',
                    updated_at='2026-10-07T14:00:00+00:00',
                )
                candidate = item['stage']
                candidate['details']['result_summary']['summary'][key] = value

                self.assertFalse(
                    EODPipelineStatusService._history_stage_has_authoritative_completion(
                        stage_id='signal_decider_evaluations',
                        candidate=candidate,
                    )
                )
                built_stage = EODPipelineStatusService()._build_official_stage(
                    stage_id='signal_decider_evaluations',
                    market_date='2026-10-07',
                    official_run=item['run'],
                    official_stage=candidate,
                    fallback_stage={
                        'status': 'pending',
                        'details': {},
                        'source_run_ids': {},
                    },
                )
                self.assertEqual(built_stage['status'], 'partial')

    def test_canonical_decider_proof_rejects_malformed_pending_counter(
        self,
    ) -> None:
        item = _legacy_completed_signal_decider_stage(
            market_date='2026-10-07',
            updated_at='2026-10-07T14:00:00+00:00',
        )
        candidate = item['stage']
        candidate['details']['result_summary']['progress']['summary'][
            'role_progress'
        ]['signal-decider']['pending_symbols'] = 'bad'

        self.assertFalse(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='signal_decider',
                candidate=candidate,
            )
        )
        built_stage = EODPipelineStatusService()._build_official_stage(
            stage_id='signal_decider',
            market_date='2026-10-07',
            official_run=item['run'],
            official_stage=candidate,
            fallback_stage={
                'status': 'pending',
                'details': {},
                'source_run_ids': {},
            },
        )

        self.assertEqual(built_stage['status'], 'partial')

    def test_canonical_decider_proof_accepts_complete_reused_recovery_cohort(
        self,
    ) -> None:
        item = _legacy_completed_signal_decider_stage(
            market_date='2026-10-07',
            updated_at='2026-10-07T14:00:00+00:00',
        )
        candidate = item['stage']
        summary = candidate['details']['result_summary']['progress']['summary']
        summary['computed_feedback_count'] = 2
        summary['reused_feedback_count'] = 158
        summary['stale_feedback_count'] = 0
        summary['role_progress']['short-term-decider'].update(
            computed_symbols=0,
            reused_symbols=80,
            stale_symbols=0,
        )
        summary['role_progress']['signal-decider'].update(
            computed_symbols=2,
            reused_symbols=78,
            stale_symbols=0,
        )

        self.assertTrue(
            EODPipelineStatusService._history_stage_has_authoritative_completion(
                stage_id='signal_decider',
                candidate=candidate,
            )
        )
        built_stage = EODPipelineStatusService()._build_official_stage(
            stage_id='signal_decider',
            market_date='2026-10-07',
            official_run=item['run'],
            official_stage=candidate,
            fallback_stage={
                'status': 'pending',
                'details': {},
                'source_run_ids': {},
            },
        )

        self.assertEqual(built_stage['status'], 'completed')

    def test_canonical_decider_proof_rejects_invalid_reuse_accounting(
        self,
    ) -> None:
        cases = (
            ('missing_reused', None, 0),
            ('string_reused', '158', 0),
            ('overcounted_reused', 159, 0),
            ('stale_reuse', 158, 1),
        )
        for label, reused, stale in cases:
            with self.subTest(label=label):
                item = _legacy_completed_signal_decider_stage(
                    market_date='2026-10-07',
                    updated_at='2026-10-07T14:00:00+00:00',
                )
                candidate = item['stage']
                summary = candidate['details']['result_summary']['progress'][
                    'summary'
                ]
                summary['computed_feedback_count'] = 2
                summary['reused_feedback_count'] = reused
                summary['stale_feedback_count'] = stale

                self.assertFalse(
                    EODPipelineStatusService._history_stage_has_authoritative_completion(
                        stage_id='signal_decider',
                        candidate=candidate,
                    )
                )

    def test_canonical_decider_proof_rejects_contradictory_terminal_controls(
        self,
    ) -> None:
        cases = (
            ('pending_failure_count', 'pending_failure_count', '0'),
            ('pending_feedback_count', 'pending_feedback_count', 1),
            ('buffered_failure_count', 'buffered_failure_count', '0'),
            ('flushed_failure_count', 'flushed_failure_count', 1),
            ('failure_groups', 'failure_groups', {'timeout': 1}),
            ('cycle_status', 'cycle_status', 'pending'),
            ('completed_roles', 'completed_roles', ['signal-decider']),
            ('remaining_roles', 'remaining_roles', ['signal-decider']),
            ('is_live', 'is_live', True),
            ('current_role', 'current_role', 'signal-decider'),
            ('computed_feedback_count', 'computed_feedback_count', 159),
            ('terminal_feedback_count', 'terminal_feedback_count', 159),
        )
        for label, key, value in cases:
            with self.subTest(label=label):
                item = _legacy_completed_signal_decider_stage(
                    market_date='2026-10-07',
                    updated_at='2026-10-07T14:00:00+00:00',
                )
                candidate = item['stage']
                summary = candidate['details']['result_summary']['progress'][
                    'summary'
                ]
                summary[key] = value

                self.assertFalse(
                    EODPipelineStatusService._history_stage_has_authoritative_completion(
                        stage_id='signal_decider',
                        candidate=candidate,
                    )
                )
                built_stage = EODPipelineStatusService()._build_official_stage(
                    stage_id='signal_decider',
                    market_date='2026-10-07',
                    official_run=item['run'],
                    official_stage=candidate,
                    fallback_stage={
                        'status': 'pending',
                        'details': {},
                        'source_run_ids': {},
                    },
                )
                self.assertEqual(built_stage['status'], 'partial')

    def test_future_decision_counts_cannot_bypass_semantic_history_authority(
        self,
    ) -> None:
        decider_without_role_results = {
            'status': 'completed',
            'completed': True,
            'final_pending': 0,
            'progress': {
                'summary': {
                    'run_status': 'completed',
                    'run_table_status': 'completed',
                    'expected_feedback_count': 65,
                    'completed_feedback_count': 65,
                    'failed_items': 0,
                    'role_progress': {
                        'signal-decider': {
                            'status': 'completed',
                            'boundary_status': 'completed',
                            'total_symbols': 65,
                            'processed_symbols': 65,
                            'completed_symbols': 65,
                            'failed_items': 0,
                        },
                    },
                },
            },
        }
        decider_with_non_integer_request = (
            _legacy_completed_signal_decider_stage(
                market_date='2026-10-07',
                updated_at='2026-10-07T15:00:00+00:00',
                expected=65,
                completed=65,
            )['stage']['details']['result_summary']
        )
        decider_with_non_integer_request['progress']['result']['roles'][
            'short-term-decider'
        ]['requested_count'] = '32'

        evaluation_cases: list[tuple[str, dict]] = []
        for label, horizons in (
            ('missing_horizons', None),
            ('malformed_horizons', '1,3,5,10'),
            ('duplicate_horizons', [1, 1, 3, 5]),
        ):
            result = {
                'request': {},
                'summary': {
                    'status': 'completed',
                    'decisions_considered': 960,
                    'outcomes_upserted': 65,
                    'evaluations_upserted': 65,
                    'coverage': {
                        'count': 65,
                        'computed_count': 65,
                        'pending_maturity_count': 0,
                        'insufficient_price_data_count': 0,
                        'not_computed_count': 0,
                    },
                    'not_computed_count': 0,
                    'warning_count': 0,
                    'warnings': [],
                },
            }
            if horizons is not None:
                result['request']['horizons'] = horizons
            evaluation_cases.append((label, result))

        evaluation_cases.append(
            (
                'coverage_count_mismatch',
                {
                    'request': {'horizons': [1, 3, 5, 10]},
                    'summary': {
                        'status': 'completed',
                        'decisions_considered': 960,
                        'outcomes_upserted': 3_840,
                        'evaluations_upserted': 3_840,
                        'coverage': {
                            'count': 65,
                            'computed_count': 65,
                            'pending_maturity_count': 0,
                            'insufficient_price_data_count': 0,
                            'not_computed_count': 0,
                        },
                        'not_computed_count': 0,
                        'warning_count': 0,
                        'warnings': [],
                    },
                },
            )
        )
        non_integer_decisions = (
            _legacy_completed_signal_decider_evaluations_stage(
                market_date='2026-10-07',
                updated_at='2026-10-07T15:00:00+00:00',
            )['stage']['details']['result_summary']
        )
        non_integer_decisions['summary']['decisions_considered'] = '960'
        evaluation_cases.append(
            ('non_integer_decisions_considered', non_integer_decisions)
        )
        non_integer_coverage = (
            _legacy_completed_signal_decider_evaluations_stage(
                market_date='2026-10-07',
                updated_at='2026-10-07T15:00:00+00:00',
            )['stage']['details']['result_summary']
        )
        non_integer_coverage['summary']['coverage']['count'] = '3840'
        evaluation_cases.append(
            ('non_integer_coverage_count', non_integer_coverage)
        )

        cases = [
            (
                'decider_missing_role_results',
                'signal_decider',
                decider_without_role_results,
            ),
            (
                'decider_non_integer_requested_count',
                'signal_decider',
                decider_with_non_integer_request,
            ),
            *(
                (label, 'signal_decider_evaluations', result)
                for label, result in evaluation_cases
            ),
        ]
        for label, stage_id, result in cases:
            with self.subTest(label=label):
                counts = DailyPipelineStageService._extract_stage_counts(
                    stage_id=stage_id,
                    result=result,
                )
                exact_complete = bool(
                    counts['total'] > 0
                    and counts['processed'] == counts['total']
                    and counts['succeeded'] == counts['total']
                    and counts['failed'] == 0
                )
                self.assertFalse(exact_complete)
                self.assertTrue(
                    counts['failed'] > 0
                    or counts['total'] > counts['processed']
                )

                if stage_id == 'signal_decider':
                    forged_complete_count = result['progress']['summary'][
                        'completed_feedback_count'
                    ]
                else:
                    forged_complete_count = result['summary'][
                        'evaluations_upserted'
                    ]
                candidate = _completed_official_stage(
                    run_id=518,
                    stage_id=stage_id,
                    market_date='2026-10-07',
                    processed=forged_complete_count,
                    total=forged_complete_count,
                    updated_at='2026-10-07T15:00:00+00:00',
                )['stage']
                candidate.update(
                    succeeded=forged_complete_count,
                    failed=0,
                    remaining=0,
                )
                candidate['details'] = {'result_summary': result}
                self.assertFalse(
                    EODPipelineStatusService._history_stage_has_authoritative_completion(
                        stage_id=stage_id,
                        candidate=candidate,
                    )
                )
                built_stage = EODPipelineStatusService()._build_official_stage(
                    stage_id=stage_id,
                    market_date='2026-10-07',
                    official_run={
                        'id': 518,
                        'trigger': 'manual_api',
                        'updated_at': '2026-10-07T15:00:00+00:00',
                        'run_metadata': {},
                    },
                    official_stage=candidate,
                    fallback_stage={
                        'status': 'pending',
                        'details': {},
                        'source_run_ids': {},
                    },
                )
                self.assertNotEqual(built_stage['status'], 'completed')

    def test_read_prefers_newer_completed_decision_history_over_stale_blocked_overlays(self):
        market_date = '2026-10-07'
        for stage_id in ('signal_decider', 'next_open_execution_plan'):
            for stale_overlay in ('live_progress', 'latest_official_run'):
                with self.subTest(stage_id=stage_id, stale_overlay=stale_overlay):
                    completed_history = _completed_official_stage(
                        run_id=516,
                        stage_id=stage_id,
                        market_date=market_date,
                        processed=115,
                        total=115,
                        updated_at='2026-10-07T14:00:00+00:00',
                    )
                    expected_total = 115
                    if stage_id == 'signal_decider':
                        completed_history = (
                            _completed_signal_decider_history_stage(
                                run_id=516,
                                market_date=market_date,
                                symbol_count=58,
                                updated_at=(
                                    '2026-10-07T14:00:00+00:00'
                                ),
                            )
                        )
                        expected_total = 116
                    elif stage_id == 'next_open_execution_plan':
                        completed_history = _completed_next_open_history_stage(
                            run_id=516,
                            market_date=market_date,
                            total=115,
                            updated_at='2026-10-07T14:00:00+00:00',
                        )
                    blocked = _incomplete_official_stage(
                        run_id=511,
                        stage_id=stage_id,
                        market_date=market_date,
                        status='blocked',
                        updated_at='2026-10-07T13:00:00+00:00',
                        succeeded=0,
                        failed=0,
                    )
                    blocked['stage']['details'] = {
                        'result_summary': {
                            'triggered': False,
                            'status': 'blocked',
                            'reason': 'inherited_downstream_recovery_pending',
                        }
                    }
                    eod_run = None
                    latest_official_run = None
                    if stale_overlay == 'live_progress':
                        eod_run = _blocked_downstream_output_eod_run(
                            market_date=market_date,
                            updated_at='2026-10-07T13:00:00+00:00',
                        )
                        eod_run['run_metadata']['stage_progress'][stage_id] = {
                            **blocked['stage'],
                            'status': 'blocked',
                        }
                    else:
                        latest_official_run = {
                            **blocked['run'],
                            'stages': {stage_id: blocked['stage']},
                        }
                    stage_history = {stage_id: completed_history}
                    if stage_id == 'next_open_execution_plan':
                        stage_history['signal_decider'] = (
                            _completed_signal_decider_history_stage(
                                run_id=516,
                                market_date=market_date,
                                symbol_count=115,
                                updated_at=(
                                    '2026-10-07T13:59:00+00:00'
                                ),
                            )
                        )
                    service = EODPipelineStatusService(
                        market_state_loader=lambda: _FakeMarketState(),
                        pykrx_run_loader=lambda **_: eod_run,
                        pykrx_stage_counts_loader=lambda *_: {},
                        live_symbol_count_loader=lambda: 2_703,
                        fundamentals_refresh_loader=lambda **_: None,
                        official_run_loader=lambda **_: latest_official_run,
                        official_stage_history_loader=lambda **_: stage_history,
                    )

                    result = service.read(market_date=market_date)

                    stage = result['stages'][stage_id]
                    self.assertEqual(stage['status'], 'completed')
                    self.assertEqual(stage['processed'], expected_total)
                    self.assertEqual(stage['total'], expected_total)
                    self.assertEqual(
                        stage['details']['progress_source'],
                        'official_stage_history',
                    )

    def test_read_keeps_newer_blocked_eod_progress_over_older_persisted_outputs(self):
        market_date = '2026-10-07'
        old_history = _completed_official_stage(
            run_id=509,
            stage_id='signal_features',
            market_date=market_date,
            processed=2_703,
            total=2_703,
            updated_at='2026-10-07T12:15:00+00:00',
        )
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: _blocked_downstream_output_eod_run(
                market_date=market_date,
                updated_at='2026-10-07T13:00:00+00:00',
            ),
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'signal_features': old_history,
            },
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2_703,
                'updated_at': '2026-10-07T12:00:00+00:00',
            },
            external_news_counts_loader=lambda **_: {
                'items': 259,
                'symbols': 77,
                'classified_items': 193,
                'updated_at': '2026-10-07T12:00:00+00:00',
            },
        )

        result = service.read(market_date=market_date)

        for stage_id in (
            'signal_features',
            'external_news_collect',
            'external_news_enrich',
        ):
            stage = result['stages'][stage_id]
            self.assertEqual(stage['status'], 'blocked')
            self.assertEqual(stage['source_run_ids'], {'eod_run_id': 561})
            self.assertEqual(
                stage['details']['progress_source'],
                'live_run_metadata',
            )

    def test_read_prefers_newer_persisted_output_over_older_blocked_history(self):
        market_date = '2026-10-07'
        blocked_history = _incomplete_official_stage(
            run_id=510,
            stage_id='signal_features',
            market_date=market_date,
            status='blocked',
            updated_at='2026-10-07T12:34:45+00:00',
            succeeded=0,
            failed=0,
        )
        blocked_history['stage']['details'] = {
            'result_summary': {
                'triggered': False,
                'status': 'blocked',
                'reason': 'inherited_downstream_recovery_pending',
            }
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'signal_features': blocked_history,
            },
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2_703,
                'updated_at': '2026-10-07T13:46:33+00:00',
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['signal_features']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['details']['rows_persisted'], 2_703)
        self.assertEqual(
            stage['details']['mode'],
            'derived_from_daily_signal_features',
        )

    def test_read_prefers_newer_recovery_history_over_older_latest_official_stage(self):
        market_date = '2026-10-07'
        recovered_history = _completed_official_stage(
            run_id=512,
            stage_id='signal_features',
            market_date=market_date,
            processed=2_703,
            total=2_703,
            updated_at='2026-10-07T14:00:00+00:00',
        )
        older_blocked = _incomplete_official_stage(
            run_id=511,
            stage_id='signal_features',
            market_date=market_date,
            status='blocked',
            updated_at='2026-10-07T13:00:00+00:00',
            succeeded=0,
            failed=0,
        )
        older_blocked['stage']['details'] = {
            'result_summary': {
                'triggered': False,
                'status': 'blocked',
                'reason': 'declared_dependencies_incomplete',
            }
        }
        latest_official_run = {
            **older_blocked['run'],
            'stages': {'signal_features': older_blocked['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: latest_official_run,
            official_stage_history_loader=lambda **_: {
                'signal_features': recovered_history,
            },
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2_703,
                'updated_at': '2026-10-07T12:00:00+00:00',
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['signal_features']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['details']['progress_source'], 'official_stage_history')

    def test_read_keeps_blocked_progress_when_output_freshness_is_not_proven(self):
        market_date = '2026-10-07'
        cases = (
            ('equal', '2026-10-07T13:00:00+00:00', '2026-10-07T13:00:00+00:00'),
            ('persisted_missing', None, '2026-10-07T13:00:00+00:00'),
            ('blocked_missing', '2026-10-07T13:00:00+00:00', None),
            ('persisted_invalid', 'not-a-timestamp', '2026-10-07T13:00:00+00:00'),
            ('blocked_invalid', '2026-10-07T13:00:00+00:00', 'not-a-timestamp'),
        )
        for label, persisted_updated_at, blocked_updated_at in cases:
            with self.subTest(label=label):
                service = EODPipelineStatusService(
                    market_state_loader=lambda: _FakeMarketState(),
                    pykrx_run_loader=lambda **_: _blocked_downstream_output_eod_run(
                        market_date=market_date,
                        updated_at=blocked_updated_at,
                    ),
                    pykrx_stage_counts_loader=lambda *_: {},
                    live_symbol_count_loader=lambda: 2_703,
                    fundamentals_refresh_loader=lambda **_: None,
                    official_run_loader=lambda **_: None,
                    official_stage_history_loader=lambda **_: {},
                    signal_feature_counts_loader=lambda **_: {
                        'persisted': 2_703,
                        'updated_at': persisted_updated_at,
                    },
                )

                result = service.read(market_date=market_date)

                stage = result['stages']['signal_features']
                self.assertEqual(stage['status'], 'blocked')
                self.assertEqual(stage['source_run_ids'], {'eod_run_id': 561})

    def test_read_does_not_use_started_at_as_terminal_blocked_freshness(self):
        market_date = '2026-10-07'
        blocked_history = _incomplete_official_stage(
            run_id=510,
            stage_id='signal_features',
            market_date=market_date,
            status='blocked',
            updated_at='2026-10-07T13:00:00+00:00',
            succeeded=0,
            failed=0,
        )
        blocked_history['stage']['updated_at'] = None
        blocked_history['stage']['completed_at'] = None
        blocked_history['stage']['details'] = {
            'result_summary': {
                'triggered': False,
                'status': 'blocked',
                'reason': 'inherited_downstream_recovery_pending',
            }
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'signal_features': blocked_history,
            },
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2_703,
                'updated_at': '2026-10-07T14:00:00+00:00',
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['signal_features']
        self.assertEqual(stage['status'], 'blocked')
        self.assertEqual(stage['details']['progress_source'], 'official_stage_history')

    def test_read_does_not_treat_partial_signal_decider_rows_as_recovery_evidence(self):
        market_date = '2026-10-07'
        eod_run = _blocked_downstream_output_eod_run(
            market_date=market_date,
            updated_at='2026-10-07T13:00:00+00:00',
        )
        eod_run['run_metadata']['stage_progress']['signal_decider'] = {
            'status': 'blocked',
            'processed': 0,
            'total': 115,
            'succeeded': 0,
            'failed': 0,
            'updated_at': '2026-10-07T13:00:00+00:00',
            'result_summary': {
                'triggered': False,
                'status': 'blocked',
                'reason': 'summary_run_symbol_scope_mismatch',
                'market_date': market_date,
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: eod_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            signal_decider_counts_loader=lambda **_: {
                'feedback_rows': 65,
                'feedback_symbols': 65,
                'decision_snapshots': 65,
                'decision_snapshot_symbols': 65,
                'updated_at': '2026-10-07T14:00:00+00:00',
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['signal_decider']
        self.assertEqual(stage['status'], 'blocked')
        self.assertEqual(stage['total'], 115)
        self.assertEqual(stage['source_run_ids'], {'eod_run_id': 561})
        self.assertEqual(stage['details']['progress_source'], 'live_run_metadata')

    def test_read_prefers_newer_completed_feedback_history_over_older_blocked_progress(self):
        market_date = '2026-10-07'
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: _blocked_downstream_output_eod_run(
                market_date=market_date,
                updated_at='2026-10-07T12:34:45+00:00',
            ),
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'agent_feedback_summaries': _completed_feedback_history_stage(
                    run_id=514,
                    market_date=market_date,
                    total=470,
                    updated_at='2026-10-07T16:08:53+00:00',
                ),
            },
        )

        result = service.read(
            market_date=market_date,
            include_feedback_context=False,
            include_feedback_stage=False,
        )

        stage = result['stages']['agent_feedback_summaries']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['processed'], 470)
        self.assertEqual(stage['details']['progress_source'], 'official_stage_history')

    def test_read_keeps_feedback_blocked_when_completed_freshness_is_not_proven(self):
        market_date = '2026-10-07'
        cases = (
            ('older', 'completed', '2026-10-07T12:00:00+00:00'),
            ('equal', 'completed', '2026-10-07T13:00:00+00:00'),
            ('missing', 'completed', None),
            ('invalid', 'completed', 'not-a-timestamp'),
            ('incomplete_newer', 'partial', '2026-10-07T14:00:00+00:00'),
        )
        for label, history_status, history_updated_at in cases:
            with self.subTest(label=label):
                if history_status == 'completed':
                    history = _completed_official_stage(
                        run_id=514,
                        stage_id='agent_feedback_summaries',
                        market_date=market_date,
                        processed=470,
                        total=470,
                        updated_at=history_updated_at,
                    )
                else:
                    history = _incomplete_official_stage(
                        run_id=514,
                        stage_id='agent_feedback_summaries',
                        market_date=market_date,
                        status=history_status,
                        updated_at=history_updated_at,
                        succeeded=469,
                        failed=1,
                    )
                service = EODPipelineStatusService(
                    market_state_loader=lambda: _FakeMarketState(),
                    pykrx_run_loader=lambda **_: _blocked_downstream_output_eod_run(
                        market_date=market_date,
                        updated_at='2026-10-07T13:00:00+00:00',
                    ),
                    pykrx_stage_counts_loader=lambda *_: {},
                    live_symbol_count_loader=lambda: 2_703,
                    fundamentals_refresh_loader=lambda **_: None,
                    official_run_loader=lambda **_: None,
                    official_stage_history_loader=lambda **_: {
                        'agent_feedback_summaries': history,
                    },
                )

                result = service.read(
                    market_date=market_date,
                    include_feedback_context=False,
                    include_feedback_stage=False,
                )

                stage = result['stages']['agent_feedback_summaries']
                self.assertEqual(stage['status'], 'blocked')
                self.assertEqual(stage['source_run_ids'], {'eod_run_id': 561})

    def test_read_preserves_newer_completed_feedback_context_over_stale_blocked_overlays(self):
        market_date = '2026-10-07'
        blocked_history = _incomplete_official_stage(
            run_id=510,
            stage_id='agent_feedback_summaries',
            market_date=market_date,
            status='blocked',
            updated_at='2026-10-07T13:00:00+00:00',
            succeeded=0,
            failed=0,
        )
        blocked_history['stage']['details'] = {
            'result_summary': {
                'triggered': False,
                'status': 'blocked',
                'reason': 'inherited_downstream_recovery_pending',
            }
        }
        latest_official_run = {
            **blocked_history['run'],
            'stages': {
                'agent_feedback_summaries': blocked_history['stage'],
            },
        }
        feedback_run = {
            'source': 'persisted_feedback_run',
            'run_id': 'feedback-cycle-2026-10-07-f83cb89026',
            'status': 'completed',
            'requested_symbols': ['005930'],
            'summary': {
                'run_status': 'completed',
                'run_table_status': 'completed',
                'failed_items': 0,
                'completed_feedback_count': 470,
                'expected_feedback_count': 470,
                'latest_feedback_at': '2026-10-07T14:00:00+00:00',
                'run_completed_at': '2026-10-07T14:00:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: _blocked_downstream_output_eod_run(
                market_date=market_date,
                updated_at='2026-10-07T12:34:45+00:00',
            ),
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_703,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: latest_official_run,
            official_stage_history_loader=lambda **_: {
                'agent_feedback_summaries': blocked_history,
            },
            feedback_run_loader=lambda **_: feedback_run,
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['agent_feedback_summaries']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['processed'], 470)
        self.assertEqual(stage['total'], 470)
        self.assertEqual(stage['succeeded'], 470)
        self.assertEqual(stage['failed'], 0)
        self.assertEqual(stage['remaining'], 0)
        self.assertEqual(
            stage['source_run_ids'],
            {'feedback_run_id': 'feedback-cycle-2026-10-07-f83cb89026'},
        )
        self.assertEqual(stage['details']['mode'], 'agent_feedback_run_state')

    def test_read_reports_external_news_enrich_pending_when_persisted_items_are_unclassified(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            external_news_counts_loader=lambda **_: {
                'items': 7,
                'symbols': 3,
                'classified_items': 0,
                'updated_at': '2026-05-28T06:40:00+00:00',
            },
        )

        result = service.read(market_date='2026-05-28')

        self.assertEqual(result['stages']['external_news_collect']['status'], 'completed')
        enrich_stage = result['stages']['external_news_enrich']
        self.assertEqual(enrich_stage['status'], 'pending')
        self.assertEqual(enrich_stage['source_status'], 'stored_external_news_unclassified')
        self.assertEqual(enrich_stage['details']['items_persisted'], 7)
        self.assertEqual(enrich_stage['details']['classified_items'], 0)
        self.assertEqual(enrich_stage['details']['reason'], 'external_news_items_need_classification')

    def test_read_reports_external_news_enrich_partial_when_only_some_items_have_contract_classification(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            external_news_counts_loader=lambda **_: {
                'items': 7,
                'symbols': 3,
                'contract_classified_items': 4,
                'legacy_classified_items': 2,
                'updated_at': '2026-05-28T06:40:00+00:00',
            },
        )

        result = service.read(market_date='2026-05-28')

        enrich_stage = result['stages']['external_news_enrich']
        self.assertEqual(enrich_stage['status'], 'partial')
        self.assertEqual(enrich_stage['source_status'], 'stored_external_news_partially_classified')
        self.assertEqual(enrich_stage['processed'], 4)
        self.assertEqual(enrich_stage['total'], 7)
        self.assertEqual(enrich_stage['details']['classified_items'], 4)
        self.assertEqual(enrich_stage['details']['contract_classified_items'], 4)
        self.assertEqual(enrich_stage['details']['legacy_classified_items'], 2)
        self.assertEqual(enrich_stage['details']['classification_coverage_ratio'], 0.571429)

    def test_read_includes_pykrx_enrichment_stage_from_coverage(self):
        coverage = {
            'requested_symbol_count': 2,
            'requested_market_count': 1,
            'categories': {
                category: {
                    'present_rows': 0,
                    'expected_rows': 2 if category not in {'market_breadth', 'investor_flow_market'} else 1,
                    'coverage_status': 'missing',
                    'latest_captured_at': None,
                }
                for category in DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
            },
        }
        coverage['categories']['sector_classification'] = {
            'present_rows': 2,
            'expected_rows': 2,
            'coverage_status': 'complete',
            'latest_captured_at': '2026-04-21T15:10:00+09:00',
        }
        coverage['categories']['market_breadth'] = {
            'present_rows': 1,
            'expected_rows': 1,
            'coverage_status': 'complete',
            'latest_captured_at': '2026-04-21T15:10:00+09:00',
        }

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            factor_stage_counts_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 0,
            pykrx_enrichment_loader=lambda **_: coverage,
        )

        result = service.read(market_date='2026-04-21', fs_div='CFS')

        self.assertLess(
            result['stage_order'].index('price_collection'),
            result['stage_order'].index('pykrx_enrichment'),
        )
        self.assertEqual(result['stages']['pykrx_enrichment']['status'], 'partial')
        self.assertEqual(result['stages']['pykrx_enrichment']['details']['requested_symbol_count'], 2)
        self.assertEqual(
            result['stages']['pykrx_enrichment']['details']['categories']['sector_classification']['coverage_status'],
            'complete',
        )

    def test_read_exposes_pykrx_enrichment_stage_from_live_run_progress(self):
        pykrx_run = {
            'id': 91,
            'trade_date': '2026-04-21',
            'status': 'completed_with_errors',
            'run_metadata': {
                'stage_progress': {
                    'pykrx_enrichment': {
                        'status': 'failed',
                        'started_at': '2026-04-21T08:00:00+00:00',
                        'updated_at': '2026-04-21T08:02:00+00:00',
                        'current_step': 'pykrx_enrichment_failed',
                        'current_activity': 'PyKRX enrichment collection failed',
                        'processed': 1,
                        'total': len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                        'succeeded': 1,
                        'failed': 3,
                        'category_row_counts': {
                            'sector_classification': 1,
                            'investor_flow_symbol': 0,
                            'short_selling': 0,
                        },
                        'failed_categories': ['investor_flow_symbol', 'short_selling'],
                        'downstream_allowed': False,
                        'downstream_preflight_status': 'blocked',
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            factor_stage_counts_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 0,
            pykrx_enrichment_loader=lambda **_: {},
        )

        result = service.read(market_date='2026-04-21', fs_div='CFS')

        stage = result['stages']['pykrx_enrichment']
        self.assertEqual(stage['status'], 'failed')
        self.assertEqual(stage['source_status'], 'failed')
        self.assertEqual(stage['source_run_ids'], {'eod_run_id': 91})
        self.assertEqual(stage['details']['progress_source'], 'live_run_metadata')
        self.assertFalse(stage['details']['downstream_allowed'])
        self.assertEqual(stage['details']['downstream_preflight_status'], 'blocked')
        self.assertEqual(
            stage['details']['category_row_counts'],
            {
                'sector_classification': 1,
                'investor_flow_symbol': 0,
                'short_selling': 0,
            },
        )

    def test_macro_context_stage_reports_persisted_context_status(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 0,
            fundamentals_refresh_loader=lambda **_: None,
            price_bar_count_loader=lambda **_: 0,
            pykrx_enrichment_loader=lambda **_: {},
            macro_context_counts_loader=lambda **_: {
                'persisted': 3,
                'required_series_count': 3,
                'updated_at': '2026-06-20T06:00:00+00:00',
                'context_status': 'available',
                'source_statuses': {
                    'world_bank': {'status': 'available', 'series_count': 3},
                    'fred': {'status': 'disabled_missing_api_key'},
                },
                'macro_regime_summary': {
                    'growth_label': 'growth_expansion',
                    'inflation_label': 'inflation_stable',
                    'llm_generated': False,
                },
                'usage_policy': {
                    'contextual_input_only': True,
                    'direct_buy_sell_signal': False,
                    'llm_generated': False,
                },
            },
        )

        result = service.read(market_date='2026-06-20', fs_div='CFS')

        stage = result['stages']['macro_context_refresh']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['source_status'], 'stored_macro_context_fresh')
        self.assertEqual(stage['succeeded'], 3)
        self.assertEqual(stage['details']['context_status'], 'available')
        self.assertEqual(stage['details']['source_statuses']['fred']['status'], 'disabled_missing_api_key')
        self.assertTrue(stage['details']['usage_policy']['contextual_input_only'])
        self.assertFalse(stage['details']['usage_policy']['direct_buy_sell_signal'])
