from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.missing_data_taxonomy import CATEGORY_DEFINITIONS_BY_KEY
from app.agent_context.read_service import (
    DECIDER_QUALITY_BLOCK_CATEGORY_KEYS,
    DECIDER_QUALITY_STATUS_AVAILABLE,
    DECIDER_QUALITY_STATUS_EMPTY,
    DECIDER_QUALITY_STATUS_NOT_APPLICABLE,
    DECIDER_QUALITY_STATUS_PROVIDER_ERROR,
    DECIDER_QUALITY_STATUS_STALE,
    DECIDER_QUALITY_STATUS_UNAVAILABLE,
)
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
from app.db import session_scope
from app.db_models import AgentContextFeedback

CORE_BLOCK_NAMES: tuple[str, ...] = (
    'liquidity_turnover',
    'intraday_realtime',
)
OPTIONAL_BLOCK_NAMES: tuple[str, ...] = (
    'factor_score_coverage',
    'financials_cashflow',
    'disclosures_news',
    'flows_short_selling',
    'sector_relative_strength',
    'technical_evidence',
)
CORE_STATUS_KEYS: tuple[str, ...] = (
    'available',
    'missing',
    'stale',
    'derived',
    'provider_error',
)
OPTIONAL_STATUS_KEYS: tuple[str, ...] = (
    'available',
    'empty',
    'unavailable',
    'provider_error',
)
RAW_STATUS_KEYS: tuple[str, ...] = (
    DECIDER_QUALITY_STATUS_AVAILABLE,
    'partial',
    DECIDER_QUALITY_STATUS_STALE,
    DECIDER_QUALITY_STATUS_EMPTY,
    DECIDER_QUALITY_STATUS_UNAVAILABLE,
    DECIDER_QUALITY_STATUS_PROVIDER_ERROR,
    DECIDER_QUALITY_STATUS_NOT_APPLICABLE,
)
EXPECTED_CONTEXT_OBJECT_PATHS: tuple[str, ...] = (
    'symbol_day_context.quote_context',
    'symbol_day_context.price_context',
    'symbol_day_context.intraday_execution_context',
    'symbol_day_context.signal_features',
    'symbol_day_context.fundamentals',
    'symbol_day_context.recent_disclosures',
    'symbol_day_context.recent_external_news',
    'symbol_day_context.market_relative_context',
    'symbol_day_context.sector_peer_context',
    'symbol_day_context.market_breadth_context',
    'symbol_day_context.investor_flow_context',
    'symbol_day_context.short_selling_context',
    'symbol_day_context.valuation_context',
    'symbol_day_context.technical_snapshot',
    'symbol_day_context.action_summary',
    'liquidity_context.turnover',
    'liquidity_context.ranking',
    'liquidity_context.intraday',
    'decider_data_quality_context',
    'decider_data_quality_context.blocks.factor_score_coverage',
    'decider_data_quality_context.blocks.liquidity_turnover',
    'decider_data_quality_context.blocks.intraday_realtime',
    'decider_data_quality_context.blocks.financials_cashflow',
    'decider_data_quality_context.blocks.disclosures_news',
    'decider_data_quality_context.blocks.flows_short_selling',
    'decider_data_quality_context.blocks.sector_relative_strength',
    'decider_data_quality_context.blocks.technical_evidence',
)
DEFAULT_AGENT_NAMES: tuple[str, ...] = tuple(sorted(DECISION_SUPPORT_AGENT_NAMES))
EFFECT_BUCKET_DECISION_AFFECTED = 'decision_affected'
EFFECT_BUCKET_CONFIDENCE_REDUCER = 'confidence_reducer'
EFFECT_BUCKET_NEUTRAL_AUXILIARY = 'neutral_auxiliary'
EFFECT_BUCKETS: tuple[str, ...] = (
    EFFECT_BUCKET_DECISION_AFFECTED,
    EFFECT_BUCKET_CONFIDENCE_REDUCER,
    EFFECT_BUCKET_NEUTRAL_AUXILIARY,
)


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    if not symbols:
        return []
    normalized_symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = piece.strip().upper()
            if not symbol:
                continue
            if len(symbol) > 32:
                raise ValueError('symbol must be 32 characters or fewer')
            if symbol in seen:
                continue
            seen.add(symbol)
            normalized_symbols.append(symbol)
    return normalized_symbols


def _normalize_agent_names(agent_names: Sequence[str] | None) -> list[str]:
    raw_names = agent_names if agent_names else DEFAULT_AGENT_NAMES
    normalized_names: list[str] = []
    seen: set[str] = set()
    for raw_name in raw_names:
        for piece in (raw_name or '').split(','):
            name = piece.strip().lower()
            if not name:
                continue
            if len(name) > 64:
                raise ValueError('agent_name must be 64 characters or fewer')
            if name in seen:
                continue
            seen.add(name)
            normalized_names.append(name)
    if not normalized_names:
        raise ValueError('At least one agent_name is required')
    return normalized_names


def _normalize_optional_run_id(run_id: str | None) -> str | None:
    if run_id is None:
        return None
    normalized = run_id.strip()
    if not normalized:
        return None
    if len(normalized) > 128:
        raise ValueError('run_id must be 128 characters or fewer')
    return normalized


def _maybe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _new_status_counts(keys: Sequence[str]) -> dict[str, int]:
    return {key: 0 for key in keys}


def _new_path_bucket() -> dict[str, Any]:
    return {
        'count': 0,
        'symbols': set(),
        'agents': set(),
        'run_ids': set(),
    }


def _new_block_bucket(block_name: str) -> dict[str, Any]:
    return {
        'block_name': block_name,
        'occurrences': 0,
        'counts': _new_status_counts(CORE_STATUS_KEYS if block_name in CORE_BLOCK_NAMES else OPTIONAL_STATUS_KEYS),
        'raw_status_counts': _new_status_counts(RAW_STATUS_KEYS),
        'symbols': set(),
        'agents': set(),
    }


def _new_category_bucket(
    *,
    category: str,
    label: str | None,
    decision_tier: str | None,
    severity: str | None,
    default_effect: str | None,
    blocking_by_default: bool | None,
    empty_is_neutral_by_default: bool | None,
) -> dict[str, Any]:
    return {
        'category': category,
        'label': label,
        'decision_tier': decision_tier,
        'severity': severity,
        'default_missing_effect': default_effect,
        'blocking_by_default': bool(blocking_by_default),
        'empty_is_neutral_by_default': bool(empty_is_neutral_by_default),
        'occurrences': 0,
        'status_counts': _new_status_counts(CORE_STATUS_KEYS + OPTIONAL_STATUS_KEYS),
        'raw_status_counts': _new_status_counts(RAW_STATUS_KEYS),
        'effect_counts': _new_status_counts(EFFECT_BUCKETS),
        'symbols': set(),
        'agents': set(),
        'run_ids': set(),
    }


def _new_effect_bucket(effect_bucket: str) -> dict[str, Any]:
    return {
        'effect_bucket': effect_bucket,
        'occurrences': 0,
        'status_counts': _new_status_counts(CORE_STATUS_KEYS + OPTIONAL_STATUS_KEYS),
        'raw_status_counts': _new_status_counts(RAW_STATUS_KEYS),
        'symbols': set(),
        'agents': set(),
        'run_ids': set(),
        'categories': {},
    }


def _new_scope(scope_name: str) -> dict[str, Any]:
    return {
        'scope_name': scope_name,
        'feedback_rows': 0,
        'symbols': set(),
        'agents': set(),
        'run_ids': set(),
        'rows_missing_quality_context': 0,
        'core_blocks': {
            'counts': _new_status_counts(CORE_STATUS_KEYS),
            'raw_status_counts': _new_status_counts(RAW_STATUS_KEYS),
            'by_block_name': {block_name: _new_block_bucket(block_name) for block_name in CORE_BLOCK_NAMES},
        },
        'optional_blocks': {
            'counts': _new_status_counts(OPTIONAL_STATUS_KEYS),
            'raw_status_counts': _new_status_counts(RAW_STATUS_KEYS),
            'by_block_name': {block_name: _new_block_bucket(block_name) for block_name in OPTIONAL_BLOCK_NAMES},
        },
        'top_missing_categories': {},
        'gap_effects': {bucket: _new_effect_bucket(bucket) for bucket in EFFECT_BUCKETS},
        'raw_null_paths': {},
        'missing_expected_paths': {},
    }


def _resolve_path_state(payload: dict[str, Any], path: str) -> str:
    current: Any = payload
    for segment in path.split('.'):
        if not isinstance(current, dict):
            return 'missing'
        if segment not in current:
            return 'missing'
        current = current.get(segment)
    if current is None:
        return 'null'
    return 'present'


def _record_path_bucket(
    buckets: dict[str, dict[str, Any]],
    *,
    path: str,
    symbol: str,
    agent_name: str,
    run_id: str | None,
) -> None:
    bucket = buckets.setdefault(path, _new_path_bucket())
    bucket['count'] += 1
    bucket['symbols'].add(symbol)
    bucket['agents'].add(agent_name)
    if run_id:
        bucket['run_ids'].add(run_id)


def _raw_status(block: dict[str, Any]) -> str:
    status = str(block.get('status') or '').strip().lower()
    return status if status in RAW_STATUS_KEYS else 'partial'


def _core_bucket_for_block(block_name: str, block: dict[str, Any]) -> str:
    raw_status = _raw_status(block)
    if raw_status == DECIDER_QUALITY_STATUS_NOT_APPLICABLE:
        return DECIDER_QUALITY_STATUS_AVAILABLE
    if raw_status == DECIDER_QUALITY_STATUS_PROVIDER_ERROR:
        return DECIDER_QUALITY_STATUS_PROVIDER_ERROR
    if raw_status == DECIDER_QUALITY_STATUS_STALE:
        return DECIDER_QUALITY_STATUS_STALE
    if block_name == 'liquidity_turnover':
        turnover = _maybe_dict(block.get('turnover'))
        source_status = str(turnover.get('source_status') or turnover.get('turnover_source') or '').strip().lower()
        if source_status == 'derived':
            return 'derived'
    if raw_status == DECIDER_QUALITY_STATUS_AVAILABLE:
        return DECIDER_QUALITY_STATUS_AVAILABLE
    return 'missing'


def _optional_bucket_for_block(block: dict[str, Any]) -> str:
    raw_status = _raw_status(block)
    if raw_status == DECIDER_QUALITY_STATUS_NOT_APPLICABLE:
        return DECIDER_QUALITY_STATUS_AVAILABLE
    if raw_status == DECIDER_QUALITY_STATUS_PROVIDER_ERROR:
        return DECIDER_QUALITY_STATUS_PROVIDER_ERROR
    if raw_status == DECIDER_QUALITY_STATUS_EMPTY:
        return DECIDER_QUALITY_STATUS_EMPTY
    if raw_status == DECIDER_QUALITY_STATUS_AVAILABLE:
        return DECIDER_QUALITY_STATUS_AVAILABLE
    return DECIDER_QUALITY_STATUS_UNAVAILABLE


def _effect_bucket_for_block(block: dict[str, Any]) -> str | None:
    raw_status = _raw_status(block)
    if raw_status in {
        DECIDER_QUALITY_STATUS_AVAILABLE,
        DECIDER_QUALITY_STATUS_NOT_APPLICABLE,
    }:
        return None
    decision_guidance = _maybe_dict(block.get('decision_guidance'))
    if raw_status == DECIDER_QUALITY_STATUS_EMPTY and bool(
        decision_guidance.get('empty_is_neutral_by_default')
    ):
        return EFFECT_BUCKET_NEUTRAL_AUXILIARY
    if bool(decision_guidance.get('blocking_by_default')):
        return EFFECT_BUCKET_DECISION_AFFECTED
    return EFFECT_BUCKET_CONFIDENCE_REDUCER


def _category_metadata_from_block(
    block_name: str,
    block: dict[str, Any],
) -> tuple[
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    bool | None,
    bool | None,
]:
    decision_guidance = _maybe_dict(block.get('decision_guidance'))
    category = str(
        decision_guidance.get('category')
        or DECIDER_QUALITY_BLOCK_CATEGORY_KEYS.get(block_name)
        or ''
    ).strip() or None
    if category is None:
        return None, None, None, None, None, None, None
    definition = CATEGORY_DEFINITIONS_BY_KEY.get(category)
    if definition is None:
        return (
            category,
            None,
            str(decision_guidance.get('decision_tier') or '').strip() or None,
            str(decision_guidance.get('severity') or '').strip() or None,
            str(decision_guidance.get('default_missing_effect') or '').strip() or None,
            bool(decision_guidance.get('blocking_by_default')),
            bool(decision_guidance.get('empty_is_neutral_by_default')),
        )
    return (
        definition.key,
        definition.label,
        definition.decision_tier,
        definition.severity,
        definition.default_decider_effect,
        definition.default_decider_effect == 'core_blocker',
        definition.empty_is_neutral_by_default,
    )


def _category_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    severity_rank = {'high': 0, 'medium': 1, 'low': 2}
    return (-int(item['occurrences']), severity_rank.get(str(item.get('severity') or ''), 99), item['category'])


def _serialize_block_groups(groups: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for block_name, bucket in sorted(groups.items()):
        payload.append(
            {
                'block_name': block_name,
                'occurrences': bucket['occurrences'],
                'counts': dict(bucket['counts']),
                'raw_status_counts': dict(bucket['raw_status_counts']),
                'symbols': sorted(bucket['symbols']),
                'agents': sorted(bucket['agents']),
            }
        )
    return payload


def _serialize_path_buckets(buckets: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for path, bucket in sorted(
        buckets.items(),
        key=lambda item: (-item[1]['count'], item[0]),
    ):
        payload.append(
            {
                'path': path,
                'count': bucket['count'],
                'symbols': sorted(bucket['symbols']),
                'agents': sorted(bucket['agents']),
                'run_ids': sorted(bucket['run_ids']),
            }
        )
    return payload


def _serialize_category_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    status_counts = {
        key: value
        for key, value in bucket['status_counts'].items()
        if value > 0
    }
    raw_status_counts = {
        key: value
        for key, value in bucket['raw_status_counts'].items()
        if value > 0
    }
    effect_counts = {
        key: value
        for key, value in bucket['effect_counts'].items()
        if value > 0
    }
    return {
        'category': bucket['category'],
        'label': bucket['label'],
        'decision_tier': bucket['decision_tier'],
        'severity': bucket['severity'],
        'default_missing_effect': bucket['default_missing_effect'],
        'blocking_by_default': bucket['blocking_by_default'],
        'empty_is_neutral_by_default': bucket['empty_is_neutral_by_default'],
        'occurrences': bucket['occurrences'],
        'status_counts': status_counts,
        'raw_status_counts': raw_status_counts,
        'effect_counts': effect_counts,
        'symbols': sorted(bucket['symbols']),
        'agents': sorted(bucket['agents']),
        'run_ids': sorted(bucket['run_ids']),
    }


def _serialize_effect_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    categories = [
        _serialize_category_bucket(category_bucket)
        for category_bucket in sorted(
            bucket['categories'].values(),
            key=_category_sort_key,
        )
    ]
    return {
        'effect_bucket': bucket['effect_bucket'],
        'occurrences': bucket['occurrences'],
        'status_counts': {
            key: value
            for key, value in bucket['status_counts'].items()
            if value > 0
        },
        'raw_status_counts': {
            key: value
            for key, value in bucket['raw_status_counts'].items()
            if value > 0
        },
        'category_count': len(categories),
        'symbols': sorted(bucket['symbols']),
        'agents': sorted(bucket['agents']),
        'run_ids': sorted(bucket['run_ids']),
        'categories': categories,
    }


def _serialize_scope(scope: dict[str, Any]) -> dict[str, Any]:
    core_counts = dict(scope['core_blocks']['counts'])
    optional_counts = dict(scope['optional_blocks']['counts'])
    raw_null_count = sum(bucket['count'] for bucket in scope['raw_null_paths'].values())
    missing_expected_count = sum(bucket['count'] for bucket in scope['missing_expected_paths'].values())
    core_regression_count = core_counts['missing'] + core_counts[DECIDER_QUALITY_STATUS_PROVIDER_ERROR]
    regression_status = (
        'alert'
        if raw_null_count > 0 or missing_expected_count > 0 or core_regression_count > 0
        else 'pass'
    )
    return {
        'feedback_rows': scope['feedback_rows'],
        'symbols_evaluated': len(scope['symbols']),
        'agents_present': sorted(scope['agents']),
        'run_ids_present': sorted(scope['run_ids']),
        'rows_missing_quality_context': scope['rows_missing_quality_context'],
        'core_blocks': {
            'counts': core_counts,
            'raw_status_counts': dict(scope['core_blocks']['raw_status_counts']),
            'by_block_name': _serialize_block_groups(scope['core_blocks']['by_block_name']),
        },
        'optional_blocks': {
            'counts': optional_counts,
            'raw_status_counts': dict(scope['optional_blocks']['raw_status_counts']),
            'by_block_name': _serialize_block_groups(scope['optional_blocks']['by_block_name']),
        },
        'top_missing_categories': [
            _serialize_category_bucket(bucket)
            for bucket in sorted(scope['top_missing_categories'].values(), key=_category_sort_key)
        ],
        'gap_effects': {
            bucket_name: _serialize_effect_bucket(scope['gap_effects'][bucket_name])
            for bucket_name in EFFECT_BUCKETS
        },
        'contract_regressions': {
            'status': regression_status,
            'raw_null_count': raw_null_count,
            'missing_expected_object_count': missing_expected_count,
            'unexpected_missing_core_block_count': core_regression_count,
            'raw_null_paths': _serialize_path_buckets(scope['raw_null_paths']),
            'missing_expected_object_paths': _serialize_path_buckets(scope['missing_expected_paths']),
        },
    }


class DeciderCoverageReportRepository:
    def list_feedback_rows(
        self,
        *,
        market_date: date,
        run_id: str | None = None,
        symbols: Sequence[str] | None = None,
        agent_names: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = (
                select(
                    AgentContextFeedback.id,
                    AgentContextFeedback.market_date,
                    AgentContextFeedback.symbol,
                    AgentContextFeedback.agent_name,
                    AgentContextFeedback.run_id,
                    AgentContextFeedback.summary_payload,
                    AgentContextFeedback.feedback_payload,
                    AgentContextFeedback.normalized_feedback_payload,
                    AgentContextFeedback.missing_data_categories_payload,
                    AgentContextFeedback.context_payload,
                    AgentContextFeedback.received_at,
                )
                .where(AgentContextFeedback.market_date == market_date)
                .order_by(
                    AgentContextFeedback.agent_name.asc(),
                    AgentContextFeedback.symbol.asc(),
                    AgentContextFeedback.received_at.desc(),
                    AgentContextFeedback.id.desc(),
                )
            )
            if run_id:
                stmt = stmt.where(AgentContextFeedback.run_id == run_id)
            if symbols:
                stmt = stmt.where(AgentContextFeedback.symbol.in_(symbols))
            if agent_names:
                stmt = stmt.where(AgentContextFeedback.agent_name.in_(agent_names))
            return [dict(row) for row in session.execute(stmt).mappings().all()]


class DeciderCoverageReportService:
    def __init__(self, *, repository: DeciderCoverageReportRepository | None = None) -> None:
        self.repository = repository or DeciderCoverageReportRepository()

    def get_report(
        self,
        *,
        market_date: str | date,
        run_id: str | None = None,
        symbols: Sequence[str] | None = None,
        agent_names: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_run_id = _normalize_optional_run_id(run_id)
        normalized_symbols = _normalize_symbols(symbols)
        normalized_agent_names = _normalize_agent_names(agent_names)
        rows = self.repository.list_feedback_rows(
            market_date=normalized_market_date,
            run_id=normalized_run_id,
            symbols=normalized_symbols,
            agent_names=normalized_agent_names,
        )

        overall_scope = _new_scope('all')
        scopes_by_agent = {agent_name: _new_scope(agent_name) for agent_name in normalized_agent_names}

        for row in rows:
            agent_name = str(row.get('agent_name') or '').strip().lower()
            if agent_name not in scopes_by_agent:
                scopes_by_agent[agent_name] = _new_scope(agent_name)
            symbol = str(row.get('symbol') or '').strip().upper()
            row_run_id = _normalize_optional_run_id(row.get('run_id'))
            context_payload = _maybe_dict(row.get('context_payload'))
            decider_data_quality_context = _maybe_dict(
                context_payload.get('decider_data_quality_context')
            )
            blocks = _maybe_dict(decider_data_quality_context.get('blocks'))

            for scope in (overall_scope, scopes_by_agent[agent_name]):
                scope['feedback_rows'] += 1
                scope['symbols'].add(symbol)
                scope['agents'].add(agent_name)
                if row_run_id:
                    scope['run_ids'].add(row_run_id)

                for path in EXPECTED_CONTEXT_OBJECT_PATHS:
                    path_state = _resolve_path_state(context_payload, path)
                    if path_state == 'null':
                        _record_path_bucket(
                            scope['raw_null_paths'],
                            path=path,
                            symbol=symbol,
                            agent_name=agent_name,
                            run_id=row_run_id,
                        )
                    elif path_state == 'missing':
                        _record_path_bucket(
                            scope['missing_expected_paths'],
                            path=path,
                            symbol=symbol,
                            agent_name=agent_name,
                            run_id=row_run_id,
                        )

                if not blocks:
                    scope['rows_missing_quality_context'] += 1
                    continue

                for block_name, block_value in blocks.items():
                    block = _maybe_dict(block_value)
                    raw_status = _raw_status(block)

                    if block_name in CORE_BLOCK_NAMES:
                        normalized_status = _core_bucket_for_block(block_name, block)
                        block_group = scope['core_blocks']
                    elif block_name in OPTIONAL_BLOCK_NAMES:
                        normalized_status = _optional_bucket_for_block(block)
                        block_group = scope['optional_blocks']
                    else:
                        continue

                    block_group['counts'][normalized_status] += 1
                    block_group['raw_status_counts'][raw_status] += 1
                    block_bucket = block_group['by_block_name'][block_name]
                    block_bucket['occurrences'] += 1
                    block_bucket['counts'][normalized_status] += 1
                    block_bucket['raw_status_counts'][raw_status] += 1
                    block_bucket['symbols'].add(symbol)
                    block_bucket['agents'].add(agent_name)

                    effect_bucket = _effect_bucket_for_block(block)
                    if effect_bucket is None:
                        continue

                    effect_payload = scope['gap_effects'][effect_bucket]
                    effect_payload['occurrences'] += 1
                    effect_payload['status_counts'][normalized_status] += 1
                    effect_payload['raw_status_counts'][raw_status] += 1
                    effect_payload['symbols'].add(symbol)
                    effect_payload['agents'].add(agent_name)
                    if row_run_id:
                        effect_payload['run_ids'].add(row_run_id)

                    (
                        category_key,
                        category_label,
                        decision_tier,
                        severity,
                        default_effect,
                        blocking_by_default,
                        empty_is_neutral_by_default,
                    ) = _category_metadata_from_block(block_name, block)
                    if category_key is None:
                        continue

                    category_bucket = effect_payload['categories'].setdefault(
                        category_key,
                        _new_category_bucket(
                            category=category_key,
                            label=category_label,
                            decision_tier=decision_tier,
                            severity=severity,
                            default_effect=default_effect,
                            blocking_by_default=blocking_by_default,
                            empty_is_neutral_by_default=empty_is_neutral_by_default,
                        ),
                    )
                    category_bucket['occurrences'] += 1
                    category_bucket['status_counts'][normalized_status] += 1
                    category_bucket['raw_status_counts'][raw_status] += 1
                    category_bucket['effect_counts'][effect_bucket] += 1
                    category_bucket['symbols'].add(symbol)
                    category_bucket['agents'].add(agent_name)
                    if row_run_id:
                        category_bucket['run_ids'].add(row_run_id)

                    if effect_bucket == EFFECT_BUCKET_NEUTRAL_AUXILIARY:
                        continue

                    top_missing_bucket = scope['top_missing_categories'].setdefault(
                        category_key,
                        _new_category_bucket(
                            category=category_key,
                            label=category_label,
                            decision_tier=decision_tier,
                            severity=severity,
                            default_effect=default_effect,
                            blocking_by_default=blocking_by_default,
                            empty_is_neutral_by_default=empty_is_neutral_by_default,
                        ),
                    )
                    top_missing_bucket['occurrences'] += 1
                    top_missing_bucket['status_counts'][normalized_status] += 1
                    top_missing_bucket['raw_status_counts'][raw_status] += 1
                    top_missing_bucket['effect_counts'][effect_bucket] += 1
                    top_missing_bucket['symbols'].add(symbol)
                    top_missing_bucket['agents'].add(agent_name)
                    if row_run_id:
                        top_missing_bucket['run_ids'].add(row_run_id)

        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'run_id': normalized_run_id,
                'symbols': normalized_symbols or None,
                'agent_names': normalized_agent_names,
            },
            'summary': _serialize_scope(overall_scope),
            'per_agent': {
                agent_name: _serialize_scope(scope)
                for agent_name, scope in sorted(scopes_by_agent.items())
                if scope['feedback_rows'] > 0
            },
        }


decider_coverage_report_service = DeciderCoverageReportService()
