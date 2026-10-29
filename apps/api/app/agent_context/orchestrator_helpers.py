from __future__ import annotations

import json
import hashlib
import re
import uuid
from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Any, Sequence

from fastapi.encoders import jsonable_encoder
from app.agent_context.signal_envelope import (
    SIGNAL_ENVELOPE_SCHEMA_VERSION,
    build_decider_signal_envelopes,
)
from app.agent_context.signal_decider_loop_service import signal_decider_loop_service
from app.agent_context.response_contract import build_agent_contract_provenance
from app.agent_context.model_identity import build_runtime_contract_provenance

ABORTLIKE_ERROR_MARKERS = (
    'this operation was aborted',
    'aborted',
    'abortedlastrun',
    'killed',
    'failed',
)


def is_abortlike_error(error: Any) -> bool:
    text = str(error or '').lower()
    return any(marker in text for marker in ABORTLIKE_ERROR_MARKERS)


FEEDBACK_SESSION_KEY_PREFIX = 'feedback'
FRESH_FEEDBACK_SESSION_KEY_PREFIX = 'feedback-fresh'
DECIDER_SOURCE_CONTEXT_GENERATION_SCHEMA_VERSION = (
    'decider_source_context_generation_v1'
)
DECIDER_INPUT_GENERATION_SCHEMA_VERSION = 'decider_input_generation_v1'
DURABLE_DECIDER_INPUT_GENERATION_SCHEMA_VERSION = (
    'durable_decider_input_generation_v1'
)
DECIDER_SOURCE_CONTEXT_VOLATILE_KEYS = frozenset(
    {
        'age_seconds',
        'decision_time',
        'knowledge_cutoff_at',
        'requested_at',
    }
)
DECIDER_MARKET_CONTEXT_VOLATILE_KEYS = frozenset({'captured_at'})
DURABLE_DECIDER_SOURCE_CONTEXT_KEYS = (
    'symbol',
    'market_date',
    'company',
    'price_context',
    'price_context_history',
    'technical_snapshot',
    'action_summary',
    'signal_features',
    'market_relative_context',
    'sector_peer_context',
    'market_breadth_context',
    'macro_context',
    'investor_flow_context',
    'short_selling_context',
    'valuation_context',
    'scores',
    'factor_summary',
    'persisted_factor_rows',
    'fundamentals',
    'recent_disclosures',
    'recent_dart_events',
    'recent_external_news',
    'agent_feedback_eligibility',
    'signal_envelopes',
)
DURABLE_DECIDER_CONTEXT_PROVENANCE_KEYS = frozenset(
    {
        'price_context',
        'market_relative_context',
        'signal_features',
        'sector_peer_context',
        'market_breadth_context',
        'macro_context',
        'investor_flow_context',
        'short_selling_context',
        'valuation_context',
        'fundamentals',
        'recent_disclosures',
        'recent_dart_events',
        'recent_external_news',
        'factor_rows',
    }
)
DURABLE_DECIDER_AVAILABILITY_KEYS = frozenset(
    {
        'company',
        'price_context',
        'technical_snapshot',
        'action_summary',
        'signal_features',
        'market_relative_context',
        'sector_peer_context',
        'market_breadth_context',
        'investor_flow_context',
        'short_selling_context',
        'valuation_context',
        'persisted_factor_rows',
        'fundamentals',
        'recent_disclosures',
        'recent_dart_events',
        'recent_external_news',
        'macro_context',
    }
)
EXTERNAL_NEWS_PROVIDER_RUNTIME_KEYS = frozenset(
    {'enabled', 'collection_enabled', 'status', 'reason'}
)


def feedback_session_key(*, agent_name: str, slot: int) -> str:
    return f"{FEEDBACK_SESSION_KEY_PREFIX}/{agent_name}/slot-{slot:02d}"


def fresh_feedback_session_key(
    *,
    agent_name: str,
    request_run_id: str,
    batch_index: int,
    slot: int,
) -> str:
    return (
        f'{FRESH_FEEDBACK_SESSION_KEY_PREFIX}/{agent_name}/{request_run_id}/'
        f'batch-{batch_index:03d}/slot-{slot:02d}/{uuid.uuid4().hex[:10]}'
    )


def subagent_test_session_key(*, agent_name: str, request_run_id: str, symbol: str) -> str:
    safe_symbol = ''.join(ch for ch in symbol if ch.isalnum())[:24] or 'symbol'
    return f"subagent-test/{agent_name}/{request_run_id}/{safe_symbol}-{uuid.uuid4().hex[:8]}"


def normalize_requested_symbols(symbols: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = piece.strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        raise ValueError('At least one symbol is required')
    return normalized


def strip_runtime_session_fields(item: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in item.items()
        if key not in {'session_slot', 'session_key'}
    }


def _canonical_decider_source_context(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _canonical_decider_source_context(item)
            for key, item in value.items()
            if str(key) not in DECIDER_SOURCE_CONTEXT_VOLATILE_KEYS
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_decider_source_context(item) for item in value]
    return value


def build_decider_source_context_generation(
    item: dict[str, Any],
) -> dict[str, Any]:
    canonical_context = _canonical_decider_source_context(
        strip_runtime_session_fields(item)
    )
    encoded = json.dumps(
        jsonable_encoder(canonical_context),
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    )
    return {
        'schema_version': DECIDER_SOURCE_CONTEXT_GENERATION_SCHEMA_VERSION,
        'fingerprint': (
            'sha256:' + hashlib.sha256(encoded.encode('utf-8')).hexdigest()
        ),
    }


def _stable_market_context(value: Any) -> Any:
    """Remove collection clocks that do not change the market facts.

    Cached fallback providers can refresh ``captured_at`` while returning the
    same value and effective date. Binding that clock into a decider generation
    fingerprint makes a long-running dispatch invalidate its own results at a
    cache boundary even though no decision input changed.
    """
    if isinstance(value, dict):
        return {
            str(key): _stable_market_context(item)
            for key, item in value.items()
            if str(key) not in DECIDER_MARKET_CONTEXT_VOLATILE_KEYS
        }
    if isinstance(value, (list, tuple)):
        return [_stable_market_context(item) for item in value]
    return value


def _stable_macro_context(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    result = deepcopy(value)
    result.pop('generated_at', None)
    series = result.get('series')
    if isinstance(series, list):
        for row in series:
            if not isinstance(row, dict):
                continue
            freshness = row.get('freshness')
            if isinstance(freshness, dict):
                freshness.pop('age_hours', None)
    return result


def _durable_macro_context(value: Any) -> Any:
    """Remove time-derived freshness state while preserving macro facts.

    A next-open decision can cross weekends or holidays while the underlying
    slow-moving macro observations remain unchanged.  Recomputing the same
    snapshot then changes ``available``/``fresh`` labels to ``stale`` purely
    because wall-clock time advanced.  Those labels are execution-time context,
    not a new decision input; the durable generation already binds the original
    knowledge cutoff separately.  Keep observation values, periods, units, and
    provenance, but exclude derived freshness labels and aggregate counters.
    """
    result = _stable_macro_context(value)
    if not isinstance(result, dict):
        return result

    result.pop('status', None)
    result.pop('freshness_status', None)
    freshness = result.get('freshness')
    if isinstance(freshness, dict):
        for key in (
            'age_hours',
            'freshness_status',
            'knowledge_cutoff_applied',
            'knowledge_cutoff_at',
            'status',
        ):
            freshness.pop(key, None)

    macro_regime_summary = result.get('macro_regime_summary')
    if isinstance(macro_regime_summary, dict):
        macro_regime_summary.pop('freshness_label', None)

    source_statuses = result.get('source_statuses')
    if isinstance(source_statuses, dict):
        for source_status in source_statuses.values():
            if not isinstance(source_status, dict):
                continue
            for key in (
                'available_count',
                'missing_count',
                'stale_count',
                'status',
            ):
                source_status.pop(key, None)

    series = result.get('series')
    if isinstance(series, list):
        for row in series:
            if not isinstance(row, dict):
                continue
            row.pop('status', None)
            row_freshness = row.get('freshness')
            if not isinstance(row_freshness, dict):
                continue
            for key in (
                'age_hours',
                'freshness_status',
                'knowledge_cutoff_applied',
                'knowledge_cutoff_at',
                'status',
            ):
                row_freshness.pop(key, None)
    return result


def _remove_nested_key(value: Any, key_to_remove: str) -> None:
    if isinstance(value, dict):
        value.pop(key_to_remove, None)
        for nested in value.values():
            _remove_nested_key(nested, key_to_remove)
    elif isinstance(value, list):
        for nested in value:
            _remove_nested_key(nested, key_to_remove)


def _strip_durable_replay_metadata(value: dict[str, Any]) -> dict[str, Any]:
    """Drop recomputation clocks that do not change decision facts."""
    result = deepcopy(value)
    context_provenance = result.get('context_provenance')
    if isinstance(context_provenance, dict):
        signal_features = context_provenance.get('signal_features')
        if isinstance(signal_features, dict):
            signal_features.pop('latest_available_timestamp', None)
    eligibility = result.get('agent_feedback_eligibility')
    if isinstance(eligibility, dict):
        _remove_nested_key(eligibility, 'evaluated_at')
    return result


def _restore_nested_key(
    target: Any,
    source: Any,
    key_to_restore: str,
) -> None:
    if isinstance(target, dict) and isinstance(source, dict):
        if key_to_restore in source:
            target[key_to_restore] = deepcopy(source[key_to_restore])
        for key, source_value in source.items():
            if key == key_to_restore or key not in target:
                continue
            _restore_nested_key(target[key], source_value, key_to_restore)
    elif isinstance(target, list) and isinstance(source, list):
        for target_item, source_item in zip(target, source):
            _restore_nested_key(target_item, source_item, key_to_restore)


def _restore_legacy_replay_metadata(
    projected: dict[str, Any],
    expected_source_context: dict[str, Any] | None,
    *,
    restore_company_identifier: bool = True,
    restore_replay_clocks: bool = True,
) -> dict[str, Any]:
    result = deepcopy(projected)
    if not isinstance(expected_source_context, dict):
        return result
    persisted_context = expected_source_context.get('symbol_day_context')
    if not isinstance(persisted_context, dict):
        persisted_context = expected_source_context

    if restore_company_identifier:
        expected_company = persisted_context.get('company')
        current_company = result.get('company')
        if (
            isinstance(expected_company, dict)
            and isinstance(current_company, dict)
            and 'standard_code' in expected_company
        ):
            current_company['standard_code'] = deepcopy(
                expected_company['standard_code']
            )

    if not restore_replay_clocks:
        return result

    expected_provenance = persisted_context.get('context_provenance')
    current_provenance = result.get('context_provenance')
    if isinstance(expected_provenance, dict) and isinstance(
        current_provenance, dict
    ):
        expected_signal_features = expected_provenance.get('signal_features')
        current_signal_features = current_provenance.get('signal_features')
        if isinstance(expected_signal_features, dict) and isinstance(
            current_signal_features, dict
        ) and 'latest_available_timestamp' in expected_signal_features:
            current_signal_features['latest_available_timestamp'] = deepcopy(
                expected_signal_features['latest_available_timestamp']
            )

    expected_eligibility = persisted_context.get('agent_feedback_eligibility')
    current_eligibility = result.get('agent_feedback_eligibility')
    if isinstance(expected_eligibility, dict) and isinstance(
        current_eligibility, dict
    ):
        _restore_nested_key(
            current_eligibility,
            expected_eligibility,
            'evaluated_at',
        )
    return result


def _restore_post_decision_evaluation_metadata(
    projected: dict[str, Any],
    expected_source_context: dict[str, Any] | None,
) -> dict[str, Any]:
    """Freeze evaluation-derived eligibility metadata at decision time.

    Signal-decider evaluations are intentionally written after the decision
    snapshot and may later be upserted.  Their global diagnostic counts and
    forced-inclusion explanation must therefore replay from the persisted
    request, while every current eligibility gate and non-evaluation reason
    remains authoritative.
    """
    result = deepcopy(projected)
    if not isinstance(expected_source_context, dict):
        return result
    persisted_context = expected_source_context.get('symbol_day_context')
    if not isinstance(persisted_context, dict):
        persisted_context = expected_source_context

    expected_eligibility = persisted_context.get(
        'agent_feedback_eligibility'
    )
    current_eligibility = result.get('agent_feedback_eligibility')
    if not isinstance(expected_eligibility, dict) or not isinstance(
        current_eligibility, dict
    ):
        return result

    expected_candidate_cap = expected_eligibility.get('candidate_cap')
    current_candidate_cap = current_eligibility.get('candidate_cap')
    if isinstance(expected_candidate_cap, dict) and isinstance(
        current_candidate_cap, dict
    ):
        expected_diagnostics = expected_candidate_cap.get(
            'forced_source_diagnostics'
        )
        current_diagnostics = current_candidate_cap.get(
            'forced_source_diagnostics'
        )
        if isinstance(expected_diagnostics, dict) and isinstance(
            current_diagnostics, dict
        ):
            diagnostic_key = 'prior_missed_opportunities'
            if diagnostic_key in expected_diagnostics:
                current_diagnostics[diagnostic_key] = deepcopy(
                    expected_diagnostics[diagnostic_key]
                )
            else:
                current_diagnostics.pop(diagnostic_key, None)

    expected_reasons = expected_eligibility.get(
        'forced_inclusion_reasons'
    )
    current_reasons = current_eligibility.get('forced_inclusion_reasons')
    if isinstance(expected_reasons, list) and isinstance(current_reasons, list):
        evaluation_source = 'signal_decider_evaluations'
        restored_reasons = [
            deepcopy(reason)
            for reason in current_reasons
            if not (
                isinstance(reason, dict)
                and reason.get('source') == evaluation_source
            )
        ]
        persisted_evaluation_reasons = [
            (index, deepcopy(reason))
            for index, reason in enumerate(expected_reasons)
            if isinstance(reason, dict)
            and reason.get('source') == evaluation_source
        ]
        for index, reason in persisted_evaluation_reasons:
            restored_reasons.insert(min(index, len(restored_reasons)), reason)
        current_eligibility['forced_inclusion_reasons'] = restored_reasons
    return result


def _restore_legacy_macro_freshness_metadata(
    projected: dict[str, Any],
    expected_source_context: dict[str, Any] | None,
) -> dict[str, Any]:
    """Rebuild the pre-stability fingerprint without replacing macro facts."""
    result = deepcopy(projected)
    if not isinstance(expected_source_context, dict):
        return result
    persisted_context = expected_source_context.get('symbol_day_context')
    if not isinstance(persisted_context, dict):
        persisted_context = expected_source_context

    def restore_status_projection(
        target: Any,
        source: Any,
        *,
        series_row: bool = False,
    ) -> None:
        if not isinstance(target, dict) or not isinstance(source, dict):
            return
        for key in ('status', 'freshness_status'):
            if key in source:
                target[key] = deepcopy(source[key])

        source_freshness = source.get('freshness')
        target_freshness = target.get('freshness')
        if isinstance(source_freshness, dict):
            if not isinstance(target_freshness, dict):
                target_freshness = {}
                target['freshness'] = target_freshness
            for key, value in source_freshness.items():
                if key == 'age_hours' and series_row:
                    continue
                if key == 'knowledge_cutoff_applied' and not series_row:
                    continue
                target_freshness[key] = deepcopy(value)

        source_summary = source.get('macro_regime_summary')
        target_summary = target.get('macro_regime_summary')
        if (
            isinstance(source_summary, dict)
            and isinstance(target_summary, dict)
            and 'freshness_label' in source_summary
        ):
            target_summary['freshness_label'] = deepcopy(
                source_summary['freshness_label']
            )

        source_statuses = source.get('source_statuses')
        target_statuses = target.get('source_statuses')
        if isinstance(source_statuses, dict) and isinstance(
            target_statuses, dict
        ):
            for source_name, source_status in source_statuses.items():
                target_status = target_statuses.get(source_name)
                if not isinstance(source_status, dict) or not isinstance(
                    target_status, dict
                ):
                    continue
                for key in (
                    'available_count',
                    'missing_count',
                    'stale_count',
                    'status',
                ):
                    if key in source_status:
                        target_status[key] = deepcopy(source_status[key])

        source_series = source.get('series')
        target_series = target.get('series')
        if not isinstance(source_series, list) or not isinstance(
            target_series, list
        ):
            return
        source_by_identity = {
            (
                str(row.get('source') or ''),
                str(row.get('series_id') or ''),
            ): row
            for row in source_series
            if isinstance(row, dict)
        }
        for index, target_row in enumerate(target_series):
            if not isinstance(target_row, dict):
                continue
            identity = (
                str(target_row.get('source') or ''),
                str(target_row.get('series_id') or ''),
            )
            source_row = source_by_identity.get(identity)
            if source_row is None and index < len(source_series):
                source_row = source_series[index]
            restore_status_projection(
                target_row,
                source_row,
                series_row=True,
            )

    expected_macro = expected_source_context.get('macro_context')
    if not isinstance(expected_macro, dict):
        expected_macro = persisted_context.get('macro_context')
    restore_status_projection(result.get('macro_context'), expected_macro)

    expected_provenance = persisted_context.get('context_provenance')
    current_provenance = result.get('context_provenance')
    if isinstance(expected_provenance, dict) and isinstance(
        current_provenance, dict
    ):
        restore_status_projection(
            current_provenance.get('macro_context'),
            expected_provenance.get('macro_context'),
        )
    return result


def _source_names(value: Any) -> tuple[Any, ...]:
    """Normalize provenance source fields without treating text as characters."""
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(value)
    return (value,)


def _durable_decider_source_context(item: dict[str, Any]) -> dict[str, Any]:
    """Project a decider input onto data that remains authoritative overnight.

    Current quote, order-book, session, and open-drive derivatives are enforced
    independently by the next-open live-market gate. Keeping them in the durable
    fingerprint would revoke a valid completed-EOD decision merely because the
    next trading session opened. The projection intentionally keeps the source-day
    EOD facts and their provenance, including the complete eligibility record.
    """

    projected = {
        key: item[key]
        for key in DURABLE_DECIDER_SOURCE_CONTEXT_KEYS
        if key in item
    }

    if 'macro_context' in projected:
        projected['macro_context'] = _durable_macro_context(
            projected['macro_context']
        )
    company = item.get('company')
    if isinstance(company, dict):
        projected['company'] = {
            key: value
            for key, value in company.items()
            if key not in {'last_refreshed_at', 'source', 'standard_code'}
        }

    def stable_external_news(value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        result = deepcopy(value)
        result.pop('knowledge_cutoff_source', None)

        def strip_provider_runtime_fields(provider: Any) -> None:
            if not isinstance(provider, dict):
                return
            for key in EXTERNAL_NEWS_PROVIDER_RUNTIME_KEYS:
                provider.pop(key, None)

        strip_provider_runtime_fields(result.get('provider'))
        strip_provider_runtime_fields(result.get('provider_status'))
        root_requested_coverage = result.get('requested_symbol_coverage')
        if isinstance(root_requested_coverage, dict):
            strip_provider_runtime_fields(
                root_requested_coverage.get('provider_status')
            )
        freshness = result.get('freshness_provenance')
        if isinstance(freshness, dict):
            strip_provider_runtime_fields(freshness.get('provider_status'))
            requested_coverage = freshness.get('requested_symbol_coverage')
            if isinstance(requested_coverage, dict):
                strip_provider_runtime_fields(
                    requested_coverage.get('provider_status')
                )
        return result

    if 'recent_external_news' in projected:
        projected['recent_external_news'] = stable_external_news(
            projected['recent_external_news']
        )
    liquidity_context = item.get('liquidity_context')
    if isinstance(liquidity_context, dict):
        projected['liquidity_context'] = {
            key: liquidity_context[key]
            for key in ('status', 'reason', 'turnover', 'ranking')
            if key in liquidity_context
        }
    context_provenance = item.get('context_provenance')
    if isinstance(context_provenance, dict):
        projected['context_provenance'] = {
            key: (
                stable_external_news(value)
                if key == 'recent_external_news'
                else _durable_macro_context(value)
                if key == 'macro_context'
                else value
            )
            for key, value in context_provenance.items()
            if key in DURABLE_DECIDER_CONTEXT_PROVENANCE_KEYS
        }
    availability = item.get('availability')
    if isinstance(availability, dict):
        projected['availability'] = {
            key: value
            for key, value in availability.items()
            if key in DURABLE_DECIDER_AVAILABILITY_KEYS
        }
    return _strip_durable_replay_metadata(projected)


def durable_decider_source_context_fingerprint_matches(
    *,
    expected_fingerprint: Any,
    observed_fingerprint: Any,
    refreshed_source_context: dict[str, Any],
    expected_source_context: dict[str, Any] | None = None,
) -> bool:
    """Accept only known non-semantic legacy fingerprint differences.

    Older durable generations included ``macro_context.freshness``'s
    ``knowledge_cutoff_applied`` invocation marker. The knowledge boundary is
    independently bound by ``source_knowledge_cutoff_fingerprint``, so the
    marker itself is not source data. They also included replay timestamps from
    signal-feature provenance and evaluation evidence, plus the exchange master
    ``standard_code`` identifier that can be hydrated after EOD. Post-decision
    evaluation upserts can also change their own forced-inclusion explanation
    and global diagnostic count. Reconstructing only those persisted metadata
    values keeps every current eligibility gate and other material field in the
    projection, so real source changes are still rejected.
    """
    if not isinstance(expected_fingerprint, str) or not expected_fingerprint:
        return False
    if expected_fingerprint == observed_fingerprint:
        return True

    projected = _durable_decider_source_context(refreshed_source_context)
    legacy_metadata_variants = [
        projected,
        _restore_legacy_replay_metadata(
            projected,
            expected_source_context,
            restore_company_identifier=True,
            restore_replay_clocks=False,
        ),
        _restore_legacy_replay_metadata(
            projected,
            expected_source_context,
            restore_company_identifier=False,
            restore_replay_clocks=True,
        ),
        _restore_legacy_replay_metadata(
            projected,
            expected_source_context,
        ),
    ]
    evaluation_metadata_variants: list[dict[str, Any]] = []
    for metadata_variant in legacy_metadata_variants:
        restored_evaluation_metadata = (
            _restore_post_decision_evaluation_metadata(
                metadata_variant,
                expected_source_context,
            )
        )
        evaluation_metadata_variants.extend(
            [
                restored_evaluation_metadata,
                _strip_durable_replay_metadata(
                    restored_evaluation_metadata
                ),
            ]
        )
    metadata_variants = [
        *legacy_metadata_variants,
        *evaluation_metadata_variants,
    ]
    metadata_variants.extend(
        _restore_legacy_macro_freshness_metadata(
            metadata_variant,
            (
                expected_source_context
                if isinstance(expected_source_context, dict)
                else refreshed_source_context
            ),
        )
        for metadata_variant in list(metadata_variants)
    )
    legacy_projections = []
    for metadata_variant in metadata_variants:
        legacy_projections.append(metadata_variant)
        macro_context = metadata_variant.get('macro_context')
        freshness = (
            macro_context.get('freshness')
            if isinstance(macro_context, dict)
            else None
        )
        if isinstance(freshness, dict):
            for marker in (False, None):
                legacy_projection = deepcopy(metadata_variant)
                legacy_projection['macro_context']['freshness'][
                    'knowledge_cutoff_applied'
                ] = marker
                legacy_projections.append(legacy_projection)
    legacy_fingerprints = {
        build_decider_source_context_generation(legacy_projection)[
            'fingerprint'
        ]
        for legacy_projection in legacy_projections
    }
    return expected_fingerprint in legacy_fingerprints


def build_durable_decider_input_generation(
    *,
    source_context: dict[str, Any],
    source_knowledge_cutoff_at: str | None = None,
    policy_context: dict[str, Any] | None = None,
    previous_decision_context: dict[str, Any] | None = None,
    upstream_summaries: dict[str, Any] | None = None,
    upstream_traceability: dict[str, Any] | None = None,
) -> dict[str, Any]:
    market_breadth = source_context.get('market_breadth_context')
    market_breadth = market_breadth if isinstance(market_breadth, dict) else {}
    breadth_provenance = market_breadth.get('freshness_provenance')
    breadth_provenance = (
        breadth_provenance if isinstance(breadth_provenance, dict) else {}
    )
    context_provenance = source_context.get('context_provenance')
    context_provenance = (
        context_provenance if isinstance(context_provenance, dict) else {}
    )
    context_breadth_provenance = context_provenance.get(
        'market_breadth_context'
    )
    context_breadth_provenance = (
        context_breadth_provenance
        if isinstance(context_breadth_provenance, dict)
        else {}
    )
    breadth_sources = {
        str(value or '').strip().lower()
        for value in (
            market_breadth.get('source'),
            breadth_provenance.get('source_provider'),
            context_breadth_provenance.get('source_provider'),
        )
    }
    breadth_source_tables = {
        str(value or '').strip().lower()
        for value in (
            _source_names(breadth_provenance.get('source_tables'))
            + _source_names(context_breadth_provenance.get('source_tables'))
        )
    }
    non_durable_reasons: list[str] = []
    if (
        'kis_market_snapshot_v1' in breadth_sources
        or 'current_market_snapshot_cache' in breadth_source_tables
    ):
        non_durable_reasons.append('live_market_breadth_context')
    components = {
        'source_context_fingerprint': build_decider_source_context_generation(
            _durable_decider_source_context(source_context)
        )['fingerprint'],
        'policy_context_fingerprint': build_decider_source_context_generation(
            policy_context or {}
        )['fingerprint'],
        'previous_decision_context_fingerprint': (
            build_decider_source_context_generation(
                previous_decision_context or {}
            )['fingerprint']
        ),
        'upstream_bundle_fingerprint': build_decider_source_context_generation(
            {
                'summaries': upstream_summaries or {},
                'traceability': upstream_traceability or {},
            }
        )['fingerprint'],
        'source_knowledge_cutoff_fingerprint': (
            build_decider_source_context_generation(
                {'source_knowledge_boundary': source_knowledge_cutoff_at}
            )['fingerprint']
        ),
    }
    return {
        'schema_version': DURABLE_DECIDER_INPUT_GENERATION_SCHEMA_VERSION,
        'durable_reuse_eligible': not non_durable_reasons,
        'non_durable_reasons': non_durable_reasons,
        **components,
        'fingerprint': build_decider_source_context_generation(
            {
                'durable_reuse_eligible': not non_durable_reasons,
                'non_durable_reasons': non_durable_reasons,
                **components,
            }
        )['fingerprint'],
    }


def build_decider_input_generation(
    *,
    source_context: dict[str, Any],
    market_context: dict[str, Any],
    policy_context: dict[str, Any],
    previous_decision_context: dict[str, Any],
    upstream_summaries: dict[str, Any],
    upstream_traceability: dict[str, Any],
) -> dict[str, Any]:
    components = {
        'source_context_fingerprint': build_decider_source_context_generation(
            {
                **source_context,
                **(
                    {
                        'macro_context': _stable_macro_context(
                            source_context['macro_context']
                        )
                    }
                    if 'macro_context' in source_context
                    else {}
                ),
            }
        )['fingerprint'],
        'market_context_fingerprint': build_decider_source_context_generation(
            _stable_market_context(market_context)
        )['fingerprint'],
        'policy_context_fingerprint': build_decider_source_context_generation(
            policy_context
        )['fingerprint'],
        'previous_decision_context_fingerprint': (
            build_decider_source_context_generation(previous_decision_context)[
                'fingerprint'
            ]
        ),
        'upstream_bundle_fingerprint': build_decider_source_context_generation(
            {
                'summaries': upstream_summaries,
                'traceability': upstream_traceability,
            }
        )['fingerprint'],
    }
    return {
        'schema_version': DECIDER_INPUT_GENERATION_SCHEMA_VERSION,
        **components,
        'fingerprint': build_decider_source_context_generation(components)[
            'fingerprint'
        ],
    }


def feedback_row_lookup(rows: Sequence[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get('symbol') or '').strip()
        agent_name = str(row.get('agent_name') or '').strip()
        if not symbol or not agent_name:
            continue
        lookup[(symbol, agent_name)] = row
    return lookup


def request_scope_from_items(batch_items: Sequence[dict[str, Any]], key: str) -> str | None:
    values = {
        str(item.get(key)).strip()
        for item in batch_items
        if item.get(key) is not None and str(item.get(key)).strip()
    }
    if not values:
        return None
    if len(values) > 1:
        raise ValueError(f'batch items mixed {key}: {sorted(values)}')
    return next(iter(values))


def _canonical_symbol(value: Any) -> str | None:
    symbol = str(value or '').strip().upper()
    return symbol or None


def coerce_decoded_batch_response(decoded: dict[str, Any] | None) -> dict[str, Any] | None:
    """Normalize common agent output-shape mistakes before symbol correlation.

    Some agents occasionally return {"batch": {"items": [...]}} or put feedback
    fields directly on each item instead of under item.feedback. Preserve the
    response but coerce it into the canonical {"items": [{"symbol", "feedback"}]}
    envelope so downstream role-specific normalizers can apply their usual
    mismatch/risk-flag handling instead of failing the whole batch early.
    """
    if not isinstance(decoded, dict):
        return decoded

    working = decoded
    batch = working.get('batch')
    if isinstance(batch, dict) and isinstance(batch.get('items'), list):
        working = {**batch, '_coerced_from_batch_key': True}

    items = working.get('items')
    if not isinstance(items, list):
        return working

    normalized_items: list[Any] = []
    for item in items:
        if not isinstance(item, dict):
            normalized_items.append(item)
            continue
        if isinstance(item.get('feedback'), dict):
            normalized_items.append(item)
            continue
        symbol = item.get('symbol')
        feedback = {
            key: value
            for key, value in item.items()
            if key not in {'feedback'}
        }
        normalized_items.append({'symbol': symbol, 'feedback': feedback, '_coerced_feedback_from_item': True})
    return {**working, 'items': normalized_items}


def correlate_decoded_batch_response(
    *,
    decoded: dict[str, Any] | None,
    expected_symbols: Sequence[str],
    session_reuse_mode: str | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    expected_symbol_pairs = [
        (str(symbol).strip(), _canonical_symbol(symbol))
        for symbol in expected_symbols
        if str(symbol or '').strip()
    ]
    expected = [canonical for _, canonical in expected_symbol_pairs if canonical is not None]
    metadata: dict[str, Any] = {
        'expected_symbols': expected,
        'expected_item_count': len(expected),
        'response_item_count': 0,
        'item_count_matches': False,
        'parsed_symbols': [],
        'response_symbols': [],
        'duplicate_symbols': [],
        'item_symbol_mismatches': [],
        'missing_symbols': expected,
        'unexpected_symbols': [],
        'fallback_attributions': [],
        'structure_mismatch_flags': [],
        'attribution_status': 'not_decoded',
        'failure_class': 'invalid_json',
        'session_reuse_mode': session_reuse_mode,
    }
    if decoded is None:
        return None, metadata

    structure_mismatch_flags: list[str] = []
    if isinstance(decoded, dict) and decoded.get('_coerced_from_batch_key') is True:
        structure_mismatch_flags.append('batch_wrapper')
    if isinstance(decoded, dict) and decoded.get('_coerced_from_top_level_object') is True:
        structure_mismatch_flags.append('top_level_feedback_object')

    decoded_items = decoded.get('items') if isinstance(decoded, dict) else None
    if not isinstance(decoded_items, list):
        metadata['attribution_status'] = 'missing_items'
        metadata['failure_class'] = 'batch_response_missing_items'
        return decoded, metadata

    metadata['response_item_count'] = len(decoded_items)
    metadata['item_count_matches'] = len(decoded_items) == len(expected)

    canonical_by_symbol = {
        canonical: exact
        for exact, canonical in expected_symbol_pairs
        if canonical is not None
    }
    response_symbols: list[str] = []
    item_symbol_mismatches: list[dict[str, Any]] = []
    normalized_items: list[Any] = []
    for item_index, item in enumerate(decoded_items):
        if not isinstance(item, dict):
            normalized_items.append(item)
            continue
        copied = dict(item)
        if copied.get('_coerced_feedback_from_item') is True:
            structure_mismatch_flags.append(f'item_feedback_missing:{item_index}')
        raw_item_symbol = copied.get('symbol')
        response_symbol = str(raw_item_symbol) if raw_item_symbol is not None else None
        item_symbol = _canonical_symbol(raw_item_symbol)
        if item_symbol:
            response_symbols.append(item_symbol)
            if item_symbol in canonical_by_symbol:
                expected_exact_symbol = canonical_by_symbol[item_symbol]
                copied['symbol'] = expected_exact_symbol
                if response_symbol != expected_exact_symbol:
                    item_symbol_mismatches.append(
                        {
                            'item_index': item_index,
                            'expected_symbol': expected_exact_symbol,
                            'response_symbol': response_symbol,
                        }
                    )
        normalized_items.append(copied)

    normalized_decoded = {**decoded, 'items': normalized_items}
    response_symbol_set = set(response_symbols)
    expected_set = set(expected)
    missing_symbols = [symbol for symbol in expected if symbol not in response_symbol_set]
    unexpected_symbols = [symbol for symbol in response_symbols if symbol not in expected_set]
    seen_response_symbols: set[str] = set()
    duplicate_symbols: list[str] = []
    for response_symbol in response_symbols:
        if response_symbol in seen_response_symbols and response_symbol not in duplicate_symbols:
            duplicate_symbols.append(response_symbol)
        seen_response_symbols.add(response_symbol)
    item_count_matches = len(decoded_items) == len(expected)
    attribution_matches = (
        not missing_symbols
        and not unexpected_symbols
        and not duplicate_symbols
        and not item_symbol_mismatches
        and item_count_matches
        and not structure_mismatch_flags
    )

    metadata.update(
        {
            'parsed_symbols': response_symbols,
            'response_symbols': response_symbols,
            'missing_symbols': missing_symbols,
            'unexpected_symbols': unexpected_symbols,
            'duplicate_symbols': duplicate_symbols,
            'item_symbol_mismatches': item_symbol_mismatches,
            'structure_mismatch_flags': structure_mismatch_flags,
            'attribution_status': 'matched' if attribution_matches else 'mismatch',
            'failure_class': (
                None
                if attribution_matches
                else (
                    'batch_response_structure_mismatch'
                    if structure_mismatch_flags
                    else 'batch_response_attribution_mismatch'
                )
            ),
        }
    )
    return normalized_decoded, metadata


def slot_from_items(batch_items: Sequence[dict[str, Any]]) -> int | None:
    raw_slot = request_scope_from_items(batch_items, 'session_slot')
    if raw_slot is None:
        return None
    return int(raw_slot)


def extract_response_mismatch_flags(normalized_feedback: dict[str, Any] | None) -> list[str]:
    if not isinstance(normalized_feedback, dict):
        return []
    explicit_flags = [
        str(flag).strip()
        for key in ('structural_mismatch_flags', 'response_schema_violation_flags')
        for flag in (normalized_feedback.get(key) or [])
        if str(flag).strip()
    ]
    if explicit_flags:
        return list(dict.fromkeys(explicit_flags))
    risk_flags = normalized_feedback.get('risk_flags')
    if not isinstance(risk_flags, list):
        return []
    return [
        str(flag)
        for flag in risk_flags
        if isinstance(flag, str)
        and flag
        in {
            'role_missing',
            'role_mismatch',
            'agent_name_missing',
            'agent_name_mismatch',
            'symbol_missing',
            'symbol_mismatch',
            'market_date_missing',
            'market_date_mismatch',
            'traceability_missing',
            'traceability_symbol_missing',
            'traceability_symbol_mismatch',
        }
    ]


def failure_log_fields(failure: dict[str, Any]) -> dict[str, Any]:
    context = failure.get('context') if isinstance(failure.get('context'), dict) else {}
    diagnostics = failure.get('diagnostics') if isinstance(failure.get('diagnostics'), dict) else {}
    dispatch_diagnostics = (
        context.get('dispatch_diagnostics')
        if isinstance(context.get('dispatch_diagnostics'), dict)
        else {}
    )
    batch_symbols = context.get('batch_symbols')
    return {
        'dispatch_id': failure.get('dispatch_id') or context.get('dispatch_id'),
        'attempt_id': failure.get('attempt_id') or context.get('attempt_id'),
        'slot': failure.get('slot') or context.get('session_slot'),
        'session_key': failure.get('session_key') or context.get('session_key'),
        'session_id': (
            failure.get('session_id')
            or context.get('session_id')
            or diagnostics.get('session_id')
            or dispatch_diagnostics.get('session_id')
        ),
        'response_id': (
            failure.get('response_id')
            or context.get('response_id')
            or diagnostics.get('response_id')
            or dispatch_diagnostics.get('response_id')
        ),
        'request_marker': (
            failure.get('request_marker')
            or context.get('request_marker')
            or diagnostics.get('request_marker')
            or dispatch_diagnostics.get('request_marker')
        ),
        'batch_symbols': batch_symbols if isinstance(batch_symbols, list) else None,
        'diagnostics': diagnostics or dispatch_diagnostics or None,
    }


def build_completed_dispatch_item(
    *,
    role: str,
    agent_name: str,
    symbol: str,
    normalized_feedback: dict[str, Any],
    storage: dict[str, Any] | None = None,
    session_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = {
        'role': role,
        'agent_name': agent_name,
        'symbol': symbol,
        'status': 'completed',
        'response': {'normalized_feedback': normalized_feedback},
    }
    if storage is not None:
        item['storage'] = storage
    if session_ref is not None:
        item['session_ref'] = session_ref
    return item


def build_failed_dispatch_item(
    *,
    role: str,
    agent_name: str,
    symbol: str,
    error: str,
    retryable: bool,
    failure_kind: str | None = None,
    context: dict[str, Any] | None = None,
    dispatch_id: str | None = None,
    attempt_id: str | None = None,
    slot: int | None = None,
    session_key: str | None = None,
    session_id: str | None = None,
    response_id: str | None = None,
    request_marker: str | None = None,
    diagnostics: dict[str, Any] | None = None,
    batch_symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    item = {
        'role': role,
        'agent_name': agent_name,
        'symbol': symbol,
        'status': 'failed',
        'retryable': retryable,
        'error': error,
    }
    if failure_kind is not None:
        item['failure_kind'] = failure_kind
    if context is not None:
        item['context'] = context
    if dispatch_id is not None:
        item['dispatch_id'] = dispatch_id
    if attempt_id is not None:
        item['attempt_id'] = attempt_id
    if slot is not None:
        item['slot'] = slot
    if session_key is not None:
        item['session_key'] = session_key
    if session_id is not None:
        item['session_id'] = session_id
    if response_id is not None:
        item['response_id'] = response_id
    if request_marker is not None:
        item['request_marker'] = request_marker
    if diagnostics is not None:
        item['diagnostics'] = diagnostics
    if batch_symbols is not None:
        item['batch_symbols'] = list(batch_symbols)
    return item


def resolve_dispatch_status(*, completed: int, failed: int) -> str:
    if failed and completed:
        return 'partial'
    if failed:
        return 'failed'
    return 'completed'


def build_response_storage_payload(
    *,
    spec,
    batch_result: dict[str, Any],
    normalized_feedback: dict[str, Any],
    collector_mode: str,
) -> dict[str, Any]:
    raw_response = batch_result.get('response') if isinstance(batch_result.get('response'), dict) else None
    model_name = None
    if raw_response is not None:
        model_name = raw_response.get('model') or raw_response.get('model_name')
    resolved_model_name = model_name or f'openclaw/{spec.agent_name}'
    request_provenance = (
        batch_result.get('request_provenance')
        if isinstance(batch_result.get('request_provenance'), dict)
        else {}
    )
    canonical_contract_provenance = build_agent_contract_provenance(role=spec.role)
    model_identity = (
        normalized_feedback.get('model_identity')
        if isinstance(normalized_feedback.get('model_identity'), dict)
        else batch_result.get('model_identity')
        if isinstance(batch_result.get('model_identity'), dict)
        else request_provenance.get('model_identity')
        if isinstance(request_provenance.get('model_identity'), dict)
        else None
    )
    runtime_contract = (
        request_provenance.get('runtime_contract')
        if isinstance(request_provenance.get('runtime_contract'), dict)
        else build_runtime_contract_provenance(spec.role)
    )
    decision_provenance = {
        'model': resolved_model_name,
        'model_identity': model_identity,
        'prompt_sha256': request_provenance.get('prompt_sha256'),
        'prompt_chars': request_provenance.get('prompt_chars'),
        'contract_version': (
            request_provenance.get('contract_version')
            or canonical_contract_provenance['contract_version']
        ),
        'contract_sha256': (
            request_provenance.get('contract_sha256')
            or canonical_contract_provenance['contract_sha256']
        ),
        'role': request_provenance.get('role') or spec.role,
        'runtime_contract': runtime_contract,
    }
    return {
        'attempt': batch_result.get('attempt', 1),
        'attempt_id': batch_result.get('attempt_id'),
        'dispatch_id': batch_result.get('dispatch_id'),
        'request_run_id': batch_result.get('request_run_id'),
        'request_marker': batch_result.get('request_marker'),
        'market_date': batch_result.get('market_date'),
        'role': spec.role,
        'agent_name': spec.agent_name,
        'batch_symbols': batch_result.get('batch_symbols'),
        'slot': batch_result.get('slot'),
        'session_key': batch_result.get('session_ref', {}).get('session_key') if isinstance(batch_result.get('session_ref'), dict) else None,
        'session_id': batch_result.get('session_ref', {}).get('session_id') if isinstance(batch_result.get('session_ref'), dict) else None,
        'response_id': batch_result.get('session_ref', {}).get('response_id') if isinstance(batch_result.get('session_ref'), dict) else None,
        'started_at': batch_result.get('started_at'),
        'completed_at': batch_result.get('completed_at'),
        'model': resolved_model_name,
        'decision_provenance': decision_provenance,
        'output_text': batch_result.get('output_text'),
        'raw_response': raw_response,
        'normalized_feedback': normalized_feedback,
        'normalization_mismatch_flags': extract_response_mismatch_flags(normalized_feedback),
        'session_ref': batch_result.get('session_ref'),
        'response_correlation': batch_result.get('response_correlation'),
        'session_reuse_mode': batch_result.get('session_reuse_mode'),
        'collector_mode': collector_mode,
    }


def _compact_valuation_context_for_decider(context: dict[str, Any]) -> dict[str, Any]:
    compacted = dict(context)
    snapshot = context.get('valuation_snapshot')
    if not isinstance(snapshot, dict):
        return compacted

    compacted_snapshot = dict(snapshot)
    omitted_fields = [
        field
        for field in ('close_price', 'volume', 'trading_value')
        if field in compacted_snapshot
    ]
    for field in omitted_fields:
        compacted_snapshot.pop(field, None)
    compacted['valuation_snapshot'] = compacted_snapshot
    if omitted_fields:
        compacted['market_activity_reference'] = {
            'authority': 'symbol_day_context.price_context',
            'omitted_auxiliary_fields': omitted_fields,
            'reason': (
                'The valuation endpoint is authoritative for valuation metrics. Its '
                'repeated price and volume are not execution references, and any '
                'eligible raw trading value is exposed only through '
                'liquidity_context.turnover after Core source selection.'
            ),
        }
    return compacted


def _compact_sector_peer_context_for_decider(context: dict[str, Any]) -> dict[str, Any]:
    compacted = dict(context)
    omitted_fields = [
        field for field in ('close_price', 'change_pct') if field in compacted
    ]
    for field in omitted_fields:
        compacted.pop(field, None)
    self_entry = context.get('self')
    if isinstance(self_entry, dict):
        compacted_self = dict(self_entry)
        if 'change_pct' in compacted_self:
            compacted_self.pop('change_pct', None)
            omitted_fields.append('self.change_pct')
        compacted['self'] = compacted_self
    if omitted_fields:
        compacted['market_activity_reference'] = {
            'authority': 'symbol_day_context.price_context',
            'omitted_auxiliary_fields': omitted_fields,
            'reason': (
                'The sector endpoint is authoritative for sector membership and peer '
                'comparison only. Its self price/change fields are not independent '
                'execution evidence.'
            ),
        }
    return compacted


def _compact_symbol_day_context_for_decider(context: dict[str, Any]) -> dict[str, Any]:
    """Keep decision-relevant symbol context without shipping bulky raw provenance."""
    keep_keys = (
        'symbol',
        'market',
        'market_date',
        'exchange',
        'currency',
        'calendar_timezone',
        'company',
        'market_session',
        'market_data_phase_contract',
        'decider_data_quality_context',
        'decision_input_quality_context',
        'quote_context',
        'price_context',
        'liquidity_context',
        'technical_snapshot',
        'action_summary',
        'intraday_execution_context',
        'valuation_context',
        'intraday_wait_recheck_context',
        'investor_flow_context',
        'short_selling_context',
        'sector_peer_context',
        'market_relative_context',
        'market_breadth_context',
        'macro_context',
        'recent_disclosures',
        'recent_dart_events',
        'recent_external_news',
        'fundamentals',
        'price_screen',
        'fundamental_screen',
        'screening_decision',
    )
    compacted = {key: context.get(key) for key in keep_keys if key in context}
    valuation_context = compacted.get('valuation_context')
    if isinstance(valuation_context, dict):
        compacted['valuation_context'] = _compact_valuation_context_for_decider(
            valuation_context
        )
    sector_peer_context = compacted.get('sector_peer_context')
    if isinstance(sector_peer_context, dict):
        compacted['sector_peer_context'] = _compact_sector_peer_context_for_decider(
            sector_peer_context
        )
    completeness = context.get('context_completeness')
    if isinstance(completeness, dict):
        compacted['context_completeness'] = {
            key: completeness.get(key)
            for key in ('summary', 'missing_sections', 'critical_missing_sections', 'coverage_status')
            if key in completeness
        }
    provenance = context.get('context_provenance')
    if isinstance(provenance, dict):
        compacted['context_provenance'] = {
            key: provenance.get(key)
            for key in (
                'summary',
                'missing_sources',
                'stale_sources',
                'source_status_counts',
                'universe',
                'prices',
                'fundamentals',
                'collection_run_id',
                'screening_rank',
                'screening_score',
            )
            if key in provenance
        }
    return compacted


def _compact_factor_scores(scores: dict[str, Any]) -> dict[str, Any]:
    compacted: dict[str, Any] = {}
    keep_fields = {
        'score',
        'rank',
        'percentile',
        'status',
        'value_score',
        'value_rank',
        'value_percentile',
        'quality_score',
        'quality_rank',
        'quality_percentile',
        'momentum_score',
        'momentum_rank',
        'momentum_percentile',
        'signal_count',
        'volume_power_rank',
        'volume_power',
        'overtime_volume_rank',
        'overtime_volume',
        'overtime_fluctuation_rank',
        'overtime_fluctuation_rate',
    }
    for name, payload in scores.items():
        if isinstance(payload, dict):
            compacted[name] = {key: payload.get(key) for key in keep_fields if key in payload}
        else:
            compacted[name] = payload
    return compacted


def _compact_decider_data_quality_context(context: dict[str, Any]) -> dict[str, Any]:
    compacted = {
        key: context.get(key)
        for key in ('status', 'requested_market_date', 'block_status_counts', 'decision_priority')
        if key in context
    }
    blocks = context.get('blocks')
    if isinstance(blocks, dict):
        compacted['blocks'] = {
            name: {
                key: block.get(key)
                for key in ('status', 'decision_guidance', 'providers')
                if isinstance(block, dict) and key in block
            }
            for name, block in blocks.items()
            if isinstance(block, dict)
        }
    return compacted


def _compact_pre_decider_enrichment_context(context: dict[str, Any]) -> dict[str, Any]:
    compacted = {
        key: context.get(key)
        for key in ('schema_version', 'status', 'market_date')
        if key in context
    }
    for key in ('quote_orderbook_snapshot', 'trading_value'):
        value = context.get(key)
        if isinstance(value, dict):
            compacted[key] = value
    flow_context = context.get('flow_short_sector_context')
    if isinstance(flow_context, dict):
        compacted['flow_short_sector_context'] = {
            field: flow_context.get(field)
            for field in (
                'status',
                'summary',
                'investor_flow',
                'short_selling',
                'sector_relative_strength',
                'missing_fields',
                'risk_flags',
            )
            if field in flow_context
        }
    return compacted


def _compact_market_regime_context(context: dict[str, Any]) -> dict[str, Any]:
    compacted = {
        key: context.get(key)
        for key in (
            'schema_version',
            'status',
            'market_date',
            'market',
            'posture',
            'market_regime',
            'risk_mode',
            'regime_confidence',
            'regime_reasons',
            'reason_code',
            'reason_summary',
            'source_status',
            'recommended_exec_action',
            'recommended_sizing_multiplier',
            'quantity_multiplier',
            'entry_priority_boost',
            'valid_until',
            'sizing_window',
            'exec_guardrails',
            'missing_fields',
        )
        if key in context
    }
    for key in ('market_breadth', 'sector_context', 'relative_strength_context', 'open_drive_context', 'fx_context', 'freshness'):
        value = context.get(key)
        if isinstance(value, dict):
            compacted_value = dict(value)
            if key == 'sector_context' and 'self_change_pct' in compacted_value:
                compacted_value.pop('self_change_pct', None)
                compacted_value['market_activity_reference'] = {
                    'authority': 'symbol_day_context.price_context',
                    'omitted_auxiliary_fields': ['self_change_pct'],
                    'reason': (
                        'The market-regime sector context is comparative context. '
                        'Its repeated self return is not an independent execution '
                        'reference.'
                    ),
                }
            compacted[key] = compacted_value
    return compacted


_CANONICAL_MARKET_ACTIVITY_SOURCE_TOKENS = (
    'price_context',
    'technical_snapshot',
    'price context',
    'technical close',
    '가격·기술',
    '가격/기술',
    '기술 종가',
    '기준가격',
    '가격 컨텍스트',
    '가격 문맥',
    '가격 데이터',
    '가격 필드',
    '가격봉·기술',
    '가격봉/기술',
    '기술·가격',
    '기술/가격',
    '일봉·기술',
    '일봉/기술',
    '기술 일봉',
    '기술 스냅샷',
    '가격 자료',
    '일봉 종가',
)
_AUXILIARY_MARKET_ACTIVITY_SOURCE_TOKENS = (
    'liquidity_context',
    'valuation_context',
    'valuation_snapshot',
    'sector_peer_context',
    'liquidity context',
    'valuation endpoint',
    'sector endpoint',
    '유동성',
    '유동성 컨텍스트',
    '유동성 문맥',
    '밸류에이션',
    '가치평가',
    '평가 자료',
    '업종 종가',
    '업종 문맥',
    '업종 자료',
    '섹터 종가',
    '섹터 문맥',
)
_CROSS_SOURCE_CONFLICT_TOKENS = (
    'mismatch',
    'conflict',
    'inconsisten',
    'discrepan',
    'diverg',
    '불일치',
    '충돌',
    '상충',
    '비정합',
    '달라',
    '다르',
    '다릅',
    'different',
    'differ',
    'does not match',
    'do not match',
    'not match',
    '일치하지',
)
_CROSS_SOURCE_CONFLICT_PATTERNS = (
    r'(?<![가-힣A-Za-z0-9_])상이(?:함|하|한|해|했|였|$|\s|[.,])',
)
_CORE_REVALIDATION_RUNTIME_PHRASES = (
    'source revalidation',
    'source_revalidation',
    'source-revalidation',
    'required revalidation',
    'revalidation pass',
    'revalidation result',
    'generation fence',
    'decision_input_generation_fence',
    'source generation',
    '원천 재검증',
    '필수 재검증',
    '필수 입력 재검증',
    '필수 입력 세대 재검증',
    '입력 생성 펜스의 재검증',
    '입력 생성 펜스 재검증',
    '요구된 재검증',
    'core 재검증',
    '원천 검증 완료',
    '동일한 원천 종가 정합성 검증',
    '원천 데이터 재검증',
)
_GENERIC_REVALIDATION_STATUS_PHRASES = (
    '재검증 완료',
    '재검증 통과',
    '재검증 결과',
)
_CORE_REVALIDATION_CONTEXT_TOKENS = (
    'core',
    '코어',
    'source',
    '원천',
    'fence',
    '펜스',
    'generation',
    '입력 생성',
)
_PREVIOUS_CONTEXT_STALE_EXECUTION_PHRASES = (
    '가격 자료 정합성',
    '자료 원천별로 달라',
    '원천별로 달라',
    '원천 충돌이 지속되면',
)
_STALE_MARKET_ACTIVITY_TOKENS = (
    '종가',
    '가격',
    'close_price',
    'reference_close',
    '거래대금',
    'turnover',
)
_MARKET_ACTIVITY_MEASURE_TOKENS = (
    '종가',
    'close_price',
    'reference_close',
    '거래량',
    'volume',
    '거래대금',
    'turnover',
)
_STALE_SOURCE_IDENTITY_TOKENS = (
    '원천',
    '자료원',
    '자료 경로',
    '데이터',
    '필드',
    '입력',
    '컨텍스트',
    '문맥',
    '기준값',
    '기준 가격',
    '기준가격',
    '가격 기준',
    '종가 기준',
    '동일 거래일',
    '동일 시장일',
    '단일 소스',
    '단일 원천',
    '산출 경로',
    '산출 기준',
    '단일 기준',
    '정합성',
    '병합',
)
_STALE_PRICE_CONFLICT_PATTERNS = (
    r'종가(?:도|가|는|의)?\s*(?:값\s*)?(?:불일치|충돌|상충|비정합)',
    r'종가\s*차이',
    r'(?:개별\s*)?가격\s*(?:불일치|충돌|상충|비정합)',
    (
        r'종가.{0,100}\d[\d,]*(?:원)?\s*(?:과|와|/|·|,)\s*'
        r'\d[\d,]*(?:원)?.{0,60}'
        r'(?:불일치|충돌|상충|차이|달라|다릅|일치하지|differ|not match)'
    ),
    (
        r'가격\s*정합성\s*'
        r'(?:불일치|충돌|상충|문제|이슈|해소|미확보|부족|검증|입증|정합화)'
    ),
    (
        r'거래대금(?:도)?.{0,100}\d[\d,]*(?:원)?\s*(?:과|와|/|·|,)\s*'
        r'\d[\d,]*(?:원)?.{0,60}(?:불일치|충돌|상충|차이)'
    ),
    r'거래대금.{0,40}회전율.{0,40}(?:불일치|충돌|상충)',
    r'(?:가격|종가).{0,30}거래대금.{0,30}기준.{0,30}(?:불일치|충돌|상충|차이)',
    r'거래대금.{0,60}(?:저장|산출|자료)\s*경로별.{0,30}차이',
    (
        r'종가.{0,40}거래대금.{0,60}공매도.{0,60}'
        r'(?:범위|집계).{0,40}차이.{0,80}'
        r'(?:신뢰|판단).{0,40}(?:제한|부족)'
    ),
)
_STALE_PRICE_NORMALIZATION_PATTERNS = (
    r'일관된\s*단일\s*종가',
    r'정합화된\s*가격',
    (
        r'종가.{0,80}(?:단일\s*(?:기준값|소스|원천)|산출\s*경로)'
        r'.{0,60}(?:정규화|정합화|단일화)'
    ),
    (
        r'(?:정합성|정합화|정규화|단일화).{0,100}'
        r'(?:해소|해야|필요|확보|먼저|검증\s*추가|'
        r'사용하도록|한\s*(?:뒤|후)|\s후|'
        r'확인(?:이\s*필요|해야|되어야|$))'
    ),
)
_INTERNAL_TECHNICAL_LEVEL_PATTERNS = (
    r'(?:저항|지지)(?:값|선)?.{0,60}현재\s*종가',
    r'현재\s*종가.{0,60}(?:저항|지지)(?:값|선)?',
)
_DECISIVE_SOURCE_IDENTITY_TOKENS = (
    '필드',
    '원천',
    '자료원',
    '자료 경로',
    '데이터',
    '기준가격',
    '기준 가격',
    '가격 기준',
    '입력',
    '산출 기준',
)
_STRUCTURED_BLOCKER_TOKENS = (
    '대기',
    'wait',
    'block',
    '제한',
    '불가',
    '부분',
    '근거 부족',
    '해소 전',
    '정정',
    '재검증',
)
_STRUCTURED_CROSS_LEAF_FIELDS = (
    'finding',
    'decision',
    'reason',
    'warning',
    'blocker',
)


def _collect_string_leaves(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [
            leaf
            for nested_value in value.values()
            for leaf in _collect_string_leaves(nested_value)
        ]
    if isinstance(value, list):
        return [
            leaf
            for nested_value in value
            for leaf in _collect_string_leaves(nested_value)
        ]
    return []


def _has_cross_source_conflict(value: str) -> bool:
    return any(token in value for token in _CROSS_SOURCE_CONFLICT_TOKENS) or any(
        re.search(pattern, value) for pattern in _CROSS_SOURCE_CONFLICT_PATTERNS
    )


def _collect_immediate_structured_conflict_text(value: dict[str, Any]) -> str:
    leaves: list[str] = []
    for key, nested_value in value.items():
        normalized_key = str(key).casefold()
        if isinstance(nested_value, str):
            leaves.append(nested_value)
        elif (
            'source' in normalized_key
            or normalized_key in {'sources', 'primary', 'secondary'}
        ):
            leaves.extend(_collect_string_leaves(nested_value))
    return ' '.join(leaves)


def _has_core_revalidation_runtime_claim(value: str) -> bool:
    if any(phrase in value for phrase in _CORE_REVALIDATION_RUNTIME_PHRASES):
        return True
    return any(
        phrase in value for phrase in _GENERIC_REVALIDATION_STATUS_PHRASES
    ) and any(token in value for token in _CORE_REVALIDATION_CONTEXT_TOKENS)


def _noncanonical_execution_claim_reason_codes(
    value: str,
    *,
    section_name: str | None = None,
) -> list[str]:
    normalized = value.casefold()
    reason_codes: list[str] = []
    if (
        any(
            token in normalized
            for token in _CANONICAL_MARKET_ACTIVITY_SOURCE_TOKENS
        )
        and any(
            token in normalized
            for token in _AUXILIARY_MARKET_ACTIVITY_SOURCE_TOKENS
        )
        and _has_cross_source_conflict(normalized)
    ):
        reason_codes.append('noncanonical_market_activity_conflict')
    if _has_core_revalidation_runtime_claim(normalized):
        reason_codes.append('core_post_response_revalidation_claim')
    if section_name == 'previous_decision_context' and any(
        phrase in normalized for phrase in _PREVIOUS_CONTEXT_STALE_EXECUTION_PHRASES
    ):
        reason_codes.append('previous_noncanonical_execution_claim')
    has_market_activity = any(
        token in normalized for token in _STALE_MARKET_ACTIVITY_TOKENS
    )
    has_market_activity_measure = any(
        token in normalized for token in _MARKET_ACTIVITY_MEASURE_TOKENS
    )
    has_source_identity = any(
        token in normalized for token in _STALE_SOURCE_IDENTITY_TOKENS
    )
    has_conflict = _has_cross_source_conflict(normalized)
    has_cross_source_difference = bool(
        has_market_activity_measure
        and '차이' in normalized
        and (
            (
                any(
                    token in normalized
                    for token in _AUXILIARY_MARKET_ACTIVITY_SOURCE_TOKENS
                )
                and (
                    any(
                        token in normalized
                        for token in _CANONICAL_MARKET_ACTIVITY_SOURCE_TOKENS
                    )
                    or any(token in normalized for token in ('일봉', '기술', '당일'))
                )
            )
            or bool(
                re.search(
                    r'(?:당일|동일\s*(?:거래일|시장일)).{0,100}원천.{0,60}차이',
                    normalized,
                )
            )
        )
    )
    has_named_source_difference = bool(
        '차이' in normalized
        and re.search(
            (
                r'(?:가격[\s·/]*기술|기술[\s·/]*가격)'
                r'.{0,100}원천.{0,100}'
                r'(?:유동성|평가|가치평가|업종|섹터)'
                r'.{0,100}원천.{0,60}차이'
            ),
            normalized,
        )
    )
    is_internal_technical_level_relation = (
        any(
            re.search(pattern, normalized)
            for pattern in _INTERNAL_TECHNICAL_LEVEL_PATTERNS
        )
        and not any(
            token in normalized for token in _DECISIVE_SOURCE_IDENTITY_TOKENS
        )
    )
    if (
        (
            has_market_activity
            and has_source_identity
            and has_conflict
            and not is_internal_technical_level_relation
        )
        or any(
            re.search(pattern, normalized)
            for pattern in _STALE_PRICE_CONFLICT_PATTERNS
        )
        or has_cross_source_difference
        or has_named_source_difference
        or (
            has_market_activity
            and any(
                re.search(pattern, normalized)
                for pattern in _STALE_PRICE_NORMALIZATION_PATTERNS
            )
        )
    ):
        reason_codes.append('stale_price_conflict_claim')
    return reason_codes


def _sanitize_noncanonical_execution_narratives(
    value: Any,
    *,
    section_name: str | None = None,
) -> tuple[Any, int]:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        count = 0
        direct_blocker_items = {
            str(key): nested_value
            for key, nested_value in value.items()
            if isinstance(nested_value, str)
            and str(key).casefold() in _STRUCTURED_CROSS_LEAF_FIELDS
        }
        direct_text = _collect_immediate_structured_conflict_text(value)
        structured_reason_codes = _noncanonical_execution_claim_reason_codes(
            direct_text,
            section_name=section_name,
        )
        structured_conflict = bool(
            direct_blocker_items
            and set(structured_reason_codes).intersection(
                {
                    'noncanonical_market_activity_conflict',
                    'previous_noncanonical_execution_claim',
                    'stale_price_conflict_claim',
                }
            )
            and any(token in direct_text.casefold() for token in _STRUCTURED_BLOCKER_TOKENS)
        )
        for key, nested_value in value.items():
            normalized_key = str(key).casefold()
            normalized_nested_value = (
                nested_value.casefold() if isinstance(nested_value, str) else ''
            )
            if (
                structured_conflict
                and isinstance(nested_value, str)
                and normalized_key in _STRUCTURED_CROSS_LEAF_FIELDS
                and not _noncanonical_execution_claim_reason_codes(
                    nested_value,
                    section_name=section_name,
                )
                and normalized_key == 'finding'
                and any(
                    token in normalized_nested_value
                    for token in _MARKET_ACTIVITY_MEASURE_TOKENS
                )
                and _has_cross_source_conflict(normalized_nested_value)
            ):
                sanitized[key] = (
                    '[noncanonical_execution_claim_withheld:'
                    'structured_noncanonical_market_activity_conflict; '
                    'use current canonical contexts and quality hard blocks]'
                )
                count += 1
                continue
            sanitized_value, nested_count = _sanitize_noncanonical_execution_narratives(
                nested_value,
                section_name=section_name,
            )
            sanitized[key] = sanitized_value
            count += nested_count
        return sanitized, count
    if isinstance(value, list):
        sanitized_items: list[Any] = []
        count = 0
        for nested_value in value:
            sanitized_value, nested_count = _sanitize_noncanonical_execution_narratives(
                nested_value,
                section_name=section_name,
            )
            sanitized_items.append(sanitized_value)
            count += nested_count
        return sanitized_items, count
    if isinstance(value, str):
        reason_codes = _noncanonical_execution_claim_reason_codes(
            value,
            section_name=section_name,
        )
        if reason_codes:
            return (
                '[noncanonical_execution_claim_withheld:'
                + ','.join(reason_codes)
                + '; use current canonical contexts and quality hard blocks]',
                1,
            )
    return value, 0


def trim_batch_item_for_prompt(item: dict[str, Any]) -> dict[str, Any]:
    trimmed = json.loads(json.dumps(item, ensure_ascii=False))
    source_revalidation_request = trimmed.pop('source_revalidation_request', None)
    decision_input_generation_fence = trimmed.pop(
        'decision_input_generation_fence',
        None,
    )
    if isinstance(source_revalidation_request, dict) or isinstance(
        decision_input_generation_fence,
        dict,
    ):
        trimmed['core_post_response_revalidation'] = {
            'status': 'enforced_by_core_after_model_response',
            'decision_effect': 'not_a_pre_decision_evidence_gap',
            'guidance': (
                'Core revalidates the durable input generation after the model '
                'responds. Do not wait or reject solely because this runtime control '
                'exists; use the supplied decision-input quality hard blocks instead.'
            ),
        }
    sanitized_claim_count = 0
    sanitized_sections: list[str] = []
    narrative_sections = (
        'upstream_summaries',
        'previous_decision_context',
        'signal_envelopes',
    )
    for section_name in narrative_sections:
        section = trimmed.get(section_name)
        if not isinstance(section, (dict, list)):
            continue
        sanitized_section, section_claim_count = (
            _sanitize_noncanonical_execution_narratives(
                section,
                section_name=section_name,
            )
        )
        trimmed[section_name] = sanitized_section
        if section_claim_count:
            sanitized_claim_count += section_claim_count
            sanitized_sections.append(section_name)
    if any(
        isinstance(trimmed.get(section_name), (dict, list))
        for section_name in narrative_sections
    ):
        trimmed['narrative_execution_authority'] = {
            'status': 'noncanonical_narratives_are_not_current_execution_evidence',
            'current_authority_paths': [
                'symbol_day_context.price_context',
                'symbol_day_context.technical_snapshot',
                'liquidity_context.turnover',
                'decision_input_quality_context.execution_feasibility',
                'decision_input_quality_context.final_judgment',
                'decision_input_quality_context.final_judgment.hard_blocks',
            ],
            'sanitized_claim_count': sanitized_claim_count,
            'sanitized_sections': sanitized_sections,
            'guidance': (
                'Use upstream summaries, previous decisions, and signal envelopes '
                'for thesis continuity and delta only. Do not inherit their '
                'price-conflict or Core-revalidation claims as current blockers.'
            ),
        }
    symbol_day_context = trimmed.get('symbol_day_context')
    if isinstance(symbol_day_context, dict):
        compact_symbol_context = _compact_symbol_day_context_for_decider(symbol_day_context)
        trimmed_symbol_context = trim_batch_item_for_prompt(compact_symbol_context)
        if isinstance(compact_symbol_context.get('context_provenance'), dict):
            trimmed_symbol_context['context_provenance'] = compact_symbol_context['context_provenance']
        trimmed['symbol_day_context'] = trimmed_symbol_context
    else:
        bulky_duplicate_keys = {
            'context_provenance',
            'persisted_factor_rows',
            'price_context_history',
        }
        for key in bulky_duplicate_keys:
            if key in trimmed:
                trimmed[key] = {
                    'compressed_payload': _compress_prompt_value(trimmed[key]),
                    'source_field': key,
                    'note': 'Bulky supporting context compressed for prompt transport; not dropped.',
                }
    scores = trimmed.get('scores')
    if isinstance(scores, dict):
        trimmed['scores'] = _compact_factor_scores(scores)
    factor_summary = trimmed.get('factor_summary')
    if isinstance(factor_summary, dict):
        factor_scores = factor_summary.get('factor_scores')
        if isinstance(factor_scores, dict):
            factor_summary['factor_scores'] = _compact_factor_scores(factor_scores)
    decider_data_quality_context = trimmed.get('decider_data_quality_context')
    if isinstance(decider_data_quality_context, dict):
        trimmed['decider_data_quality_context'] = _compact_decider_data_quality_context(decider_data_quality_context)
    pre_decider_enrichment_context = trimmed.get('pre_decider_enrichment_context')
    if isinstance(pre_decider_enrichment_context, dict):
        trimmed['pre_decider_enrichment_context'] = _compact_pre_decider_enrichment_context(pre_decider_enrichment_context)
    market_regime_context = trimmed.get('market_regime_context')
    if isinstance(market_regime_context, dict):
        trimmed['market_regime_context'] = _compact_market_regime_context(market_regime_context)
    technical_snapshot = trimmed.get('technical_snapshot')
    if isinstance(technical_snapshot, dict):
        technical_snapshot.pop('signal_payload', None)
    action_summary = trimmed.get('action_summary')
    if isinstance(action_summary, dict):
        action_summary.pop('summary_payload', None)
    fundamentals = trimmed.get('fundamentals')
    if isinstance(fundamentals, dict):
        latest_snapshot = fundamentals.get('latest_snapshot')
        if isinstance(latest_snapshot, dict):
            latest_snapshot.pop('raw_rows', None)
            statement = latest_snapshot.get('statement')
            if isinstance(statement, dict):
                keep_statement_keys = {'fs_div', 'fs_label', 'statement_type', 'currency', 'source_status', 'source_message', 'row_count'}
                latest_snapshot['statement'] = {k: v for k, v in statement.items() if k in keep_statement_keys}
            highlights = latest_snapshot.get('highlights')
            if isinstance(highlights, dict):
                latest_snapshot['highlights'] = {
                    key: highlights.get(key)
                    for key in (
                        'revenue',
                        'operating_profit',
                        'net_income',
                        'total_assets',
                        'total_liabilities',
                        'equity',
                        'operating_cash_flow',
                        'free_cash_flow',
                        'shares_outstanding_growth_yoy',
                        'summary',
                        'data_quality',
                    )
                    if key in highlights
                }
            fundamental_context = latest_snapshot.get('fundamental_context')
            if isinstance(fundamental_context, dict):
                latest_snapshot['fundamental_context'] = {
                    key: fundamental_context.get(key)
                    for key in (
                        'quality_summary',
                        'growth_summary',
                        'balance_sheet_summary',
                        'cash_flow_summary',
                        'risk_flags',
                        'missing_fields',
                    )
                    if key in fundamental_context
                }
        history_summary = fundamentals.get('history_summary')
        if isinstance(history_summary, dict):
            recent_periods = history_summary.get('recent_periods')
            if isinstance(recent_periods, list):
                history_summary['recent_periods'] = recent_periods[:3]
    recent_disclosures = trimmed.get('recent_disclosures')
    if isinstance(recent_disclosures, dict):
        items = recent_disclosures.get('items')
        if isinstance(items, list):
            recent_disclosures['items'] = items[:3]
            recent_disclosures['count'] = min(recent_disclosures.get('count', len(items)), len(recent_disclosures['items']))
    recent_dart_events = trimmed.get('recent_dart_events')
    if isinstance(recent_dart_events, dict):
        items = recent_dart_events.get('items')
        if isinstance(items, list):
            recent_dart_events['items'] = items[:3]
            recent_dart_events['item_count'] = min(recent_dart_events.get('item_count', len(items)), len(recent_dart_events['items']))
    recent_external_news = trimmed.get('recent_external_news')
    if isinstance(recent_external_news, dict):
        for key in ('material_items', 'unknown_items'):
            grouped_items = recent_external_news.get(key)
            if isinstance(grouped_items, list):
                recent_external_news[key] = grouped_items[:3]
        noise_filter = recent_external_news.get('noise_filter')
        if isinstance(noise_filter, dict):
            noise_items = noise_filter.get('items')
            if isinstance(noise_items, list):
                noise_filter['items'] = noise_items[:2]
                noise_filter['included_count'] = min(int(noise_filter.get('included_count') or len(noise_items)), len(noise_filter['items']))
                noise_filter['omitted_count'] = max(0, int(noise_filter.get('item_count') or len(noise_items)) - len(noise_filter['items']))
        items = recent_external_news.get('items')
        if isinstance(items, list):
            recent_external_news['items'] = items[:3]
            recent_external_news['item_count'] = min(recent_external_news.get('item_count', len(items)), len(recent_external_news['items']))
    return trimmed


_PROMPT_COMPRESS_RAW_KEYS = {
    'raw',
    'raw_item',
    'raw_items',
    'raw_payload',
    'raw_response',
    'raw_rows',
    'source_payload',
    'full_payload',
    'context_provenance',
    'persisted_factor_rows',
    'price_context_history',
    'request_payload',
    'response_payload',
    'summary_payload',
}

_PROMPT_SLIM_PRIORITY_KEYS = (
    'symbol',
    'market_date',
    'agent_name',
    'role',
    'preferred_action',
    'next_day_open_action',
    'tactical_action',
    'decision_action',
    'wait_reason',
    'confidence',
    'confidence_score_10',
    'conviction_score_10',
    'summary',
    'thesis_summary',
    'target_price',
    'stop_loss_price',
    'trigger_price',
    'chase_limit_price',
    'expected_return_pct',
    'risk_flags',
    'supporting_evidence',
    'decision_risks',
    'tactical_risks',
    'data_quality_warnings',
    'specialist_disagreement',
    'traceability',
    'scores',
    'factor_summary',
    'symbol_day_context',
    'liquidity_context',
    'macro_context',
    'market_regime_context',
    'decision_input_quality_context',
    'decider_data_quality_context',
    'signal_envelopes',
    'market_context',
    'core_post_response_revalidation',
    'narrative_execution_authority',
    'pre_decider_enrichment_context',
    'upstream_summaries',
    'upstream_traceability',
    'previous_decision_context',
)


def _truncate_prompt_string(value: str, *, limit: int) -> str:
    if len(value) <= limit:
        return value
    return f'{value[: max(0, limit - 32)]}...[truncated {len(value) - limit} chars]'


def _stable_prompt_hash(value: Any) -> str:
    encoded = json.dumps(jsonable_encoder(value), ensure_ascii=False, sort_keys=True, default=str)
    return 'sha256:' + hashlib.sha256(encoded.encode('utf-8')).hexdigest()[:16]


def _text_evidence_summary(value: str) -> dict[str, Any]:
    stripped = value.strip()
    sentences = [part.strip() for part in re.split(r'(?<=[.!?。！？])\s+|\n+', stripped) if part.strip()]
    if not sentences:
        sentences = [stripped] if stripped else []
    snippets: list[str] = []
    for sentence in sentences:
        if sentence in snippets:
            continue
        snippets.append(_truncate_prompt_string(sentence, limit=260))
        if len(snippets) >= 5:
            break
    if len(stripped) > 800 and stripped[-260:].strip() not in snippets:
        snippets.append(_truncate_prompt_string(stripped[-260:].strip(), limit=260))
    return {
        'compression': 'text_evidence_summary_v1',
        'char_count': len(value),
        'hash': _stable_prompt_hash(value),
        'snippets': snippets,
    }


def _scalar_summary(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate_prompt_string(value, limit=220)
    return _truncate_prompt_string(str(value), limit=220)


def _compressed_list_summary(value: list[Any], *, depth: int) -> dict[str, Any]:
    numeric_values = [item for item in value if isinstance(item, (int, float)) and not isinstance(item, bool)]
    scalar_counts: dict[str, int] = {}
    for item in value:
        if item is None or isinstance(item, (bool, int, float, str)):
            label = str(_scalar_summary(item))
            scalar_counts[label] = scalar_counts.get(label, 0) + 1
    summary: dict[str, Any] = {
        'compression': 'list_summary_v1',
        'item_count': len(value),
        'hash': _stable_prompt_hash(value),
        'samples': [_compress_prompt_value(item, depth=depth + 1) for item in value[:5]],
    }
    if len(value) > 5:
        summary['tail_sample'] = _compress_prompt_value(value[-1], depth=depth + 1)
    if numeric_values:
        summary['numeric_range'] = {
            'min': min(numeric_values),
            'max': max(numeric_values),
            'avg': sum(float(item) for item in numeric_values) / len(numeric_values),
        }
    if scalar_counts:
        summary['top_scalar_values'] = [
            {'value': key, 'count': count}
            for key, count in sorted(scalar_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
        ]
    return summary


def _compressed_dict_summary(value: dict[str, Any], *, depth: int) -> dict[str, Any]:
    priority_payload = {
        key: _compress_prompt_value(value[key], depth=depth + 1)
        for key in _PROMPT_SLIM_PRIORITY_KEYS
        if key in value
    }
    numeric_fields: dict[str, Any] = {}
    scalar_fields: dict[str, Any] = {}
    nested_shapes: dict[str, Any] = {}
    for key, item in value.items():
        key_text = str(key)
        if key_text in priority_payload or key_text.startswith('_'):
            continue
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            numeric_fields[key_text] = item
        elif item is None or isinstance(item, (bool, str)):
            scalar_fields[key_text] = _scalar_summary(item)
        elif isinstance(item, list):
            nested_shapes[key_text] = {'type': 'list', 'item_count': len(item)}
        elif isinstance(item, dict):
            nested_shapes[key_text] = {'type': 'dict', 'key_count': len(item), 'keys': list(item.keys())[:12]}
    summary: dict[str, Any] = {
        'compression': 'dict_summary_v1',
        'key_count': len(value),
        'keys': list(value.keys())[:30],
        'hash': _stable_prompt_hash(value),
    }
    if priority_payload:
        summary['priority_fields'] = priority_payload
    if numeric_fields:
        summary['numeric_fields'] = numeric_fields
    if scalar_fields:
        summary['scalar_fields'] = scalar_fields
    if nested_shapes:
        summary['nested_shapes'] = nested_shapes
    return summary


def _compress_prompt_value(value: Any, *, depth: int = 0) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        if len(value) > (1600 if depth <= 2 else 700):
            return _text_evidence_summary(value)
        return value
    if isinstance(value, list):
        if len(value) > (8 if depth <= 2 else 4):
            return _compressed_list_summary(value, depth=depth)
        return [_compress_prompt_value(item, depth=depth + 1) for item in value]
    if isinstance(value, dict):
        if depth >= 5:
            return _compressed_dict_summary(value, depth=depth)

        ordered_keys = [
            key for key in _PROMPT_SLIM_PRIORITY_KEYS if key in value
        ]
        ordered_keys.extend(
            key
            for key in value
            if key not in set(ordered_keys)
            and not str(key).startswith('_')
        )
        limit = 20 if depth == 0 else 18 if depth == 1 else 10
        selected_keys = ordered_keys[:limit]
        compact = {
            str(key): (
                {
                    'compressed_payload': _compress_prompt_value(value[key], depth=depth + 1),
                    'source_field': str(key),
                    'note': 'Large/raw field compressed for prompt transport; not dropped.',
                }
                if str(key) in _PROMPT_COMPRESS_RAW_KEYS
                else _compress_prompt_value(value[key], depth=depth + 1)
            )
            for key in selected_keys
        }
        dropped_count = max(0, len(ordered_keys) - len(selected_keys))
        if dropped_count:
            remaining = {str(key): value[key] for key in ordered_keys[limit:]}
            compact['compressed_remaining_fields'] = _compressed_dict_summary(remaining, depth=depth + 1)
        return compact
    return _text_evidence_summary(str(value))


def compressed_batch_item_for_prompt(item: dict[str, Any]) -> dict[str, Any]:
    """Return a bounded one-symbol prompt payload without silently dropping raw evidence."""
    trimmed = trim_batch_item_for_prompt(item)
    compressed = _compress_prompt_value(trimmed)
    if not isinstance(compressed, dict):
        return {'payload': compressed}
    dispatched_policy = trimmed.get('signal_decider_policy')
    if isinstance(dispatched_policy, dict):
        compressed['signal_decider_policy'] = dispatched_policy
    trimmed_symbol_context = trimmed.get('symbol_day_context')
    compressed_symbol_context = compressed.get('symbol_day_context')
    wait_recheck_context = (
        trimmed_symbol_context.get('intraday_wait_recheck_context')
        if isinstance(trimmed_symbol_context, dict)
        else None
    )
    if (
        isinstance(wait_recheck_context, dict)
        and isinstance(compressed_symbol_context, dict)
    ):
        compressed_symbol_context['intraday_wait_recheck_context'] = (
            wait_recheck_context
        )
    return compressed


def slim_batch_item_for_prompt(item: dict[str, Any]) -> dict[str, Any]:
    """Backward-compatible alias for older tests/imports."""
    return compressed_batch_item_for_prompt(item)


def build_upstream_feedback_bundle(
    *,
    symbol: str,
    market_date: date,
    buffered_lookup: dict[tuple[str, str], dict[str, Any]],
    upstream_reuse_decisions: dict[str, dict[str, Any]],
    upstream_summary_roles: Sequence[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[str]]:
    upstream_summaries: dict[str, dict[str, Any]] = {}
    upstream_traceability: dict[str, dict[str, Any]] = {}
    missing_roles: list[str] = []
    symbol_decisions = upstream_reuse_decisions.get(symbol, {})

    for role in upstream_summary_roles:
        buffered_row = buffered_lookup.get((symbol, role))
        if buffered_row is not None:
            feedback_payload = buffered_row.get('feedback') if isinstance(buffered_row.get('feedback'), dict) else None
            if feedback_payload:
                upstream_summaries[role] = feedback_payload
                upstream_traceability[role] = {
                    'agent_name': role,
                    'run_id': buffered_row.get('run_id'),
                    'market_date': market_date.isoformat(),
                    'source': 'current_run_buffer',
                }
                continue

        decision = symbol_decisions.get(role)
        latest_feedback = getattr(decision, 'latest_feedback', None) if decision is not None else None
        feedback_payload = latest_feedback.get('feedback_payload') if isinstance(latest_feedback, dict) else None
        feedback_market_date = latest_feedback.get('market_date') if isinstance(latest_feedback, dict) else None
        reuse_reason = str(getattr(decision, 'reason', '') or '')
        if (
            isinstance(feedback_payload, dict)
            and feedback_market_date == market_date.isoformat()
            and decision is not None
            and not bool(decision.should_dispatch)
        ):
            upstream_summaries[role] = feedback_payload
            upstream_traceability[role] = {
                'agent_name': role,
                'run_id': latest_feedback.get('run_id'),
                'market_date': feedback_market_date,
                'received_at': latest_feedback.get('received_at'),
                'feedback_id': latest_feedback.get('id'),
                'source': 'persisted_feedback',
                'reuse_reason': reuse_reason or None,
                'refresh_pending': False,
            }
            continue
        if decision is not None and bool(decision.should_dispatch):
            missing_roles.append(role)
            continue
        fingerprint_reuse_allowed = (
            role == 'fundamental-summarizer'
            and reuse_reason == 'reuse_unchanged_fundamental_inputs'
            and decision is not None
            and not bool(decision.should_dispatch)
        )
        if isinstance(feedback_payload, dict) and (
            feedback_market_date == market_date.isoformat() or fingerprint_reuse_allowed
        ):
            upstream_summaries[role] = feedback_payload
            upstream_traceability[role] = {
                'agent_name': role,
                'run_id': latest_feedback.get('run_id'),
                'market_date': feedback_market_date,
                'received_at': latest_feedback.get('received_at'),
                'feedback_id': latest_feedback.get('id'),
                'source': 'persisted_feedback_fingerprint_reuse' if fingerprint_reuse_allowed else 'persisted_feedback',
                'reuse_reason': reuse_reason if fingerprint_reuse_allowed else None,
                'input_fingerprint': getattr(decision, 'input_fingerprint', None) if fingerprint_reuse_allowed else None,
                'stored_input_fingerprint': getattr(decision, 'stored_input_fingerprint', None) if fingerprint_reuse_allowed else None,
            }
            continue

        missing_roles.append(role)

    return upstream_summaries, upstream_traceability, missing_roles


def build_signal_decider_payload(
    *,
    item: dict[str, Any],
    market_date: date,
    request_run_id: str,
    market_context: dict[str, Any] | None,
    upstream_summaries: dict[str, dict[str, Any]],
    upstream_traceability: dict[str, dict[str, Any]],
    signal_decider_agent_name: str,
    signal_decider_policy: dict[str, Any] | None = None,
    previous_decision_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    symbol_day_context = jsonable_encoder(strip_runtime_session_fields(item))
    company_context = (
        symbol_day_context.get('company')
        if isinstance(symbol_day_context.get('company'), dict)
        else {}
    )
    canonical_market = str(
        symbol_day_context.get('market') or company_context.get('market') or ''
    ).strip().upper()
    if canonical_market:
        symbol_day_context['market'] = canonical_market
    score_context = symbol_day_context.pop('scores', {}) if isinstance(symbol_day_context.get('scores'), dict) else {}
    factor_summary = (
        symbol_day_context.pop('factor_summary', {})
        if isinstance(symbol_day_context.get('factor_summary'), dict)
        else {}
    )
    liquidity_context = (
        symbol_day_context.pop('liquidity_context', {})
        if isinstance(symbol_day_context.get('liquidity_context'), dict)
        else {}
    )
    decider_data_quality_context = (
        symbol_day_context.pop('decider_data_quality_context', {})
        if isinstance(symbol_day_context.get('decider_data_quality_context'), dict)
        else {}
    )
    decision_input_quality_context = (
        symbol_day_context.pop('decision_input_quality_context', {})
        if isinstance(symbol_day_context.get('decision_input_quality_context'), dict)
        else {}
    )
    pre_decider_enrichment_context = (
        symbol_day_context.pop('pre_decider_enrichment_context', {})
        if isinstance(symbol_day_context.get('pre_decider_enrichment_context'), dict)
        else {}
    )
    market_regime_context = (
        symbol_day_context.pop('market_regime_context', {})
        if isinstance(symbol_day_context.get('market_regime_context'), dict)
        else {}
    )
    macro_context = (
        symbol_day_context.pop('macro_context', {})
        if isinstance(symbol_day_context.get('macro_context'), dict)
        else {}
    )
    existing_signal_envelopes = (
        symbol_day_context.pop('signal_envelopes', [])
        if isinstance(symbol_day_context.get('signal_envelopes'), list)
        else []
    )
    symbol = str(item.get('symbol') or '')
    signal_envelopes = build_decider_signal_envelopes(
        symbol=symbol,
        market_date=market_date,
        factor_summary=factor_summary,
        upstream_summaries=upstream_summaries,
        upstream_traceability=upstream_traceability,
        existing_envelopes=existing_signal_envelopes,
    )
    requested_at = datetime.now(timezone.utc).isoformat()
    resolved_signal_decider_policy = jsonable_encoder(
        signal_decider_policy
        if signal_decider_policy is not None
        else (
            signal_decider_loop_service.get_active_policy_context()
            if signal_decider_agent_name == 'signal-decider'
            else {}
        )
    )
    required_feedback_fields = [
        'role',
        'agent_name',
        'symbol',
        'market_date',
        'preferred_action',
        'next_day_open_action',
        'wait_reason',
        'entry_price_basis',
        'target_price',
        'expected_return_pct',
        'stop_loss_price',
        'suggested_holding_period',
        'confidence_score_10',
        'conviction_score_10',
        'thesis_summary',
        'supporting_evidence',
        'decision_risks',
        'decision_delta',
        'invalidation_conditions',
        'data_quality_warnings',
        'specialist_disagreement',
        'traceability',
        'summary',
        'confidence',
        'risk_flags',
        'details',
        'improvement_notes',
    ]
    if signal_decider_agent_name == 'short-term-decider':
        required_feedback_fields = [
            'role',
            'agent_name',
            'symbol',
            'market_date',
            'tactical_action',
            'preferred_action',
            'wait_reason',
            'trigger_price',
            'chase_limit_price',
            'target_price',
            'stop_loss_price',
            'expected_holding_period',
            'confidence_score_10',
            'conviction_score_10',
            'thesis_summary',
            'trigger_conditions',
            'supporting_evidence',
            'tactical_risks',
            'decision_delta',
            'invalidation_conditions',
            'data_quality_warnings',
            'traceability',
            'summary',
            'confidence',
            'risk_flags',
            'details',
            'improvement_notes',
        ]

    return {
        'role': signal_decider_agent_name,
        'agent_name': signal_decider_agent_name,
        'symbol': symbol,
        'market_date': market_date.isoformat(),
        'symbol_day_context': symbol_day_context,
        'scores': jsonable_encoder(score_context),
        'factor_summary': jsonable_encoder(factor_summary),
        'liquidity_context': jsonable_encoder(liquidity_context),
        'pre_decider_enrichment_context': jsonable_encoder(pre_decider_enrichment_context),
        'market_regime_context': jsonable_encoder(market_regime_context),
        'decider_data_quality_context': jsonable_encoder(decider_data_quality_context),
        'decision_input_quality_context': jsonable_encoder(decision_input_quality_context),
        'macro_context': jsonable_encoder(macro_context),
        'signal_envelopes': jsonable_encoder(signal_envelopes),
        'market_context': jsonable_encoder(market_context or {}),
        'upstream_summaries': jsonable_encoder(upstream_summaries),
        'upstream_traceability': jsonable_encoder(upstream_traceability),
        'previous_decision_context': jsonable_encoder(previous_decision_context or {}),
        'decision_support_constraints': {
            'decision_support_only': True,
            'allow_execution_logic': False,
            'allow_position_sizing': False,
            'allow_leverage_guidance': False,
            'avoid_fake_precision': True,
            'decision_history_guidance': {
                'use_previous_decision_context_when_available': True,
                'repeat_wait_requires_delta_summary': True,
                'forbid_copy_paste_repeat_wait_explanations': True,
                'required_transition_coverage': ['wait_to_wait', 'wait_to_buy_or_watch', 'buy_or_watch_to_wait'],
                'required_delta_fields': [
                    'previous_market_date',
                    'previous_recommendation',
                    'previous_recommendation_class',
                    'previous_wait_reason',
                    'current_recommendation',
                    'current_recommendation_class',
                    'current_wait_reason',
                    'transition',
                    'persistence_trend',
                    'changed_fields',
                    'stable_fields',
                    'what_changed',
                    'what_stayed_same',
                    'upgrade_trigger',
                ],
            },
            'missing_context_policy': {
                'core_execution_blocks': ['liquidity_turnover', 'intraday_realtime'],
                'supporting_context_blocks': [
                    'factor_score_coverage',
                    'technical_evidence',
                    'flows_short_selling',
                    'sector_relative_strength',
                ],
                'auxiliary_context_blocks': ['disclosures_news', 'financials_cashflow'],
                'auxiliary_context_absence_not_tradeability_blocker': True,
                'empty_recent_news_is_neutral': True,
                'quarterly_financial_detail_confidence_only_unless_thesis_depends_on_it': True,
                'forbid_untradeable_from_auxiliary_absence_only': True,
                'pre_decider_enrichment_context_authoritative': True,
                'market_regime_context_authoritative': True,
                'market_regime_posture_values': ['normal', 'caution', 'risk_off', 'unavailable', 'stale'],
                'market_regime_values': ['defensive', 'normal', 'aggressive_open', 'trend_day'],
                'market_regime_exec_actions': ['allow', 'downsize', 'reject', 'no_opinion'],
                'quantity_multiplier_is_exec_side_capped_hint': True,
                'aggressive_open_requires_bounded_window': True,
                'trend_day_requires_bounded_window': True,
                'market_regime_never_bypasses_safety_gates': True,
                'account_for_market_regime_when_choosing_buy_wait_reject': True,
                'risk_off_default_wait_or_reject_unless_exceptional_leadership': True,
                'caution_requires_stronger_evidence_and_reduced_sizing': True,
                'weak_sector_requires_sector_leadership_or_confirmation': True,
                'macro_context_contextual_input_only': True,
                'macro_context_not_a_direct_buy_sell_signal': True,
                'macro_context_regime_summary_is_deterministic_no_llm': True,
                'signal_envelope_schema_version': SIGNAL_ENVELOPE_SCHEMA_VERSION,
                'signal_envelopes_are_compact_normalized_inputs': True,
                'signal_envelopes_do_not_replace_raw_payloads': True,
                'decision_input_quality_context_authoritative': True,
                'decision_input_quality_sections': [
                    'data_quality',
                    'market_setup_edge',
                    'execution_feasibility',
                    'final_judgment',
                ],
                'respect_decision_input_quality_hard_blocks': True,
                'performance_criteria_requires_model_guidance_eligibility': True,
                'performance_criteria_sections': [
                    'overall',
                    'performance_buckets',
                    'rule_candidates',
                    'guidance',
                ],
                'use_eligible_replay_evidence_to_tighten_buy_thresholds': True,
                'tighten_buy_threshold_for_bad_buy_or_negative_return_clusters': True,
                'relax_wait_reject_threshold_for_missed_opportunity_clusters': False,
                'manual_review_criteria_never_authorize_buy': True,
                'deterministic_gate_requires_explicit_eligibility': True,
                'do_not_limit_decider_improvement_to_reason_names_or_text_detail': True,
                'pre_decider_enrichment_blocks': [
                    'quote_orderbook_snapshot',
                    'trading_value',
                    'flow_short_sector_context',
                ],
                'trading_value_source_values': ['raw', 'estimated', 'missing'],
                'do_not_emit_missing_warning_when_pre_decider_block_available': True,
            },
            'external_news_contract': {
                'materiality_values': ['material', 'noise', 'unknown'],
                'access_status_values': ['full_text', 'headline_only', 'paywalled', 'fetch_failed'],
                'material_items_first': True,
                'bounded_noise_filter_required': True,
                'source_evidence_required_for_claims': True,
                'price_forecast_from_news_alone_prohibited': True,
                'headline_only_or_paywalled_not_full_text': True,
            },
        },
        'signal_decider_policy': resolved_signal_decider_policy,
        'output_contract': {
            'batch_envelope_required': True,
            'assigned_symbol': symbol,
            'one_item_per_assigned_symbol': True,
            'symbol_identity': {
                'mandatory': True,
                'exact_match_required': True,
                'item_symbol_must_equal_assigned_symbol': True,
                'feedback_symbol_must_equal_item_symbol': True,
                'traceability_symbol_must_equal_item_symbol': True,
                'forbidden': ['omitted_symbol', 'alias_symbol', 'normalized_without_leading_zeroes', 'unrequested_symbol'],
            },
            'top_level_batch_shape': {'items': [{'symbol': symbol, 'feedback': {'symbol': symbol}}]},
            'single_symbol_batch_rule': 'Even one-symbol requests must return the batch envelope with exactly one item.',
            'failure_safe': 'If inputs are missing, weak, conflicting, or truncated, return a complete conservative wait or reject item with null unsupported numeric fields and explicit warnings.',
            'required_feedback_fields': required_feedback_fields,
            'self_check_before_final_output': [
                'output_is_parseable_json_object',
                'top_level_has_items_array',
                'exactly_one_item_for_each_assigned_symbol',
                'no_missing_or_unexpected_symbols',
                'item_symbol_feedback_symbol_and_traceability_symbol_match_assigned_symbol',
                'all_required_feedback_fields_present',
            ],
        },
        'orchestration': {
            'request_run_id': request_run_id,
            'requested_at': requested_at,
            'request_model': f'openclaw/{signal_decider_agent_name}',
            'source': 'agent_feedback_orchestrator.dispatch_for_symbols',
            'mode': 'role_sequential_runtime_buffered',
            'required_upstream_roles': list(upstream_summaries.keys()),
        },
    }
