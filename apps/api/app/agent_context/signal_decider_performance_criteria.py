from __future__ import annotations

from typing import Any


PERFORMANCE_CRITERIA_SCHEMA_VERSION = 'signal_decider_performance_criteria_v1'


def compact_performance_buckets(group_summaries: dict[str, Any]) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    preferred_sections = (
        'decision_actions',
        'outcome_path_buckets',
        'reward_risk_buckets',
        'confidence_buckets',
        'factor_signal_score_buckets',
        'factor_momentum_buckets',
        'factor_quality_buckets',
    )
    for section in preferred_sections:
        section_payload = group_summaries.get(section)
        buckets = section_payload.get('items') if isinstance(section_payload, dict) else None
        if not isinstance(buckets, list):
            continue
        for bucket in buckets[:4]:
            if not isinstance(bucket, dict):
                continue
            compacted.append(
                {
                    'bucket_type': bucket.get('bucket_type') or section,
                    'bucket_key': bucket.get('bucket_key'),
                    'bucket_value': bucket.get('bucket_value'),
                    'sample_count': bucket.get('sample_count'),
                    'positive_rate': bucket.get('positive_rate'),
                    'negative_rate': bucket.get('negative_rate'),
                    'bad_buy_rate': bucket.get('bad_buy_rate'),
                    'missed_opportunity_rate': bucket.get('missed_opportunity_rate'),
                    'avg_close_return_pct': bucket.get('avg_close_return_pct'),
                    'avg_max_upside_return_pct': bucket.get('avg_max_upside_return_pct'),
                    'avg_max_drawdown_return_pct': bucket.get('avg_max_drawdown_return_pct'),
                }
            )
    return compacted[:18]


def compact_rule_candidates(
    rule_candidates: dict[str, Any],
    *,
    force_advisory_only: bool = False,
) -> list[dict[str, Any]]:
    items = rule_candidates.get('items') if isinstance(rule_candidates, dict) else None
    if not isinstance(items, list):
        return []
    candidate_type_aliases = {
        'veto': 'buy_veto',
        'downsize': 'tighten_buy_gate',
    }
    compacted: list[dict[str, Any]] = []
    for item in items[:8]:
        if not isinstance(item, dict):
            continue
        candidate_type = candidate_type_aliases.get(
            str(item.get('candidate_type') or '').strip(),
            str(item.get('candidate_type') or '').strip(),
        )
        predicate = item.get('predicate') if isinstance(item.get('predicate'), dict) else {}
        predicate_branch = (
            'all'
            if 'all' in predicate and 'any' not in predicate
            else 'any'
            if 'any' in predicate and 'all' not in predicate
            else None
        )
        predicate_items = predicate.get(predicate_branch) if predicate_branch else None
        canonical_predicate = bool(
            predicate.get('schema_version') == 'signal_decider_rule_predicate_v1'
            and predicate_branch
            and set(predicate) == {'schema_version', predicate_branch}
            and isinstance(predicate_items, list)
            and predicate_items
            and all(isinstance(predicate_item, dict) for predicate_item in predicate_items)
        )
        activation_status = str(item.get('activation_status') or 'needs_replay').strip()
        manual_review_only = item.get('manual_review_only') is not False
        deterministic_gate_eligible = bool(
            not force_advisory_only
            and candidate_type in {'buy_veto', 'tighten_buy_gate'}
            and canonical_predicate
            and activation_status == 'active'
            and item.get('active_policy') is True
            and item.get('deterministic_gate_eligible') is True
            and not manual_review_only
        )
        compacted.append(
            {
                'candidate_id': item.get('candidate_id'),
                'candidate_type': candidate_type,
                'predicate': predicate,
                'reason': item.get('reason'),
                'support_sample_count': item.get('support_sample_count'),
                'evaluated_sample_count': item.get('evaluated_sample_count'),
                'negative_rate': item.get('negative_rate'),
                'activation_status': activation_status,
                'manual_review_only': True if force_advisory_only else manual_review_only,
                'active_policy': deterministic_gate_eligible,
                'deterministic_gate_eligible': deterministic_gate_eligible,
            }
        )
    return compacted


def compact_guidance(guidance: dict[str, Any]) -> dict[str, Any]:
    return {
        key: list(value)[:3]
        for key, value in guidance.items()
        if key in {'prompt_guidance', 'scoring_guidance', 'operator_review'}
        and isinstance(value, list)
    }


def build_published_performance_criteria(
    *,
    report: dict[str, Any],
    artifact_version: str,
    source_fingerprint: str,
    content_sha256: str,
    published_at: str,
    effective_from: str,
    week_start: str,
    week_end: str,
    valid_through: str,
    actual_execution: dict[str, Any],
) -> dict[str, Any]:
    summary = report.get('summary') if isinstance(report.get('summary'), dict) else {}
    request = report.get('request') if isinstance(report.get('request'), dict) else {}
    return {
        'schema_version': PERFORMANCE_CRITERIA_SCHEMA_VERSION,
        'status': 'ready',
        'source': 'published_weekly_learning_artifact',
        'artifact': {
            'artifact_version': artifact_version,
            'source_fingerprint': source_fingerprint,
            'content_sha256': content_sha256,
            'published_at': published_at,
            'effective_from': effective_from,
            'week_start': week_start,
            'week_end': week_end,
            'valid_through': valid_through,
        },
        'request': request,
        'sample_count': int(summary.get('mature_computed_count') or 0),
        'overall': {
            'decision_action_counts': summary.get('decision_action_counts') or {},
            'grade_label_counts': summary.get('grade_label_counts') or {},
            'positive_outcome_rate': summary.get('positive_outcome_rate'),
            'negative_outcome_rate': summary.get('negative_outcome_rate'),
            'coverage_gap_count': summary.get('coverage_gap_count'),
        },
        'performance_buckets': compact_performance_buckets(
            report.get('group_summaries')
            if isinstance(report.get('group_summaries'), dict)
            else {}
        ),
        'rule_candidates': compact_rule_candidates(
            report.get('rule_candidates')
            if isinstance(report.get('rule_candidates'), dict)
            else {},
            force_advisory_only=True,
        ),
        'guidance': compact_guidance(
            report.get('calibration_hints')
            if isinstance(report.get('calibration_hints'), dict)
            else {}
        ),
        'actual_execution': actual_execution,
        'usage_policy': {
            'model_guidance_eligible': True,
            'deterministic_gate_eligible': False,
            'use_for_buy_wait_reject_thresholds': False,
            'tighten_buy_when_matching_bucket_has_bad_buy_or_negative_return': True,
            'relax_wait_or_reject_when_matching_bucket_has_missed_opportunity': False,
            'allow_buy_authorization': False,
            'allow_sell_authorization': False,
            'reward_risk_and_outcome_path_override_name_only_reasoning': True,
            'auto_apply_policy_changes': False,
            'manual_review_only': True,
            'manual_policy_activation_required': True,
            'actual_execution_is_advisory_corroboration_only': True,
        },
    }
