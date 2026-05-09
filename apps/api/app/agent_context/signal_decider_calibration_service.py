from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from itertools import combinations
from typing import Any, Sequence

from app.agent_context.signal_decider_loop_service import (
    DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
    DEFAULT_SIGNAL_DECIDER_REVIEW_PRIMARY_HORIZON,
    NEGATIVE_SIGNAL_DECIDER_GRADE_LABELS,
    SIGNAL_DECIDER_EVALUATION_HORIZONS,
    signal_decider_loop_service,
)


POSITIVE_GRADE_LABELS: tuple[str, ...] = (
    'good_buy',
    'correct_wait',
    'correct_reject',
)
NEGATIVE_GRADE_LABELS: tuple[str, ...] = NEGATIVE_SIGNAL_DECIDER_GRADE_LABELS
CALIBRATION_REPORT_VERSION = 'signal_decider_calibration_report_v1'
DEFAULT_CLUSTER_LIMIT = 10
MAX_EXCLUSION_SAMPLE_ITEMS = 20
MIN_CLUSTER_DIRECTION_RATE = 0.60
GROUP_SECTION_ORDER: tuple[str, ...] = (
    'decision_actions',
    'confidence_buckets',
    'wait_reason_taxonomy',
    'liquidity_buckets',
    'price_location_buckets',
    'factor_signal_score_buckets',
    'factor_unified_value_buckets',
    'factor_momentum_buckets',
    'factor_quality_buckets',
    'signal_feature_buckets',
)
GROUP_SECTION_LABELS: dict[str, str] = {
    'decision_actions': 'Decision action buckets',
    'confidence_buckets': 'Confidence buckets',
    'wait_reason_taxonomy': 'Wait-reason taxonomy buckets',
    'liquidity_buckets': 'Liquidity buckets',
    'price_location_buckets': 'Price-location buckets',
    'factor_signal_score_buckets': 'Signal-score buckets',
    'factor_unified_value_buckets': 'Unified-value buckets',
    'factor_momentum_buckets': 'Momentum buckets',
    'factor_quality_buckets': 'Quality buckets',
    'signal_feature_buckets': 'Signal-feature buckets',
}


@dataclass(frozen=True)
class CalibrationToken:
    category: str
    key: str
    value: str
    label: str
    cluster_eligible: bool = True


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    if not symbols:
        return []
    normalized_symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in str(raw_symbol or '').split(','):
            normalized = str(piece).strip().upper()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_symbols.append(normalized)
    return normalized_symbols


def _normalize_horizon_days(value: int | None) -> int:
    resolved = DEFAULT_SIGNAL_DECIDER_REVIEW_PRIMARY_HORIZON if value is None else int(value)
    if resolved not in SIGNAL_DECIDER_EVALUATION_HORIZONS:
        raise ValueError(
            f'horizon_days must be drawn from {list(SIGNAL_DECIDER_EVALUATION_HORIZONS)}'
        )
    return resolved


def _normalize_limit(value: int | None, *, default: int, maximum: int) -> int:
    resolved = default if value is None else int(value)
    return max(1, min(resolved, maximum))


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _maybe_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _rate(*, numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _summed_average(*, total: float, count: int) -> float | None:
    if count <= 0:
        return None
    return round(total / count, 6)


def _confidence_bucket(score_10: float | None) -> str | None:
    if score_10 is None:
        return None
    if score_10 >= 8.0:
        return 'high'
    if score_10 >= 5.0:
        return 'medium'
    return 'low'


def _price_location_bucket(close_location_ratio: float | None) -> str | None:
    if close_location_ratio is None:
        return None
    if close_location_ratio >= 0.75:
        return 'extended_high'
    if close_location_ratio <= 0.25:
        return 'near_session_low'
    return 'mid_range'


def _factor_score_bucket(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 0.80:
        return 'high'
    if score >= 0.55:
        return 'medium'
    return 'low'


def _factor_bucket_value(*, status: str | None, score: float | None) -> str | None:
    score_bucket = _factor_score_bucket(score)
    if status and score_bucket:
        return f'{status}:{score_bucket}'
    if score_bucket:
        return score_bucket
    return status


def _liquidity_bucket(liquidity_context: dict[str, Any]) -> str | None:
    ranking = _maybe_dict(liquidity_context.get('ranking'))
    percentile = _to_float(ranking.get('liquidity_percentile'))
    passes_liquidity_floor = ranking.get('passes_liquidity_floor')
    status = _normalize_optional_text(liquidity_context.get('status')) or _normalize_optional_text(
        ranking.get('status')
    )
    if passes_liquidity_floor is False:
        return 'fails_liquidity_floor'
    if percentile is not None:
        if percentile >= 0.80:
            return 'high_liquidity'
        if percentile >= 0.50:
            return 'medium_liquidity'
        return 'lower_liquidity'
    if status:
        return f'{status}_context'
    return None


def _signal_feature_bucket(factor_summary: dict[str, Any]) -> str | None:
    signal_features = _maybe_dict(factor_summary.get('signal_features'))
    row_kind = _normalize_optional_text(signal_features.get('row_kind'))
    if row_kind:
        return row_kind
    return _normalize_optional_text(signal_features.get('status'))


def _new_bucket_stats(
    *,
    bucket_type: str,
    bucket_key: str,
    bucket_value: str,
    bucket_label: str,
) -> dict[str, Any]:
    return {
        'bucket_type': bucket_type,
        'bucket_key': bucket_key,
        'bucket_value': bucket_value,
        'bucket_label': bucket_label,
        'sample_count': 0,
        'decision_action_counts': Counter(),
        'grade_label_counts': Counter(),
        'positive_count': 0,
        'negative_count': 0,
        'buy_sample_count': 0,
        'bad_buy_count': 0,
        'wait_reject_sample_count': 0,
        'missed_opportunity_count': 0,
        'close_return_sum': 0.0,
        'close_return_count': 0,
        'max_upside_sum': 0.0,
        'max_upside_count': 0,
        'max_drawdown_sum': 0.0,
        'max_drawdown_count': 0,
        'grade_score_sum': 0.0,
        'grade_score_count': 0,
        'examples': [],
    }


def _update_bucket_stats(bucket: dict[str, Any], *, item: dict[str, Any]) -> None:
    decision_action = str(item.get('decision_action') or '')
    grade_label = str(item.get('grade_label') or '')
    bucket['sample_count'] += 1
    if decision_action:
        bucket['decision_action_counts'][decision_action] += 1
    if grade_label:
        bucket['grade_label_counts'][grade_label] += 1
    if item.get('positive_outcome') is True:
        bucket['positive_count'] += 1
    if item.get('negative_outcome') is True:
        bucket['negative_count'] += 1
    if decision_action == 'buy':
        bucket['buy_sample_count'] += 1
        if grade_label == 'bad_buy':
            bucket['bad_buy_count'] += 1
    if decision_action in {'wait', 'reject'}:
        bucket['wait_reject_sample_count'] += 1
        if item.get('missed_opportunity') is True:
            bucket['missed_opportunity_count'] += 1
    close_return_pct = _to_float(item.get('close_return_pct'))
    if close_return_pct is not None:
        bucket['close_return_sum'] += close_return_pct
        bucket['close_return_count'] += 1
    max_upside_return_pct = _to_float(item.get('max_upside_return_pct'))
    if max_upside_return_pct is not None:
        bucket['max_upside_sum'] += max_upside_return_pct
        bucket['max_upside_count'] += 1
    max_drawdown_return_pct = _to_float(item.get('max_drawdown_return_pct'))
    if max_drawdown_return_pct is not None:
        bucket['max_drawdown_sum'] += max_drawdown_return_pct
        bucket['max_drawdown_count'] += 1
    grade_score = _to_float(item.get('grade_score'))
    if grade_score is not None:
        bucket['grade_score_sum'] += grade_score
        bucket['grade_score_count'] += 1
    examples: list[dict[str, Any]] = bucket['examples']
    if len(examples) < 3:
        examples.append(
            {
                'snapshot_id': item['snapshot_id'],
                'symbol': item['symbol'],
                'market_date': item['market_date'],
                'decision_action': decision_action,
                'grade_label': grade_label,
            }
        )


def _finalize_bucket_stats(bucket: dict[str, Any]) -> dict[str, Any]:
    sample_count = int(bucket['sample_count'] or 0)
    return {
        'bucket_type': bucket['bucket_type'],
        'bucket_key': bucket['bucket_key'],
        'bucket_value': bucket['bucket_value'],
        'bucket_label': bucket['bucket_label'],
        'sample_count': sample_count,
        'decision_action_counts': dict(sorted(bucket['decision_action_counts'].items())),
        'grade_label_counts': dict(sorted(bucket['grade_label_counts'].items())),
        'positive_count': int(bucket['positive_count'] or 0),
        'negative_count': int(bucket['negative_count'] or 0),
        'positive_rate': _rate(
            numerator=int(bucket['positive_count'] or 0),
            denominator=sample_count,
        ),
        'negative_rate': _rate(
            numerator=int(bucket['negative_count'] or 0),
            denominator=sample_count,
        ),
        'good_buy_rate': _rate(
            numerator=int(bucket['buy_sample_count'] or 0) - int(bucket['bad_buy_count'] or 0),
            denominator=int(bucket['buy_sample_count'] or 0),
        ),
        'bad_buy_rate': _rate(
            numerator=int(bucket['bad_buy_count'] or 0),
            denominator=int(bucket['buy_sample_count'] or 0),
        ),
        'missed_opportunity_rate': _rate(
            numerator=int(bucket['missed_opportunity_count'] or 0),
            denominator=int(bucket['wait_reject_sample_count'] or 0),
        ),
        'avg_close_return_pct': _summed_average(
            total=float(bucket['close_return_sum'] or 0.0),
            count=int(bucket['close_return_count'] or 0),
        ),
        'avg_max_upside_return_pct': _summed_average(
            total=float(bucket['max_upside_sum'] or 0.0),
            count=int(bucket['max_upside_count'] or 0),
        ),
        'avg_max_drawdown_return_pct': _summed_average(
            total=float(bucket['max_drawdown_sum'] or 0.0),
            count=int(bucket['max_drawdown_count'] or 0),
        ),
        'avg_grade_score': _summed_average(
            total=float(bucket['grade_score_sum'] or 0.0),
            count=int(bucket['grade_score_count'] or 0),
        ),
        'examples': list(bucket['examples']),
    }


class SignalDeciderCalibrationService:
    def __init__(self, *, loop_service=signal_decider_loop_service) -> None:
        self.loop_service = loop_service

    def build_report(
        self,
        *,
        market_date_from: str | date,
        market_date_to: str | date,
        symbols: Sequence[str] | None = None,
        policy_version: str | None = None,
        horizon_days: int = DEFAULT_SIGNAL_DECIDER_REVIEW_PRIMARY_HORIZON,
        evaluation_version: str | None = None,
        min_sample_size: int = 3,
        cluster_limit: int = DEFAULT_CLUSTER_LIMIT,
    ) -> dict[str, Any]:
        normalized_from = _normalize_market_date(market_date_from)
        normalized_to = _normalize_market_date(market_date_to)
        if normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        normalized_symbols = _normalize_symbols(symbols)
        normalized_policy_version = _normalize_optional_text(policy_version)
        normalized_horizon_days = _normalize_horizon_days(horizon_days)
        resolved_evaluation_version = (
            _normalize_optional_text(evaluation_version) or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        resolved_min_sample_size = _normalize_limit(min_sample_size, default=3, maximum=1000)
        resolved_cluster_limit = _normalize_limit(cluster_limit, default=DEFAULT_CLUSTER_LIMIT, maximum=50)

        rows = self.loop_service.repository.list_decision_snapshots(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            symbols=normalized_symbols or None,
            policy_versions=[normalized_policy_version] if normalized_policy_version else None,
            dedupe_feedback_results=True,
        )
        snapshot_ids = [row.id for row in rows]
        outcomes = self.loop_service.repository.list_realized_outcomes(
            decision_snapshot_ids=snapshot_ids,
            horizons=[normalized_horizon_days],
        )
        evaluations = self.loop_service.repository.list_evaluations(
            decision_snapshot_ids=snapshot_ids,
            horizons=[normalized_horizon_days],
            evaluation_version=resolved_evaluation_version,
        )
        outcomes_by_snapshot = self.loop_service._group_by_snapshot_id(outcomes)
        evaluations_by_snapshot = self.loop_service._group_by_snapshot_id(evaluations)
        future_bars_cache: dict[tuple[str, date], list[dict[str, Any]]] = {}
        latest_trade_date_cache: dict[tuple[str, date], date | None] = {}

        mature_items: list[dict[str, Any]] = []
        excluded_items: list[dict[str, Any]] = []
        coverage_items: list[dict[str, Any]] = []
        for row in rows:
            decision = self.loop_service._build_decision_snapshot(row)
            cache_key = (row.symbol, row.market_date)
            if cache_key not in future_bars_cache:
                future_bars_cache[cache_key] = self.loop_service._load_future_price_bars(
                    symbol=row.symbol,
                    market_date=row.market_date,
                    max_horizon=normalized_horizon_days,
                )
            if cache_key not in latest_trade_date_cache:
                latest_trade_date_cache[cache_key] = self.loop_service._load_latest_available_trade_date(
                    symbol=row.symbol,
                    market_date=row.market_date,
                )
            outcome = next(
                (
                    item
                    for item in outcomes_by_snapshot.get(row.id, [])
                    if int(item.get('horizon_days') or 0) == normalized_horizon_days
                ),
                None,
            )
            evaluation = next(
                (
                    item
                    for item in evaluations_by_snapshot.get(row.id, [])
                    if int(item.get('horizon_days') or 0) == normalized_horizon_days
                ),
                None,
            )
            coverage = self.loop_service._build_horizon_coverage(
                snapshot_row=row,
                future_bars=future_bars_cache[cache_key],
                latest_available_trade_date=latest_trade_date_cache[cache_key],
                horizon_days=normalized_horizon_days,
                outcome=outcome,
                evaluation=evaluation,
            )
            coverage_items.append(coverage)
            if (
                coverage.get('status_reason') == 'computed'
                and _normalize_optional_text(_maybe_dict(evaluation).get('evaluation_status')) == 'graded'
            ):
                mature_items.append(
                    self._build_calibration_item(
                        row=row,
                        decision=decision,
                        outcome=outcome or {},
                        evaluation=evaluation or {},
                        coverage=coverage,
                    )
                )
                continue
            excluded_items.append(
                {
                    'snapshot_id': row.id,
                    'feedback_id': row.feedback_id,
                    'market_date': row.market_date.isoformat(),
                    'symbol': row.symbol,
                    'policy_version': decision['policy_version'],
                    'decision_action': decision['decision']['action'],
                    'wait_reason': decision.get('wait_reason'),
                    'status_reason': coverage.get('status_reason'),
                    'status_message': coverage.get('status_message'),
                    'matured': bool(coverage.get('matured')),
                    'stored_outcome': bool(coverage.get('stored_outcome')),
                    'stored_evaluation': bool(coverage.get('stored_evaluation')),
                    'earliest_computable_date': coverage.get('earliest_computable_date'),
                    'latest_available_trade_date': coverage.get('latest_available_trade_date'),
                }
            )

        coverage_summary = self.loop_service._build_coverage_summary(coverage_items=coverage_items)
        excluded_status_reason_counts = Counter(
            str(item.get('status_reason') or '')
            for item in excluded_items
            if item.get('status_reason')
        )
        action_counts = Counter(item['decision_action'] for item in mature_items if item.get('decision_action'))
        grade_label_counts = Counter(item['grade_label'] for item in mature_items if item.get('grade_label'))
        policy_versions = sorted({item['policy_version'] for item in mature_items if item.get('policy_version')})
        positive_count = sum(1 for item in mature_items if item.get('positive_outcome') is True)
        negative_count = sum(1 for item in mature_items if item.get('negative_outcome') is True)
        group_summaries = self._build_group_summaries(items=mature_items)
        clusters = self._build_condition_clusters(
            items=mature_items,
            min_sample_size=resolved_min_sample_size,
            cluster_limit=resolved_cluster_limit,
        )
        hints = self._build_calibration_hints(
            horizon_days=normalized_horizon_days,
            condition_clusters=clusters,
            exclusion_status_reason_counts=dict(coverage_summary.get('status_reason_counts') or {}),
        )
        report_status = 'empty'
        if rows:
            report_status = 'ready' if len(mature_items) >= resolved_min_sample_size else 'insufficient_sample'

        return {
            'request': {
                'market_date_from': normalized_from.isoformat(),
                'market_date_to': normalized_to.isoformat(),
                'symbols': normalized_symbols,
                'policy_version': normalized_policy_version,
                'horizon_days': normalized_horizon_days,
                'evaluation_version': resolved_evaluation_version,
                'min_sample_size': resolved_min_sample_size,
                'cluster_limit': resolved_cluster_limit,
            },
            'review_boundary': {
                'report_version': CALIBRATION_REPORT_VERSION,
                'manual_review_only': True,
                'auto_apply_prompt_changes': False,
                'auto_apply_policy_changes': False,
                'replay_read_model': self.loop_service.build_replay_read_model_descriptor(),
            },
            'summary': {
                'status': report_status,
                'snapshots_considered': len(rows),
                'mature_computed_count': len(mature_items),
                'excluded_count': len(excluded_items),
                'coverage': coverage_summary,
                'coverage_gap_count': int(coverage_summary.get('not_computed_count') or 0)
                + int(coverage_summary.get('insufficient_price_data_count') or 0),
                'decision_action_counts': dict(sorted(action_counts.items())),
                'grade_label_counts': dict(sorted(grade_label_counts.items())),
                'policy_versions': policy_versions,
                'positive_outcome_count': positive_count,
                'negative_outcome_count': negative_count,
                'positive_outcome_rate': _rate(numerator=positive_count, denominator=len(mature_items)),
                'negative_outcome_rate': _rate(numerator=negative_count, denominator=len(mature_items)),
                'ready_for_operator_review': len(mature_items) >= resolved_min_sample_size,
            },
            'exclusions': {
                'status_reason_counts': dict(sorted(excluded_status_reason_counts.items())),
                'items': excluded_items[:MAX_EXCLUSION_SAMPLE_ITEMS],
            },
            'group_summaries': group_summaries,
            'condition_clusters': clusters,
            'calibration_hints': hints,
        }

    def _build_calibration_item(
        self,
        *,
        row,
        decision: dict[str, Any],
        outcome: dict[str, Any],
        evaluation: dict[str, Any],
        coverage: dict[str, Any],
    ) -> dict[str, Any]:
        request_payload = _maybe_dict(row.request_payload)
        factor_summary = _maybe_dict(request_payload.get('factor_summary'))
        liquidity_context = _maybe_dict(request_payload.get('liquidity_context'))
        feature_snapshot = _maybe_dict(decision.get('feature_snapshot'))
        scores = _maybe_dict(request_payload.get('scores')) or _maybe_dict(feature_snapshot.get('scores'))
        diagnostics = _maybe_dict(evaluation.get('diagnostics'))
        close_return_pct = _to_float(diagnostics.get('close_return_pct'))
        max_upside_return_pct = _to_float(diagnostics.get('max_upside_return_pct'))
        max_drawdown_return_pct = _to_float(diagnostics.get('max_drawdown_return_pct'))
        if close_return_pct is None:
            close_return_pct = _to_float(outcome.get('close_return_pct'))
        if max_upside_return_pct is None:
            max_upside_return_pct = _to_float(outcome.get('max_upside_return_pct'))
        if max_drawdown_return_pct is None:
            max_drawdown_return_pct = _to_float(outcome.get('max_drawdown_return_pct'))
        grade_label = str(evaluation.get('grade_label') or '')
        return {
            'snapshot_id': row.id,
            'feedback_id': row.feedback_id,
            'market_date': row.market_date.isoformat(),
            'symbol': row.symbol,
            'policy_version': decision['policy_version'],
            'decision_action': decision['decision']['action'],
            'wait_reason': decision.get('wait_reason'),
            'grade_label': grade_label,
            'grade_score': _to_float(evaluation.get('grade_score')),
            'missed_opportunity': bool(evaluation.get('missed_opportunity')),
            'positive_outcome': grade_label in set(POSITIVE_GRADE_LABELS),
            'negative_outcome': grade_label in set(NEGATIVE_GRADE_LABELS),
            'close_return_pct': close_return_pct,
            'max_upside_return_pct': max_upside_return_pct,
            'max_drawdown_return_pct': max_drawdown_return_pct,
            'coverage': coverage,
            'group_tokens': self._build_group_tokens(
                decision=decision,
                factor_summary=factor_summary,
                liquidity_context=liquidity_context,
                feature_snapshot=feature_snapshot,
                scores=scores,
            ),
        }

    def _build_group_tokens(
        self,
        *,
        decision: dict[str, Any],
        factor_summary: dict[str, Any],
        liquidity_context: dict[str, Any],
        feature_snapshot: dict[str, Any],
        scores: dict[str, Any],
    ) -> list[CalibrationToken]:
        tokens: list[CalibrationToken] = []
        decision_fields = _maybe_dict(decision.get('decision'))
        action = _normalize_optional_text(decision_fields.get('action'))
        if action:
            tokens.append(
                CalibrationToken(
                    category='decision_actions',
                    key='decision.action',
                    value=action,
                    label=f'Decision action: {action}',
                )
            )
        confidence_bucket = _confidence_bucket(_to_float(decision_fields.get('confidence_score_10')))
        if confidence_bucket:
            tokens.append(
                CalibrationToken(
                    category='confidence_buckets',
                    key='decision.confidence_bucket',
                    value=confidence_bucket,
                    label=f'Confidence bucket: {confidence_bucket}',
                )
            )
        wait_reason = _maybe_dict(decision.get('wait_reason'))
        wait_reason_code = _normalize_optional_text(wait_reason.get('code'))
        wait_reason_label = _normalize_optional_text(wait_reason.get('label'))
        if wait_reason_code:
            tokens.append(
                CalibrationToken(
                    category='wait_reason_taxonomy',
                    key='wait_reason.code',
                    value=wait_reason_code,
                    label=f'Wait reason: {wait_reason_label or wait_reason_code}',
                )
            )
        signal_score_bucket = _normalize_optional_text(
            _maybe_dict(feature_snapshot.get('action_summary')).get('signal_score_bucket')
        )
        if signal_score_bucket:
            tokens.append(
                CalibrationToken(
                    category='factor_signal_score_buckets',
                    key='feature.action_summary.signal_score_bucket',
                    value=signal_score_bucket,
                    label=f'Signal score: {signal_score_bucket}',
                )
            )
        liquidity_bucket = _liquidity_bucket(liquidity_context)
        if liquidity_bucket:
            tokens.append(
                CalibrationToken(
                    category='liquidity_buckets',
                    key='liquidity.bucket',
                    value=liquidity_bucket,
                    label=f'Liquidity: {liquidity_bucket}',
                )
            )
        price_location_bucket = _price_location_bucket(
            _to_float(_maybe_dict(feature_snapshot.get('price_context')).get('close_location_ratio'))
        )
        if price_location_bucket:
            tokens.append(
                CalibrationToken(
                    category='price_location_buckets',
                    key='price_context.close_location_bucket',
                    value=price_location_bucket,
                    label=f'Price location: {price_location_bucket}',
                )
            )
        factor_scores = _maybe_dict(factor_summary.get('factor_scores'))
        for family, category, label in (
            ('unified_value', 'factor_unified_value_buckets', 'Unified value'),
            ('momentum', 'factor_momentum_buckets', 'Momentum'),
            ('quality', 'factor_quality_buckets', 'Quality'),
        ):
            score_payload = _maybe_dict(scores.get(family))
            factor_payload = _maybe_dict(factor_scores.get(family))
            bucket_value = _factor_bucket_value(
                status=_normalize_optional_text(
                    factor_payload.get('status') or score_payload.get('status')
                ),
                score=_to_float(score_payload.get('score') or factor_payload.get('score')),
            )
            if bucket_value:
                tokens.append(
                    CalibrationToken(
                        category=category,
                        key=f'factor.{family}.bucket',
                        value=bucket_value,
                        label=f'{label}: {bucket_value}',
                    )
                )
        signal_feature_bucket = _signal_feature_bucket(factor_summary)
        if signal_feature_bucket:
            tokens.append(
                CalibrationToken(
                    category='signal_feature_buckets',
                    key='factor.signal_features.bucket',
                    value=signal_feature_bucket,
                    label=f'Signal features: {signal_feature_bucket}',
                )
            )
        return tokens

    def _build_group_summaries(self, *, items: Sequence[dict[str, Any]]) -> dict[str, Any]:
        grouped: dict[str, dict[tuple[str, str], dict[str, Any]]] = {
            section: {} for section in GROUP_SECTION_ORDER
        }
        for item in items:
            for token in list(item.get('group_tokens') or []):
                bucket_map = grouped.setdefault(token.category, {})
                bucket = bucket_map.setdefault(
                    (token.key, token.value),
                    _new_bucket_stats(
                        bucket_type=token.category,
                        bucket_key=token.key,
                        bucket_value=token.value,
                        bucket_label=token.label,
                    ),
                )
                _update_bucket_stats(bucket, item=item)
        result: dict[str, Any] = {}
        for section in GROUP_SECTION_ORDER:
            raw_items = [
                _finalize_bucket_stats(bucket)
                for bucket in grouped.get(section, {}).values()
            ]
            raw_items.sort(
                key=lambda item: (
                    -int(item.get('sample_count') or 0),
                    str(item.get('bucket_value') or ''),
                )
            )
            result[section] = {
                'label': GROUP_SECTION_LABELS[section],
                'items': raw_items,
            }
        return result

    def _build_condition_clusters(
        self,
        *,
        items: Sequence[dict[str, Any]],
        min_sample_size: int,
        cluster_limit: int,
    ) -> dict[str, Any]:
        cluster_buckets: dict[tuple[tuple[str, str], ...], dict[str, Any]] = {}
        for item in items:
            tokens = [
                token
                for token in list(item.get('group_tokens') or [])
                if isinstance(token, CalibrationToken) and token.cluster_eligible
            ]
            deduped_tokens: list[CalibrationToken] = []
            seen_tokens: set[tuple[str, str]] = set()
            for token in tokens:
                token_key = (token.key, token.value)
                if token_key in seen_tokens:
                    continue
                seen_tokens.add(token_key)
                deduped_tokens.append(token)
            for size in (1, 2):
                for combo in combinations(deduped_tokens, size):
                    if len({token.category for token in combo}) != len(combo):
                        continue
                    ordered_combo = tuple(sorted(combo, key=lambda token: (token.key, token.value)))
                    cluster_key = tuple((token.key, token.value) for token in ordered_combo)
                    bucket = cluster_buckets.setdefault(
                        cluster_key,
                        {
                            **_new_bucket_stats(
                                bucket_type='condition_cluster',
                                bucket_key=' && '.join(f'{key}={value}' for key, value in cluster_key),
                                bucket_value=' && '.join(value for _, value in cluster_key),
                                bucket_label=' + '.join(token.label for token in ordered_combo),
                            ),
                            'tokens': [
                                {
                                    'category': token.category,
                                    'key': token.key,
                                    'value': token.value,
                                    'label': token.label,
                                }
                                for token in ordered_combo
                            ],
                            'token_count': len(ordered_combo),
                        },
                    )
                    _update_bucket_stats(bucket, item=item)

        finalized_clusters: list[dict[str, Any]] = []
        for bucket in cluster_buckets.values():
            finalized = _finalize_bucket_stats(bucket)
            finalized['tokens'] = list(bucket['tokens'])
            finalized['token_count'] = int(bucket['token_count'] or 0)
            finalized['cluster_key'] = str(finalized.get('bucket_key') or '')
            finalized_clusters.append(finalized)

        baseline_positive_rate = _rate(
            numerator=sum(1 for item in items if item.get('positive_outcome') is True),
            denominator=len(items),
        ) or 0.0
        baseline_negative_rate = _rate(
            numerator=sum(1 for item in items if item.get('negative_outcome') is True),
            denominator=len(items),
        ) or 0.0
        positive_candidates = self._select_directional_clusters(
            clusters=finalized_clusters,
            baseline_rate=baseline_positive_rate,
            direction='positive',
            min_sample_size=min_sample_size,
            cluster_limit=cluster_limit,
        )
        negative_candidates = self._select_directional_clusters(
            clusters=finalized_clusters,
            baseline_rate=baseline_negative_rate,
            direction='negative',
            min_sample_size=min_sample_size,
            cluster_limit=cluster_limit,
        )
        return {
            'baseline_positive_rate': baseline_positive_rate,
            'baseline_negative_rate': baseline_negative_rate,
            'positive': positive_candidates,
            'negative': negative_candidates,
        }

    def _select_directional_clusters(
        self,
        *,
        clusters: Sequence[dict[str, Any]],
        baseline_rate: float,
        direction: str,
        min_sample_size: int,
        cluster_limit: int,
    ) -> list[dict[str, Any]]:
        direction_key = 'positive_rate' if direction == 'positive' else 'negative_rate'
        direction_count_key = 'positive_count' if direction == 'positive' else 'negative_count'
        directional_clusters: list[dict[str, Any]] = []
        fallback_clusters: list[dict[str, Any]] = []
        for cluster in clusters:
            sample_count = int(cluster.get('sample_count') or 0)
            direction_rate = _to_float(cluster.get(direction_key)) or 0.0
            direction_count = int(cluster.get(direction_count_key) or 0)
            if sample_count < min_sample_size or direction_count <= 0:
                continue
            candidate = dict(cluster)
            candidate['direction'] = direction
            candidate['lift_vs_baseline'] = round(direction_rate - baseline_rate, 6)
            candidate['strength_score'] = round((direction_rate - baseline_rate) * sample_count, 6)
            fallback_clusters.append(candidate)
            if direction_rate >= MIN_CLUSTER_DIRECTION_RATE or direction_rate > baseline_rate:
                directional_clusters.append(candidate)
        selected_clusters = directional_clusters or fallback_clusters
        selected_clusters.sort(
            key=lambda item: (
                -float(item.get('strength_score') or 0.0),
                self._cluster_priority(item),
                -float(item.get(direction_key) or 0.0),
                -int(item.get('sample_count') or 0),
                -int(item.get('token_count') or 0),
                str(item.get('cluster_key') or ''),
            )
        )
        return selected_clusters[:cluster_limit]

    def _cluster_priority(self, cluster: dict[str, Any]) -> int:
        categories = {
            str(token.get('category') or '')
            for token in list(cluster.get('tokens') or [])
            if isinstance(token, dict)
        }
        if 'wait_reason_taxonomy' in categories:
            return 0
        if categories.intersection(
            {
                'confidence_buckets',
                'factor_signal_score_buckets',
                'factor_unified_value_buckets',
                'factor_momentum_buckets',
                'factor_quality_buckets',
            }
        ):
            return 1
        if categories.intersection({'liquidity_buckets', 'price_location_buckets'}):
            return 2
        if 'decision_actions' in categories:
            return 3
        return 4

    def _build_calibration_hints(
        self,
        *,
        horizon_days: int,
        condition_clusters: dict[str, Any],
        exclusion_status_reason_counts: dict[str, Any],
    ) -> dict[str, Any]:
        prompt_guidance: list[dict[str, Any]] = []
        scoring_guidance: list[dict[str, Any]] = []
        operator_review: list[dict[str, Any]] = []
        if int(exclusion_status_reason_counts.get('not_computed') or 0) > 0 or int(
            exclusion_status_reason_counts.get('insufficient_price_data') or 0
        ) > 0:
            operator_review.append(
                {
                    'kind': 'operator_review',
                    'title': 'Repair mature replay coverage gaps before changing prompt or scoring guidance.',
                    'action': (
                        'Resolve stored replay gaps and rerun the calibration report before acting on the '
                        'negative or positive condition clusters.'
                    ),
                    'manual_review_only': True,
                    'auto_apply': False,
                    'evidence': {
                        'horizon_days': horizon_days,
                        'status_reason_counts': {
                            key: int(value or 0)
                            for key, value in exclusion_status_reason_counts.items()
                            if key in {'not_computed', 'insufficient_price_data'} and int(value or 0) > 0
                        },
                    },
                }
            )

        for cluster in list(condition_clusters.get('positive') or []) + list(condition_clusters.get('negative') or []):
            hint = self._cluster_hint(cluster=cluster, horizon_days=horizon_days)
            if hint is None:
                continue
            if hint['kind'] == 'scoring_guidance' and len(scoring_guidance) < 2:
                scoring_guidance.append(hint)
            elif hint['kind'] == 'operator_review' and len(operator_review) < 2:
                operator_review.append(hint)
            elif hint['kind'] == 'prompt_guidance' and len(prompt_guidance) < 2:
                prompt_guidance.append(hint)

        return {
            'prompt_guidance': prompt_guidance,
            'scoring_guidance': scoring_guidance,
            'operator_review': operator_review,
        }

    def _cluster_hint(
        self,
        *,
        cluster: dict[str, Any],
        horizon_days: int,
    ) -> dict[str, Any] | None:
        tokens = list(cluster.get('tokens') or [])
        if not tokens:
            return None
        categories = {str(token.get('category') or '') for token in tokens}
        direction = str(cluster.get('direction') or 'negative')
        direction_rate_key = 'positive_rate' if direction == 'positive' else 'negative_rate'
        direction_rate = _to_float(cluster.get(direction_rate_key))
        cluster_label = str(cluster.get('bucket_label') or cluster.get('cluster_key') or '')
        if not cluster_label:
            return None
        if 'wait_reason_taxonomy' in categories:
            kind = 'operator_review'
            action = (
                f'Review whether {cluster_label.lower()} is weighted too aggressively in WAIT or REJECT guidance.'
                if direction == 'negative'
                else f'Keep {cluster_label.lower()} available as an explicit operator-facing blocker.'
            )
        elif categories.intersection(
            {
                'confidence_buckets',
                'factor_signal_score_buckets',
                'factor_unified_value_buckets',
                'factor_momentum_buckets',
                'factor_quality_buckets',
            }
        ):
            kind = 'scoring_guidance'
            action = (
                f'Consider reinforcing confidence when {cluster_label.lower()} recurs.'
                if direction == 'positive'
                else f'Consider reducing confidence or adding an explicit downgrade when {cluster_label.lower()} recurs.'
            )
        else:
            kind = 'prompt_guidance'
            action = (
                f'Consider emphasizing {cluster_label.lower()} as a supportive prompt condition.'
                if direction == 'positive'
                else f'Consider tightening prompt language around {cluster_label.lower()}.'
            )
        return {
            'kind': kind,
            'title': (
                f'Positive cluster: {cluster_label}'
                if direction == 'positive'
                else f'Negative cluster: {cluster_label}'
            ),
            'action': action,
            'manual_review_only': True,
            'auto_apply': False,
            'evidence': {
                'horizon_days': horizon_days,
                'sample_count': int(cluster.get('sample_count') or 0),
                'direction': direction,
                'direction_rate': direction_rate,
                'lift_vs_baseline': _to_float(cluster.get('lift_vs_baseline')),
                'grade_label_counts': dict(cluster.get('grade_label_counts') or {}),
                'examples': list(cluster.get('examples') or []),
                'cluster_key': cluster.get('cluster_key'),
            },
        }


signal_decider_calibration_service = SignalDeciderCalibrationService()
