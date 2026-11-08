from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
import math
from typing import Any
from zoneinfo import ZoneInfo


SIGNAL_FEATURE_READINESS_CONTRACT_VERSION = 'signal_feature_readiness_v1'
_DIRECT_ROW_KINDS = {'direct_only', 'direct_plus_fallback'}
_FALLBACK_ROW_KINDS = {'fallback_only', 'direct_plus_fallback'}
_MARKET_FALLBACK_GROUPS = {
    'price_action',
    'returns',
    'liquidity',
    'volatility',
    'quote_snapshot',
}
_VOLUME_POWER_PLACEHOLDER = 999.99
_SEOUL_TZ = ZoneInfo('Asia/Seoul')


def _has_numeric_decision_value(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, (int, float, Decimal)):
        try:
            return math.isfinite(float(value))
        except (TypeError, ValueError, OverflowError):
            return False
    if isinstance(value, Mapping):
        return any(_has_numeric_decision_value(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_has_numeric_decision_value(item) for item in value)
    return False


def _finite_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return normalized if math.isfinite(normalized) else None


def _direct_source_metric(
    payload: Mapping[str, Any],
    *field_names: str,
) -> float | None:
    raw = payload.get('raw')
    candidate_mappings = (
        payload,
        raw if isinstance(raw, Mapping) else {},
    )
    for candidate in candidate_mappings:
        for field_name in field_names:
            if field_name not in candidate:
                continue
            normalized = _finite_float(candidate.get(field_name))
            if normalized is not None:
                return normalized
    return None


def direct_signal_source_is_decision_usable(
    *,
    source: str,
    payload: Mapping[str, Any],
) -> bool:
    normalized_source = str(source).strip().lower().replace('_', '-')
    rank = _direct_source_metric(payload, 'rank', 'data_rank')
    rank_is_usable = bool(
        rank is not None
        and rank > 0
        and float(rank).is_integer()
    )
    if not rank_is_usable:
        return False
    if normalized_source == 'volume-power':
        volume_power = _direct_source_metric(
            payload,
            'volume_power',
            'tday_rltv',
        )
        return bool(
            volume_power is not None
            and volume_power > 0
            and abs(volume_power - _VOLUME_POWER_PLACEHOLDER) >= 1e-9
        )
    if normalized_source == 'overtime-volume':
        overtime_volume = _direct_source_metric(
            payload,
            'overtime_volume',
            'ovtm_untp_vol',
        )
        return bool(overtime_volume is not None and overtime_volume > 0)
    if normalized_source == 'overtime-fluctuation':
        return (
            _direct_source_metric(
                payload,
                'overtime_change_pct',
                'ovtm_untp_prdy_ctrt',
            )
            is not None
        )
    return False


def quote_snapshot_is_same_day(
    *,
    payload: Mapping[str, Any],
    freshness: Mapping[str, Any],
    expected_market_date: str,
) -> bool:
    quote_freshness = freshness.get('quote_snapshot')
    quote_freshness = (
        quote_freshness
        if isinstance(quote_freshness, Mapping)
        else {}
    )
    trade_dates = [
        str(value).strip()
        for value in (
            payload.get('trade_date'),
            quote_freshness.get('trade_date'),
        )
        if str(value or '').strip()
    ]
    if trade_dates:
        return all(value == expected_market_date for value in trade_dates)

    observed_dates: list[str] = []
    for raw_timestamp in (
        payload.get('cached_at'),
        payload.get('captured_at'),
        quote_freshness.get('cached_at'),
        quote_freshness.get('captured_at'),
    ):
        timestamp_text = str(raw_timestamp or '').strip()
        if not timestamp_text:
            continue
        try:
            observed_at = datetime.fromisoformat(
                timestamp_text.replace('Z', '+00:00')
            )
        except ValueError:
            continue
        if observed_at.tzinfo is None:
            continue
        observed_dates.append(
            observed_at.astimezone(_SEOUL_TZ).date().isoformat()
        )
    return bool(observed_dates) and all(
        observed_date == expected_market_date
        for observed_date in observed_dates
    )


def decision_usable_direct_signal_sources(
    feature_payload: Any,
) -> list[str]:
    if not isinstance(feature_payload, Mapping):
        return []
    sources = feature_payload.get('sources')
    if not isinstance(sources, Mapping):
        return []
    return sorted(
        str(source)
        for source, payload in sources.items()
        if str(source).strip()
        and isinstance(payload, Mapping)
        and direct_signal_source_is_decision_usable(
            source=str(source),
            payload=payload,
        )
    )


def quote_snapshot_is_decision_usable(
    *,
    payload: Mapping[str, Any],
    freshness: Mapping[str, Any],
    expected_market_date: str,
) -> bool:
    try:
        price = float(payload.get('price'))
    except (TypeError, ValueError, OverflowError):
        return False
    return bool(
        payload.get('available') is not False
        and math.isfinite(price)
        and price > 0
        and expected_market_date
        and quote_snapshot_is_same_day(
            payload=payload,
            freshness=freshness,
            expected_market_date=expected_market_date,
        )
    )


def classify_signal_feature_readiness(
    *,
    signal_count: Any,
    feature_payload: Any,
    expected_market_date: str | None = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(feature_payload, Mapping):
        return {
            'contract_version': SIGNAL_FEATURE_READINESS_CONTRACT_VERSION,
            'semantic_ready': False,
            'quality_status': 'retry',
            'quality_reasons': ['feature_payload_missing'],
            'direct_signal_source_count': 0,
            'usable_fallback_groups': [],
        }

    payload_market_date = str(feature_payload.get('market_date') or '').strip()
    freshness = feature_payload.get('freshness')
    freshness_market_date = str(
        freshness.get('market_date')
        if isinstance(freshness, Mapping)
        else ''
    ).strip()
    effective_market_date = str(expected_market_date or payload_market_date).strip()
    if not payload_market_date or not freshness_market_date:
        reasons.append('market_date_proof_missing')
    elif payload_market_date != freshness_market_date:
        reasons.append('market_date_proof_mismatch')
    elif effective_market_date and payload_market_date != effective_market_date:
        reasons.append('market_date_stale')

    provenance = feature_payload.get('row_provenance')
    if not isinstance(provenance, Mapping):
        provenance = {}
        reasons.append('row_provenance_missing')
    row_kind = str(provenance.get('row_kind') or '').strip().lower()

    try:
        normalized_signal_count = max(int(signal_count or 0), 0)
    except (TypeError, ValueError):
        normalized_signal_count = 0
    usable_direct_sources = decision_usable_direct_signal_sources(
        feature_payload
    )
    try:
        provenance_direct_count = max(
            int(provenance.get('direct_signal_source_count') or 0),
            0,
        )
    except (TypeError, ValueError):
        provenance_direct_count = 0
    direct_ready = bool(
        normalized_signal_count > 0
        and len(usable_direct_sources) == normalized_signal_count
        and provenance_direct_count == normalized_signal_count
        and row_kind in _DIRECT_ROW_KINDS
    )

    fallback_features = feature_payload.get('fallback_features')
    fallback_features = (
        fallback_features
        if isinstance(fallback_features, Mapping)
        else {}
    )
    raw_fallback_groups = provenance.get('fallback_groups_present')
    if not isinstance(raw_fallback_groups, (list, tuple, set)):
        raw_fallback_groups = []
    declared_fallback_groups = {
        str(group)
        for group in raw_fallback_groups
        if str(group).strip()
    }
    usable_fallback_groups: list[str] = []
    for group, payload in fallback_features.items():
        if (
            group not in _MARKET_FALLBACK_GROUPS
            or group not in declared_fallback_groups
        ):
            continue
        if group == 'quote_snapshot':
            if (
                not isinstance(payload, Mapping)
                or not effective_market_date
                or not quote_snapshot_is_decision_usable(
                    payload=payload,
                    freshness=(
                        freshness if isinstance(freshness, Mapping) else {}
                    ),
                    expected_market_date=effective_market_date,
                )
            ):
                continue
        elif not _has_numeric_decision_value(payload):
            continue
        usable_fallback_groups.append(str(group))
    usable_fallback_groups.sort()
    fallback_ready = bool(
        usable_fallback_groups
        and row_kind in _FALLBACK_ROW_KINDS
    )

    date_and_provenance_ready = not any(
        reason
        in {
            'market_date_proof_missing',
            'market_date_proof_mismatch',
            'market_date_stale',
            'row_provenance_missing',
        }
        for reason in reasons
    )
    semantic_ready = bool(
        date_and_provenance_ready and (direct_ready or fallback_ready)
    )
    if not semantic_ready:
        if normalized_signal_count > 0 and not direct_ready:
            reasons.append('direct_signal_evidence_incomplete')
        if fallback_features and not fallback_ready:
            reasons.append('fallback_decision_values_missing')
        if not direct_ready and not fallback_ready:
            reasons.append('no_decision_usable_signal_feature_evidence')
        quality_status = 'retry'
    else:
        collection = feature_payload.get('collection')
        source_errors = (
            collection.get('source_errors')
            if isinstance(collection, Mapping)
            else {}
        )
        if fallback_ready and not direct_ready:
            reasons.append('fallback_only')
        if isinstance(source_errors, Mapping) and source_errors:
            reasons.append('source_errors_present')
        quality_status = 'degraded' if reasons else 'complete'

    stored_readiness = feature_payload.get('readiness')
    if (
        isinstance(stored_readiness, Mapping)
        and stored_readiness.get('contract_version')
        == SIGNAL_FEATURE_READINESS_CONTRACT_VERSION
    ):
        stored_quality_status = str(
            stored_readiness.get('quality_status') or ''
        ).strip().lower()
        raw_stored_reasons = stored_readiness.get('quality_reasons')
        if not isinstance(raw_stored_reasons, (list, tuple, set)):
            raw_stored_reasons = []
        stored_reasons = [
            str(reason).strip()
            for reason in raw_stored_reasons
            if str(reason).strip()
        ]
        if stored_quality_status == 'retry':
            semantic_ready = False
            quality_status = 'retry'
            reasons.extend(stored_reasons or ['stored_readiness_retry'])
        elif semantic_ready and stored_quality_status == 'degraded':
            quality_status = 'degraded'
            reasons.extend(stored_reasons or ['stored_readiness_degraded'])

    return {
        'contract_version': SIGNAL_FEATURE_READINESS_CONTRACT_VERSION,
        'semantic_ready': semantic_ready,
        'quality_status': quality_status,
        'quality_reasons': list(dict.fromkeys(reasons)),
        'direct_signal_source_count': len(usable_direct_sources),
        'usable_fallback_groups': usable_fallback_groups,
    }
