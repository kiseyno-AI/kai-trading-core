from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable


DART_SHARE_ISSUANCE_OPS_POLICY_VERSION = (
    'dart_share_issuance_ops_backfill_v2'
)
DART_SHARE_ISSUANCE_MANIFEST_VERSION = (
    'dart_share_issuance_stock_code_manifest_v2'
)
DART_SHARE_ISSUANCE_CURSOR_VERSION = (
    'dart_share_issuance_stock_code_cursor_v1'
)
DART_SHARE_ISSUANCE_OUTCOME_MANIFEST_VERSION = (
    'dart_share_issuance_outcome_manifest_v2'
)
DART_SHARE_ISSUANCE_AGGREGATE_AUDIT_VERSION = (
    'dart_share_issuance_aggregate_audit_v1'
)
DART_SHARE_ISSUANCE_BACKFILL_TRIGGER = 'ops_dart_issuance_backfill'
DART_SHARE_ISSUANCE_AUDIT_TRIGGER = 'ops_dart_issuance_audit'
DART_SHARE_ISSUANCE_ACTIVATION_AUDIT_TRIGGER = (
    'ops_dart_issuance_activation'
)
DART_SHARE_ISSUANCE_MANIFEST_SOURCE_ACTIVE_UNIVERSE = (
    'current_active_universe'
)
DART_SHARE_ISSUANCE_MANIFEST_SOURCE_EXPLICIT = 'explicit_stock_codes'
DART_SHARE_ISSUANCE_AUDIT_SCOPE_GLOBAL_ACTIVATION = (
    'global_activation_bootstrap'
)
DART_SHARE_ISSUANCE_AUDIT_SCOPE_EXPLICIT_SUBSET = (
    'bounded_explicit_subset'
)


def _canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def _strict_stock_code_manifest(
    values: Iterable[Any] | None,
) -> list[str] | None:
    if values is None or isinstance(values, (str, bytes, dict)):
        return None
    normalized: list[str] = []
    try:
        iterator = iter(values)
    except TypeError:
        return None
    for value in iterator:
        raw = str(value or '').strip()
        if not raw or not raw.isdigit() or len(raw) > 6:
            return None
        normalized.append(raw.zfill(6))
    if not normalized:
        return None
    if normalized != sorted(set(normalized)):
        return None
    return normalized


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value or '').strip())
    except ValueError:
        return None


def _parse_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or '').strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        except ValueError:
            return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _observed_scope_count(values: Iterable[Any] | None) -> int | None:
    if values is None:
        return None
    try:
        return len(list(values))
    except (TypeError, RuntimeError):
        return None


def _expected_evidence_policy() -> dict[str, str]:
    # Lazy import avoids making the persistence store import the signals
    # service (which imports the sync service) during module initialization.
    from app.dart.signals_service import (
        SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION,
        SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA,
        SHARE_ISSUANCE_RULE_VERSION,
    )

    return {
        'authority_policy_version': (
            SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
        ),
        'coverage_schema_version': (
            SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA
        ),
        'rule_version': SHARE_ISSUANCE_RULE_VERSION,
    }


def _build_audit_manifest(
    *,
    market_date: date,
    window_start: date,
    window_end: date,
    stock_codes: list[str],
    manifest_source: str,
    audit_scope: str,
) -> dict[str, Any]:
    return {
        'manifest_version': DART_SHARE_ISSUANCE_MANIFEST_VERSION,
        'policy_version': DART_SHARE_ISSUANCE_OPS_POLICY_VERSION,
        'market_date': market_date.isoformat(),
        'window_start': window_start.isoformat(),
        'window_end': window_end.isoformat(),
        'requested_stock_codes': stock_codes,
        'manifest_source': manifest_source,
        'audit_scope': audit_scope,
    }


def inspect_share_issuance_activation_audit(
    run: dict[str, Any] | None,
    *,
    current_market_date: date | str | None = None,
    current_stock_codes: Iterable[Any] | None = None,
) -> dict[str, Any]:
    """Validate a durable bootstrap proof for the global issuance path.

    ``current_market_date`` and ``current_stock_codes`` are accepted for
    caller observability only.  Activation deliberately depends on the
    immutable, fully-audited bootstrap manifest.  Later dates and additive
    symbols are admitted by the daily global scan and become that run's
    explicit retry debt when evidence is not ready.
    """
    reasons: list[str] = []

    def reject(reason: str) -> None:
        if reason not in reasons:
            reasons.append(reason)

    from app.dart.signals_service import (
        SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS,
    )

    if not isinstance(run, dict):
        return {
            'enabled': False,
            'reasons': ['durable_full_manifest_audit_missing'],
            'audit_run_id': None,
        }

    run_id = run.get('id')
    raw_metadata = run.get('run_metadata')
    if not isinstance(raw_metadata, dict):
        reject('audit_run_metadata_invalid')
        metadata: dict[str, Any] = {}
    else:
        metadata = dict(raw_metadata)
    raw_aggregate = metadata.get('aggregate_audit')
    if not isinstance(raw_aggregate, dict):
        reject('aggregate_audit_invalid')
        aggregate: dict[str, Any] = {}
    else:
        aggregate = dict(raw_aggregate)
    requested_stock_codes = aggregate.get('requested_stock_codes')
    manifest_source = aggregate.get('manifest_source')
    audit_scope = aggregate.get('audit_scope')
    classifications = aggregate.get('classification_stock_codes')
    outcome_manifest = aggregate.get('outcome_manifest')

    normalized_requested = _strict_stock_code_manifest(
        requested_stock_codes
        if isinstance(requested_stock_codes, list)
        else None
    )
    if normalized_requested is None:
        reject('requested_manifest_not_exact_sorted_unique')
        normalized_requested = []
    audit_market_date = _parse_date(aggregate.get('market_date'))
    audit_window_start = _parse_date(aggregate.get('window_start'))
    audit_window_end = _parse_date(aggregate.get('window_end'))
    if audit_market_date is None:
        reject('audit_market_date_invalid')
    if audit_window_start is None:
        reject('audit_window_start_invalid')
    if audit_window_end is None:
        reject('audit_window_end_invalid')
    if (
        audit_market_date is not None
        and audit_window_end != audit_market_date
    ):
        reject('audit_window_end_mismatch')
    if (
        audit_market_date is not None
        and audit_window_start is not None
        and audit_window_start
        > audit_market_date
        - timedelta(days=SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS)
    ):
        reject('audit_window_coverage_too_short')

    audit_manifest: dict[str, Any] | None = None
    audit_manifest_hash: str | None = None
    if (
        normalized_requested
        and audit_market_date is not None
        and audit_window_start is not None
        and audit_window_end is not None
    ):
        audit_manifest = _build_audit_manifest(
            market_date=audit_market_date,
            window_start=audit_window_start,
            window_end=audit_window_end,
            stock_codes=normalized_requested,
            manifest_source=str(manifest_source or ''),
            audit_scope=str(audit_scope or ''),
        )
        audit_manifest_hash = _canonical_json_sha256(audit_manifest)

    if str(run.get('status') or '').strip().lower() != 'completed':
        reject('audit_run_not_completed')
    if run.get('trigger') != (
        DART_SHARE_ISSUANCE_ACTIVATION_AUDIT_TRIGGER
    ):
        reject('audit_trigger_mismatch')
    if metadata.get('audit_type') != 'dart_share_issuance_aggregate':
        reject('audit_type_mismatch')
    if aggregate.get('audit_version') != (
        DART_SHARE_ISSUANCE_AGGREGATE_AUDIT_VERSION
    ):
        reject('audit_version_mismatch')
    if aggregate.get('policy_version') != (
        DART_SHARE_ISSUANCE_OPS_POLICY_VERSION
    ):
        reject('audit_policy_version_mismatch')
    if manifest_source != (
        DART_SHARE_ISSUANCE_MANIFEST_SOURCE_ACTIVE_UNIVERSE
    ):
        reject('activation_manifest_source_mismatch')
    if audit_scope != DART_SHARE_ISSUANCE_AUDIT_SCOPE_GLOBAL_ACTIVATION:
        reject('activation_audit_scope_mismatch')
    if aggregate.get('evidence_policy') != _expected_evidence_policy():
        reject('audit_evidence_policy_version_mismatch')
    for key in (
        'aggregate_evidence_ready',
        'aggregate_evidence_complete',
        'durable_audit_complete',
        'aggregate_manifest_classification_complete',
        'aggregate_mapping_contract_exact',
    ):
        if aggregate.get(key) is not True:
            reject(f'{key}_not_proven')
    if aggregate.get('persistence_status') != 'completed':
        reject('audit_persistence_not_completed')
    if aggregate.get('audit_run_id') != run_id:
        reject('audit_run_identity_mismatch')
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        reject('audit_run_identity_invalid')
    if aggregate.get('mapping_lookup_error') is not None:
        reject('mapping_lookup_error_present')
    if aggregate.get('evidence_read_error') is not None:
        reject('evidence_read_error_present')
    if aggregate.get('unexpected_mapping_stock_codes') != []:
        reject('unexpected_mapping_stock_codes_present')
    mapping_authority = aggregate.get('mapping_authority')
    if (
        not isinstance(mapping_authority, dict)
        or mapping_authority.get('authoritative') is not True
    ):
        reject('mapping_authority_not_authoritative')
    started_at = _parse_utc_datetime(run.get('started_at'))
    completed_at = _parse_utc_datetime(run.get('completed_at'))
    if started_at is None:
        reject('audit_started_at_invalid')
    if completed_at is None:
        reject('audit_completed_at_invalid')
    if (
        started_at is not None
        and completed_at is not None
        and completed_at < started_at
    ):
        reject('audit_completion_precedes_start')

    if run.get('window_start') != aggregate.get('window_start'):
        reject('audit_run_window_start_mismatch')
    if run.get('window_end') != aggregate.get('window_end'):
        reject('audit_run_window_end_mismatch')
    if aggregate.get('manifest_version') != (
        DART_SHARE_ISSUANCE_MANIFEST_VERSION
    ):
        reject('audit_manifest_version_mismatch')
    if (
        audit_manifest_hash is None
        or aggregate.get('manifest_hash') != audit_manifest_hash
    ):
        reject('audit_manifest_hash_mismatch')
    if aggregate.get('requested_count') != len(normalized_requested):
        reject('requested_manifest_count_mismatch')

    classification_keys = (
        'completed_positive',
        'completed_negative',
        'terminal_not_applicable',
        'retry_debt',
    )
    if not isinstance(classifications, dict):
        reject('classification_manifest_missing')
        classification_lists = {key: [] for key in classification_keys}
    else:
        classification_lists: dict[str, list[str]] = {}
        for key in classification_keys:
            value = classifications.get(key)
            normalized = _strict_stock_code_manifest(value)
            if value == []:
                normalized = []
            if normalized is None:
                reject(f'{key}_classification_invalid')
                normalized = []
            classification_lists[key] = normalized
    classified = [
        stock_code
        for key in classification_keys
        for stock_code in classification_lists[key]
    ]
    if sorted(classified) != normalized_requested or len(classified) != len(
        set(classified)
    ):
        reject('classification_partition_not_exact')
    if classification_lists['retry_debt']:
        reject('retry_debt_not_empty')
    expected_counts = {
        key: len(classification_lists[key]) for key in classification_keys
    }
    if aggregate.get('classification_counts') != expected_counts:
        reject('classification_counts_mismatch')

    if not isinstance(outcome_manifest, dict):
        reject('outcome_manifest_missing')
    else:
        if outcome_manifest.get('outcome_manifest_version') != (
            DART_SHARE_ISSUANCE_OUTCOME_MANIFEST_VERSION
        ):
            reject('outcome_manifest_version_mismatch')
        if outcome_manifest.get('policy_version') != (
            DART_SHARE_ISSUANCE_OPS_POLICY_VERSION
        ):
            reject('outcome_policy_version_mismatch')
        if outcome_manifest.get('manifest_source') != manifest_source:
            reject('outcome_manifest_source_mismatch')
        if outcome_manifest.get('audit_scope') != audit_scope:
            reject('outcome_audit_scope_mismatch')
        if outcome_manifest.get('source_manifest_version') != (
            DART_SHARE_ISSUANCE_MANIFEST_VERSION
        ):
            reject('outcome_source_manifest_version_mismatch')
        if outcome_manifest.get('source_manifest_hash') != (
            audit_manifest_hash
        ):
            reject('outcome_source_manifest_hash_mismatch')
        for field, expected_value in (
            ('market_date', aggregate.get('market_date')),
            ('window_start', aggregate.get('window_start')),
            ('window_end', aggregate.get('window_end')),
        ):
            if outcome_manifest.get(field) != expected_value:
                reject(f'outcome_{field}_mismatch')
        if outcome_manifest.get('requested_stock_codes') != (
            normalized_requested
        ):
            reject('outcome_requested_manifest_mismatch')

        outcomes = outcome_manifest.get('outcomes')
        outcome_classifications: dict[str, str] = {}
        if not isinstance(outcomes, list):
            reject('outcome_classifications_missing')
        else:
            for item in outcomes:
                if not isinstance(item, dict):
                    reject('outcome_classification_invalid')
                    continue
                raw_stock_code = str(item.get('stock_code') or '').strip()
                stock_code = (
                    raw_stock_code.zfill(6)
                    if raw_stock_code.isdigit()
                    and len(raw_stock_code) <= 6
                    else ''
                )
                classification = str(
                    item.get('classification') or ''
                ).strip()
                if (
                    not stock_code
                    or stock_code in outcome_classifications
                    or classification not in classification_keys
                ):
                    reject('outcome_classification_invalid')
                    continue
                outcome_classifications[stock_code] = classification
                if classification == 'completed_positive':
                    if not str(item.get('corp_code') or '').strip():
                        reject('positive_outcome_evidence_invalid')
                    positive_date = _parse_date(
                        item.get('positive_event_date')
                    )
                    if (
                        positive_date is None
                        or audit_window_start is None
                        or positive_date < audit_window_start
                        or audit_market_date is None
                        or positive_date > audit_market_date
                    ):
                        reject('positive_outcome_evidence_invalid')
                elif classification == 'completed_negative':
                    if not str(item.get('corp_code') or '').strip():
                        reject('negative_outcome_coverage_invalid')
                    coverage_start = _parse_date(
                        item.get('coverage_window_start')
                    )
                    coverage_end = _parse_date(
                        item.get('coverage_window_end')
                    )
                    if (
                        coverage_start is None
                        or audit_window_start is None
                        or coverage_start > audit_window_start
                        or coverage_end != audit_window_end
                    ):
                        reject('negative_outcome_coverage_invalid')
                elif classification == 'terminal_not_applicable':
                    if item.get('reason') != (
                        'authoritative_unsupported_mapping'
                    ):
                        reject('terminal_outcome_authority_invalid')
            if sorted(outcome_classifications) != normalized_requested:
                reject('outcome_classification_scope_mismatch')
            for key in classification_keys:
                actual_codes = sorted(
                    stock_code
                    for stock_code, classification in (
                        outcome_classifications.items()
                    )
                    if classification == key
                )
                if actual_codes != classification_lists[key]:
                    reject('outcome_classification_partition_mismatch')
                    break

        if aggregate.get('outcome_manifest_hash_algorithm') != 'sha256':
            reject('outcome_manifest_hash_algorithm_mismatch')
        if aggregate.get('outcome_manifest_sha256') != (
            _canonical_json_sha256(outcome_manifest)
        ):
            reject('outcome_manifest_hash_mismatch')

    return {
        'enabled': not reasons,
        'reasons': reasons,
        'audit_run_id': run_id,
        'manifest_hash': aggregate.get('manifest_hash'),
        'recomputed_manifest_hash': audit_manifest_hash,
        'market_date': aggregate.get('market_date'),
        'current_market_date': (
            _parse_date(current_market_date).isoformat()
            if _parse_date(current_market_date) is not None
            else None
        ),
        'requested_count': len(normalized_requested),
        'current_requested_count': _observed_scope_count(
            current_stock_codes
        ),
        'retry_debt_count': len(classification_lists['retry_debt']),
        'policy_version': aggregate.get('policy_version'),
        'bootstrap_authorization': not reasons,
    }
