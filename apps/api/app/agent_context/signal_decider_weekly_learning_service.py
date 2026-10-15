from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
import re
from typing import Any, Callable, Mapping

from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.agent_context.decider_replay_service import (
    actual_execution_outcome_exclusion_reason,
)
from app.agent_context.model_identity import (
    expected_decider_model,
    select_model_identity_cohort_rows,
)
from app.agent_context.roles import SIGNAL_DECIDER_AGENT_NAME
from app.agent_context.signal_decider_calibration_service import (
    signal_decider_calibration_service,
)
from app.agent_context.signal_decider_loop_service import (
    DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
    DEFAULT_SIGNAL_DECIDER_REVIEW_PRIMARY_HORIZON,
    SIGNAL_DECIDER_EVALUATION_HORIZONS,
)
from app.agent_context.signal_decider_performance_criteria import (
    build_published_performance_criteria,
)
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    SignalDeciderActualExecutionOutcome,
    SignalDeciderDecisionSnapshot,
    SignalDeciderWeeklyLearningArtifact,
)


WEEKLY_LEARNING_ARTIFACT_SCHEMA_VERSION = (
    'signal_decider_weekly_learning_artifact_v1'
)
WEEKLY_LEARNING_CONTEXT_SCHEMA_VERSION = (
    'signal_decider_weekly_learning_context_v1'
)
DEFAULT_WEEKLY_LEARNING_LOOKBACK_DAYS = 90
DEFAULT_WEEKLY_LEARNING_VALIDITY_DAYS = 14
MAX_EVIDENCE_MANIFEST_BYTES = 65_536
MAX_IDEMPOTENCY_KEY_LENGTH = 160
_SENSITIVE_MANIFEST_KEY_PARTS = (
    'authorization',
    'cookie',
    'credential',
    'password',
    'secret',
    'token',
)


def _normalize_date(value: str | date, *, field_name: str) -> date:
    if isinstance(value, datetime):
        raise ValueError(f'{field_name} must be a date, not a datetime')
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f'{field_name} must be an ISO date') from exc


def _canonical_json(value: Any) -> str:
    return json.dumps(
        jsonable_encoder(value),
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(',', ':'),
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode('utf-8')).hexdigest()


def _mapping_value(row: Mapping[str, Any] | Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key)


def _normalize_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _artifact_record(row: Mapping[str, Any] | Any) -> dict[str, Any]:
    return {
        'id': _mapping_value(row, 'id'),
        'artifact_version': _mapping_value(row, 'artifact_version'),
        'schema_version': _mapping_value(row, 'schema_version'),
        'week_start': _normalize_date(
            _mapping_value(row, 'week_start'),
            field_name='week_start',
        ),
        'week_end': _normalize_date(
            _mapping_value(row, 'week_end'),
            field_name='week_end',
        ),
        'valid_through': _normalize_date(
            _mapping_value(row, 'valid_through'),
            field_name='valid_through',
        ),
        'horizon_days': int(_mapping_value(row, 'horizon_days')),
        'evaluation_version': _mapping_value(row, 'evaluation_version'),
        'min_sample_size': int(_mapping_value(row, 'min_sample_size')),
        'source_fingerprint': _mapping_value(row, 'source_fingerprint'),
        'content_sha256': _mapping_value(row, 'content_sha256'),
        'publication_status': _mapping_value(row, 'publication_status'),
        'eligibility_payload': deepcopy(
            _mapping_value(row, 'eligibility_payload') or {}
        ),
        'learning_context_payload': deepcopy(
            _mapping_value(row, 'learning_context_payload') or {}
        ),
        'evidence_manifest_payload': deepcopy(
            _mapping_value(row, 'evidence_manifest_payload') or {}
        ),
        'source_report_payload': deepcopy(
            _mapping_value(row, 'source_report_payload') or {}
        ),
        'generated_at': _normalize_datetime(_mapping_value(row, 'generated_at')),
        'published_at': _normalize_datetime(_mapping_value(row, 'published_at')),
    }


def _artifact_hash(record: Mapping[str, Any] | Any) -> str:
    material = _artifact_record(record)
    material.pop('id', None)
    material.pop('content_sha256', None)
    nested_artifact = (
        material.get('learning_context_payload', {})
        .get('performance_criteria', {})
        .get('artifact')
    )
    if isinstance(nested_artifact, dict):
        nested_artifact['content_sha256'] = ''
    return _sha256(material)


def _serialize_artifact(row: Mapping[str, Any] | Any) -> dict[str, Any]:
    record = _artifact_record(row)
    eligibility = record['eligibility_payload']
    training_window = {
        key: eligibility.get(key)
        for key in (
            'training_date_from',
            'training_date_to',
            'lookback_days',
            'maturity_buffer_days',
        )
        if key in eligibility
    }
    published_date = (
        record['published_at'].date()
        if record['published_at'] is not None
        else record['week_end']
    )
    effective_from = max(
        record['week_end'] + timedelta(days=1),
        published_date + timedelta(days=1),
    )
    return {
        'id': record['id'],
        'artifact_version': record['artifact_version'],
        'schema_version': record['schema_version'],
        'week_start': record['week_start'].isoformat(),
        'week_end': record['week_end'].isoformat(),
        'valid_through': record['valid_through'].isoformat(),
        'effective_from': effective_from.isoformat(),
        'horizon_days': record['horizon_days'],
        'evaluation_version': record['evaluation_version'],
        'min_sample_size': record['min_sample_size'],
        'source_fingerprint': record['source_fingerprint'],
        'content_sha256': record['content_sha256'],
        'publication_status': record['publication_status'],
        'eligibility': eligibility,
        'training_window': training_window,
        'learning_context': record['learning_context_payload'],
        'evidence_manifest': record['evidence_manifest_payload'],
        'source_report': record['source_report_payload'],
        'generated_at': (
            record['generated_at'].isoformat()
            if record['generated_at'] is not None
            else None
        ),
        'published_at': (
            record['published_at'].isoformat()
            if record['published_at'] is not None
            else None
        ),
    }


def _contains_sensitive_manifest_key(value: Any) -> bool:
    if isinstance(value, dict):
        for raw_key, nested in value.items():
            key = str(raw_key).strip().lower()
            if any(part in key for part in _SENSITIVE_MANIFEST_KEY_PARTS):
                return True
            if _contains_sensitive_manifest_key(nested):
                return True
    elif isinstance(value, list):
        return any(_contains_sensitive_manifest_key(item) for item in value)
    return False


def _contains_non_finite_number(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Decimal):
        return not value.is_finite()
    if isinstance(value, dict):
        return any(_contains_non_finite_number(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_non_finite_number(item) for item in value)
    return False


def _normalize_idempotency_key(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        raise ValueError('evidence_manifest.idempotency_key may not be empty')
    if len(normalized) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise ValueError(
            'evidence_manifest.idempotency_key may not exceed '
            f'{MAX_IDEMPOTENCY_KEY_LENGTH} characters'
        )
    if re.fullmatch(r'[A-Za-z0-9._:/-]+', normalized) is None:
        raise ValueError(
            'evidence_manifest.idempotency_key contains unsupported characters'
        )
    return normalized


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (ArithmeticError, ValueError):
        return None
    return result if result.is_finite() else None


class SignalDeciderWeeklyLearningRepository:
    def summarize_actual_execution_outcomes(
        self,
        *,
        week_start: date,
        week_end: date,
    ) -> dict[str, Any]:
        with session_scope() as session:
            rows = list(
                session.execute(
                    select(
                        SignalDeciderActualExecutionOutcome,
                        SignalDeciderDecisionSnapshot,
                    )
                    .join(
                        SignalDeciderDecisionSnapshot,
                        SignalDeciderDecisionSnapshot.id
                        == SignalDeciderActualExecutionOutcome.decision_snapshot_id,
                    )
                    .where(
                        SignalDeciderActualExecutionOutcome.market_date
                        >= week_start,
                        SignalDeciderActualExecutionOutcome.market_date
                        <= week_end,
                        SignalDeciderDecisionSnapshot.agent_name
                        == SIGNAL_DECIDER_AGENT_NAME,
                    )
                    .order_by(
                        SignalDeciderActualExecutionOutcome.market_date.asc(),
                        SignalDeciderActualExecutionOutcome.id.asc(),
                    )
                ).all()
            )

        snapshots = [snapshot for _, snapshot in rows]
        verified_snapshots, identity_exclusions, cohort = (
            select_model_identity_cohort_rows(
                snapshots,
                role=SIGNAL_DECIDER_AGENT_NAME,
                expected_model=expected_decider_model(
                    SIGNAL_DECIDER_AGENT_NAME,
                    settings_obj=settings,
                ),
            )
        )
        verified_snapshot_ids = {int(snapshot.id) for snapshot in verified_snapshots}
        identity_exclusion_by_snapshot = {
            int(item['snapshot_id']): str(item.get('status_reason') or '')
            for item in identity_exclusions
        }
        exclusion_counts: Counter[str] = Counter()
        eligible_outcomes: list[SignalDeciderActualExecutionOutcome] = []
        for outcome, snapshot in rows:
            if int(snapshot.id) not in verified_snapshot_ids:
                exclusion_counts[
                    identity_exclusion_by_snapshot.get(
                        int(snapshot.id),
                        'model_identity_unverified',
                    )
                ] += 1
                continue
            exclusion_reason = actual_execution_outcome_exclusion_reason(outcome)
            if exclusion_reason is not None:
                exclusion_counts[exclusion_reason] += 1
                continue
            eligible_outcomes.append(outcome)

        pnl_values = [
            value
            for value in (
                _decimal(outcome.realized_pnl_krw)
                for outcome in eligible_outcomes
            )
            if value is not None
        ]
        return_values = [
            value
            for value in (
                _decimal(outcome.realized_return_pct)
                for outcome in eligible_outcomes
            )
            if value is not None
        ]
        win_count = sum(1 for value in pnl_values if value > 0)
        loss_count = sum(1 for value in pnl_values if value < 0)
        eligible_count = len(eligible_outcomes)
        total_pnl = sum(pnl_values, Decimal('0'))
        average_return = (
            sum(return_values, Decimal('0')) / Decimal(len(return_values))
            if return_values
            else None
        )
        return {
            'status': 'eligible' if eligible_count else 'unavailable',
            'source_count': len(rows),
            'model_verified_count': sum(
                1 for _, snapshot in rows if int(snapshot.id) in verified_snapshot_ids
            ),
            'eligible_count': eligible_count,
            'excluded_count': len(rows) - eligible_count,
            'win_count': win_count,
            'loss_count': loss_count,
            'win_rate': (
                round(win_count / eligible_count, 6) if eligible_count else None
            ),
            'total_realized_pnl_krw': f'{total_pnl:.2f}',
            'average_realized_return_pct': (
                round(float(average_return), 6)
                if average_return is not None
                else None
            ),
            'metric_basis': 'gross_before_transaction_costs',
            'model_identity_cohort': cohort,
            'exclusion_reason_counts': dict(sorted(exclusion_counts.items())),
        }

    def get_by_source_fingerprint(
        self,
        source_fingerprint: str,
    ) -> dict[str, Any] | None:
        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderWeeklyLearningArtifact).where(
                    SignalDeciderWeeklyLearningArtifact.source_fingerprint
                    == source_fingerprint
                )
            ).scalar_one_or_none()
            return _artifact_record(row) if row is not None else None

    def insert_artifact(self, item: dict[str, Any]) -> dict[str, Any]:
        with session_scope() as session:
            row = SignalDeciderWeeklyLearningArtifact(**item)
            session.add(row)
            session.flush()
            return _artifact_record(row)

    def list_published_candidates(
        self,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            rows = list(
                session.execute(
                select(SignalDeciderWeeklyLearningArtifact)
                .where(
                    SignalDeciderWeeklyLearningArtifact.publication_status
                    == 'published'
                )
                .order_by(
                    SignalDeciderWeeklyLearningArtifact.published_at.desc(),
                    SignalDeciderWeeklyLearningArtifact.id.desc(),
                )
                .limit(max(1, min(int(limit), 500)))
                ).scalars().all()
            )
            return [_artifact_record(row) for row in rows]


class SignalDeciderWeeklyLearningService:
    def __init__(
        self,
        *,
        calibration_service=signal_decider_calibration_service,
        repository: SignalDeciderWeeklyLearningRepository | None = None,
        now: Callable[[], datetime] | None = None,
        lookback_days: int = DEFAULT_WEEKLY_LEARNING_LOOKBACK_DAYS,
        validity_days: int = DEFAULT_WEEKLY_LEARNING_VALIDITY_DAYS,
    ) -> None:
        self.calibration_service = calibration_service
        self.repository = repository or SignalDeciderWeeklyLearningRepository()
        self._now = now or (lambda: datetime.now(timezone.utc))
        self.lookback_days = int(lookback_days)
        self.validity_days = int(validity_days)
        if self.lookback_days < 1:
            raise ValueError('lookback_days must be positive')
        if self.validity_days < 1:
            raise ValueError('validity_days must be positive')

    def publish(
        self,
        *,
        week_start: str | date,
        week_end: str | date,
        horizon_days: int = DEFAULT_SIGNAL_DECIDER_REVIEW_PRIMARY_HORIZON,
        evaluation_version: str = DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
        min_sample_size: int = 30,
        evidence_manifest: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        resolved_week_start = _normalize_date(
            week_start,
            field_name='week_start',
        )
        resolved_week_end = _normalize_date(week_end, field_name='week_end')
        if resolved_week_start > resolved_week_end:
            raise ValueError('week_start must be on or before week_end')
        if (resolved_week_end - resolved_week_start).days > 13:
            raise ValueError('publication evidence window may not exceed 14 days')
        resolved_horizon = int(horizon_days)
        if resolved_horizon not in SIGNAL_DECIDER_EVALUATION_HORIZONS:
            raise ValueError(
                'horizon_days must be drawn from '
                f'{list(SIGNAL_DECIDER_EVALUATION_HORIZONS)}'
            )
        resolved_evaluation_version = str(evaluation_version or '').strip()
        if not resolved_evaluation_version or len(resolved_evaluation_version) > 64:
            raise ValueError('evaluation_version must be between 1 and 64 characters')
        resolved_min_sample_size = int(min_sample_size)
        if not 3 <= resolved_min_sample_size <= 1000:
            raise ValueError('min_sample_size must be between 3 and 1000')
        resolved_manifest = deepcopy(evidence_manifest or {})
        if not isinstance(resolved_manifest, dict):
            raise ValueError('evidence_manifest must be an object')
        if _contains_sensitive_manifest_key(resolved_manifest):
            raise ValueError('evidence_manifest may not contain sensitive credential fields')
        if _contains_non_finite_number(resolved_manifest):
            raise ValueError('evidence_manifest must contain only finite JSON numbers')
        if len(_canonical_json(resolved_manifest).encode('utf-8')) > MAX_EVIDENCE_MANIFEST_BYTES:
            raise ValueError(
                f'evidence_manifest may not exceed {MAX_EVIDENCE_MANIFEST_BYTES} bytes'
            )

        idempotency_key = _normalize_idempotency_key(
            resolved_manifest.get('idempotency_key')
        )
        maturity_buffer_days = max(14, resolved_horizon * 3)
        stable_publication_scope = {
            'schema_version': WEEKLY_LEARNING_ARTIFACT_SCHEMA_VERSION,
            'idempotency_key': idempotency_key,
            'week_start': resolved_week_start.isoformat(),
            'week_end': resolved_week_end.isoformat(),
            'lookback_days': self.lookback_days,
            'maturity_buffer_days': maturity_buffer_days,
            'validity_days': self.validity_days,
            'horizon_days': resolved_horizon,
            'evaluation_version': resolved_evaluation_version,
            'min_sample_size': resolved_min_sample_size,
        }
        source_fingerprint = (
            _sha256(stable_publication_scope)
            if idempotency_key is not None
            else None
        )
        if source_fingerprint is not None:
            existing = self.repository.get_by_source_fingerprint(
                source_fingerprint
            )
            if existing is not None:
                return {
                    'status': _mapping_value(existing, 'publication_status'),
                    'idempotent_replay': True,
                    'artifact': _serialize_artifact(existing),
                }

        # The publication week is an evidence/audit window, not the training
        # cohort. A conservative calendar buffer keeps recent h5 decisions,
        # whose forward bars are still pending, out of coverage-gap counts.
        training_date_to = resolved_week_start - timedelta(
            days=maturity_buffer_days
        )
        training_date_from = training_date_to - timedelta(
            days=self.lookback_days
        )
        report = jsonable_encoder(
            self.calibration_service.build_report(
                market_date_from=training_date_from,
                market_date_to=training_date_to,
                horizon_days=resolved_horizon,
                evaluation_version=resolved_evaluation_version,
                min_sample_size=resolved_min_sample_size,
                cluster_limit=10,
            )
        )
        actual_execution = jsonable_encoder(
            self.repository.summarize_actual_execution_outcomes(
                week_start=resolved_week_start,
                week_end=resolved_week_end,
            )
        )
        eligibility = self._build_eligibility(
            report=report,
            min_sample_size=resolved_min_sample_size,
            training_date_from=training_date_from,
            training_date_to=training_date_to,
            lookback_days=self.lookback_days,
            maturity_buffer_days=maturity_buffer_days,
        )
        request_material = {
            'schema_version': WEEKLY_LEARNING_ARTIFACT_SCHEMA_VERSION,
            'week_start': resolved_week_start.isoformat(),
            'week_end': resolved_week_end.isoformat(),
            'training_date_from': training_date_from.isoformat(),
            'training_date_to': training_date_to.isoformat(),
            'lookback_days': self.lookback_days,
            'maturity_buffer_days': maturity_buffer_days,
            'validity_days': self.validity_days,
            'horizon_days': resolved_horizon,
            'evaluation_version': resolved_evaluation_version,
            'min_sample_size': resolved_min_sample_size,
            'evidence_manifest': resolved_manifest,
            'calibration_report': report,
            'actual_execution': actual_execution,
        }
        source_fingerprint = source_fingerprint or _sha256(request_material)
        existing = self.repository.get_by_source_fingerprint(source_fingerprint)
        if existing is not None:
            return {
                'status': _mapping_value(existing, 'publication_status'),
                'idempotent_replay': True,
                'artifact': _serialize_artifact(existing),
            }

        now = self._now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        artifact_version = (
            f'signal_decider_weekly_learning_{resolved_week_end.isoformat()}_'
            f'{source_fingerprint[:12]}'
        )
        publication_status = (
            'published' if eligibility['eligible'] else 'ineligible'
        )
        published_at = now if publication_status == 'published' else None
        valid_through = resolved_week_end + timedelta(days=self.validity_days)
        effective_from = max(
            resolved_week_end + timedelta(days=1),
            now.date() + timedelta(days=1),
        )
        learning_context = {
            'schema_version': WEEKLY_LEARNING_CONTEXT_SCHEMA_VERSION,
            'training_window': {
                'training_date_from': training_date_from.isoformat(),
                'training_date_to': training_date_to.isoformat(),
                'lookback_days': self.lookback_days,
                'maturity_buffer_days': maturity_buffer_days,
            },
            'performance_criteria': build_published_performance_criteria(
                report=report,
                artifact_version=artifact_version,
                source_fingerprint=source_fingerprint,
                content_sha256='',
                published_at=published_at.isoformat() if published_at else '',
                effective_from=effective_from.isoformat(),
                week_start=resolved_week_start.isoformat(),
                week_end=resolved_week_end.isoformat(),
                valid_through=valid_through.isoformat(),
                actual_execution=actual_execution,
            ),
        }
        item = {
            'artifact_version': artifact_version,
            'schema_version': WEEKLY_LEARNING_ARTIFACT_SCHEMA_VERSION,
            'week_start': resolved_week_start,
            'week_end': resolved_week_end,
            'valid_through': valid_through,
            'horizon_days': resolved_horizon,
            'evaluation_version': resolved_evaluation_version,
            'min_sample_size': resolved_min_sample_size,
            'source_fingerprint': source_fingerprint,
            'content_sha256': '',
            'publication_status': publication_status,
            'eligibility_payload': eligibility,
            'learning_context_payload': learning_context,
            'evidence_manifest_payload': resolved_manifest,
            'source_report_payload': report,
            'generated_at': now,
            'published_at': published_at,
        }
        content_sha256 = _artifact_hash(item)
        item['content_sha256'] = content_sha256
        item['learning_context_payload']['performance_criteria']['artifact'][
            'content_sha256'
        ] = content_sha256
        try:
            stored = self.repository.insert_artifact(item)
        except IntegrityError:
            existing = self.repository.get_by_source_fingerprint(
                source_fingerprint
            )
            if existing is None:
                raise
            return {
                'status': _mapping_value(existing, 'publication_status'),
                'idempotent_replay': True,
                'artifact': _serialize_artifact(existing),
            }
        return {
            'status': publication_status,
            'idempotent_replay': False,
            'artifact': _serialize_artifact(stored),
        }

    def get_latest_published(
        self,
        *,
        as_of_date: str | date | None = None,
    ) -> dict[str, Any]:
        resolved_as_of = (
            _normalize_date(as_of_date, field_name='as_of_date')
            if as_of_date is not None
            else self._now().date()
        )
        rows = self.repository.list_published_candidates(limit=100)
        if not rows:
            return {'available': False, 'artifact': None}
        first_rejection: dict[str, Any] | None = None
        for row in rows:
            artifact = _serialize_artifact(row)
            if _artifact_hash(row) != artifact['content_sha256']:
                first_rejection = first_rejection or {
                    'available': False,
                    'artifact': None,
                    'reason': 'artifact_content_hash_mismatch',
                }
                continue
            eligibility = artifact.get('eligibility')
            if (
                artifact.get('publication_status') != 'published'
                or not isinstance(eligibility, dict)
                or eligibility.get('eligible') is not True
            ):
                first_rejection = first_rejection or {
                    'available': False,
                    'artifact': None,
                    'reason': 'artifact_not_eligible',
                }
                continue
            effective_from = date.fromisoformat(artifact['effective_from'])
            if resolved_as_of < effective_from:
                first_rejection = first_rejection or {
                    'available': False,
                    'artifact': artifact,
                    'reason': 'artifact_not_yet_effective',
                    'stale': False,
                    'as_of_date': resolved_as_of.isoformat(),
                    'effective_from': effective_from.isoformat(),
                }
                continue
            valid_through = date.fromisoformat(artifact['valid_through'])
            if resolved_as_of > valid_through:
                first_rejection = first_rejection or {
                    'available': False,
                    'artifact': artifact,
                    'reason': 'artifact_stale',
                    'stale': True,
                    'as_of_date': resolved_as_of.isoformat(),
                    'valid_through': valid_through.isoformat(),
                }
                continue
            return {
                'available': True,
                'artifact': artifact,
                'stale': False,
                'as_of_date': resolved_as_of.isoformat(),
                'effective_from': effective_from.isoformat(),
                'valid_through': valid_through.isoformat(),
            }
        return first_rejection or {'available': False, 'artifact': None}

    def get_latest_performance_criteria(
        self,
        *,
        as_of_date: str | date | None = None,
    ) -> dict[str, Any] | None:
        latest = self.get_latest_published(as_of_date=as_of_date)
        if latest.get('available') is not True:
            return None
        artifact = latest.get('artifact')
        learning_context = (
            artifact.get('learning_context')
            if isinstance(artifact, dict)
            else None
        )
        criteria = (
            learning_context.get('performance_criteria')
            if isinstance(learning_context, dict)
            else None
        )
        return deepcopy(criteria) if isinstance(criteria, dict) else None

    @staticmethod
    def _build_eligibility(
        *,
        report: dict[str, Any],
        min_sample_size: int,
        training_date_from: date,
        training_date_to: date,
        lookback_days: int,
        maturity_buffer_days: int,
    ) -> dict[str, Any]:
        summary = report.get('summary') if isinstance(report.get('summary'), dict) else {}
        request = report.get('request') if isinstance(report.get('request'), dict) else {}
        review_boundary = (
            report.get('review_boundary')
            if isinstance(report.get('review_boundary'), dict)
            else {}
        )
        sample_count = int(summary.get('mature_computed_count') or 0)
        reasons: list[str] = []
        if summary.get('status') != 'ready':
            reasons.append('calibration_report_not_ready')
        if sample_count < min_sample_size:
            reasons.append('insufficient_verified_evaluation_sample')
        if summary.get('ready_for_operator_review') is not True:
            reasons.append('operator_review_not_ready')
        if int(summary.get('coverage_gap_count') or 0) > 0:
            reasons.append('evaluation_coverage_gaps_present')
        if request.get('model_identity_cohort') in {None, '', 'invalid_expected_model'}:
            reasons.append('verified_model_identity_cohort_missing')
        if not (
            review_boundary.get('manual_review_only') is True
            and review_boundary.get('auto_apply_prompt_changes') is False
            and review_boundary.get('auto_apply_policy_changes') is False
        ):
            reasons.append('unsafe_review_boundary')
        return {
            'eligible': not reasons,
            'reasons': reasons,
            'verified_evaluation_sample_count': sample_count,
            'min_sample_size': min_sample_size,
            'coverage_gap_count': int(summary.get('coverage_gap_count') or 0),
            'model_identity_cohort': request.get('model_identity_cohort'),
            'training_date_from': training_date_from.isoformat(),
            'training_date_to': training_date_to.isoformat(),
            'lookback_days': lookback_days,
            'maturity_buffer_days': maturity_buffer_days,
            'manual_review_only': True,
            'auto_apply_prompt_changes': False,
            'auto_apply_policy_changes': False,
        }


signal_decider_weekly_learning_service = SignalDeciderWeeklyLearningService()
