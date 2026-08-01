from __future__ import annotations

from datetime import date
from typing import Any

from app.core.settings import settings

FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER: tuple[str, ...] = (
    'pykrx_enrichment',
    'fundamentals_daily_refresh',
    'market_risk_filter',
    'piotroski_filter',
    'momentum_factors',
    'value_factors',
    'low_risk_factors',
    'beta_factors',
    'advanced_value_factors',
    'unified_value_factors',
    'quality_factors',
    'composite_factors',
)
PREREQUISITE_WAITING_STATUSES = {'pending', 'running'}
PREREQUISITE_BLOCKED_STATUSES = {'failed', 'partial', 'blocked'}
GENERIC_PARTIAL_PREREQUISITE_MIN_COVERAGE_RATIO = 0.99
PYKRX_ENRICHMENT_PREREQUISITE_STAGE_ID = 'pykrx_enrichment'
PYKRX_DECIDER_ENRICHMENT_REQUIRED_CATEGORIES: tuple[str, ...] = (
    'sector_classification',
    'market_breadth',
    'investor_flow_market',
    'investor_flow_symbol',
    'short_selling',
    'valuation_snapshot',
)


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _normalize_fs_div(fs_div: str | None) -> str:
    return (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()


def _fallback_stage_payload(*, stage_id: str, market_date: str) -> dict[str, Any]:
    return {
        'task': stage_id,
        'label': stage_id.replace('_', ' '),
        'status': 'pending',
        'market_date': market_date,
        'current_activity': f'Pending {stage_id.replace("_", " ")}',
        'details': {},
    }


class AgentFeedbackPrerequisiteService:
    @staticmethod
    def _classify_stage_gate_status(stage_payload: dict[str, Any]) -> str:
        status = str(stage_payload.get('status') or 'pending').strip().lower()
        details = stage_payload.get('details') if isinstance(stage_payload.get('details'), dict) else {}
        explicit_gate_status = str(
            stage_payload.get('gate_status') or details.get('gate_status') or ''
        ).strip().lower()
        if str(stage_payload.get('task') or '') != PYKRX_ENRICHMENT_PREREQUISITE_STAGE_ID:
            if AgentFeedbackPrerequisiteService._has_complete_persisted_stage_coverage(stage_payload):
                return 'ready'
            if status == 'partial' and AgentFeedbackPrerequisiteService._is_nonblocking_fundamentals_partial(stage_payload):
                return 'ready'
        if explicit_gate_status in {'ready', 'blocked', 'waiting'}:
            return explicit_gate_status
        if str(stage_payload.get('task') or '') == PYKRX_ENRICHMENT_PREREQUISITE_STAGE_ID:
            category_gate_status = str(details.get('decider_category_gate_status') or '').strip().lower()
            if category_gate_status in {'ready', 'blocked', 'waiting'}:
                return category_gate_status
        failed_blocking_stages = details.get('failed_blocking_stages')
        if isinstance(failed_blocking_stages, list) and failed_blocking_stages:
            return 'blocked'
        if status == 'partial' and AgentFeedbackPrerequisiteService._is_progressable_terminal_partial(stage_payload):
            return 'ready'
        if status in PREREQUISITE_BLOCKED_STATUSES:
            return 'blocked'
        if status == 'completed':
            return 'ready'
        if status in PREREQUISITE_WAITING_STATUSES:
            return 'waiting'
        return 'waiting'

    @staticmethod
    def _is_progressable_terminal_partial(stage_payload: dict[str, Any]) -> bool:
        details = stage_payload.get('details') if isinstance(stage_payload.get('details'), dict) else {}
        blocking_stages = details.get('blocking_stages')
        failed_blocking_stages = details.get('failed_blocking_stages')
        if isinstance(blocking_stages, list) and blocking_stages:
            return False
        if isinstance(failed_blocking_stages, list) and failed_blocking_stages:
            return False

        total = AgentFeedbackPrerequisiteService._coerce_positive_int(
            stage_payload.get('total'),
            details.get('expected_rows'),
            details.get('requested_symbol_count'),
        )
        succeeded = AgentFeedbackPrerequisiteService._coerce_positive_int(
            stage_payload.get('succeeded'),
            stage_payload.get('processed'),
            details.get('rows_persisted'),
            details.get('stored_rows_present'),
        )
        if total <= 0 or succeeded <= 0:
            return False
        coverage_ratio = min(succeeded, total) / total
        if coverage_ratio < GENERIC_PARTIAL_PREREQUISITE_MIN_COVERAGE_RATIO:
            return False
        retry_cycles_requested = AgentFeedbackPrerequisiteService._coerce_positive_int(
            details.get('retry_cycles_requested'),
        )
        retry_cycles_completed = AgentFeedbackPrerequisiteService._coerce_positive_int(
            details.get('retry_cycles_completed'),
        )
        remaining_retryable_events = AgentFeedbackPrerequisiteService._coerce_optional_int(
            details.get('remaining_retryable_events'),
        )
        if retry_cycles_requested > 0:
            return retry_cycles_completed >= retry_cycles_requested or remaining_retryable_events == 0
        if remaining_retryable_events is not None:
            return remaining_retryable_events == 0
        return True

    @staticmethod
    def _has_complete_persisted_stage_coverage(stage_payload: dict[str, Any]) -> bool:
        details = stage_payload.get('details') if isinstance(stage_payload.get('details'), dict) else {}
        expected_rows = AgentFeedbackPrerequisiteService._coerce_positive_int(
            details.get('expected_rows'),
            stage_payload.get('total'),
        )
        rows_persisted = AgentFeedbackPrerequisiteService._coerce_positive_int(
            details.get('rows_persisted'),
            stage_payload.get('succeeded'),
            stage_payload.get('processed'),
        )
        return expected_rows > 0 and rows_persisted >= expected_rows

    @staticmethod
    def _is_nonblocking_fundamentals_partial(stage_payload: dict[str, Any]) -> bool:
        if str(stage_payload.get('task') or '') != 'fundamentals_daily_refresh':
            return False
        details = stage_payload.get('details') if isinstance(stage_payload.get('details'), dict) else {}
        result_summary = details.get('result_summary') if isinstance(details.get('result_summary'), dict) else {}
        partial_reasons = result_summary.get('partial_reasons')
        if not isinstance(partial_reasons, list) or set(partial_reasons) != {'piotroski_filter_followup_failed'}:
            return False
        symbols_failed = AgentFeedbackPrerequisiteService._coerce_optional_int(result_summary.get('symbols_failed')) or 0
        events_failed = AgentFeedbackPrerequisiteService._coerce_optional_int(result_summary.get('events_failed')) or 0
        remaining_retryable_events = AgentFeedbackPrerequisiteService._coerce_optional_int(
            result_summary.get('remaining_retryable_events')
        )
        if remaining_retryable_events is None:
            remaining_retryable_events = AgentFeedbackPrerequisiteService._coerce_optional_int(
                details.get('remaining_retryable_events')
            )
        return symbols_failed == 0 and events_failed == 0 and (remaining_retryable_events in (None, 0))

    @staticmethod
    def _coerce_positive_int(*values: Any) -> int:
        for value in values:
            try:
                parsed = int(value or 0)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed
        return 0

    @staticmethod
    def _coerce_optional_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_optional_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _has_pykrx_provider_error(payload: dict[str, Any]) -> bool:
        errors = payload.get('errors')
        if not isinstance(errors, list):
            return False
        for item in errors:
            message = ''
            if isinstance(item, dict):
                message = str(item.get('error') or '')
            else:
                message = str(item or '')
            normalized = message.lower()
            if 'pykrx stock client is unavailable' in normalized or 'expecting value' in normalized:
                return True
        return False

    @classmethod
    def _pykrx_category_gate_item(
        cls,
        *,
        category: str,
        payload: dict[str, Any],
        missing_category_payload: bool,
        provider_degraded: bool = False,
    ) -> dict[str, Any]:
        expected_rows = cls._coerce_optional_int(payload.get('expected_rows'))
        present_rows = cls._coerce_optional_int(payload.get('present_rows')) or 0
        coverage_ratio = cls._coerce_optional_float(payload.get('coverage_ratio'))
        if coverage_ratio is None and expected_rows and expected_rows > 0:
            coverage_ratio = round(min(present_rows, expected_rows) / expected_rows, 6)
        coverage_status = str(payload.get('coverage_status') or 'missing').strip().lower()
        missing_rows = max(expected_rows - present_rows, 0) if expected_rows is not None else None

        provider_error = cls._has_pykrx_provider_error(payload)

        if provider_degraded and present_rows <= 0 and (expected_rows is None or expected_rows <= 0):
            gate_status = 'ready'
            reason = 'pykrx_provider_unavailable_optional_enrichment_degraded'
        elif provider_error and present_rows <= 0:
            gate_status = 'ready'
            reason = 'pykrx_provider_unavailable_optional_enrichment_degraded'
        elif missing_category_payload or expected_rows is None or expected_rows <= 0:
            gate_status = 'waiting'
            reason = 'coverage_expectation_unavailable'
        elif present_rows <= 0 or coverage_status == 'missing':
            gate_status = 'blocked'
            reason = 'missing_required_pykrx_enrichment_category'
        elif coverage_status != 'complete':
            gate_status = 'ready'
            reason = 'required_pykrx_enrichment_category_near_complete'
        else:
            gate_status = 'ready'
            reason = None

        return {
            'category': category,
            'scope_type': payload.get('scope_type'),
            'gate_status': gate_status,
            'coverage_status': coverage_status,
            'expected_rows': expected_rows,
            'present_rows': present_rows,
            'missing_rows': missing_rows,
            'coverage_ratio': coverage_ratio,
            'reason': reason,
        }

    @classmethod
    def _augment_pykrx_enrichment_prerequisite_stage(cls, stage_payload: dict[str, Any]) -> dict[str, Any]:
        details = dict(stage_payload.get('details') if isinstance(stage_payload.get('details'), dict) else {})
        if (
            str(details.get('decider_category_gate_status') or '').strip().lower()
            in {'ready', 'blocked', 'waiting'}
            and isinstance(details.get('decider_category_gates'), list)
        ):
            return {
                **stage_payload,
                'details': details,
            }
        category_payloads = cls._resolve_pykrx_category_payloads(details=details)
        result_summary = details.get('result_summary') if isinstance(details.get('result_summary'), dict) else {}
        provider_degraded = str(result_summary.get('status') or stage_payload.get('status') or '').strip().lower() == 'partial' and any(
            cls._has_pykrx_provider_error(dict(payload))
            for payload in category_payloads.values()
            if isinstance(payload, dict)
        )
        details.update(
            cls.build_pykrx_enrichment_gate_details(
                category_payloads=category_payloads,
                provider_degraded=provider_degraded,
            )
        )
        return {
            **stage_payload,
            'details': details,
        }

    @classmethod
    def _resolve_pykrx_category_payloads(cls, *, details: dict[str, Any]) -> dict[str, Any]:
        categories = details.get('categories')
        if isinstance(categories, dict) and categories:
            return dict(categories)

        result_summary = details.get('result_summary')
        if isinstance(result_summary, dict):
            result_categories = result_summary.get('categories')
            if isinstance(result_categories, dict) and result_categories:
                return dict(result_categories)

        row_counts = details.get('category_row_counts')
        if not isinstance(row_counts, dict) or not row_counts:
            return {}

        payloads: dict[str, dict[str, Any]] = {}
        failed_categories = {
            str(category)
            for category in details.get('failed_categories', [])
            if str(category or '').strip()
        }
        for category in PYKRX_DECIDER_ENRICHMENT_REQUIRED_CATEGORIES:
            present_rows = cls._coerce_optional_int(row_counts.get(category)) or 0
            if present_rows <= 0:
                continue
            payloads[category] = {
                'present_rows': present_rows,
                'expected_rows': present_rows,
                'coverage_ratio': 1.0,
                'coverage_status': 'partial' if category in failed_categories else 'complete',
                'reason': 'derived_from_category_row_counts',
            }
        return payloads

    @classmethod
    def build_pykrx_enrichment_gate_details(
        cls,
        *,
        category_payloads: dict[str, Any],
        provider_degraded: bool = False,
    ) -> dict[str, Any]:
        gate_items = [
            cls._pykrx_category_gate_item(
                category=category,
                payload=(
                    dict(category_payloads.get(category))
                    if isinstance(category_payloads.get(category), dict)
                    else {}
                ),
                missing_category_payload=not isinstance(category_payloads.get(category), dict),
                provider_degraded=provider_degraded,
            )
            for category in PYKRX_DECIDER_ENRICHMENT_REQUIRED_CATEGORIES
        ]
        blocked_categories = [item for item in gate_items if item['gate_status'] == 'blocked']
        waiting_categories = [item for item in gate_items if item['gate_status'] == 'waiting']
        warning_categories = [
            item
            for item in gate_items
            if item['gate_status'] == 'ready' and item.get('coverage_status') != 'complete'
        ]
        if blocked_categories:
            category_gate_status = 'blocked'
        elif waiting_categories:
            category_gate_status = 'waiting'
        else:
            category_gate_status = 'ready'

        return {
            'decider_required_categories': list(PYKRX_DECIDER_ENRICHMENT_REQUIRED_CATEGORIES),
            'decider_category_gates': gate_items,
            'decider_blocking_categories': blocked_categories,
            'decider_waiting_categories': waiting_categories,
            'decider_warning_categories': warning_categories,
            'decider_category_gate_status': category_gate_status,
        }

    def inspect_feedback_prerequisites(
        self,
        *,
        market_date: str | date,
        fs_div: str | None = None,
        feedback_run_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from app.eod.pipeline_status import eod_pipeline_status_service

        normalized_market_date = _normalize_market_date(market_date)
        resolved_fs_div = _normalize_fs_div(fs_div)
        pipeline_status = eod_pipeline_status_service.read(
            market_date=normalized_market_date.isoformat(),
            fs_div=resolved_fs_div,
            include_feedback_context=feedback_run_context is not None,
            feedback_run_context=feedback_run_context,
            include_feedback_stage=False,
        )
        stages = pipeline_status.get('stages') if isinstance(pipeline_status.get('stages'), dict) else {}

        stage_items: list[dict[str, Any]] = []
        stage_map: dict[str, dict[str, Any]] = {}
        ready_stage_ids: list[str] = []
        waiting_stage_ids: list[str] = []
        blocked_stage_ids: list[str] = []

        for stage_id in FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER:
            raw_stage_payload = stages.get(stage_id) if isinstance(stages, dict) else None
            stage_payload = (
                dict(raw_stage_payload)
                if isinstance(raw_stage_payload, dict)
                else _fallback_stage_payload(
                    stage_id=stage_id,
                    market_date=normalized_market_date.isoformat(),
                )
            )
            stage_payload.setdefault('task', stage_id)
            if stage_id == PYKRX_ENRICHMENT_PREREQUISITE_STAGE_ID:
                stage_payload = self._augment_pykrx_enrichment_prerequisite_stage(stage_payload)
            gate_status = self._classify_stage_gate_status(stage_payload)
            item = {
                'stage': stage_id,
                'label': stage_payload.get('label') or stage_id.replace('_', ' '),
                'status': stage_payload.get('status') or 'pending',
                'gate_status': gate_status,
                'severity': stage_payload.get('severity') or (
                    stage_payload.get('details', {}).get('severity')
                    if isinstance(stage_payload.get('details'), dict)
                    else None
                ),
                'blocking_status': stage_payload.get('blocking_status') or (
                    stage_payload.get('details', {}).get('blocking_status')
                    if isinstance(stage_payload.get('details'), dict)
                    else None
                ),
                'gate_reason': (
                    stage_payload.get('details', {}).get('gate_reason')
                    if isinstance(stage_payload.get('details'), dict)
                    else None
                ),
                'current_activity': stage_payload.get('current_activity'),
                'details': stage_payload.get('details') if isinstance(stage_payload.get('details'), dict) else {},
            }
            stage_items.append(item)
            stage_map[stage_id] = item
            if gate_status == 'ready':
                ready_stage_ids.append(stage_id)
            elif gate_status == 'blocked':
                blocked_stage_ids.append(stage_id)
            else:
                waiting_stage_ids.append(stage_id)

        overall_status = 'ready'
        if blocked_stage_ids:
            overall_status = 'blocked'
        elif waiting_stage_ids:
            overall_status = 'waiting'

        return {
            'market_date': normalized_market_date.isoformat(),
            'fs_div': resolved_fs_div,
            'status': overall_status,
            'ready': overall_status == 'ready',
            'required_stage_ids': list(FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER),
            'ready_stage_ids': ready_stage_ids,
            'waiting_stage_ids': waiting_stage_ids,
            'blocked_stage_ids': blocked_stage_ids,
            'missing_prerequisites': [item for item in stage_items if item['gate_status'] != 'ready'],
            'stages': stage_map,
        }


agent_feedback_prerequisite_service = AgentFeedbackPrerequisiteService()
