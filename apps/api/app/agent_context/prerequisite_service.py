from __future__ import annotations

from datetime import date
from typing import Any

from app.core.settings import settings

FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER: tuple[str, ...] = (
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
        failed_blocking_stages = details.get('failed_blocking_stages')
        if status in PREREQUISITE_BLOCKED_STATUSES:
            return 'blocked'
        if isinstance(failed_blocking_stages, list) and failed_blocking_stages:
            return 'blocked'
        if status == 'completed':
            return 'ready'
        if status in PREREQUISITE_WAITING_STATUSES:
            return 'waiting'
        return 'waiting'

    def inspect_feedback_prerequisites(
        self,
        *,
        market_date: str | date,
        fs_div: str | None = None,
    ) -> dict[str, Any]:
        from app.eod.pipeline_status import eod_pipeline_status_service

        normalized_market_date = _normalize_market_date(market_date)
        resolved_fs_div = _normalize_fs_div(fs_div)
        pipeline_status = eod_pipeline_status_service.read(
            market_date=normalized_market_date.isoformat(),
            fs_div=resolved_fs_div,
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
            gate_status = self._classify_stage_gate_status(stage_payload)
            item = {
                'stage': stage_id,
                'label': stage_payload.get('label') or stage_id.replace('_', ' '),
                'status': stage_payload.get('status') or 'pending',
                'gate_status': gate_status,
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
