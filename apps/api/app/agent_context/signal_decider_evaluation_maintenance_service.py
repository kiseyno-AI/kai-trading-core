from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from app.agent_context.decider_evaluation_service import (
    decider_post_decision_evaluation_service,
)
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.signal_decider_calendar_review_service import (
    signal_decider_calendar_review_service,
)
from app.agent_context.signal_decider_loop_service import (
    DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    signal_decider_loop_service,
)
from app.repositories.eod_run_repository import load_latest_available_daily_price_trade_date


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str] | None:
    if not symbols:
        return None
    items: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        symbol = str(raw_symbol or '').strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        items.append(symbol)
    return items or None


def _normalize_horizons(horizons: Sequence[int] | None) -> list[int] | None:
    if not horizons:
        return None
    items: list[int] = []
    seen: set[int] = set()
    for raw_horizon in horizons:
        horizon_days = int(raw_horizon)
        if horizon_days <= 0 or horizon_days in seen:
            continue
        seen.add(horizon_days)
        items.append(horizon_days)
    return items or None


def _normalize_primary_horizons(primary_horizons: Sequence[int] | None) -> list[int] | None:
    normalized = _normalize_horizons(primary_horizons)
    return normalized if normalized is not None else None


@dataclass
class SignalDeciderEvaluationMaintenanceService:
    loop_service: Any = signal_decider_loop_service
    calendar_review_service: Any = signal_decider_calendar_review_service
    prerequisite_service: Any = agent_feedback_prerequisite_service
    post_decision_service: Any | None = None

    @staticmethod
    def _mature_gap_warning_message(not_computed_count: int) -> str | None:
        if not_computed_count <= 0:
            return None
        return (
            f'{not_computed_count} mature signal-decider horizon(s) remain '
            'not_computed after evaluation refresh.'
        )

    @staticmethod
    def _extract_coverage_counts(summary: dict[str, Any] | None) -> dict[str, int]:
        summary_payload = dict(summary or {})
        coverage_payload = dict(summary_payload.get('coverage') or {})
        return {
            'computed_count': int(
                summary_payload.get('computed_count')
                or coverage_payload.get('computed_count')
                or 0
            ),
            'pending_maturity_count': int(
                summary_payload.get('pending_maturity_count')
                or coverage_payload.get('pending_maturity_count')
                or 0
            ),
            'insufficient_price_data_count': int(
                summary_payload.get('insufficient_price_data_count')
                or coverage_payload.get('insufficient_price_data_count')
                or 0
            ),
            'not_computed_count': int(
                summary_payload.get('not_computed_count')
                or coverage_payload.get('not_computed_count')
                or 0
            ),
        }

    def _build_summary(
        self,
        *,
        status: str,
        evaluation_summary: dict[str, Any] | None,
        calendar_summary: dict[str, Any] | None,
        warning_message: str | None,
    ) -> dict[str, Any]:
        coverage_counts = self._extract_coverage_counts(evaluation_summary)
        summary: dict[str, Any] = {
            'status': status,
            **coverage_counts,
            'evaluation_refresh': dict(evaluation_summary or {}),
            'warning_count': 1 if warning_message is not None else 0,
            'warnings': [warning_message] if warning_message is not None else [],
        }
        if calendar_summary is not None:
            summary['calendar_refresh'] = dict(calendar_summary)
        return summary

    def refresh_recent_coverage(
        self,
        *,
        as_of_market_date: str | date,
        lookback_days: int = DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
        symbols: Sequence[str] | None = None,
        horizons: Sequence[int] | None = None,
        primary_horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        force_recompute: bool = False,
        refresh_calendar: bool = True,
        calendar_lookback_days: int | None = None,
        fail_on_mature_gaps: bool = False,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(as_of_market_date)
        resolved_lookback_days = max(1, int(lookback_days))
        normalized_symbols = _normalize_symbols(symbols)
        normalized_horizons = _normalize_horizons(horizons)
        normalized_primary_horizons = _normalize_primary_horizons(primary_horizons)
        prerequisites = self.prerequisite_service.inspect_feedback_prerequisites(
            market_date=normalized_market_date.isoformat(),
            symbols=normalized_symbols or None,
        )
        request_payload = {
            'as_of_market_date': normalized_market_date.isoformat(),
            'lookback_days': resolved_lookback_days,
            'symbols': normalized_symbols,
            'horizons': normalized_horizons,
            'primary_horizons': normalized_primary_horizons,
            'evaluation_version': evaluation_version,
            'force_recompute': force_recompute,
            'refresh_calendar': refresh_calendar,
            'calendar_lookback_days': calendar_lookback_days,
            'fail_on_mature_gaps': fail_on_mature_gaps,
            'trigger': trigger,
        }
        post_decision_maturity_refresh = None
        if self.post_decision_service is not None:
            post_decision_maturity_refresh = (
                self.post_decision_service.refresh_mature_invalid_evaluations(
                    as_of_market_date=normalized_market_date.isoformat(),
                    lookback_days=resolved_lookback_days,
                    symbols=normalized_symbols,
                )
            )
        if not force_recompute and not prerequisites['ready']:
            status = str(prerequisites.get('status') or 'waiting').strip().lower() or 'waiting'
            summary = self._build_summary(
                status=status,
                evaluation_summary=None,
                calendar_summary=None,
                warning_message=None,
            )
            if isinstance(post_decision_maturity_refresh, dict):
                summary['post_decision_maturity_refresh'] = dict(
                    post_decision_maturity_refresh.get('summary') or {}
                )
            return {
                'triggered': False,
                'status': status,
                'reason': 'feedback_prerequisites_not_ready',
                'market_date': normalized_market_date.isoformat(),
                'request': request_payload,
                'summary': summary,
                'prerequisites': prerequisites,
                'warnings': [],
                'post_decision_maturity_refresh': (
                    post_decision_maturity_refresh
                ),
            }

        evaluation_result = self.loop_service.compute_recent_evaluations(
            as_of_market_date=normalized_market_date,
            lookback_days=resolved_lookback_days,
            symbols=normalized_symbols,
            horizons=normalized_horizons,
            evaluation_version=evaluation_version,
            force_recompute=force_recompute,
        )
        evaluation_summary = dict(evaluation_result.get('summary') or {})
        calendar_result = None
        calendar_summary = None
        if refresh_calendar:
            resolved_calendar_lookback_days = (
                resolved_lookback_days
                if calendar_lookback_days is None
                else max(0, int(calendar_lookback_days))
            )
            if resolved_calendar_lookback_days == 0:
                calendar_result = self.calendar_review_service.refresh_reviews(
                    market_date_from=normalized_market_date,
                    market_date_to=normalized_market_date,
                    as_of_market_date=normalized_market_date,
                    lookback_days=0,
                    symbols=normalized_symbols,
                    horizons=normalized_horizons,
                    primary_horizons=normalized_primary_horizons,
                    evaluation_version=evaluation_version,
                    force_recompute=force_recompute,
                    trigger=trigger,
                    refresh_evaluations=False,
                    evaluation_refresh_summary=evaluation_summary,
                )
            else:
                calendar_result = self.calendar_review_service.refresh_recent_reviews(
                    as_of_market_date=normalized_market_date,
                    lookback_days=resolved_calendar_lookback_days,
                    symbols=normalized_symbols,
                    horizons=normalized_horizons,
                    primary_horizons=normalized_primary_horizons,
                    evaluation_version=evaluation_version,
                    force_recompute=force_recompute,
                    trigger=trigger,
                    refresh_evaluations=False,
                    evaluation_refresh_summary=evaluation_summary,
                )
            calendar_summary = dict(calendar_result.get('summary') or {})
        warning_message = self._mature_gap_warning_message(
            self._extract_coverage_counts(evaluation_summary)['not_computed_count']
        )
        status = 'completed'
        if warning_message is not None:
            status = 'failed' if fail_on_mature_gaps else 'warning'
        summary = self._build_summary(
            status=status,
            evaluation_summary=evaluation_summary,
            calendar_summary=calendar_summary,
            warning_message=warning_message,
        )
        if isinstance(post_decision_maturity_refresh, dict):
            summary['post_decision_maturity_refresh'] = dict(
                post_decision_maturity_refresh.get('summary') or {}
            )
        return {
            'triggered': True,
            'status': status,
            'market_date': normalized_market_date.isoformat(),
            'request': request_payload,
            'summary': summary,
            'prerequisites': prerequisites,
            'warnings': [warning_message] if warning_message is not None else [],
            'evaluation_refresh': evaluation_result,
            'calendar_refresh': calendar_result,
            'post_decision_maturity_refresh': (
                post_decision_maturity_refresh
            ),
        }

    def refresh_latest_stored_coverage(
        self,
        *,
        lookback_days: int = DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
        symbols: Sequence[str] | None = None,
        horizons: Sequence[int] | None = None,
        primary_horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        force_recompute: bool = False,
        refresh_calendar: bool = True,
        fail_on_mature_gaps: bool = False,
        trigger: str = 'startup',
    ) -> dict[str, Any]:
        latest_trade_date = load_latest_available_daily_price_trade_date()
        if latest_trade_date is None:
            return {
                'triggered': False,
                'status': 'skipped',
                'reason': 'no_stored_price_bars',
                'market_date': None,
                'request': {
                    'lookback_days': max(1, int(lookback_days)),
                    'symbols': _normalize_symbols(symbols),
                    'horizons': _normalize_horizons(horizons),
                    'primary_horizons': _normalize_primary_horizons(primary_horizons),
                    'evaluation_version': evaluation_version,
                    'force_recompute': force_recompute,
                    'refresh_calendar': refresh_calendar,
                    'fail_on_mature_gaps': fail_on_mature_gaps,
                    'trigger': trigger,
                },
                'summary': self._build_summary(
                    status='skipped',
                    evaluation_summary=None,
                    calendar_summary=None,
                    warning_message=None,
                ),
                'warnings': [],
            }
        return self.refresh_recent_coverage(
            as_of_market_date=latest_trade_date,
            lookback_days=lookback_days,
            symbols=symbols,
            horizons=horizons,
            primary_horizons=primary_horizons,
            evaluation_version=evaluation_version,
            force_recompute=force_recompute,
            refresh_calendar=refresh_calendar,
            fail_on_mature_gaps=fail_on_mature_gaps,
            trigger=trigger,
        )

    def repair_coverage_range(
        self,
        *,
        market_date_from: str | date,
        market_date_to: str | date,
        symbols: Sequence[str] | None = None,
        horizons: Sequence[int] | None = None,
        primary_horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        force_recompute: bool = True,
        refresh_calendar: bool = True,
        fail_on_mature_gaps: bool = True,
        trigger: str = 'repair',
    ) -> dict[str, Any]:
        normalized_from = _normalize_market_date(market_date_from)
        normalized_to = _normalize_market_date(market_date_to)
        if normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        normalized_symbols = _normalize_symbols(symbols)
        normalized_horizons = _normalize_horizons(horizons)
        normalized_primary_horizons = _normalize_primary_horizons(primary_horizons)
        evaluation_result = self.loop_service.compute_evaluations(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            symbols=normalized_symbols,
            horizons=normalized_horizons,
            evaluation_version=evaluation_version,
            force_recompute=force_recompute,
        )
        evaluation_summary = dict(evaluation_result.get('summary') or {})
        calendar_result = None
        calendar_summary = None
        if refresh_calendar:
            calendar_result = self.calendar_review_service.refresh_reviews(
                market_date_from=normalized_from,
                market_date_to=normalized_to,
                as_of_market_date=normalized_to,
                lookback_days=max(1, (normalized_to - normalized_from).days),
                symbols=normalized_symbols,
                horizons=normalized_horizons,
                primary_horizons=normalized_primary_horizons,
                evaluation_version=evaluation_version,
                force_recompute=force_recompute,
                trigger=trigger,
                refresh_evaluations=False,
                evaluation_refresh_summary=evaluation_summary,
            )
            calendar_summary = dict(calendar_result.get('summary') or {})
        warning_message = self._mature_gap_warning_message(
            self._extract_coverage_counts(evaluation_summary)['not_computed_count']
        )
        status = 'completed'
        if warning_message is not None:
            status = 'failed' if fail_on_mature_gaps else 'warning'
        return {
            'triggered': True,
            'status': status,
            'market_date': normalized_to.isoformat(),
            'request': {
                'market_date_from': normalized_from.isoformat(),
                'market_date_to': normalized_to.isoformat(),
                'symbols': normalized_symbols,
                'horizons': normalized_horizons,
                'primary_horizons': normalized_primary_horizons,
                'evaluation_version': evaluation_version,
                'force_recompute': force_recompute,
                'refresh_calendar': refresh_calendar,
                'fail_on_mature_gaps': fail_on_mature_gaps,
                'trigger': trigger,
            },
            'summary': self._build_summary(
                status=status,
                evaluation_summary=evaluation_summary,
                calendar_summary=calendar_summary,
                warning_message=warning_message,
            ),
            'warnings': [warning_message] if warning_message is not None else [],
            'evaluation_refresh': evaluation_result,
            'calendar_refresh': calendar_result,
        }


signal_decider_evaluation_maintenance_service = (
    SignalDeciderEvaluationMaintenanceService(
        post_decision_service=decider_post_decision_evaluation_service,
    )
)
