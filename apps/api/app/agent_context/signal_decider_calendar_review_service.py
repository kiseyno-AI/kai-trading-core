from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Any, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.agent_context.signal_decider_loop_service import (
    DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
    SIGNAL_DECIDER_EVALUATION_HORIZONS,
    _normalize_horizons,
    _normalize_market_date,
    _normalize_optional_text,
    _normalize_symbols,
    _resolve_calendar_month_window,
    _resolve_review_horizons,
    signal_decider_loop_service,
)
from app.db import session_scope
from app.db_models import SignalDeciderCalendarReviewItem, SignalDeciderCalendarReviewRun


def _serialize_run_row(row: SignalDeciderCalendarReviewRun) -> dict[str, Any]:
    return {
        'id': row.id,
        'market_date_from': row.market_date_from.isoformat(),
        'market_date_to': row.market_date_to.isoformat(),
        'as_of_market_date': row.as_of_market_date.isoformat() if row.as_of_market_date else None,
        'lookback_days': row.lookback_days,
        'evaluation_version': row.evaluation_version,
        'horizons': list(row.horizons_payload or []),
        'primary_horizons': list(row.primary_horizons_payload or []),
        'symbols': list(row.symbols_payload or []) if isinstance(row.symbols_payload, list) else None,
        'trigger': row.trigger,
        'status': row.status,
        'force_recompute': bool(row.force_recompute),
        'summary': dict(row.summary_payload or {}) if isinstance(row.summary_payload, dict) else None,
        'error_message': row.error_message,
        'created_at': row.created_at.isoformat() if row.created_at else None,
        'updated_at': row.updated_at.isoformat() if row.updated_at else None,
        'completed_at': row.completed_at.isoformat() if row.completed_at else None,
    }


def _serialize_item_row(row: SignalDeciderCalendarReviewItem) -> dict[str, Any]:
    return {
        'id': row.id,
        'decision_snapshot_id': row.decision_snapshot_id,
        'review_run_id': row.review_run_id,
        'market_date': row.market_date.isoformat(),
        'symbol': row.symbol,
        'policy_version': row.policy_version,
        'decision_action': row.decision_action,
        'evaluation_version': row.evaluation_version,
        'primary_horizon_days': row.primary_horizon_days,
        'review_status': row.review_status,
        'insight_code': row.insight_code,
        'insight_priority': row.insight_priority,
        'quality_verdict': row.quality_verdict,
        'quality_reason_code': row.quality_reason_code,
        'quality_reason_text': row.quality_reason_text,
        'review_conclusion': row.review_conclusion,
        'review_summary_text': row.review_summary_text,
        'path_outcome': row.path_outcome,
        'intraday_path_result': row.intraday_path_result,
        'item_payload': dict(row.item_payload or {}),
        'detail_payload': dict(row.detail_payload or {}),
        'refreshed_at': row.refreshed_at.isoformat() if row.refreshed_at else None,
        'created_at': row.created_at.isoformat() if row.created_at else None,
        'updated_at': row.updated_at.isoformat() if row.updated_at else None,
    }


class SignalDeciderCalendarReviewRepository:
    def _utcnow(self) -> datetime:
        return datetime.now(timezone.utc)

    def start_run(
        self,
        *,
        market_date_from: date,
        market_date_to: date,
        as_of_market_date: date | None,
        lookback_days: int | None,
        evaluation_version: str,
        horizons: Sequence[int],
        primary_horizons: Sequence[int],
        symbols: Sequence[str] | None,
        trigger: str,
        force_recompute: bool,
    ) -> dict[str, Any]:
        with session_scope() as session:
            row = SignalDeciderCalendarReviewRun(
                market_date_from=market_date_from,
                market_date_to=market_date_to,
                as_of_market_date=as_of_market_date,
                lookback_days=lookback_days,
                evaluation_version=evaluation_version,
                horizons_payload=list(horizons),
                primary_horizons_payload=list(primary_horizons),
                symbols_payload=list(symbols) if symbols else None,
                trigger=trigger,
                status='running',
                force_recompute=force_recompute,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return _serialize_run_row(row)

    def finish_run(
        self,
        *,
        run_id: int,
        status: str,
        summary_payload: dict[str, Any] | None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        with session_scope() as session:
            row = session.get(SignalDeciderCalendarReviewRun, int(run_id))
            if row is None:
                raise ValueError(f'calendar review run {run_id} was not found')
            row.status = status
            row.summary_payload = summary_payload
            row.error_message = error_message
            row.completed_at = self._utcnow()
            session.flush()
            session.refresh(row)
            return _serialize_run_row(row)

    def list_recent_runs(
        self,
        *,
        evaluation_version: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = select(SignalDeciderCalendarReviewRun).order_by(
                SignalDeciderCalendarReviewRun.created_at.desc(),
                SignalDeciderCalendarReviewRun.id.desc(),
            )
            if evaluation_version:
                stmt = stmt.where(SignalDeciderCalendarReviewRun.evaluation_version == evaluation_version)
            rows = session.execute(stmt.limit(max(1, limit))).scalars().all()
            return [_serialize_run_row(row) for row in rows]

    def latest_run(self, *, evaluation_version: str | None = None) -> dict[str, Any] | None:
        rows = self.list_recent_runs(evaluation_version=evaluation_version, limit=1)
        return rows[0] if rows else None

    def bulk_upsert_review_items(self, *, items: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        if not items:
            return []
        with session_scope() as session:
            stmt = insert(SignalDeciderCalendarReviewItem).values(list(items))
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                constraint='uq_sd_cal_review_items_snapshot_eval_horizon',
                set_={
                    'review_run_id': excluded.review_run_id,
                    'market_date': excluded.market_date,
                    'symbol': excluded.symbol,
                    'policy_version': excluded.policy_version,
                    'decision_action': excluded.decision_action,
                    'review_status': excluded.review_status,
                    'insight_code': excluded.insight_code,
                    'insight_priority': excluded.insight_priority,
                    'quality_verdict': excluded.quality_verdict,
                    'quality_reason_code': excluded.quality_reason_code,
                    'quality_reason_text': excluded.quality_reason_text,
                    'review_conclusion': excluded.review_conclusion,
                    'review_summary_text': excluded.review_summary_text,
                    'path_outcome': excluded.path_outcome,
                    'intraday_path_result': excluded.intraday_path_result,
                    'item_payload': excluded.item_payload,
                    'detail_payload': excluded.detail_payload,
                    'refreshed_at': excluded.refreshed_at,
                    'updated_at': excluded.updated_at,
                },
            ).returning(SignalDeciderCalendarReviewItem.id)
            ids = session.execute(stmt).scalars().all()
            rows = session.execute(
                select(SignalDeciderCalendarReviewItem)
                .where(SignalDeciderCalendarReviewItem.id.in_(ids))
                .order_by(SignalDeciderCalendarReviewItem.id.asc())
            ).scalars().all()
            by_key = {
                (row.decision_snapshot_id, row.evaluation_version, row.primary_horizon_days): _serialize_item_row(row)
                for row in rows
            }
            return [
                by_key[(item['decision_snapshot_id'], item['evaluation_version'], item['primary_horizon_days'])]
                for item in items
            ]

    def list_review_items(
        self,
        *,
        market_date: date | None = None,
        market_date_from: date | None = None,
        market_date_to: date | None = None,
        evaluation_version: str,
        primary_horizon_days: int,
        symbols: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        normalized_symbols = _normalize_symbols(symbols)
        with session_scope() as session:
            stmt = select(SignalDeciderCalendarReviewItem).where(
                SignalDeciderCalendarReviewItem.evaluation_version == evaluation_version,
                SignalDeciderCalendarReviewItem.primary_horizon_days == int(primary_horizon_days),
            )
            if market_date is not None:
                stmt = stmt.where(SignalDeciderCalendarReviewItem.market_date == market_date)
            if market_date_from is not None:
                stmt = stmt.where(SignalDeciderCalendarReviewItem.market_date >= market_date_from)
            if market_date_to is not None:
                stmt = stmt.where(SignalDeciderCalendarReviewItem.market_date <= market_date_to)
            if normalized_symbols:
                stmt = stmt.where(SignalDeciderCalendarReviewItem.symbol.in_(normalized_symbols))
            rows = session.execute(
                stmt.order_by(
                    SignalDeciderCalendarReviewItem.market_date.asc(),
                    SignalDeciderCalendarReviewItem.symbol.asc(),
                    SignalDeciderCalendarReviewItem.decision_snapshot_id.asc(),
                )
            ).scalars().all()
            return [_serialize_item_row(row) for row in rows]

    def get_review_item(
        self,
        *,
        snapshot_id: int,
        evaluation_version: str,
        primary_horizon_days: int,
    ) -> dict[str, Any] | None:
        with session_scope() as session:
            stmt = select(SignalDeciderCalendarReviewItem).where(
                SignalDeciderCalendarReviewItem.decision_snapshot_id == int(snapshot_id),
                SignalDeciderCalendarReviewItem.evaluation_version == evaluation_version,
                SignalDeciderCalendarReviewItem.primary_horizon_days == int(primary_horizon_days),
            )
            row = session.execute(stmt).scalar_one_or_none()
            return _serialize_item_row(row) if row is not None else None


class SignalDeciderCalendarReviewService:
    def __init__(
        self,
        *,
        repository: SignalDeciderCalendarReviewRepository | None = None,
        loop_service=signal_decider_loop_service,
    ) -> None:
        self.repository = repository or SignalDeciderCalendarReviewRepository()
        self.loop_service = loop_service

    def _normalize_primary_horizons(self, primary_horizons: Sequence[int] | None) -> list[int]:
        resolved = list(SIGNAL_DECIDER_EVALUATION_HORIZONS) if not primary_horizons else sorted({int(value) for value in primary_horizons})
        invalid = [value for value in resolved if value not in SIGNAL_DECIDER_EVALUATION_HORIZONS]
        if invalid:
            raise ValueError(
                f'primary_horizons must be drawn from {list(SIGNAL_DECIDER_EVALUATION_HORIZONS)}; got {invalid}'
            )
        return resolved

    def _build_read_model_descriptor(self) -> dict[str, Any]:
        descriptor = self.loop_service.build_replay_read_model_descriptor()
        descriptor['artifact_source'] = 'signal_decider_calendar_review_items'
        descriptor['artifact_refresh_process'] = 'agent-signal-decider-calendar-refresh'
        descriptor['artifact_run_endpoint'] = '/agent-context/signal-decider/calendar/maintenance'
        return descriptor

    def _compact_review_item(self, detail_item: dict[str, Any]) -> dict[str, Any]:
        return {
            key: detail_item[key]
            for key in (
                'snapshot_id',
                'feedback_id',
                'decision_run_id',
                'market_date',
                'symbol',
                'policy_version',
                'decision',
                'summary',
                'supporting_evidence',
                'decision_risks',
                'received_at',
                'created_at',
                'review',
            )
            if key in detail_item
        }

    def _build_persisted_item_row(
        self,
        *,
        run_id: int,
        evaluation_version: str,
        primary_horizon_days: int,
        compact_item: dict[str, Any],
        detail_item: dict[str, Any],
    ) -> dict[str, Any]:
        review = dict(compact_item.get('review') or {})
        path_analysis = dict(review.get('path_analysis') or {})
        return {
            'decision_snapshot_id': int(detail_item['snapshot_id']),
            'review_run_id': int(run_id),
            'market_date': date.fromisoformat(str(detail_item['market_date'])),
            'symbol': str(detail_item['symbol']),
            'policy_version': str(detail_item['policy_version']),
            'decision_action': str((detail_item.get('decision') or {}).get('action') or ''),
            'evaluation_version': evaluation_version,
            'primary_horizon_days': int(primary_horizon_days),
            'review_status': str(review.get('status') or 'pending'),
            'insight_code': str((review.get('insight') or {}).get('code') or 'evaluation_pending'),
            'insight_priority': str((review.get('insight') or {}).get('priority') or 'medium'),
            'quality_verdict': str(review.get('quality_verdict') or (review.get('insight') or {}).get('code') or ''),
            'quality_reason_code': _normalize_optional_text(
                str(review.get('quality_reason_code')) if review.get('quality_reason_code') is not None else None
            ),
            'quality_reason_text': _normalize_optional_text(
                str(review.get('quality_reason')) if review.get('quality_reason') is not None else None
            ),
            'review_conclusion': str(review.get('review_conclusion') or (review.get('narrative') or {}).get('headline') or ''),
            'review_summary_text': str(review.get('review_conclusion_text') or review.get('narrative_text') or ''),
            'path_outcome': str(review.get('path_outcome') or path_analysis.get('path_marker') or 'pending'),
            'intraday_path_result': str(
                review.get('intraday_path_result') or path_analysis.get('trigger_sequence') or 'pending'
            ),
            'item_payload': compact_item,
            'detail_payload': detail_item,
            'refreshed_at': datetime.now(timezone.utc),
        }

    def _filter_horizons_in_payload(self, payload: dict[str, Any], horizons: Sequence[int]) -> dict[str, Any]:
        horizon_set = {int(value) for value in horizons}
        filtered = deepcopy(payload)
        review = dict(filtered.get('review') or {})
        for key in ('horizons', 'outcome_horizons'):
            review[key] = [
                item
                for item in list(review.get(key) or [])
                if int((item or {}).get('horizon_days') or 0) in horizon_set
            ]
        filtered['review'] = review
        if 'outcomes' in filtered:
            filtered['outcomes'] = [
                item
                for item in list(filtered.get('outcomes') or [])
                if int((item or {}).get('horizon_days') or 0) in horizon_set
            ]
        if 'evaluations' in filtered:
            filtered['evaluations'] = [
                item
                for item in list(filtered.get('evaluations') or [])
                if int((item or {}).get('horizon_days') or 0) in horizon_set
            ]
        return filtered

    def _build_calendar_summary_payload(
        self,
        *,
        review_items: Sequence[dict[str, Any]],
        month: str | None,
        horizons: Sequence[int],
        primary_horizon_days: int,
        evaluation_version: str,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in review_items:
            grouped.setdefault(str(item['market_date']), []).append(item)
        items: list[dict[str, Any]] = []
        for market_date in sorted(grouped):
            day_items = grouped[market_date]
            day_summary = self.loop_service.summarize_review_items(
                items=day_items,
                horizons=horizons,
                primary_horizon_days=primary_horizon_days,
                evaluation_version=evaluation_version,
            )
            reviewed_count = int((day_summary.get('evaluation_coverage') or {}).get('complete_count') or 0)
            decision_count = int(day_summary.get('count') or 0)
            items.append(
                {
                    'market_date': market_date,
                    'calendar_status': self.loop_service.build_calendar_day_status(
                        decision_count=decision_count,
                        reviewed_count=reviewed_count,
                    ),
                    'decision_count': decision_count,
                    'stored_decision_count': decision_count,
                    'reviewed_decision_count': reviewed_count,
                    'pending_review_count': int((day_summary.get('evaluation_coverage') or {}).get('pending_count') or 0),
                    'symbol_count': day_summary['symbol_count'],
                    'decision_actions': day_summary['decision_actions'],
                    'review_status_counts': day_summary['review_status_counts'],
                    'evaluation_coverage': day_summary['evaluation_coverage'],
                    'scorecard': self.loop_service.build_review_scorecard(day_items),
                }
            )
        overall = self.loop_service.summarize_review_items(
            items=review_items,
            horizons=horizons,
            primary_horizon_days=primary_horizon_days,
            evaluation_version=evaluation_version,
        )
        reviewed_count = int((overall.get('evaluation_coverage') or {}).get('complete_count') or 0)
        decision_count = int(overall.get('count') or 0)
        return items, {
            'month': month,
            'day_count': len(items),
            'calendar_status': self.loop_service.build_calendar_day_status(
                decision_count=decision_count,
                reviewed_count=reviewed_count,
            ),
            'decision_count': decision_count,
            'stored_decision_count': decision_count,
            'reviewed_decision_count': reviewed_count,
            'pending_review_count': int((overall.get('evaluation_coverage') or {}).get('pending_count') or 0),
            'symbol_count': overall['symbol_count'],
            'decision_actions': overall['decision_actions'],
            'review_status_counts': overall['review_status_counts'],
            'evaluation_coverage': overall['evaluation_coverage'],
            'scorecard': self.loop_service.build_review_scorecard(review_items),
        }

    def refresh_reviews(
        self,
        *,
        market_date_from: str | date,
        market_date_to: str | date,
        symbols: Sequence[str] | None = None,
        horizons: Sequence[int] | None = None,
        primary_horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        force_recompute: bool = False,
        trigger: str = 'manual',
        as_of_market_date: str | date | None = None,
        lookback_days: int | None = None,
    ) -> dict[str, Any]:
        normalized_from = _normalize_market_date(market_date_from)
        normalized_to = _normalize_market_date(market_date_to)
        if normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        normalized_symbols = _normalize_symbols(symbols)
        normalized_horizons = _normalize_horizons(horizons)
        normalized_primary_horizons = self._normalize_primary_horizons(primary_horizons)
        resolved_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        normalized_trigger = _normalize_optional_text(trigger, default='manual') or 'manual'
        normalized_as_of_market_date = _normalize_market_date(as_of_market_date) if as_of_market_date is not None else None
        run = self.repository.start_run(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            as_of_market_date=normalized_as_of_market_date,
            lookback_days=lookback_days,
            evaluation_version=resolved_version,
            horizons=normalized_horizons,
            primary_horizons=normalized_primary_horizons,
            symbols=normalized_symbols,
            trigger=normalized_trigger,
            force_recompute=force_recompute,
        )
        run_id = int(run['id'])
        try:
            evaluation_result = self.loop_service.compute_evaluations(
                market_date_from=normalized_from,
                market_date_to=normalized_to,
                symbols=normalized_symbols,
                horizons=normalized_horizons,
                evaluation_version=resolved_version,
                force_recompute=force_recompute,
            )
            snapshot_rows = self.loop_service.repository.list_decision_snapshots(
                market_date_from=normalized_from,
                market_date_to=normalized_to,
                symbols=normalized_symbols or None,
            )
            upsert_items: list[dict[str, Any]] = []
            for primary_horizon_days in normalized_primary_horizons:
                detail_items = self.loop_service.build_review_items(
                    rows=snapshot_rows,
                    horizons=normalized_horizons,
                    primary_horizon_days=primary_horizon_days,
                    evaluation_version=resolved_version,
                    include_detail_payloads=True,
                )
                for detail_item in detail_items:
                    compact_item = self._compact_review_item(detail_item)
                    upsert_items.append(
                        self._build_persisted_item_row(
                            run_id=run_id,
                            evaluation_version=resolved_version,
                            primary_horizon_days=primary_horizon_days,
                            compact_item=compact_item,
                            detail_item=detail_item,
                        )
                    )
            persisted_items = self.repository.bulk_upsert_review_items(items=upsert_items)
            summary_payload = {
                'decision_count': len(snapshot_rows),
                'review_rows_upserted': len(persisted_items),
                'primary_horizon_count': len(normalized_primary_horizons),
                'evaluation_refresh': evaluation_result.get('summary') or {},
            }
            finished_run = self.repository.finish_run(
                run_id=run_id,
                status='completed',
                summary_payload=summary_payload,
            )
            return {
                'request': {
                    'market_date_from': normalized_from.isoformat(),
                    'market_date_to': normalized_to.isoformat(),
                    'as_of_market_date': normalized_as_of_market_date.isoformat() if normalized_as_of_market_date else None,
                    'lookback_days': lookback_days,
                    'symbols': normalized_symbols,
                    'horizons': normalized_horizons,
                    'primary_horizons': normalized_primary_horizons,
                    'evaluation_version': resolved_version,
                    'force_recompute': force_recompute,
                    'trigger': normalized_trigger,
                },
                'summary': summary_payload,
                'run': finished_run,
            }
        except Exception as exc:
            self.repository.finish_run(
                run_id=run_id,
                status='failed',
                summary_payload=None,
                error_message=str(exc),
            )
            raise

    def refresh_recent_reviews(
        self,
        *,
        as_of_market_date: str | date,
        lookback_days: int = DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
        symbols: Sequence[str] | None = None,
        horizons: Sequence[int] | None = None,
        primary_horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        force_recompute: bool = False,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        normalized_as_of_market_date = _normalize_market_date(as_of_market_date)
        resolved_lookback_days = max(1, int(lookback_days))
        market_date_from = normalized_as_of_market_date - date.resolution * resolved_lookback_days
        return self.refresh_reviews(
            market_date_from=market_date_from,
            market_date_to=normalized_as_of_market_date,
            as_of_market_date=normalized_as_of_market_date,
            lookback_days=resolved_lookback_days,
            symbols=symbols,
            horizons=horizons,
            primary_horizons=primary_horizons,
            evaluation_version=evaluation_version,
            force_recompute=force_recompute,
            trigger=trigger,
        )

    def inspect_maintenance(
        self,
        *,
        evaluation_version: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        resolved_version = _normalize_optional_text(evaluation_version)
        items = self.repository.list_recent_runs(
            evaluation_version=resolved_version,
            limit=max(1, limit),
        )
        return {
            'request': {
                'evaluation_version': resolved_version,
                'limit': max(1, limit),
            },
            'summary': {
                'count': len(items),
                'latest_status': items[0]['status'] if items else None,
                'latest_completed_at': items[0]['completed_at'] if items else None,
            },
            'items': items,
        }

    def get_review_calendar(
        self,
        *,
        month: str,
        horizons: Sequence[int] | None = None,
        primary_horizon_days: int = 5,
        evaluation_version: str | None = None,
    ) -> dict[str, Any]:
        normalized_month, market_date_from, market_date_to = _resolve_calendar_month_window(month)
        resolved_horizons, resolved_primary_horizon = _resolve_review_horizons(
            horizons,
            primary_horizon_days=primary_horizon_days,
        )
        resolved_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        stored_rows = self.repository.list_review_items(
            market_date_from=market_date_from,
            market_date_to=market_date_to,
            evaluation_version=resolved_version,
            primary_horizon_days=resolved_primary_horizon,
        )
        review_items = [
            self._filter_horizons_in_payload(dict(row.get('item_payload') or {}), resolved_horizons)
            for row in stored_rows
        ]
        items, summary = self._build_calendar_summary_payload(
            review_items=review_items,
            month=normalized_month,
            horizons=resolved_horizons,
            primary_horizon_days=resolved_primary_horizon,
            evaluation_version=resolved_version,
        )
        return {
            'request': {
                'month': normalized_month,
                'market_date_from': market_date_from.isoformat(),
                'market_date_to': market_date_to.isoformat(),
                'horizons': resolved_horizons,
                'primary_horizon_days': resolved_primary_horizon,
                'evaluation_version': resolved_version,
            },
            'read_model': self._build_read_model_descriptor(),
            'taxonomy': self.loop_service.build_replay_review_taxonomy(),
            'summary': summary,
            'items': items,
        }

    def get_review_date(
        self,
        *,
        market_date: str | date,
        horizons: Sequence[int] | None = None,
        primary_horizon_days: int = 5,
        evaluation_version: str | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        resolved_horizons, resolved_primary_horizon = _resolve_review_horizons(
            horizons,
            primary_horizon_days=primary_horizon_days,
        )
        resolved_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        stored_rows = self.repository.list_review_items(
            market_date=normalized_market_date,
            evaluation_version=resolved_version,
            primary_horizon_days=resolved_primary_horizon,
        )
        items = [
            self._filter_horizons_in_payload(dict(row.get('item_payload') or {}), resolved_horizons)
            for row in stored_rows
        ]
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'horizons': resolved_horizons,
                'primary_horizon_days': resolved_primary_horizon,
                'evaluation_version': resolved_version,
            },
            'read_model': self._build_read_model_descriptor(),
            'taxonomy': self.loop_service.build_replay_review_taxonomy(),
            'summary': self.loop_service.summarize_review_items(
                items=items,
                horizons=resolved_horizons,
                primary_horizon_days=resolved_primary_horizon,
                evaluation_version=resolved_version,
            ),
            'items': items,
        }

    def get_review_detail(
        self,
        *,
        market_date: str | date,
        snapshot_id: int,
        horizons: Sequence[int] | None = None,
        primary_horizon_days: int = 5,
        evaluation_version: str | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        resolved_horizons, resolved_primary_horizon = _resolve_review_horizons(
            horizons,
            primary_horizon_days=primary_horizon_days,
        )
        resolved_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        stored_row = self.repository.get_review_item(
            snapshot_id=int(snapshot_id),
            evaluation_version=resolved_version,
            primary_horizon_days=resolved_primary_horizon,
        )
        if stored_row is None:
            raise ValueError(f'signal-decider calendar review snapshot {int(snapshot_id)} was not found')
        if stored_row['market_date'] != normalized_market_date.isoformat():
            raise ValueError(
                f'signal-decider calendar review snapshot {int(snapshot_id)} does not belong to market date '
                f'{normalized_market_date.isoformat()}'
            )
        detail_item = self._filter_horizons_in_payload(dict(stored_row.get('detail_payload') or {}), resolved_horizons)
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'snapshot_id': int(snapshot_id),
                'horizons': resolved_horizons,
                'primary_horizon_days': resolved_primary_horizon,
                'evaluation_version': resolved_version,
            },
            'read_model': self._build_read_model_descriptor(),
            'taxonomy': self.loop_service.build_replay_review_taxonomy(),
            'summary': {
                'market_date': normalized_market_date.isoformat(),
                'symbol': detail_item['symbol'],
                'review_status': detail_item['review']['status'],
                'insight_code': detail_item['review']['insight']['code'],
                'insight_priority': detail_item['review']['narrative']['insight_priority'],
                'headline': detail_item['review']['narrative']['headline'],
                'primary_path_marker': detail_item['review']['path_analysis']['path_marker'],
            },
            'item': detail_item,
        }


signal_decider_calendar_review_service = SignalDeciderCalendarReviewService()
