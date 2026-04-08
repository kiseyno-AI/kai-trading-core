from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import session_scope
from app.db_models import (
    DartDisclosureEvent,
    DartFundamentalsCaptureRun,
    DartFundamentalsHighlight,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _capture_run_activity_at(row: DartFundamentalsCaptureRun) -> datetime:
    progress_at = (row.run_metadata or {}).get('last_progress_at')
    if isinstance(progress_at, str):
        try:
            return datetime.fromisoformat(progress_at)
        except ValueError:
            pass
    return row.started_at


def _serialize_capture_run(row: DartFundamentalsCaptureRun) -> dict[str, Any]:
    return {
        'id': row.id,
        'trigger': row.trigger,
        'status': row.status,
        'limit_requested': row.limit_requested,
        'force': row.force,
        'fs_div': row.fs_div,
        'events_seen': row.events_seen,
        'events_captured': row.events_captured,
        'events_failed': row.events_failed,
        'events_skipped': row.events_skipped,
        'error_message': row.error_message,
        'run_metadata': row.run_metadata,
        'started_at': row.started_at.isoformat() if row.started_at else None,
        'completed_at': row.completed_at.isoformat() if row.completed_at else None,
    }


class DartFundamentalsRepository:
    def create_capture_run(
        self,
        *,
        trigger: str,
        limit_requested: int | None,
        force: bool,
        fs_div: str,
        run_metadata: dict[str, Any] | None = None,
    ) -> int:
        with session_scope() as session:
            row = DartFundamentalsCaptureRun(
                trigger=trigger,
                status='running',
                limit_requested=limit_requested,
                force=force,
                fs_div=fs_div,
                run_metadata=run_metadata,
            )
            session.add(row)
            session.flush()
            return row.id

    def complete_capture_run(
        self,
        run_id: int,
        *,
        status: str,
        events_seen: int,
        events_captured: int,
        events_failed: int,
        events_skipped: int,
        error_message: str | None = None,
        run_metadata: dict[str, Any] | None = None,
    ) -> None:
        with session_scope() as session:
            row = session.get(DartFundamentalsCaptureRun, run_id)
            if row is None:
                return
            row.status = status
            row.events_seen = events_seen
            row.events_captured = events_captured
            row.events_failed = events_failed
            row.events_skipped = events_skipped
            row.error_message = error_message
            row.run_metadata = run_metadata
            row.completed_at = _utcnow()

    def update_capture_run_progress(
        self,
        run_id: int,
        *,
        events_seen: int,
        events_captured: int,
        events_failed: int,
        events_skipped: int,
        run_metadata: dict[str, Any] | None = None,
    ) -> None:
        with session_scope() as session:
            row = session.get(DartFundamentalsCaptureRun, run_id)
            if row is None:
                return
            row.events_seen = events_seen
            row.events_captured = events_captured
            row.events_failed = events_failed
            row.events_skipped = events_skipped
            row.run_metadata = run_metadata

    def fail_stale_capture_runs(
        self,
        *,
        stale_before: datetime,
        error_message: str,
    ) -> list[int]:
        with session_scope() as session:
            rows = session.execute(
                select(DartFundamentalsCaptureRun).where(DartFundamentalsCaptureRun.status == 'running')
            ).scalars().all()
            failed_run_ids: list[int] = []
            for row in rows:
                if _capture_run_activity_at(row) >= stale_before:
                    continue
                row.status = 'failed'
                row.error_message = error_message
                row.completed_at = _utcnow()
                failed_run_ids.append(row.id)
            return failed_run_ids

    def list_capture_runs(self, *, limit: int = 10) -> list[dict[str, Any]]:
        with session_scope() as session:
            rows = session.execute(
                select(DartFundamentalsCaptureRun)
                .order_by(DartFundamentalsCaptureRun.started_at.desc(), DartFundamentalsCaptureRun.id.desc())
                .limit(limit)
            ).scalars()
            return [_serialize_capture_run(row) for row in rows]

    def get_latest_capture_run(self) -> dict[str, Any] | None:
        with session_scope() as session:
            row = session.execute(
                select(DartFundamentalsCaptureRun)
                .order_by(DartFundamentalsCaptureRun.started_at.desc(), DartFundamentalsCaptureRun.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            return _serialize_capture_run(row)

    def list_capture_candidates(
        self,
        *,
        limit: int | None = 20,
        force: bool = False,
        stock_codes: list[str] | None = None,
        receipt_date_from: date | None = None,
        receipt_date_to: date | None = None,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = select(DartDisclosureEvent).where(DartDisclosureEvent.is_financial_report.is_(True))
            if not force:
                stmt = stmt.where(DartDisclosureEvent.fundamentals_capture_status.in_(('pending', 'failed')))
            if stock_codes:
                stmt = stmt.where(DartDisclosureEvent.stock_code.in_(stock_codes))
            if receipt_date_from:
                stmt = stmt.where(DartDisclosureEvent.receipt_date >= receipt_date_from)
            if receipt_date_to:
                stmt = stmt.where(DartDisclosureEvent.receipt_date <= receipt_date_to)
            rows = session.execute(
                stmt.order_by(DartDisclosureEvent.receipt_date.desc(), DartDisclosureEvent.rcept_no.desc())
                if limit is None
                else stmt.order_by(DartDisclosureEvent.receipt_date.desc(), DartDisclosureEvent.rcept_no.desc()).limit(limit)
            ).scalars()
            return [self._serialize_event(row) for row in rows]

    def list_financial_events(
        self,
        *,
        limit: int = 25,
        capture_status: str | None = None,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = select(DartDisclosureEvent).where(DartDisclosureEvent.is_financial_report.is_(True))
            if capture_status:
                stmt = stmt.where(DartDisclosureEvent.fundamentals_capture_status == capture_status)
            rows = session.execute(
                stmt.order_by(DartDisclosureEvent.receipt_date.desc(), DartDisclosureEvent.rcept_no.desc()).limit(limit)
            ).scalars()
            return [self._serialize_event(row) for row in rows]

    def mark_event_capture_failed(self, event_id: int, *, error_message: str) -> None:
        with session_scope() as session:
            row = session.get(DartDisclosureEvent, event_id)
            if row is None:
                return
            row.fundamentals_capture_status = 'failed'
            row.fundamentals_capture_attempts += 1
            row.fundamentals_last_attempt_at = _utcnow()
            row.fundamentals_last_error = error_message

    def mark_event_capture_success(self, event_id: int) -> None:
        with session_scope() as session:
            row = session.get(DartDisclosureEvent, event_id)
            if row is None:
                return
            now = _utcnow()
            row.fundamentals_capture_status = 'captured'
            row.fundamentals_capture_attempts += 1
            row.fundamentals_last_attempt_at = now
            row.fundamentals_last_success_at = now
            row.fundamentals_last_error = None

    def replace_snapshot_dataset(
        self,
        *,
        event_id: int,
        capture_run_id: int,
        snapshot_payload: dict[str, Any],
        row_payloads: list[dict[str, Any]],
        highlight_payloads: list[dict[str, Any]],
    ) -> int:
        snapshot_values = snapshot_payload.copy()
        snapshot_values['disclosure_event_id'] = event_id
        snapshot_values['capture_run_id'] = capture_run_id
        snapshot_values['updated_at'] = _utcnow()

        with session_scope() as session:
            statement = pg_insert(DartFundamentalsSnapshot).values(snapshot_values)
            snapshot_id = session.execute(
                statement.on_conflict_do_update(
                    constraint='uq_dart_fundamentals_snapshots_event_period_fs_div',
                    set_={
                        'capture_run_id': statement.excluded.capture_run_id,
                        'rcept_no': statement.excluded.rcept_no,
                        'corp_code': statement.excluded.corp_code,
                        'stock_code': statement.excluded.stock_code,
                        'corp_name': statement.excluded.corp_name,
                        'filing_category': statement.excluded.filing_category,
                        'statement_type': statement.excluded.statement_type,
                        'period_type': statement.excluded.period_type,
                        'period_end_month': statement.excluded.period_end_month,
                        'currency': statement.excluded.currency,
                        'source_status': statement.excluded.source_status,
                        'source_message': statement.excluded.source_message,
                        'request_payload': statement.excluded.request_payload,
                        'response_payload': statement.excluded.response_payload,
                        'captured_at': func.now(),
                        'updated_at': statement.excluded.updated_at,
                    },
                )
                .returning(DartFundamentalsSnapshot.id)
            ).scalar_one()

            session.execute(delete(DartFundamentalsRow).where(DartFundamentalsRow.snapshot_id == snapshot_id))
            session.execute(delete(DartFundamentalsHighlight).where(DartFundamentalsHighlight.snapshot_id == snapshot_id))

            if row_payloads:
                session.execute(
                    pg_insert(DartFundamentalsRow).values(
                        [{**row_payload, 'snapshot_id': snapshot_id} for row_payload in row_payloads]
                    )
                )
            if highlight_payloads:
                session.execute(
                    pg_insert(DartFundamentalsHighlight).values(
                        [{**payload, 'snapshot_id': snapshot_id} for payload in highlight_payloads]
                    )
                )
            return snapshot_id

    def get_capture_summary(self) -> dict[str, Any]:
        with session_scope() as session:
            capture_status_rows = session.execute(
                select(
                    DartDisclosureEvent.fundamentals_capture_status,
                    func.count(),
                )
                .select_from(DartDisclosureEvent)
                .where(DartDisclosureEvent.is_financial_report.is_(True))
                .group_by(DartDisclosureEvent.fundamentals_capture_status)
            ).all()
            snapshots_total = session.execute(select(func.count()).select_from(DartFundamentalsSnapshot)).scalar_one()
            latest_snapshot_at = session.execute(select(func.max(DartFundamentalsSnapshot.captured_at))).scalar_one()
            return {
                'financial_event_capture_status_counts': {status: count for status, count in capture_status_rows},
                'snapshots_total': snapshots_total,
                'latest_snapshot_at': latest_snapshot_at.isoformat() if latest_snapshot_at else None,
            }

    def list_stored_fundamentals(
        self,
        *,
        stock_codes: list[str],
        fs_div: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._list_stored_fundamentals(
            stock_codes=stock_codes,
            fs_div=fs_div,
            include_statement_rows=True,
        )

    def list_stored_fundamentals_summary(
        self,
        *,
        stock_codes: list[str],
        fs_div: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._list_stored_fundamentals(
            stock_codes=stock_codes,
            fs_div=fs_div,
            include_statement_rows=False,
        )

    def _list_stored_fundamentals(
        self,
        *,
        stock_codes: list[str],
        fs_div: str | None = None,
        include_statement_rows: bool,
    ) -> list[dict[str, Any]]:
        if not stock_codes:
            return []

        with session_scope() as session:
            stmt = (
                select(DartFundamentalsSnapshot, DartDisclosureEvent)
                .join(
                    DartDisclosureEvent,
                    DartDisclosureEvent.id == DartFundamentalsSnapshot.disclosure_event_id,
                )
                .where(DartFundamentalsSnapshot.stock_code.in_(stock_codes))
            )
            if fs_div:
                stmt = stmt.where(DartFundamentalsSnapshot.fs_div == fs_div)

            snapshot_rows = session.execute(
                stmt.order_by(
                    DartFundamentalsSnapshot.stock_code.asc(),
                    DartDisclosureEvent.receipt_date.desc(),
                    DartFundamentalsSnapshot.business_year.desc(),
                    DartFundamentalsSnapshot.report_code.desc(),
                    DartFundamentalsSnapshot.id.desc(),
                )
            ).all()
            if not snapshot_rows:
                return []

            snapshot_ids = [snapshot.id for snapshot, _event in snapshot_rows]
            highlight_rows = session.execute(
                select(DartFundamentalsHighlight)
                .where(DartFundamentalsHighlight.snapshot_id.in_(snapshot_ids))
                .order_by(DartFundamentalsHighlight.snapshot_id.asc(), DartFundamentalsHighlight.metric_key.asc())
            ).scalars()
            statement_row_counts = session.execute(
                select(DartFundamentalsRow.snapshot_id, func.count(DartFundamentalsRow.id))
                .where(DartFundamentalsRow.snapshot_id.in_(snapshot_ids))
                .group_by(DartFundamentalsRow.snapshot_id)
            ).all()

            statement_rows = []
            if include_statement_rows:
                statement_rows = session.execute(
                    select(DartFundamentalsRow)
                    .where(DartFundamentalsRow.snapshot_id.in_(snapshot_ids))
                    .order_by(
                        DartFundamentalsRow.snapshot_id.asc(),
                        DartFundamentalsRow.row_order.asc(),
                        DartFundamentalsRow.id.asc(),
                    )
                ).scalars()

            highlights_by_snapshot: dict[int, list[dict[str, Any]]] = defaultdict(list)
            rows_by_snapshot: dict[int, list[dict[str, Any]]] = defaultdict(list)
            row_count_by_snapshot = {snapshot_id: row_count for snapshot_id, row_count in statement_row_counts}
            for row in highlight_rows:
                highlights_by_snapshot[row.snapshot_id].append(self._serialize_highlight(row))
            for row in statement_rows:
                rows_by_snapshot[row.snapshot_id].append(self._serialize_statement_row(row))

            items: list[dict[str, Any]] = []
            for snapshot, event in snapshot_rows:
                items.append(
                    {
                        **self._serialize_snapshot(snapshot),
                        'disclosure': self._serialize_event(event),
                        'highlights': highlights_by_snapshot.get(snapshot.id, []),
                        'statement_rows': rows_by_snapshot.get(snapshot.id, []),
                        'statement_row_count': row_count_by_snapshot.get(snapshot.id, 0),
                    }
                )
            return items

    def _serialize_event(self, row: DartDisclosureEvent) -> dict[str, Any]:
        return {
            'id': row.id,
            'rcept_no': row.rcept_no,
            'corp_code': row.corp_code,
            'corp_name': row.corp_name,
            'stock_code': row.stock_code,
            'report_nm': row.report_nm,
            'filing_category': row.filing_category,
            'receipt_date': row.receipt_date.isoformat() if row.receipt_date else None,
            'fundamentals_capture_status': row.fundamentals_capture_status,
            'fundamentals_capture_attempts': row.fundamentals_capture_attempts,
            'fundamentals_last_attempt_at': row.fundamentals_last_attempt_at.isoformat() if row.fundamentals_last_attempt_at else None,
            'fundamentals_last_success_at': row.fundamentals_last_success_at.isoformat() if row.fundamentals_last_success_at else None,
            'fundamentals_last_error': row.fundamentals_last_error,
        }

    def _serialize_snapshot(self, row: DartFundamentalsSnapshot) -> dict[str, Any]:
        return {
            'id': row.id,
            'disclosure_event_id': row.disclosure_event_id,
            'capture_run_id': row.capture_run_id,
            'rcept_no': row.rcept_no,
            'corp_code': row.corp_code,
            'stock_code': row.stock_code,
            'corp_name': row.corp_name,
            'business_year': row.business_year,
            'report_code': row.report_code,
            'filing_category': row.filing_category,
            'fs_div': row.fs_div,
            'statement_type': row.statement_type,
            'period_type': row.period_type,
            'period_end_month': row.period_end_month,
            'currency': row.currency,
            'source_status': row.source_status,
            'source_message': row.source_message,
            'request_payload': row.request_payload,
            'response_payload': row.response_payload,
            'captured_at': row.captured_at.isoformat() if row.captured_at else None,
            'created_at': row.created_at.isoformat() if row.created_at else None,
            'updated_at': row.updated_at.isoformat() if row.updated_at else None,
        }

    def _serialize_highlight(self, row: DartFundamentalsHighlight) -> dict[str, Any]:
        return {
            'metric_key': row.metric_key,
            'metric_type': row.metric_type,
            'statement_type': row.statement_type,
            'statement_name': row.statement_name,
            'account_id': row.account_id,
            'account_name': row.account_name,
            'current_amount': row.current_amount,
            'current_cumulative_amount': row.current_cumulative_amount,
            'prior_amount': row.prior_amount,
            'prior_cumulative_amount': row.prior_cumulative_amount,
            'pre_prior_amount': row.pre_prior_amount,
            'currency': row.currency,
            'ratio_value': str(row.ratio_value) if row.ratio_value is not None else None,
        }

    def _serialize_statement_row(self, row: DartFundamentalsRow) -> dict[str, Any]:
        return {
            'row_key': row.row_key,
            'receipt_number': row.receipt_number,
            'report_code': row.report_code,
            'business_year': row.business_year,
            'statement_type': row.statement_type,
            'statement_name': row.statement_name,
            'account_id': row.account_id,
            'account_name': row.account_name,
            'account_detail': row.account_detail,
            'current_period_name': row.current_period_name,
            'current_amount': row.current_amount,
            'current_cumulative_amount': row.current_cumulative_amount,
            'prior_period_name': row.prior_period_name,
            'prior_amount': row.prior_amount,
            'prior_quarter_name': row.prior_quarter_name,
            'prior_quarter_amount': row.prior_quarter_amount,
            'prior_cumulative_amount': row.prior_cumulative_amount,
            'pre_prior_period_name': row.pre_prior_period_name,
            'pre_prior_amount': row.pre_prior_amount,
            'order': row.row_order,
            'currency': row.currency,
        }


dart_fundamentals_repository = DartFundamentalsRepository()
