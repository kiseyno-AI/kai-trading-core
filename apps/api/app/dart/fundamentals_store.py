from __future__ import annotations

from collections import defaultdict
from contextlib import nullcontext
from datetime import date, datetime, timezone
from typing import Any, Sequence

from sqlalchemy import case, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import session_scope
from app.db_models import (
    DartDisclosureEvent,
    DartFundamentalsCaptureRun,
    DartFundamentalsRefreshRun,
    DartFundamentalsHighlight,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
)
from app.dart.service import classify_disclosure_event


CAPTURE_STATUS_PENDING = 'pending'
CAPTURE_STATUS_CAPTURED = 'captured'
CAPTURE_STATUS_FAILED = 'failed'
CAPTURE_STATUS_NO_DATA = 'no_data'
CAPTURE_STATUS_MAPPING_FAILED = 'mapping_failed'

RETRYABLE_CAPTURE_STATUSES = (
    CAPTURE_STATUS_PENDING,
    CAPTURE_STATUS_FAILED,
)
TERMINAL_CAPTURE_STATUSES = (
    CAPTURE_STATUS_NO_DATA,
    CAPTURE_STATUS_MAPPING_FAILED,
)
USABLE_FUNDAMENTALS_SOURCE_STATUS = '000'


def is_usable_fundamentals_source_status(source_status: str | None) -> bool:
    return source_status == USABLE_FUNDAMENTALS_SOURCE_STATUS


def eligible_fundamentals_fs_divs(requested_fs_div: str) -> tuple[str, ...]:
    return ('CFS', 'OFS') if requested_fs_div == 'CFS' else (requested_fs_div,)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_capture_cutoff(value: datetime | str) -> datetime:
    normalized = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(value.replace('Z', '+00:00'))
    )
    if normalized.tzinfo is None or normalized.utcoffset() is None:
        raise ValueError('captured_before must include a timezone offset')
    return normalized.astimezone(timezone.utc)


def _capture_run_activity_at(row: DartFundamentalsCaptureRun) -> datetime:
    progress_at = (row.run_metadata or {}).get('last_progress_at')
    if isinstance(progress_at, str):
        try:
            return datetime.fromisoformat(progress_at)
        except ValueError:
            pass
    return row.started_at


def _refresh_run_activity_at(row: DartFundamentalsRefreshRun) -> datetime:
    progress_at = (row.run_metadata or {}).get('last_progress_at')
    if isinstance(progress_at, str):
        try:
            return datetime.fromisoformat(progress_at)
        except ValueError:
            pass
    return row.started_at


def _with_stale_failure_metadata(
    run_metadata: dict[str, Any] | None,
    *,
    error_message: str,
) -> dict[str, Any]:
    updated = dict(run_metadata or {})
    updated['stale_failure_reason'] = error_message
    updated['stale_failed_at'] = _utcnow().isoformat()
    return updated


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


def _serialize_refresh_run(row: DartFundamentalsRefreshRun) -> dict[str, Any]:
    return {
        'id': row.id,
        'market_date': row.market_date.isoformat(),
        'trigger': row.trigger,
        'status': row.status,
        'fs_div': row.fs_div,
        'force': row.force,
        'symbol_limit': row.symbol_limit,
        'symbol_offset': row.symbol_offset,
        'symbols_total': row.symbols_total,
        'symbols_checked': row.symbols_checked,
        'symbols_missing': row.symbols_missing,
        'symbols_stale': row.symbols_stale,
        'symbols_fresh': row.symbols_fresh,
        'symbols_refreshed': row.symbols_refreshed,
        'symbols_failed': row.symbols_failed,
        'events_detected': row.events_detected,
        'events_captured': row.events_captured,
        'events_failed': row.events_failed,
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
            session.execute(
                update(DartFundamentalsCaptureRun)
                .where(DartFundamentalsCaptureRun.id == run_id)
                .values(
                    status=status,
                    events_seen=events_seen,
                    events_captured=events_captured,
                    events_failed=events_failed,
                    events_skipped=events_skipped,
                    error_message=error_message,
                    run_metadata=run_metadata,
                    completed_at=func.now(),
                )
            )

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
            session.execute(
                update(DartFundamentalsCaptureRun)
                .where(DartFundamentalsCaptureRun.id == run_id)
                .values(
                    events_seen=events_seen,
                    events_captured=events_captured,
                    events_failed=events_failed,
                    events_skipped=events_skipped,
                    run_metadata=run_metadata,
                )
            )

    def fail_stale_capture_runs(
        self,
        *,
        stale_before: datetime,
        error_message: str,
    ) -> list[int]:
        with session_scope() as session:
            rows = session.execute(
                select(
                    DartFundamentalsCaptureRun.id,
                    DartFundamentalsCaptureRun.started_at,
                    DartFundamentalsCaptureRun.run_metadata,
                ).where(DartFundamentalsCaptureRun.status == 'running')
            ).all()
            failed_run_ids: list[int] = []
            for run_id, started_at, run_metadata in rows:
                progress_at = None
                if isinstance(run_metadata, dict):
                    progress_at = run_metadata.get('last_progress_at')
                activity_at = started_at
                if isinstance(progress_at, str):
                    try:
                        activity_at = datetime.fromisoformat(progress_at)
                    except ValueError:
                        pass
                if activity_at >= stale_before:
                    continue
                failed_run_ids.append(run_id)

            if failed_run_ids:
                session.execute(
                    update(DartFundamentalsCaptureRun)
                    .where(DartFundamentalsCaptureRun.id.in_(failed_run_ids))
                    .values(
                        status='failed',
                        error_message=error_message,
                        completed_at=func.now(),
                    )
                )
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

    def create_refresh_run(
        self,
        *,
        market_date: date,
        trigger: str,
        fs_div: str,
        force: bool,
        symbol_limit: int | None,
        symbol_offset: int,
        symbols_total: int,
        run_metadata: dict[str, Any] | None = None,
    ) -> int:
        with session_scope() as session:
            row = DartFundamentalsRefreshRun(
                market_date=market_date,
                trigger=trigger,
                status='running',
                fs_div=fs_div,
                force=force,
                symbol_limit=symbol_limit,
                symbol_offset=symbol_offset,
                symbols_total=symbols_total,
                run_metadata=run_metadata,
            )
            session.add(row)
            session.flush()
            return row.id

    def update_refresh_run_progress(
        self,
        run_id: int,
        *,
        symbols_checked: int,
        symbols_missing: int,
        symbols_stale: int,
        symbols_fresh: int,
        symbols_refreshed: int,
        symbols_failed: int,
        events_detected: int,
        events_captured: int,
        events_failed: int,
        run_metadata: dict[str, Any] | None = None,
    ) -> None:
        with session_scope() as session:
            session.execute(
                update(DartFundamentalsRefreshRun)
                .where(DartFundamentalsRefreshRun.id == run_id)
                .values(
                    symbols_checked=symbols_checked,
                    symbols_missing=symbols_missing,
                    symbols_stale=symbols_stale,
                    symbols_fresh=symbols_fresh,
                    symbols_refreshed=symbols_refreshed,
                    symbols_failed=symbols_failed,
                    events_detected=events_detected,
                    events_captured=events_captured,
                    events_failed=events_failed,
                    run_metadata=run_metadata,
                )
            )

    def complete_refresh_run(
        self,
        run_id: int,
        *,
        status: str,
        symbols_checked: int,
        symbols_missing: int,
        symbols_stale: int,
        symbols_fresh: int,
        symbols_refreshed: int,
        symbols_failed: int,
        events_detected: int,
        events_captured: int,
        events_failed: int,
        error_message: str | None = None,
        run_metadata: dict[str, Any] | None = None,
    ) -> None:
        with session_scope() as session:
            session.execute(
                update(DartFundamentalsRefreshRun)
                .where(DartFundamentalsRefreshRun.id == run_id)
                .values(
                    status=status,
                    symbols_checked=symbols_checked,
                    symbols_missing=symbols_missing,
                    symbols_stale=symbols_stale,
                    symbols_fresh=symbols_fresh,
                    symbols_refreshed=symbols_refreshed,
                    symbols_failed=symbols_failed,
                    events_detected=events_detected,
                    events_captured=events_captured,
                    events_failed=events_failed,
                    error_message=error_message,
                    run_metadata=run_metadata,
                    completed_at=func.now(),
                )
            )

    def fail_stale_refresh_runs(
        self,
        *,
        stale_before: datetime,
        error_message: str,
    ) -> list[int]:
        with session_scope() as session:
            rows = session.execute(
                select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.status == 'running')
            ).scalars().all()
            failed_run_ids: list[int] = []
            for row in rows:
                if _refresh_run_activity_at(row) >= stale_before:
                    continue
                row.status = 'failed'
                row.error_message = error_message
                row.run_metadata = _with_stale_failure_metadata(
                    row.run_metadata,
                    error_message=error_message,
                )
                row.completed_at = _utcnow()
                failed_run_ids.append(row.id)
            return failed_run_ids

    def get_latest_refresh_run(
        self,
        *,
        market_date: date | None = None,
        fs_div: str | None = None,
        statuses: Sequence[str] | None = None,
        exclude_run_id: int | None = None,
    ) -> dict[str, Any] | None:
        with session_scope() as session:
            stmt = select(DartFundamentalsRefreshRun)
            if market_date is not None:
                stmt = stmt.where(DartFundamentalsRefreshRun.market_date == market_date)
            if fs_div is not None:
                stmt = stmt.where(DartFundamentalsRefreshRun.fs_div == fs_div)
            if statuses:
                stmt = stmt.where(DartFundamentalsRefreshRun.status.in_(statuses))
            if exclude_run_id is not None:
                stmt = stmt.where(
                    DartFundamentalsRefreshRun.id != exclude_run_id
                )
            row = session.execute(
                stmt.order_by(DartFundamentalsRefreshRun.started_at.desc(), DartFundamentalsRefreshRun.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            return _serialize_refresh_run(row)

    def list_refresh_runs(
        self,
        *,
        limit: int = 10,
        market_date: date | None = None,
        fs_div: str | None = None,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = select(DartFundamentalsRefreshRun)
            if market_date is not None:
                stmt = stmt.where(DartFundamentalsRefreshRun.market_date == market_date)
            if fs_div is not None:
                stmt = stmt.where(DartFundamentalsRefreshRun.fs_div == fs_div)
            rows = session.execute(
                stmt.order_by(DartFundamentalsRefreshRun.started_at.desc(), DartFundamentalsRefreshRun.id.desc())
                .limit(limit)
            ).scalars()
            return [_serialize_refresh_run(row) for row in rows]

    def list_capture_candidates(
        self,
        *,
        limit: int | None = 20,
        force: bool = False,
        stock_codes: list[str] | None = None,
        receipt_date_from: date | None = None,
        receipt_date_to: date | None = None,
        event_ids: list[int] | None = None,
        oldest_first: bool = False,
    ) -> list[dict[str, Any]]:
        if event_ids == []:
            return []

        with session_scope() as session:
            stmt = select(DartDisclosureEvent).where(DartDisclosureEvent.is_financial_report.is_(True))
            if not force:
                stmt = stmt.where(DartDisclosureEvent.fundamentals_capture_status.in_(RETRYABLE_CAPTURE_STATUSES))
            if stock_codes:
                stmt = stmt.where(DartDisclosureEvent.stock_code.in_(stock_codes))
            if receipt_date_from:
                stmt = stmt.where(DartDisclosureEvent.receipt_date >= receipt_date_from)
            if receipt_date_to:
                stmt = stmt.where(DartDisclosureEvent.receipt_date <= receipt_date_to)
            if event_ids is not None:
                stmt = stmt.where(DartDisclosureEvent.id.in_(event_ids))
            if oldest_first:
                ordered_stmt = stmt.order_by(
                    DartDisclosureEvent.fundamentals_capture_attempts.asc(),
                    DartDisclosureEvent.receipt_date.asc(),
                    DartDisclosureEvent.rcept_no.asc(),
                )
            else:
                ordered_stmt = stmt.order_by(
                    DartDisclosureEvent.receipt_date.desc(),
                    DartDisclosureEvent.rcept_no.desc(),
                )
            rows = session.execute(
                ordered_stmt
                if limit is None
                else ordered_stmt.limit(limit)
            ).scalars()
            return [self._serialize_event(row) for row in rows]

    def list_symbol_fundamentals_states(
        self,
        *,
        stock_codes: list[str],
        fs_div: str,
    ) -> dict[str, dict[str, Any]]:
        normalized_codes = [code for code in dict.fromkeys(stock_codes) if code]
        requested_fs_div = fs_div.strip().upper()
        if len(normalized_codes) > 500:
            merged: dict[str, dict[str, Any]] = {}
            for index in range(0, len(normalized_codes), 500):
                merged.update(
                    self.list_symbol_fundamentals_states(
                        stock_codes=normalized_codes[index : index + 500],
                        fs_div=fs_div,
                    )
                )
            return merged
        states: dict[str, dict[str, Any]] = {
            code: {
                'stock_code': code,
                'has_fundamentals': False,
                'latest_fundamentals_receipt_date': None,
                'latest_resolved_fundamentals_receipt_date': None,
                'latest_snapshot': None,
                'latest_terminal_fundamentals_event': None,
            }
            for code in normalized_codes
        }
        if not normalized_codes:
            return states

        with session_scope() as session:
            eligible_fs_divs = eligible_fundamentals_fs_divs(requested_fs_div)
            latest_snapshot_rank = func.row_number().over(
                partition_by=DartFundamentalsSnapshot.stock_code,
                order_by=(
                    DartDisclosureEvent.receipt_date.desc().nullslast(),
                    case((DartFundamentalsSnapshot.fs_div == requested_fs_div, 0), else_=1),
                    DartFundamentalsSnapshot.business_year.desc(),
                    DartFundamentalsSnapshot.report_code.desc(),
                    DartFundamentalsSnapshot.id.desc(),
                ),
            ).label('row_rank')
            latest_snapshot_subquery = (
                select(
                    DartFundamentalsSnapshot.id.label('id'),
                    DartFundamentalsSnapshot.stock_code.label('stock_code'),
                    DartFundamentalsSnapshot.business_year.label('business_year'),
                    DartFundamentalsSnapshot.report_code.label('report_code'),
                    DartFundamentalsSnapshot.fs_div.label('fs_div'),
                    DartFundamentalsSnapshot.source_status.label('source_status'),
                    DartFundamentalsSnapshot.captured_at.label('captured_at'),
                    DartDisclosureEvent.id.label('event_id'),
                    DartDisclosureEvent.rcept_no.label('rcept_no'),
                    DartDisclosureEvent.receipt_date.label('receipt_date'),
                    DartDisclosureEvent.report_nm.label('report_nm'),
                    DartDisclosureEvent.fundamentals_capture_status.label('fundamentals_capture_status'),
                    latest_snapshot_rank,
                )
                .join(DartDisclosureEvent, DartDisclosureEvent.id == DartFundamentalsSnapshot.disclosure_event_id)
                .where(
                    DartFundamentalsSnapshot.stock_code.in_(normalized_codes),
                    DartFundamentalsSnapshot.fs_div.in_(eligible_fs_divs),
                    DartFundamentalsSnapshot.source_status == USABLE_FUNDAMENTALS_SOURCE_STATUS,
                    DartDisclosureEvent.fundamentals_capture_status == CAPTURE_STATUS_CAPTURED,
                )
                .subquery()
            )
            latest_snapshot_rows = session.execute(
                select(latest_snapshot_subquery).where(latest_snapshot_subquery.c.row_rank == 1)
            ).all()
            for row in latest_snapshot_rows:
                latest = row._mapping
                stock_code = latest.get('stock_code')
                if not stock_code:
                    continue
                receipt_date = latest.get('receipt_date')
                captured_at = latest.get('captured_at')
                states[stock_code] = {
                    'stock_code': stock_code,
                    'has_fundamentals': True,
                    'latest_fundamentals_receipt_date': receipt_date.isoformat() if receipt_date else None,
                    'latest_resolved_fundamentals_receipt_date': receipt_date.isoformat() if receipt_date else None,
                    'latest_snapshot': {
                        'id': latest.get('id'),
                        'stock_code': stock_code,
                        'business_year': latest.get('business_year'),
                        'report_code': latest.get('report_code'),
                        'fs_div': latest.get('fs_div'),
                        'statement': {
                            'source_status': latest.get('source_status'),
                        },
                        'filing': {
                            'id': latest.get('event_id'),
                            'rcept_no': latest.get('rcept_no'),
                            'receipt_date': receipt_date.isoformat() if receipt_date else None,
                            'report_name': latest.get('report_nm'),
                            'capture_status': latest.get('fundamentals_capture_status'),
                            'captured_at': captured_at.isoformat() if captured_at else None,
                        },
                        'disclosure': {
                            'id': latest.get('event_id'),
                            'rcept_no': latest.get('rcept_no'),
                            'receipt_date': receipt_date.isoformat() if receipt_date else None,
                            'report_nm': latest.get('report_nm'),
                            'fundamentals_capture_status': latest.get('fundamentals_capture_status'),
                        },
                    },
                    'latest_terminal_fundamentals_event': None,
                }

            latest_terminal_rank = func.row_number().over(
                partition_by=DartDisclosureEvent.stock_code,
                order_by=(
                    DartDisclosureEvent.receipt_date.desc(),
                    DartDisclosureEvent.id.desc(),
                ),
            ).label('row_rank')
            latest_terminal_subquery = (
                select(
                    DartDisclosureEvent.id.label('id'),
                    DartDisclosureEvent.rcept_no.label('rcept_no'),
                    DartDisclosureEvent.corp_code.label('corp_code'),
                    DartDisclosureEvent.corp_name.label('corp_name'),
                    DartDisclosureEvent.stock_code.label('stock_code'),
                    DartDisclosureEvent.corp_cls.label('corp_cls'),
                    DartDisclosureEvent.report_nm.label('report_nm'),
                    DartDisclosureEvent.filing_category.label('filing_category'),
                    DartDisclosureEvent.is_financial_report.label('is_financial_report'),
                    DartDisclosureEvent.filer_name.label('filer_name'),
                    DartDisclosureEvent.receipt_date.label('receipt_date'),
                    DartDisclosureEvent.remark.label('remark'),
                    DartDisclosureEvent.first_seen_run_id.label('first_seen_run_id'),
                    DartDisclosureEvent.last_seen_run_id.label('last_seen_run_id'),
                    DartDisclosureEvent.fundamentals_capture_status.label('fundamentals_capture_status'),
                    DartDisclosureEvent.fundamentals_capture_attempts.label('fundamentals_capture_attempts'),
                    DartDisclosureEvent.fundamentals_last_attempt_at.label('fundamentals_last_attempt_at'),
                    DartDisclosureEvent.fundamentals_last_success_at.label('fundamentals_last_success_at'),
                    DartDisclosureEvent.fundamentals_last_error.label('fundamentals_last_error'),
                    DartDisclosureEvent.created_at.label('created_at'),
                    DartDisclosureEvent.updated_at.label('updated_at'),
                    latest_terminal_rank,
                )
                .where(
                    DartDisclosureEvent.stock_code.in_(normalized_codes),
                    DartDisclosureEvent.is_financial_report.is_(True),
                    DartDisclosureEvent.receipt_date.is_not(None),
                    DartDisclosureEvent.fundamentals_capture_status.in_(TERMINAL_CAPTURE_STATUSES),
                )
                .subquery()
            )
            latest_terminal_rows = session.execute(
                select(latest_terminal_subquery).where(latest_terminal_subquery.c.row_rank == 1)
            ).all()
            for row in latest_terminal_rows:
                event = row._mapping
                stock_code = event.get('stock_code')
                if not stock_code:
                    continue
                state = states[stock_code]
                terminal_receipt_date = event.get('receipt_date').isoformat() if event.get('receipt_date') else None
                state['latest_terminal_fundamentals_event'] = {
                    key: (
                        value.isoformat()
                        if isinstance(value, (date, datetime))
                        else value
                    )
                    for key, value in event.items()
                    if key != 'row_rank'
                }
                current_resolved_date = state.get('latest_resolved_fundamentals_receipt_date')
                if terminal_receipt_date and (
                    current_resolved_date is None or terminal_receipt_date > current_resolved_date
                ):
                    state['latest_resolved_fundamentals_receipt_date'] = terminal_receipt_date
        return states

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
            row.fundamentals_capture_status = CAPTURE_STATUS_FAILED
            row.fundamentals_capture_attempts += 1
            row.fundamentals_last_attempt_at = _utcnow()
            row.fundamentals_last_error = error_message

    def mark_event_capture_terminal(self, event_id: int, *, capture_status: str, error_message: str) -> None:
        if capture_status not in TERMINAL_CAPTURE_STATUSES:
            raise ValueError(f'Unsupported terminal capture status: {capture_status}')

        with session_scope() as session:
            row = session.get(DartDisclosureEvent, event_id)
            if row is None:
                return
            row.fundamentals_capture_status = capture_status
            row.fundamentals_capture_attempts += 1
            row.fundamentals_last_attempt_at = _utcnow()
            row.fundamentals_last_error = error_message

    def mark_event_capture_success(self, event_id: int) -> None:
        with session_scope() as session:
            row = session.get(DartDisclosureEvent, event_id)
            if row is None:
                return
            now = _utcnow()
            row.fundamentals_capture_status = CAPTURE_STATUS_CAPTURED
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
                normalized_highlights = _normalize_highlight_payloads(highlight_payloads)
                session.execute(
                    pg_insert(DartFundamentalsHighlight).values(
                        [{**payload, 'snapshot_id': snapshot_id} for payload in normalized_highlights]
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
                'retryable_capture_statuses': list(RETRYABLE_CAPTURE_STATUSES),
                'terminal_capture_statuses': list(TERMINAL_CAPTURE_STATUSES),
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
            receipt_date_to=None,
            include_statement_rows=True,
        )

    def list_stored_fundamentals_summary(
        self,
        *,
        stock_codes: list[str],
        fs_div: str | None = None,
        captured_before: datetime | str | None = None,
        receipt_date_to: date | None = None,
        session: Any | None = None,
    ) -> list[dict[str, Any]]:
        return self._list_stored_fundamentals(
            stock_codes=stock_codes,
            fs_div=fs_div,
            captured_before=captured_before,
            receipt_date_to=receipt_date_to,
            include_statement_rows=False,
            session=session,
        )

    def _list_stored_fundamentals(
        self,
        *,
        stock_codes: list[str],
        fs_div: str | None = None,
        captured_before: datetime | str | None = None,
        receipt_date_to: date | None,
        include_statement_rows: bool,
        session: Any | None = None,
    ) -> list[dict[str, Any]]:
        if not stock_codes:
            return []

        session_context = (
            nullcontext(session)
            if session is not None
            else session_scope()
        )
        with session_context as active_session:
            requested_fs_div = (fs_div or 'CFS').strip().upper()
            eligible_fs_divs = eligible_fundamentals_fs_divs(requested_fs_div)
            stmt = (
                select(DartFundamentalsSnapshot, DartDisclosureEvent)
                .join(
                    DartDisclosureEvent,
                    DartDisclosureEvent.id == DartFundamentalsSnapshot.disclosure_event_id,
                )
                .where(
                    DartFundamentalsSnapshot.stock_code.in_(stock_codes),
                    DartFundamentalsSnapshot.fs_div.in_(eligible_fs_divs),
                    DartFundamentalsSnapshot.source_status == USABLE_FUNDAMENTALS_SOURCE_STATUS,
                    DartDisclosureEvent.fundamentals_capture_status == CAPTURE_STATUS_CAPTURED,
                )
            )
            if captured_before is not None:
                stmt = stmt.where(
                    DartFundamentalsSnapshot.captured_at
                    < _normalize_capture_cutoff(captured_before)
                )
            if receipt_date_to is not None:
                stmt = stmt.where(
                    or_(
                        DartDisclosureEvent.receipt_date.is_(None),
                        DartDisclosureEvent.receipt_date <= receipt_date_to,
                    )
                )

            snapshot_rows = active_session.execute(
                stmt.order_by(
                    DartFundamentalsSnapshot.stock_code.asc(),
                    DartDisclosureEvent.receipt_date.desc(),
                    DartDisclosureEvent.rcept_no.desc(),
                    case(
                        (DartFundamentalsSnapshot.fs_div == requested_fs_div, 0),
                        else_=1,
                    ),
                    DartFundamentalsSnapshot.business_year.desc(),
                    DartFundamentalsSnapshot.report_code.desc(),
                    DartFundamentalsSnapshot.id.desc(),
                )
            ).all()
            if not snapshot_rows:
                return []

            selected_snapshot_rows = []
            selected_periods: set[tuple[str | None, int, str]] = set()
            for snapshot, event in snapshot_rows:
                period_key = (
                    snapshot.stock_code,
                    snapshot.business_year,
                    snapshot.report_code,
                )
                if period_key in selected_periods:
                    continue
                selected_periods.add(period_key)
                selected_snapshot_rows.append((snapshot, event))
            snapshot_rows = selected_snapshot_rows

            snapshot_ids = [snapshot.id for snapshot, _event in snapshot_rows]
            detail_snapshot_ids = snapshot_ids
            if not include_statement_rows:
                # Agent-context summaries retain every canonical period for
                # history metadata, but only the newest period per symbol is
                # expanded into highlights and statement counts downstream.
                # Limiting detail reads here avoids random I/O across the full
                # multi-year DART history during durable-input revalidation.
                detail_snapshot_ids = []
                detail_loaded_symbols: set[str | None] = set()
                for snapshot, _event in snapshot_rows:
                    if snapshot.stock_code in detail_loaded_symbols:
                        continue
                    detail_loaded_symbols.add(snapshot.stock_code)
                    detail_snapshot_ids.append(snapshot.id)
            highlight_rows = active_session.execute(
                select(DartFundamentalsHighlight)
                .where(
                    DartFundamentalsHighlight.snapshot_id.in_(
                        detail_snapshot_ids
                    )
                )
                .order_by(DartFundamentalsHighlight.snapshot_id.asc(), DartFundamentalsHighlight.metric_key.asc())
            ).scalars()
            statement_row_counts = active_session.execute(
                select(DartFundamentalsRow.snapshot_id, func.count())
                .where(DartFundamentalsRow.snapshot_id.in_(detail_snapshot_ids))
                .group_by(DartFundamentalsRow.snapshot_id)
            ).all()

            statement_rows = []
            if include_statement_rows:
                statement_rows = active_session.execute(
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
        disclosure_flags = classify_disclosure_event(
            report_name=row.report_nm,
            remark=row.remark,
            filing_category=row.filing_category,
            is_financial_report=True,
        )
        return {
            'id': row.id,
            'rcept_no': row.rcept_no,
            'corp_code': row.corp_code,
            'corp_name': row.corp_name,
            'stock_code': row.stock_code,
            'report_nm': row.report_nm,
            'filing_category': row.filing_category,
            'receipt_date': row.receipt_date.isoformat() if row.receipt_date else None,
            'remark': row.remark,
            'fundamentals_capture_status': row.fundamentals_capture_status,
            'fundamentals_capture_attempts': row.fundamentals_capture_attempts,
            'fundamentals_last_attempt_at': row.fundamentals_last_attempt_at.isoformat() if row.fundamentals_last_attempt_at else None,
            'fundamentals_last_success_at': row.fundamentals_last_success_at.isoformat() if row.fundamentals_last_success_at else None,
            'fundamentals_last_error': row.fundamentals_last_error,
            'is_correction': disclosure_flags['is_correction'],
            'is_ownership_related': disclosure_flags['is_ownership_related'],
            'is_large_holder_related': disclosure_flags['is_large_holder_related'],
            'is_audit_related': disclosure_flags['is_audit_related'],
            'disclosure_tags': disclosure_flags['disclosure_tags'],
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


def _normalize_highlight_payloads(highlight_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_by_key: dict[str, dict[str, Any]] = {}
    ordered_keys: list[str] = []
    for payload in highlight_payloads:
        metric_key = str(payload.get('metric_key') or '').strip()
        if not metric_key:
            continue
        existing = normalized_by_key.get(metric_key)
        if existing is None:
            normalized_by_key[metric_key] = payload.copy()
            ordered_keys.append(metric_key)
            continue
        normalized_by_key[metric_key] = _merge_highlight_payload(existing, payload)
    return [normalized_by_key[metric_key] for metric_key in ordered_keys]


def _merge_highlight_payload(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = existing.copy()
    for key, value in incoming.items():
        if key == 'metric_key':
            continue
        if _is_meaningful_highlight_value(merged.get(key)):
            continue
        if _is_meaningful_highlight_value(value):
            merged[key] = value
    return merged


def _is_meaningful_highlight_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ''
    return True


dart_fundamentals_repository = DartFundamentalsRepository()
