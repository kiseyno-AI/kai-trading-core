from __future__ import annotations

from collections import defaultdict
from datetime import date

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import session_scope
from app.db_models import DartDisclosureEvent, DartDisclosureSyncRun


def _normalize_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _serialize_run(row: DartDisclosureSyncRun) -> dict:
    return {
        'id': row.id,
        'trigger': row.trigger,
        'status': row.status,
        'window_start': row.window_start.isoformat(),
        'window_end': row.window_end.isoformat(),
        'page_size': row.page_size,
        'pages_fetched': row.pages_fetched,
        'events_seen': row.events_seen,
        'events_inserted': row.events_inserted,
        'events_updated': row.events_updated,
        'financial_report_events': row.financial_report_events,
        'error_message': row.error_message,
        'run_metadata': row.run_metadata,
        'started_at': row.started_at.isoformat() if row.started_at else None,
        'completed_at': row.completed_at.isoformat() if row.completed_at else None,
    }


def _serialize_event(row: DartDisclosureEvent) -> dict:
    return {
        'id': row.id,
        'rcept_no': row.rcept_no,
        'corp_code': row.corp_code,
        'corp_name': row.corp_name,
        'stock_code': row.stock_code,
        'corp_cls': row.corp_cls,
        'report_nm': row.report_nm,
        'filing_category': row.filing_category,
        'is_financial_report': row.is_financial_report,
        'filer_name': row.filer_name,
        'receipt_date': row.receipt_date.isoformat() if row.receipt_date else None,
        'remark': row.remark,
        'first_seen_run_id': row.first_seen_run_id,
        'last_seen_run_id': row.last_seen_run_id,
        'fundamentals_capture_status': row.fundamentals_capture_status,
        'fundamentals_capture_attempts': row.fundamentals_capture_attempts,
        'fundamentals_last_attempt_at': row.fundamentals_last_attempt_at.isoformat() if row.fundamentals_last_attempt_at else None,
        'fundamentals_last_success_at': row.fundamentals_last_success_at.isoformat() if row.fundamentals_last_success_at else None,
        'fundamentals_last_error': row.fundamentals_last_error,
        'payload': row.payload,
        'created_at': row.created_at.isoformat() if row.created_at else None,
        'updated_at': row.updated_at.isoformat() if row.updated_at else None,
    }


class DartSyncRepository:
    def create_sync_run(
        self,
        *,
        window_start: date | str,
        window_end: date | str,
        trigger: str,
        page_size: int,
        run_metadata: dict | None = None,
    ) -> int:
        with session_scope() as session:
            row = DartDisclosureSyncRun(
                trigger=trigger,
                status='running',
                window_start=_normalize_date(window_start),
                window_end=_normalize_date(window_end),
                page_size=page_size,
                run_metadata=run_metadata,
            )
            session.add(row)
            session.flush()
            return row.id

    def complete_sync_run(
        self,
        run_id: int,
        *,
        status: str,
        pages_fetched: int,
        events_seen: int,
        events_inserted: int,
        events_updated: int,
        financial_report_events: int,
        error_message: str | None = None,
        run_metadata: dict | None = None,
    ) -> None:
        with session_scope() as session:
            session.execute(
                update(DartDisclosureSyncRun)
                .where(DartDisclosureSyncRun.id == run_id)
                .values(
                    status=status,
                    pages_fetched=pages_fetched,
                    events_seen=events_seen,
                    events_inserted=events_inserted,
                    events_updated=events_updated,
                    financial_report_events=financial_report_events,
                    error_message=error_message,
                    run_metadata=run_metadata,
                    completed_at=func.now(),
                )
            )

    def upsert_events(self, items: list[dict], *, run_id: int) -> dict[str, int]:
        if not items:
            return {'seen': 0, 'inserted': 0, 'updated': 0, 'financial_report_events': 0}

        event_rows = [item.copy() for item in items]
        rcept_nos = [item['rcept_no'] for item in event_rows]
        financial_count = sum(1 for item in event_rows if item.get('is_financial_report'))

        with session_scope() as session:
            existing_rcept_nos = set(
                session.execute(
                    select(DartDisclosureEvent.rcept_no).where(DartDisclosureEvent.rcept_no.in_(rcept_nos))
                ).scalars()
            )
            inserted = sum(1 for rcept_no in rcept_nos if rcept_no not in existing_rcept_nos)
            updated = len(event_rows) - inserted

            statement = pg_insert(DartDisclosureEvent).values(event_rows)
            session.execute(
                statement.on_conflict_do_update(
                    index_elements=[DartDisclosureEvent.rcept_no],
                    set_={
                        'corp_code': statement.excluded.corp_code,
                        'corp_name': statement.excluded.corp_name,
                        'stock_code': statement.excluded.stock_code,
                        'corp_cls': statement.excluded.corp_cls,
                        'report_nm': statement.excluded.report_nm,
                        'filing_category': statement.excluded.filing_category,
                        'is_financial_report': statement.excluded.is_financial_report,
                        'filer_name': statement.excluded.filer_name,
                        'receipt_date': statement.excluded.receipt_date,
                        'remark': statement.excluded.remark,
                        'payload': statement.excluded.payload,
                        'last_seen_run_id': statement.excluded.last_seen_run_id,
                        'updated_at': func.now(),
                    },
                )
            )

        return {
            'seen': len(event_rows),
            'inserted': inserted,
            'updated': updated,
            'financial_report_events': financial_count,
        }

    def get_latest_sync_run(self) -> dict | None:
        with session_scope() as session:
            row = session.execute(
                select(DartDisclosureSyncRun)
                .order_by(DartDisclosureSyncRun.started_at.desc(), DartDisclosureSyncRun.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            return _serialize_run(row)

    def list_sync_runs(self, *, limit: int = 10) -> list[dict]:
        with session_scope() as session:
            rows = session.execute(
                select(DartDisclosureSyncRun)
                .order_by(DartDisclosureSyncRun.started_at.desc(), DartDisclosureSyncRun.id.desc())
                .limit(limit)
            ).scalars()
            return [_serialize_run(row) for row in rows]

    def list_events(
        self,
        *,
        limit: int = 50,
        financial_only: bool = False,
        capture_status: str | None = None,
    ) -> list[dict]:
        with session_scope() as session:
            stmt = select(DartDisclosureEvent)
            if financial_only:
                stmt = stmt.where(DartDisclosureEvent.is_financial_report.is_(True))
            if capture_status:
                stmt = stmt.where(DartDisclosureEvent.fundamentals_capture_status == capture_status)
            stmt = stmt.order_by(DartDisclosureEvent.receipt_date.desc(), DartDisclosureEvent.rcept_no.desc()).limit(limit)
            rows = session.execute(stmt).scalars()
            return [_serialize_event(row) for row in rows]

    def list_recent_events_by_stock_codes(
        self,
        *,
        stock_codes: list[str],
        receipt_date_to: date | str | None = None,
        limit_per_symbol: int = 5,
    ) -> dict[str, list[dict]]:
        normalized_stock_codes = [code for code in dict.fromkeys(stock_codes) if code]
        if not normalized_stock_codes or limit_per_symbol <= 0:
            return {code: [] for code in normalized_stock_codes}

        cutoff_date = _normalize_date(receipt_date_to) if receipt_date_to is not None else None
        ranked_events = (
            select(
                DartDisclosureEvent.id.label('event_id'),
                func.row_number()
                .over(
                    partition_by=DartDisclosureEvent.stock_code,
                    order_by=(
                        DartDisclosureEvent.receipt_date.desc(),
                        DartDisclosureEvent.rcept_no.desc(),
                    ),
                )
                .label('symbol_rank'),
            )
            .where(DartDisclosureEvent.stock_code.in_(normalized_stock_codes))
        )
        if cutoff_date is not None:
            ranked_events = ranked_events.where(DartDisclosureEvent.receipt_date <= cutoff_date)
        ranked_events_subquery = ranked_events.subquery()

        with session_scope() as session:
            rows = session.execute(
                select(DartDisclosureEvent)
                .join(ranked_events_subquery, DartDisclosureEvent.id == ranked_events_subquery.c.event_id)
                .where(ranked_events_subquery.c.symbol_rank <= limit_per_symbol)
                .order_by(
                    DartDisclosureEvent.stock_code.asc(),
                    DartDisclosureEvent.receipt_date.desc(),
                    DartDisclosureEvent.rcept_no.desc(),
                )
            ).scalars()

            grouped: dict[str, list[dict]] = defaultdict(list)
            for row in rows:
                stock_code = row.stock_code or ''
                grouped[stock_code].append(_serialize_event(row))

        return {code: grouped.get(code, []) for code in normalized_stock_codes}

    def get_summary(self) -> dict:
        with session_scope() as session:
            total_events = session.execute(select(func.count()).select_from(DartDisclosureEvent)).scalar_one()
            financial_events = session.execute(
                select(func.count()).select_from(DartDisclosureEvent).where(DartDisclosureEvent.is_financial_report.is_(True))
            ).scalar_one()
            latest_receipt_date = session.execute(select(func.max(DartDisclosureEvent.receipt_date))).scalar_one()
            capture_status_rows = session.execute(
                select(
                    DartDisclosureEvent.fundamentals_capture_status,
                    func.count(),
                )
                .select_from(DartDisclosureEvent)
                .group_by(DartDisclosureEvent.fundamentals_capture_status)
            ).all()
            return {
                'total_events': total_events,
                'financial_report_events': financial_events,
                'latest_receipt_date': latest_receipt_date.isoformat() if latest_receipt_date else None,
                'capture_status_counts': {status: count for status, count in capture_status_rows},
            }


dart_sync_repository = DartSyncRepository()
