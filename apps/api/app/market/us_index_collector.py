import asyncio
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from app.brokers.kis.client import KISClient
from app.brokers.kis.provider import kis_real_client
from app.db import session_scope
from app.market.us_index_prices import (
    SUPPORTED_US_INDEX_CODES, USIndexPricesUnavailable, validate_kis_us_index_prices,
)
from app.market.us_index_snapshot_store import save_us_index_snapshot
from app.market.us_session import latest_completed_us_session


def _utc_now() -> datetime:
    return datetime.now(UTC)


async def collect_us_index_inputs(
    *, as_of: datetime, index_codes: Sequence[str] = SUPPORTED_US_INDEX_CODES,
    client: KISClient | None = None, session_factory=session_scope,
    clock: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    """Manually collect a fixed US session without involving daily jobs or orders."""
    if (
        not isinstance(index_codes, Sequence) or isinstance(index_codes, str) or not index_codes
        or any(not isinstance(code, str) or code.strip().upper() not in SUPPORTED_US_INDEX_CODES
               for code in index_codes)
    ):
        raise ValueError('index_codes must select SPX, COMP and/or SOX')
    index_codes = tuple(dict.fromkeys(code.strip().upper() for code in index_codes))
    session = latest_completed_us_session(as_of=as_of)
    as_of = as_of.astimezone(UTC)
    client = client if client is not None else kis_real_client
    environment = 'mock' if client.use_mock else 'real'
    indices = []
    for code in index_codes:
        stage = 'fetch'
        try:
            payload = await client.fetch_overseas_index_daily_prices(
                index_code=code, start_date=session.previous_session_date.strftime('%Y%m%d'),
                end_date=session.session_date.strftime('%Y%m%d'),
            )
            stage = 'validate'
            collected_at = clock()
            try:
                validate_kis_us_index_prices(payload, index_code=code, session=session)
            except USIndexPricesUnavailable:
                indices.append({
                    'index_code': code, 'status': 'unavailable', 'stage': 'validate',
                    'reason': 'missing_session_prices',
                })
                continue
            stage = 'store'
            saved = await asyncio.to_thread(
                save_us_index_snapshot, payload, index_code=code, as_of=as_of,
                collected_at=collected_at, source_environment=environment,
                session_factory=session_factory,
            )
            indices.append({
                'index_code': code, 'status': 'saved', 'snapshot_id': saved.snapshot_id,
                'collected_at': saved.collected_at.isoformat(),
            })
        except Exception as exc:
            indices.append({
                'index_code': code, 'status': 'failed', 'stage': stage,
                'error_type': type(exc).__name__,
            })
    counts = {state: sum(item['status'] == state for item in indices)
              for state in ('saved', 'unavailable', 'failed')}
    status = 'completed' if not counts['failed'] else (
        'failed' if counts['failed'] == len(indices) else 'partial'
    )
    return {
        'status': status, 'data_complete': counts['saved'] == len(indices),
        'as_of': as_of.isoformat(), 'session_date': session.session_date.isoformat(),
        'previous_session_date': session.previous_session_date.isoformat(),
        'source_environment': environment,
        'counts': counts, 'indices': indices,
    }
