from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import session_scope
from app.db_models import (
    Symbol,
    USAgentFeedback,
    USEquityPipelineRun,
    USEquityFundamentalSnapshot,
    USEquityPriceBar,
    USEquityScreeningSnapshot,
    USMarketDataCollectionRun,
)
from app.us_equities.market_data import USPriceBar
from app.us_equities.sec_fundamentals import SECFundamentalSnapshotData


US_EQUITY_PIPELINE_VERSION = 'us_equity_v1'


def claim_us_equity_pipeline_run(
    *,
    market_date: date,
    run_id: str,
    stale_after_minutes: int,
    force: bool = False,
) -> dict:
    now = datetime.now(timezone.utc)
    with session_scope() as session:
        inserted_id = session.execute(
            pg_insert(USEquityPipelineRun)
            .values(
                market_date=market_date,
                pipeline_version=US_EQUITY_PIPELINE_VERSION,
                run_id=run_id,
                status='running',
                attempt_count=1,
                started_at=now,
                heartbeat_at=now,
                stages_payload={},
            )
            .on_conflict_do_nothing(constraint='uq_us_equity_pipeline_date_version')
            .returning(USEquityPipelineRun.id)
        ).scalar_one_or_none()
        row = session.execute(
            select(USEquityPipelineRun)
            .where(
                USEquityPipelineRun.market_date == market_date,
                USEquityPipelineRun.pipeline_version == US_EQUITY_PIPELINE_VERSION,
            )
            .with_for_update()
        ).scalar_one()
        if inserted_id is not None:
            return {'claimed': True, 'status': 'running', 'run_id': run_id, 'attempt_count': 1}
        heartbeat = row.heartbeat_at
        if heartbeat.tzinfo is None:
            heartbeat = heartbeat.replace(tzinfo=timezone.utc)
        stale = heartbeat < now - timedelta(minutes=stale_after_minutes)
        if row.status == 'completed' and not force:
            return {'claimed': False, 'status': 'already_completed', 'run_id': row.run_id}
        if row.status == 'running' and not stale:
            return {'claimed': False, 'status': 'already_running', 'run_id': row.run_id}
        row.run_id = run_id
        row.status = 'running'
        row.attempt_count += 1
        row.started_at = now
        row.heartbeat_at = now
        row.finished_at = None
        row.stages_payload = {}
        row.last_error = None
        return {
            'claimed': True,
            'status': 'reclaimed' if stale else 'forced' if force else 'retry',
            'run_id': run_id,
            'attempt_count': row.attempt_count,
        }


def touch_us_equity_pipeline_run(*, market_date: date, run_id: str) -> bool:
    with session_scope() as session:
        result = session.execute(
            update(USEquityPipelineRun)
            .where(
                USEquityPipelineRun.market_date == market_date,
                USEquityPipelineRun.pipeline_version == US_EQUITY_PIPELINE_VERSION,
                USEquityPipelineRun.run_id == run_id,
                USEquityPipelineRun.status == 'running',
            )
            .values(heartbeat_at=datetime.now(timezone.utc))
        )
        return bool(result.rowcount)


def update_us_equity_pipeline_run(
    *,
    market_date: date,
    run_id: str,
    status: str,
    stages: dict,
    error: str | None = None,
) -> bool:
    now = datetime.now(timezone.utc)
    values = {
        'status': status,
        'heartbeat_at': now,
        'stages_payload': stages,
        'last_error': error,
    }
    if status in {'completed', 'failed'}:
        values['finished_at'] = now
    with session_scope() as session:
        result = session.execute(
            update(USEquityPipelineRun)
            .where(
                USEquityPipelineRun.market_date == market_date,
                USEquityPipelineRun.pipeline_version == US_EQUITY_PIPELINE_VERSION,
                USEquityPipelineRun.run_id == run_id,
                USEquityPipelineRun.status == 'running',
            )
            .values(**values)
        )
        return bool(result.rowcount)


def create_us_market_data_run(*, market_date: date, universe_count: int, source: str = 'kis') -> int:
    with session_scope() as session:
        row = USMarketDataCollectionRun(
            market_date=market_date,
            status='running',
            source=source,
            universe_count=universe_count,
            price_history_count=0,
            price_eligible_count=0,
            fundamental_eligible_count=0,
            error_count=0,
            summary_payload={},
        )
        session.add(row)
        session.flush()
        return row.id


def finish_us_market_data_run(
    run_id: int,
    *,
    status: str,
    price_history_count: int,
    price_eligible_count: int,
    fundamental_eligible_count: int = 0,
    error_count: int = 0,
    summary_payload: dict | None = None,
) -> None:
    with session_scope() as session:
        session.execute(
            update(USMarketDataCollectionRun)
            .where(USMarketDataCollectionRun.id == run_id)
            .values(
                status=status,
                finished_at=datetime.now(timezone.utc),
                price_history_count=price_history_count,
                price_eligible_count=price_eligible_count,
                fundamental_eligible_count=fundamental_eligible_count,
                error_count=error_count,
                summary_payload=summary_payload or {},
            )
        )


def load_active_us_symbols(*, limit: int | None = None) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(
                Symbol.id,
                Symbol.symbol,
                Symbol.market,
                Symbol.exchange_code,
                Symbol.regulatory_id,
                Symbol.source_payload,
            )
            .where(
                Symbol.is_active.is_(True),
                Symbol.country_code == 'US',
                Symbol.instrument_type == 'equity',
                Symbol.market.in_(['NASDAQ', 'NYSE', 'NYSE_AMERICAN']),
            )
            .order_by(Symbol.market.asc(), Symbol.symbol.asc())
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return [dict(row._mapping) for row in session.execute(stmt)]


def upsert_us_price_bars(
    *,
    symbol_id: int,
    bars: Iterable[USPriceBar],
    collection_run_id: int | None,
    source: str = 'kis',
) -> int:
    values = [
        {
            'symbol_id': symbol_id,
            'collection_run_id': collection_run_id,
            'trade_date': bar.trade_date,
            'open_price': bar.open_price,
            'high_price': bar.high_price,
            'low_price': bar.low_price,
            'close_price': bar.close_price,
            'volume': bar.volume,
            'turnover_usd': bar.turnover,
            'source': source,
        }
        for bar in bars
    ]
    if not values:
        return 0
    with session_scope() as session:
        statement = pg_insert(USEquityPriceBar).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_us_equity_price_bars_symbol_date_source',
                set_={
                    'collection_run_id': statement.excluded.collection_run_id,
                    'open_price': statement.excluded.open_price,
                    'high_price': statement.excluded.high_price,
                    'low_price': statement.excluded.low_price,
                    'close_price': statement.excluded.close_price,
                    'volume': statement.excluded.volume,
                    'turnover_usd': statement.excluded.turnover_usd,
                    'captured_at': datetime.now(timezone.utc),
                },
            )
        )
    return len(values)


def upsert_us_screening_snapshots(*, rows: Iterable[dict]) -> int:
    values = list(rows)
    if not values:
        return 0
    with session_scope() as session:
        statement = pg_insert(USEquityScreeningSnapshot).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_us_equity_screening_symbol_date_stage',
                set_={
                    'collection_run_id': statement.excluded.collection_run_id,
                    'eligible_for_agent': statement.excluded.eligible_for_agent,
                    'rank': statement.excluded.rank,
                    'composite_score': statement.excluded.composite_score,
                    'metrics_payload': statement.excluded.metrics_payload,
                    'decision_payload': statement.excluded.decision_payload,
                    'updated_at': datetime.now(timezone.utc),
                },
            )
        )
    return len(values)


def upsert_us_fundamental_snapshot(*, symbol_id: int, snapshot: SECFundamentalSnapshotData) -> int:
    values = {
        'symbol_id': symbol_id,
        'cik': snapshot.cik,
        'accession_number': snapshot.accession_number,
        'form': snapshot.form,
        'filed_date': snapshot.filed_date,
        'period_end_date': snapshot.period_end_date,
        'fiscal_year': snapshot.fiscal_year,
        'fiscal_period': snapshot.fiscal_period,
        'revenue_usd': snapshot.revenue_usd,
        'operating_cash_flow_usd': snapshot.operating_cash_flow_usd,
        'net_income_usd': snapshot.net_income_usd,
        'assets_usd': snapshot.assets_usd,
        'liabilities_usd': snapshot.liabilities_usd,
        'equity_usd': (
            snapshot.assets_usd - snapshot.liabilities_usd
            if snapshot.assets_usd is not None and snapshot.liabilities_usd is not None
            else None
        ),
        'shares_outstanding': snapshot.shares_outstanding,
        'source_payload': snapshot.source_payload,
    }
    with session_scope() as session:
        statement = pg_insert(USEquityFundamentalSnapshot).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_us_fundamentals_symbol_accession',
                set_={
                    key: getattr(statement.excluded, key)
                    for key in values
                    if key not in {'symbol_id', 'accession_number'}
                },
            )
        )
    return 1


def load_us_price_screen_candidates(*, market_date: date, limit: int = 300) -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            select(
                Symbol.id,
                Symbol.symbol,
                Symbol.market,
                Symbol.regulatory_id,
                USEquityScreeningSnapshot.collection_run_id,
                USEquityScreeningSnapshot.rank,
                USEquityScreeningSnapshot.composite_score,
                USEquityScreeningSnapshot.metrics_payload,
                USEquityScreeningSnapshot.decision_payload,
            )
            .join(Symbol, Symbol.id == USEquityScreeningSnapshot.symbol_id)
            .where(
                USEquityScreeningSnapshot.market_date == market_date,
                USEquityScreeningSnapshot.stage == 'price_screen',
                USEquityScreeningSnapshot.eligible_for_agent.is_(True),
            )
            .order_by(USEquityScreeningSnapshot.rank.asc(), Symbol.symbol.asc())
            .limit(limit)
        ).all()
        return [dict(row._mapping) for row in rows]


def load_us_agent_candidates(*, market_date: date, limit: int = 150) -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            select(
                Symbol.id,
                Symbol.symbol,
                Symbol.name_en,
                Symbol.market,
                Symbol.exchange_code,
                Symbol.regulatory_id,
                USEquityScreeningSnapshot.collection_run_id,
                USEquityScreeningSnapshot.rank,
                USEquityScreeningSnapshot.composite_score,
                USEquityScreeningSnapshot.metrics_payload,
                USEquityScreeningSnapshot.decision_payload,
            )
            .join(Symbol, Symbol.id == USEquityScreeningSnapshot.symbol_id)
            .where(
                USEquityScreeningSnapshot.market_date == market_date,
                USEquityScreeningSnapshot.stage == 'fundamental_screen',
                USEquityScreeningSnapshot.eligible_for_agent.is_(True),
            )
            .order_by(USEquityScreeningSnapshot.rank.asc(), Symbol.symbol.asc())
            .limit(limit)
        ).all()
        return [dict(row._mapping) for row in rows]


def update_us_market_data_run_fundamentals(
    run_id: int,
    *,
    fundamental_eligible_count: int,
    error_count_delta: int = 0,
) -> None:
    with session_scope() as session:
        row = session.get(USMarketDataCollectionRun, run_id)
        if row is None:
            return
        row.fundamental_eligible_count = fundamental_eligible_count
        row.error_count += error_count_delta


def upsert_us_agent_feedback(
    *,
    market_date: date,
    symbol_id: int,
    agent_name: str,
    run_id: str,
    context_payload: dict,
    response_payload: dict,
    normalized_feedback_payload: dict,
) -> int:
    received_at = datetime.now(timezone.utc)
    with session_scope() as session:
        statement = pg_insert(USAgentFeedback).values(
            market_date=market_date,
            symbol_id=symbol_id,
            agent_name=agent_name,
            run_id=run_id,
            status='completed',
            context_payload=context_payload,
            response_payload=response_payload,
            normalized_feedback_payload=normalized_feedback_payload,
            received_at=received_at,
        )
        record_id = session.execute(
            statement.on_conflict_do_update(
                constraint='uq_us_agent_feedback_date_symbol_agent',
                set_={
                    'run_id': statement.excluded.run_id,
                    'status': statement.excluded.status,
                    'context_payload': statement.excluded.context_payload,
                    'response_payload': statement.excluded.response_payload,
                    'normalized_feedback_payload': statement.excluded.normalized_feedback_payload,
                    'received_at': received_at,
                    'updated_at': received_at,
                },
            ).returning(USAgentFeedback.id)
        ).scalar_one()
        return int(record_id)


def load_us_signal_feedback(*, market_date: date, limit: int = 80, run_id: str | None = None) -> list[dict]:
    with session_scope() as session:
        statement = (
            select(
                USAgentFeedback.id.label('feedback_id'),
                USAgentFeedback.run_id,
                USAgentFeedback.normalized_feedback_payload,
                Symbol.id.label('symbol_id'),
                Symbol.symbol,
                Symbol.standard_code,
                Symbol.market,
                Symbol.exchange_code,
                USEquityScreeningSnapshot.rank,
                USEquityScreeningSnapshot.composite_score,
                USEquityScreeningSnapshot.metrics_payload,
                USEquityScreeningSnapshot.decision_payload,
            )
            .join(Symbol, Symbol.id == USAgentFeedback.symbol_id)
            .join(
                USEquityScreeningSnapshot,
                (USEquityScreeningSnapshot.symbol_id == Symbol.id)
                & (USEquityScreeningSnapshot.market_date == USAgentFeedback.market_date),
            )
            .where(
                USAgentFeedback.market_date == market_date,
                USAgentFeedback.agent_name == 'signal-decider',
                USAgentFeedback.status == 'completed',
                USEquityScreeningSnapshot.stage == 'fundamental_screen',
                USEquityScreeningSnapshot.eligible_for_agent.is_(True),
            )
            .order_by(USEquityScreeningSnapshot.rank.asc(), Symbol.symbol.asc())
        )
        if run_id is not None:
            statement = statement.where(USAgentFeedback.run_id == run_id)
        rows = session.execute(statement.limit(limit)).all()
        return [dict(row._mapping) for row in rows]


def load_us_price_bars(*, symbol_ids: Iterable[int]) -> dict[int, list[USPriceBar]]:
    ids = list(dict.fromkeys(int(value) for value in symbol_ids))
    if not ids:
        return {}
    result: dict[int, list[USPriceBar]] = {symbol_id: [] for symbol_id in ids}
    with session_scope() as session:
        rows = session.execute(
            select(USEquityPriceBar, Symbol.symbol)
            .join(Symbol, Symbol.id == USEquityPriceBar.symbol_id)
            .where(USEquityPriceBar.symbol_id.in_(ids))
            .order_by(USEquityPriceBar.symbol_id.asc(), USEquityPriceBar.trade_date.asc())
        ).all()
        for row, symbol in rows:
            result[row.symbol_id].append(
                USPriceBar(
                    symbol=symbol,
                    trade_date=row.trade_date,
                    open_price=Decimal(row.open_price),
                    high_price=Decimal(row.high_price),
                    low_price=Decimal(row.low_price),
                    close_price=Decimal(row.close_price),
                    volume=row.volume,
                    turnover=Decimal(row.turnover_usd),
                )
            )
    return result


def load_us_screening_snapshots(
    *,
    market_date: date,
    eligible_for_agent: bool | None = None,
    limit: int = 500,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(
                Symbol.symbol,
                Symbol.name_en,
                Symbol.market,
                USEquityScreeningSnapshot.stage,
                USEquityScreeningSnapshot.eligible_for_agent,
                USEquityScreeningSnapshot.rank,
                USEquityScreeningSnapshot.composite_score,
                USEquityScreeningSnapshot.metrics_payload,
                USEquityScreeningSnapshot.decision_payload,
                USEquityScreeningSnapshot.updated_at,
            )
            .join(Symbol, Symbol.id == USEquityScreeningSnapshot.symbol_id)
            .where(USEquityScreeningSnapshot.market_date == market_date)
            .order_by(
                USEquityScreeningSnapshot.eligible_for_agent.desc(),
                USEquityScreeningSnapshot.rank.asc().nulls_last(),
                Symbol.symbol.asc(),
            )
            .limit(limit)
        )
        if eligible_for_agent is not None:
            stmt = stmt.where(USEquityScreeningSnapshot.eligible_for_agent == eligible_for_agent)
        return [dict(row._mapping) for row in session.execute(stmt)]
