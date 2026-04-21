from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, desc, func, or_, select

from app.agent_context.derived_contexts import (
    DEFAULT_DERIVED_CONTEXT_HISTORY_DAYS,
    SIGNAL_PARTICIPATION_SOURCES,
    apply_participation_run_coverage,
    build_signal_participation_context as build_persisted_signal_participation_context,
)
from app.db import session_scope
from app.db_models import (
    DailyActionSummary,
    DailyPriceBar,
    DailySignalFeature,
    DailyTechnicalSnapshot,
    DailyTrendingRank,
    DartDisclosureEvent,
    DartFundamentalsSnapshot,
    EODCollectionRun,
    Symbol,
)
from app.market_data_store import load_daily_market_participation_context
from app.market_data_store import load_daily_market_participation_context_history

PARTICIPATION_CONTEXT_HISTORY_DAYS = DEFAULT_DERIVED_CONTEXT_HISTORY_DAYS


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _safe_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _limit_participation_context_leaders(context: dict[str, Any], *, limit: int) -> dict[str, Any]:
    limited = dict(context)
    limited['leaders_by_source'] = {
        source: list((limited.get('leaders_by_source') or {}).get(source) or [])[:limit]
        for source, _, _ in SIGNAL_PARTICIPATION_SOURCES
    }
    return limited


def _build_participation_context_history(
    rows: list[dict[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    return [
        {
            'market_date': row['market_date'],
            'source': row['source'],
            'captured_at': row['captured_at'],
            'participation_context': _limit_participation_context_leaders(
                dict(row.get('context_payload') or {}),
                limit=limit,
            ),
        }
        for row in rows
    ]


def _build_signal_feature_leader_rows(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    return [
        {
            'symbol': row['symbol'],
            'name_ko': row['name_ko'],
            'market': row['market'],
            'signal_count': row['signal_count'],
            'volume_power_rank': row['volume_power_rank'],
            'volume_power': row['volume_power'],
            'overtime_volume_rank': row['overtime_volume_rank'],
            'overtime_volume': row['overtime_volume'],
            'overtime_fluctuation_rank': row['overtime_fluctuation_rank'],
            'overtime_fluctuation_rate': row['overtime_fluctuation_rate'],
        }
        for row in rows[:limit]
    ]


def _build_signal_participation_context(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    run: EODCollectionRun,
) -> dict[str, Any]:
    base_context = build_persisted_signal_participation_context(rows, limit=limit)
    return apply_participation_run_coverage(
        base_context,
        symbols_total=run.symbols_total,
        symbols_succeeded=run.symbols_succeeded,
    )


@dataclass(frozen=True)
class PayloadWindows:
    technical_days: int = 252
    market_breadth_days: int = 20
    disclosures_days: int = 30
    fundamentals_years: int = 3
    top_n: int = 20


class AgentPayloadBuilder:
    def __init__(self, *, windows: PayloadWindows | None = None) -> None:
        self.windows = windows or PayloadWindows()

    def build_payloads(self, *, run: EODCollectionRun) -> dict[str, dict[str, Any]]:
        base = self._build_base_payload(run=run)
        market = self._build_market_payload(run=run)
        technical = self._build_technical_payload(run=run)
        fundamentals = self._build_fundamental_payload(run=run)
        return {
            'eod-summarizer': base | {'role': 'eod-summarizer', 'market_context': market},
            'technical-summarizer': base | {'role': 'technical-summarizer', 'technical_context': technical},
            'fundamental-summarizer': base | {'role': 'fundamental-summarizer', 'fundamental_context': fundamentals},
        }

    def _build_base_payload(self, *, run: EODCollectionRun) -> dict[str, Any]:
        return {
            'trade_date': run.trade_date.isoformat(),
            'market': run.market,
            'eod_run': {
                'id': run.id,
                'status': run.status,
                'trigger': run.trigger,
                'source': run.source,
                'symbols_total': run.symbols_total,
                'symbols_succeeded': run.symbols_succeeded,
                'symbols_failed': run.symbols_failed,
                'error_message': run.error_message,
                'started_at': run.started_at.isoformat() if run.started_at else None,
                'completed_at': run.completed_at.isoformat() if run.completed_at else None,
            },
            'data_quality': {
                'has_errors': run.status == 'completed_with_errors' or bool(run.error_message),
                'error_message': run.error_message,
                'success_ratio': (run.symbols_succeeded / run.symbols_total) if run.symbols_total else None,
            },
            'run_metadata': run.run_metadata or {},
            'windows': {
                'technical_days': self.windows.technical_days,
                'market_breadth_days': self.windows.market_breadth_days,
                'disclosures_days': self.windows.disclosures_days,
                'fundamentals_years': self.windows.fundamentals_years,
                'top_n': self.windows.top_n,
            },
        }

    def _build_market_payload(self, *, run: EODCollectionRun) -> dict[str, Any]:
        market = getattr(run, 'market', 'KRX') or 'KRX'
        with session_scope() as session:
            period_rows = [
                {
                    'period': rank.period,
                    'symbol': symbol.symbol,
                    'name_ko': symbol.name_ko,
                    'market': symbol.market,
                    'rank': rank.absolute_rank,
                    'direction': rank.trend_direction,
                    'metric_name': rank.metric_name,
                    'metric_value': _to_float(rank.metric_value),
                    'price': rank.price_payload,
                    'technical': rank.technical_payload,
                    'action_summary': rank.action_summary_payload,
                }
                for rank, symbol in session.execute(
                    select(DailyTrendingRank, Symbol)
                    .join(Symbol, Symbol.id == DailyTrendingRank.symbol_id)
                    .where(
                        DailyTrendingRank.market_date == run.trade_date,
                        DailyTrendingRank.period.in_(['1d', '5d', '20d']),
                        DailyTrendingRank.absolute_rank <= self.windows.top_n,
                    )
                    .order_by(DailyTrendingRank.period.asc(), DailyTrendingRank.absolute_rank.asc())
                ).all()
            ]

            market_signal_rows = [
                {
                    'symbol': symbol.symbol,
                    'name_ko': symbol.name_ko,
                    'market': symbol.market,
                    'signal_count': feature.signal_count,
                    'volume_power_rank': feature.volume_power_rank,
                    'volume_power': _to_float(feature.volume_power),
                    'overtime_volume_rank': feature.overtime_volume_rank,
                    'overtime_volume': feature.overtime_volume,
                    'overtime_fluctuation_rank': feature.overtime_fluctuation_rank,
                    'overtime_fluctuation_rate': _to_float(feature.overtime_fluctuation_rate),
                    'feature_payload': feature.feature_payload or {},
                }
                for feature, symbol in session.execute(
                    select(DailySignalFeature, Symbol)
                    .join(Symbol, Symbol.id == DailySignalFeature.symbol_id)
                    .where(DailySignalFeature.market_date == run.trade_date)
                    .order_by(
                        DailySignalFeature.signal_count.desc(),
                        DailySignalFeature.volume_power_rank.asc().nullslast(),
                        DailySignalFeature.overtime_volume_rank.asc().nullslast(),
                        DailySignalFeature.overtime_fluctuation_rank.asc().nullslast(),
                        Symbol.symbol.asc(),
                    )
                ).all()
            ]

            price_window_start = run.trade_date - timedelta(days=self.windows.market_breadth_days * 2)
            price_rows = [
                {
                    'trade_date': _safe_date(trade_date),
                    'symbol_count': symbol_count,
                    'total_turnover': int(total_turnover or 0),
                    'total_volume': int(total_volume or 0),
                }
                for trade_date, symbol_count, total_turnover, total_volume in session.execute(
                    select(DailyPriceBar.trade_date, func.count(), func.sum(DailyPriceBar.turnover), func.sum(DailyPriceBar.volume))
                    .where(DailyPriceBar.trade_date >= price_window_start, DailyPriceBar.trade_date <= run.trade_date)
                    .group_by(DailyPriceBar.trade_date)
                    .order_by(DailyPriceBar.trade_date.asc())
                ).all()
            ]

        grouped: dict[str, list[dict[str, Any]]] = {'1d': [], '5d': [], '20d': []}
        for row in period_rows:
            grouped.setdefault(row['period'], []).append({k: v for k, v in row.items() if k != 'period'})

        signal_rows = _build_signal_feature_leader_rows(market_signal_rows, limit=self.windows.top_n)
        stored_participation_context = load_daily_market_participation_context(
            market_date=run.trade_date,
            market=market,
        )
        stored_participation_history = load_daily_market_participation_context_history(
            market_date=run.trade_date,
            market=market,
            history_days=PARTICIPATION_CONTEXT_HISTORY_DAYS,
        )
        participation_context = apply_participation_run_coverage(
            stored_participation_context.get('context_payload') if stored_participation_context else None,
            symbols_total=run.symbols_total,
            symbols_succeeded=run.symbols_succeeded,
        )
        participation_context = _limit_participation_context_leaders(
            participation_context,
            limit=self.windows.top_n,
        )
        participation_context_history = _build_participation_context_history(
            stored_participation_history,
            limit=self.windows.top_n,
        )

        return {
            'top_trending': grouped,
            'signal_feature_leaders': signal_rows,
            'participation_context': participation_context,
            'participation_context_history': participation_context_history,
            'breadth_window': price_rows[-self.windows.market_breadth_days :],
        }

    def _build_technical_payload(self, *, run: EODCollectionRun) -> dict[str, Any]:
        window_start = run.trade_date - timedelta(days=self.windows.technical_days + 60)
        with session_scope() as session:
            top_actions = [
                {
                    'symbol': symbol.symbol,
                    'name_ko': symbol.name_ko,
                    'market': symbol.market,
                    'action_label': summary.action_label,
                    'bias_direction': summary.bias_direction,
                    'signal_score': summary.signal_score,
                    'confidence_tier': summary.confidence_tier,
                    'risk_tier': summary.risk_tier,
                    'reasons': summary.reasons,
                    'summary_payload': summary.summary_payload,
                }
                for summary, symbol in session.execute(
                    select(DailyActionSummary, Symbol)
                    .join(Symbol, Symbol.id == DailyActionSummary.symbol_id)
                    .where(DailyActionSummary.market_date == run.trade_date)
                    .order_by(DailyActionSummary.signal_score.desc(), Symbol.symbol.asc())
                    .limit(self.windows.top_n)
                ).all()
            ]

            top_technicals = [
                {
                    'symbol': symbol.symbol,
                    'name_ko': symbol.name_ko,
                    'market': symbol.market,
                    'trend_state': snapshot.trend_state,
                    'ma_alignment': snapshot.ma_alignment,
                    'breakout_20d': snapshot.breakout_20d,
                    'breakdown_20d': snapshot.breakdown_20d,
                    'return_5d': _to_float(snapshot.return_5d),
                    'return_20d': _to_float(snapshot.return_20d),
                    'volume_ratio_20d': _to_float(snapshot.volume_ratio_20d),
                    'atr_pct_14': _to_float(snapshot.atr_pct_14),
                    'signal_payload': snapshot.signal_payload,
                }
                for snapshot, symbol in session.execute(
                    select(DailyTechnicalSnapshot, Symbol)
                    .join(Symbol, Symbol.id == DailyTechnicalSnapshot.symbol_id)
                    .where(DailyTechnicalSnapshot.market_date == run.trade_date)
                    .order_by(
                        DailyTechnicalSnapshot.breakout_20d.desc(),
                        DailyTechnicalSnapshot.return_20d.desc().nullslast(),
                        Symbol.symbol.asc(),
                    )
                    .limit(self.windows.top_n)
                ).all()
            ]

            regime_rows = {
                state: count
                for state, count in session.execute(
                    select(DailyTechnicalSnapshot.trend_state, func.count())
                    .where(DailyTechnicalSnapshot.market_date == run.trade_date)
                    .group_by(DailyTechnicalSnapshot.trend_state)
                ).all()
            }

            history_rows = [
                {
                    'trade_date': _safe_date(trade_date),
                    'average_close': _to_float(avg_close),
                    'total_turnover': int(total_turnover or 0),
                }
                for trade_date, avg_close, total_turnover in session.execute(
                    select(DailyPriceBar.trade_date, func.avg(DailyPriceBar.close_price), func.sum(DailyPriceBar.turnover))
                    .where(and_(DailyPriceBar.trade_date >= window_start, DailyPriceBar.trade_date <= run.trade_date))
                    .group_by(DailyPriceBar.trade_date)
                    .order_by(DailyPriceBar.trade_date.asc())
                ).all()
            ]

        return {
            'trend_state_distribution': regime_rows,
            'top_signal_scores': top_actions,
            'top_technical_snapshots': top_technicals,
            'market_window': history_rows[-self.windows.technical_days :],
        }

    def _build_fundamental_payload(self, *, run: EODCollectionRun) -> dict[str, Any]:
        disclosure_start = run.trade_date - timedelta(days=self.windows.disclosures_days)
        minimum_business_year = max(run.trade_date.year - self.windows.fundamentals_years + 1, 2015)
        with session_scope() as session:
            disclosures = [
                {
                    'rcept_no': item.rcept_no,
                    'receipt_date': _safe_date(item.receipt_date),
                    'stock_code': item.stock_code,
                    'corp_name': item.corp_name,
                    'report_nm': item.report_nm,
                    'filing_category': item.filing_category,
                    'is_financial_report': item.is_financial_report,
                    'fundamentals_capture_status': item.fundamentals_capture_status,
                    'fundamentals_last_error': item.fundamentals_last_error,
                }
                for item in session.execute(
                    select(DartDisclosureEvent)
                    .where(
                        or_(
                            DartDisclosureEvent.receipt_date == run.trade_date,
                            and_(
                                DartDisclosureEvent.receipt_date >= disclosure_start,
                                DartDisclosureEvent.receipt_date <= run.trade_date,
                            ),
                        )
                    )
                    .order_by(desc(DartDisclosureEvent.receipt_date), desc(DartDisclosureEvent.id))
                    .limit(self.windows.top_n * 3)
                ).scalars().all()
            ]

            fundamentals = [
                {
                    'rcept_no': item.rcept_no,
                    'stock_code': item.stock_code,
                    'corp_name': item.corp_name,
                    'business_year': item.business_year,
                    'report_code': item.report_code,
                    'filing_category': item.filing_category,
                    'fs_div': item.fs_div,
                    'statement_type': item.statement_type,
                    'period_type': item.period_type,
                    'source_status': item.source_status,
                    'captured_at': item.captured_at.isoformat() if item.captured_at else None,
                }
                for item in session.execute(
                    select(DartFundamentalsSnapshot)
                    .where(DartFundamentalsSnapshot.business_year >= minimum_business_year)
                    .order_by(desc(DartFundamentalsSnapshot.business_year), desc(DartFundamentalsSnapshot.id))
                    .limit(self.windows.top_n)
                ).scalars().all()
            ]

        filing_counter = Counter(item['filing_category'] for item in disclosures)
        financial_counter = Counter(item['fundamentals_capture_status'] for item in disclosures if item['is_financial_report'])

        return {
            'disclosure_window_start': disclosure_start.isoformat(),
            'filing_category_counts': dict(filing_counter),
            'financial_capture_status_counts': dict(financial_counter),
            'recent_disclosures': disclosures[: self.windows.top_n],
            'latest_fundamentals_snapshots': fundamentals,
        }


agent_payload_builder = AgentPayloadBuilder()
