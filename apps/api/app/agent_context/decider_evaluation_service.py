from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
from app.agent_context.signal_decider_snapshot_validity import (
    is_decision_snapshot_valid_for_source_date,
)
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    DeciderPostDecisionEvaluation,
    SignalDeciderDecisionSnapshot,
)
from app.market_data_store import load_daily_bars_for_symbol_range

DEFAULT_DECIDER_EVALUATION_WINDOW_DAYS = 5
DECIDER_EVALUATION_ENGINE_VERSION = 'decider_post_decision_eval_v1'
SUPPORTED_DECIDER_AGENT_NAMES: tuple[str, ...] = DECISION_SUPPORT_AGENT_NAMES
BUY_ACTION = 'buy'
NON_ENTER_NOW_ACTIONS = {'wait', 'reject'}
OUTCOME_TARGET_HIT = 'target_hit_before_stop'
OUTCOME_STOP_HIT = 'stop_hit_before_target'
OUTCOME_AMBIGUOUS = 'target_and_stop_same_day_ambiguous'
OUTCOME_NEITHER_HIT = 'neither_hit_within_5d'
OUTCOME_INVALID_LEVELS = 'invalid_no_price_levels'
OUTCOME_INVALID_MISSING_DATA = 'invalid_missing_price_data'
OUTCOME_NOT_APPLICABLE = 'not_applicable_wait_or_reject'


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_symbol(symbol: str) -> str:
    normalized = str(symbol).strip().upper()
    if not normalized:
        raise ValueError('symbol is required')
    if len(normalized) > 32:
        raise ValueError('symbol must be 32 characters or fewer')
    return normalized


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    if not symbols:
        return []
    normalized_symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in str(raw_symbol or '').split(','):
            symbol = _normalize_symbol(piece)
            if symbol in seen:
                continue
            seen.add(symbol)
            normalized_symbols.append(symbol)
    return normalized_symbols


def _normalize_agent_names(agent_names: Sequence[str] | None) -> list[str]:
    if not agent_names:
        return list(SUPPORTED_DECIDER_AGENT_NAMES)

    normalized_agent_names: list[str] = []
    seen: set[str] = set()
    supported = set(SUPPORTED_DECIDER_AGENT_NAMES)
    for raw_agent_name in agent_names:
        for piece in str(raw_agent_name or '').split(','):
            normalized = str(piece).strip().lower()
            if not normalized:
                continue
            if normalized not in supported:
                raise ValueError(
                    f'Unsupported agent_name "{normalized}". Supported values: {", ".join(SUPPORTED_DECIDER_AGENT_NAMES)}'
                )
            if normalized in seen:
                continue
            seen.add(normalized)
            normalized_agent_names.append(normalized)
    return normalized_agent_names or list(SUPPORTED_DECIDER_AGENT_NAMES)


def _normalize_outcome_labels(labels: Sequence[str] | None) -> list[str]:
    if not labels:
        return []
    normalized_labels: list[str] = []
    seen: set[str] = set()
    for raw_label in labels:
        for piece in str(raw_label or '').split(','):
            normalized = str(piece).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_labels.append(normalized)
    return normalized_labels


def _normalize_limit(limit: int, *, default: int = 50, maximum: int = 200) -> int:
    resolved = int(limit or default)
    return max(1, min(resolved, maximum))


def _normalize_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    if not isinstance(value, list):
        return []
    items: list[str] = []
    seen: set[str] = set()
    for item in value:
        normalized = _normalize_optional_text(item)
        if normalized is None or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
    return items


def _dedupe_text_list(items: Sequence[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = _normalize_optional_text(item)
        if normalized is None or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            return int(float(normalized))
        except ValueError:
            return None
    return None


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            return float(normalized)
        except ValueError:
            return None
    return None


def _maybe_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _serialize_date(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _return_pct(price: int | None, entry_price: int | None) -> float | None:
    if price is None or entry_price is None or entry_price <= 0:
        return None
    return round((price - entry_price) / entry_price, 6)


def _bar_has_complete_ohlc(bar: dict[str, Any]) -> bool:
    open_price = _to_int(bar.get('open_price'))
    high_price = _to_int(bar.get('high_price'))
    low_price = _to_int(bar.get('low_price'))
    close_price = _to_int(bar.get('close_price'))
    return all(
        value is not None and value > 0
        for value in (open_price, high_price, low_price, close_price)
    )


def _build_observed_price_path(bars: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            'trade_date': _normalize_optional_text(bar.get('trade_date')),
            'open_price': _to_int(bar.get('open_price')),
            'high_price': _to_int(bar.get('high_price')),
            'low_price': _to_int(bar.get('low_price')),
            'close_price': _to_int(bar.get('close_price')),
            'source': _normalize_optional_text(bar.get('source')),
        }
        for bar in bars
    ]


def _resolve_feedback_payload(source_row: dict[str, Any]) -> dict[str, Any]:
    normalized_feedback_payload = source_row.get('normalized_feedback_payload')
    if isinstance(normalized_feedback_payload, dict) and normalized_feedback_payload:
        return normalized_feedback_payload
    feedback_payload = source_row.get('feedback_payload')
    if isinstance(feedback_payload, dict):
        return feedback_payload
    return {}


def _serialize_evaluation(record: DeciderPostDecisionEvaluation) -> dict[str, Any]:
    warnings = (
        record.data_quality_warnings_payload
        if isinstance(record.data_quality_warnings_payload, list)
        else _normalize_text_list(record.data_quality_warnings_payload)
    )
    metadata = record.metadata_payload if isinstance(record.metadata_payload, dict) else {}
    return {
        'evaluation_id': record.id,
        'source': {
            'feedback_id': record.feedback_id,
            'decision_snapshot_id': record.decision_snapshot_id,
        },
        'agent_name': record.agent_name,
        'symbol': record.symbol,
        'market_date': record.market_date.isoformat(),
        'window_days': record.window_days,
        'action': record.decision_action,
        'entry_trade_date': _serialize_date(record.entry_trade_date),
        'entry_price': record.entry_price,
        'target_price': record.target_price,
        'stop_loss_price': record.stop_loss_price,
        'first_target_hit_trade_date': _serialize_date(record.first_target_hit_trade_date),
        'first_stop_hit_trade_date': _serialize_date(record.first_stop_hit_trade_date),
        'mfe_pct': _to_float(record.mfe_pct),
        'mae_pct': _to_float(record.mae_pct),
        'outcome_label': record.outcome_label,
        'outcome_reason': record.outcome_reason,
        'data_quality_warnings': warnings,
        'metadata': metadata,
        'computed_at': _serialize_datetime(record.computed_at),
    }


class DeciderPostDecisionEvaluationRepository:
    def list_feedback_sources(
        self,
        *,
        market_date_from: date,
        market_date_to: date,
        agent_names: Sequence[str],
        symbols: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        snapshot_id_subquery = (
            select(SignalDeciderDecisionSnapshot.id)
            .where(SignalDeciderDecisionSnapshot.feedback_id == AgentContextFeedback.id)
            .order_by(
                SignalDeciderDecisionSnapshot.created_at.desc(),
                SignalDeciderDecisionSnapshot.id.desc(),
            )
            .limit(1)
            .scalar_subquery()
        )
        snapshot_feedback_received_at_subquery = (
            select(SignalDeciderDecisionSnapshot.feedback_received_at)
            .where(SignalDeciderDecisionSnapshot.feedback_id == AgentContextFeedback.id)
            .order_by(
                SignalDeciderDecisionSnapshot.created_at.desc(),
                SignalDeciderDecisionSnapshot.id.desc(),
            )
            .limit(1)
            .scalar_subquery()
        )
        snapshot_created_at_subquery = (
            select(SignalDeciderDecisionSnapshot.created_at)
            .where(SignalDeciderDecisionSnapshot.feedback_id == AgentContextFeedback.id)
            .order_by(
                SignalDeciderDecisionSnapshot.created_at.desc(),
                SignalDeciderDecisionSnapshot.id.desc(),
            )
            .limit(1)
            .scalar_subquery()
        )
        with session_scope() as session:
            stmt = (
                select(
                    AgentContextFeedback.id.label('feedback_id'),
                    AgentContextFeedback.market_date.label('market_date'),
                    AgentContextFeedback.symbol.label('symbol'),
                    AgentContextFeedback.agent_name.label('agent_name'),
                    AgentContextFeedback.agent_version.label('agent_version'),
                    AgentContextFeedback.run_id.label('run_id'),
                    AgentContextFeedback.received_at.label('received_at'),
                    AgentContextFeedback.normalized_feedback_payload.label('normalized_feedback_payload'),
                    AgentContextFeedback.feedback_payload.label('feedback_payload'),
                    snapshot_id_subquery.label('decision_snapshot_id'),
                    snapshot_feedback_received_at_subquery.label('feedback_received_at'),
                    snapshot_created_at_subquery.label('created_at'),
                )
                .where(
                    AgentContextFeedback.market_date >= market_date_from,
                    AgentContextFeedback.market_date <= market_date_to,
                    AgentContextFeedback.agent_name.in_(list(agent_names)),
                )
                .order_by(
                    AgentContextFeedback.market_date.desc(),
                    AgentContextFeedback.received_at.desc(),
                    AgentContextFeedback.id.desc(),
                )
            )
            if symbols:
                stmt = stmt.where(AgentContextFeedback.symbol.in_(list(symbols)))
            rows = session.execute(stmt).mappings().all()
        return [dict(row) for row in rows]

    def bulk_upsert_evaluations(self, *, items: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        if not items:
            return []

        with session_scope() as session:
            stmt = insert(DeciderPostDecisionEvaluation).values(list(items))
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                constraint='uq_decider_post_decision_evaluations_feedback_window',
                set_={
                    'decision_snapshot_id': excluded.decision_snapshot_id,
                    'agent_name': excluded.agent_name,
                    'market_date': excluded.market_date,
                    'symbol': excluded.symbol,
                    'decision_action': excluded.decision_action,
                    'entry_trade_date': excluded.entry_trade_date,
                    'entry_price': excluded.entry_price,
                    'target_price': excluded.target_price,
                    'stop_loss_price': excluded.stop_loss_price,
                    'first_target_hit_trade_date': excluded.first_target_hit_trade_date,
                    'first_stop_hit_trade_date': excluded.first_stop_hit_trade_date,
                    'mfe_pct': excluded.mfe_pct,
                    'mae_pct': excluded.mae_pct,
                    'outcome_label': excluded.outcome_label,
                    'outcome_reason': excluded.outcome_reason,
                    'data_quality_warnings_payload': excluded.data_quality_warnings_payload,
                    'metadata_payload': excluded.metadata_payload,
                    'computed_at': excluded.computed_at,
                    'updated_at': excluded.updated_at,
                },
            ).returning(DeciderPostDecisionEvaluation.id)
            record_ids = list(session.execute(stmt).scalars().all())
            if not record_ids:
                return []
            rows = session.execute(
                select(DeciderPostDecisionEvaluation).where(
                    DeciderPostDecisionEvaluation.id.in_(record_ids)
                )
            ).scalars().all()
        rows_by_id = {row.id: row for row in rows}
        return [
            _serialize_evaluation(rows_by_id[record_id])
            for record_id in record_ids
            if record_id in rows_by_id
        ]

    def list_evaluations(
        self,
        *,
        market_date: date | None = None,
        market_date_from: date | None = None,
        market_date_to: date | None = None,
        agent_names: Sequence[str] | None = None,
        symbols: Sequence[str] | None = None,
        outcome_labels: Sequence[str] | None = None,
        window_days: int = DEFAULT_DECIDER_EVALUATION_WINDOW_DAYS,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = select(DeciderPostDecisionEvaluation).where(
                DeciderPostDecisionEvaluation.window_days == int(window_days)
            )
            if market_date is not None:
                stmt = stmt.where(DeciderPostDecisionEvaluation.market_date == market_date)
            if market_date_from is not None:
                stmt = stmt.where(DeciderPostDecisionEvaluation.market_date >= market_date_from)
            if market_date_to is not None:
                stmt = stmt.where(DeciderPostDecisionEvaluation.market_date <= market_date_to)
            if agent_names:
                stmt = stmt.where(DeciderPostDecisionEvaluation.agent_name.in_(list(agent_names)))
            if symbols:
                stmt = stmt.where(DeciderPostDecisionEvaluation.symbol.in_(list(symbols)))
            if outcome_labels:
                stmt = stmt.where(DeciderPostDecisionEvaluation.outcome_label.in_(list(outcome_labels)))
            stmt = stmt.order_by(
                DeciderPostDecisionEvaluation.market_date.desc(),
                DeciderPostDecisionEvaluation.computed_at.desc(),
                DeciderPostDecisionEvaluation.id.desc(),
            ).limit(_normalize_limit(limit))
            rows = session.execute(stmt).scalars().all()
        return [_serialize_evaluation(row) for row in rows]


class DeciderPostDecisionEvaluationService:
    def __init__(
        self,
        *,
        repository: DeciderPostDecisionEvaluationRepository | None = None,
        daily_bar_loader: Callable[..., list[dict[str, Any]]] = load_daily_bars_for_symbol_range,
    ) -> None:
        self.repository = repository or DeciderPostDecisionEvaluationRepository()
        self.daily_bar_loader = daily_bar_loader

    def compute_evaluations(
        self,
        *,
        market_date_from: str | date,
        market_date_to: str | date,
        agent_names: Sequence[str] | None = None,
        symbols: Sequence[str] | None = None,
        window_days: int = DEFAULT_DECIDER_EVALUATION_WINDOW_DAYS,
        force_recompute: bool = False,
    ) -> dict[str, Any]:
        normalized_from = _normalize_market_date(market_date_from)
        normalized_to = _normalize_market_date(market_date_to)
        if normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        resolved_window_days = self._normalize_window_days(window_days)
        normalized_agent_names = _normalize_agent_names(agent_names)
        normalized_symbols = _normalize_symbols(symbols)
        source_rows = self.repository.list_feedback_sources(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            agent_names=normalized_agent_names,
            symbols=normalized_symbols or None,
        )
        source_rows = [
            source_row
            for source_row in source_rows
            if source_row.get('decision_snapshot_id') is None
            or is_decision_snapshot_valid_for_source_date(source_row)
        ]
        evaluation_items = [
            self._build_evaluation_item(source_row=source_row, window_days=resolved_window_days)
            for source_row in source_rows
        ]
        persisted = self.repository.bulk_upsert_evaluations(items=evaluation_items)
        return {
            'request': {
                'market_date_from': normalized_from.isoformat(),
                'market_date_to': normalized_to.isoformat(),
                'agent_names': normalized_agent_names,
                'symbols': normalized_symbols,
                'window_days': resolved_window_days,
                'force_recompute': force_recompute,
                'evaluation_engine_version': DECIDER_EVALUATION_ENGINE_VERSION,
            },
            'summary': {
                'feedback_rows_considered': len(source_rows),
                'evaluations_upserted': len(persisted),
                'outcome_label_counts': self._count_by_key(persisted, key='outcome_label'),
                'agent_counts': self._count_by_key(persisted, key='agent_name'),
            },
            'items': persisted,
        }

    def list_evaluations(
        self,
        *,
        market_date: str | date | None = None,
        market_date_from: str | date | None = None,
        market_date_to: str | date | None = None,
        agent_names: Sequence[str] | None = None,
        symbols: Sequence[str] | None = None,
        outcome_labels: Sequence[str] | None = None,
        window_days: int = DEFAULT_DECIDER_EVALUATION_WINDOW_DAYS,
        limit: int = 50,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date) if market_date is not None else None
        normalized_from = _normalize_market_date(market_date_from) if market_date_from is not None else None
        normalized_to = _normalize_market_date(market_date_to) if market_date_to is not None else None
        if normalized_market_date is not None:
            normalized_from = normalized_market_date if normalized_from is None else normalized_from
            normalized_to = normalized_market_date if normalized_to is None else normalized_to
        if normalized_from is not None and normalized_to is not None and normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')

        resolved_window_days = self._normalize_window_days(window_days)
        normalized_agent_names = _normalize_agent_names(agent_names)
        normalized_symbols = _normalize_symbols(symbols)
        normalized_outcome_labels = _normalize_outcome_labels(outcome_labels)
        resolved_limit = _normalize_limit(limit)
        items = self.repository.list_evaluations(
            market_date=normalized_market_date,
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            agent_names=normalized_agent_names,
            symbols=normalized_symbols or None,
            outcome_labels=normalized_outcome_labels or None,
            window_days=resolved_window_days,
            limit=resolved_limit,
        )
        return {
            'request': {
                'market_date': normalized_market_date.isoformat() if normalized_market_date is not None else None,
                'market_date_from': normalized_from.isoformat() if normalized_from is not None else None,
                'market_date_to': normalized_to.isoformat() if normalized_to is not None else None,
                'agent_names': normalized_agent_names,
                'symbols': normalized_symbols,
                'outcome_labels': normalized_outcome_labels,
                'window_days': resolved_window_days,
                'limit': resolved_limit,
            },
            'summary': {
                'count': len(items),
                'outcome_label_counts': self._count_by_key(items, key='outcome_label'),
                'agent_counts': self._count_by_key(items, key='agent_name'),
            },
            'items': items,
        }

    @staticmethod
    def _normalize_window_days(window_days: int) -> int:
        resolved = int(window_days)
        if resolved != DEFAULT_DECIDER_EVALUATION_WINDOW_DAYS:
            raise ValueError('Only window_days=5 is supported in this slice')
        return resolved

    def _load_future_price_bars(
        self,
        *,
        symbol: str,
        market_date: date,
        window_days: int,
    ) -> list[dict[str, Any]]:
        bars = self.daily_bar_loader(
            symbol,
            start_date=market_date,
            limit=window_days + 2,
            prefer_recent_limit=False,
        )
        return [
            bar
            for bar in bars
            if str(bar.get('trade_date') or '') > market_date.isoformat()
        ][:window_days]

    def _build_evaluation_item(
        self,
        *,
        source_row: dict[str, Any],
        window_days: int,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        payload = _resolve_feedback_payload(source_row)
        action = (
            _normalize_optional_text(
                payload.get('preferred_action')
                or payload.get('next_day_open_action')
                or payload.get('tactical_action')
            )
            or 'wait'
        ).lower()
        target_price = _to_int(payload.get('target_price'))
        stop_loss_price = _to_int(payload.get('stop_loss_price'))
        trigger_price = _to_int(payload.get('trigger_price'))
        chase_limit_price = _to_int(payload.get('chase_limit_price'))
        entry_price_basis = _normalize_optional_text(payload.get('entry_price_basis')) or 'next_open'
        feedback_warnings = _normalize_text_list(payload.get('data_quality_warnings'))

        future_bars = self._load_future_price_bars(
            symbol=str(source_row['symbol']),
            market_date=source_row['market_date'],
            window_days=window_days,
        )
        usable_bars = [bar for bar in future_bars if _bar_has_complete_ohlc(bar)]
        entry_bar = future_bars[0] if future_bars else None
        entry_trade_date = _maybe_date(entry_bar.get('trade_date')) if entry_bar is not None else None
        entry_price = _to_int(entry_bar.get('open_price')) if entry_bar is not None else None
        max_high_price = max((_to_int(bar.get('high_price')) or 0) for bar in usable_bars) if usable_bars else None
        min_low_price = min((_to_int(bar.get('low_price')) or 0) for bar in usable_bars) if usable_bars else None
        mfe_pct = _return_pct(max_high_price, entry_price)
        mae_pct = _return_pct(min_low_price, entry_price)
        first_target_hit_trade_date: date | None = None
        first_stop_hit_trade_date: date | None = None
        ambiguous_trade_date: date | None = None

        computed_warnings = list(feedback_warnings)
        action_interpretation = 'enter_now_next_open'

        if action in NON_ENTER_NOW_ACTIONS:
            outcome_label = OUTCOME_NOT_APPLICABLE
            outcome_reason = f'{action} decisions are stored for traceability but are not evaluated as enter-now positions.'
            action_interpretation = 'non_enter_now'
        elif action != BUY_ACTION:
            computed_warnings.append('unsupported_tactical_action_for_generic_5d_evaluation')
            outcome_label = OUTCOME_NOT_APPLICABLE
            outcome_reason = (
                f'{source_row["agent_name"]} action "{action}" is not yet mapped to a generic next-open 5-day evaluation.'
            )
            action_interpretation = 'unsupported_tactical_action'
        elif target_price is None or stop_loss_price is None or target_price <= 0 or stop_loss_price <= 0:
            if target_price is None or target_price <= 0:
                computed_warnings.append('missing_target_price')
            if stop_loss_price is None or stop_loss_price <= 0:
                computed_warnings.append('missing_stop_loss_price')
            outcome_label = OUTCOME_INVALID_LEVELS
            outcome_reason = 'Buy decisions require explicit positive target and stop-loss prices.'
        elif len(future_bars) < window_days or len(usable_bars) < window_days or entry_price is None or entry_price <= 0:
            if len(future_bars) < window_days:
                computed_warnings.append('incomplete_next_5_trading_days')
            if len(usable_bars) < len(future_bars):
                computed_warnings.append('incomplete_ohlc_in_observed_window')
            if entry_price is None or entry_price <= 0:
                computed_warnings.append('missing_next_open_entry_price')
            outcome_label = OUTCOME_INVALID_MISSING_DATA
            outcome_reason = (
                f'Stored daily_price_bars did not provide a complete next {window_days}-trading-day OHLC window.'
            )
        else:
            first_target_hit_trade_date, first_stop_hit_trade_date, ambiguous_trade_date = self._resolve_hit_dates(
                observed_bars=usable_bars,
                target_price=target_price,
                stop_loss_price=stop_loss_price,
            )
            if ambiguous_trade_date is not None:
                outcome_label = OUTCOME_AMBIGUOUS
                outcome_reason = (
                    f'Target and stop-loss were both inside the {ambiguous_trade_date.isoformat()} daily range.'
                )
            elif first_target_hit_trade_date is not None and (
                first_stop_hit_trade_date is None or first_target_hit_trade_date < first_stop_hit_trade_date
            ):
                outcome_label = OUTCOME_TARGET_HIT
                outcome_reason = (
                    f'Target was first reachable on {first_target_hit_trade_date.isoformat()} before any stop-loss touch.'
                )
            elif first_stop_hit_trade_date is not None and (
                first_target_hit_trade_date is None or first_stop_hit_trade_date < first_target_hit_trade_date
            ):
                outcome_label = OUTCOME_STOP_HIT
                outcome_reason = (
                    f'Stop-loss was first reachable on {first_stop_hit_trade_date.isoformat()} before the target.'
                )
            else:
                outcome_label = OUTCOME_NEITHER_HIT
                outcome_reason = f'Neither target nor stop-loss was touched within the next {window_days} trading days.'
        metadata_payload = {
            'schema_version': DECIDER_EVALUATION_ENGINE_VERSION,
            'market_data_source': 'daily_price_bars',
            'entry_price_basis': entry_price_basis,
            'action_interpretation': action_interpretation,
            'feedback_run_id': _normalize_optional_text(source_row.get('run_id')),
            'feedback_received_at': _serialize_datetime(source_row.get('received_at')),
            'observed_bars_count': len(future_bars),
            'usable_bars_count': len(usable_bars),
            'required_bars_count': window_days,
            'price_window_complete': len(future_bars) == window_days and len(usable_bars) == window_days,
            'observed_trade_dates': [
                _normalize_optional_text(bar.get('trade_date'))
                for bar in future_bars
                if _normalize_optional_text(bar.get('trade_date')) is not None
            ],
            'trigger_price': trigger_price,
            'chase_limit_price': chase_limit_price,
            'latest_decision_snapshot_id': source_row.get('decision_snapshot_id'),
            'observed_price_path': _build_observed_price_path(future_bars),
        }
        return {
            'feedback_id': int(source_row['feedback_id']),
            'decision_snapshot_id': source_row.get('decision_snapshot_id'),
            'agent_name': str(source_row['agent_name']),
            'market_date': source_row['market_date'],
            'symbol': str(source_row['symbol']),
            'window_days': window_days,
            'decision_action': action,
            'entry_trade_date': entry_trade_date,
            'entry_price': entry_price,
            'target_price': target_price,
            'stop_loss_price': stop_loss_price,
            'first_target_hit_trade_date': first_target_hit_trade_date,
            'first_stop_hit_trade_date': first_stop_hit_trade_date,
            'mfe_pct': mfe_pct,
            'mae_pct': mae_pct,
            'outcome_label': outcome_label,
            'outcome_reason': outcome_reason,
            'data_quality_warnings_payload': _dedupe_text_list(computed_warnings),
            'metadata_payload': metadata_payload,
            'computed_at': now,
            'created_at': now,
            'updated_at': now,
        }

    @staticmethod
    def _resolve_hit_dates(
        *,
        observed_bars: Sequence[dict[str, Any]],
        target_price: int | None,
        stop_loss_price: int | None,
    ) -> tuple[date | None, date | None, date | None]:
        first_target_hit_trade_date: date | None = None
        first_stop_hit_trade_date: date | None = None
        ambiguous_trade_date: date | None = None
        for bar in observed_bars:
            trade_date = _maybe_date(bar.get('trade_date'))
            if trade_date is None:
                continue
            high_price = _to_int(bar.get('high_price'))
            low_price = _to_int(bar.get('low_price'))
            if high_price is None or low_price is None:
                continue
            target_touched = target_price is not None and high_price >= target_price
            stop_touched = stop_loss_price is not None and low_price <= stop_loss_price
            if target_touched and stop_touched and first_target_hit_trade_date is None and first_stop_hit_trade_date is None:
                ambiguous_trade_date = trade_date
                first_target_hit_trade_date = trade_date
                first_stop_hit_trade_date = trade_date
                break
            if target_touched and first_target_hit_trade_date is None:
                first_target_hit_trade_date = trade_date
            if stop_touched and first_stop_hit_trade_date is None:
                first_stop_hit_trade_date = trade_date
        return first_target_hit_trade_date, first_stop_hit_trade_date, ambiguous_trade_date

    @staticmethod
    def _count_by_key(items: Sequence[dict[str, Any]], *, key: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in items:
            value = _normalize_optional_text(item.get(key))
            if value is None:
                continue
            counts[value] = counts.get(value, 0) + 1
        return counts


decider_post_decision_evaluation_service = DeciderPostDecisionEvaluationService()
