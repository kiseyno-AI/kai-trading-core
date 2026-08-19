from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any, Callable, Sequence

from fastapi.encoders import jsonable_encoder

from app.agent_context.orchestrator import DISPATCH_SPECS, AgentDispatchSpec, agent_feedback_orchestrator
from app.agent_context.response_contract import normalize_agent_response
from app.repositories.us_equity_repository import load_us_agent_candidates, upsert_us_agent_feedback


US_AGENT_ROLE_CAPS = {
    'eod-summarizer': 150,
    'technical-summarizer': 120,
    'fundamental-summarizer': 100,
    'short-term-decider': 80,
    'signal-decider': 80,
}


def build_us_agent_context(item: dict, *, market_date: date) -> dict[str, Any]:
    return {
        'schema_version': 'us_agent_context_v1',
        'market_date': market_date.isoformat(),
        'symbol': str(item['symbol']),
        'symbol_day_context': {
            'symbol': str(item['symbol']),
            'market': 'US',
            'exchange': str(item.get('exchange_code') or item.get('market') or ''),
            'currency': 'USD',
            'calendar_timezone': 'America/New_York',
            'company': {
                'name': item.get('name_en') or item['symbol'],
                'ticker': item['symbol'],
                'cik': item.get('regulatory_id'),
            },
            'price_screen': (item.get('metrics_payload') or {}).get('price') or {},
            'fundamental_screen': (item.get('metrics_payload') or {}).get('fundamental') or {},
            'screening_decision': item.get('decision_payload') or {},
            'context_provenance': {
                'universe': 'nasdaq_trader+korea_investment_master',
                'prices': 'kis_overseas_dailyprice_adjusted',
                'fundamentals': 'sec_edgar_companyfacts+submissions',
                'screening_rank': item.get('rank'),
                'screening_score': str(item.get('composite_score')) if item.get('composite_score') is not None else None,
            },
        },
    }


class USAgentAnalysisService:
    def __init__(
        self,
        *,
        candidate_loader: Callable[..., list[dict]] = load_us_agent_candidates,
        dispatcher_getter: Callable[[], Any] | None = None,
    ) -> None:
        self._candidate_loader = candidate_loader
        self._dispatcher_getter = dispatcher_getter or agent_feedback_orchestrator._get_client

    def prepare(self, *, market_date: date, roles: Sequence[str] | None = None) -> dict[str, Any]:
        candidates = self._candidate_loader(market_date=market_date, limit=150)
        if not candidates:
            raise ValueError('No fundamental-screen candidates found for the requested market date')
        requested = set(roles or US_AGENT_ROLE_CAPS)
        unknown = requested - set(US_AGENT_ROLE_CAPS)
        if unknown:
            raise ValueError(f'Unsupported US agent roles: {", ".join(sorted(unknown))}')
        contexts = {str(item['symbol']): build_us_agent_context(item, market_date=market_date) for item in candidates}
        scopes = {
            role: [str(item['symbol']) for item in candidates[:US_AGENT_ROLE_CAPS[role]]]
            for role in US_AGENT_ROLE_CAPS
            if role in requested
        }
        return {
            'market_date': market_date,
            'candidates': candidates,
            'contexts': contexts,
            'role_scopes': scopes,
            'counts': {role: len(symbols) for role, symbols in scopes.items()},
        }

    def dispatch(
        self,
        *,
        market_date: date,
        roles: Sequence[str] | None = None,
        persist: bool = True,
        batch_size: int = 5,
    ) -> dict[str, Any]:
        prepared = self.prepare(market_date=market_date, roles=roles)
        candidate_by_symbol = {str(item['symbol']): item for item in prepared['candidates']}
        specs_by_role = {spec.role: spec for spec in DISPATCH_SPECS}
        run_id = f'us-symbol-run-{market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        dispatcher = self._dispatcher_getter()
        feedback_by_symbol: dict[str, dict[str, dict]] = {}
        completed: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for role, symbols in prepared['role_scopes'].items():
            spec: AgentDispatchSpec = specs_by_role[role]
            for start in range(0, len(symbols), max(1, batch_size)):
                batch_symbols = symbols[start:start + max(1, batch_size)]
                batch_items = []
                for symbol in batch_symbols:
                    context = dict(prepared['contexts'][symbol])
                    context['upstream_agent_feedback'] = feedback_by_symbol.get(symbol, {})
                    context['request_run_id'] = run_id
                    context['session_key'] = f'feedback-us/{role}/{market_date.isoformat()}/{start // max(1, batch_size):03d}'
                    batch_items.append(context)
                try:
                    batch_result = dispatcher.dispatch_batch(
                        spec=spec,
                        market_date=market_date,
                        batch_items=batch_items,
                    )
                    decoded_items = (batch_result.get('decoded') or {}).get('items') or []
                    decoded_by_symbol = {
                        str(row.get('symbol')): row.get('feedback')
                        for row in decoded_items
                        if isinstance(row, dict) and isinstance(row.get('feedback'), dict)
                    }
                    for context in batch_items:
                        symbol = str(context['symbol'])
                        feedback = decoded_by_symbol.get(symbol)
                        if feedback is None:
                            failures.append({'role': role, 'symbol': symbol, 'error': 'missing_symbol_feedback'})
                            continue
                        normalized = normalize_agent_response(
                            role=role,
                            output_text=json.dumps(feedback, ensure_ascii=False),
                            raw_response=feedback,
                            market_date=market_date.isoformat(),
                            symbol=symbol,
                            agent_name=spec.agent_name,
                        )
                        feedback_by_symbol.setdefault(symbol, {})[role] = normalized
                        feedback_id = None
                        if persist:
                            feedback_id = upsert_us_agent_feedback(
                                market_date=market_date,
                                symbol_id=int(candidate_by_symbol[symbol]['id']),
                                agent_name=spec.agent_name,
                                run_id=run_id,
                                context_payload=context,
                                response_payload=jsonable_encoder(batch_result),
                                normalized_feedback_payload=normalized,
                            )
                        completed.append({'role': role, 'symbol': symbol, 'feedback_id': feedback_id})
                except Exception as exc:
                    failures.extend(
                        {'role': role, 'symbol': symbol, 'error': f'{type(exc).__name__}: {exc}'}
                        for symbol in batch_symbols
                    )
        status = 'partial' if completed and failures else 'failed' if failures else 'completed'
        return {
            'status': status,
            'market_date': market_date.isoformat(),
            'run_id': run_id,
            'persist': persist,
            'role_caps': US_AGENT_ROLE_CAPS,
            'role_counts': prepared['counts'],
            'completed': len(completed),
            'failed': len(failures),
            'items': completed,
            'failures': failures,
        }


us_agent_analysis_service = USAgentAnalysisService()
