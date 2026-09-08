from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any, Callable, Sequence

from fastapi.encoders import jsonable_encoder

from app.agent_context.orchestrator import DISPATCH_SPECS, AgentDispatchSpec, agent_feedback_orchestrator
from app.agent_context.orchestrator_runtime_helpers import (
    batch_response_contract_failure,
    normalized_feedback_contract_failure,
)
from app.agent_context.response_contract import normalize_agent_response
from app.repositories.us_equity_repository import load_us_agent_candidates, upsert_us_agent_feedback


US_AGENT_ROLE_CAPS = {
    'eod-summarizer': 150,
    'technical-summarizer': 120,
    'fundamental-summarizer': 100,
    'short-term-decider': 80,
    'signal-decider': 80,
}

US_AGENT_ROLE_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    'short-term-decider': (
        'eod-summarizer',
        'technical-summarizer',
        'fundamental-summarizer',
    ),
    'signal-decider': (
        'eod-summarizer',
        'technical-summarizer',
        'fundamental-summarizer',
    ),
}


def _expand_roles(roles: Sequence[str] | None) -> set[str]:
    requested = set(roles or US_AGENT_ROLE_CAPS)
    pending = list(requested)
    while pending:
        role = pending.pop()
        for dependency in US_AGENT_ROLE_DEPENDENCIES.get(role, ()):
            if dependency not in requested:
                requested.add(dependency)
                pending.append(dependency)
    return requested


def _fundamental_fact(value: Any) -> dict[str, Any]:
    return {
        'status': 'available' if value is not None else 'missing',
        'value': value,
    }


def build_us_agent_context(item: dict, *, market_date: date) -> dict[str, Any]:
    metrics_payload = item.get('metrics_payload') or {}
    price_screen = jsonable_encoder(metrics_payload.get('price') or {})
    fundamental_screen = jsonable_encoder(metrics_payload.get('fundamental') or {})
    latest_price = price_screen.get('latest_price')
    median_dollar_volume = price_screen.get('median_dollar_volume_20d')
    median_volume = price_screen.get('median_volume_20d')
    fundamental_values = {
        'revenue': fundamental_screen.get('ttm_revenue'),
        'net_income': fundamental_screen.get('ttm_net_income'),
        'operating_cash_flow': fundamental_screen.get('ttm_operating_cash_flow'),
        'total_assets': fundamental_screen.get('total_assets'),
        'total_liabilities': fundamental_screen.get('total_liabilities'),
        'shares_outstanding_growth_yoy': fundamental_screen.get('shares_outstanding_growth_yoy'),
    }
    missing_fundamental_fields = [
        key for key, value in fundamental_values.items() if value is None
    ]
    fundamental_risk_flags = []
    if fundamental_screen.get('has_bankruptcy_or_delinquency_flag'):
        fundamental_risk_flags.append('bankruptcy_or_delinquency_flag')

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
            'market_session': {
                'strategy_phase': 'post_close_next_open_planning',
                'execution_session': 'next_us_regular_session_open',
                'intraday_snapshot_expected': False,
            },
            'market_data_phase_contract': {
                'strategy_phase': 'post_close_next_open_planning',
                'authoritative_inputs': [
                    'prior_session_adjusted_eod_price_history',
                    'latest_available_sec_periodic_fundamentals',
                ],
                'expected_not_applicable_inputs': [
                    'intraday_quote',
                    'bid_ask_spread',
                    'orderbook_depth',
                ],
                'missing_intraday_is_automatic_wait_blocker': False,
                'decision_guidance': (
                    'Use adjusted EOD price history and SEC fundamentals as authoritative next-open '
                    'inputs. Missing intraday quote, spread, or orderbook data is expected in this '
                    'post-close phase and must not by itself force wait or reject.'
                ),
            },
            'decider_data_quality_context': {
                'status': 'available',
                'requested_market_date': market_date.isoformat(),
                'block_status_counts': {'available': 2, 'not_applicable': 1},
                'decision_priority': {
                    'core_execution': {
                        'status': 'available',
                        'blocking_by_default': True,
                    },
                    'intraday_realtime': {
                        'status': 'not_applicable',
                        'blocking_by_default': False,
                    },
                },
                'blocks': {
                    'eod_price_history': {
                        'status': 'available',
                        'decision_guidance': {
                            'decision_tier': 'core_execution',
                            'authoritative_for': 'next_open_planning',
                        },
                        'providers': {'kis_adjusted_daily_price': {'status': 'available'}},
                    },
                    'financials': {
                        'status': 'available' if not missing_fundamental_fields else 'partial',
                        'decision_guidance': {
                            'decision_tier': 'core_execution',
                            'authoritative_for': 'next_open_planning',
                        },
                        'providers': {'sec_edgar': {'status': 'available'}},
                    },
                    'intraday_realtime': {
                        'status': 'not_applicable',
                        'decision_guidance': {
                            'decision_tier': 'expected_not_applicable',
                            'blocking_by_default': False,
                            'reason': 'post_close_next_open_planning',
                        },
                        'providers': {
                            'intraday_quote_spread_orderbook': {'status': 'not_applicable'},
                        },
                    },
                },
            },
            'company': {
                'name': item.get('name_en') or item['symbol'],
                'ticker': item['symbol'],
                'cik': item.get('regulatory_id'),
            },
            'price_context': {
                'schema_version': 'us_price_context_v1',
                'source': 'kis_overseas_dailyprice_adjusted',
                'trade_date': market_date.isoformat(),
                'close_price': latest_price,
                'latest_price': latest_price,
                'history_days': price_screen.get('history_days'),
                'returns': {
                    'return_5d': price_screen.get('return_5d'),
                    'momentum_6m': price_screen.get('momentum_6m'),
                    'momentum_12m': price_screen.get('momentum_12m'),
                },
                'volatility': {
                    'annualized_60d': price_screen.get('volatility_60d'),
                },
                'drawdown': {
                    'max_120d': price_screen.get('max_drawdown_120d'),
                },
                'liquidity': {
                    'median_dollar_volume_20d': median_dollar_volume,
                    'median_volume_20d': median_volume,
                },
            },
            'technical_snapshot': {
                'source': 'us_price_screen_v1',
                'market_date': market_date.isoformat(),
                'close_price': latest_price,
                'return_5d': price_screen.get('return_5d'),
                'momentum_6m': price_screen.get('momentum_6m'),
                'momentum_12m': price_screen.get('momentum_12m'),
                'volatility_60d': price_screen.get('volatility_60d'),
                'max_drawdown_120d': price_screen.get('max_drawdown_120d'),
                'history_days': price_screen.get('history_days'),
                'price_level_inputs': {
                    'reference_price': latest_price,
                    'annualized_volatility_60d': price_screen.get('volatility_60d'),
                    'max_drawdown_120d': price_screen.get('max_drawdown_120d'),
                },
            },
            'liquidity_context': {
                'status': 'derived' if median_dollar_volume is not None else 'unavailable',
                'turnover': {
                    'status': 'derived' if median_dollar_volume is not None else 'unavailable',
                    'currency': 'USD',
                    'median_dollar_volume_20d': median_dollar_volume,
                    'reason': None if median_dollar_volume is not None else 'missing_us_price_screen_turnover',
                },
                'ranking': {
                    'status': 'available' if median_volume is not None else 'unavailable',
                    'median_volume_20d': median_volume,
                },
                'intraday': {
                    'status': 'not_applicable',
                    'reason': 'eod_us_screening_context',
                },
            },
            'fundamentals': {
                'status': 'available' if not missing_fundamental_fields else 'partial',
                'latest_snapshot': {
                    'source': 'sec_edgar_companyfacts+submissions',
                    'period': {
                        'filing_date': fundamental_screen.get('latest_periodic_filing_date'),
                    },
                    'highlights': fundamental_values,
                    'fundamental_context': {
                        'quality_summary': {
                            'revenue': _fundamental_fact(fundamental_values['revenue']),
                            'net_income': _fundamental_fact(fundamental_values['net_income']),
                        },
                        'growth_summary': {
                            'shares_outstanding_growth_yoy': _fundamental_fact(
                                fundamental_values['shares_outstanding_growth_yoy']
                            ),
                        },
                        'balance_sheet_summary': {
                            'total_assets': _fundamental_fact(fundamental_values['total_assets']),
                            'total_liabilities': _fundamental_fact(fundamental_values['total_liabilities']),
                        },
                        'cash_flow_summary': {
                            'operating_cash_flow': _fundamental_fact(
                                fundamental_values['operating_cash_flow']
                            ),
                        },
                        'risk_flags': fundamental_risk_flags,
                        'missing_fields': missing_fundamental_fields,
                    },
                },
            },
            'price_screen': price_screen,
            'fundamental_screen': fundamental_screen,
            'screening_decision': item.get('decision_payload') or {},
            'context_provenance': {
                'collection_run_id': item.get('collection_run_id'),
                'universe': 'nasdaq_trader+korea_investment_master',
                'prices': 'kis_overseas_dailyprice_adjusted',
                'fundamentals': 'sec_edgar_companyfacts+submissions',
                'screening_rank': item.get('rank'),
                'screening_score': (
                    str(item.get('composite_score'))
                    if item.get('composite_score') is not None
                    else None
                ),
            },
        },
    }


class USAgentAnalysisService:
    def __init__(
        self,
        *,
        candidate_loader: Callable[..., list[dict]] = load_us_agent_candidates,
        dispatcher_getter: Callable[[], Any] | None = None,
        session_cleanup: Callable[..., dict[str, Any]] | None = None,
    ) -> None:
        self._candidate_loader = candidate_loader
        self._dispatcher_getter = dispatcher_getter or agent_feedback_orchestrator._get_client
        self._session_cleanup = session_cleanup or agent_feedback_orchestrator._cleanup_session_key

    def _cleanup_fresh_batch_session(
        self,
        *,
        market_date: date,
        symbol: str,
        agent_name: str,
        run_id: str,
        session_key: str,
    ) -> dict[str, Any]:
        audit_context = {
            'session_key': session_key,
            'agent_name': agent_name,
            'symbol': symbol,
        }
        try:
            result = self._session_cleanup(
                market_date=market_date,
                symbol=symbol,
                agent_name=agent_name,
                run_id=run_id,
                session_key=session_key,
            )
        except Exception as exc:  # noqa: BLE001
            return {
                **audit_context,
                'status': 'deferred',
                'reason': 'fresh_batch_session_cleanup_failed',
                'error_type': type(exc).__name__,
                'error': str(exc),
            }
        if isinstance(result, dict):
            return {**result, **audit_context}
        return {
            **audit_context,
            'status': 'unknown',
            'reason': 'fresh_batch_session_cleanup_returned_non_object',
        }

    def prepare(
        self,
        *,
        market_date: date,
        collection_run_id: int,
        roles: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        candidates = self._candidate_loader(
            market_date=market_date,
            collection_run_id=collection_run_id,
            limit=150,
        )
        if not candidates:
            raise ValueError('No fundamental-screen candidates found for the requested market date')
        requested = _expand_roles(roles)
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
            'collection_run_id': collection_run_id,
            'candidates': candidates,
            'contexts': contexts,
            'role_scopes': scopes,
            'counts': {role: len(symbols) for role, symbols in scopes.items()},
        }

    def dispatch(
        self,
        *,
        market_date: date,
        collection_run_id: int,
        roles: Sequence[str] | None = None,
        persist: bool = True,
        batch_size: int = 5,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        prepared = self.prepare(
            market_date=market_date,
            collection_run_id=collection_run_id,
            roles=roles,
        )
        candidate_by_symbol = {str(item['symbol']): item for item in prepared['candidates']}
        specs_by_role = {spec.role: spec for spec in DISPATCH_SPECS}
        run_id = f'us-symbol-run-{market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        dispatcher = self._dispatcher_getter()
        feedback_by_symbol: dict[str, dict[str, dict]] = {}
        completed: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        session_cleanup: list[dict[str, Any]] = []
        bounded_batch_size = max(1, batch_size)
        total_batches = sum(
            (len(symbols) + bounded_batch_size - 1) // bounded_batch_size
            for symbols in prepared['role_scopes'].values()
        )
        completed_batches = 0
        for role, symbols in prepared['role_scopes'].items():
            spec: AgentDispatchSpec = specs_by_role[role]
            for start in range(0, len(symbols), bounded_batch_size):
                batch_index = (start // bounded_batch_size) + 1
                batch_session_key = (
                    f'agent:{role}:feedback-us/{role}/{run_id}/'
                    f'batch-{batch_index:03d}/{uuid.uuid4().hex[:10]}'
                )
                batch_symbols = symbols[start:start + bounded_batch_size]
                batch_items = []
                for symbol in batch_symbols:
                    missing_dependencies = [
                        dependency
                        for dependency in US_AGENT_ROLE_DEPENDENCIES.get(role, ())
                        if dependency not in feedback_by_symbol.get(symbol, {})
                    ]
                    if missing_dependencies:
                        failures.append({
                            'role': role,
                            'symbol': symbol,
                            'error': f"missing_upstream_feedback:{','.join(missing_dependencies)}",
                        })
                        continue
                    context = dict(prepared['contexts'][symbol])
                    context['upstream_agent_feedback'] = feedback_by_symbol.get(symbol, {})
                    context['request_run_id'] = run_id
                    context['session_key'] = batch_session_key
                    context['session_reuse_mode'] = 'fresh_per_batch'
                    batch_items.append(context)
                if not batch_items:
                    completed_batches += 1
                    if progress_callback is not None:
                        progress_callback({
                            'status': 'running',
                            'market_date': market_date.isoformat(),
                            'collection_run_id': collection_run_id,
                            'run_id': run_id,
                            'persist': persist,
                            'current_role': role,
                            'completed_batches': completed_batches,
                            'total_batches': total_batches,
                            'completed': len(completed),
                            'failed': len(failures),
                        })
                    continue
                try:
                    batch_result = dispatcher.dispatch_batch(
                        spec=spec,
                        market_date=market_date,
                        batch_items=batch_items,
                    )
                    batch_contract_failure = batch_response_contract_failure(batch_result)
                    decoded_items = (batch_result.get('decoded') or {}).get('items') or []
                    decoded_by_symbol = {
                        str(row.get('symbol')): row.get('feedback')
                        for row in decoded_items
                        if isinstance(row, dict) and isinstance(row.get('feedback'), dict)
                    }
                    staged_feedback: list[tuple[dict[str, Any], str, dict[str, Any]]] = []
                    for context in batch_items:
                        symbol = str(context['symbol'])
                        feedback = decoded_by_symbol.get(symbol)
                        if feedback is None:
                            batch_contract_failure = 'response_contract_invalid:feedback_missing'
                            break
                        normalized = normalize_agent_response(
                            role=role,
                            output_text=json.dumps(feedback, ensure_ascii=False),
                            raw_response=feedback,
                            market_date=market_date.isoformat(),
                            symbol=symbol,
                            agent_name=spec.agent_name,
                        )
                        normalized_contract_failure = normalized_feedback_contract_failure(normalized)
                        if normalized_contract_failure is not None:
                            batch_contract_failure = normalized_contract_failure
                            break
                        staged_feedback.append((context, symbol, normalized))

                    if batch_contract_failure is not None:
                        failures.extend(
                            {
                                'role': role,
                                'symbol': str(context['symbol']),
                                'error': batch_contract_failure,
                            }
                            for context in batch_items
                        )
                    else:
                        for context, symbol, normalized in staged_feedback:
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
                finally:
                    session_cleanup.append(
                        self._cleanup_fresh_batch_session(
                            market_date=market_date,
                            symbol=str(batch_items[0]['symbol']),
                            agent_name=spec.agent_name,
                            run_id=run_id,
                            session_key=batch_session_key,
                        )
                    )
                completed_batches += 1
                if progress_callback is not None:
                    progress_callback({
                        'status': 'running',
                        'market_date': market_date.isoformat(),
                        'collection_run_id': collection_run_id,
                        'run_id': run_id,
                        'persist': persist,
                        'current_role': role,
                        'completed_batches': completed_batches,
                        'total_batches': total_batches,
                        'completed': len(completed),
                        'failed': len(failures),
                    })
        status = 'partial' if completed and failures else 'failed' if failures else 'completed'
        return {
            'status': status,
            'market_date': market_date.isoformat(),
            'collection_run_id': collection_run_id,
            'run_id': run_id,
            'persist': persist,
            'role_caps': US_AGENT_ROLE_CAPS,
            'role_counts': prepared['counts'],
            'completed': len(completed),
            'failed': len(failures),
            'completed_batches': completed_batches,
            'total_batches': total_batches,
            'items': completed,
            'failures': failures,
            'session_cleanup': session_cleanup,
        }


us_agent_analysis_service = USAgentAnalysisService()
