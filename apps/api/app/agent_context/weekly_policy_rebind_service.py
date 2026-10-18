from __future__ import annotations

from datetime import date
import hashlib
from typing import Any, Callable, Mapping

from app.agent_context.retry_service import (
    FeedbackRetryCycleConfig,
    WEEKLY_POLICY_REBIND_SCHEMA_VERSION,
    _normalize_feedback_fs_div,
    _normalize_role_symbol_scopes,
    build_weekly_policy_rebind_idempotency_key,
    feedback_retry_run_is_complete_for_weekly_policy_rebind,
    find_feedback_retry_run_by_idempotency_key,
    agent_feedback_retry_service,
    resolve_latest_completed_summary_knowledge_cutoff,
)
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
from app.agent_context.run_modes import (
    FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION,
    FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION,
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)
from app.agent_context.run_state_service import (
    agent_context_feedback_run_state_service,
    resolve_effective_decider_scope_authority_from_runs,
)
from app.agent_context.signal_decider_weekly_learning_service import (
    signal_decider_weekly_learning_service,
)
from app.market.clock import get_market_state
from app.eod.summary_cutoff_contract import (
    COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION,
)
from app.market.session import (
    latest_krx_trading_date,
    next_krx_trading_date,
    previous_krx_trading_date,
)


class WeeklyPolicyRebindPreparationService:
    """Prepare an immutable, no-order weekly policy rebind request."""

    def __init__(
        self,
        *,
        market_state_provider: Callable[[], Any] = get_market_state,
        latest_trading_date_resolver: Callable[[date], date | None] = (
            latest_krx_trading_date
        ),
        next_trading_date_resolver: Callable[[date], date | None] = (
            next_krx_trading_date
        ),
        previous_trading_date_resolver: Callable[[date], date | None] = (
            previous_krx_trading_date
        ),
        weekly_learning_service=signal_decider_weekly_learning_service,
        run_state_service=agent_context_feedback_run_state_service,
        summary_cutoff_resolver: Callable[..., dict[str, Any]] = (
            resolve_latest_completed_summary_knowledge_cutoff
        ),
        idempotent_run_resolver: Callable[..., dict[str, Any] | None] = (
            find_feedback_retry_run_by_idempotency_key
        ),
        retry_service=agent_feedback_retry_service,
    ) -> None:
        self.market_state_provider = market_state_provider
        self.latest_trading_date_resolver = latest_trading_date_resolver
        self.next_trading_date_resolver = next_trading_date_resolver
        self.previous_trading_date_resolver = previous_trading_date_resolver
        self.weekly_learning_service = weekly_learning_service
        self.run_state_service = run_state_service
        self.summary_cutoff_resolver = summary_cutoff_resolver
        self.idempotent_run_resolver = idempotent_run_resolver
        self.retry_service = retry_service

    @staticmethod
    def _base_response(*, guards: dict[str, Any]) -> dict[str, Any]:
        return {
            'status': 'blocked',
            'reason': None,
            'artifact_version': None,
            'content_sha256': None,
            'artifact_week_start': None,
            'artifact_week_end': None,
            'source_market_date': None,
            'target_trade_date': None,
            'policy_context_as_of_date': None,
            'source_summary_run_id': None,
            'knowledge_cutoff_at': None,
            'symbols': [],
            'roles': list(DECISION_SUPPORT_AGENT_NAMES),
            'role_symbol_scopes': None,
            'feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            'fs_div': None,
            'completed_scope_proof': None,
            'idempotency_key': None,
            'scope_authority': None,
            'retry_request': None,
            'guards': guards,
        }

    @staticmethod
    def _blocked(
        response: dict[str, Any],
        *,
        reason: str,
    ) -> dict[str, Any]:
        response['status'] = 'blocked'
        response['reason'] = reason
        return response

    def _resolve_effective_decider_authority(
        self,
        *,
        market_date: date,
    ) -> tuple[Mapping[str, Any] | None, dict[str, list[str]] | None, str | None]:
        list_runs = getattr(self.run_state_service, 'list_runs', None)
        if callable(list_runs):
            runs = list_runs(market_date=market_date)
        else:
            latest = self.run_state_service.find_latest_run(
                market_date=market_date,
                requested_roles=list(DECISION_SUPPORT_AGENT_NAMES),
                request_modes=[DAILY_RETRY_CYCLE_MODE],
                feedback_scope_mode=(
                    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                ),
            )
            runs = [latest] if isinstance(latest, Mapping) else []

        resolved = resolve_effective_decider_scope_authority_from_runs(
            [dict(run) for run in runs if isinstance(run, Mapping)]
        )
        if resolved.get('ready') is not True:
            return None, None, str(
                resolved.get('reason') or 'decider_scope_authority_missing'
            )
        return (
            resolved.get('base_parent'),
            resolved.get('role_symbol_scopes'),
            None,
        )

    def prepare(
        self,
        *,
        as_of_date: date,
        market: str = 'KRX',
        target_trade_date: date | None = None,
        require_saturday: bool = True,
    ) -> dict[str, Any]:
        normalized_market = str(market or '').strip().upper()
        market_state = self.market_state_provider()
        server_date_raw = getattr(market_state, 'current_calendar_date', None)
        try:
            server_date = date.fromisoformat(str(server_date_raw or ''))
        except ValueError:
            server_date = None
        current_session_open = bool(
            getattr(market_state, 'is_open', False)
        ) or str(getattr(market_state, 'state', '') or '').upper() == 'OPEN'
        guards: dict[str, Any] = {
            'market_is_krx': normalized_market == 'KRX',
            'as_of_date_matches_server': server_date == as_of_date,
            'saturday_window': as_of_date.weekday() == 5,
            'current_session_not_open': not current_session_open,
            'target_session_state': None,
            'target_session_not_open': False,
            'artifact_effective': False,
            'source_is_latest_completed_trading_day': False,
            'source_is_week_end': False,
            'source_is_in_artifact_week': False,
            'knowledge_cutoff_exact': False,
            'completed_scope_verified': False,
            'calendar_source': getattr(
                market_state, 'calendar_source', None
            ),
            'calendar_source_status': getattr(
                market_state, 'calendar_source_status', None
            ),
        }
        response = self._base_response(guards=guards)
        if normalized_market != 'KRX':
            return self._blocked(response, reason='unsupported_market')
        if server_date != as_of_date:
            return self._blocked(response, reason='as_of_date_not_current')
        if require_saturday and as_of_date.weekday() != 5:
            return self._blocked(response, reason='not_saturday_window')
        if current_session_open:
            return self._blocked(response, reason='market_session_open')

        resolved_target_trade_date = target_trade_date
        source_market_date = (
            self.previous_trading_date_resolver(
                resolved_target_trade_date
            )
            if resolved_target_trade_date is not None
            else self.latest_trading_date_resolver(as_of_date)
        )
        if source_market_date is None:
            return self._blocked(
                response,
                reason='latest_completed_trading_day_unavailable',
            )
        response['source_market_date'] = source_market_date.isoformat()
        expected_latest_completed = (
            self.previous_trading_date_resolver(as_of_date)
            if resolved_target_trade_date == as_of_date
            and str(getattr(market_state, 'state', '')).upper()
            == 'PREOPEN'
            else self.latest_trading_date_resolver(as_of_date)
        )
        guards['source_is_latest_completed_trading_day'] = (
            source_market_date == expected_latest_completed
        )
        if not guards['source_is_latest_completed_trading_day']:
            return self._blocked(
                response,
                reason='source_is_not_latest_completed_trading_day',
            )
        resolved_target_trade_date = (
            resolved_target_trade_date
            or self.next_trading_date_resolver(source_market_date)
        )
        if resolved_target_trade_date is None:
            return self._blocked(
                response,
                reason='target_trade_date_unavailable',
            )
        response['target_trade_date'] = resolved_target_trade_date.isoformat()
        response['policy_context_as_of_date'] = (
            resolved_target_trade_date.isoformat()
        )
        guards['target_session_state'] = (
            'future'
            if resolved_target_trade_date > as_of_date
            else str(getattr(market_state, 'state', '')).lower()
        )
        guards['target_session_not_open'] = bool(
            resolved_target_trade_date > as_of_date
            or (
                resolved_target_trade_date == as_of_date
                and str(getattr(market_state, 'state', '')).upper()
                == 'PREOPEN'
                and not bool(getattr(market_state, 'is_open', False))
            )
        )
        if not guards['target_session_not_open']:
            return self._blocked(response, reason='target_session_not_future')

        latest_policy = self.weekly_learning_service.get_latest_published(
            as_of_date=resolved_target_trade_date
        )
        artifact = (
            latest_policy.get('artifact')
            if isinstance(latest_policy, Mapping)
            and latest_policy.get('available') is True
            and isinstance(latest_policy.get('artifact'), Mapping)
            else None
        )
        if artifact is None:
            response['reason'] = str(
                latest_policy.get('reason')
                if isinstance(latest_policy, Mapping)
                else ''
            ) or 'weekly_policy_artifact_unavailable'
            return response
        response['artifact_version'] = artifact.get('artifact_version')
        response['content_sha256'] = artifact.get('content_sha256')
        response['artifact_week_start'] = artifact.get('week_start')
        response['artifact_week_end'] = artifact.get('week_end')
        try:
            artifact_week_start = date.fromisoformat(
                str(artifact.get('week_start'))
            )
            artifact_week_end = date.fromisoformat(
                str(artifact.get('week_end'))
            )
            artifact_effective_from = date.fromisoformat(
                str(artifact.get('effective_from'))
            )
            artifact_valid_through = date.fromisoformat(
                str(artifact.get('valid_through'))
            )
        except (TypeError, ValueError):
            return self._blocked(
                response,
                reason='weekly_policy_artifact_dates_invalid',
            )
        latest_artifact_week_trading_date = (
            self.latest_trading_date_resolver(artifact_week_end)
        )
        guards['source_is_week_end'] = bool(
            latest_artifact_week_trading_date == source_market_date
        )
        guards['source_is_in_artifact_week'] = bool(
            artifact_week_start <= source_market_date <= artifact_week_end
        )
        artifact_source_compatible = bool(
            artifact_week_end <= source_market_date
            or (
                artifact_week_end > source_market_date
                and latest_artifact_week_trading_date == source_market_date
                and guards['source_is_in_artifact_week']
            )
        )
        artifact_effective = bool(
            artifact_source_compatible
            and artifact_effective_from
            <= resolved_target_trade_date
            <= artifact_valid_through
        )
        guards['artifact_effective'] = artifact_effective
        if require_saturday and not guards['source_is_week_end']:
            return self._blocked(response, reason='source_is_not_week_end')
        if not artifact_effective:
            return self._blocked(
                response,
                reason='weekly_policy_artifact_source_mismatch',
            )

        (
            decider_parent,
            effective_role_symbol_scopes,
            decider_scope_error,
        ) = self._resolve_effective_decider_authority(
            market_date=source_market_date,
        )
        if decider_scope_error is not None:
            return self._blocked(response, reason=decider_scope_error)
        decider_request = (
            decider_parent.get('request_payload')
            if isinstance(decider_parent, Mapping)
            and isinstance(decider_parent.get('request_payload'), Mapping)
            else {}
        )
        persisted_authority = (
            decider_request.get('scope_authority')
            if isinstance(decider_request.get('scope_authority'), Mapping)
            else None
        )
        if persisted_authority is None:
            return self._blocked(
                response,
                reason='decider_scope_authority_missing',
            )
        source_summary_run_id = str(
            persisted_authority.get('source_summary_run_id') or ''
        ).strip()
        source_feedback_scope_mode = str(
            persisted_authority.get('source_feedback_scope_mode') or ''
        ).strip()
        if not source_summary_run_id or source_feedback_scope_mode not in {
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
        }:
            return self._blocked(
                response,
                reason='decider_scope_authority_invalid',
            )
        source_run = self.run_state_service.get_run(
            run_id=source_summary_run_id
        )
        if not isinstance(source_run, Mapping):
            return self._blocked(response, reason='summary_run_missing')
        raw_fs_div = persisted_authority.get('fs_div')
        if not isinstance(raw_fs_div, str) or not raw_fs_div.strip():
            return self._blocked(response, reason='summary_run_fs_div_missing')
        fs_div = _normalize_feedback_fs_div(raw_fs_div)
        exact_summary = self.summary_cutoff_resolver(
            market_date=source_market_date,
            feedback_scope_mode=source_feedback_scope_mode,
            fs_div=fs_div,
            source_run_id=source_summary_run_id,
        )
        response['source_summary_run_id'] = source_summary_run_id
        if exact_summary.get('ready') is not True:
            return self._blocked(
                response,
                reason=str(
                    exact_summary.get('reason')
                    or 'summary_scope_proof_invalid'
                ),
            )
        symbols = exact_summary.get('requested_symbols')
        completed_scope_proof = exact_summary.get('completed_scope_proof')
        cutoff = exact_summary.get('knowledge_cutoff_at')
        if not (
            isinstance(symbols, list)
            and symbols
            and isinstance(completed_scope_proof, Mapping)
            and cutoff is not None
        ):
            return self._blocked(
                response,
                reason='summary_scope_proof_incomplete',
            )
        raw_completed_symbols = completed_scope_proof.get(
            'completed_symbols'
        )
        raw_admitted_symbols = completed_scope_proof.get(
            'admitted_symbols'
        )
        normalized_symbols = sorted(
            {
                str(symbol or '').strip().upper()
                for symbol in symbols
                if str(symbol or '').strip()
            }
        )
        normalized_completed_symbols = sorted(
            {
                str(symbol or '').strip().upper()
                for symbol in raw_completed_symbols or []
                if str(symbol or '').strip()
            }
        )
        normalized_admitted_symbols = sorted(
            {
                str(symbol or '').strip().upper()
                for symbol in raw_admitted_symbols or []
                if str(symbol or '').strip()
            }
        )
        if not (
            completed_scope_proof.get('schema_version')
            == COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION
            and isinstance(raw_completed_symbols, list)
            and isinstance(raw_admitted_symbols, list)
            and len(normalized_symbols) == len(symbols)
            and len(normalized_completed_symbols)
            == len(raw_completed_symbols)
            and len(normalized_admitted_symbols)
            == len(raw_admitted_symbols)
            and normalized_completed_symbols == normalized_symbols
            and set(normalized_symbols).issubset(
                normalized_admitted_symbols
            )
        ):
            return self._blocked(
                response,
                reason='summary_scope_proof_invalid',
            )
        symbols = normalized_symbols
        completed_scope_proof = {
            'schema_version': (
                COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION
            ),
            'completed_symbols': normalized_completed_symbols,
            'admitted_symbols': normalized_admitted_symbols,
        }
        try:
            role_symbol_scopes = _normalize_role_symbol_scopes(
                effective_role_symbol_scopes,
                roles=DECISION_SUPPORT_AGENT_NAMES,
                requested_symbols=symbols,
            )
        except ValueError:
            role_symbol_scopes = None
        if role_symbol_scopes is None:
            return self._blocked(
                response,
                reason='decider_role_symbol_scopes_invalid',
            )
        cutoff_iso = cutoff.isoformat()
        if (
            str(persisted_authority.get('knowledge_cutoff_at') or '').strip()
            != cutoff_iso
        ):
            return self._blocked(
                response,
                reason='decider_summary_cutoff_mismatch',
            )
        guards['knowledge_cutoff_exact'] = True
        guards['completed_scope_verified'] = True
        response.update(
            {
                'knowledge_cutoff_at': cutoff_iso,
                'symbols': list(symbols),
                'role_symbol_scopes': role_symbol_scopes,
                'fs_div': fs_div,
                'completed_scope_proof': dict(completed_scope_proof),
            }
        )
        idempotency_key = build_weekly_policy_rebind_idempotency_key(
            artifact_version=str(artifact['artifact_version']),
            content_sha256=str(artifact['content_sha256']),
            source_summary_run_id=source_summary_run_id,
            source_market_date=source_market_date,
            target_trade_date=resolved_target_trade_date,
            policy_context_as_of_date=resolved_target_trade_date,
            knowledge_cutoff_at=cutoff_iso,
            symbols=symbols,
            roles=DECISION_SUPPORT_AGENT_NAMES,
            role_symbol_scopes=role_symbol_scopes,
            fs_div=fs_div,
        )
        policy_binding = {
            'schema_version': WEEKLY_POLICY_REBIND_SCHEMA_VERSION,
            'artifact_version': artifact['artifact_version'],
            'content_sha256': artifact['content_sha256'],
            'source_market_date': source_market_date.isoformat(),
            'target_trade_date': resolved_target_trade_date.isoformat(),
            'policy_context_as_of_date': (
                resolved_target_trade_date.isoformat()
            ),
            'idempotency_key': idempotency_key,
        }
        scope_authority = {
            'schema_version': FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION,
            'kind': (
                FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION
            ),
            'source_summary_run_id': source_summary_run_id,
            'source_feedback_scope_mode': (
                source_feedback_scope_mode
            ),
            'decision_feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            'knowledge_cutoff_at': cutoff_iso,
            'fs_div': fs_div,
            'completed_scope_proof': dict(completed_scope_proof),
            'completed_symbols': normalized_completed_symbols,
            'admitted_symbol_count': len(normalized_admitted_symbols),
            'admitted_symbols_sha256': hashlib.sha256(
                '\n'.join(normalized_admitted_symbols).encode('utf-8')
            ).hexdigest(),
            'decider_role_symbol_scopes': role_symbol_scopes,
            'policy_binding': policy_binding,
        }
        retry_request = {
            'market_date': source_market_date.isoformat(),
            'policy_context_as_of_date': (
                resolved_target_trade_date.isoformat()
            ),
            'symbols': list(symbols),
            'roles': list(DECISION_SUPPORT_AGENT_NAMES),
            'role_symbol_scopes': role_symbol_scopes,
            'knowledge_cutoff_at': cutoff_iso,
            'feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            'fs_div': fs_div,
            'max_cycles': 3,
            'outer_batch_size': 20,
            'reuse_existing': True,
            'force': False,
            'force_recompute': True,
            'idempotency_key': idempotency_key,
            'scope_authority': scope_authority,
        }
        response.update(
            {
                'status': 'ready',
                'reason': 'weekly_policy_rebind_ready',
                'idempotency_key': idempotency_key,
                'scope_authority': scope_authority,
                'retry_request': retry_request,
            }
        )
        existing = self.idempotent_run_resolver(
            market_date=source_market_date,
            idempotency_key=idempotency_key,
        )
        if feedback_retry_run_is_complete_for_weekly_policy_rebind(
            existing,
            idempotency_key=idempotency_key,
            role_symbol_scopes=role_symbol_scopes,
        ):
            response['status'] = 'noop'
            response['reason'] = 'weekly_policy_rebind_already_completed'
            response['completed_run_id'] = existing.get('run_id')
        return response

    def coordinate_for_target(
        self,
        *,
        target_trade_date: date,
    ) -> dict[str, Any]:
        """Recompute both deciders before open without touching orders."""

        market_state = self.market_state_provider()
        try:
            as_of_date = date.fromisoformat(
                str(getattr(market_state, 'current_calendar_date', '') or '')
            )
        except ValueError:
            return {
                'status': 'blocked',
                'reason': 'current_market_date_unavailable',
            }
        prepared = self.prepare(
            as_of_date=as_of_date,
            target_trade_date=target_trade_date,
            require_saturday=False,
        )
        if prepared.get('status') != 'ready':
            return prepared
        retry_request = prepared.get('retry_request')
        if not isinstance(retry_request, Mapping):
            return {
                **prepared,
                'status': 'blocked',
                'reason': 'retry_request_missing',
            }

        def assert_session_not_open(**_: Any) -> None:
            current = self.market_state_provider()
            if (
                date.fromisoformat(
                    str(
                        getattr(current, 'current_calendar_date', '') or ''
                    )
                )
                == target_trade_date
                and (
                    bool(getattr(current, 'is_open', False))
                    or str(getattr(current, 'state', '')).upper()
                    != 'PREOPEN'
                )
            ):
                raise RuntimeError(
                    'target KRX session is no longer PREOPEN'
                )

        config = FeedbackRetryCycleConfig(
            market_date=date.fromisoformat(str(retry_request['market_date'])),
            policy_context_as_of_date=date.fromisoformat(
                str(retry_request['policy_context_as_of_date'])
            ),
            symbols=tuple(retry_request['symbols']),
            roles=tuple(retry_request['roles']),
            knowledge_cutoff_at=str(retry_request['knowledge_cutoff_at']),
            feedback_scope_mode=str(
                retry_request['feedback_scope_mode']
            ),
            fs_div=str(retry_request['fs_div']),
            max_cycles=int(retry_request['max_cycles']),
            outer_batch_size=int(retry_request['outer_batch_size']),
            reuse_existing=bool(retry_request['reuse_existing']),
            force_recompute=bool(retry_request['force_recompute']),
            idempotency_key=str(retry_request['idempotency_key']),
            scope_authority=retry_request['scope_authority'],
            role_symbol_scopes=retry_request['role_symbol_scopes'],
            progress_callback=assert_session_not_open,
        )
        try:
            retry_result = self.retry_service.run_retry_cycles(
                config=config,
                force=False,
            )
        except Exception as exc:  # noqa: BLE001
            return {
                **prepared,
                'status': 'blocked',
                'reason': 'weekly_policy_rebind_failed',
                'error_type': type(exc).__name__,
            }
        completed = bool(
            retry_result.get('completed') is True
            and str(retry_result.get('status') or '').lower() == 'completed'
        )
        return {
            **prepared,
            'status': 'completed' if completed else 'blocked',
            'reason': (
                'weekly_policy_rebind_completed'
                if completed
                else 'weekly_policy_rebind_incomplete'
            ),
            'retry_result': retry_result,
        }


weekly_policy_rebind_preparation_service = (
    WeeklyPolicyRebindPreparationService()
)
