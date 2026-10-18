from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
from app.agent_context.retry_service import (
    _normalize_feedback_scope_authority,
)
from app.agent_context.run_modes import (
    DAILY_RETRY_CYCLE_MODE,
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)
from app.agent_context.weekly_policy_rebind_service import (
    WeeklyPolicyRebindPreparationService,
)


def _symbols(count: int = 97) -> list[str]:
    return [f'{index:06d}' for index in range(1, count + 1)]


class _WeeklyLearningService:
    def __init__(
        self,
        *,
        week_end: date,
        effective_from: date,
        valid_through: date,
    ) -> None:
        self.artifact = {
            'artifact_version': f'weekly-{week_end.isoformat()}',
            'content_sha256': 'a' * 64,
            'week_start': (week_end - timedelta(days=4)).isoformat(),
            'week_end': week_end.isoformat(),
            'effective_from': effective_from.isoformat(),
            'valid_through': valid_through.isoformat(),
        }
        self.as_of_dates: list[date] = []

    def get_latest_published(self, *, as_of_date: date) -> dict:
        self.as_of_dates.append(as_of_date)
        return {'available': True, 'artifact': dict(self.artifact)}


class _RunStateService:
    def __init__(
        self,
        *,
        symbols: list[str],
        cutoff: datetime,
    ) -> None:
        self.source_run_id = 'feedback-cycle-2026-10-16-summary'
        self.role_symbol_scopes = {
            role: list(symbols[:80]) for role in DECISION_SUPPORT_AGENT_NAMES
        }
        self.decider_parent = {
            'run_id': 'feedback-cycle-2026-10-16-deciders',
            'request_payload': {
                'mode': DAILY_RETRY_CYCLE_MODE,
                'roles': list(DECISION_SUPPORT_AGENT_NAMES),
                'scope': 'requested_symbols_scored_by_strategy_aware_prefilter',
                'feedback_scope_mode': (
                    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                ),
                'role_symbol_scopes': self.role_symbol_scopes,
                'scope_authority': {
                    'source_summary_run_id': self.source_run_id,
                    'source_feedback_scope_mode': (
                        FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
                    ),
                    'knowledge_cutoff_at': cutoff.isoformat(),
                    'fs_div': 'CFS',
                },
            },
        }
        self.runs = [self.decider_parent]
        self.find_calls: list[dict] = []

    def find_latest_run(self, **kwargs) -> dict:
        self.find_calls.append(kwargs)
        return self.decider_parent

    def list_runs(self, **kwargs) -> list[dict]:
        self.find_calls.append(kwargs)
        return list(self.runs)

    def get_run(self, *, run_id: str) -> dict | None:
        if run_id != self.source_run_id:
            return None
        return {'run_id': run_id, 'status': 'completed'}


class _RetryService:
    def __init__(self) -> None:
        self.calls: list[tuple[object, bool]] = []

    def run_retry_cycles(self, *, config, force: bool) -> dict:
        self.calls.append((config, force))
        return {
            'status': 'completed',
            'completed': True,
            'run_id': 'feedback-cycle-policy-rebind',
        }


class _ProgressCheckingRetryService(_RetryService):
    def run_retry_cycles(self, *, config, force: bool) -> dict:
        self.calls.append((config, force))
        assert config.progress_callback is not None
        config.progress_callback(stage='before_dispatch')
        return {'status': 'completed', 'completed': True}


def _market_state(
    value: date,
    *,
    state: str = 'weekend',
    is_open: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        current_calendar_date=value.isoformat(),
        state=state,
        is_open=is_open,
        calendar_source='test_calendar',
        calendar_source_status='ok',
    )


def _service(
    *,
    as_of_date: date,
    source_market_date: date,
    target_trade_date: date,
    artifact_week_end: date,
    market_state: SimpleNamespace | None = None,
    market_state_provider=None,
    retry_service: _RetryService | None = None,
) -> tuple[WeeklyPolicyRebindPreparationService, _RunStateService]:
    symbols = _symbols()
    cutoff = datetime(
        source_market_date.year,
        source_market_date.month,
        source_market_date.day,
        7,
        0,
        tzinfo=timezone.utc,
    )
    run_state_service = _RunStateService(symbols=symbols, cutoff=cutoff)
    weekly_service = _WeeklyLearningService(
        week_end=artifact_week_end,
        effective_from=target_trade_date,
        valid_through=date(2026, 10, 31),
    )

    def summary_cutoff_resolver(**kwargs) -> dict:
        assert kwargs == {
            'market_date': source_market_date,
            'feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
            ),
            'fs_div': 'CFS',
            'source_run_id': run_state_service.source_run_id,
        }
        return {
            'ready': True,
            'requested_symbols': list(symbols),
            'knowledge_cutoff_at': cutoff,
            'completed_scope_proof': {
                'schema_version': 'completed_summarizer_intersection_v1',
                'completed_symbols': list(symbols),
                'admitted_symbols': list(symbols),
            },
        }

    return (
        WeeklyPolicyRebindPreparationService(
            market_state_provider=(
                market_state_provider
                or (lambda: market_state or _market_state(as_of_date))
            ),
            latest_trading_date_resolver=lambda _: source_market_date,
            next_trading_date_resolver=lambda _: target_trade_date,
            previous_trading_date_resolver=lambda _: source_market_date,
            weekly_learning_service=weekly_service,
            run_state_service=run_state_service,
            summary_cutoff_resolver=summary_cutoff_resolver,
            idempotent_run_resolver=lambda **_: None,
            retry_service=retry_service or _RetryService(),
        ),
        run_state_service,
    )


def test_prepare_saturday_binds_exact_friday_cohort_and_decider_scopes() -> None:
    service, run_state = _service(
        as_of_date=date(2026, 10, 17),
        source_market_date=date(2026, 10, 16),
        target_trade_date=date(2026, 10, 19),
        artifact_week_end=date(2026, 10, 16),
    )

    result = service.prepare(as_of_date=date(2026, 10, 17))

    assert result['status'] == 'ready'
    assert result['source_market_date'] == '2026-10-16'
    assert result['target_trade_date'] == '2026-10-19'
    assert result['policy_context_as_of_date'] == '2026-10-19'
    assert result['symbols'] == _symbols()
    assert result['roles'] == list(DECISION_SUPPORT_AGENT_NAMES)
    assert result['retry_request']['role_symbol_scopes'] == (
        run_state.role_symbol_scopes
    )
    assert all(
        len(scope) == 80
        for scope in result['retry_request']['role_symbol_scopes'].values()
    )
    assert result['retry_request']['symbols'] == _symbols()
    assert result['retry_request']['scope_authority'][
        'source_summary_run_id'
    ] == run_state.source_run_id
    assert result['retry_request']['scope_authority']['policy_binding'][
        'idempotency_key'
    ] == result['idempotency_key']
    assert result['guards'] == {
        **result['guards'],
        'artifact_effective': True,
        'source_is_latest_completed_trading_day': True,
        'source_is_week_end': True,
        'knowledge_cutoff_exact': True,
        'completed_scope_verified': True,
    }


def test_prepare_preserves_broad_scope_under_newer_targeted_patch() -> None:
    service, run_state = _service(
        as_of_date=date(2026, 10, 17),
        source_market_date=date(2026, 10, 16),
        target_trade_date=date(2026, 10, 19),
        artifact_week_end=date(2026, 10, 16),
    )
    broad_symbols = ['000001', '000002']
    broad_scopes = {
        role: list(broad_symbols) for role in DECISION_SUPPORT_AGENT_NAMES
    }
    run_state.role_symbol_scopes = broad_scopes
    run_state.decider_parent['request_payload'][
        'role_symbol_scopes'
    ] = broad_scopes
    targeted_patch = {
        'run_id': 'feedback-cycle-2026-10-16-targeted-patch',
        'request_payload': {
            'mode': DAILY_RETRY_CYCLE_MODE,
            'roles': list(DECISION_SUPPORT_AGENT_NAMES),
            'scope': 'requested_symbols_scored_by_strategy_aware_prefilter',
            'feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            ),
            'requested_symbols': ['000001'],
            'role_symbol_scopes': {
                role: ['000001']
                for role in DECISION_SUPPORT_AGENT_NAMES
            },
        },
    }
    unrelated_broad_run = {
        'run_id': 'feedback-cycle-2026-10-16-unrelated-broad',
        'request_payload': {
            'mode': DAILY_RETRY_CYCLE_MODE,
            'roles': list(DECISION_SUPPORT_AGENT_NAMES),
            'scope': 'active_strategy_aware_prefilter_scoring',
            'feedback_scope_mode': (
                FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
            ),
            'role_symbol_scopes': {
                role: ['000003']
                for role in DECISION_SUPPORT_AGENT_NAMES
            },
        },
    }
    run_state.runs = [
        unrelated_broad_run,
        targeted_patch,
        run_state.decider_parent,
    ]

    result = service.prepare(as_of_date=date(2026, 10, 17))

    assert result['status'] == 'ready'
    assert result['source_summary_run_id'] == run_state.source_run_id
    assert result['retry_request']['role_symbol_scopes'] == broad_scopes
    assert result['scope_authority']['decider_role_symbol_scopes'] == (
        broad_scopes
    )
    retry_request = result['retry_request']
    latest = {
        'available': True,
        'artifact': {
            'artifact_version': result['artifact_version'],
            'content_sha256': result['content_sha256'],
            'week_start': result['artifact_week_start'],
            'week_end': result['artifact_week_end'],
        },
    }
    with (
        patch(
            'app.agent_context.retry_service.signal_decider_weekly_learning_service.get_latest_published',
            return_value=latest,
        ),
        patch(
            'app.agent_context.retry_service.agent_context_feedback_run_state_service',
            run_state,
        ),
        patch(
            'app.agent_context.retry_service.resolve_latest_completed_summary_knowledge_cutoff',
            side_effect=service.summary_cutoff_resolver,
        ),
    ):
        normalized = _normalize_feedback_scope_authority(
            retry_request['scope_authority'],
            market_date=date(2026, 10, 16),
            policy_context_as_of_date=date(2026, 10, 19),
            roles=retry_request['roles'],
            requested_symbols=retry_request['symbols'],
            role_symbol_scopes=retry_request['role_symbol_scopes'],
            feedback_scope_mode=retry_request['feedback_scope_mode'],
            knowledge_cutoff_at=retry_request['knowledge_cutoff_at'],
            fs_div=retry_request['fs_div'],
        )
    assert normalized == retry_request['scope_authority']


def test_prepare_scope_authority_is_retry_canonical_without_shape_drift() -> None:
    target = date(2026, 10, 19)
    source = date(2026, 10, 16)
    service, _ = _service(
        as_of_date=date(2026, 10, 17),
        source_market_date=source,
        target_trade_date=target,
        artifact_week_end=source,
    )
    prepared = service.prepare(as_of_date=date(2026, 10, 17))
    retry_request = prepared['retry_request']
    latest = {
        'available': True,
        'artifact': {
            'artifact_version': prepared['artifact_version'],
            'content_sha256': prepared['content_sha256'],
            'week_start': prepared['artifact_week_start'],
            'week_end': prepared['artifact_week_end'],
        },
    }

    with (
        patch(
            'app.agent_context.retry_service.signal_decider_weekly_learning_service.get_latest_published',
            return_value=latest,
        ),
        patch(
            'app.agent_context.retry_service._validate_weekly_policy_rebind_authority_against_persisted_state'
        ),
    ):
        normalized = _normalize_feedback_scope_authority(
            retry_request['scope_authority'],
            market_date=source,
            policy_context_as_of_date=target,
            roles=retry_request['roles'],
            requested_symbols=retry_request['symbols'],
            role_symbol_scopes=retry_request['role_symbol_scopes'],
            feedback_scope_mode=retry_request['feedback_scope_mode'],
            knowledge_cutoff_at=retry_request['knowledge_cutoff_at'],
            fs_div=retry_request['fs_div'],
        )

    assert normalized == retry_request['scope_authority']


def test_prepare_preopen_accepts_tuesday_artifact_for_wednesday_source() -> None:
    target = date(2026, 10, 22)
    service, _ = _service(
        as_of_date=target,
        source_market_date=date(2026, 10, 21),
        target_trade_date=target,
        artifact_week_end=date(2026, 10, 20),
        market_state=_market_state(target, state='PREOPEN'),
    )

    result = service.prepare(
        as_of_date=target,
        target_trade_date=target,
        require_saturday=False,
    )

    assert result['status'] == 'ready'
    assert result['source_market_date'] == '2026-10-21'
    assert result['target_trade_date'] == '2026-10-22'
    assert result['guards']['artifact_effective'] is True
    assert result['guards']['target_session_not_open'] is True


def test_prepare_saturday_accepts_thursday_source_when_friday_is_holiday() -> None:
    service, _ = _service(
        as_of_date=date(2026, 10, 17),
        source_market_date=date(2026, 10, 15),
        target_trade_date=date(2026, 10, 19),
        artifact_week_end=date(2026, 10, 16),
    )

    result = service.prepare(as_of_date=date(2026, 10, 17))

    assert result['status'] == 'ready'
    assert result['source_market_date'] == '2026-10-15'
    assert result['artifact_week_end'] == '2026-10-16'
    assert result['guards']['source_is_latest_completed_trading_day'] is True
    assert result['guards']['source_is_week_end'] is True
    assert result['guards']['source_is_in_artifact_week'] is True


def test_prepare_blocks_open_target_before_reading_learning_authority() -> None:
    target = date(2026, 10, 22)
    service, run_state = _service(
        as_of_date=target,
        source_market_date=date(2026, 10, 21),
        target_trade_date=target,
        artifact_week_end=date(2026, 10, 20),
        market_state=_market_state(target, state='OPEN', is_open=False),
    )

    result = service.prepare(
        as_of_date=target,
        target_trade_date=target,
        require_saturday=False,
    )

    assert result['status'] == 'blocked'
    assert result['reason'] == 'market_session_open'
    assert run_state.find_calls == []


def test_coordinate_preopen_recomputes_only_exact_decider_role_scopes() -> None:
    target = date(2026, 10, 22)
    retry_service = _RetryService()
    service, run_state = _service(
        as_of_date=target,
        source_market_date=date(2026, 10, 21),
        target_trade_date=target,
        artifact_week_end=date(2026, 10, 20),
        market_state=_market_state(target, state='PREOPEN'),
        retry_service=retry_service,
    )

    result = service.coordinate_for_target(target_trade_date=target)

    assert result['status'] == 'completed'
    assert len(retry_service.calls) == 1
    config, force = retry_service.calls[0]
    assert force is False
    assert config.market_date == date(2026, 10, 21)
    assert config.policy_context_as_of_date == target
    assert config.roles == DECISION_SUPPORT_AGENT_NAMES
    assert config.role_symbol_scopes == run_state.role_symbol_scopes
    assert config.force_recompute is True
    assert config.reuse_existing is True


def test_coordinate_blocks_if_target_session_opens_during_retry() -> None:
    target = date(2026, 10, 22)
    states = [
        _market_state(target, state='PREOPEN'),
        _market_state(target, state='PREOPEN'),
        _market_state(target, state='OPEN', is_open=True),
    ]

    def market_state_provider() -> SimpleNamespace:
        return states.pop(0) if len(states) > 1 else states[0]

    retry_service = _ProgressCheckingRetryService()
    service, _ = _service(
        as_of_date=target,
        source_market_date=date(2026, 10, 21),
        target_trade_date=target,
        artifact_week_end=date(2026, 10, 20),
        market_state_provider=market_state_provider,
        retry_service=retry_service,
    )

    result = service.coordinate_for_target(target_trade_date=target)

    assert result['status'] == 'blocked'
    assert result['reason'] == 'weekly_policy_rebind_failed'
    assert result['error_type'] == 'RuntimeError'
