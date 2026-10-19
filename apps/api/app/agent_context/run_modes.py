from __future__ import annotations

DAILY_RETRY_CYCLE_MODE = 'daily_retry_cycle_role_sequenced'
FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH = 'decider_scope_fast_path'
FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE = 'broad_feedback_universe'
FEEDBACK_SCOPE_MODES = (
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
)
FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION = (
    'agent_feedback_scope_authority_v1'
)
FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION = (
    'completed_summary_intersection_authoritative_cohort'
)
OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION = (
    'openclaw_shared_dispatch_circuit_breaker_v1'
)
OPENCLAW_SHARED_DISPATCH_RETRYABLE_FAILURE_KINDS = (
    'openclaw_provider_rate_limited',
    'openclaw_transport_timeout',
    'openclaw_transport_error',
    'openclaw_storage_admission_blocked',
)
OPENCLAW_SHARED_DISPATCH_STOP_FAILURE_KINDS = (
    *OPENCLAW_SHARED_DISPATCH_RETRYABLE_FAILURE_KINDS,
    'openclaw_plugin_state_capacity_exhausted',
)
OPENCLAW_SHARED_DISPATCH_ORCHESTRATOR_SOURCE = (
    'openclaw_orchestrator_shared_dispatch'
)


def normalize_feedback_scope_mode(value: str | None) -> str:
    resolved = str(value or FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH).strip()
    if resolved not in FEEDBACK_SCOPE_MODES:
        raise ValueError(
            'feedback_scope_mode must be one of: '
            + ', '.join(FEEDBACK_SCOPE_MODES)
        )
    return resolved
