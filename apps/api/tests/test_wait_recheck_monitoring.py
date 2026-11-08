from __future__ import annotations

from datetime import date, datetime, timezone
from inspect import getsource, signature
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context import wait_recheck_service as wait_recheck_module
from app.agent_context import orchestrator as orchestrator_module
from app.agent_context import orchestrator_helpers
from app.agent_context.wait_recheck_service import (
    SignalDeciderWaitRecheckService,
)
from app.quotes.kis_realtime_prices import KISRealtimePriceTick


def _tick(
    *,
    symbol: str = '005930',
    observed_at: datetime | None = None,
) -> KISRealtimePriceTick:
    received_at = observed_at or datetime(
        2026,
        10,
        21,
        0,
        5,
        tzinfo=timezone.utc,
    )
    return KISRealtimePriceTick(
        symbol=symbol,
        current_price=71_000,
        price_as_of=received_at,
        received_at=received_at,
        change=100,
        change_pct=0.14,
        volume=100_000,
        turnover=7_000_000_000,
        bid_price=70_900,
        ask_price=71_000,
        trade_halt=False,
        raw_fields={},
    )


def test_trigger_touch_never_calls_a_decider() -> None:
    service = SignalDeciderWaitRecheckService()
    tick = _tick()

    with patch(
        'app.agent_context.orchestrator.agent_feedback_orchestrator.'
        'dispatch_for_symbols'
    ) as dispatch_for_symbols:
        touched = service.observe_realtime_tick(
            tick,
            subscribed=True,
            limited=False,
            now=tick.received_at,
        )
        cycle_result = service.run_open_cycle(
            market=SimpleNamespace(
                market='KRX',
                state='OPEN',
                is_trading_day=True,
                is_open=True,
                server_time=tick.received_at.isoformat(),
            )
        )

    assert touched is False
    dispatch_for_symbols.assert_not_called()
    assert cycle_result == {
        'status': 'disabled',
        'events_processed': 0,
        'results': [],
        'follow_up_required': False,
        'direct_order_authority': False,
    }


def test_active_wait_monitor_service_cannot_create_order_authority() -> None:
    service = SignalDeciderWaitRecheckService()

    assert service.runtime_snapshot()['enabled'] is False
    assert service.runtime_snapshot()['direct_order_authority'] is False
    assert not hasattr(service, '_dispatch_event')
    assert not hasattr(service, '_retry_event_ids')
    assert not hasattr(wait_recheck_module, 'wait_recheck_dispatch_completed')
    assert not hasattr(wait_recheck_module, 'session_scope')
    source = service.load_subscription_source(
        target_trade_date=date(2026, 10, 21),
    )
    assert source.symbols == ()
    assert source.membership_complete is True
    assert service.runtime_snapshot()['candidate_count'] == 0


def test_orchestrator_has_no_wait_recheck_overlay_surface() -> None:
    assert 'source_context_overlays' not in signature(
        orchestrator_module.agent_feedback_orchestrator.dispatch_for_symbols
    ).parameters
    assert 'intraday_wait_recheck_context' not in getsource(
        orchestrator_module
    )
    assert 'intraday_wait_recheck_context' not in getsource(
        orchestrator_helpers
    )


def test_historical_wait_events_have_no_runtime_reader() -> None:
    implementation = getsource(wait_recheck_module)

    assert not hasattr(wait_recheck_module, 'load_validated_wait_recheck_context')
    assert not hasattr(wait_recheck_module, 'validate_wait_recheck_event_binding')
    assert 'SignalDeciderWaitRecheckEvent' not in implementation
    assert 'session_scope' not in implementation
