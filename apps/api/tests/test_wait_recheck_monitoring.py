from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import delete, select

from app.agent_context.orchestrator import AgentFeedbackOrchestrator
from app.agent_context.orchestrator_helpers import compressed_batch_item_for_prompt
from app.agent_context.wait_recheck_service import (
    WAIT_RECHECK_CONTEXT_SCHEMA_VERSION,
    SignalDeciderWaitRecheckService,
    WaitRecheckObservation,
    build_wait_recheck_candidate,
    canonical_wait_recheck_context,
    merge_wait_recheck_monitored_source,
    load_validated_wait_recheck_context,
    validate_wait_recheck_context,
    wait_recheck_dispatch_completed,
)
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    SignalDeciderDecisionSnapshot,
    SignalDeciderWaitRecheckEvent,
)
from app.quotes.kis_realtime_prices import (
    KISRealtimeMonitoredSymbolSource,
    KISRealtimePriceTick,
    KISRealtimePriceService,
)


def _snapshot(
    *,
    snapshot_id: int = 11,
    symbol: str = '005930',
    action: str = 'wait',
    trigger_price: int | None = 71_000,
    chase_limit_price: int | None = 71_500,
    target_price: int | None = 74_000,
    stop_loss_price: int | None = 69_500,
    upgrade_target_action: str = 'watch_for_trigger',
    upgrade_trigger_price: int | None = 71_000,
) -> SimpleNamespace:
    created_at = datetime(2026, 10, 20, 11, 0, tzinfo=timezone.utc)
    return SimpleNamespace(
        id=snapshot_id,
        market_date=date(2026, 10, 20),
        symbol=symbol,
        agent_name='short-term-decider',
        decision_action=action,
        preferred_action=action,
        next_day_open_action=action,
        content_fingerprint='a' * 64,
        created_at=created_at,
        feedback_received_at=created_at,
        response_payload={
            'tactical_action': action,
            'preferred_action': action,
            'trigger_price': trigger_price,
            'chase_limit_price': chase_limit_price,
            'target_price': target_price,
            'stop_loss_price': stop_loss_price,
            'decision_delta': {
                'upgrade_trigger': {
                    'target_action': upgrade_target_action,
                    'trigger_price': upgrade_trigger_price,
                }
            },
        },
        request_payload={
            'symbol_day_context': {
                'symbol': symbol,
                'market': 'KOSPI',
                'company': {'market': 'KOSPI'},
            },
            'decision_input_generation_fence': {
                'enforcement': 'required',
            },
            'durable_decision_input_generation': {
                'schema_version': 'durable_decider_input_generation_v1',
                'durable_reuse_eligible': True,
            },
        },
    )


def _tick(
    *,
    ask_price: int = 71_000,
    received_at: datetime | None = None,
) -> KISRealtimePriceTick:
    observed_at = received_at or datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc)
    return KISRealtimePriceTick(
        symbol='005930',
        current_price=70_900,
        price_as_of=observed_at,
        received_at=observed_at,
        change=100,
        change_pct=0.14,
        volume=100_000,
        turnover=7_000_000_000,
        bid_price=70_900,
        ask_price=ask_price,
        trade_halt=False,
        raw_fields={},
    )


def test_wait_candidate_requires_explicit_bounded_watch_upgrade() -> None:
    target_trade_date = date(2026, 10, 21)

    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=target_trade_date,
    )

    assert candidate is not None
    assert candidate.symbol == '005930'
    assert candidate.trigger_price == 71_000
    assert candidate.chase_limit_price == 71_500
    assert candidate.target_price == 74_000
    assert candidate.stop_loss_price == 69_500

    candidate_without_source_chase = build_wait_recheck_candidate(
        _snapshot(
            chase_limit_price=None,
            upgrade_trigger_price=None,
        ),
        target_trade_date=target_trade_date,
    )
    assert candidate_without_source_chase is not None
    assert candidate_without_source_chase.chase_limit_price is None

    assert build_wait_recheck_candidate(
        _snapshot(action='reject'),
        target_trade_date=target_trade_date,
    ) is None
    assert build_wait_recheck_candidate(
        _snapshot(upgrade_target_action='scalp_buy_candidate'),
        target_trade_date=target_trade_date,
    ) is None
    assert build_wait_recheck_candidate(
        _snapshot(trigger_price=None, upgrade_trigger_price=None),
        target_trade_date=target_trade_date,
    ) is None
    assert build_wait_recheck_candidate(
        _snapshot(chase_limit_price=70_500),
        target_trade_date=target_trade_date,
    ) is None
    assert build_wait_recheck_candidate(
        _snapshot(upgrade_trigger_price=71_100),
        target_trade_date=target_trade_date,
    ) is None


def test_realtime_tick_queues_once_only_for_fresh_subscribed_ask_inside_band() -> None:
    service = SignalDeciderWaitRecheckService()
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    service.replace_candidate_cache([candidate])
    now = datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc)

    assert service.observe_realtime_tick(
        _tick(ask_price=70_999, received_at=now),
        subscribed=True,
        limited=False,
        now=now,
    ) is False
    assert service.observe_realtime_tick(
        _tick(ask_price=71_000, received_at=now),
        subscribed=False,
        limited=False,
        now=now,
    ) is False
    assert service.observe_realtime_tick(
        _tick(ask_price=71_000, received_at=now - timedelta(seconds=31)),
        subscribed=True,
        limited=False,
        now=now,
    ) is False
    assert service.observe_realtime_tick(
        _tick(ask_price=71_500, received_at=now),
        subscribed=True,
        limited=False,
        now=now,
    ) is True
    assert service.observe_realtime_tick(
        _tick(ask_price=71_200, received_at=now),
        subscribed=True,
        limited=False,
        now=now,
    ) is False

    queued = service.pop_pending_observations(limit=10)
    assert len(queued) == 1
    assert queued[0].price_basis == 'best_ask'
    assert queued[0].observed_price == 71_200


def test_wait_without_source_chase_rechecks_below_target_but_never_at_target() -> None:
    service = SignalDeciderWaitRecheckService()
    candidate = build_wait_recheck_candidate(
        _snapshot(
            chase_limit_price=None,
            upgrade_trigger_price=None,
        ),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    service.replace_candidate_cache([candidate])
    now = datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc)

    assert service.observe_realtime_tick(
        _tick(ask_price=74_000, received_at=now),
        subscribed=True,
        limited=False,
        now=now,
    ) is False
    assert service.observe_realtime_tick(
        _tick(ask_price=73_999, received_at=now),
        subscribed=True,
        limited=False,
        now=now,
    ) is True


def test_open_cycle_discards_preopen_or_aged_observations_before_claim() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    original_enabled = settings.signal_decider_wait_recheck_monitoring_enabled
    settings.signal_decider_wait_recheck_monitoring_enabled = True
    try:
        for observed_at, server_time in (
            (
                datetime(2026, 10, 20, 23, 59, 59, tzinfo=timezone.utc),
                '2026-10-21T09:00:05+09:00',
            ),
            (
                datetime(2026, 10, 21, 0, 0, 1, tzinfo=timezone.utc),
                '2026-10-21T09:00:40+09:00',
            ),
        ):
            service = SignalDeciderWaitRecheckService()
            service.replace_candidate_cache([candidate])
            assert service.observe_realtime_tick(
                _tick(ask_price=71_200, received_at=observed_at),
                subscribed=True,
                limited=False,
                now=observed_at,
            ) is True
            market = SimpleNamespace(
                market='KRX',
                state='OPEN',
                is_trading_day=True,
                server_time=server_time,
                session_open_at='2026-10-21T09:00:00+09:00',
                session_close_at='2026-10-21T15:30:00+09:00',
            )
            with (
                patch(
                    'app.agent_context.wait_recheck_service._utcnow',
                    return_value=datetime.fromisoformat(server_time),
                ),
                patch.object(
                    service,
                    '_claim_observation',
                    return_value=None,
                ) as claim,
                patch.object(service, '_retry_event_ids', return_value=[]),
            ):
                    result = service.run_open_cycle(market=market)
            assert result['status'] == 'idle'
            claim.assert_not_called()
            assert service.observe_realtime_tick(
                _tick(
                    ask_price=71_200,
                    received_at=datetime(
                        2026,
                        10,
                        21,
                        0,
                        0,
                        6,
                        tzinfo=timezone.utc,
                    ),
                ),
                subscribed=True,
                limited=False,
                now=datetime(
                    2026,
                    10,
                    21,
                    0,
                    0,
                    6,
                    tzinfo=timezone.utc,
                ),
            ) is True
    finally:
        settings.signal_decider_wait_recheck_monitoring_enabled = original_enabled


def test_pending_touch_refreshes_to_latest_tick_before_open_cycle() -> None:
    service = SignalDeciderWaitRecheckService()
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    service.replace_candidate_cache([candidate])
    first_observed_at = datetime(
        2026,
        10,
        21,
        0,
        5,
        tzinfo=timezone.utc,
    )
    latest_observed_at = first_observed_at + timedelta(seconds=25)
    dispatch_now = first_observed_at + timedelta(seconds=40)

    assert service.observe_realtime_tick(
        _tick(ask_price=71_100, received_at=first_observed_at),
        subscribed=True,
        limited=False,
        now=first_observed_at,
    ) is True
    assert service.observe_realtime_tick(
        _tick(ask_price=71_200, received_at=latest_observed_at),
        subscribed=True,
        limited=False,
        now=latest_observed_at,
    ) is False

    market = SimpleNamespace(
        market='KRX',
        state='OPEN',
        is_trading_day=True,
        # Simulate the runner's market snapshot predating the latest WS tick.
        server_time=(latest_observed_at - timedelta(seconds=10)).isoformat(),
        session_open_at='2026-10-21T09:00:00+09:00',
        session_close_at='2026-10-21T15:30:00+09:00',
    )
    with (
        patch.object(
            settings,
            'signal_decider_wait_recheck_monitoring_enabled',
            True,
        ),
        patch(
            'app.agent_context.wait_recheck_service._utcnow',
            return_value=dispatch_now,
        ),
        patch.object(
            service,
            '_claim_observation',
            return_value=17,
        ) as claim,
        patch.object(service, '_retry_event_ids', return_value=[]),
        patch.object(
            service,
            '_dispatch_event',
            return_value={'event_id': 17, 'status': 'completed'},
        ),
    ):
        result = service.run_open_cycle(market=market)

    assert result['events_processed'] == 1
    claimed_observation = claim.call_args.kwargs['observation']
    assert claimed_observation.received_at == latest_observed_at
    assert claimed_observation.observed_price == 71_200


def test_wait_recheck_context_is_immutable_and_grants_no_order_authority() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    observation = SimpleNamespace(
        candidate=candidate,
        observed_price=71_200,
        current_price=71_100,
        bid_price=71_100,
        price_basis='best_ask',
        price_as_of=datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc),
        received_at=datetime(2026, 10, 21, 0, 5, 1, tzinfo=timezone.utc),
        quote_source='kis_realtime_ws',
    )

    context, fingerprint = canonical_wait_recheck_context(
        event_id=7,
        observation=observation,
        market_state='OPEN',
        market_server_time='2026-10-21T09:05:01+09:00',
    )

    assert context['schema_version'] == WAIT_RECHECK_CONTEXT_SCHEMA_VERSION
    assert context['execution_authority'] == {
        'direct_order_authority': False,
        'fresh_decider_required': True,
        'only_promotable_action': 'watch_for_trigger',
        'fresh_decider_must_supply_complete_price_levels': True,
    }
    assert validate_wait_recheck_context(context, expected_sha256=fingerprint)
    context['observation']['observed_price'] = 71_300
    assert not validate_wait_recheck_context(context, expected_sha256=fingerprint)


def test_wait_membership_is_appended_below_holdings_and_exec_protection() -> None:
    observed_at = datetime(2026, 10, 21, 0, 1, tzinfo=timezone.utc)
    base = KISRealtimeMonitoredSymbolSource(
        symbols=('000660', '005930'),
        market_date='2026-10-21',
        source_version=observed_at,
        membership_complete=True,
        protected_symbols=('000660', '005930'),
        critical_protected_symbols=('000660',),
    )

    merged = merge_wait_recheck_monitored_source(
        base,
        symbols=('035420', '005930'),
        source_version=observed_at + timedelta(seconds=1),
        membership_complete=True,
    )

    assert merged.symbols == ('000660', '005930', '035420')
    assert merged.protected_symbols == ('000660', '005930')
    assert merged.critical_protected_symbols == ('000660',)
    assert merged.membership_complete is True


def test_wait_membership_partition_replaces_old_symbols_when_exec_is_partial() -> None:
    observed_at = datetime(2026, 10, 21, 0, 1, tzinfo=timezone.utc)
    service = KISRealtimePriceService()

    first = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('000660',),
            market_date=None,
            source_version=None,
            membership_complete=False,
            protected_symbols=('000660',),
            critical_protected_symbols=('000660',),
        ),
        symbols=('035420',),
        source_version=observed_at,
        membership_complete=True,
    )
    service._apply_runtime_monitored_source(
        first,
        trigger='test:first',
        expected_observation_revision=None,
    )
    second = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('000660',),
            market_date=None,
            source_version=None,
            membership_complete=False,
            protected_symbols=('000660',),
            critical_protected_symbols=('000660',),
        ),
        symbols=('005930',),
        source_version=observed_at + timedelta(seconds=1),
        membership_complete=True,
    )
    service._apply_runtime_monitored_source(
        second,
        trigger='test:second',
        expected_observation_revision=None,
    )

    assert service.subscription_snapshot()['allowed_symbols'] == [
        '000660',
        '005930',
    ]

    cleared = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('000660',),
            market_date=None,
            source_version=None,
            membership_complete=False,
            protected_symbols=('000660',),
            critical_protected_symbols=('000660',),
        ),
        symbols=(),
        source_version=observed_at + timedelta(seconds=2),
        membership_complete=True,
    )
    service._apply_runtime_monitored_source(
        cleared,
        trigger='test:cleared',
        expected_observation_revision=None,
    )
    assert service.subscription_snapshot()['allowed_symbols'] == ['000660']


def test_complete_base_refresh_preserves_active_wait_partition() -> None:
    observed_at = datetime(2026, 10, 21, 0, 1, tzinfo=timezone.utc)
    service = KISRealtimePriceService()
    combined = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('000660',),
            market_date=None,
            source_version=observed_at,
            membership_complete=True,
            protected_symbols=('000660',),
            critical_protected_symbols=('000660',),
        ),
        symbols=('035420',),
        source_version=observed_at,
        membership_complete=True,
    )
    service._apply_runtime_monitored_source(
        combined,
        trigger='test:combined',
        expected_observation_revision=None,
    )

    service._apply_runtime_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('000660',),
            market_date=None,
            source_version=observed_at + timedelta(seconds=1),
            membership_complete=True,
            protected_symbols=('000660',),
            critical_protected_symbols=('000660',),
        ),
        trigger='test:base-only',
        expected_observation_revision=None,
    )

    snapshot = service.subscription_snapshot()
    assert snapshot['allowed_symbols'] == ['000660', '035420']
    assert snapshot['wait_recheck_symbols'] == ['035420']


def test_newer_wait_refresh_cannot_promote_stale_base_membership() -> None:
    current_at = datetime(2026, 10, 21, 1, 8, tzinfo=timezone.utc)
    service = KISRealtimePriceService()
    first = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('000660',),
            market_date='2026-10-21',
            source_version=current_at - timedelta(minutes=3),
            membership_complete=True,
            protected_symbols=('000660',),
            critical_protected_symbols=('000660',),
        ),
        symbols=('035420',),
        source_version=current_at - timedelta(minutes=2),
        membership_complete=True,
    )
    stale_base_with_new_wait = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('005930',),
            market_date='2026-10-21',
            source_version=current_at - timedelta(minutes=7),
            membership_complete=True,
            protected_symbols=('005930',),
            critical_protected_symbols=('005930',),
        ),
        symbols=('121440',),
        source_version=current_at - timedelta(minutes=1),
        membership_complete=True,
    )

    with patch(
        'app.quotes.kis_realtime_prices._utc_now',
        return_value=current_at,
    ):
        service._apply_runtime_monitored_source(
            first,
            trigger='test:newer-base',
            expected_observation_revision=None,
        )
        service._apply_runtime_monitored_source(
            stale_base_with_new_wait,
            trigger='test:stale-base-new-wait',
            expected_observation_revision=None,
        )

    snapshot = service.subscription_snapshot()
    assert snapshot['allowed_symbols'] == ['000660', '121440']
    assert snapshot['protected_symbols'] == ['000660']
    assert snapshot['critical_protected_symbols'] == ['000660']
    assert snapshot['wait_recheck_symbols'] == ['121440']


def test_wait_retirement_cannot_remove_overlapping_partial_base_symbol() -> None:
    current_at = datetime(2026, 10, 21, 1, 8, tzinfo=timezone.utc)
    service = KISRealtimePriceService()
    overlapping = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('005930',),
            market_date='2026-10-21',
            source_version=current_at - timedelta(minutes=3),
            membership_complete=True,
        ),
        symbols=('005930',),
        source_version=current_at - timedelta(minutes=2),
        membership_complete=True,
    )
    partial_base_after_wait_retirement = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=(),
            market_date='2026-10-21',
            source_version=current_at - timedelta(minutes=1),
            membership_complete=False,
        ),
        symbols=(),
        source_version=current_at,
        membership_complete=True,
    )

    with patch(
        'app.quotes.kis_realtime_prices._utc_now',
        return_value=current_at,
    ):
        service._apply_runtime_monitored_source(
            overlapping,
            trigger='test:overlapping-base-wait',
            expected_observation_revision=None,
        )
        assert service.subscription_snapshot()['wait_recheck_symbols'] == [
            '005930'
        ]
        service._apply_runtime_monitored_source(
            partial_base_after_wait_retirement,
            trigger='test:partial-base-wait-retired',
            expected_observation_revision=None,
        )

    snapshot = service.subscription_snapshot()
    assert snapshot['allowed_symbols'] == ['005930']
    assert snapshot['wait_recheck_symbols'] == []


def test_base_only_refresh_preserves_promoted_wait_overlap_on_retirement() -> None:
    current_at = datetime(2026, 10, 21, 1, 8, tzinfo=timezone.utc)
    service = KISRealtimePriceService()
    initial = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('000660',),
            market_date='2026-10-21',
            source_version=current_at - timedelta(minutes=5),
            membership_complete=True,
        ),
        symbols=('005930',),
        source_version=current_at - timedelta(minutes=4),
        membership_complete=True,
    )
    base_only_overlap = KISRealtimeMonitoredSymbolSource(
        symbols=('000660', '005930'),
        market_date='2026-10-21',
        source_version=current_at - timedelta(minutes=3),
        membership_complete=True,
    )
    partial_base_after_wait_retirement = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('000660',),
            market_date='2026-10-21',
            source_version=current_at - timedelta(minutes=2),
            membership_complete=False,
        ),
        symbols=(),
        source_version=current_at - timedelta(minutes=1),
        membership_complete=True,
    )

    with patch(
        'app.quotes.kis_realtime_prices._utc_now',
        return_value=current_at,
    ):
        service._apply_runtime_monitored_source(
            initial,
            trigger='test:initial-wait-only',
            expected_observation_revision=None,
        )
        service._apply_runtime_monitored_source(
            base_only_overlap,
            trigger='test:base-only-promotes-overlap',
            expected_observation_revision=None,
        )
        service._apply_runtime_monitored_source(
            partial_base_after_wait_retirement,
            trigger='test:partial-base-after-retirement',
            expected_observation_revision=None,
        )

    snapshot = service.subscription_snapshot()
    assert snapshot['allowed_symbols'] == ['000660', '005930']
    assert snapshot['wait_recheck_symbols'] == []


def test_wait_recheck_overlay_survives_prompt_compaction() -> None:
    overlay = {
        'schema_version': WAIT_RECHECK_CONTEXT_SCHEMA_VERSION,
        'event_id': 7,
        'execution_authority': {
            'direct_order_authority': False,
            'fresh_decider_required': True,
            'only_promotable_action': 'watch_for_trigger',
        },
    }
    item = {
        'symbol': '005930',
        'market': 'KOSPI',
        'company': {'market': 'KOSPI'},
        'intraday_wait_recheck_context': overlay,
    }

    payload = AgentFeedbackOrchestrator._build_signal_decider_payload(
        item=item,
        market_date=date(2026, 10, 20),
        request_run_id='wait-recheck-test',
        market_context={},
        upstream_summaries={},
        upstream_traceability={},
        signal_decider_policy={},
        agent_name='short-term-decider',
    )
    compressed = compressed_batch_item_for_prompt(payload)

    assert payload['symbol_day_context']['intraday_wait_recheck_context'] == overlay
    assert (
        compressed['symbol_day_context']['intraday_wait_recheck_context']
        == overlay
    )


def test_wait_recheck_dispatch_requires_one_terminal_fresh_computation() -> None:
    completed = {
        'summary': {
            'status': 'completed',
            'completed': 1,
            'computed_count': 1,
            'reused_count': 0,
            'failed': 0,
        }
    }

    assert wait_recheck_dispatch_completed(completed) is True
    assert wait_recheck_dispatch_completed(
        {
            **completed,
            'summary': {
                **completed['summary'],
                'status': 'partial',
                'failed': 1,
            },
        }
    ) is False


def test_dispatch_event_calls_only_fresh_short_term_recompute_contract() -> None:
    request_run_id = 'wait-recheck-2026-10-21-005930-9-1'
    with session_scope() as session:
        session.execute(delete(SignalDeciderWaitRecheckEvent))
        session.execute(delete(SignalDeciderDecisionSnapshot))
        result_snapshot = SignalDeciderDecisionSnapshot(
            feedback_id=None,
            content_fingerprint='c' * 64,
            market_date=date(2026, 10, 20),
            symbol='005930',
            agent_name='short-term-decider',
            agent_version='test',
            decision_run_id=request_run_id,
            feedback_received_at=datetime(
                2026,
                10,
                21,
                0,
                5,
                tzinfo=timezone.utc,
            ),
            policy_version='signal_decider_policy_v1',
            decision_action='wait',
            preferred_action='wait',
            next_day_open_action='wait',
            entry_price_basis='trigger_limit',
            target_price=74_000,
            expected_return_pct=None,
            stop_loss_price=69_500,
            suggested_holding_period='1-3 trading days',
            confidence_score_10=6,
            conviction_score_10=6,
            thesis_summary='test',
            supporting_evidence_payload=[],
            decision_risks_payload=[],
            invalidation_conditions_payload=[],
            data_quality_warnings_payload=[],
            specialist_disagreement_payload=[],
            traceability_payload={},
            input_feature_snapshot_payload={},
            request_payload={},
            response_payload={
                'tactical_action': 'watch_for_trigger',
                'preferred_action': 'watch_for_trigger',
            },
            response_summary_payload={},
        )
        session.add(result_snapshot)
        session.flush()
        result_snapshot_id = result_snapshot.id

    context = {
        'schema_version': WAIT_RECHECK_CONTEXT_SCHEMA_VERSION,
        'event_id': 9,
    }
    attempt = {
        'event_id': 9,
        'symbol': '005930',
        'source_market_date': date(2026, 10, 20),
        'target_trade_date': date(2026, 10, 21),
        'source_snapshot_id': 1,
        'source_decision_run_id': 'source-run',
        'request_run_id': request_run_id,
        'context': context,
        'fs_div': 'CFS',
        'disclosures_limit': 5,
        'knowledge_cutoff_at': '2026-10-20T11:00:00+00:00',
        'upstream_summary_run_id': 'summary-run',
    }
    completed = {
        'summary': {
            'status': 'completed',
            'completed': 1,
            'computed_count': 1,
            'reused_count': 0,
            'failed': 0,
        }
    }
    service = SignalDeciderWaitRecheckService()
    try:
        with (
            patch.object(service, '_begin_attempt', return_value=attempt),
            patch.object(service, '_finish_event') as finish_event,
            patch(
                'app.agent_context.orchestrator.agent_feedback_orchestrator.'
                'dispatch_for_symbols',
                return_value=completed,
            ) as dispatch,
        ):
            result = service._dispatch_event(
                9,
                now=datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc),
            )

        assert result['status'] == 'completed'
        assert result['result_action'] == 'watch_for_trigger'
        assert result['direct_order_authority'] is False
        dispatch.assert_called_once_with(
            market_date=date(2026, 10, 20),
            symbols=['005930'],
            roles=('short-term-decider',),
            fs_div='CFS',
            disclosures_limit=5,
            knowledge_cutoff_at='2026-10-20T11:00:00+00:00',
            policy_context_as_of_date=date(2026, 10, 21),
            reuse_existing=True,
            request_run_id=request_run_id,
            parent_run_id='source-run',
            force=False,
            force_recompute=True,
            upstream_summary_run_id='summary-run',
            source_context_overlays={'005930': context},
        )
        finish_event.assert_called_once_with(
            event_id=9,
            request_run_id=request_run_id,
            status='completed',
            result_snapshot_id=result_snapshot_id,
            result_action='watch_for_trigger',
        )
    finally:
        with session_scope() as session:
            session.execute(delete(SignalDeciderWaitRecheckEvent))
            session.execute(delete(SignalDeciderDecisionSnapshot))
    assert wait_recheck_dispatch_completed(
        {
            **completed,
            'summary': {
                **completed['summary'],
                'computed_count': 0,
                'reused_count': 1,
            },
        }
    ) is False
def test_durable_claim_is_unique_and_context_readback_is_verified() -> None:
    target_trade_date = date(2026, 10, 21)
    source_market_date = date(2026, 10, 20)
    received_at = datetime(2026, 10, 20, 11, 0, tzinfo=timezone.utc)
    source_snapshot = _snapshot(
        chase_limit_price=None,
        upgrade_trigger_price=None,
    )
    request_payload = source_snapshot.request_payload
    response_payload = source_snapshot.response_payload
    with session_scope() as session:
        session.execute(delete(SignalDeciderWaitRecheckEvent))
        session.execute(delete(SignalDeciderDecisionSnapshot))
        row = SignalDeciderDecisionSnapshot(
            feedback_id=None,
            content_fingerprint='b' * 64,
            market_date=source_market_date,
            symbol='005930',
            agent_name='short-term-decider',
            agent_version='test',
            decision_run_id='source-run',
            feedback_received_at=received_at,
            policy_version='signal_decider_policy_v1',
            decision_action='wait',
            preferred_action='wait',
            next_day_open_action='wait',
            entry_price_basis='next_open',
            target_price=74_000,
            expected_return_pct=None,
            stop_loss_price=69_500,
            suggested_holding_period='1-3 trading days',
            confidence_score_10=6,
            conviction_score_10=6,
            thesis_summary='test',
            supporting_evidence_payload=[],
            decision_risks_payload=[],
            invalidation_conditions_payload=[],
            data_quality_warnings_payload=[],
            specialist_disagreement_payload=[],
            traceability_payload={},
            input_feature_snapshot_payload={},
            request_payload=request_payload,
            response_payload=response_payload,
            response_summary_payload={},
        )
        session.add(row)
        session.flush()
        snapshot_id = row.id

    service = SignalDeciderWaitRecheckService()
    original_enabled = settings.signal_decider_wait_recheck_monitoring_enabled
    settings.signal_decider_wait_recheck_monitoring_enabled = True
    try:
        refresh_completed_at = datetime(
            2026,
            10,
            21,
            0,
            4,
            tzinfo=timezone.utc,
        )
        with (
            patch(
                'app.agent_context.wait_recheck_service.'
                'previous_krx_trading_date',
                return_value=source_market_date,
            ),
            patch(
                'app.agent_context.wait_recheck_service._utcnow',
                return_value=refresh_completed_at,
            ),
        ):
            source = service.load_subscription_source(
                target_trade_date=target_trade_date
            )
        assert source.symbols == ('005930',)
        assert source.source_version == refresh_completed_at
        candidate = source.candidates[0]
        assert candidate.source_snapshot_id == snapshot_id
        observation = WaitRecheckObservation(
            candidate=candidate,
            observed_price=71_200,
            current_price=71_100,
            bid_price=71_100,
            price_basis='best_ask',
            price_as_of=datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc),
            received_at=datetime(2026, 10, 21, 0, 5, 1, tzinfo=timezone.utc),
            quote_source='kis_realtime_ws',
        )
        event_id = service._claim_observation(
            observation=observation,
            market_state='OPEN',
            market_server_time='2026-10-21T09:05:01+09:00',
        )
        duplicate_event_id = service._claim_observation(
            observation=observation,
            market_state='OPEN',
            market_server_time='2026-10-21T09:05:02+09:00',
        )
        assert event_id is not None
        assert duplicate_event_id == event_id
        first_attempt = service._begin_attempt(
            event_id,
            now=datetime(2026, 10, 21, 0, 5, 2, tzinfo=timezone.utc),
        )
        duplicate_attempt = service._begin_attempt(
            event_id,
            now=datetime(2026, 10, 21, 0, 5, 3, tzinfo=timezone.utc),
        )
        assert first_attempt is not None
        assert duplicate_attempt is None
        service._finish_event(
            event_id=event_id,
            request_run_id='stale-attempt',
            status='completed',
            result_action='watch_for_trigger',
        )
        with session_scope() as session:
            assert session.get(
                SignalDeciderWaitRecheckEvent,
                event_id,
            ).status == 'running'
        service._finish_event(
            event_id=event_id,
            request_run_id=first_attempt['request_run_id'],
            status='completed',
            result_action='wait',
        )
        context = load_validated_wait_recheck_context(
            event_id=event_id,
            symbol='005930',
        )
        assert context is not None
        assert context['execution_authority']['direct_order_authority'] is False
        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderWaitRecheckEvent)
            ).scalars().all()
            assert len(rows) == 1
            assert rows[0].status == 'completed'
            assert rows[0].result_action == 'wait'

            rows[0].status = 'queued'
            rows[0].attempt_count = 0
            rows[0].request_run_id = None
            rows[0].context_payload = {'corrupted': True}

        invalid_attempt = service._begin_attempt(
            event_id,
            now=datetime(2026, 10, 21, 0, 6, tzinfo=timezone.utc),
        )
        assert invalid_attempt is None
        with session_scope() as session:
            expired = session.get(SignalDeciderWaitRecheckEvent, event_id)
            assert expired is not None
            assert expired.status == 'expired'
            assert expired.last_attempt_at == datetime(
                2026,
                10,
                21,
                0,
                6,
                tzinfo=timezone.utc,
            )
            assert expired.last_error_payload == {
                'code': 'wait_recheck_context_invalid',
            }
    finally:
        settings.signal_decider_wait_recheck_monitoring_enabled = original_enabled
        with session_scope() as session:
            session.execute(delete(SignalDeciderWaitRecheckEvent))
            session.execute(delete(SignalDeciderDecisionSnapshot))
