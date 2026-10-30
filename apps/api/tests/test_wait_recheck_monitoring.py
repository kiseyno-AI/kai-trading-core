from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from sqlalchemy import delete, select

from app.agent_context.orchestrator import AgentFeedbackOrchestrator
from app.agent_context.orchestrator_helpers import compressed_batch_item_for_prompt
from app.agent_context.wait_recheck_service import (
    WAIT_RECHECK_CONTEXT_SCHEMA_VERSION,
    WAIT_RECHECK_LEGACY_CONTEXT_SCHEMA_VERSION,
    SignalDeciderWaitRecheckService,
    WaitRecheckCandidate,
    WaitRecheckObservation,
    _canonical_json_sha256,
    _material_change_reasons,
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
    agent_name: str = 'short-term-decider',
) -> SimpleNamespace:
    created_at = datetime(2026, 10, 20, 11, 0, tzinfo=timezone.utc)
    return SimpleNamespace(
        id=snapshot_id,
        market_date=date(2026, 10, 20),
        symbol=symbol,
        agent_name=agent_name,
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
    symbol: str = '005930',
    ask_price: int = 71_000,
    received_at: datetime | None = None,
    price_as_of: datetime | None = None,
    volume: int = 100_000,
    raw_fields: dict[str, str] | None = None,
) -> KISRealtimePriceTick:
    observed_at = received_at or datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc)
    return KISRealtimePriceTick(
        symbol=symbol,
        current_price=70_900,
        price_as_of=price_as_of or observed_at,
        received_at=observed_at,
        change=100,
        change_pct=0.14,
        volume=volume,
        turnover=7_000_000_000,
        bid_price=70_900,
        ask_price=ask_price,
        trade_halt=False,
        raw_fields=raw_fields or {},
    )


def _enqueue_supported_window(
    service: SignalDeciderWaitRecheckService,
    *,
    candidate: WaitRecheckCandidate,
    end_at: datetime,
    ask_prices: tuple[int, int, int] = (71_000, 71_000, 71_000),
    volumes: tuple[int, int, int] = (100_000, 100_100, 100_200),
    replace_cache: bool = True,
) -> tuple[bool, bool, bool]:
    if replace_cache:
        service.replace_candidate_cache([candidate])
    results = []
    for seconds_before_end, ask_price, volume in zip(
        (5, 2, 0),
        ask_prices,
        volumes,
        strict=True,
    ):
        observed_at = end_at - timedelta(seconds=seconds_before_end)
        results.append(
            service.observe_realtime_tick(
                _tick(
                    symbol=candidate.symbol,
                    ask_price=ask_price,
                    received_at=observed_at,
                    volume=volume,
                ),
                subscribed=True,
                limited=False,
                now=observed_at,
            )
        )
    return results[0], results[1], results[2]


def _supported_observation(
    candidate: WaitRecheckCandidate,
    *,
    end_at: datetime | None = None,
    ask_prices: tuple[int, int, int] = (71_000, 71_000, 71_000),
    volumes: tuple[int, int, int] = (100_000, 100_100, 100_200),
) -> WaitRecheckObservation:
    service = SignalDeciderWaitRecheckService()
    observed_at = end_at or datetime(
        2026,
        10,
        21,
        0,
        5,
        5,
        tzinfo=timezone.utc,
    )
    assert _enqueue_supported_window(
        service,
        candidate=candidate,
        end_at=observed_at,
        ask_prices=ask_prices,
        volumes=volumes,
    ) == (False, False, True)
    observations = service.pop_pending_observations(limit=10)
    assert len(observations) == 1
    return observations[0]


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


def test_same_symbol_wait_candidates_remain_independent_by_agent() -> None:
    target_trade_date = date(2026, 10, 21)
    short_candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=target_trade_date,
    )
    signal_candidate = build_wait_recheck_candidate(
        _snapshot(
            snapshot_id=12,
            agent_name='signal-decider',
            upgrade_target_action='buy',
        ),
        target_trade_date=target_trade_date,
    )

    assert short_candidate is not None
    assert signal_candidate is not None
    assert short_candidate.agent_name == 'short-term-decider'
    assert signal_candidate.agent_name == 'signal-decider'

    service = SignalDeciderWaitRecheckService()
    service.replace_candidate_cache([short_candidate, signal_candidate])
    now = datetime(2026, 10, 21, 0, 5, 5, tzinfo=timezone.utc)

    assert _enqueue_supported_window(
        service,
        candidate=short_candidate,
        end_at=now,
        replace_cache=False,
    ) == (False, False, True)
    observations = service.pop_pending_observations(limit=10)
    assert {
        observation.candidate.agent_name for observation in observations
    } == {'signal-decider', 'short-term-decider'}


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

    service = SignalDeciderWaitRecheckService()
    assert _enqueue_supported_window(
        service,
        candidate=candidate,
        end_at=now + timedelta(seconds=5),
    ) == (False, False, True)
    assert service.observe_realtime_tick(
        _tick(
            ask_price=71_050,
            received_at=now + timedelta(seconds=6),
        ),
        subscribed=True,
        limited=False,
        now=now + timedelta(seconds=6),
    ) is False

    queued = service.pop_pending_observations(limit=10)
    assert len(queued) == 1
    assert queued[0].price_basis == 'best_ask'
    assert queued[0].observed_price == 71_050
    assert queued[0].intraday_evidence['status'] == 'supported'


def test_rolling_evidence_wakes_once_only_after_support_is_complete() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    service = SignalDeciderWaitRecheckService()
    service.replace_candidate_cache([candidate])
    wake = Mock()
    service.set_wake_callback(wake)
    end_at = datetime(2026, 10, 21, 0, 5, 5, tzinfo=timezone.utc)

    assert service.observe_realtime_tick(
        _tick(received_at=end_at - timedelta(seconds=5)),
        subscribed=True,
        limited=False,
        now=end_at - timedelta(seconds=5),
    ) is False
    assert service.observe_realtime_tick(
        _tick(received_at=end_at - timedelta(seconds=2)),
        subscribed=True,
        limited=False,
        now=end_at - timedelta(seconds=2),
    ) is False
    assert service.pop_pending_observations(limit=10) == []
    wake.assert_not_called()

    assert service.observe_realtime_tick(
        _tick(received_at=end_at),
        subscribed=True,
        limited=False,
        now=end_at,
    ) is True
    assert service.runtime_snapshot()['pending_count'] == 1
    wake.assert_called_once_with()

    assert service.observe_realtime_tick(
        _tick(received_at=end_at + timedelta(seconds=1)),
        subscribed=True,
        limited=False,
        now=end_at + timedelta(seconds=1),
    ) is False
    wake.assert_called_once_with()
    assert service.runtime_snapshot()['wake_notification_count'] == 1


def test_high_rate_frames_are_coalesced_without_starving_evidence_span() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    service = SignalDeciderWaitRecheckService()
    service.replace_candidate_cache([candidate])
    started_at = datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc)
    queued = False

    # Twenty frames per second model a liquid symbol, while KIS market time
    # remains second-granularity. The evidence buffer must retain one sample
    # per market second instead of evicting the five-second span at max size.
    for index in range(121):
        received_at = started_at + timedelta(milliseconds=50 * index)
        price_as_of = started_at + timedelta(
            seconds=(50 * index) // 1000
        )
        queued = service.observe_realtime_tick(
            _tick(
                received_at=received_at,
                price_as_of=price_as_of,
                volume=100_000 + index,
            ),
            subscribed=True,
            limited=False,
            now=received_at,
        ) or queued

    assert queued is True
    observations = service.pop_pending_observations(limit=10)
    assert len(observations) == 1
    evidence = observations[0].intraday_evidence
    assert evidence['status'] == 'supported'
    assert evidence['sample_count'] == 7
    assert float(evidence['observed_span_seconds']) >= 5


def test_replayed_exchange_timestamp_cannot_manufacture_evidence() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    service = SignalDeciderWaitRecheckService()
    service.replace_candidate_cache([candidate])
    market_time = datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc)

    for received_offset in (0, 3, 6):
        received_at = market_time + timedelta(seconds=received_offset)
        assert service.observe_realtime_tick(
            _tick(
                received_at=received_at,
                price_as_of=market_time,
                volume=100_000 + received_offset,
            ),
            subscribed=True,
            limited=False,
            now=received_at,
        ) is False

    assert service.runtime_snapshot()['rolling_evidence_sample_count'] == 1
    assert service.pop_pending_observations(limit=10) == []

    # A later-arriving frame cannot move exchange time backwards either.
    received_at = market_time + timedelta(seconds=7)
    assert service.observe_realtime_tick(
        _tick(
            received_at=received_at,
            price_as_of=market_time - timedelta(seconds=1),
        ),
        subscribed=True,
        limited=False,
        now=received_at,
    ) is False
    assert service.runtime_snapshot()['rolling_evidence_sample_count'] == 1


def test_rr_prefilter_blocks_short_before_claim_but_keeps_signal_lane() -> None:
    short_candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    signal_candidate = build_wait_recheck_candidate(
        _snapshot(
            snapshot_id=12,
            agent_name='signal-decider',
            upgrade_target_action='buy',
        ),
        target_trade_date=date(2026, 10, 21),
    )
    assert short_candidate is not None
    assert signal_candidate is not None
    service = SignalDeciderWaitRecheckService()
    service.replace_candidate_cache([short_candidate, signal_candidate])
    end_at = datetime(2026, 10, 21, 0, 5, 5, tzinfo=timezone.utc)

    assert _enqueue_supported_window(
        service,
        candidate=signal_candidate,
        end_at=end_at,
        ask_prices=(71_200, 71_200, 71_200),
        replace_cache=False,
    ) == (False, False, True)
    observations = service.pop_pending_observations(limit=10)
    assert [item.candidate.agent_name for item in observations] == [
        'signal-decider'
    ]
    assert service.runtime_snapshot()[
        'reward_risk_prefilter_rejection_count'
    ] >= 3

    market = SimpleNamespace(
        market='KRX',
        state='OPEN',
        is_trading_day=True,
        server_time='2026-10-21T09:05:05+09:00',
        session_open_at='2026-10-21T09:00:00+09:00',
        session_close_at='2026-10-21T15:30:00+09:00',
    )
    short_only = SignalDeciderWaitRecheckService()
    with patch.object(
        settings,
        'signal_decider_wait_recheck_monitoring_enabled',
        True,
    ):
        assert _enqueue_supported_window(
            short_only,
            candidate=short_candidate,
            end_at=end_at,
            ask_prices=(71_200, 71_200, 71_200),
        ) == (False, False, False)
        with (
            patch(
                'app.agent_context.wait_recheck_service._utcnow',
                return_value=end_at,
            ),
            patch.object(short_only, '_claim_observation') as claim,
            patch.object(short_only, '_retry_event_ids', return_value=[]),
        ):
            result = short_only.run_open_cycle(market=market)
    assert result['status'] == 'idle'
    claim.assert_not_called()


def test_pending_observation_is_cancelled_when_latest_price_fails_reward_risk() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    service = SignalDeciderWaitRecheckService()
    end_at = datetime(2026, 10, 21, 0, 5, 5, tzinfo=timezone.utc)
    assert _enqueue_supported_window(
        service,
        candidate=candidate,
        end_at=end_at,
    ) == (False, False, True)
    assert service.runtime_snapshot()['pending_count'] == 1
    assert service.runtime_snapshot()['reserved_count'] == 1

    assert service.observe_realtime_tick(
        _tick(
            ask_price=71_200,
            received_at=end_at + timedelta(seconds=1),
        ),
        subscribed=True,
        limited=False,
        now=end_at + timedelta(seconds=1),
    ) is False
    snapshot = service.runtime_snapshot()
    assert snapshot['pending_count'] == 0
    assert snapshot['reserved_count'] == 0
    assert service.pop_pending_observations(limit=10) == []


def test_pending_observation_is_cancelled_by_newer_limited_tick() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    service = SignalDeciderWaitRecheckService()
    end_at = datetime(2026, 10, 21, 0, 5, 5, tzinfo=timezone.utc)
    assert _enqueue_supported_window(
        service,
        candidate=candidate,
        end_at=end_at,
    ) == (False, False, True)
    assert service.runtime_snapshot()['pending_count'] == 1

    assert service.observe_realtime_tick(
        _tick(received_at=end_at + timedelta(seconds=1)),
        subscribed=True,
        limited=True,
        now=end_at + timedelta(seconds=1),
    ) is False
    snapshot = service.runtime_snapshot()
    assert snapshot['pending_count'] == 0
    assert snapshot['reserved_count'] == 0
    assert snapshot['rolling_evidence_sample_count'] == 0
    assert service.pop_pending_observations(limit=10) == []

    # Recovery must establish a wholly new rolling window; pre-limit samples
    # cannot make the first healthy tick immediately dispatchable.
    assert service.observe_realtime_tick(
        _tick(received_at=end_at + timedelta(seconds=2)),
        subscribed=True,
        limited=False,
        now=end_at + timedelta(seconds=2),
    ) is False
    assert service.runtime_snapshot()['rolling_evidence_sample_count'] == 1
    assert service.observe_realtime_tick(
        _tick(received_at=end_at + timedelta(seconds=4)),
        subscribed=True,
        limited=False,
        now=end_at + timedelta(seconds=4),
    ) is False
    assert service.observe_realtime_tick(
        _tick(received_at=end_at + timedelta(seconds=7)),
        subscribed=True,
        limited=False,
        now=end_at + timedelta(seconds=7),
    ) is True


def test_wait_without_source_chase_rechecks_below_target_but_never_at_target() -> None:
    service = SignalDeciderWaitRecheckService()
    candidate = build_wait_recheck_candidate(
        _snapshot(
            chase_limit_price=None,
            target_price=100_000,
            upgrade_trigger_price=None,
        ),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    service.replace_candidate_cache([candidate])
    now = datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc)

    assert service.observe_realtime_tick(
        _tick(ask_price=100_000, received_at=now),
        subscribed=True,
        limited=False,
        now=now,
    ) is False
    service = SignalDeciderWaitRecheckService()
    assert _enqueue_supported_window(
        service,
        candidate=candidate,
        end_at=now + timedelta(seconds=5),
        ask_prices=(74_000, 74_000, 74_000),
    ) == (False, False, True)


def test_preopen_ticks_never_queue_and_aged_observations_are_discarded() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    original_enabled = settings.signal_decider_wait_recheck_monitoring_enabled
    settings.signal_decider_wait_recheck_monitoring_enabled = True
    try:
        preopen_service = SignalDeciderWaitRecheckService()
        preopen_at = datetime(
            2026,
            10,
            20,
            23,
            59,
            59,
            tzinfo=timezone.utc,
        )
        assert _enqueue_supported_window(
            preopen_service,
            candidate=candidate,
            end_at=preopen_at,
        ) == (False, False, False)
        preopen_snapshot = preopen_service.runtime_snapshot()
        assert preopen_snapshot['pending_count'] == 0
        assert preopen_snapshot['rolling_evidence_sample_count'] == 0

        delayed_preopen_trade = replace(
            _tick(
                received_at=datetime(
                    2026,
                    10,
                    21,
                    0,
                    0,
                    1,
                    tzinfo=timezone.utc,
                )
            ),
            price_as_of=preopen_at,
        )
        assert preopen_service.observe_realtime_tick(
            delayed_preopen_trade,
            subscribed=True,
            limited=False,
            now=delayed_preopen_trade.received_at,
        ) is False
        assert (
            preopen_service.runtime_snapshot()[
                'rolling_evidence_sample_count'
            ]
            == 0
        )

        first_supported_at = datetime(
            2026,
            10,
            21,
            0,
            0,
            6,
            tzinfo=timezone.utc,
        )
        assert _enqueue_supported_window(
            preopen_service,
            candidate=candidate,
            end_at=first_supported_at,
            replace_cache=False,
        ) == (False, False, True)

        aged_service = SignalDeciderWaitRecheckService()
        assert _enqueue_supported_window(
            aged_service,
            candidate=candidate,
            end_at=first_supported_at,
        ) == (False, False, True)
        server_time = '2026-10-21T09:00:40+09:00'
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
                aged_service,
                '_claim_observation',
                return_value=None,
            ) as claim,
            patch.object(aged_service, '_retry_event_ids', return_value=[]),
        ):
            result = aged_service.run_open_cycle(market=market)
        assert result['status'] == 'idle'
        claim.assert_not_called()
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

    assert _enqueue_supported_window(
        service,
        candidate=candidate,
        end_at=first_observed_at,
    ) == (False, False, True)
    assert service.observe_realtime_tick(
        _tick(ask_price=71_100, received_at=latest_observed_at),
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
    assert claimed_observation.observed_price == 71_100


def test_wait_recheck_context_is_immutable_and_grants_no_order_authority() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    observation = _supported_observation(
        candidate,
        end_at=datetime(2026, 10, 21, 0, 5, 5, tzinfo=timezone.utc),
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
    hash_tampered = deepcopy(context)
    hash_tampered['observation']['observed_price'] = 71_050
    assert not validate_wait_recheck_context(
        hash_tampered,
        expected_sha256=fingerprint,
    )

    evidence_tampered = deepcopy(context)
    evidence_tampered['intraday_evidence']['sample_count'] = 4
    evidence_tampered_sha256 = _canonical_json_sha256(evidence_tampered)
    assert not validate_wait_recheck_context(
        evidence_tampered,
        expected_sha256=evidence_tampered_sha256,
    )

    policy_tampered = deepcopy(context)
    policy_tampered['economics_prefilter'][
        'minimum_required_ratio'
    ] = '1.000000'
    assert not validate_wait_recheck_context(
        policy_tampered,
        expected_sha256=_canonical_json_sha256(policy_tampered),
    )
    requirements_tampered = deepcopy(context)
    requirements_tampered['intraday_evidence']['requirements'][
        'maximum_sample_count'
    ] += 1
    assert not validate_wait_recheck_context(
        requirements_tampered,
        expected_sha256=_canonical_json_sha256(requirements_tampered),
    )
    reason_tampered = deepcopy(context)
    reason_tampered['generation_lineage']['material_change_reasons'] = [
        'made_up_reason'
    ]
    assert not validate_wait_recheck_context(
        reason_tampered,
        expected_sha256=_canonical_json_sha256(reason_tampered),
    )

    legacy_context = deepcopy(context)
    legacy_context['schema_version'] = WAIT_RECHECK_LEGACY_CONTEXT_SCHEMA_VERSION
    legacy_context.pop('generation_lineage')
    legacy_context.pop('economics_prefilter')
    legacy_context.pop('intraday_evidence')
    legacy_sha256 = _canonical_json_sha256(legacy_context)
    assert validate_wait_recheck_context(
        legacy_context,
        expected_sha256=legacy_sha256,
    )


def test_signal_wait_recheck_context_only_promotes_fresh_buy() -> None:
    candidate = build_wait_recheck_candidate(
        _snapshot(
            snapshot_id=12,
            agent_name='signal-decider',
            upgrade_target_action='buy',
        ),
        target_trade_date=date(2026, 10, 21),
    )
    assert candidate is not None
    observation = _supported_observation(
        candidate,
        end_at=datetime(2026, 10, 21, 0, 5, 5, tzinfo=timezone.utc),
    )

    context, fingerprint = canonical_wait_recheck_context(
        event_id=8,
        observation=observation,
        market_state='OPEN',
        market_server_time='2026-10-21T09:05:01+09:00',
    )

    assert context['agent_name'] == 'signal-decider'
    assert context['execution_authority']['only_promotable_action'] == 'buy'
    assert validate_wait_recheck_context(context, expected_sha256=fingerprint)


def test_generation_two_requires_material_change_and_preserves_lineage() -> None:
    initial = build_wait_recheck_candidate(
        _snapshot(),
        target_trade_date=date(2026, 10, 21),
    )
    assert initial is not None
    prior_observed_at = datetime(
        2026,
        10,
        21,
        0,
        1,
        tzinfo=timezone.utc,
    )
    generation_two = replace(
        initial,
        generation=2,
        rearmed_from_event_id=41,
        rearm_not_before=prior_observed_at + timedelta(seconds=120),
        prior_observed_price=71_500,
        prior_observed_at=prior_observed_at,
        prior_gross_reward_risk_ratio='1.250000',
        prior_intraday_evidence={
            'flow': {
                'last_cumulative_volume': '80000.000000',
                'latest_total_depth_imbalance': '-0.200000',
            }
        },
    )
    observation = _supported_observation(
        generation_two,
        end_at=datetime(2026, 10, 21, 0, 5, 5, tzinfo=timezone.utc),
    )

    reasons = _material_change_reasons(
        candidate=generation_two,
        observed_price=observation.observed_price,
        evidence=observation.intraday_evidence,
    )
    assert 'reward_risk_threshold_crossed' in reasons
    assert 'observed_entry_price_improved' in reasons
    assert observation.material_change_reasons == reasons

    no_change_candidate = replace(
        generation_two,
        prior_observed_price=observation.observed_price,
        prior_gross_reward_risk_ratio='2.000000',
        prior_intraday_evidence={
            'flow': {
                'last_cumulative_volume': '100200.000000',
            }
        },
    )
    assert _material_change_reasons(
        candidate=no_change_candidate,
        observed_price=observation.observed_price,
        evidence=observation.intraday_evidence,
    ) == ()

    context, fingerprint = canonical_wait_recheck_context(
        event_id=42,
        observation=observation,
        market_state='OPEN',
        market_server_time='2026-10-21T09:05:05+09:00',
    )
    assert context['generation_lineage'] == {
        'generation': 2,
        'rearmed_from_event_id': 41,
        'prior_observed_price': 71_500,
        'prior_observed_at': prior_observed_at.isoformat(),
        'material_change_required': True,
        'material_change_reasons': list(reasons),
    }
    assert validate_wait_recheck_context(context, expected_sha256=fingerprint)


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


def test_wait_membership_precedes_passive_observation_symbols_at_subscription_cap() -> None:
    observed_at = datetime(2026, 10, 21, 0, 1, tzinfo=timezone.utc)
    service = KISRealtimePriceService()
    merged = merge_wait_recheck_monitored_source(
        KISRealtimeMonitoredSymbolSource(
            symbols=('000660', '005930', '035420'),
            market_date=None,
            source_version=observed_at,
            membership_complete=True,
            protected_symbols=('000660',),
            critical_protected_symbols=('000660',),
        ),
        symbols=('035420',),
        source_version=observed_at + timedelta(seconds=1),
        membership_complete=True,
    )

    with patch.object(settings, 'kis_realtime_price_max_subscriptions', 2):
        service._apply_runtime_monitored_source(
            merged,
            trigger='test:wait-priority',
            expected_observation_revision=None,
        )

        snapshot = service.subscription_snapshot()

    assert snapshot['allowed_symbols'] == ['000660', '035420']
    assert snapshot['limited_symbols'] == ['005930']
    assert snapshot['wait_recheck_symbols'] == ['035420']


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
        'agent_name': 'short-term-decider',
    }
    attempt = {
        'event_id': 9,
        'symbol': '005930',
        'agent_name': 'short-term-decider',
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


def test_dispatch_event_recomputes_exact_signal_agent_and_reads_signal_buy() -> None:
    request_run_id = 'wait-recheck-2026-10-21-signal-decider-005930-10-1'
    with session_scope() as session:
        result_snapshot = SignalDeciderDecisionSnapshot(
            feedback_id=None,
            content_fingerprint='d' * 64,
            market_date=date(2026, 10, 20),
            symbol='005930',
            agent_name='signal-decider',
            agent_version='test',
            decision_run_id=request_run_id,
            feedback_received_at=datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc),
            policy_version='signal_decider_policy_v1',
            decision_action='buy',
            preferred_action='buy',
            next_day_open_action='buy',
            entry_price_basis='trigger_limit',
            target_price=74_000,
            expected_return_pct=3.9,
            stop_loss_price=69_500,
            suggested_holding_period='10-30 trading days',
            confidence_score_10=7,
            conviction_score_10=7,
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
                'next_day_open_action': 'buy',
                'preferred_action': 'buy',
                'trigger_price': 71_000,
                'chase_limit_price': 71_500,
            },
            response_summary_payload={},
        )
        session.add(result_snapshot)
        session.flush()
        result_snapshot_id = result_snapshot.id

    attempt = {
        'event_id': 10,
        'symbol': '005930',
        'agent_name': 'signal-decider',
        'source_market_date': date(2026, 10, 20),
        'target_trade_date': date(2026, 10, 21),
        'source_snapshot_id': 1,
        'source_decision_run_id': 'source-run',
        'request_run_id': request_run_id,
        'context': {'schema_version': WAIT_RECHECK_CONTEXT_SCHEMA_VERSION},
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
                10,
                now=datetime(2026, 10, 21, 0, 5, tzinfo=timezone.utc),
            )

        assert result['status'] == 'completed'
        assert result['result_action'] == 'buy'
        dispatch.assert_called_once()
        assert dispatch.call_args.kwargs['roles'] == ('signal-decider',)
        finish_event.assert_called_once_with(
            event_id=10,
            request_run_id=request_run_id,
            status='completed',
            result_snapshot_id=result_snapshot_id,
            result_action='buy',
        )
    finally:
        with session_scope() as session:
            session.execute(
                delete(SignalDeciderDecisionSnapshot).where(
                    SignalDeciderDecisionSnapshot.id == result_snapshot_id
                )
            )

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
        observation = _supported_observation(
            candidate,
            end_at=datetime(
                2026,
                10,
                21,
                0,
                5,
                5,
                tzinfo=timezone.utc,
            ),
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
        assert load_validated_wait_recheck_context(
            event_id=event_id,
            symbol='005930',
            agent_name='short-term-decider',
        ) == context
        assert load_validated_wait_recheck_context(
            event_id=event_id,
            symbol='005930',
            agent_name='signal-decider',
        ) is None
        assert context['execution_authority']['direct_order_authority'] is False
        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderWaitRecheckEvent)
            ).scalars().all()
            assert len(rows) == 1
            assert rows[0].status == 'completed'
            assert rows[0].result_action == 'wait'

            original_context = rows[0].context_payload
            original_context_sha256 = rows[0].context_sha256

        exact_binding_drift = {
            **original_context,
            'symbol': ' 005930 ',
        }
        with session_scope() as session:
            drifted = session.get(SignalDeciderWaitRecheckEvent, event_id)
            assert drifted is not None
            drifted.context_payload = exact_binding_drift
            drifted.context_sha256 = _canonical_json_sha256(
                exact_binding_drift
            )
        assert load_validated_wait_recheck_context(
            event_id=event_id,
            symbol='005930',
            agent_name='short-term-decider',
        ) is None
        with session_scope() as session:
            drifted = session.get(SignalDeciderWaitRecheckEvent, event_id)
            assert drifted is not None
            drifted.context_payload = original_context
            drifted.context_sha256 = original_context_sha256

        swapped_candidate = build_wait_recheck_candidate(
            _snapshot(
                snapshot_id=snapshot_id + 100,
                symbol='000660',
                chase_limit_price=None,
                upgrade_trigger_price=None,
            ),
            target_trade_date=target_trade_date,
        )
        assert swapped_candidate is not None
        swapped_observation = _supported_observation(
            swapped_candidate,
            end_at=datetime(
                2026,
                10,
                21,
                0,
                5,
                11,
                tzinfo=timezone.utc,
            ),
        )
        swapped_context, swapped_context_sha256 = (
            canonical_wait_recheck_context(
                event_id=event_id + 1,
                observation=swapped_observation,
                market_state='OPEN',
                market_server_time='2026-10-21T09:05:11+09:00',
            )
        )
        with session_scope() as session:
            swapped = session.get(SignalDeciderWaitRecheckEvent, event_id)
            assert swapped is not None
            swapped.status = 'queued'
            swapped.attempt_count = 0
            swapped.request_run_id = None
            swapped.context_payload = swapped_context
            swapped.context_sha256 = swapped_context_sha256

        assert load_validated_wait_recheck_context(
            event_id=event_id,
            symbol='005930',
            agent_name='short-term-decider',
        ) is None
        swapped_attempt = service._begin_attempt(
            event_id,
            now=datetime(2026, 10, 21, 0, 5, 30, tzinfo=timezone.utc),
        )
        assert swapped_attempt is None
        with session_scope() as session:
            expired = session.get(SignalDeciderWaitRecheckEvent, event_id)
            assert expired is not None
            assert expired.status == 'expired'
            assert expired.last_error_payload == {
                'code': 'wait_recheck_context_lineage_mismatch',
            }

            expired.status = 'queued'
            expired.attempt_count = 0
            expired.request_run_id = None
            expired.context_payload = original_context
            expired.context_sha256 = original_context_sha256
            expired.last_attempt_at = None
            expired.last_error_payload = None

        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderWaitRecheckEvent)
            ).scalars().all()
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


def test_stale_queued_event_immediately_restores_fresh_evidence_membership() -> None:
    source_market_date = date(2026, 10, 20)
    target_trade_date = date(2026, 10, 21)
    observed_at = datetime(2026, 10, 21, 0, 5, 5, tzinfo=timezone.utc)
    fixture = _snapshot()
    with session_scope() as session:
        session.execute(delete(SignalDeciderWaitRecheckEvent))
        session.execute(delete(SignalDeciderDecisionSnapshot))
        snapshot = SignalDeciderDecisionSnapshot(
            feedback_id=None,
            content_fingerprint='f' * 64,
            market_date=source_market_date,
            symbol='005930',
            agent_name='short-term-decider',
            agent_version='test',
            decision_run_id='stale-queued-source',
            feedback_received_at=fixture.feedback_received_at,
            policy_version='signal_decider_policy_v1',
            decision_action='wait',
            preferred_action='wait',
            next_day_open_action='wait',
            entry_price_basis='next_open',
            target_price=fixture.response_payload['target_price'],
            expected_return_pct=None,
            stop_loss_price=fixture.response_payload['stop_loss_price'],
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
            request_payload=fixture.request_payload,
            response_payload=fixture.response_payload,
            response_summary_payload={},
        )
        session.add(snapshot)

    service = SignalDeciderWaitRecheckService()
    try:
        with (
            patch.object(
                settings,
                'signal_decider_wait_recheck_monitoring_enabled',
                True,
            ),
            patch(
                'app.agent_context.wait_recheck_service.'
                'previous_krx_trading_date',
                return_value=source_market_date,
            ),
        ):
            source = service.load_subscription_source(
                target_trade_date=target_trade_date
            )
            candidate = source.candidates[0]
            event_id = service._claim_observation(
                observation=_supported_observation(
                    candidate,
                    end_at=observed_at,
                ),
                market_state='OPEN',
                market_server_time=observed_at.isoformat(),
            )
            assert event_id is not None

            queued_source = service.load_subscription_source(
                target_trade_date=target_trade_date
            )
            assert queued_source.candidates == ()
            assert service.runtime_snapshot()['candidate_count'] == 0

            stale_now = observed_at + timedelta(
                seconds=(
                    float(settings.kis_realtime_price_stale_after_seconds)
                    + 1
                )
            )
            assert service._begin_attempt(event_id, now=stale_now) is None

            # _begin_attempt refreshes only after the expiry commit. No manual
            # membership poll is needed to resume collecting a new window.
            snapshot = service.runtime_snapshot()
            assert snapshot['candidate_symbols'] == ['005930']
            assert snapshot['candidate_count'] == 1
            with session_scope() as session:
                expired = session.get(
                    SignalDeciderWaitRecheckEvent,
                    event_id,
                )
                assert expired is not None
                assert expired.status == 'expired'
                assert expired.last_error_payload == {
                    'code': 'wait_recheck_observation_stale'
                }
    finally:
        with session_scope() as session:
            session.execute(delete(SignalDeciderWaitRecheckEvent))
            session.execute(delete(SignalDeciderDecisionSnapshot))


def test_completed_wait_rearms_generation_two_after_cooldown_and_caps_lineage() -> None:
    source_market_date = date(2026, 10, 20)
    target_trade_date = date(2026, 10, 21)
    completed_at = datetime(2026, 10, 21, 0, 6, tzinfo=timezone.utc)

    def new_wait_snapshot(
        *,
        decision_run_id: str,
        fingerprint: str,
        received_at: datetime,
    ) -> SignalDeciderDecisionSnapshot:
        fixture = _snapshot(
            target_price=76_000,
            stop_loss_price=69_000,
            chase_limit_price=72_000,
        )
        return SignalDeciderDecisionSnapshot(
            feedback_id=None,
            content_fingerprint=fingerprint,
            market_date=source_market_date,
            symbol='005930',
            agent_name='short-term-decider',
            agent_version='test',
            decision_run_id=decision_run_id,
            feedback_received_at=received_at,
            policy_version='signal_decider_policy_v1',
            decision_action='wait',
            preferred_action='wait',
            next_day_open_action='wait',
            entry_price_basis='next_open',
            target_price=76_000,
            expected_return_pct=None,
            stop_loss_price=69_000,
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
            request_payload=fixture.request_payload,
            response_payload=fixture.response_payload,
            response_summary_payload={},
            created_at=received_at,
        )

    with session_scope() as session:
        session.execute(delete(SignalDeciderWaitRecheckEvent))
        session.execute(delete(SignalDeciderDecisionSnapshot))
        source_snapshot = new_wait_snapshot(
            decision_run_id='generation-one-source',
            fingerprint='a' * 64,
            received_at=datetime(
                2026,
                10,
                20,
                10,
                tzinfo=timezone.utc,
            ),
        )
        result_snapshot = new_wait_snapshot(
            decision_run_id='generation-one-result',
            fingerprint='b' * 64,
            received_at=datetime(
                2026,
                10,
                20,
                11,
                tzinfo=timezone.utc,
            ),
        )
        session.add_all([source_snapshot, result_snapshot])
        session.flush()
        source_candidate = build_wait_recheck_candidate(
            source_snapshot,
            target_trade_date=target_trade_date,
        )
        assert source_candidate is not None
        source_observation = _supported_observation(
            source_candidate,
            end_at=datetime(
                2026,
                10,
                21,
                0,
                5,
                5,
                tzinfo=timezone.utc,
            ),
            ask_prices=(71_500, 71_500, 71_500),
            volumes=(100_000, 100_100, 100_200),
        )
        generation_one = SignalDeciderWaitRecheckEvent(
            generation=1,
            rearmed_from_event_id=None,
            source_snapshot_id=source_snapshot.id,
            result_snapshot_id=result_snapshot.id,
            source_snapshot_fingerprint=source_snapshot.content_fingerprint,
            source_market_date=source_market_date,
            target_trade_date=target_trade_date,
            symbol='005930',
            agent_name='short-term-decider',
            trigger_price=source_candidate.trigger_price,
            chase_limit_price=source_candidate.chase_limit_price,
            target_price=source_candidate.target_price,
            stop_loss_price=source_candidate.stop_loss_price,
            observed_price=source_observation.observed_price,
            observed_current_price=source_observation.current_price,
            observed_bid_price=source_observation.bid_price,
            price_basis=source_observation.price_basis,
            quote_source=source_observation.quote_source,
            price_as_of=source_observation.price_as_of,
            observed_at=source_observation.received_at,
            status='completed',
            attempt_count=1,
            result_action='wait',
            context_payload={},
            context_sha256='pending',
            completed_at=completed_at,
        )
        session.add(generation_one)
        session.flush()
        generation_one_context, generation_one_sha256 = (
            canonical_wait_recheck_context(
                event_id=generation_one.id,
                observation=source_observation,
                market_state='OPEN',
                market_server_time='2026-10-21T09:05:05+09:00',
            )
        )
        generation_one.context_payload = generation_one_context
        generation_one.context_sha256 = generation_one_sha256
        generation_one_id = generation_one.id
        result_snapshot_id = result_snapshot.id

    service = SignalDeciderWaitRecheckService()
    try:
        with (
            patch.object(
                settings,
                'signal_decider_wait_recheck_monitoring_enabled',
                True,
            ),
            patch.object(
                settings,
                'signal_decider_wait_recheck_max_generations_per_day',
                2,
            ),
            patch(
                'app.agent_context.wait_recheck_service.'
                'previous_krx_trading_date',
                return_value=source_market_date,
            ),
            patch(
                'app.agent_context.wait_recheck_service._utcnow',
                return_value=completed_at,
            ),
        ):
            source = service.load_subscription_source(
                target_trade_date=target_trade_date
            )
            assert source.symbols == ('005930',)
            assert len(source.candidates) == 1
            generation_two = source.candidates[0]
            rearm_not_before = completed_at + timedelta(
                seconds=(
                    settings.signal_decider_wait_recheck_rearm_cooldown_seconds
                )
            )
            assert generation_two.source_snapshot_id == result_snapshot_id
            assert generation_two.generation == 2
            assert generation_two.rearmed_from_event_id == generation_one_id
            assert generation_two.rearm_not_before == rearm_not_before

            assert _enqueue_supported_window(
                service,
                candidate=generation_two,
                end_at=rearm_not_before - timedelta(seconds=1),
                ask_prices=(71_000, 71_000, 71_000),
                volumes=(200_000, 200_100, 200_200),
                replace_cache=False,
            ) == (False, False, False)
            assert service.pop_pending_observations(limit=10) == []

            assert _enqueue_supported_window(
                service,
                candidate=generation_two,
                end_at=rearm_not_before + timedelta(seconds=5),
                ask_prices=(71_000, 71_000, 71_000),
                volumes=(200_300, 200_400, 200_500),
                replace_cache=False,
            ) == (False, False, True)
            observations = service.pop_pending_observations(limit=10)
            assert len(observations) == 1
            observation = observations[0]
            assert 'observed_entry_price_improved' in (
                observation.material_change_reasons
            )
            assert 'cumulative_volume_growth' in (
                observation.material_change_reasons
            )

            generation_two_id = service._claim_observation(
                observation=observation,
                market_state='OPEN',
                market_server_time=rearm_not_before.isoformat(),
            )
            assert generation_two_id is not None
            assert service._claim_observation(
                observation=observation,
                market_state='OPEN',
                market_server_time=rearm_not_before.isoformat(),
            ) == generation_two_id
            assert load_validated_wait_recheck_context(
                event_id=generation_two_id,
                symbol='005930',
                agent_name='short-term-decider',
            ) is not None

            with session_scope() as session:
                parent = session.get(
                    SignalDeciderWaitRecheckEvent,
                    generation_one_id,
                )
                assert parent is not None
                parent.status = 'failed'
            assert load_validated_wait_recheck_context(
                event_id=generation_two_id,
                symbol='005930',
                agent_name='short-term-decider',
            ) is None
            with session_scope() as session:
                parent = session.get(
                    SignalDeciderWaitRecheckEvent,
                    generation_one_id,
                )
                assert parent is not None
                parent.status = 'completed'

            with session_scope() as session:
                child = session.get(
                    SignalDeciderWaitRecheckEvent,
                    generation_two_id,
                )
                assert child is not None
                original_child_context = deepcopy(child.context_payload)
                lineage_tampered = deepcopy(original_child_context)
                lineage_tampered['generation_lineage'][
                    'prior_observed_price'
                ] += 1
                child.context_payload = lineage_tampered
                child.context_sha256 = _canonical_json_sha256(
                    lineage_tampered
                )
            assert load_validated_wait_recheck_context(
                event_id=generation_two_id,
                symbol='005930',
                agent_name='short-term-decider',
            ) is None

            with session_scope() as session:
                child = session.get(
                    SignalDeciderWaitRecheckEvent,
                    generation_two_id,
                )
                assert child is not None
                reason_tampered = deepcopy(original_child_context)
                reason_tampered['generation_lineage'][
                    'material_change_reasons'
                ] = ['bid_depth_imbalance_improved']
                child.context_payload = reason_tampered
                child.context_sha256 = _canonical_json_sha256(
                    reason_tampered
                )
            assert load_validated_wait_recheck_context(
                event_id=generation_two_id,
                symbol='005930',
                agent_name='short-term-decider',
            ) is None

            with session_scope() as session:
                child = session.get(
                    SignalDeciderWaitRecheckEvent,
                    generation_two_id,
                )
                assert child is not None
                child.context_payload = original_child_context
                child.context_sha256 = _canonical_json_sha256(
                    original_child_context
                )

            with session_scope() as session:
                rows = session.execute(
                    select(SignalDeciderWaitRecheckEvent).order_by(
                        SignalDeciderWaitRecheckEvent.generation.asc()
                    )
                ).scalars().all()
                assert len(rows) == 2
                assert rows[1].id == generation_two_id
                assert rows[1].generation == 2
                assert rows[1].rearmed_from_event_id == generation_one_id
                assert rows[1].source_snapshot_id == result_snapshot_id

                capped_result_snapshot = new_wait_snapshot(
                    decision_run_id='generation-two-result',
                    fingerprint='c' * 64,
                    received_at=datetime(
                        2026,
                        10,
                        20,
                        12,
                        tzinfo=timezone.utc,
                    ),
                )
                session.add(capped_result_snapshot)
                session.flush()
                rows[1].status = 'completed'
                rows[1].result_action = 'wait'
                rows[1].result_snapshot_id = capped_result_snapshot.id
                rows[1].completed_at = rearm_not_before + timedelta(seconds=1)

            capped = service.load_subscription_source(
                target_trade_date=target_trade_date
            )
            assert capped.symbols == ()
            assert capped.candidates == ()
    finally:
        with session_scope() as session:
            session.execute(delete(SignalDeciderWaitRecheckEvent))
            session.execute(delete(SignalDeciderDecisionSnapshot))


def test_failed_attempt_waits_for_fresh_evidence_and_archives_retry_context() -> None:
    source_market_date = date(2026, 10, 20)
    target_trade_date = date(2026, 10, 21)
    first_observed_at = datetime(
        2026,
        10,
        21,
        0,
        5,
        5,
        tzinfo=timezone.utc,
    )
    fixture = _snapshot()
    with session_scope() as session:
        session.execute(delete(SignalDeciderWaitRecheckEvent))
        session.execute(delete(SignalDeciderDecisionSnapshot))
        snapshot = SignalDeciderDecisionSnapshot(
            feedback_id=None,
            content_fingerprint='d' * 64,
            market_date=source_market_date,
            symbol='005930',
            agent_name='short-term-decider',
            agent_version='test',
            decision_run_id='fresh-retry-source',
            feedback_received_at=fixture.feedback_received_at,
            policy_version='signal_decider_policy_v1',
            decision_action='wait',
            preferred_action='wait',
            next_day_open_action='wait',
            entry_price_basis='next_open',
            target_price=fixture.response_payload['target_price'],
            expected_return_pct=None,
            stop_loss_price=fixture.response_payload['stop_loss_price'],
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
            request_payload=fixture.request_payload,
            response_payload=fixture.response_payload,
            response_summary_payload={},
        )
        session.add(snapshot)
        session.flush()
        snapshot_id = snapshot.id

    service = SignalDeciderWaitRecheckService()
    try:
        with (
            patch.object(
                settings,
                'signal_decider_wait_recheck_monitoring_enabled',
                True,
            ),
            patch.object(
                settings,
                'signal_decider_wait_recheck_max_daily_events',
                1,
            ),
            patch(
                'app.agent_context.wait_recheck_service.'
                'previous_krx_trading_date',
                return_value=source_market_date,
            ),
        ):
            source = service.load_subscription_source(
                target_trade_date=target_trade_date
            )
            candidate = source.candidates[0]
            assert candidate.source_snapshot_id == snapshot_id
            first_observation = _supported_observation(
                candidate,
                end_at=first_observed_at,
            )
            event_id = service._claim_observation(
                observation=first_observation,
                market_state='OPEN',
                market_server_time=first_observed_at.isoformat(),
            )
            assert event_id is not None
            first_attempt = service._begin_attempt(
                event_id,
                now=first_observed_at,
            )
            assert first_attempt is not None
            first_context = first_attempt['context']
            failure_completed_at = first_observed_at + timedelta(minutes=10)
            with patch(
                'app.agent_context.wait_recheck_service._utcnow',
                return_value=failure_completed_at,
            ):
                service._finish_event(
                    event_id=event_id,
                    request_run_id=first_attempt['request_run_id'],
                    status='failed',
                    error_code='provider_rate_limited',
                )

            # A decider may persist its snapshot before the surrounding
            # orchestration call reports a non-terminal failure. That
            # unaccepted snapshot must not displace the failed event's
            # immutable retry source.
            with session_scope() as session:
                original = session.get(
                    SignalDeciderDecisionSnapshot,
                    snapshot_id,
                )
                assert original is not None
                copied_values = {
                    column.name: deepcopy(
                        getattr(original, column.name)
                    )
                    for column in SignalDeciderDecisionSnapshot.__table__.columns
                    if column.name not in {'id', 'created_at', 'updated_at'}
                }
                copied_values['content_fingerprint'] = 'e' * 64
                copied_values['decision_run_id'] = first_attempt[
                    'request_run_id'
                ]
                session.add(
                    SignalDeciderDecisionSnapshot(**copied_values)
                )

            retry_ready_at = failure_completed_at + timedelta(
                seconds=(
                    settings.signal_decider_wait_recheck_retry_cooldown_seconds
                )
            )
            assert service._retry_event_ids(
                target_trade_date=target_trade_date,
                limit=10,
                now=retry_ready_at,
            ) == []

            retry_source = service.load_subscription_source(
                target_trade_date=target_trade_date
            )
            assert len(retry_source.candidates) == 1
            retry_candidate = retry_source.candidates[0]
            assert retry_candidate.retry_event_id == event_id
            assert retry_candidate.rearm_not_before == retry_ready_at
            assert _enqueue_supported_window(
                service,
                candidate=retry_candidate,
                end_at=retry_ready_at + timedelta(seconds=5),
                volumes=(101_000, 101_100, 101_200),
                replace_cache=False,
            ) == (False, False, True)
            fresh_observations = service.pop_pending_observations(limit=10)
            assert len(fresh_observations) == 1
            assert service._claim_observation(
                observation=fresh_observations[0],
                market_state='OPEN',
                market_server_time=(
                    retry_ready_at + timedelta(seconds=5)
                ).isoformat(),
            ) == event_id

        with session_scope() as session:
            events = session.execute(
                select(SignalDeciderWaitRecheckEvent)
            ).scalars().all()
            assert len(events) == 1
            event = events[0]
            assert event.status == 'queued'
            assert event.attempt_count == 1
            assert event.request_run_id is None
            assert event.last_error_payload is None
            assert len(event.attempt_history_payload) == 1
            archived = event.attempt_history_payload[0]
            assert archived['status'] == 'failed'
            assert archived['request_run_id'] == first_attempt['request_run_id']
            assert archived['last_error_payload'] == {
                'code': 'provider_rate_limited'
            }
            assert archived['context_payload'] == first_context
            assert event.context_sha256 != archived['context_sha256']
    finally:
        with session_scope() as session:
            session.execute(delete(SignalDeciderWaitRecheckEvent))
            session.execute(delete(SignalDeciderDecisionSnapshot))
