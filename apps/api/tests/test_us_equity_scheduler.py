import asyncio
from datetime import UTC, date, datetime

from app.us_equities import scheduler as scheduler_module
from app.us_equities.scheduler import USEquityPipelineScheduler, _stage_checkpoints, is_us_pipeline_due


def test_us_pipeline_due_uses_new_york_clock_independent_of_krx() -> None:
    # 22:00 UTC is 18:00 EDT and 07:00 the next day in Seoul.
    assert is_us_pipeline_due(
        now=datetime(2026, 8, 19, 22, 0, tzinfo=UTC),
        last_completed_market_date=None,
        scheduled_local_time='18:00',
    )


def test_us_pipeline_not_due_twice_or_on_exchange_holiday() -> None:
    assert not is_us_pipeline_due(
        now=datetime(2026, 8, 19, 23, 0, tzinfo=UTC),
        last_completed_market_date=date(2026, 8, 19),
        scheduled_local_time='18:00',
    )
    assert not is_us_pipeline_due(
        now=datetime(2026, 7, 3, 23, 0, tzinfo=UTC),
        last_completed_market_date=None,
        scheduled_local_time='18:00',
    )


def test_scheduler_uses_durable_claim_before_starting_pipeline(monkeypatch) -> None:
    monkeypatch.setattr(
        scheduler_module,
        'claim_us_equity_pipeline_run',
        lambda **kwargs: {'claimed': False, 'status': 'already_completed', 'run_id': 'prior-run'},
    )
    scheduler = USEquityPipelineScheduler()

    result = asyncio.run(
        scheduler.run_if_due(now=datetime(2026, 8, 19, 22, 0, tzinfo=UTC))
    )

    assert result == {
        'status': 'already_completed',
        'market_date': '2026-08-19',
        'run_id': 'prior-run',
    }


def test_scheduler_blocks_execution_when_agent_stage_is_partial(monkeypatch) -> None:
    class AsyncStage:
        async def refresh(self, **kwargs):
            return {'status': 'completed'}

        async def collect_and_screen(self, **kwargs):
            return {'status': 'completed'}

    class PartialAgents:
        def dispatch(self, **kwargs):
            return {'status': 'partial', 'failed': 1, 'run_id': 'agent-run'}

    execution_calls = []

    class Execution:
        async def dispatch(self, **kwargs):
            execution_calls.append(kwargs)
            return {'status': 'dispatched'}

    updates = []
    monkeypatch.setattr(scheduler_module, 'us_universe_refresh_service', AsyncStage())
    monkeypatch.setattr(scheduler_module, 'us_price_collection_service', AsyncStage())
    monkeypatch.setattr(scheduler_module, 'us_fundamental_collection_service', AsyncStage())
    monkeypatch.setattr(scheduler_module, 'us_agent_analysis_service', PartialAgents())
    monkeypatch.setattr(scheduler_module, 'us_execution_plan_service', Execution())
    monkeypatch.setattr(
        scheduler_module,
        'update_us_equity_pipeline_run',
        lambda **kwargs: updates.append(kwargs) or True,
    )

    result = asyncio.run(
        USEquityPipelineScheduler()._run_pipeline(date(2026, 8, 19), run_id='pipeline-run')
    )

    assert result['status'] == 'failed'
    assert execution_calls == []
    assert updates[-1]['status'] == 'failed'


def test_scheduler_renews_durable_lease_during_long_stage(monkeypatch) -> None:
    touches = []
    monkeypatch.setattr(scheduler_module, 'US_EQUITY_PIPELINE_LEASE_HEARTBEAT_SECONDS', 0.001)
    monkeypatch.setattr(
        scheduler_module,
        'touch_us_equity_pipeline_run',
        lambda **kwargs: touches.append(kwargs) or True,
    )

    async def exercise() -> None:
        task = asyncio.create_task(
            USEquityPipelineScheduler()._maintain_lease(
                market_date=date(2026, 8, 19),
                run_id='pipeline-run',
            )
        )
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(exercise())

    assert touches
    assert touches[0] == {'market_date': date(2026, 8, 19), 'run_id': 'pipeline-run'}


def test_stage_checkpoints_exclude_large_execution_payloads() -> None:
    checkpoints = _stage_checkpoints(
        {
            'agents': {'status': 'completed', 'run_id': 'agent-run', 'completed': 5, 'items': ['large']},
            'execution': {'status': 'ready', 'dry_run': True, 'payload': {'items': ['large']}},
        }
    )

    assert checkpoints == {
        'agents': {'status': 'completed', 'run_id': 'agent-run', 'completed': 5},
        'execution': {'status': 'ready', 'dry_run': True},
    }
