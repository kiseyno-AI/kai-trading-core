from contextlib import ExitStack, contextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.db import engine
from app.db_models import DailyPipelineRun, DailyPipelineStageRun, EODCollectionRun
from app.eod.daily_pipeline_state import DailyPipelineRunStateService
from app.eod.market_membership import market_membership_sha256
from app.eod.pykrx_backfill import PykrxBackfillService, PykrxSymbol
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome


@pytest.fixture
def pipeline_sessions():
    schema = 'audit_pipeline_cleanup_' + uuid4().hex
    with engine.begin() as conn:
        conn.execute(CreateSchema(schema))
    try:
        test_engine = engine.execution_options(schema_translate_map={None: schema})
        for model in (DailyPipelineRun, DailyPipelineStageRun, EODCollectionRun):
            model.__table__.create(test_engine)
        sessions = sessionmaker(bind=test_engine, expire_on_commit=False)

        @contextmanager
        def isolated_session_scope(**_options):
            with sessions.begin() as session:
                yield session

        with (
            patch('app.eod.daily_pipeline_state.session_scope', isolated_session_scope),
            patch('app.repositories.eod_run_repository.session_scope', isolated_session_scope),
        ):
            yield sessions
    finally:
        with engine.begin() as conn:
            conn.execute(DropSchema(schema, cascade=True))


def _published_scope(stage):
    symbols = ['005930']
    return {
        **build_exact_symbol_scope_outcome(
            manifest_version=f'{stage}_scope_outcomes_v1',
            selected_symbols=symbols,
            successful_symbols=symbols,
            retry_debt_symbols=[],
        ),
        'scope_finalization': {
            'stage': stage,
            'market_date': '2026-04-03',
            'exact_symbol_count': 1,
            'exact_membership_sha256': market_membership_sha256(symbols),
            'successful_symbol_count': 1,
            'successful_membership_sha256': market_membership_sha256(symbols),
            'deleted_stale_row_count': 0,
            'ready': True,
        },
    }


@pytest.mark.parametrize(
    ('failure_target', 'selected_stages'),
    [
        ('daily_price_context_service.rebuild_for_market_date', ()),
        ('pykrx_enrichment_service.collect_for_market_date', ('pykrx_enrichment',)),
        ('daily_technical_snapshot_service.rebuild_for_market_date', ('technical_snapshots',)),
        ('daily_action_summary_service.rebuild_for_market_date', ('technical_snapshots', 'action_summaries')),
        ('compute_and_persist_daily_trending_ranks', ('technical_snapshots', 'trending_ranks')),
        ('daily_signal_feature_service.refresh_for_market_date', ('signal_features',)),
        ('daily_market_risk_filter_service.rebuild_for_market_date', ('market_risk_filter',)),
    ],
)
def test_derived_database_failure_terminalizes_persisted_runs(
    pipeline_sessions, failure_target, selected_stages,
):
    service = PykrxBackfillService()
    service.symbol_pause_seconds = 0
    error = OperationalError(
        'INSERT INTO derived_context', {}, RuntimeError('derived database write failed'),
    )
    dependency_prefix = 'app.eod.pykrx_backfill.'
    runner_prefix = 'app.eod.pykrx_pipeline_runner.'
    with ExitStack() as stack:
        stack.enter_context(patch.object(service, 'load_active_universe', return_value=[
            PykrxSymbol('005930', 'KOSPI', name_ko='Samsung'),
        ]))
        stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
        stack.enter_context(patch.object(service, '_map_symbol_bars', return_value=[{
            'trade_date': '2026-04-03', 'open_price': 1, 'high_price': 2,
            'low_price': 1, 'close_price': 2, 'volume': 3, 'turnover': 4,
        }]))
        stack.enter_context(patch(dependency_prefix + '_get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False)))
        stack.enter_context(patch(dependency_prefix + 'persist_daily_price_bars', return_value=1))
        stack.enter_context(patch(dependency_prefix + 'daily_pipeline_run_state_service', DailyPipelineRunStateService()))
        stack.enter_context(patch(dependency_prefix + 'log_fetch_event'))
        stack.enter_context(patch(runner_prefix + '_enrich_bars_with_market_turnover', side_effect=lambda **kwargs: kwargs['bars']))
        stack.enter_context(patch(runner_prefix + '_validated_exact_piotroski_membership_symbols', return_value=['005930']))
        stack.enter_context(patch(runner_prefix + '_required_provider_blocked_result', return_value=None))
        stack.enter_context(patch(runner_prefix + '_finalize_daily_stage_scope', return_value={'ready': True, 'deleted_stale_row_count': 0}))
        stack.enter_context(patch(dependency_prefix + 'daily_price_context_service.rebuild_for_market_date', return_value={
            'contexts_persisted': 1, 'contexts_failed': 0, **_published_scope('price_contexts'),
        }))
        stack.enter_context(patch(dependency_prefix + 'daily_technical_snapshot_service.rebuild_for_market_date', return_value={
            'snapshots_persisted': 1, 'snapshots_failed': 0, **_published_scope('technical_snapshots'),
        }))
        failed_write = stack.enter_context(patch(dependency_prefix + failure_target, side_effect=error))
        with pytest.raises(OperationalError) as raised:
            service._run_market_data_pipeline(
                start_date='20260403', end_date='20260403', markets=('KOSPI', 'KOSDAQ'),
                trigger='auto_close_scheduler', refresh_universe=False, mode='daily_auto',
                selected_stages=('price_collection', *selected_stages),
                chain_fundamentals_followup=False, track_daily_pipeline=True,
            )
        assert raised.value is error
        failed_write.assert_called_once()

    with pipeline_sessions() as session:
        eod_run = session.execute(select(EODCollectionRun)).scalar_one()
        official_run = session.execute(select(DailyPipelineRun)).scalar_one()
        assert eod_run.status == 'failed'
        assert official_run.status == 'failed'
        assert eod_run.completed_at is not None
        assert official_run.completed_at is not None
        assert eod_run.symbols_succeeded == 1
        assert eod_run.symbols_failed == 0
        assert eod_run.trade_date == date(2026, 4, 3)
        assert 'derived database write failed' in eod_run.error_message
        assert 'derived database write failed' in official_run.error_message
        stages = session.execute(select(DailyPipelineStageRun)).scalars().all()
        assert stages
        assert all(stage.status not in {'pending', 'running'} for stage in stages)
