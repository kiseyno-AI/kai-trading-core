from __future__ import annotations

import asyncio
from contextlib import nullcontext
from unittest.mock import AsyncMock, patch

from app.core.settings import settings
from app.eod.daily_pipeline_stages import (
    DailyPipelineStageService,
    inspect_daily_pipeline_required_provider_readiness,
)
from app.signals.market_risk_snapshot import (
    BLOCKED_MISSING_CREDENTIAL_CLASSIFICATION,
    REQUIRED_PROVIDER_ADMISSION_SCHEMA_VERSION,
    DailyMarketRiskSnapshotService,
    build_required_provider_admission,
)


def _execute_stage(
    service: DailyPipelineStageService,
    stage_id: str,
    *,
    exact_scope_symbols: list[str] | None = None,
) -> dict:
    return service._execute_stage(
        stage_id=stage_id,
        market_date='2026-10-30',
        symbols=['005930', '000660'],
        markets=['KOSPI', 'KOSDAQ'],
        force=False,
        bypass_prerequisite_gate=False,
        fs_div='CFS',
        symbol_limit=None,
        symbol_offset=0,
        capture_limit=500,
        retry_cycles=None,
        feedback_max_cycles=3,
        feedback_outer_batch_size=20,
        reuse_existing_feedback=True,
        exact_scope_symbols=exact_scope_symbols,
    )


def test_required_provider_preflight_excludes_optional_naver_and_fred() -> None:
    with (
        patch.object(settings, 'dart_api_key', ''),
        patch.object(settings, 'kis_app_key', ''),
        patch.object(settings, 'kis_app_secret', ''),
        patch.object(settings, 'naver_search_client_id', ''),
        patch.object(settings, 'naver_search_client_secret', ''),
        patch.object(settings, 'fred_api_key', ''),
    ):
        optional_only = inspect_daily_pipeline_required_provider_readiness(
            [
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
            ]
        )
        required = inspect_daily_pipeline_required_provider_readiness(
            [
                'fundamentals_daily_refresh',
                'market_risk_filter',
                'external_news_collect',
                'macro_context_refresh',
            ]
        )

    assert optional_only['ready'] is True
    assert optional_only['providers'] == {}
    assert required['schema_version'] == (
        REQUIRED_PROVIDER_ADMISSION_SCHEMA_VERSION
    )
    assert required['classification'] == (
        BLOCKED_MISSING_CREDENTIAL_CLASSIFICATION
    )
    assert required['attempt_started'] is False
    assert required['consumes_retry_budget'] is False
    assert set(required['providers']) == {'dart', 'kis'}
    assert set(required['missing_credentials']) == {
        'DART_API_KEY',
        'KIS_APP_KEY',
        'KIS_APP_SECRET',
    }
    assert all(
        blocker['classification']
        == BLOCKED_MISSING_CREDENTIAL_CLASSIFICATION
        and blocker['attempt_started'] is False
        and blocker['consumes_retry_budget'] is False
        and blocker['recoverable'] is True
        for blocker in required['blockers']
    )


def test_required_provider_preflight_becomes_ready_after_configuration() -> None:
    with (
        patch.object(settings, 'dart_api_key', 'dart-key'),
        patch.object(settings, 'kis_app_key', 'kis-key'),
        patch.object(settings, 'kis_app_secret', 'kis-secret'),
    ):
        result = inspect_daily_pipeline_required_provider_readiness(
            [
                'fundamentals_daily_refresh',
                'market_risk_snapshots',
                'market_risk_filter',
            ]
        )

    assert result['ready'] is True
    assert result['classification'] == 'ready'
    assert result['blockers'] == []
    assert all(
        provider['ready'] is True
        and provider['missing_credentials'] == []
        for provider in result['providers'].values()
    )


def test_dart_stage_is_blocked_before_collection_without_retry_debt() -> None:
    service = DailyPipelineStageService(
        required_provider_readiness=(
            inspect_daily_pipeline_required_provider_readiness
        )
    )
    refresh = AsyncMock()

    with (
        patch.object(settings, 'dart_api_key', ''),
        patch(
            'app.eod.daily_pipeline_stages.'
            'dart_fundamentals_capture_service.'
            'run_daily_incremental_refresh',
            new=refresh,
        ),
    ):
        result = _execute_stage(
            service,
            'fundamentals_daily_refresh',
            exact_scope_symbols=['005930', '000660'],
        )

    refresh.assert_not_awaited()
    assert result['status'] == 'blocked'
    assert result['classification'] == (
        BLOCKED_MISSING_CREDENTIAL_CLASSIFICATION
    )
    assert result['provider'] == 'dart'
    assert result['deferred_symbols'] == ['005930', '000660']
    assert result['events_failed'] == 0
    assert 'failed_symbols' not in result
    assert '_retry_metadata' not in result
    assert DailyPipelineStageService._resolve_result_status(result) == (
        'blocked'
    )
    assert DailyPipelineStageService._extract_stage_counts(
        stage_id='fundamentals_daily_refresh',
        result=result,
    ) == {
        'processed': 0,
        'total': 0,
        'succeeded': 0,
        'failed': 0,
    }


def test_kis_pipeline_stages_block_before_snapshot_or_filter_collection() -> None:
    service = DailyPipelineStageService(
        required_provider_readiness=(
            inspect_daily_pipeline_required_provider_readiness
        )
    )
    collect_snapshots = AsyncMock()
    rebuild_filter = AsyncMock()

    with (
        patch.object(settings, 'kis_app_key', ''),
        patch.object(settings, 'kis_app_secret', ''),
        patch(
            'app.eod.daily_pipeline_stages.'
            'daily_market_risk_snapshot_service.collect_for_market_date',
            new=collect_snapshots,
        ),
        patch(
            'app.eod.daily_pipeline_stages.'
            'daily_market_risk_filter_service.rebuild_for_market_date',
            new=rebuild_filter,
        ),
    ):
        snapshot_result = _execute_stage(
            service,
            'market_risk_snapshots',
            exact_scope_symbols=['005930', '000660'],
        )
        filter_result = _execute_stage(
            service,
            'market_risk_filter',
            exact_scope_symbols=['005930', '000660'],
        )

    collect_snapshots.assert_not_awaited()
    rebuild_filter.assert_not_awaited()
    for stage_id, result in (
        ('market_risk_snapshots', snapshot_result),
        ('market_risk_filter', filter_result),
    ):
        assert result['status'] == 'blocked'
        assert result['classification'] == (
            BLOCKED_MISSING_CREDENTIAL_CLASSIFICATION
        )
        assert result['provider'] == 'kis'
        assert result['failed_symbols'] == []
        assert result['deferred_symbols'] == ['005930', '000660']
        assert '_retry_metadata' not in result
        assert 'scope_finalization' not in result
        assert DailyPipelineStageService._extract_stage_counts(
            stage_id=stage_id,
            result=result,
        ) == {
            'processed': 0,
            'total': 0,
            'succeeded': 0,
            'failed': 0,
        }


class _FailIfCalledKISClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch_domestic_quote_risk_status(
        self,
        symbol: str,
        *,
        market_code: str,
    ) -> dict:
        self.calls.append(symbol)
        raise AssertionError('KIS must not be called while credentials are missing')


def test_kis_collector_blocks_before_symbol_lookup_fetch_or_persistence() -> None:
    client = _FailIfCalledKISClient()
    readiness = lambda: build_required_provider_admission(
        provider='kis',
        missing_credentials=['KIS_APP_KEY', 'KIS_APP_SECRET'],
        required_stage_ids=[
            'market_risk_snapshots',
            'market_risk_filter',
        ],
    )
    service = DailyMarketRiskSnapshotService(
        client=client,
        provider_readiness=readiness,
    )

    with (
        patch(
            'app.repositories.eod_run_repository.'
            'daily_price_bar_writer_admission_lock',
            return_value=nullcontext(),
        ),
        patch.object(service, '_load_symbol_rows') as load_symbols,
        patch(
            'app.signals.market_risk_snapshot.'
            'replace_daily_derived_results_exact_scope'
        ) as replace_exact_scope,
        patch(
            'app.signals.market_risk_snapshot.'
            'upsert_daily_market_risk_snapshots'
        ) as bulk_upsert,
    ):
        result = asyncio.run(
            service.collect_for_market_date(
                market_date='2026-10-30',
                symbols=['005930', '000660'],
                exact_scope_symbols=['005930', '000660'],
            )
        )

    load_symbols.assert_not_called()
    replace_exact_scope.assert_not_called()
    bulk_upsert.assert_not_called()
    assert client.calls == []
    assert result['status'] == 'blocked'
    assert result['classification'] == (
        BLOCKED_MISSING_CREDENTIAL_CLASSIFICATION
    )
    assert result['snapshots_failed'] == 0
    assert result['failed_symbols'] == []
    assert result['deferred_symbols'] == ['005930', '000660']
    assert result['provider_admission']['attempt_started'] is False
    assert result['provider_admission']['consumes_retry_budget'] is False
    assert result['provider_admission']['recoverable'] is True
    assert 'scope_finalization' not in result
    assert '_retry_metadata' not in result
