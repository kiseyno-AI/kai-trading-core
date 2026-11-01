import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_eod
from app.eod.auto_pykrx import AutoPykrxEODScheduler
from app.eod.pykrx_backfill import _log_pykrx_fetch
from app.eod.pykrx_pipeline_support import compact_daily_pipeline_result
from app.market.clock import MarketState
from app.repositories.eod_run_repository import AutoEODRecoveryTarget


def _non_trading_market(
    state: str,
    *,
    current_date: str = '2026-09-13',
    latest_session: str = '2026-09-11',
) -> MarketState:
    return MarketState(
        market='KRX',
        state=state,
        is_trading_day=False,
        is_open=False,
        server_time=f'{current_date}T14:00:00+09:00',
        session_open_at=None,
        session_close_at=None,
        latest_valid_trading_date=latest_session,
    )


def _terminal_run(*, trade_date: str = '2026-09-11') -> dict:
    return {
        'id': 594,
        'market': 'KRX',
        'trade_date': trade_date,
        'source': 'pykrx',
        'trigger': 'auto_close_scheduler',
        'status': 'completed',
        'symbols_total': 360,
        'symbols_succeeded': 357,
        'symbols_failed': 0,
        'error_message': None,
        'started_at': '2026-09-11T09:00:00+00:00',
        'completed_at': '2026-09-11T10:00:00+00:00',
        'run_metadata': {
            'daily_pipeline_run_id': 596,
            'failed_symbols': [],
            'downstream_retry_symbols': [],
            'large_payload': {'symbol_rows': ['x' * 1000] * 1000},
        },
    }


class AutoPykrxWeekendContractTests(unittest.IsolatedAsyncioTestCase):
    def test_daily_pipeline_result_compaction_is_idempotent(self) -> None:
        raw_result = {
            'triggered': False,
            'existing_run': _terminal_run(),
            'items': [
                {'symbol': str(index).zfill(6), 'details': {'value': index}}
                for index in range(20)
            ],
            'overflow': {f'field_{index}': index for index in range(50)},
            'run_metadata': {'large_payload': ['x' * 1000] * 100},
        }

        compacted_once = compact_daily_pipeline_result(raw_result)
        compacted_twice = compact_daily_pipeline_result(compacted_once)

        self.assertEqual(compacted_twice, compacted_once)
        assert compacted_once is not None
        self.assertEqual(
            compacted_once['existing_run']['metadata_summary'][
                'daily_pipeline_run_id'
            ],
            596,
        )
        self.assertEqual(compacted_once['_omitted_field_count'], 1)

    async def test_non_trading_day_with_unchanged_terminal_proof_skips_provider_pipeline(
        self,
    ) -> None:
        for state in ('WEEKEND', 'HOLIDAY'):
            with self.subTest(state=state):
                scheduler = AutoPykrxEODScheduler()
                market = _non_trading_market(state)
                with (
                    patch(
                        'app.eod.auto_pykrx.async_get_market_state',
                        new=AsyncMock(return_value=market),
                    ),
                    patch(
                        'app.eod.auto_pykrx._oldest_recovery_window_date',
                        new=AsyncMock(return_value='2026-09-07'),
                    ),
                    patch(
                        'app.eod.auto_pykrx.'
                        'load_oldest_outstanding_auto_eod_recovery_target',
                        return_value=None,
                    ),
                    patch(
                        'app.eod.auto_pykrx.load_latest_eod_collection_run',
                        return_value=_terminal_run(),
                    ),
                    patch.object(
                        scheduler,
                        '_run_scheduled_daily_pipeline',
                        new=AsyncMock(),
                    ) as run_pipeline,
                ):
                    await scheduler.schedule_daily_pipeline_if_due()

                run_pipeline.assert_not_awaited()
                self.assertIsNone(scheduler._task)
                self.assertEqual(scheduler.snapshot()['last_reason'], 'already_completed')

    async def test_next_session_without_terminal_proof_reactivates_pipeline(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = _non_trading_market(
            'WEEKEND',
            current_date='2026-09-20',
            latest_session='2026-09-18',
        )
        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                new=AsyncMock(return_value=market),
            ),
            patch(
                'app.eod.auto_pykrx._oldest_recovery_window_date',
                new=AsyncMock(return_value='2026-09-14'),
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=None,
            ),
            patch(
                'app.eod.auto_pykrx.load_latest_eod_collection_run',
                return_value=None,
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            assert scheduler._task is not None
            await scheduler._task

        run_pipeline.assert_awaited_once_with(
            '2026-09-18',
            markets=('KOSPI', 'KOSDAQ'),
        )

    async def test_recovery_debt_reactivates_pipeline_despite_terminal_latest_run(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = _non_trading_market('HOLIDAY')
        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                new=AsyncMock(return_value=market),
            ),
            patch(
                'app.eod.auto_pykrx._oldest_recovery_window_date',
                new=AsyncMock(return_value='2026-09-07'),
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date='2026-09-11',
                    eod_run_id=594,
                ),
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            assert scheduler._task is not None
            await scheduler._task

        run_pipeline.assert_awaited_once_with(
            '2026-09-11',
            markets=('KOSPI', 'KOSDAQ'),
            point_in_time_recovery=True,
            recovery_eod_run_id=594,
        )

    def test_status_snapshot_never_exposes_full_run_metadata_and_is_size_bounded(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        scheduler.state.last_result = {
            'triggered': False,
            'reason': 'already_completed',
            'market_date': '2026-09-11',
            'latest_available_trade_date': '2026-09-11',
            'existing_run': _terminal_run(),
            'items': [
                {'symbol': str(index).zfill(6), 'evidence': 'x' * 1000}
                for index in range(1000)
            ],
        }

        snapshot = scheduler.snapshot()
        encoded = json.dumps(snapshot, ensure_ascii=True)

        self.assertNotIn('run_metadata', encoded)
        self.assertLess(len(encoded), 20_000)
        self.assertEqual(snapshot['last_result']['existing_run']['id'], 594)
        self.assertEqual(
            snapshot['last_result']['existing_run']['symbols_succeeded'],
            357,
        )
        self.assertEqual(
            snapshot['last_result']['existing_run']['metadata_summary'][
                'daily_pipeline_run_id'
            ],
            596,
        )

    def test_daily_pipeline_audit_event_uses_the_same_bounded_projection(
        self,
    ) -> None:
        with patch('app.eod.pykrx_backfill.log_fetch_event') as log_fetch:
            _log_pykrx_fetch(
                'run_daily_pipeline_if_due',
                'skipped',
                triggered=False,
                reason='already_completed',
                existing_run=_terminal_run(),
                items=[
                    {'symbol': str(index).zfill(6), 'evidence': 'x' * 1000}
                    for index in range(1000)
                ],
            )

        payload = log_fetch.call_args.kwargs
        encoded = json.dumps(payload, ensure_ascii=True)
        self.assertNotIn('run_metadata', encoded)
        self.assertLess(len(encoded), 20_000)
        self.assertEqual(payload['existing_run']['id'], 594)
        self.assertEqual(
            payload['existing_run']['metadata_summary'][
                'daily_pipeline_run_id'
            ],
            596,
        )

    def test_eod_status_route_response_remains_bounded(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        scheduler.state.last_result = {
            'triggered': False,
            'reason': 'already_completed',
            'existing_run': _terminal_run(),
            'items': [{'evidence': 'x' * 1000}] * 1000,
        }
        market = SimpleNamespace(
            market='KRX',
            state='WEEKEND',
            is_trading_day=False,
            is_open=False,
            reason='weekend',
            server_time='2026-09-13T14:00:00+09:00',
            current_calendar_date='2026-09-13',
            latest_valid_trading_date='2026-09-11',
            market_session='weekend',
        )
        app = FastAPI()
        app.include_router(routes_eod.router)

        with (
            patch(
                'app.api.routes_eod.async_get_market_state',
                new=AsyncMock(return_value=market),
            ),
            patch(
                'app.api.routes_eod.async_build_market_session_metadata',
                new=AsyncMock(return_value={'status': 'weekend'}),
            ),
            patch(
                'app.api.routes_eod.auto_pykrx_eod_scheduler.snapshot',
                side_effect=scheduler.snapshot,
            ),
            patch(
                'app.api.routes_eod.job_state.snapshot',
                return_value={'status': 'idle'},
            ),
            patch(
                'app.api.routes_eod.master_refresh_state.snapshot',
                return_value={'status': 'idle'},
            ),
            patch(
                'app.api.routes_eod.background_runner.snapshot',
                return_value={'status': 'running'},
            ),
        ):
            response = TestClient(app).get('/eod/status')

        self.assertEqual(response.status_code, 200)
        self.assertLess(len(response.content), 30_000)
        self.assertNotIn(b'run_metadata', response.content)


if __name__ == '__main__':
    unittest.main()
