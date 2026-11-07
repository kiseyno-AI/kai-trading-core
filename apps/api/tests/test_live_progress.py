import json
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.eod import daily_pipeline_state as state_module
from app.eod.live_progress import DailyPipelineLiveProgress


def _run(run_id=700, status='running', **extra):
    return {
        'id': run_id,
        'market_date': '2026-11-06',
        'trigger': 'auto_close_scheduler',
        'status': status,
        'current_stage': extra.pop('current_stage', None),
        'started_at': '2026-11-06T11:10:00+00:00',
        'updated_at': '2026-11-06T11:10:00+00:00',
        'completed_at': extra.pop('completed_at', None),
        'run_metadata': {
            'resolved_stages': ['price_collection', 'technical_snapshots', 'signal_decider'],
        },
        **extra,
    }


def _stage(stage_id, status='running', run_id=700, **counts):
    return {
        'pipeline_run_id': run_id,
        'market_date': '2026-11-06',
        'stage_id': stage_id,
        'status': status,
        'processed': counts.get('processed', 0),
        'total': counts.get('total', 0),
        'succeeded': counts.get('succeeded', 0),
        'failed': counts.get('failed', 0),
        'current_step': counts.get('current_step'),
        'current_activity': counts.get('current_activity'),
        'started_at': counts.get('started_at', '2026-11-06T11:10:00+00:00'),
        'updated_at': counts.get('updated_at', '2026-11-06T11:12:00+00:00'),
        'completed_at': counts.get('completed_at'),
    }


class LiveProgressTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        root = Path(self._dir.name)
        self.snapshot_path = root / 'live' / 'current.json'
        self.log_path = root / 'log' / 'live.jsonl'
        self.live = DailyPipelineLiveProgress(
            snapshot_path=self.snapshot_path,
            event_log_path=self.log_path,
        )

    def test_stage_progress_is_visible_between_start_and_finish(self):
        self.live.publish_run(_run())
        self.live.publish_stage(_stage('price_collection', processed=0, total=2769))
        self.live.publish_stage(
            _stage('price_collection', processed=1000, total=2769, current_step='fetch_symbol_ohlcv')
        )

        now = datetime(2026, 11, 6, 11, 14, tzinfo=timezone.utc)
        snapshot = self.live.snapshot(now=now)

        self.assertTrue(snapshot['active'])
        run = snapshot['active_run']
        self.assertEqual(run['current_stage'], 'price_collection')
        self.assertEqual(run['pending_stage_ids'], ['technical_snapshots', 'signal_decider'])
        self.assertEqual(run['stage_progress'], {'done': 0, 'total': 3})
        stage = run['stages'][0]
        self.assertEqual(stage['percent'], 36.1)
        self.assertEqual(stage['elapsed_seconds'], 240.0)
        self.assertGreater(stage['eta_seconds'], 0)

        self.live.publish_stage(
            _stage(
                'price_collection',
                status='completed',
                processed=2769,
                total=2769,
                completed_at='2026-11-06T11:30:00+00:00',
            )
        )
        self.live.publish_run(_run(status='completed', completed_at='2026-11-06T12:00:00+00:00'))
        finished = self.live.snapshot(now=now)
        self.assertFalse(finished['active'])
        self.assertEqual(finished['runs'][0]['stage_progress'], {'done': 1, 'total': 3})

    def test_snapshot_file_and_event_log_are_written(self):
        self.live.publish_run(_run())
        self.live.publish_stage(_stage('technical_snapshots', total=2769))

        stored = json.loads(self.snapshot_path.read_text(encoding='utf-8'))
        self.assertEqual(stored['schema_version'], 'daily_pipeline_live_v1')
        self.assertEqual(stored['active_run']['current_stage'], 'technical_snapshots')
        events = [json.loads(line) for line in self.log_path.read_text(encoding='utf-8').splitlines()]
        self.assertEqual([event['kind'] for event in events], ['run', 'stage'])
        self.assertEqual(events[1]['stage_id'], 'technical_snapshots')

    def test_progress_log_is_bounded_to_milestones(self):
        self.live.publish_run(_run())
        for processed in range(0, 2770, 10):
            self.live.publish_stage(_stage('price_collection', processed=processed, total=2769))

        logged = self.live.events_since(0, limit=1000)['events']
        stage_events = [event for event in logged if event['kind'] == 'stage']
        self.assertLessEqual(len(stage_events), 25)
        # Memory still carries the latest count even when not logged.
        stage = self.live.snapshot()['active_run']['stages'][0]
        self.assertEqual(stage['processed'], 2760)

    def test_grouped_collection_progress_shows_the_running_part(self):
        # The grouped pykrx call keeps only the universe stage "running" in
        # the pipeline table while it collects prices for every symbol.
        self.live.publish_run(_run())
        self.live.publish_stage(_stage('pykrx_universe_refresh', total=0))
        self.live.publish_collection_progress(
            eod_run_id=591,
            market_date='2026-11-06',
            stage='price_collection',
            status='running',
            current_step='fetch_symbol_ohlcv',
            processed=1200,
            total=2769,
            current_symbol='005930',
        )

        run = self.live.snapshot()['active_run']
        self.assertEqual(run['collection_stage'], 'price_collection')
        price = next(stage for stage in run['stages'] if stage['stage_id'] == 'price_collection')
        self.assertEqual(price['processed'], 1200)
        self.assertEqual(price['current_symbol'], '005930')
        self.assertEqual(price['source'], 'eod_collection_run')

        # The tracked stage row later finishing replaces the mirror.
        self.live.publish_stage(
            _stage('price_collection', status='completed', processed=2769, total=2769)
        )
        run = self.live.snapshot()['runs'][0]
        price = next(stage for stage in run['stages'] if stage['stage_id'] == 'price_collection')
        self.assertEqual(price['status'], 'completed')
        self.assertEqual(price['source'], 'pipeline_stage')

    def test_substeps_attach_to_the_stage_running_on_this_thread(self):
        self.live.publish_run(_run())
        self.live.publish_stage(_stage('technical_snapshots', total=2769))
        self.live.publish_substep(step='price_contexts', status='running')
        self.live.publish_substep(step='price_contexts', status='completed', result_status='completed')
        self.live.publish_substep(step='technical_snapshots', status='running')

        other_thread = threading.Thread(
            target=lambda: self.live.publish_substep(step='ignored', status='running')
        )
        other_thread.start()
        other_thread.join()

        stage = self.live.snapshot()['active_run']['stages'][0]
        self.assertEqual(stage['current_substep'], 'technical_snapshots')
        self.assertEqual(stage['substeps']['price_contexts']['result_status'], 'completed')
        self.assertNotIn('ignored', stage['substeps'])

    def test_per_symbol_callback_updates_counts_without_a_db_write(self):
        self.live.publish_run(_run())
        self.live.publish_stage(_stage('technical_snapshots', total=2769))
        self.live.publish_substep(step='technical_snapshots', status='running')
        for index in range(0, 1500):
            self.live.thread_stage_progress(
                {
                    'status': 'running',
                    'processed': index + 1,
                    'succeeded': index + 1,
                    'failed': 0,
                    'total': 2769,
                    'current_symbol': f'{index:06d}',
                    'current_step': 'computing_symbol_snapshot',
                }
            )

        stage = self.live.snapshot()['active_run']['stages'][0]
        self.assertEqual(stage['processed'], 1500)
        self.assertEqual(stage['status'], 'running')
        self.assertEqual(stage['current_symbol'], '001499')
        self.assertEqual(stage['substeps']['technical_snapshots']['processed'], 1500)
        progress_events = [
            event for event in self.live.events_since(0, limit=1000)['events']
            if event['kind'] == 'stage_progress'
        ]
        self.assertLessEqual(len(progress_events), 12)

    def test_reader_prefers_a_newer_snapshot_from_another_process(self):
        other = DailyPipelineLiveProgress(snapshot_path=self.snapshot_path, event_log_path=self.log_path)
        other.publish_run(_run(run_id=901))
        other.publish_stage(_stage('signal_features', run_id=901, processed=10, total=20))

        fresh = DailyPipelineLiveProgress(snapshot_path=self.snapshot_path, event_log_path=self.log_path)
        best = fresh.read_best_snapshot()

        self.assertEqual(best['origin'], 'snapshot_file')
        self.assertEqual(best['active_run']['run_id'], 901)
        self.assertEqual(best['active_run']['stages'][0]['percent'], 50.0)

    def test_silent_running_run_is_flagged_stale(self):
        self.live.publish_run(_run(updated_at='2026-11-06T11:10:00+00:00'))
        later = datetime(2026, 11, 6, 11, 10, tzinfo=timezone.utc) + timedelta(minutes=30)

        run = self.live.snapshot(now=later)['active_run']

        self.assertTrue(run['stale_suspected'])

    def test_publishing_never_raises_on_bad_input(self):
        for call in (
            lambda: self.live.publish_run(None),
            lambda: self.live.publish_run({'status': 'running'}),
            lambda: self.live.publish_stage({'stage_id': 'x'}),
            lambda: self.live.publish_heartbeat(pipeline_run_id=None, stage_id='x', alive=True),
            lambda: self.live.publish_substep(step='x', status='running'),
        ):
            call()
        self.assertFalse(self.live.snapshot()['active'])


class RunStateServiceHookTests(unittest.TestCase):
    def test_state_service_writes_are_mirrored(self):
        published = []

        class Recorder:
            def publish_run(self, run):
                published.append(('run', run['id']))

            def publish_stage(self, stage):
                published.append(('stage', stage['stage_id']))

            def publish_heartbeat(self, **kwargs):
                published.append(('heartbeat', kwargs['stage_id'], kwargs['alive']))

        class Fake:
            @state_module._publish_live_run
            def start_run(self, **kwargs):
                return {'id': 5, 'status': 'running'}

            @state_module._publish_live_stage
            def update_stage(self, **kwargs):
                return {'pipeline_run_id': 5, 'stage_id': kwargs['stage_id']}

            @state_module._publish_live_heartbeat
            def heartbeat_stage(self, **kwargs):
                return True

        original = state_module.daily_pipeline_live_progress
        state_module.daily_pipeline_live_progress = Recorder()
        try:
            fake = Fake()
            fake.start_run(market_date='2026-11-06')
            fake.update_stage(stage_id='signal_features')
            fake.heartbeat_stage(pipeline_run_id=5, stage_id='signal_features')
        finally:
            state_module.daily_pipeline_live_progress = original

        self.assertEqual(
            published,
            [('run', 5), ('stage', 'signal_features'), ('heartbeat', 'signal_features', True)],
        )

    def test_real_service_methods_are_wrapped(self):
        service = state_module.DailyPipelineRunStateService
        for name in ('start_run', 'update_stage', 'heartbeat_stage', 'finish_run', 'mark_active_runs_interrupted'):
            with self.subTest(method=name):
                self.assertTrue(hasattr(getattr(service, name), '__wrapped__'))


if __name__ == '__main__':
    unittest.main()
