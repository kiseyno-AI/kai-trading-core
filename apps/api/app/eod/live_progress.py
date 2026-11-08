"""Live, DB-free progress feed for the daily pipeline.

Every daily-pipeline write path (run start/finish, stage start/progress/
heartbeat/finish, the grouped pykrx collection's per-symbol progress and the
sub-steps inside derived stages) publishes here. Readers get the current
state without touching the database:

* in-process: ``daily_pipeline_live_progress.snapshot()`` /
  ``events_since(seq)``; the Core API serves them at
  ``GET /eod/pipeline/live`` and ``GET /eod/pipeline/live/events``;
* other processes / agents: ``var/pipeline-live/current.json`` (atomic
  snapshot, rewritten at most once per second plus on every status change)
  and the append-only JSONL log ``var/log/daily-pipeline-live.jsonl``.

Operator CLI runs (``python -m app.internal.ops eod-pipeline-run``) live in
their own process; they write the same snapshot file, and the API endpoint
returns whichever of its memory or the file is newer.

Publishing never raises: a broken feed must not fail the pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from collections import deque
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 'daily_pipeline_live_v1'
TERMINAL_STATUSES = frozenset(
    {'completed', 'partial', 'failed', 'blocked', 'skipped', 'cancelled', 'interrupted'}
)
MAX_RUNS = 6
MAX_EVENTS = 1000
SNAPSHOT_EVENT_LIMIT = 40
SNAPSHOT_MIN_INTERVAL_SECONDS = 1.0
EVENT_LOG_MAX_BYTES = 20 * 1024 * 1024
STALE_AFTER_SECONDS = 15 * 60


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[2]


def default_snapshot_path() -> Path:
    configured = os.environ.get('DAILY_PIPELINE_LIVE_DIR', '').strip()
    base = Path(configured).expanduser() if configured else _root_dir() / 'var' / 'pipeline-live'
    return base / 'current.json'


def default_event_log_path() -> Path:
    configured = os.environ.get('DAILY_PIPELINE_LIVE_LOG', '').strip()
    if configured:
        return Path(configured).expanduser()
    return _root_dir() / 'var' / 'log' / 'daily-pipeline-live.jsonl'


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _parse(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _status(value: Any) -> str:
    return str(value or '').strip().lower() or 'running'


def _clean_text(value: Any, limit: int = 300) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if len(text) <= limit else text[: limit - 1] + '…'


class DailyPipelineLiveProgress:
    def __init__(
        self,
        *,
        snapshot_path: Path | None = None,
        event_log_path: Path | None = None,
        write_files: bool = True,
    ) -> None:
        self._lock = threading.RLock()
        self._runs: dict[str, dict[str, Any]] = {}
        self._collections: dict[str, dict[str, Any]] = {}
        self._events: deque[dict[str, Any]] = deque(maxlen=MAX_EVENTS)
        self._seq = 0
        self._updated_at: datetime | None = None
        self._last_snapshot_write = 0.0
        self._snapshot_path = snapshot_path
        self._event_log_path = event_log_path
        self._write_files = write_files
        self._local = threading.local()

    # ------------------------------------------------------------------ paths
    @property
    def snapshot_path(self) -> Path:
        return self._snapshot_path or default_snapshot_path()

    @property
    def event_log_path(self) -> Path:
        return self._event_log_path or default_event_log_path()

    # -------------------------------------------------------------- publishing
    def publish_run(self, run: Mapping[str, Any] | None) -> None:
        """Run row as serialized by the run-state service (start/finish)."""

        try:
            if not isinstance(run, Mapping) or run.get('id') is None:
                return
            with self._lock:
                key = str(run['id'])
                entry = self._runs.setdefault(key, {'stages': {}})
                previous_status = entry.get('status')
                entry.update(
                    {
                        'run_id': run.get('id'),
                        'market_date': _iso(run.get('market_date')),
                        'trigger': run.get('trigger'),
                        'status': _status(run.get('status')),
                        'current_stage': run.get('current_stage'),
                        'started_at': _iso(run.get('started_at')) or entry.get('started_at'),
                        'updated_at': _iso(run.get('updated_at')) or _now().isoformat(),
                        'completed_at': _iso(run.get('completed_at')),
                        'error_message': _clean_text(run.get('error_message')),
                    }
                )
                metadata = run.get('run_metadata') if isinstance(run.get('run_metadata'), Mapping) else {}
                stage_order = metadata.get('resolved_stages') or metadata.get('requested_stages')
                if isinstance(stage_order, list) and stage_order:
                    entry['stage_order'] = [str(item) for item in stage_order]
                for stage in (run.get('stages') or {}).values() if isinstance(run.get('stages'), Mapping) else ():
                    self._merge_stage(entry, stage)
                self._trim_runs()
                changed = previous_status != entry['status']
                self._record(
                    'run',
                    {
                        'run_id': entry['run_id'],
                        'market_date': entry['market_date'],
                        'trigger': entry['trigger'],
                        'status': entry['status'],
                        'current_stage': entry['current_stage'],
                        'error_message': entry['error_message'],
                    },
                    force_snapshot=changed,
                )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug('daily pipeline live publish_run failed: %s', exc)

    def publish_stage(self, stage: Mapping[str, Any] | None) -> None:
        """Stage row as serialized by the run-state service (any update)."""

        try:
            if not isinstance(stage, Mapping) or stage.get('pipeline_run_id') is None:
                return
            with self._lock:
                key = str(stage['pipeline_run_id'])
                entry = self._runs.setdefault(
                    key,
                    {
                        'run_id': stage.get('pipeline_run_id'),
                        'market_date': _iso(stage.get('market_date')),
                        'status': 'running',
                        'stages': {},
                    },
                )
                stage_id = str(stage.get('stage_id') or '')
                previous = dict(entry['stages'].get(stage_id) or {})
                merged = self._merge_stage(entry, stage)
                status = merged.get('status')
                if status == 'running':
                    entry['current_stage'] = stage_id
                    entry['status'] = 'running'
                    self._local.context = (entry.get('run_id'), stage_id)
                elif status in TERMINAL_STATUSES and self._current_context() == (entry.get('run_id'), stage_id):
                    self._local.context = None
                entry['updated_at'] = merged.get('updated_at') or _now().isoformat()
                transition = previous.get('status') != status
                self._record(
                    'stage',
                    {
                        'run_id': entry.get('run_id'),
                        'market_date': entry.get('market_date'),
                        'stage_id': stage_id,
                        **self._event_counts(merged),
                    },
                    force_snapshot=transition,
                    log_event=transition or self._progress_step_changed(previous, merged),
                )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug('daily pipeline live publish_stage failed: %s', exc)

    def publish_heartbeat(self, *, pipeline_run_id: Any, stage_id: str, alive: bool) -> None:
        try:
            if pipeline_run_id is None:
                return
            with self._lock:
                entry = self._runs.get(str(pipeline_run_id))
                if entry is None:
                    return
                stage = entry['stages'].get(stage_id)
                now = _now().isoformat()
                if stage is not None:
                    stage['heartbeat_at'] = now
                    stage['heartbeat_ok'] = bool(alive)
                entry['heartbeat_at'] = now
                self._updated_at = _now()
                self._maybe_write_snapshot(force=False)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug('daily pipeline live publish_heartbeat failed: %s', exc)

    def publish_collection_progress(
        self,
        *,
        eod_run_id: Any,
        market_date: Any,
        stage: str,
        status: str,
        current_step: str | None = None,
        current_activity: str | None = None,
        processed: Any = None,
        total: Any = None,
        succeeded: Any = None,
        failed: Any = None,
        current_symbol: str | None = None,
        current_period: str | None = None,
    ) -> None:
        """Per-symbol progress of the grouped pykrx collection (EOD run).

        Mirrored into the active pipeline run for the same market date so a
        grouped ``universe → price → enrichment`` call shows which part is
        actually running instead of only the first stage.
        """

        try:
            with self._lock:
                normalized_date = _iso(market_date)
                now = _now().isoformat()
                collection = self._collections.setdefault(
                    str(eod_run_id),
                    {'eod_run_id': eod_run_id, 'market_date': normalized_date, 'stages': {}},
                )
                previous = dict(collection['stages'].get(stage) or {})
                payload = {
                    'stage_id': stage,
                    'status': _status(status),
                    'current_step': current_step,
                    'current_activity': _clean_text(current_activity),
                    'processed': _int(processed),
                    'total': _int(total),
                    'succeeded': _int(succeeded),
                    'failed': _int(failed),
                    'current_symbol': current_symbol,
                    'current_period': current_period,
                    'started_at': previous.get('started_at') or now,
                    'updated_at': now,
                    'completed_at': now if _status(status) in TERMINAL_STATUSES else None,
                    'source': 'eod_collection_run',
                    'eod_run_id': eod_run_id,
                }
                collection['stages'][stage] = payload
                collection['updated_at'] = now
                while len(self._collections) > MAX_RUNS:
                    self._collections.pop(next(iter(self._collections)))
                for entry in self._runs.values():
                    if entry.get('market_date') != normalized_date or entry.get('status') in TERMINAL_STATUSES:
                        continue
                    tracked = entry['stages'].get(stage)
                    if tracked is None or tracked.get('source') == 'eod_collection_run' or (
                        tracked.get('status') == 'running'
                        and _int(tracked.get('processed')) in (None, 0)
                    ):
                        mirrored = dict(tracked or {})
                        mirrored.update(payload)
                        entry['stages'][stage] = mirrored
                        if payload['status'] == 'running':
                            entry['collection_stage'] = stage
                    entry['updated_at'] = now
                transition = previous.get('status') != payload['status']
                self._record(
                    'collection',
                    {
                        'eod_run_id': eod_run_id,
                        'market_date': normalized_date,
                        'stage_id': stage,
                        **self._event_counts(payload),
                    },
                    force_snapshot=transition,
                    log_event=transition or self._progress_step_changed(previous, payload),
                )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug('daily pipeline live publish_collection_progress failed: %s', exc)

    def publish_substep(
        self,
        *,
        step: str,
        status: str,
        result_status: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        """Named part of the stage running on this thread (e.g. price_contexts)."""

        try:
            context = self._current_context()
            if context is None:
                return
            run_id, stage_id = context
            with self._lock:
                entry = self._runs.get(str(run_id))
                if entry is None:
                    return
                stage = entry['stages'].setdefault(stage_id, {'stage_id': stage_id, 'status': 'running'})
                substeps = stage.setdefault('substeps', {})
                now = _now().isoformat()
                current = dict(substeps.get(step) or {})
                current.setdefault('started_at', now)
                current.update(
                    {
                        'status': _status(status),
                        'updated_at': now,
                        'completed_at': now if _status(status) in TERMINAL_STATUSES else None,
                    }
                )
                if result_status:
                    current['result_status'] = result_status
                if detail:
                    current['detail'] = {str(k): _iso(v) for k, v in detail.items()}
                substeps[step] = current
                stage['current_substep'] = step if current['status'] == 'running' else stage.get('current_substep')
                stage['updated_at'] = now
                entry['updated_at'] = now
                self._record(
                    'substep',
                    {
                        'run_id': run_id,
                        'market_date': entry.get('market_date'),
                        'stage_id': stage_id,
                        'step': step,
                        'status': current['status'],
                        'result_status': result_status,
                    },
                    force_snapshot=True,
                )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug('daily pipeline live publish_substep failed: %s', exc)

    def thread_stage_progress(self, payload: Mapping[str, Any] | None) -> None:
        """Per-item progress callback for services run inside a tracked stage.

        Updates only the live view of the stage running on this thread (no
        DB write), so it is cheap enough to call for every symbol.
        """

        try:
            context = self._current_context()
            if context is None or not isinstance(payload, Mapping):
                return
            run_id, stage_id = context
            with self._lock:
                entry = self._runs.get(str(run_id))
                if entry is None:
                    return
                stage = entry['stages'].setdefault(stage_id, {'stage_id': stage_id, 'status': 'running'})
                previous = dict(stage)
                now = _now().isoformat()
                for key in ('processed', 'total', 'succeeded', 'failed'):
                    value = _int(payload.get(key))
                    if value is not None:
                        stage[key] = value
                for key in ('current_symbol', 'current_step', 'current_period'):
                    if payload.get(key) is not None:
                        stage[key] = payload.get(key)
                substep = stage.get('current_substep')
                if substep and isinstance(stage.get('substeps'), dict) and substep in stage['substeps']:
                    stage['substeps'][substep].update(
                        {
                            key: stage.get(key)
                            for key in ('processed', 'total', 'succeeded', 'failed')
                        }
                    )
                stage['updated_at'] = now
                entry['updated_at'] = now
                self._record(
                    'stage_progress',
                    {
                        'run_id': run_id,
                        'market_date': entry.get('market_date'),
                        'stage_id': stage_id,
                        'substep': substep,
                        **self._event_counts(stage),
                    },
                    force_snapshot=False,
                    log_event=self._progress_step_changed(previous, stage),
                )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug('daily pipeline live thread_stage_progress failed: %s', exc)

    # ----------------------------------------------------------------- reading
    def snapshot(self, *, now: datetime | None = None, event_limit: int = SNAPSHOT_EVENT_LIMIT) -> dict[str, Any]:
        with self._lock:
            return self._build_snapshot(now=now or _now(), event_limit=event_limit)

    def events_since(self, seq: int = 0, *, limit: int = 200) -> dict[str, Any]:
        with self._lock:
            items = [event for event in self._events if event['seq'] > seq]
            page = items[: max(1, min(limit, MAX_EVENTS))]
            return {
                'schema_version': SCHEMA_VERSION,
                'last_seq': self._seq,
                # Resume from here: the last event on this page, not the
                # newest event overall, so a truncated page loses nothing.
                'next_since': page[-1]['seq'] if page else seq,
                'events': page,
                'truncated': len(items) > len(page),
                # Events older than the in-memory window are gone.
                'gap': bool(self._events) and seq + 1 < self._events[0]['seq'],
            }

    def read_best_snapshot(self, *, now: datetime | None = None) -> dict[str, Any]:
        """This process's memory, or the shared file when another process is newer."""

        current = self.snapshot(now=now)
        stored = self.load_snapshot_file()
        if stored is None:
            current['origin'] = 'memory'
            return current
        stored_updated = _parse(stored.get('updated_at'))
        current_updated = _parse(current.get('updated_at'))
        if stored_updated and (current_updated is None or stored_updated > current_updated):
            refreshed = self._refresh_elapsed(stored, now=now or _now())
            refreshed['origin'] = 'snapshot_file'
            return refreshed
        current['origin'] = 'memory'
        return current

    def load_snapshot_file(self) -> dict[str, Any] | None:
        try:
            payload = json.loads(self.snapshot_path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            return None
        if not isinstance(payload, dict) or payload.get('schema_version') != SCHEMA_VERSION:
            return None
        return payload

    # ---------------------------------------------------------------- internals
    def _current_context(self) -> tuple[Any, str] | None:
        return getattr(self._local, 'context', None)

    def _merge_stage(self, entry: dict[str, Any], stage: Mapping[str, Any]) -> dict[str, Any]:
        stage_id = str(stage.get('stage_id') or '')
        merged = dict(entry['stages'].get(stage_id) or {})
        if merged.get('source') == 'eod_collection_run' and _status(stage.get('status')) == 'running' and _int(stage.get('processed')) in (None, 0):
            # The grouped collection mirror already carries real counts.
            merged['tracked_status'] = 'running'
            entry['stages'][stage_id] = merged
            return merged
        merged.update(
            {
                'stage_id': stage_id,
                'status': _status(stage.get('status')),
                'processed': _int(stage.get('processed')),
                'total': _int(stage.get('total')),
                'succeeded': _int(stage.get('succeeded')),
                'failed': _int(stage.get('failed')),
                'remaining': _int(stage.get('remaining')),
                'current_step': stage.get('current_step'),
                'current_activity': _clean_text(stage.get('current_activity')),
                'current_symbol': stage.get('current_symbol'),
                'current_period': stage.get('current_period'),
                'started_at': _iso(stage.get('started_at')) or merged.get('started_at'),
                'updated_at': _iso(stage.get('updated_at')) or _now().isoformat(),
                'completed_at': _iso(stage.get('completed_at')),
                'error_message': _clean_text(stage.get('error_message')),
                'source': 'pipeline_stage',
            }
        )
        merged.pop('tracked_status', None)
        entry['stages'][stage_id] = merged
        return merged

    @staticmethod
    def _event_counts(stage: Mapping[str, Any]) -> dict[str, Any]:
        return {
            'status': stage.get('status'),
            'processed': stage.get('processed'),
            'total': stage.get('total'),
            'succeeded': stage.get('succeeded'),
            'failed': stage.get('failed'),
            'current_step': stage.get('current_step'),
            'current_symbol': stage.get('current_symbol'),
            'error_message': stage.get('error_message'),
        }

    @staticmethod
    def _progress_step_changed(previous: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
        if previous.get('current_step') != current.get('current_step'):
            return True
        processed = _int(current.get('processed')) or 0
        total = _int(current.get('total')) or 0
        before = _int(previous.get('processed')) or 0
        if total <= 0:
            return processed != before
        # One log line per ~5% of progress keeps the JSONL readable.
        step = max(1, total // 20)
        return processed // step != before // step or processed == total

    def _trim_runs(self) -> None:
        while len(self._runs) > MAX_RUNS:
            self._runs.pop(next(iter(self._runs)))

    def _record(
        self,
        kind: str,
        payload: Mapping[str, Any],
        *,
        force_snapshot: bool,
        log_event: bool = True,
    ) -> None:
        self._updated_at = _now()
        if log_event:
            self._seq += 1
            event = {
                'seq': self._seq,
                'at': self._updated_at.isoformat(),
                'kind': kind,
                **{k: _iso(v) for k, v in payload.items() if v is not None},
            }
            self._events.append(event)
            self._append_event_log(event)
        self._maybe_write_snapshot(force=force_snapshot)

    def _append_event_log(self, event: Mapping[str, Any]) -> None:
        if not self._write_files:
            return
        path = self.event_log_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                if path.stat().st_size > EVENT_LOG_MAX_BYTES:
                    os.replace(path, path.with_name(path.name + '.1'))
            except FileNotFoundError:
                pass
            with path.open('a', encoding='utf-8') as handle:
                handle.write(json.dumps(event, ensure_ascii=False, default=str) + '\n')
        except OSError as exc:
            logger.debug('daily pipeline live event log write failed: %s', exc)

    def _maybe_write_snapshot(self, *, force: bool) -> None:
        if not self._write_files:
            return
        clock = time.monotonic()
        if not force and clock - self._last_snapshot_write < SNAPSHOT_MIN_INTERVAL_SECONDS:
            return
        self._last_snapshot_write = clock
        path = self.snapshot_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = self._build_snapshot(now=_now(), event_limit=SNAPSHOT_EVENT_LIMIT)
            payload['writer_pid'] = os.getpid()
            fd, tmp = tempfile.mkstemp(prefix='.current-', dir=str(path.parent))
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                    json.dump(payload, handle, ensure_ascii=False, default=str)
                os.replace(tmp, path)
            finally:
                Path(tmp).unlink(missing_ok=True)
        except OSError as exc:
            logger.debug('daily pipeline live snapshot write failed: %s', exc)

    def _build_snapshot(self, *, now: datetime, event_limit: int) -> dict[str, Any]:
        runs = []
        for entry in reversed(list(self._runs.values())):
            runs.append(self._decorate_run(entry, now=now))
        collections = [
            {
                **{k: v for k, v in item.items() if k != 'stages'},
                'stages': {
                    stage_id: self._decorate_stage(stage, now=now)
                    for stage_id, stage in item['stages'].items()
                },
            }
            for item in reversed(list(self._collections.values()))
        ]
        active = [run for run in runs if run.get('status') not in TERMINAL_STATUSES]
        return {
            'schema_version': SCHEMA_VERSION,
            'generated_at': now.isoformat(),
            'updated_at': self._updated_at.isoformat() if self._updated_at else None,
            'active': bool(active),
            'active_run': active[0] if active else None,
            'runs': runs,
            'collections': collections,
            'last_seq': self._seq,
            'recent_events': list(self._events)[-event_limit:],
        }

    def _decorate_run(self, entry: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
        run = {k: v for k, v in entry.items() if k != 'stages'}
        stages = {
            stage_id: self._decorate_stage(stage, now=now)
            for stage_id, stage in entry['stages'].items()
        }
        order = list(entry.get('stage_order') or [])
        ordered_ids = [stage_id for stage_id in order if stage_id in stages] + [
            stage_id for stage_id in stages if stage_id not in order
        ]
        run['stages'] = [stages[stage_id] for stage_id in ordered_ids]
        if order:
            run['pending_stage_ids'] = [stage_id for stage_id in order if stage_id not in stages]
            done = sum(1 for stage in stages.values() if stage.get('status') in TERMINAL_STATUSES)
            run['stage_progress'] = {'done': done, 'total': len(order)}
        started = _parse(run.get('started_at'))
        ended = _parse(run.get('completed_at')) if run.get('status') in TERMINAL_STATUSES else None
        if started:
            run['elapsed_seconds'] = round(((ended or now) - started).total_seconds(), 1)
        last = _parse(entry.get('heartbeat_at'))
        updated = _parse(run.get('updated_at'))
        if updated is not None and (last is None or updated > last):
            last = updated
        if run.get('status') not in TERMINAL_STATUSES and last is not None:
            idle = (now - last).total_seconds()
            run['seconds_since_update'] = round(idle, 1)
            run['stale_suspected'] = idle > STALE_AFTER_SECONDS
        return run

    @staticmethod
    def _decorate_stage(stage: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
        item = dict(stage)
        status = item.get('status')
        started = _parse(item.get('started_at'))
        ended = _parse(item.get('completed_at')) if status in TERMINAL_STATUSES else None
        processed = _int(item.get('processed'))
        total = _int(item.get('total'))
        if started:
            elapsed = max(((ended or now) - started).total_seconds(), 0.0)
            item['elapsed_seconds'] = round(elapsed, 1)
            if status == 'running' and processed and total and 0 < processed < total and elapsed > 0:
                rate = processed / elapsed
                item['rate_per_second'] = round(rate, 3)
                item['eta_seconds'] = round((total - processed) / rate, 1)
        if processed is not None and total:
            item['percent'] = round(min(processed / total, 1.0) * 100, 1)
        last = _parse(item.get('heartbeat_at'))
        updated = _parse(item.get('updated_at'))
        if updated is not None and (last is None or updated > last):
            last = updated
        if status == 'running' and last is not None:
            item['seconds_since_update'] = round((now - last).total_seconds(), 1)
        return item

    def _refresh_elapsed(self, payload: dict[str, Any], *, now: datetime) -> dict[str, Any]:
        refreshed = dict(payload)
        refreshed['generated_at'] = now.isoformat()
        runs = []
        for run in payload.get('runs') or []:
            entry = dict(run)
            entry['stages'] = {
                str(stage.get('stage_id')): stage for stage in (run.get('stages') or []) if isinstance(stage, dict)
            }
            runs.append(self._decorate_run(entry, now=now))
        refreshed['runs'] = runs
        active = [run for run in runs if run.get('status') not in TERMINAL_STATUSES]
        refreshed['active'] = bool(active)
        refreshed['active_run'] = active[0] if active else None
        return refreshed


daily_pipeline_live_progress = DailyPipelineLiveProgress()
