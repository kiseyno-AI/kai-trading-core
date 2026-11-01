from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from app.dart.exact_audit_signal_repair import (
    CHECKPOINT_SCHEMA_VERSION,
    ExactDartAuditSignalRepair,
    MANIFEST_SCHEMA_VERSION,
    _audit_batch_idle_transaction_timeout_ms,
    exact_dart_audit_batch_guard,
)
from app.dart.signals_service import (
    DartSignalsService,
    build_authoritative_absence_audit_signal,
)
from app.internal.exact_repair_manifest import canonical_json_sha256


def _manifest(*entries: dict, batch_size: int = 1) -> dict:
    return {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'market_date': '2026-10-28',
        'entry_count': len(entries),
        'batch_size': batch_size,
        'entries': list(entries),
    }


def _entry(stock_code: str, event_id: int, rcept_no: str, **extra) -> dict:
    return {
        'stock_code': stock_code,
        'event_id': event_id,
        'rcept_no': rcept_no,
        **extra,
    }


def _complete_negative_audit_diagnostics() -> dict:
    return {
        'version': 'dart_audit_archive_diagnostics_v1',
        'archive_sha256': 'a' * 64,
        'archive_size_bytes': 321,
        'member_count': 1,
        'members': [],
        'decoded_member_count': 1,
        'aud_opn_table_count': 1,
        'audit_report_label_row_count': 1,
        'known_opinion_token_row_count': 1,
        'positive_authority_evidence_count': 0,
        'negative_authority_evidence_count': 1,
        'ambiguous_authority_evidence_count': 0,
        'truncated': False,
    }


def _authoritative_absence_signal(entry: dict) -> dict:
    signal = build_authoritative_absence_audit_signal(
        {
            'id': entry['event_id'],
            'rcept_no': entry['rcept_no'],
            'receipt_date': '2026-10-28',
        },
        _complete_negative_audit_diagnostics(),
    )
    return {
        'opinion_code': signal.opinion_code,
        'opinion_label': signal.opinion_label,
        'has_audit_issue': signal.has_audit_issue,
        'source_section': signal.source_section,
        'evidence_text': signal.evidence_text,
        'payload': signal.payload,
    }


def _parsed_audit_signal(opinion_code: str = 'unqualified') -> dict:
    opinion_labels = {
        'unqualified': '적정',
        'qualified': '한정',
        'adverse': '부적정',
        'disclaimer': '의견거절',
    }
    opinion_label = opinion_labels[opinion_code]
    source_section = '감사의견'
    return {
        'opinion_code': opinion_code,
        'opinion_label': opinion_label,
        'has_audit_issue': opinion_code != 'unqualified',
        'source_section': source_section,
        'evidence_text': f'{source_section}: {opinion_label}',
        'payload': {
            'parser': 'semantic_audit_table_v1',
            'opinion_cell': opinion_label,
            'row_cells': [source_section, opinion_label],
        },
    }


def _terminal_proof(entry: dict, *, status: str = 'captured') -> dict:
    proof = {
        'status': status,
        'stock_code': entry['stock_code'],
        'event_id': entry['event_id'],
        'rcept_no': entry['rcept_no'],
        'audit_signal_evaluated': status == 'authoritative_absence',
    }
    if status == 'captured':
        audit_content = _parsed_audit_signal()
    else:
        audit_content = _authoritative_absence_signal(entry)
    proof.update(
        {
            'receipt_date': '2026-10-28',
            'audit_content': audit_content,
            'audit_content_sha256': canonical_json_sha256(audit_content),
        }
    )
    return proof


@contextmanager
def _guard(acquired: bool = True, reason: str | None = None, **_kwargs):
    yield acquired, reason


class ExactDartAuditSignalRepairTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _event_loader(events: dict[int, dict]):
        def load(event_id):
            event = events.get(event_id)
            if event is None:
                return None
            return {'receipt_date': '2026-10-28', **event}

        return load

    def test_guard_owns_shared_audit_lock_without_nested_writer_deadlock(self):
        outer_session = MagicMock()
        outer_session.get_bind.return_value = SimpleNamespace(
            dialect=SimpleNamespace(name='postgresql')
        )
        inner_session = MagicMock()
        inner_session.get_bind.return_value = SimpleNamespace(
            dialect=SimpleNamespace(name='postgresql')
        )

        @contextmanager
        def advisory_lock(**_kwargs):
            yield True

        @contextmanager
        def outer_scope(**_kwargs):
            yield outer_session

        @contextmanager
        def inner_scope(**_kwargs):
            yield inner_session

        service = DartSignalsService(
            client=MagicMock(),
            repository=MagicMock(),
        )
        with (
            patch(
                'app.dart.exact_audit_signal_repair.advisory_eod_collection_lock',
                advisory_lock,
            ),
            patch(
                'app.dart.exact_audit_signal_repair.session_scope',
                outer_scope,
            ),
            patch(
                'app.dart.exact_audit_signal_repair._active_writer_counts',
                return_value={},
            ),
            patch(
                'app.dart.exact_audit_signal_repair._audit_batch_idle_transaction_timeout_ms',
                return_value=123_456,
            ),
            patch(
                'app.dart.signals_service.session_scope',
                inner_scope,
            ),
            exact_dart_audit_batch_guard(
                market_date=date(2026, 10, 28),
                batch_size=1,
                concurrency=2,
            ) as (acquired, reason),
        ):
            self.assertTrue(acquired)
            self.assertIsNone(reason)
            sentinel = build_authoritative_absence_audit_signal(
                {
                    'id': 101,
                    'rcept_no': '20260301000001',
                    'receipt_date': '2026-10-28',
                },
                _complete_negative_audit_diagnostics(),
            )
            service._store_signals(
                event={
                    'id': 101,
                    'rcept_no': '20260301000001',
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                    'receipt_date': '2026-10-28',
                    'report_nm': '사업보고서 (2025.12)',
                },
                audit_signal=sentinel,
                dilution_signal=None,
                audit_signal_evaluated=True,
            )

        self.assertTrue(
            any(
                'pg_advisory_xact_lock' in str(call.args[0])
                for call in outer_session.execute.call_args_list
            )
        )
        self.assertTrue(
            any(
                'SET LOCAL idle_in_transaction_session_timeout = 123456'
                in str(call.args[0])
                for call in outer_session.execute.call_args_list
            )
        )
        inner_session.execute.assert_called_once()
        self.assertTrue(inner_session.execute.call_args.args[0].is_insert)

    def test_guard_timeout_covers_every_external_fetch_wave(self):
        with (
            patch(
                'app.dart.exact_audit_signal_repair._audit_event_timeout_seconds',
                return_value=60,
            ),
            patch(
                'app.dart.exact_audit_signal_repair.settings.db_pool_timeout_seconds',
                5,
            ),
            patch(
                'app.dart.exact_audit_signal_repair.settings.db_connect_timeout_seconds',
                5,
            ),
        ):
            self.assertEqual(
                _audit_batch_idle_transaction_timeout_ms(
                    batch_size=25,
                    concurrency=4,
                ),
                1_605_000,
            )

    def test_guard_timeout_enforces_minimum_and_rejects_over_cap(self):
        with patch(
            'app.dart.exact_audit_signal_repair._audit_event_timeout_seconds',
            return_value=0.001,
        ):
            self.assertEqual(
                _audit_batch_idle_transaction_timeout_ms(
                    batch_size=1,
                    concurrency=8,
                ),
                120_000,
            )

        with patch(
            'app.dart.exact_audit_signal_repair._audit_event_timeout_seconds',
            return_value=90_000,
        ):
            with self.assertRaisesRegex(
                ValueError,
                'exceeds the PostgreSQL idle transaction safety limit',
            ):
                _audit_batch_idle_transaction_timeout_ms(
                    batch_size=1,
                    concurrency=1,
                )

    async def test_entry_capture_is_bounded_by_the_guard_timeout_budget(self):
        entry = _entry('005930', 101, '20260301000001')

        async def capture(*, rcept_no: str):
            await asyncio.sleep(60)
            return {'rcept_no': rcept_no, 'audit_signal': {}}

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=self._event_loader(
                {101: {**entry, 'report_nm': '사업보고서 (2025.12)'}}
            ),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        with patch(
            'app.dart.exact_audit_signal_repair._audit_event_timeout_seconds',
            return_value=0.001,
        ):
            result = await repair._attempt_entry(
                entry,
                semaphore=asyncio.Semaphore(1),
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['error_type'], 'TimeoutError')
        self.assertEqual(
            result['error_message'],
            'audit capture exceeded 0.001s deadline',
        )

    async def test_run_budgets_the_actual_final_batch_and_concurrency(self):
        entries = (
            _entry('005930', 101, '20260301000001'),
            _entry('000660', 102, '20260301000002'),
            _entry('035420', 103, '20260301000003'),
        )
        events = {
            entry['event_id']: {**entry, 'report_nm': '사업보고서 (2025.12)'}
            for entry in entries
        }
        guard_calls: list[dict[str, object]] = []

        @contextmanager
        def recording_guard(**kwargs):
            guard_calls.append(kwargs)
            yield True, None

        async def capture(*, rcept_no: str):
            return {
                'rcept_no': rcept_no,
                'audit_signal': _parsed_audit_signal(),
            }

        repair = ExactDartAuditSignalRepair(
            _manifest(*entries, batch_size=2),
            capture=capture,
            event_loader=self._event_loader(events),
            terminal_state_loader=lambda entry: _terminal_proof(entry),
            guard_factory=recording_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            result = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
                concurrency=2,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            guard_calls,
            [
                {
                    'market_date': date(2026, 10, 28),
                    'batch_size': 2,
                    'concurrency': 2,
                },
                {
                    'market_date': date(2026, 10, 28),
                    'batch_size': 1,
                    'concurrency': 2,
                },
            ],
        )

    async def test_failed_batch_is_retried_while_completed_batch_is_skipped(self):
        entries = (
            _entry('005930', 101, '20260301000001'),
            _entry('000660', 102, '20260301000002'),
        )
        events = {
            entry['event_id']: {**entry, 'report_nm': '사업보고서 (2025.12)'}
            for entry in entries
        }
        attempts: dict[str, int] = {}

        async def capture(*, rcept_no: str):
            attempts[rcept_no] = attempts.get(rcept_no, 0) + 1
            if rcept_no == entries[0]['rcept_no'] and attempts[rcept_no] == 1:
                raise RuntimeError('temporary provider failure')
            return {
                'rcept_no': rcept_no,
                'audit_signal': _parsed_audit_signal(),
            }

        repair = ExactDartAuditSignalRepair(
            _manifest(*entries),
            capture=capture,
            event_loader=self._event_loader(events),
            terminal_state_loader=lambda entry: _terminal_proof(entry),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            second = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'partial')
        self.assertEqual(second['status'], 'completed')
        self.assertEqual(attempts[entries[0]['rcept_no']], 2)
        self.assertEqual(attempts[entries[1]['rcept_no']], 1)
        self.assertEqual(second['skipped_batches'], 1)

    async def test_checkpoint_rejects_manifest_hash_drift(self):
        first_entry = _entry('005930', 101, '20260301000001')
        second_entry = _entry('005930', 103, '20260301000003')
        events = {
            101: {**first_entry, 'report_nm': '사업보고서 (2025.12)'},
            103: {**second_entry, 'report_nm': '사업보고서 (2025.12)'},
        }

        async def capture(*, rcept_no: str):
            return {
                'rcept_no': rcept_no,
                'audit_signal': _parsed_audit_signal(),
            }

        first_repair = ExactDartAuditSignalRepair(
            _manifest(first_entry),
            capture=capture,
            event_loader=self._event_loader(events),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        second_repair = ExactDartAuditSignalRepair(
            _manifest(second_entry),
            capture=capture,
            event_loader=self._event_loader(events),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            completed = await first_repair.run(
                apply=True,
                expected_manifest_sha256=first_repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            blocked = await second_repair.run(
                apply=True,
                expected_manifest_sha256=second_repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(completed['status'], 'completed')
        self.assertEqual(blocked['status'], 'blocked')
        self.assertEqual(blocked['reason'], 'checkpoint_manifest_mismatch')

    async def test_checkpoint_v1_is_explicitly_incompatible_with_v2(self):
        entry = _entry('005930', 101, '20260301000001')
        capture = AsyncMock(
            return_value={
                'rcept_no': entry['rcept_no'],
                'audit_signal': _parsed_audit_signal(),
            }
        )
        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=self._event_loader(
                {101: {**entry, 'report_nm': '사업보고서 (2025.12)'}}
            ),
            terminal_state_loader=lambda item: _terminal_proof(item),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            checkpoint.write_text(
                json.dumps(
                    {
                        'schema_version': (
                            'dart_audit_signal_exact_checkpoint_v1'
                        ),
                        'manifest_sha256': repair.manifest_sha256,
                        'item_count': 1,
                        'unit_size': 1,
                        'units': {},
                    }
                ),
                encoding='utf-8',
            )
            result = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(
            CHECKPOINT_SCHEMA_VERSION,
            'dart_audit_signal_exact_checkpoint_v2',
        )
        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'checkpoint_manifest_mismatch')
        self.assertIn('schema_version', result['error_message'])
        capture.assert_not_awaited()

    async def test_active_writer_refuses_batch_without_capture(self):
        entry = _entry('005930', 101, '20260301000001')
        capture_called = False

        async def capture(*, rcept_no: str):
            nonlocal capture_called
            capture_called = True
            return {'rcept_no': rcept_no, 'audit_signal': {}}

        @contextmanager
        def blocked_guard(**_kwargs):
            yield False, 'active_writer:daily_pipeline_runs'

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=self._event_loader(
                {101: {**entry, 'report_nm': '사업보고서 (2025.12)'}}
            ),
            guard_factory=blocked_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            result = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'active_writer:daily_pipeline_runs')
        self.assertFalse(capture_called)

    async def test_reviewed_replacement_lineage_is_bound_to_manifest(self):
        replacement = _entry(
            '147760',
            108389,
            '20260316001657',
            replacement_for={
                'stock_code': '147760',
                'event_id': 108388,
                'rcept_no': '20260317000801',
                'source_error_code': '014',
                'reason': 'reviewed source document is unavailable',
            },
        )
        events = {
            108388: {
                'stock_code': '147760',
                'event_id': 108388,
                'rcept_no': '20260317000801',
                'report_nm': '[첨부정정]사업보고서 (2025.12)',
            },
            108389: {
                'stock_code': '147760',
                'event_id': 108389,
                'rcept_no': '20260316001657',
                'report_nm': '사업보고서 (2025.12)',
            },
        }
        captured_rcept_nos: list[str] = []

        async def capture(*, rcept_no: str):
            captured_rcept_nos.append(rcept_no)
            return {
                'rcept_no': rcept_no,
                'audit_signal': _parsed_audit_signal(),
            }

        repair = ExactDartAuditSignalRepair(
            _manifest(replacement),
            capture=capture,
            event_loader=self._event_loader(events),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        dry_run = await repair.run()
        with TemporaryDirectory() as directory:
            result = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(dry_run['validated_entry_count'], 1)
        self.assertEqual(
            dry_run['replacement_lineages'][0]['replacement_for']['event_id'],
            108388,
        )
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(captured_rcept_nos, ['20260316001657'])

    async def test_target_event_without_receipt_date_is_rejected(self):
        entry = _entry('005930', 101, '20260301000001')
        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=AsyncMock(),
            event_loader=lambda _event_id: {
                **entry,
                'report_nm': '사업보고서 (2025.12)',
                'receipt_date': None,
            },
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        with self.assertRaisesRegex(ValueError, 'receipt_date is missing'):
            await repair.run()

    async def test_target_event_after_manifest_market_date_is_rejected(self):
        entry = _entry('005930', 101, '20260301000001')
        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=AsyncMock(),
            event_loader=lambda _event_id: {
                **entry,
                'report_nm': '사업보고서 (2025.12)',
                'receipt_date': '2026-10-29',
            },
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        with self.assertRaisesRegex(ValueError, 'exceeds manifest market_date'):
            await repair.run()

    async def test_replacement_event_after_manifest_market_date_is_rejected(self):
        entry = _entry(
            '005930',
            101,
            '20260301000001',
            replacement_for={
                'stock_code': '005930',
                'event_id': 100,
                'rcept_no': '20260301000000',
                'source_error_code': '014',
                'reason': 'reviewed unavailable replacement',
            },
        )

        def load(event_id):
            identity = entry if event_id == 101 else entry['replacement_for']
            return {
                **identity,
                'report_nm': '사업보고서 (2025.12)',
                'receipt_date': (
                    '2026-10-28' if event_id == 101 else '2026-10-29'
                ),
            }

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=AsyncMock(),
            event_loader=load,
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        with self.assertRaisesRegex(ValueError, 'exceeds manifest market_date'):
            await repair.run()

    async def test_provider_no_data_error_remains_retry_debt(self):
        entry = _entry('005930', 101, '20260301000001')

        async def capture(*, rcept_no: str):
            raise RuntimeError(
                f'DART request failed [013]: no data for {rcept_no}'
            )

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=self._event_loader(
                {101: {**entry, 'report_nm': '사업보고서 (2025.12)'}}
            ),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            result = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['events_failed'], 1)
        self.assertTrue(result['retry_required'])

    async def test_explicit_evaluated_absence_is_terminal_without_fabrication(self):
        entry = _entry('005930', 101, '20260301000001')
        attempts = 0

        async def capture(*, rcept_no: str):
            nonlocal attempts
            attempts += 1
            return {
                'rcept_no': rcept_no,
                'audit_signal': _authoritative_absence_signal(entry),
                'audit_signal_evaluated': True,
            }

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=self._event_loader(
                {101: {**entry, 'report_nm': '사업보고서 (2025.12)'}}
            ),
            terminal_state_loader=lambda item: _terminal_proof(
                item,
                status='authoritative_absence',
            ),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            resumed = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(first['audit_signals_captured'], 0)
        self.assertEqual(first['audit_signals_authoritatively_absent'], 1)
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(resumed['skipped_batches'], 1)
        self.assertEqual(attempts, 1)

    async def test_malformed_audit_envelopes_remain_retry_debt(self):
        entry = _entry('005930', 101, '20260301000001')
        malformed_claim = _authoritative_absence_signal(entry)
        malformed_claim['payload']['evidence_semantics'] = 'tampered'
        malformed_claim['opinion_code'] = 'unqualified'
        contradictory_absence = _authoritative_absence_signal(entry)
        contradictory_absence['opinion_code'] = 'qualified'
        contradictory_absence['has_audit_issue'] = True
        inconsistent_parsed = {
            'opinion_code': 'qualified',
            'has_audit_issue': False,
        }

        for label, audit_signal in (
            ('schema_only_claim', malformed_claim),
            ('contradictory_absence', contradictory_absence),
            ('inconsistent_parsed', inconsistent_parsed),
        ):
            with self.subTest(label=label):
                async def capture(*, rcept_no: str):
                    return {
                        'rcept_no': rcept_no,
                        'audit_signal': audit_signal,
                        'audit_signal_evaluated': True,
                    }

                repair = ExactDartAuditSignalRepair(
                    _manifest(entry),
                    capture=capture,
                    event_loader=self._event_loader(
                        {
                            101: {
                                **entry,
                                'report_nm': '사업보고서 (2025.12)',
                            }
                        }
                    ),
                    terminal_state_loader=lambda _item: None,
                    guard_factory=_guard,
                    finality_validator=lambda _market_date: None,
                )
                with TemporaryDirectory() as directory:
                    result = await repair.run(
                        apply=True,
                        expected_manifest_sha256=repair.manifest_sha256,
                        checkpoint_path=(
                            Path(directory) / 'checkpoint.json'
                        ),
                    )

                self.assertEqual(result['status'], 'partial')
                self.assertTrue(result['retry_required'])
                self.assertEqual(
                    result['audit_evidence_missing'],
                    1,
                )

    async def test_completed_capture_is_replayed_after_durable_row_deletion(self):
        entry = _entry('005930', 101, '20260301000001')
        durable_signal = False
        attempts = 0

        async def capture(*, rcept_no: str):
            nonlocal attempts, durable_signal
            attempts += 1
            durable_signal = True
            return {
                'rcept_no': rcept_no,
                'audit_signal': _parsed_audit_signal(),
                'audit_signal_evaluated': True,
            }

        def terminal_state(item):
            return _terminal_proof(item) if durable_signal else None

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=self._event_loader(
                {101: {**entry, 'report_nm': '사업보고서 (2025.12)'}}
            ),
            terminal_state_loader=terminal_state,
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            durable_signal = False
            resumed = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(resumed['skipped_batches'], 0)
        self.assertEqual(attempts, 2)

    async def test_completed_capture_is_replayed_after_audit_content_drift(self):
        entry = _entry('005930', 101, '20260301000001')
        live_opinion = 'unqualified'
        attempts = 0

        async def capture(*, rcept_no: str):
            nonlocal attempts, live_opinion
            attempts += 1
            live_opinion = 'unqualified'
            return {
                'rcept_no': rcept_no,
                'audit_signal': _parsed_audit_signal(),
                'audit_signal_evaluated': True,
            }

        def terminal_state(item):
            proof = _terminal_proof(item)
            proof['audit_content'] = _parsed_audit_signal(live_opinion)
            proof['audit_content_sha256'] = canonical_json_sha256(
                proof['audit_content']
            )
            return proof

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=self._event_loader(
                {101: {**entry, 'report_nm': '사업보고서 (2025.12)'}}
            ),
            terminal_state_loader=terminal_state,
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            live_opinion = 'qualified'
            resumed = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(resumed['skipped_batches'], 0)
        self.assertEqual(attempts, 2)

    async def test_absence_checkpoint_replays_without_durable_negative_proof(self):
        entry = _entry('005930', 101, '20260301000001')
        attempts = 0

        async def capture(*, rcept_no: str):
            nonlocal attempts
            attempts += 1
            return {
                'rcept_no': rcept_no,
                'audit_signal': None,
                'audit_signal_evaluated': True,
            }

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=self._event_loader(
                {101: {**entry, 'report_nm': '사업보고서 (2025.12)'}}
            ),
            terminal_state_loader=lambda _item: None,
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            resumed = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'partial')
        self.assertEqual(resumed['status'], 'partial')
        self.assertEqual(resumed['skipped_batches'], 0)
        self.assertEqual(attempts, 2)

    async def test_completed_capture_does_not_survive_event_identity_drift(self):
        entry = _entry('005930', 101, '20260301000001')
        persisted = {
            **entry,
            'report_nm': '사업보고서 (2025.12)',
            'receipt_date': '2026-10-28',
        }

        async def capture(*, rcept_no: str):
            return {
                'rcept_no': rcept_no,
                'audit_signal': _parsed_audit_signal(),
                'audit_signal_evaluated': True,
            }

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=lambda _event_id: dict(persisted),
            terminal_state_loader=lambda item: _terminal_proof(item),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            persisted['stock_code'] = '000660'
            resumed = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'partial')
        self.assertEqual(resumed['events_failed'], 1)
        self.assertEqual(resumed['skipped_batches'], 0)

    async def test_diagnostics_do_not_promote_ambiguous_absence(self):
        entry = _entry('005930', 101, '20260301000001')

        async def capture(*, rcept_no: str):
            return {
                'rcept_no': rcept_no,
                'audit_signal': None,
                'audit_signal_evaluated': False,
                'audit_diagnostics': {
                    'authority_member_count': 1,
                    'decoded_member_count': 1,
                },
            }

        repair = ExactDartAuditSignalRepair(
            _manifest(entry),
            capture=capture,
            event_loader=self._event_loader(
                {101: {**entry, 'report_nm': '사업보고서 (2025.12)'}}
            ),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            result = await repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['audit_evidence_missing'], 1)
        self.assertEqual(result['audit_signals_authoritatively_absent'], 0)
        self.assertTrue(result['retry_required'])


if __name__ == '__main__':
    unittest.main()
