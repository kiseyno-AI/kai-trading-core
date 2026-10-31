import copy
import hashlib
import json
import unittest
from datetime import date, timedelta

from app.dart.share_issuance_audit import (
    DART_SHARE_ISSUANCE_AGGREGATE_AUDIT_VERSION,
    DART_SHARE_ISSUANCE_ACTIVATION_AUDIT_TRIGGER,
    DART_SHARE_ISSUANCE_AUDIT_SCOPE_EXPLICIT_SUBSET,
    DART_SHARE_ISSUANCE_AUDIT_SCOPE_GLOBAL_ACTIVATION,
    DART_SHARE_ISSUANCE_AUDIT_TRIGGER,
    DART_SHARE_ISSUANCE_MANIFEST_VERSION,
    DART_SHARE_ISSUANCE_MANIFEST_SOURCE_ACTIVE_UNIVERSE,
    DART_SHARE_ISSUANCE_MANIFEST_SOURCE_EXPLICIT,
    DART_SHARE_ISSUANCE_OPS_POLICY_VERSION,
    DART_SHARE_ISSUANCE_OUTCOME_MANIFEST_VERSION,
    inspect_share_issuance_activation_audit,
)
from app.dart.signals_service import (
    SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION,
    SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS,
    SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA,
    SHARE_ISSUANCE_RULE_VERSION,
)


def _sha256(value):
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


class ShareIssuanceActivationAuditTests(unittest.TestCase):
    market_date = date(2026, 10, 30)
    stock_codes = ['000660', '005930', '035420']

    def _run(self, *, activation_scope=True):
        manifest_source = (
            DART_SHARE_ISSUANCE_MANIFEST_SOURCE_ACTIVE_UNIVERSE
            if activation_scope
            else DART_SHARE_ISSUANCE_MANIFEST_SOURCE_EXPLICIT
        )
        audit_scope = (
            DART_SHARE_ISSUANCE_AUDIT_SCOPE_GLOBAL_ACTIVATION
            if activation_scope
            else DART_SHARE_ISSUANCE_AUDIT_SCOPE_EXPLICIT_SUBSET
        )
        window_start = (
            self.market_date
            - timedelta(days=SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS)
        ).isoformat()
        window_end = self.market_date.isoformat()
        manifest = {
            'manifest_version': DART_SHARE_ISSUANCE_MANIFEST_VERSION,
            'policy_version': DART_SHARE_ISSUANCE_OPS_POLICY_VERSION,
            'market_date': self.market_date.isoformat(),
            'window_start': window_start,
            'window_end': window_end,
            'requested_stock_codes': self.stock_codes,
            'manifest_source': manifest_source,
            'audit_scope': audit_scope,
        }
        manifest_hash = _sha256(manifest)
        outcomes = [
            {
                'stock_code': '000660',
                'corp_code': 'corp-000660',
                'classification': 'completed_positive',
                'positive_event_date': self.market_date.isoformat(),
            },
            {
                'stock_code': '005930',
                'corp_code': 'corp-005930',
                'classification': 'completed_negative',
                'coverage_window_start': window_start,
                'coverage_window_end': window_end,
            },
            {
                'stock_code': '035420',
                'classification': 'terminal_not_applicable',
                'reason': 'authoritative_unsupported_mapping',
            },
        ]
        outcome_manifest = {
            'outcome_manifest_version': (
                DART_SHARE_ISSUANCE_OUTCOME_MANIFEST_VERSION
            ),
            'policy_version': DART_SHARE_ISSUANCE_OPS_POLICY_VERSION,
            'source_manifest_version': DART_SHARE_ISSUANCE_MANIFEST_VERSION,
            'source_manifest_hash': manifest_hash,
            'market_date': self.market_date.isoformat(),
            'window_start': window_start,
            'window_end': window_end,
            'requested_stock_codes': self.stock_codes,
            'manifest_source': manifest_source,
            'audit_scope': audit_scope,
            'outcomes': outcomes,
        }
        aggregate = {
            'audit_version': DART_SHARE_ISSUANCE_AGGREGATE_AUDIT_VERSION,
            'policy_version': DART_SHARE_ISSUANCE_OPS_POLICY_VERSION,
            'evidence_policy': {
                'authority_policy_version': (
                    SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
                ),
                'coverage_schema_version': (
                    SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA
                ),
                'rule_version': SHARE_ISSUANCE_RULE_VERSION,
            },
            'manifest_version': DART_SHARE_ISSUANCE_MANIFEST_VERSION,
            'manifest_hash': manifest_hash,
            'manifest_source': manifest_source,
            'audit_scope': audit_scope,
            'market_date': self.market_date.isoformat(),
            'window_start': window_start,
            'window_end': window_end,
            'requested_count': len(self.stock_codes),
            'requested_stock_codes': self.stock_codes,
            'mapping_authority': {
                'source': 'upstream',
                'authoritative': True,
            },
            'mapping_lookup_error': None,
            'evidence_read_error': None,
            'unexpected_mapping_stock_codes': [],
            'classification_stock_codes': {
                'completed_positive': ['000660'],
                'completed_negative': ['005930'],
                'terminal_not_applicable': ['035420'],
                'retry_debt': [],
            },
            'classification_counts': {
                'completed_positive': 1,
                'completed_negative': 1,
                'terminal_not_applicable': 1,
                'retry_debt': 0,
            },
            'outcome_manifest': outcome_manifest,
            'outcome_manifest_hash_algorithm': 'sha256',
            'outcome_manifest_sha256': _sha256(outcome_manifest),
            'aggregate_evidence_ready': True,
            'aggregate_evidence_complete': True,
            'durable_audit_complete': True,
            'aggregate_manifest_classification_complete': True,
            'aggregate_mapping_contract_exact': True,
            'persistence_status': 'completed',
            'audit_run_id': 91,
        }
        return {
            'id': 91,
            'trigger': (
                DART_SHARE_ISSUANCE_ACTIVATION_AUDIT_TRIGGER
                if activation_scope
                else DART_SHARE_ISSUANCE_AUDIT_TRIGGER
            ),
            'status': 'completed',
            'window_start': window_start,
            'window_end': window_end,
            'started_at': '2026-10-30T17:00:00+00:00',
            'completed_at': '2026-10-30T17:01:00+00:00',
            'run_metadata': {
                'audit_type': 'dart_share_issuance_aggregate',
                'aggregate_audit': aggregate,
            },
        }

    def _inspect(self, run, **overrides):
        return inspect_share_issuance_activation_audit(
            run,
            current_market_date=overrides.get('market_date'),
            current_stock_codes=overrides.get('stock_codes'),
        )

    def test_exact_durable_bootstrap_proof_enables(self):
        result = self._inspect(self._run())

        self.assertTrue(result['enabled'])
        self.assertEqual(result['reasons'], [])
        self.assertEqual(
            result['manifest_hash'], result['recomputed_manifest_hash']
        )

    def test_later_market_date_and_additive_symbols_do_not_revoke_bootstrap(self):
        run = self._run()

        result = self._inspect(
            run,
            market_date=self.market_date + timedelta(days=1),
            stock_codes=[*self.stock_codes, '051910'],
        )

        self.assertTrue(result['enabled'])
        self.assertEqual(
            result['current_market_date'],
            (self.market_date + timedelta(days=1)).isoformat(),
        )
        self.assertEqual(
            result['current_requested_count'], len(self.stock_codes) + 1
        )

    def test_explicit_subset_audit_cannot_authorize_global_activation(self):
        result = self._inspect(self._run(activation_scope=False))

        self.assertFalse(result['enabled'])
        self.assertIn('audit_trigger_mismatch', result['reasons'])
        self.assertIn(
            'activation_manifest_source_mismatch', result['reasons']
        )
        self.assertIn('activation_audit_scope_mismatch', result['reasons'])

    def test_run_and_aggregate_windows_are_both_authoritative(self):
        run = self._run()
        run['window_start'] = '2025-10-31'
        run['run_metadata']['aggregate_audit']['window_end'] = '2026-10-29'

        result = self._inspect(run)

        self.assertFalse(result['enabled'])
        self.assertIn('audit_run_window_start_mismatch', result['reasons'])
        self.assertIn('audit_window_end_mismatch', result['reasons'])

    def test_nested_policy_and_mapping_authority_drift_fails_closed(self):
        run = self._run()
        aggregate = run['run_metadata']['aggregate_audit']
        aggregate['evidence_policy']['coverage_schema_version'] = 'stale-v0'
        aggregate['mapping_authority']['authoritative'] = False

        result = self._inspect(run)

        self.assertFalse(result['enabled'])
        self.assertIn(
            'audit_evidence_policy_version_mismatch', result['reasons']
        )
        self.assertIn(
            'mapping_authority_not_authoritative', result['reasons']
        )

    def test_outcome_source_and_classification_tampering_fails_closed(self):
        run = self._run()
        aggregate = run['run_metadata']['aggregate_audit']
        outcome_manifest = aggregate['outcome_manifest']
        outcome_manifest['source_manifest_hash'] = '0' * 64
        outcome_manifest['outcomes'][0]['classification'] = (
            'completed_negative'
        )
        aggregate['outcome_manifest_sha256'] = _sha256(outcome_manifest)

        result = self._inspect(run)

        self.assertFalse(result['enabled'])
        self.assertIn(
            'outcome_source_manifest_hash_mismatch', result['reasons']
        )
        self.assertIn(
            'outcome_classification_partition_mismatch', result['reasons']
        )

    def test_short_window_or_incompletely_persisted_proof_fails_closed(self):
        run = self._run()
        short_window = copy.deepcopy(run)
        short_window['window_start'] = '2025-10-31'
        short_window['run_metadata']['aggregate_audit'][
            'window_start'
        ] = '2025-10-31'
        incomplete = copy.deepcopy(run)
        incomplete['run_metadata']['aggregate_audit'][
            'aggregate_evidence_ready'
        ] = False
        incomplete_result = self._inspect(incomplete)

        short_result = self._inspect(short_window)

        self.assertFalse(short_result['enabled'])
        self.assertIn(
            'audit_window_coverage_too_short', short_result['reasons']
        )
        self.assertFalse(incomplete_result['enabled'])
        self.assertIn(
            'aggregate_evidence_ready_not_proven',
            incomplete_result['reasons'],
        )

    def test_malformed_persisted_payload_fails_closed_without_raising(self):
        run = self._run()
        run['run_metadata'] = ['not', 'an', 'object']

        result = self._inspect(run)

        self.assertFalse(result['enabled'])
        self.assertIn('audit_run_metadata_invalid', result['reasons'])
        self.assertIn('aggregate_audit_invalid', result['reasons'])


if __name__ == '__main__':
    unittest.main()
