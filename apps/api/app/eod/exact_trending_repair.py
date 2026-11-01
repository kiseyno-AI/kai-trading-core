from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, suppress
from copy import deepcopy
from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any

from app.db import session_scope
from app.db_models import DailyTrendingRank
from app.eod.derived_stage_repair import (
    GuardTransactionExitError,
    capture_guard_transaction_failure,
    exact_derived_stage_guard,
    normalize_manifest,
)
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.market_membership import market_membership_sha256
from app.eod.price_bar_repair_state import (
    REQUIRED_TRENDING_PERIODS,
    _derived_output_fingerprint,
    load_pending_price_bar_rebuild_batches,
    record_pending_trending_rebuild,
    resolve_pending_frozen_price_dependent_membership,
)
from app.internal.exact_repair_manifest import (
    canonical_json_sha256,
    load_checkpoint,
    load_json_mapping,
    write_checkpoint,
)
from app.market_data_store import (
    _load_trending_stocks_from_daily_price_bars,
    _load_trending_stocks_from_snapshots,
    compute_and_persist_daily_trending_ranks,
    list_symbols_with_daily_bar_on_date,
)


CHECKPOINT_SCHEMA_VERSION = 'trending_rank_exact_date_checkpoint_v1'
REPAIR_SOURCE = 'exact_trending_rank_repair_v1'
PERIODS = tuple(REQUIRED_TRENDING_PERIODS)


def _normalize_symbols(values: Sequence[str]) -> list[str]:
    symbols = sorted(
        {
            str(value).strip().upper()
            for value in values
            if str(value).strip()
        }
    )
    if any(len(symbol) != 6 or not symbol.isdigit() for symbol in symbols):
        raise ValueError('trending repair scope contains an invalid symbol')
    return symbols


def _load_source_fingerprints(
    market_date: str,
    periods: Sequence[str],
    exact_symbols: Sequence[str],
) -> dict[str, dict[str, Any]]:
    parsed_date = date.fromisoformat(market_date)
    fingerprints: dict[str, dict[str, Any]] = {}
    for period in periods:
        if period == '1d':
            rows = _load_trending_stocks_from_daily_price_bars(
                normalized_market_date=parsed_date,
                direction='both',
                limit=None,
                markets=None,
                symbols=exact_symbols,
            )
        else:
            rows = _load_trending_stocks_from_snapshots(
                normalized_market_date=parsed_date,
                period=period,
                direction='both',
                limit=None,
                markets=None,
                symbols=exact_symbols,
            )
        fingerprints[period] = {
            'row_count': len(rows),
            'source_sha256': canonical_json_sha256(rows),
        }
    return fingerprints


def _load_completion_state(
    market_date: str,
    periods: Sequence[str],
    exact_symbols: Sequence[str],
) -> dict[str, Any]:
    parsed_date = date.fromisoformat(market_date)
    expected_scope_hash = market_membership_sha256(exact_symbols)
    source_fingerprints = _load_source_fingerprints(
        market_date,
        periods,
        exact_symbols,
    )
    output_fingerprints: dict[str, dict[str, Any]] = {}
    with session_scope(statement_timeout_ms=10_000) as session:
        for period in periods:
            output_sha256, row_count = _derived_output_fingerprint(
                session,
                model=DailyTrendingRank,
                market_date=parsed_date,
                symbols=None,
                periods=(period,),
            )
            output_fingerprints[period] = {
                'row_count': row_count,
                'output_sha256': output_sha256,
                'exact_scope': True,
                'scope_symbol_count': len(exact_symbols),
                'scope_membership_sha256': expected_scope_hash,
            }

    affected = [
        batch
        for batch in load_pending_price_bar_rebuild_batches()
        if market_date
        in {
            str(value)
            for value in batch.get('required_rebuild_trade_dates') or []
        }
    ]
    metadata_verified = bool(affected)
    for batch in affected:
        try:
            metadata = dict(batch['factor_rebuild_metadata'])
            if int(metadata['schema_version']) != 2:
                raise ValueError('unsupported rebuild evidence schema')
            recorded = dict(
                dict(metadata['dates'])[market_date][
                    'trending_output_fingerprints'
                ]
            )
            for period in periods:
                expected = output_fingerprints[period]
                observed = dict(recorded[period])
                for field in (
                    'row_count',
                    'output_sha256',
                    'exact_scope',
                    'scope_symbol_count',
                    'scope_membership_sha256',
                ):
                    if observed.get(field) != expected[field]:
                        raise ValueError('trending rebuild evidence drift')
        except (KeyError, TypeError, ValueError):
            metadata_verified = False
            break
    return {
        'source_fingerprints': source_fingerprints,
        'output_fingerprints': output_fingerprints,
        'metadata_verified': metadata_verified,
        'pending_batch_count': len(affected),
    }


class ExactTrendingRankRepair:
    """Rebuild only stored-data trending ranks for an immutable date scope."""

    def __init__(
        self,
        manifest: Mapping[str, Any],
        *,
        trending_runner: Callable[..., Mapping[str, Any]] | None = None,
        evidence_recorder: Callable[..., Mapping[str, Any]] | None = None,
        stored_symbols_loader: Callable[[str], Sequence[str]] | None = None,
        frozen_membership_loader: Callable[..., Sequence[str] | None]
        | None = None,
        completion_state_loader: Callable[
            [str, Sequence[str], Sequence[str]], Mapping[str, Any]
        ]
        | None = None,
        guard_factory: Callable[..., AbstractContextManager[Any]] | None = None,
        finality_validator: Callable[[date], None] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.manifest = normalize_manifest(manifest)
        self.manifest_sha256 = canonical_json_sha256(self.manifest)
        self.trending_runner = (
            trending_runner or compute_and_persist_daily_trending_ranks
        )
        self.evidence_recorder = (
            evidence_recorder or record_pending_trending_rebuild
        )
        self.stored_symbols_loader = (
            stored_symbols_loader or list_symbols_with_daily_bar_on_date
        )
        self.frozen_membership_loader = (
            frozen_membership_loader
            or resolve_pending_frozen_price_dependent_membership
        )
        self.completion_state_loader = (
            completion_state_loader or _load_completion_state
        )
        self.guard_factory = guard_factory or exact_derived_stage_guard
        self.finality_validator = finality_validator or (
            lambda market_date: validate_finalized_krx_write_date(
                market_date,
                operation='exact stored-data trending-rank repair',
            )
        )
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def _validate_scope(self, item: Mapping[str, Any]) -> None:
        expected = list(item['symbols'])
        stored = _normalize_symbols(
            self.stored_symbols_loader(str(item['market_date']))
        )
        if not set(expected).issubset(stored):
            raise ValueError(
                'stored daily-price-bar membership is missing manifest symbols'
            )
        frozen = self.frozen_membership_loader(
            market_date=str(item['market_date'])
        )
        if frozen is None:
            raise ValueError(
                'pending frozen price-dependent membership is unavailable'
            )
        if _normalize_symbols(frozen) != expected:
            raise ValueError(
                'pending frozen membership does not match manifest'
            )

    @staticmethod
    def _validate_trending_result(
        result: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> None:
        if (
            result.get('status') != 'completed'
            or result.get('exact_scope') is not True
            or result.get('scope_symbol_count') != item['symbol_count']
            or result.get('scope_membership_sha256')
            != item['membership_sha256']
            or result.get('periods_total') != len(PERIODS)
            or result.get('periods_persisted') != len(PERIODS)
            or result.get('periods_unavailable') != 0
        ):
            raise RuntimeError('trending rebuild did not complete exact scope')
        period_results = result.get('periods')
        if not isinstance(period_results, list):
            raise RuntimeError('trending rebuild returned no period results')
        observed = {
            str(period_result.get('period') or ''): period_result.get('status')
            for period_result in period_results
            if isinstance(period_result, Mapping)
        }
        if observed != {period: 'completed' for period in PERIODS}:
            raise RuntimeError('trending rebuild period coverage is incomplete')

    @staticmethod
    def _validate_evidence(
        evidence: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> dict[str, dict[str, Any]]:
        if int(evidence.get('updated_batch_count') or 0) <= 0:
            raise RuntimeError('no pending batch received trending evidence')
        raw = evidence.get('output_fingerprints')
        if not isinstance(raw, Mapping) or set(raw) != set(PERIODS):
            raise RuntimeError('trending output fingerprints are incomplete')
        validated: dict[str, dict[str, Any]] = {}
        for period in PERIODS:
            period_evidence = raw.get(period)
            if not isinstance(period_evidence, Mapping):
                raise RuntimeError('trending output fingerprint is invalid')
            expected = {
                'exact_scope': True,
                'scope_symbol_count': item['symbol_count'],
                'scope_membership_sha256': item['membership_sha256'],
            }
            if (
                int(period_evidence.get('row_count') or 0) <= 0
                or not str(period_evidence.get('output_sha256') or '')
                or any(
                    period_evidence.get(field) != value
                    for field, value in expected.items()
                )
            ):
                raise RuntimeError('trending output fingerprint is invalid')
            validated[period] = dict(period_evidence)
        return validated

    @staticmethod
    def _validate_source_fingerprints(
        raw: Any,
    ) -> dict[str, dict[str, Any]]:
        if not isinstance(raw, Mapping) or set(raw) != set(PERIODS):
            raise RuntimeError('trending source fingerprints are incomplete')
        validated: dict[str, dict[str, Any]] = {}
        for period in PERIODS:
            value = raw.get(period)
            if not isinstance(value, Mapping):
                raise RuntimeError('trending source fingerprint is invalid')
            row_count = value.get('row_count')
            source_sha256 = value.get('source_sha256')
            if (
                isinstance(row_count, bool)
                or not isinstance(row_count, int)
                or row_count <= 0
                or not isinstance(source_sha256, str)
                or len(source_sha256) != 64
            ):
                raise RuntimeError('trending source fingerprint is invalid')
            validated[period] = {
                'row_count': row_count,
                'source_sha256': source_sha256,
            }
        return validated

    def _run_date(self, item: Mapping[str, Any]) -> dict[str, Any]:
        self._validate_scope(item)
        market_date = str(item['market_date'])
        symbols = list(item['symbols'])
        initial_state = self.completion_state_loader(
            market_date,
            PERIODS,
            symbols,
        )
        source_fingerprints = self._validate_source_fingerprints(
            initial_state.get('source_fingerprints')
        )
        repair_started_at = self.clock()
        result = dict(
            self.trending_runner(
                market_date=market_date,
                periods=list(PERIODS),
                source=REPAIR_SOURCE,
                symbols=symbols,
                authoritative_scope=True,
            )
        )
        self._validate_trending_result(result, item)
        completed_at = self.clock()
        evidence = dict(
            self.evidence_recorder(
                market_date=market_date,
                periods=PERIODS,
                repair_started_at=repair_started_at,
                rebuild_completed_at=completed_at,
                exact_symbols=symbols,
            )
        )
        fingerprints = self._validate_evidence(evidence, item)
        final_state = self.completion_state_loader(
            market_date,
            PERIODS,
            symbols,
        )
        if self._validate_source_fingerprints(
            final_state.get('source_fingerprints')
        ) != source_fingerprints:
            raise RuntimeError('trending source dependency changed during repair')
        if (
            final_state.get('metadata_verified') is not True
            or final_state.get('output_fingerprints') != fingerprints
        ):
            raise RuntimeError('trending output evidence failed live verification')
        return {
            'market_date': market_date,
            'status': 'completed',
            'exact_symbol_count': item['symbol_count'],
            'membership_sha256': item['membership_sha256'],
            'periods': list(PERIODS),
            'repair_started_at': repair_started_at.isoformat(),
            'rebuild_completed_at': completed_at.isoformat(),
            'trending_result': result,
            'source_fingerprints': source_fingerprints,
            'evidence': {
                **evidence,
                'output_fingerprints': fingerprints,
            },
        }

    @classmethod
    def _completed_checkpoint_is_valid(
        cls,
        unit: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> bool:
        result = unit.get('result')
        if not isinstance(result, Mapping) or result.get('status') != 'completed':
            return False
        if (
            result.get('market_date') != item['market_date']
            or result.get('exact_symbol_count') != item['symbol_count']
            or result.get('membership_sha256') != item['membership_sha256']
            or result.get('periods') != list(PERIODS)
        ):
            return False
        trending_result = result.get('trending_result')
        evidence = result.get('evidence')
        if not isinstance(trending_result, Mapping) or not isinstance(
            evidence, Mapping
        ):
            return False
        try:
            cls._validate_trending_result(trending_result, item)
            cls._validate_evidence(evidence, item)
            cls._validate_source_fingerprints(
                result.get('source_fingerprints')
            )
        except (RuntimeError, TypeError, ValueError):
            return False
        return True

    def _completed_live_state_is_valid(
        self,
        unit: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> bool:
        self._validate_scope(item)
        live = self.completion_state_loader(
            str(item['market_date']),
            PERIODS,
            list(item['symbols']),
        )
        if not isinstance(live, Mapping) or live.get('metadata_verified') is not True:
            return False
        expected = dict(unit['result']['evidence']['output_fingerprints'])
        observed = live.get('output_fingerprints')
        if not isinstance(observed, Mapping) or set(observed) != set(PERIODS):
            return False
        for period in PERIODS:
            expected_period = dict(expected[period])
            observed_period = observed.get(period)
            if not isinstance(observed_period, Mapping):
                return False
            for field in (
                'row_count',
                'output_sha256',
                'exact_scope',
                'scope_symbol_count',
                'scope_membership_sha256',
            ):
                if observed_period.get(field) != expected_period.get(field):
                    return False
        try:
            observed_sources = self._validate_source_fingerprints(
                live.get('source_fingerprints')
            )
        except RuntimeError:
            return False
        if observed_sources != unit['result'].get('source_fingerprints'):
            return False
        return True

    def _reconcile_applied_result(
        self,
        *,
        item: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> tuple[bool, dict[str, Any]]:
        """Re-read durable exact rows after a fence/checkpoint uncertainty."""

        reconciled_unit = {'result': dict(result)}
        try:
            live_valid = bool(
                self._completed_checkpoint_is_valid(
                    reconciled_unit,
                    item,
                )
                and self._completed_live_state_is_valid(
                    reconciled_unit,
                    item,
                )
            )
        except Exception as exc:  # noqa: BLE001
            return False, {
                'error_type': type(exc).__name__,
                'error_message': str(exc)[:500],
            }
        evidence = result.get('evidence')
        return live_valid, {
            'output_fingerprints': (
                dict(evidence.get('output_fingerprints'))
                if isinstance(evidence, Mapping)
                and isinstance(evidence.get('output_fingerprints'), Mapping)
                else None
            )
        }

    def run(
        self,
        *,
        apply: bool = False,
        expected_manifest_sha256: str | None = None,
        checkpoint_path: str | Path | None = None,
    ) -> dict[str, Any]:
        base = {
            'manifest_sha256': self.manifest_sha256,
            'date_count': self.manifest['date_count'],
            'periods': list(PERIODS),
            'mode': 'apply' if apply else 'dry_run',
        }
        if not apply:
            errors: list[dict[str, str]] = []
            for item in self.manifest['dates']:
                try:
                    self._validate_scope(item)
                except (RuntimeError, ValueError) as exc:
                    errors.append(
                        {
                            'market_date': str(item['market_date']),
                            'error': str(exc),
                        }
                    )
            return {
                **base,
                'status': 'dry_run' if not errors else 'blocked',
                'applied': False,
                'validated_date_count': self.manifest['date_count'] - len(errors),
                'scope_errors': errors,
            }

        expected_hash = str(expected_manifest_sha256 or '').strip().lower()
        if not expected_hash:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'expected_manifest_sha256_required',
            }
        if expected_hash != self.manifest_sha256:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'expected_manifest_sha256_mismatch',
            }
        if checkpoint_path is None:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_path_required',
            }
        try:
            checkpoint = load_checkpoint(
                checkpoint_path,
                schema_version=CHECKPOINT_SCHEMA_VERSION,
                manifest_sha256=self.manifest_sha256,
                item_count=self.manifest['date_count'],
                unit_size=1,
            )
        except ValueError as exc:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_manifest_mismatch',
                'error_message': str(exc),
            }

        expected_dates = {
            str(item['market_date']) for item in self.manifest['dates']
        }
        if set(checkpoint['units']) - expected_dates:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_date_set_mismatch',
            }
        completed_dates = 0
        skipped_dates = 0
        failed_dates = 0
        for item in self.manifest['dates']:
            market_date = str(item['market_date'])
            item_sha256 = canonical_json_sha256(item)
            previous = checkpoint['units'].get(market_date)
            completed_unit: Mapping[str, Any] | None = None
            if isinstance(previous, Mapping):
                if previous.get('item_sha256') != item_sha256:
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': 'checkpoint_date_hash_mismatch',
                        'market_date': market_date,
                    }
                if previous.get('status') == 'completed':
                    if not self._completed_checkpoint_is_valid(previous, item):
                        return {
                            **base,
                            'status': 'blocked',
                            'applied': False,
                            'reason': 'checkpoint_completed_date_invalid',
                            'market_date': market_date,
                        }
                    completed_unit = previous
            try:
                self.finality_validator(date.fromisoformat(market_date))
            except (TypeError, ValueError) as exc:
                result = {
                    'market_date': market_date,
                    'status': 'failed',
                    'error_type': type(exc).__name__,
                    'error_message': str(exc)[:500],
                }
                checkpoint['units'][market_date] = {
                    'status': 'failed',
                    'item_sha256': item_sha256,
                    'result': result,
                }
                write_checkpoint(checkpoint_path, checkpoint)
                failed_dates += 1
                continue

            result: dict[str, Any] | None = None
            guard_exit_state: dict[str, Any] = {}
            with suppress(
                GuardTransactionExitError
            ), capture_guard_transaction_failure(
                self.guard_factory,
                state=guard_exit_state,
                market_date=date.fromisoformat(market_date),
            ) as guard:
                acquired, reason = guard
                if not acquired:
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': str(reason or 'active_writer'),
                        'market_date': market_date,
                    }
                if completed_unit is not None:
                    try:
                        live_valid = self._completed_live_state_is_valid(
                            completed_unit,
                            item,
                        )
                    except ValueError:
                        live_valid = False
                    except Exception as exc:  # noqa: BLE001
                        return {
                            **base,
                            'status': 'blocked',
                            'applied': False,
                            'reason': 'checkpoint_live_revalidation_failed',
                            'market_date': market_date,
                            'error_type': type(exc).__name__,
                            'error_message': str(exc)[:500],
                        }
                    if live_valid:
                        completed_dates += 1
                        skipped_dates += 1
                        continue
                try:
                    result = self._run_date(item)
                except Exception as exc:  # noqa: BLE001
                    result = {
                        'market_date': market_date,
                        'status': 'failed',
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
            guard_exit_error = guard_exit_state.get('error')
            if isinstance(guard_exit_error, Exception):
                live_valid = False
                reconciliation: dict[str, Any] = {}
                if result is not None:
                    live_valid, reconciliation = (
                        self._reconcile_applied_result(
                            item=item,
                            result=result,
                        )
                    )
                uncertain_attempt = {
                    'reason': 'guard_transaction_exit_failed',
                    'error_type': type(guard_exit_error).__name__,
                    'error_message': str(guard_exit_error)[:500],
                    'result_status': (
                        result.get('status')
                        if isinstance(result, Mapping)
                        else None
                    ),
                    'live_completion_verified': live_valid,
                    'reconciliation': reconciliation,
                }
                uncertain_unit = (
                    deepcopy(dict(previous))
                    if isinstance(previous, Mapping)
                    else {
                        'status': 'uncertain',
                        'item_sha256': item_sha256,
                    }
                )
                uncertain_unit['last_uncertain_attempt'] = uncertain_attempt
                checkpoint['units'][market_date] = uncertain_unit
                checkpoint_error: Exception | None = None
                try:
                    write_checkpoint(checkpoint_path, checkpoint)
                except Exception as exc:  # noqa: BLE001
                    checkpoint_error = exc
                response = {
                    **base,
                    'status': 'partial' if live_valid else 'blocked',
                    'applied': False,
                    'reason': 'guard_transaction_exit_failed',
                    'market_date': market_date,
                    'error_type': type(guard_exit_error).__name__,
                    'error_message': str(guard_exit_error)[:500],
                    'uncertain_commit': True,
                    'checkpoint_persisted': checkpoint_error is None,
                    'reconciled_proof_count': int(live_valid),
                    'retry_required': True,
                    'reconciliation': reconciliation,
                }
                if checkpoint_error is not None:
                    response.update(
                        {
                            'checkpoint_error_type': type(
                                checkpoint_error
                            ).__name__,
                            'checkpoint_error_message': str(
                                checkpoint_error
                            )[:500],
                        }
                    )
                return response
            if result is None:
                return {
                    **base,
                    'status': 'blocked',
                    'applied': False,
                    'reason': 'trending_apply_result_missing',
                    'market_date': market_date,
                }
            checkpoint['units'][market_date] = {
                'status': result['status'],
                'item_sha256': item_sha256,
                'result': result,
            }
            try:
                write_checkpoint(checkpoint_path, checkpoint)
            except Exception as checkpoint_error:  # noqa: BLE001
                live_valid, reconciliation = self._reconcile_applied_result(
                    item=item,
                    result=result,
                )
                return {
                    **base,
                    'status': 'partial' if live_valid else 'blocked',
                    'applied': False,
                    'reason': 'checkpoint_write_failed_after_commit',
                    'market_date': market_date,
                    'error_type': type(checkpoint_error).__name__,
                    'error_message': str(checkpoint_error)[:500],
                    'uncertain_commit': True,
                    'checkpoint_persisted': False,
                    'reconciled_proof_count': int(live_valid),
                    'retry_required': True,
                    'reconciliation': reconciliation,
                }
            if result['status'] == 'completed':
                completed_dates += 1
            else:
                failed_dates += 1

        status = 'completed' if failed_dates == 0 else 'partial'
        return {
            **base,
            'status': status,
            'applied': status == 'completed',
            'completed_dates': completed_dates,
            'skipped_dates': skipped_dates,
            'failed_dates': failed_dates,
            'retry_required': failed_dates > 0,
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Rebuild exact stored-data trending ranks and record pending '
            'repair evidence without invoking a daily pipeline.'
        )
    )
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--checkpoint')
    parser.add_argument('--expected-manifest-sha256')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--dry-run', action='store_true')
    mode.add_argument('--apply', action='store_true')
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.apply and args.checkpoint and (
        Path(args.manifest).resolve() == Path(args.checkpoint).resolve()
    ):
        raise ValueError('manifest and checkpoint paths must be different')
    manifest = load_json_mapping(args.manifest, label='manifest')
    repair = ExactTrendingRankRepair(manifest)
    result = repair.run(
        apply=bool(args.apply),
        expected_manifest_sha256=args.expected_manifest_sha256,
        checkpoint_path=args.checkpoint,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result['status'] in {'completed', 'dry_run'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
