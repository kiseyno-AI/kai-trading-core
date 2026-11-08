"""Weekday data-gap fill job.

See ``docs/DATA_GAP_FILL.md`` for the specification. Short version: during
weekday market hours Core is idle, so detect gaps in the last few finalized
sessions, repair the ones that can be repaired idempotently and additively
(Tier A) under the existing writer fences, and only report the ones that would
need an audited price rewrite (Tier B).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
import json
import logging
import time as time_module
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo('Asia/Seoul')

# Order matters: participation contexts are built from the signal-feature rows.
TIER_A_KINDS: tuple[str, ...] = (
    'signal_features_missing',
    'participation_context_missing',
    'turnover_incomplete',
)
# Reported only. Fixing these rewrites prices and needs the audited repair path.
TIER_B_KINDS: tuple[str, ...] = (
    'provisional_bars',
    'missing_bars',
)

_COMPLETED_STATUSES = frozenset({'completed', 'completed_partial'})


@dataclass(frozen=True)
class Gap:
    kind: str
    market_date: str
    count: int
    detail: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            'kind': self.kind,
            'market_date': self.market_date,
            'count': self.count,
            'detail': dict(self.detail) if self.detail is not None else None,
        }


@dataclass(frozen=True)
class GapFillConfig:
    enabled: bool = False
    apply: bool = False
    lookback_trading_days: int = 10
    window_start: time = time(9, 30)
    window_end: time = time(15, 0)
    min_interval_seconds: int = 1800
    budget_seconds: int = 900
    failure_cooldown_seconds: int = 6 * 3600
    provider_check_sessions: int = 2


Detector = Callable[[str], Gap | None]
Fixer = Callable[[str], Mapping[str, Any]]


@dataclass
class DataGapFillService:
    config: GapFillConfig
    sessions_loader: Callable[[int], Sequence[str]]
    detectors: Mapping[str, Detector]
    fixers: Mapping[str, Fixer]
    finality_validator: Callable[..., Any]
    clock: Callable[[], datetime] = lambda: datetime.now(KST)
    monotonic: Callable[[], float] = time_module.monotonic
    is_trading_day: Callable[[date], bool] = lambda day: day.weekday() < 5
    _last_run_at: datetime | None = field(default=None, init=False)
    _cooldowns: dict[tuple[str, str], datetime] = field(default_factory=dict, init=False)
    last_result: dict[str, Any] | None = field(default=None, init=False)

    # ------------------------------------------------------------------ schedule
    def _not_due_reason(self, now: datetime) -> str | None:
        local = now.astimezone(KST)
        if local.weekday() >= 5 or not self.is_trading_day(local.date()):
            return 'not_a_trading_day'
        if not (self.config.window_start <= local.time().replace(tzinfo=None) < self.config.window_end):
            return 'outside_window'
        if self._last_run_at is not None and (now - self._last_run_at) < timedelta(
            seconds=self.config.min_interval_seconds
        ):
            return 'interval_not_elapsed'
        return None

    def run_if_due(self, *, now: datetime | None = None, force_schedule: bool = False) -> dict[str, Any]:
        if not self.config.enabled and not force_schedule:
            return self._record({'status': 'disabled'})
        current = now or self.clock()
        if not force_schedule:
            reason = self._not_due_reason(current)
            if reason is not None:
                return self._record({'status': 'not_due', 'reason': reason})
        return self._record(self._run(current))

    def _record(self, result: dict[str, Any]) -> dict[str, Any]:
        self.last_result = result
        return result

    # ------------------------------------------------------------------ one run
    def _run(self, now: datetime) -> dict[str, Any]:
        started = self.monotonic()
        today = now.astimezone(KST).date()
        self._last_run_at = now

        candidates = list(self.sessions_loader(self.config.lookback_trading_days + 1))
        sessions = [d for d in candidates if date.fromisoformat(d) < today][
            -self.config.lookback_trading_days :
        ]
        finalized: list[str] = []
        skipped_unfinalized: list[str] = []
        for market_date in sessions:
            try:
                self.finality_validator(
                    date.fromisoformat(market_date),
                    operation='data gap fill',
                )
            except Exception:  # noqa: BLE001
                skipped_unfinalized.append(market_date)
                continue
            finalized.append(market_date)

        gaps: list[Gap] = []
        detector_errors: list[dict[str, str]] = []
        for market_date in finalized:
            for kind in (*TIER_A_KINDS, *TIER_B_KINDS):
                detector = self.detectors.get(kind)
                if detector is None:
                    continue
                try:
                    gap = detector(market_date)
                except Exception as exc:  # noqa: BLE001
                    logger.exception('data gap detector %s failed for %s', kind, market_date)
                    detector_errors.append(
                        {'kind': kind, 'market_date': market_date, 'error': f'{type(exc).__name__}: {exc}'[:300]}
                    )
                    continue
                if gap is not None:
                    gaps.append(gap)

        tier_b = [gap.as_dict() for gap in gaps if gap.kind in TIER_B_KINDS]
        tier_a = [gap for gap in gaps if gap.kind in TIER_A_KINDS]
        tier_a.sort(key=lambda gap: (gap.market_date, TIER_A_KINDS.index(gap.kind)))

        repaired: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        deferred: list[dict[str, Any]] = []
        no_progress: list[dict[str, Any]] = []
        if self.config.apply:
            for gap in tier_a:
                key = (gap.kind, gap.market_date)
                expires = self._cooldowns.get(key)
                if expires is not None and expires > now:
                    deferred.append({**gap.as_dict(), 'reason': 'cooldown'})
                    continue
                if (self.monotonic() - started) >= self.config.budget_seconds:
                    deferred.append({**gap.as_dict(), 'reason': 'budget_exhausted'})
                    continue
                fixer = self.fixers.get(gap.kind)
                if fixer is None:
                    deferred.append({**gap.as_dict(), 'reason': 'no_fixer'})
                    continue
                try:
                    outcome = dict(fixer(gap.market_date))
                except Exception as exc:  # noqa: BLE001
                    logger.exception('data gap fixer %s failed for %s', gap.kind, gap.market_date)
                    failed.append({**gap.as_dict(), 'error': f'{type(exc).__name__}: {exc}'[:300]})
                    self._cool_down(key, now)
                    continue
                status = str(outcome.get('status') or '')
                if status in _COMPLETED_STATUSES:
                    progress = outcome.get('updated_count', outcome.get('persisted_row_count'))
                    if progress == 0:
                        # Completed but changed nothing (e.g. the remaining rows
                        # cannot be verified): do not retry every tick.
                        no_progress.append({**gap.as_dict(), 'result': _small(outcome)})
                        self._cool_down(key, now)
                    else:
                        repaired.append({**gap.as_dict(), 'result': _small(outcome)})
                elif status == 'blocked':
                    deferred.append({**gap.as_dict(), 'reason': str(outcome.get('reason') or 'blocked')})
                    self._cool_down(key, now, seconds=min(self.config.failure_cooldown_seconds, 1800))
                else:
                    failed.append({**gap.as_dict(), 'error': f'unexpected fixer status: {status or "none"}'})
                    self._cool_down(key, now)

        result = {
            'status': 'completed',
            'apply': self.config.apply,
            'sessions': finalized,
            'skipped_unfinalized': skipped_unfinalized,
            'gaps': [gap.as_dict() for gap in tier_a],
            'tier_b': tier_b,
            'repaired': repaired,
            'repaired_kinds': [kind for kind in TIER_A_KINDS if any(item['kind'] == kind for item in repaired)],
            'no_progress': no_progress,
            'deferred': deferred,
            'failed': failed,
            'detector_errors': detector_errors,
            'elapsed_seconds': round(self.monotonic() - started, 3),
        }
        logger.info(
            'data gap fill: sessions=%d gaps=%d tier_b=%d repaired=%d deferred=%d failed=%d apply=%s',
            len(finalized),
            len(tier_a),
            len(tier_b),
            len(repaired),
            len(deferred),
            len(failed),
            self.config.apply,
        )
        if tier_b:
            logger.warning('data gap fill: %d report-only gap(s) need an audited repair: %s', len(tier_b), tier_b[:5])
        return result

    def _cool_down(self, key: tuple[str, str], now: datetime, *, seconds: int | None = None) -> None:
        self._cooldowns[key] = now + timedelta(
            seconds=self.config.failure_cooldown_seconds if seconds is None else seconds
        )


def _small(outcome: Mapping[str, Any]) -> dict[str, Any]:
    keep = ('status', 'reason', 'updated_count', 'persisted_row_count', 'built_row_count', 'unverified_symbol_count')
    return {key: outcome[key] for key in keep if key in outcome}


# ---------------------------------------------------------------------- wiring
def _iso(value: Any) -> str:
    return value.isoformat() if hasattr(value, 'isoformat') else str(value)


def load_recent_sessions(limit: int) -> list[str]:
    from sqlalchemy import select

    from app.db import session_scope
    from app.db_models import DailyPriceBar

    with session_scope() as session:
        rows = session.scalars(
            select(DailyPriceBar.trade_date).distinct().order_by(DailyPriceBar.trade_date.desc()).limit(limit)
        ).all()
    return [_iso(row) for row in reversed(rows)]


def detect_signal_features_missing(market_date: str) -> Gap | None:
    from sqlalchemy import func, select

    from app.db import session_scope
    from app.db_models import DailySignalFeature
    from app.market_data_store import list_symbols_with_daily_bar_on_date

    bar_symbols = len(set(list_symbols_with_daily_bar_on_date(market_date)))
    with session_scope() as session:
        stored = int(
            session.scalar(
                select(func.count()).select_from(DailySignalFeature).where(
                    DailySignalFeature.market_date == date.fromisoformat(market_date)
                )
            )
            or 0
        )
    missing = bar_symbols - stored
    if bar_symbols and missing > 0:
        return Gap('signal_features_missing', market_date, missing, {'bar_symbols': bar_symbols, 'stored_rows': stored})
    return None


def detect_participation_context_missing(market_date: str) -> Gap | None:
    from sqlalchemy import func, select

    from app.db import session_scope
    from app.db_models import DailyMarketParticipationContext, DailySignalFeature

    target = date.fromisoformat(market_date)
    with session_scope() as session:
        rows = int(
            session.scalar(
                select(func.count()).select_from(DailySignalFeature).where(DailySignalFeature.market_date == target)
            )
            or 0
        )
        context = session.scalars(
            select(DailyMarketParticipationContext.context_payload).where(
                DailyMarketParticipationContext.market_date == target
            )
        ).first()
    if rows == 0:
        return None  # nothing to build from; the signal-feature gap comes first
    captured = (context or {}).get('captured_symbol_count') if isinstance(context, dict) else None
    if context is None or captured != rows:
        return Gap(
            'participation_context_missing',
            market_date,
            1,
            {'signal_feature_rows': rows, 'captured_symbol_count': captured},
        )
    return None


def detect_turnover_incomplete(market_date: str) -> Gap | None:
    from sqlalchemy import func, select

    from app.db import session_scope
    from app.db_models import DailyPriceBar, Symbol

    with session_scope() as session:
        count = int(
            session.scalar(
                select(func.count())
                .select_from(DailyPriceBar)
                .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                .where(
                    DailyPriceBar.trade_date == date.fromisoformat(market_date),
                    DailyPriceBar.volume > 0,
                    DailyPriceBar.turnover == 0,
                    Symbol.is_active.is_(True),
                    Symbol.market.in_(('KOSPI', 'KOSDAQ')),
                )
            )
            or 0
        )
    return Gap('turnover_incomplete', market_date, count) if count else None


# Official KRX frames for finalized sessions never change, but every scheduler tick
# builds a new service. Keep fetched frames for the life of the process so each
# session is read from KRX once, not every 30 minutes (KRX blocks repeated bulk reads).
_PROVIDER_FRAME_CACHE: dict[str, dict[str, tuple[int, int, int, int]]] = {}
_PROVIDER_FRAME_CACHE_MAX = 8


def _load_provider_frame(market_date: str) -> dict[str, tuple[int, int, int, int]]:
    cached = _PROVIDER_FRAME_CACHE.get(market_date)
    if cached is not None:
        return cached
    from app.eod.pykrx_backfill import _get_market_ohlcv_by_ticker

    compact = market_date.replace('-', '')
    provider: dict[str, tuple[int, int, int, int]] = {}
    for market in ('KOSPI', 'KOSDAQ'):
        frame = _get_market_ohlcv_by_ticker(compact, market)
        for code, row in frame.iterrows():
            provider[str(code)] = tuple(int(row[key]) for key in ('시가', '고가', '저가', '종가'))
    if provider:  # never cache an empty (failed or blocked) read
        while len(_PROVIDER_FRAME_CACHE) >= _PROVIDER_FRAME_CACHE_MAX:
            _PROVIDER_FRAME_CACHE.pop(next(iter(_PROVIDER_FRAME_CACHE)))
        _PROVIDER_FRAME_CACHE[market_date] = provider
    return provider


class _ProviderComparison:
    """One official-frame fetch per session (per process) shared by the two Tier B detectors."""

    def __init__(
        self,
        recent_sessions: Callable[[], Sequence[str]],
        frame_loader: Callable[[str], dict[str, tuple[int, int, int, int]]] = _load_provider_frame,
    ) -> None:
        self._recent_sessions = recent_sessions
        self._frame_loader = frame_loader
        self._cache: dict[str, dict[str, int]] = {}

    def compare(self, market_date: str) -> dict[str, int] | None:
        if market_date not in set(self._recent_sessions()):
            return None
        if market_date in self._cache:
            return self._cache[market_date]
        from sqlalchemy import select

        from app.db import session_scope
        from app.db_models import DailyPriceBar, Symbol

        provider = self._frame_loader(market_date)
        with session_scope() as session:
            stored = {
                symbol: (o, h, l, c)
                for symbol, o, h, l, c in session.execute(
                    select(
                        Symbol.symbol,
                        DailyPriceBar.open_price,
                        DailyPriceBar.high_price,
                        DailyPriceBar.low_price,
                        DailyPriceBar.close_price,
                    )
                    .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                    .where(
                        DailyPriceBar.trade_date == date.fromisoformat(market_date),
                        Symbol.market.in_(('KOSPI', 'KOSDAQ')),
                    )
                ).all()
            }
        result = {
            'provider_symbols': len(provider),
            'missing_bars': sum(1 for symbol in provider if symbol not in stored),
            'provisional_bars': sum(1 for symbol, ohlc in provider.items() if symbol in stored and stored[symbol] != ohlc),
        }
        self._cache[market_date] = result
        return result


def build_default_service(config: GapFillConfig | None = None) -> DataGapFillService:
    from app.core.settings import settings
    from app.eod.exact_signal_feature_repair import exact_signal_feature_guard
    from app.eod.finality import validate_finalized_krx_write_date
    from app.eod.stored_signal_feature_backfill import apply_backfill
    from app.eod.turnover_repair import daily_turnover_repair_service

    resolved = config or GapFillConfig(
        enabled=settings.data_gap_fill_enabled,
        apply=settings.data_gap_fill_apply,
        lookback_trading_days=settings.data_gap_fill_lookback_trading_days,
        window_start=_parse_hhmm(settings.data_gap_fill_window_start),
        window_end=_parse_hhmm(settings.data_gap_fill_window_end),
        min_interval_seconds=settings.data_gap_fill_min_interval_seconds,
        budget_seconds=settings.data_gap_fill_budget_seconds,
        failure_cooldown_seconds=settings.data_gap_fill_failure_cooldown_seconds,
        provider_check_sessions=settings.data_gap_fill_provider_check_sessions,
    )

    def provider_sessions() -> Sequence[str]:
        # The most recent finalized sessions only: the official frame is fetched
        # per session, so keep the number of provider calls small.
        today = datetime.now(KST).date().isoformat()
        recent = [d for d in load_recent_sessions(resolved.provider_check_sessions + 1) if d < today]
        return recent[-resolved.provider_check_sessions :] if resolved.provider_check_sessions else []

    comparison = _ProviderComparison(provider_sessions)

    def provider_gap(kind: str) -> Detector:
        def detect(market_date: str) -> Gap | None:
            result = comparison.compare(market_date)
            if result and result[kind]:
                return Gap(kind, market_date, result[kind], {'provider_symbols': result['provider_symbols']})
            return None

        return detect

    def fix_participation(market_date: str) -> Mapping[str, Any]:
        from app.agent_context.derived_contexts import daily_market_participation_context_service
        from app.market_data_store import load_daily_signal_feature_context_rows

        with exact_signal_feature_guard(market_date=date.fromisoformat(market_date)) as guard:
            acquired, reason = guard
            if not acquired:
                return {'status': 'blocked', 'reason': str(reason or 'active_writer')}
            rows = load_daily_signal_feature_context_rows(market_date=market_date)
            if not rows:
                return {'status': 'completed', 'persisted_row_count': 0}
            built = daily_market_participation_context_service.rebuild_from_rows(
                market_date=market_date, rows=rows, market='KRX', source='kis_ranking'
            )
            return {'status': 'completed', 'persisted_row_count': int(built.get('captured_symbol_count') or 0)}

    def fix_turnover(market_date: str) -> Mapping[str, Any]:
        return daily_turnover_repair_service.repair_for_market_date(
            market_date=market_date, apply=True, allow_partial_verified=True
        )

    return DataGapFillService(
        config=resolved,
        sessions_loader=load_recent_sessions,
        detectors={
            'signal_features_missing': detect_signal_features_missing,
            'participation_context_missing': detect_participation_context_missing,
            'turnover_incomplete': detect_turnover_incomplete,
            'provisional_bars': provider_gap('provisional_bars'),
            'missing_bars': provider_gap('missing_bars'),
        },
        fixers={
            'signal_features_missing': apply_backfill,
            'participation_context_missing': fix_participation,
            'turnover_incomplete': fix_turnover,
        },
        finality_validator=validate_finalized_krx_write_date,
    )


def _parse_hhmm(value: str) -> time:
    hour, minute = value.split(':')
    return time(int(hour), int(minute))


_service: DataGapFillService | None = None


def get_data_gap_fill_service() -> DataGapFillService:
    global _service
    if _service is None:
        _service = build_default_service()
    return _service


def run_if_due() -> dict[str, Any]:
    """Entry point for the background runner (blocking step)."""

    return get_data_gap_fill_service().run_if_due()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Detect (and with --apply, fill) recent data gaps.')
    parser.add_argument('--apply', action='store_true', help='run the Tier A fixers; default is report only')
    parser.add_argument('--lookback', type=int, default=None, help='trading sessions to examine')
    args = parser.parse_args(argv)
    base = build_default_service().config
    config = GapFillConfig(
        **{
            **base.__dict__,
            'enabled': True,
            'apply': bool(args.apply),
            **({'lookback_trading_days': args.lookback} if args.lookback else {}),
        }
    )
    result = build_default_service(config).run_if_due(force_schedule=True)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 2 if result.get('failed') else 0


if __name__ == '__main__':
    raise SystemExit(main())
