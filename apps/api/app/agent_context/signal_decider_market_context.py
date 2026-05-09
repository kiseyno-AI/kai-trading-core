from __future__ import annotations

import asyncio
from datetime import date, datetime
import logging
import threading
from typing import Any

from app.market.clock import get_market_state
from app.market.session import build_market_session_metadata
from app.market.snapshot import market_snapshot_service, normalize_market_snapshot_payload

logger = logging.getLogger(__name__)

EXPECTED_INDEX_KEYS: tuple[str, ...] = ('kospi', 'kosdaq')
EXPECTED_MISSING_FIELDS: tuple[str, ...] = (
    'summary.breadth_summary',
    'indices.kospi',
    'indices.kosdaq',
    'fx.usdkrw',
    'flows.foreign',
    'flows.institutional',
)


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return None
    return None


def _normalize_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.endswith('Z'):
            normalized = normalized[:-1] + '+00:00'
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def _append_missing_field(missing_fields: list[str], field_name: str) -> None:
    if field_name not in missing_fields:
        missing_fields.append(field_name)


def _format_signed_rate(value: float | None) -> str | None:
    if value is None:
        return None
    return f'{value:+.2f}%'


def _format_signed_int(value: int | None) -> str | None:
    if value is None:
        return None
    return f'{value:+d}'


def _run_coroutine(coro) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result['value'] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - defensive bridge for non-threadpool callers.
            error['value'] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if 'value' in error:
        raise error['value']
    return result.get('value')


class SignalDeciderMarketContextBuilder:
    def __init__(self, *, snapshot_service=None, market_state_loader=None) -> None:
        self.snapshot_service = snapshot_service or market_snapshot_service
        self.market_state_loader = market_state_loader or get_market_state

    def build(self, *, market_date: date) -> dict[str, Any]:
        market_session = build_market_session_metadata(
            requested_date=market_date,
            current_state=self.market_state_loader(),
        )
        try:
            snapshot = _run_coroutine(self.snapshot_service.get_market_snapshot())
        except Exception as exc:  # Market context should degrade the payload, not block signal-decider dispatch.
            logger.warning('Signal-decider market context unavailable: %s', exc)
            return self._unavailable_payload(market_date=market_date, market_session=market_session)
        return self._build_payload(snapshot=snapshot, market_date=market_date, market_session=market_session)

    def _build_payload(
        self,
        *,
        snapshot: dict[str, Any],
        market_date: date,
        market_session: dict[str, Any],
    ) -> dict[str, Any]:
        snapshot = normalize_market_snapshot_payload(snapshot)
        market_date_text = market_date.isoformat()
        missing_fields: list[str] = []
        snapshot_markets = snapshot.get('markets') if isinstance(snapshot.get('markets'), dict) else {}
        as_of = _normalize_datetime(snapshot.get('as_of'))
        as_of_text = as_of.isoformat() if as_of is not None else None

        indices = {
            key: self._build_index_context(
                key=key,
                payload=snapshot_markets.get(key),
                missing_fields=missing_fields,
            )
            for key in EXPECTED_INDEX_KEYS
        }
        indices = {key: value for key, value in indices.items() if value is not None}

        fx = self._build_fx_context(payload=snapshot.get('usd_krw'), missing_fields=missing_fields)
        flows = self._build_flow_context(snapshot_markets=snapshot_markets, missing_fields=missing_fields)
        summary = self._build_summary(
            market_date_text=market_date_text,
            snapshot_markets=snapshot_markets,
            indices=indices,
            fx=fx,
            flows=flows,
            missing_fields=missing_fields,
        )

        stale_reasons: list[str] = []
        if as_of is None:
            _append_missing_field(missing_fields, 'as_of')
        elif as_of.date().isoformat() != market_date_text:
            stale_reasons.append(f'as_of_date={as_of.date().isoformat()}')
        usd_krw = fx.get('usdkrw') if isinstance(fx.get('usdkrw'), dict) else None
        if isinstance(usd_krw, dict):
            fx_stale_reason = str(usd_krw.get('stale_reason') or '').strip()
            if fx_stale_reason:
                stale_reasons.append(fx_stale_reason)
            elif usd_krw.get('fallback_applied'):
                stale_reasons.append('usdkrw_fallback_applied')

        status = 'ready'
        if not indices and not usd_krw:
            status = 'unavailable'
        elif stale_reasons:
            status = 'stale'
        elif missing_fields:
            status = 'partial'

        return {
            'status': status,
            'market_date': market_date_text,
            'market_session': market_session,
            'as_of': as_of_text,
            'summary': summary,
            'indices': indices,
            'fx': fx,
            'flows': flows,
            'missing_fields': missing_fields,
            'stale_reason': '; '.join(stale_reasons) if stale_reasons else None,
        }

    def _build_index_context(
        self,
        *,
        key: str,
        payload: Any,
        missing_fields: list[str],
    ) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            _append_missing_field(missing_fields, f'indices.{key}')
            return None

        level = _to_float(payload.get('value'))
        change = _to_float(payload.get('change'))
        change_rate = _to_float(payload.get('change_percent'))
        if level is None and change is None and change_rate is None:
            _append_missing_field(missing_fields, f'indices.{key}')
            return None
        if level is None:
            _append_missing_field(missing_fields, f'indices.{key}.level')
        if change is None:
            _append_missing_field(missing_fields, f'indices.{key}.change')
        if change_rate is None:
            _append_missing_field(missing_fields, f'indices.{key}.change_rate')

        return {
            'level': level,
            'change': change,
            'change_rate': change_rate,
        }

    def _build_fx_context(self, *, payload: Any, missing_fields: list[str]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            _append_missing_field(missing_fields, 'fx.usdkrw')
            return {}

        value = _to_float(payload.get('rate'))
        effective_date = payload.get('effective_date')
        if value is None:
            _append_missing_field(missing_fields, 'fx.usdkrw')
            return {}
        if effective_date is None:
            _append_missing_field(missing_fields, 'fx.usdkrw.effective_date')
        captured_at = _normalize_datetime(payload.get('captured_at'))

        return {
            'usdkrw': {
                'value': value,
                'effective_date': str(effective_date) if effective_date is not None else None,
                'fallback_applied': bool(payload.get('fallback_applied')),
                'status': str(payload.get('status') or 'ready'),
                'source': str(payload.get('source') or 'live'),
                'stale_reason': payload.get('stale_reason'),
                'detail': payload.get('detail'),
                'captured_at': captured_at.isoformat() if captured_at is not None else None,
            }
        }

    def _build_flow_context(
        self,
        *,
        snapshot_markets: dict[str, Any],
        missing_fields: list[str],
    ) -> dict[str, Any]:
        flows: dict[str, Any] = {}
        foreign_values: list[int] = []
        institutional_values: list[int] = []
        missing_foreign = False
        missing_institutional = False

        for key in EXPECTED_INDEX_KEYS:
            payload = snapshot_markets.get(key)
            if not isinstance(payload, dict):
                missing_foreign = True
                missing_institutional = True
                continue
            foreign_value = _to_int(payload.get('foreign_net_value'))
            institutional_value = _to_int(payload.get('institution_net_value'))
            if foreign_value is None:
                missing_foreign = True
            else:
                foreign_values.append(foreign_value)
            if institutional_value is None:
                missing_institutional = True
            else:
                institutional_values.append(institutional_value)

        if len(foreign_values) == len(EXPECTED_INDEX_KEYS) and not missing_foreign:
            flows['foreign'] = {'value': sum(foreign_values), 'unit': 'krw'}
        else:
            _append_missing_field(missing_fields, 'flows.foreign')
        if len(institutional_values) == len(EXPECTED_INDEX_KEYS) and not missing_institutional:
            flows['institutional'] = {'value': sum(institutional_values), 'unit': 'krw'}
        else:
            _append_missing_field(missing_fields, 'flows.institutional')
        return flows

    def _build_summary(
        self,
        *,
        market_date_text: str,
        snapshot_markets: dict[str, Any],
        indices: dict[str, dict[str, Any]],
        fx: dict[str, Any],
        flows: dict[str, Any],
        missing_fields: list[str],
    ) -> dict[str, Any]:
        market_bias = self._build_market_bias(snapshot_markets=snapshot_markets, indices=indices, flows=flows)
        breadth_summary = self._build_breadth_summary(snapshot_markets=snapshot_markets)
        if breadth_summary is None:
            _append_missing_field(missing_fields, 'summary.breadth_summary')
        line = self._build_summary_line(indices=indices, fx=fx, flows=flows)
        if line is None:
            line = f'Market snapshot unavailable for {market_date_text}.'
        return {
            'market_bias': market_bias,
            'line': line,
            'breadth_summary': breadth_summary,
        }

    def _build_market_bias(
        self,
        *,
        snapshot_markets: dict[str, Any],
        indices: dict[str, dict[str, Any]],
        flows: dict[str, Any],
    ) -> str:
        directions: list[str] = []
        breadth_positive = 0
        breadth_negative = 0

        for key in EXPECTED_INDEX_KEYS:
            index = indices.get(key) or {}
            change_rate = _to_float(index.get('change_rate'))
            if change_rate is None:
                continue
            if change_rate > 0.15:
                directions.append('up')
            elif change_rate < -0.15:
                directions.append('down')
            else:
                directions.append('flat')

            snapshot_market = snapshot_markets.get(key)
            if not isinstance(snapshot_market, dict):
                continue
            advancers = _to_int(snapshot_market.get('advancers'))
            decliners = _to_int(snapshot_market.get('decliners'))
            if advancers is None or decliners is None:
                continue
            if advancers > decliners:
                breadth_positive += 1
            elif decliners > advancers:
                breadth_negative += 1

        if not directions and breadth_positive == 0 and breadth_negative == 0:
            return 'unknown'
        if all(direction == 'flat' for direction in directions) and breadth_positive == breadth_negative:
            return 'neutral'
        if all(direction in {'up', 'flat'} for direction in directions) and 'up' in directions and breadth_negative == 0:
            return 'risk_on'
        if all(direction in {'down', 'flat'} for direction in directions) and 'down' in directions and breadth_positive == 0:
            return 'risk_off'

        foreign_value = _to_int((flows.get('foreign') or {}).get('value'))
        institutional_value = _to_int((flows.get('institutional') or {}).get('value'))
        aggregate_flow = None
        if foreign_value is not None and institutional_value is not None:
            aggregate_flow = foreign_value + institutional_value
        if aggregate_flow is not None:
            if aggregate_flow > 0 and breadth_positive > breadth_negative:
                return 'risk_on'
            if aggregate_flow < 0 and breadth_negative > breadth_positive:
                return 'risk_off'
        return 'mixed'

    def _build_breadth_summary(self, *, snapshot_markets: dict[str, Any]) -> str | None:
        parts: list[str] = []
        for key in EXPECTED_INDEX_KEYS:
            payload = snapshot_markets.get(key)
            if not isinstance(payload, dict):
                continue
            advancers = _to_int(payload.get('advancers'))
            unchanged = _to_int(payload.get('unchanged'))
            decliners = _to_int(payload.get('decliners'))
            label = str(payload.get('label') or key.upper())
            if advancers is None or unchanged is None or decliners is None:
                continue
            parts.append(f'{label} breadth {advancers}/{unchanged}/{decliners}')
        if not parts:
            return None
        return '; '.join(parts)

    def _build_summary_line(
        self,
        *,
        indices: dict[str, dict[str, Any]],
        fx: dict[str, Any],
        flows: dict[str, Any],
    ) -> str | None:
        parts: list[str] = []

        for key in EXPECTED_INDEX_KEYS:
            index = indices.get(key)
            if not isinstance(index, dict):
                continue
            change_rate = _format_signed_rate(_to_float(index.get('change_rate')))
            if change_rate is None:
                continue
            parts.append(f'{key.upper()} {change_rate}')

        usd_krw = fx.get('usdkrw') if isinstance(fx.get('usdkrw'), dict) else None
        if isinstance(usd_krw, dict):
            value = _to_float(usd_krw.get('value'))
            if value is not None:
                fx_part = f'USD/KRW {value:.1f}'
                if usd_krw.get('fallback_applied'):
                    fx_part += ' (fallback)'
                parts.append(fx_part)

        foreign_value = _format_signed_int(_to_int((flows.get('foreign') or {}).get('value')))
        institutional_value = _format_signed_int(_to_int((flows.get('institutional') or {}).get('value')))
        if foreign_value is not None and institutional_value is not None:
            parts.append(f'flow F {foreign_value} / I {institutional_value}')

        if not parts:
            return None
        return '; '.join(parts)

    def _unavailable_payload(self, *, market_date: date, market_session: dict[str, Any]) -> dict[str, Any]:
        return {
            'status': 'unavailable',
            'market_date': market_date.isoformat(),
            'market_session': market_session,
            'as_of': None,
            'summary': {
                'market_bias': 'unknown',
                'line': f'Market snapshot unavailable for {market_date.isoformat()}.',
                'breadth_summary': None,
            },
            'indices': {},
            'fx': {},
            'flows': {},
            'missing_fields': list(EXPECTED_MISSING_FIELDS),
            'stale_reason': None,
        }


signal_decider_market_context_builder = SignalDeciderMarketContextBuilder()
