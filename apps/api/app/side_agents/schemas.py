from __future__ import annotations

import json
import re
from datetime import date
from typing import Any, Mapping, Sequence

MARKET_AGENT = 'market-context-analyst'
NEWS_AGENT = 'news-event-analyst'
POSTMORTEM_AGENT = 'postmortem-analyst'
SIDE_AGENT_NAMES: tuple[str, ...] = (MARKET_AGENT, NEWS_AGENT, POSTMORTEM_AGENT)

MARKET_SCHEMA = 'market_context_analyst_v1'
NEWS_SCHEMA = 'news_event_analyst_v1'
POSTMORTEM_SCHEMA = 'postmortem_analyst_v1'

_REGIMES = {'risk_on', 'neutral', 'risk_off', 'unclear'}
_TRENDS = {'up', 'down', 'range', 'unclear'}
_VOLATILITY = {'low', 'normal', 'elevated', 'high', 'unknown'}
_POSTURES = {'normal', 'selective', 'defensive', 'stand_aside'}
_DRIVER_DIR = {'supportive', 'adverse', 'neutral'}
_IMPORTANCE = {'critical', 'important', 'reference'}
_ROTATION_DIR = {'in', 'out'}

_MATERIALITY = {'material', 'noise', 'unknown'}
_NEWS_DIR = {'positive', 'negative', 'mixed', 'neutral', 'unknown'}
_EVIDENCE = {'confirmed', 'unconfirmed', 'headline_only', 'rumor'}
_REACTION = {'consistent', 'amplified', 'dampened', 'inverse', 'not_assessable'}
_HORIZON = {'intraday', 'days', 'weeks', 'structural', 'unknown'}
_SOURCE_TIER = {'official', 'major_media', 'broker', 'aggregator', 'unknown'}
_EVENT_TYPES = {
    'earnings', 'guidance', 'contract', 'investment', 'mna', 'financing_dilution',
    'regulatory_legal', 'governance', 'product_approval', 'supply_chain',
    'sector_macro', 'analyst_view', 'other',
}
_CALIBRATION_VERDICT = {'over', 'under', 'ok', 'insufficient'}
_TARGET_FILES = {'SOUL.md', 'AGENTS.md'}
_CLASSIFICATION_KEYS = (
    'true_positive', 'false_positive', 'missed_opportunity', 'regime_mismatch', 'cost_drag',
)
_FENCE = re.compile(r'^```(?:json)?\s*(.*?)\s*```$', re.DOTALL)
_DATE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_MAX_TEXT = 600


class SideAgentValidationError(ValueError):
    """The agent output does not satisfy its contract; the result must be discarded."""


def parse_json_object(text: str | None) -> dict[str, Any]:
    if not isinstance(text, str) or not text.strip():
        raise SideAgentValidationError('empty agent output')
    candidate = text.strip()
    fenced = _FENCE.match(candidate)
    if fenced:
        candidate = fenced.group(1)
    try:
        value = json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise SideAgentValidationError(f'output is not valid JSON: {exc.msg}') from exc
    if not isinstance(value, dict):
        raise SideAgentValidationError('output must be a JSON object')
    return value


def _enum(value: Any, allowed: set[str], field: str) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise SideAgentValidationError(f'{field} must be one of {sorted(allowed)}')
    return value


def _text(value: Any, field: str, *, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or (required and not value.strip()):
        raise SideAgentValidationError(f'{field} must be a non-empty string')
    return value.strip()[:_MAX_TEXT]


def _confidence(value: Any, field: str = 'confidence') -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SideAgentValidationError(f'{field} must be a number')
    number = float(value)
    if not 0.0 <= number <= 1.0:
        raise SideAgentValidationError(f'{field} must be within 0..1')
    return round(number, 4)


def _count(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SideAgentValidationError(f'{field} must be a non-negative integer')
    return value


def _rate(value: Any, field: str) -> float:
    return _confidence(value, field)


def _list(value: Any, field: str, *, max_items: int) -> list[Any]:
    if not isinstance(value, list):
        raise SideAgentValidationError(f'{field} must be a list')
    if len(value) > max_items:
        raise SideAgentValidationError(f'{field} has more than {max_items} entries')
    return value


def _str_list(value: Any, field: str, *, max_items: int) -> list[str]:
    return [_text(item, f'{field}[]') or '' for item in _list(value, field, max_items=max_items)]


def _date_or_none(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _DATE.match(value):
        raise SideAgentValidationError(f'{field} must be YYYY-MM-DD or null')
    return value


def _require_schema(obj: Mapping[str, Any], expected: str) -> None:
    if obj.get('schema_version') != expected:
        raise SideAgentValidationError(f'schema_version must be {expected}')


def validate_market_context(obj: Mapping[str, Any], *, market_date: date) -> dict[str, Any]:
    _require_schema(obj, MARKET_SCHEMA)
    if obj.get('market_date') != market_date.isoformat():
        raise SideAgentValidationError('market_date does not match the request')
    trend = obj.get('trend')
    if not isinstance(trend, Mapping):
        raise SideAgentValidationError('trend must be an object')
    drivers = [
        {
            'factor': _text(item.get('factor'), 'drivers.factor'),
            'direction': _enum(item.get('direction'), _DRIVER_DIR, 'drivers.direction'),
            'evidence': _text(item.get('evidence'), 'drivers.evidence'),
        }
        for item in _list(obj.get('drivers'), 'drivers', max_items=5)
        if isinstance(item, Mapping)
    ]
    events = [
        {
            'event': _text(item.get('event'), 'event_risks.event'),
            'date': _date_or_none(item.get('date'), 'event_risks.date'),
            'importance': _enum(item.get('importance'), _IMPORTANCE, 'event_risks.importance'),
        }
        for item in _list(obj.get('event_risks'), 'event_risks', max_items=5)
        if isinstance(item, Mapping)
    ]
    rotation = [
        {
            'sector': _text(item.get('sector'), 'rotation.sector'),
            'direction': _enum(item.get('direction'), _ROTATION_DIR, 'rotation.direction'),
        }
        for item in _list(obj.get('rotation'), 'rotation', max_items=6)
        if isinstance(item, Mapping)
    ]
    return {
        'schema_version': MARKET_SCHEMA,
        'market_date': market_date.isoformat(),
        'regime': _enum(obj.get('regime'), _REGIMES, 'regime'),
        'trend': {
            'kospi': _enum(trend.get('kospi'), _TRENDS, 'trend.kospi'),
            'kosdaq': _enum(trend.get('kosdaq'), _TRENDS, 'trend.kosdaq'),
        },
        'volatility': _enum(obj.get('volatility'), _VOLATILITY, 'volatility'),
        'rotation': rotation,
        'drivers': drivers,
        'event_risks': events,
        'posture_hint': _enum(obj.get('posture_hint'), _POSTURES, 'posture_hint'),
        'confidence': _confidence(obj.get('confidence')),
        'data_gaps': _str_list(obj.get('data_gaps', []), 'data_gaps', max_items=6),
        'pre_mortem': _text(obj.get('pre_mortem'), 'pre_mortem'),
        'summary_ko': _text(obj.get('summary_ko'), 'summary_ko'),
    }


def validate_news_events(
    obj: Mapping[str, Any], *, market_date: date, symbols: Sequence[str]
) -> dict[str, Any]:
    _require_schema(obj, NEWS_SCHEMA)
    if obj.get('market_date') != market_date.isoformat():
        raise SideAgentValidationError('market_date does not match the request')
    raw_results = _list(obj.get('results'), 'results', max_items=max(len(symbols), 1))
    expected = list(symbols)
    got = [item.get('symbol') if isinstance(item, Mapping) else None for item in raw_results]
    if got != expected:
        raise SideAgentValidationError('results must contain each assigned symbol exactly once, in order')
    results = []
    for item in raw_results:
        event_types = _str_list(item.get('event_types', []), 'event_types', max_items=6)
        for event_type in event_types:
            _enum(event_type, _EVENT_TYPES, 'event_types[]')
        results.append(
            {
                'symbol': item['symbol'],
                'materiality': _enum(item.get('materiality'), _MATERIALITY, 'materiality'),
                'event_types': event_types,
                'direction': _enum(item.get('direction'), _NEWS_DIR, 'direction'),
                'evidence_grade': _enum(item.get('evidence_grade'), _EVIDENCE, 'evidence_grade'),
                'reaction_check': _enum(item.get('reaction_check'), _REACTION, 'reaction_check'),
                'horizon': _enum(item.get('horizon'), _HORIZON, 'horizon'),
                'top_source_tier': _enum(item.get('top_source_tier'), _SOURCE_TIER, 'top_source_tier'),
                'confidence': _confidence(item.get('confidence')),
                'risk_flags': _str_list(item.get('risk_flags', []), 'risk_flags', max_items=4),
                'summary_ko': _text(item.get('summary_ko'), 'summary_ko'),
            }
        )
    return {'schema_version': NEWS_SCHEMA, 'market_date': market_date.isoformat(), 'results': results}


def validate_postmortem(
    obj: Mapping[str, Any],
    *,
    known_agents: Sequence[str],
    measured_sufficient: Mapping[str, bool] | None = None,
) -> dict[str, Any]:
    """``measured_sufficient`` is Core's own per-agent sample check.

    The model's ``sample.sufficient`` is only a claim; a proposal for an
    agent whose measured sample is insufficient is rejected regardless.
    """

    _require_schema(obj, POSTMORTEM_SCHEMA)
    period = obj.get('period')
    sample = obj.get('sample')
    classification = obj.get('classification')
    if not isinstance(period, Mapping) or not isinstance(sample, Mapping) or not isinstance(classification, Mapping):
        raise SideAgentValidationError('period, sample and classification must be objects')
    by_agent = sample.get('by_agent')
    if not isinstance(by_agent, Mapping):
        raise SideAgentValidationError('sample.by_agent must be an object')
    sufficient = sample.get('sufficient')
    if not isinstance(sufficient, bool):
        raise SideAgentValidationError('sample.sufficient must be a boolean')
    proposals = []
    for item in _list(obj.get('proposals'), 'proposals', max_items=5):
        if not isinstance(item, Mapping):
            raise SideAgentValidationError('proposals[] must be objects')
        target_agent = item.get('target_agent')
        if target_agent not in known_agents:
            raise SideAgentValidationError('proposals.target_agent is not a known agent')
        proposals.append(
            {
                'target_agent': target_agent,
                'target_file': _enum(item.get('target_file'), _TARGET_FILES, 'proposals.target_file'),
                'change': _text(item.get('change'), 'proposals.change'),
                'evidence': _text(item.get('evidence'), 'proposals.evidence'),
                'risk': _text(item.get('risk'), 'proposals.risk'),
                'confidence': min(_confidence(item.get('confidence')), 0.70),
            }
        )
    if proposals and not sufficient:
        raise SideAgentValidationError('proposals require a sufficient sample')
    if measured_sufficient is not None:
        for proposal in proposals:
            if measured_sufficient.get(proposal['target_agent']) is not True:
                raise SideAgentValidationError(
                    f"proposal for {proposal['target_agent']} lacks a sufficient measured sample"
                )
    calibration = [
        {
            'bucket': _text(item.get('bucket'), 'calibration.bucket'),
            'count': _count(item.get('count'), 'calibration.count'),
            'hit_rate': _rate(item.get('hit_rate'), 'calibration.hit_rate'),
            'verdict': _enum(item.get('verdict'), _CALIBRATION_VERDICT, 'calibration.verdict'),
        }
        for item in _list(obj.get('calibration', []), 'calibration', max_items=10)
        if isinstance(item, Mapping)
    ]
    patterns = [
        {
            'description': _text(item.get('description'), 'patterns.description'),
            'count': _count(item.get('count'), 'patterns.count'),
            'rate': _rate(item.get('rate'), 'patterns.rate'),
            'base_rate': _rate(item.get('base_rate'), 'patterns.base_rate'),
        }
        for item in _list(obj.get('patterns', []), 'patterns', max_items=5)
        if isinstance(item, Mapping)
    ]
    return {
        'schema_version': POSTMORTEM_SCHEMA,
        'period': {
            'from': _date_or_none(period.get('from'), 'period.from'),
            'to': _date_or_none(period.get('to'), 'period.to'),
        },
        'sample': {
            'total': _count(sample.get('total'), 'sample.total'),
            'by_agent': {str(k): _count(v, 'sample.by_agent') for k, v in by_agent.items()},
            'sufficient': sufficient,
        },
        'classification': {key: _count(classification.get(key, 0), f'classification.{key}') for key in _CLASSIFICATION_KEYS},
        'calibration': calibration,
        'patterns': patterns,
        'proposals': proposals,
        'pre_mortem': _text(obj.get('pre_mortem'), 'pre_mortem'),
        'summary_ko': _text(obj.get('summary_ko'), 'summary_ko'),
    }
