"""Exploratory US-rebound observations; never grants order authority."""

from dataclasses import asdict, dataclass
from datetime import date
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
import re

from app.internal.exact_repair_manifest import canonical_json_sha256


NUMERIC_FIELDS = (
    'return_pct', 'acceleration_pp', 'reference_mean_pct',
    'reference_sample_std_pct', 'shock_z', 'percentile_pct',
)
CODES = ('SPX', 'COMP', 'SOX')
SPREADS = ('COMP-SPX', 'SOX-SPX', 'SOX-COMP')
VALIDATION_STATUS = 'exploratory_unvalidated'
STRATEGY_ID = 'krx_us_rebound'
RULE_VERSION = 'krx_us_rebound_exploratory_v1'


def _text(value: Decimal) -> str:
    value_text = format(value, 'f')
    if not value:
        return '0'
    return value_text.rstrip('0').rstrip('.') if '.' in value_text else value_text


@dataclass(frozen=True)
class USReboundRuleConfig:
    min_return_pct: Decimal = Decimal('0')
    min_acceleration_pp: Decimal = Decimal('0')
    min_shock_z: Decimal = Decimal('2')
    min_percentile_pct: Decimal = Decimal('95')
    min_outperformance_pp: Decimal = Decimal('0')

    def __post_init__(self):
        if any(
            not isinstance(value, Decimal) or not value.is_finite() or value < 0
            for value in asdict(self).values()
        ):
            raise ValueError('US rebound thresholds must be finite nonnegative Decimals')
        if self.min_shock_z <= 0 or not 0 < self.min_percentile_pct <= 100:
            raise ValueError('US rebound shock and percentile thresholds must be positive and percentile at most 100')

    def as_dict(self) -> dict:
        return {
            **{name: _text(value) for name, value in asdict(self).items()},
            'validation_status': VALIDATION_STATUS,
        }

    @property
    def config_hash(self) -> str:
        return canonical_json_sha256(self.as_dict())


def _number(value) -> Decimal:
    if not isinstance(value, str) or not re.fullmatch(r'-?[0-9]+(?:\.[0-9]+)?', value):
        raise ValueError('US rebound features require finite decimal strings')
    number = Decimal(value)
    if not number.is_finite():
        raise ValueError('US rebound features require finite decimal strings')
    return number


def _validated(features: dict) -> dict:
    try:
        expected = {
            'schema_version': 'us_index_features_v2', 'status': 'computed',
            'history_source': 'current_snapshot_history', 'history_inputs': [],
            'reference_window': 20, 'history_count': 20, 'history_unavailable_reason': None,
        }
        if not isinstance(features, dict) or any(features[name] != value for name, value in expected.items()):
            raise ValueError('US rebound requires full-history version-2 features')
        if any(type(features[name]) is not int for name in ('reference_window', 'history_count')):
            raise ValueError('Reference window and history count must be integers')
        if any(
            not isinstance(features[name], str) or not re.fullmatch(r'[0-9a-f]{64}', features[name])
            for name in ('feature_id', 'input_manifest_id')
        ):
            raise ValueError('Invalid US rebound replay identity')
        feature_body = {key: value for key, value in features.items() if key != 'feature_id'}
        if features['feature_id'] != canonical_json_sha256(feature_body):
            raise ValueError('US rebound feature content hash mismatch')
        if features['source_environment'] not in ('real', 'mock'):
            raise ValueError('Unsupported US rebound input environment')
        date_fields = ('previous_us_session_date', 'us_session_date', 'target_trade_date')
        days = [date.fromisoformat(features[name]) for name in date_fields]
        if any(day.isoformat() != features[name] for name, day in zip(date_fields, days)):
            raise ValueError('US rebound session dates must use ISO format')
        if not days[0] < days[1] < days[2] or set(features['indices']) != set(CODES):
            raise ValueError('Invalid US rebound session or index bindings')
        indices = {
            code: {
                name: None if item[name] is None and name != 'return_pct' else _number(item[name])
                for name in NUMERIC_FIELDS
            }
            for code, item in features['indices'].items()
        }
        for code, item in indices.items():
            if any(item[name] is None for name in NUMERIC_FIELDS if name != 'shock_z'):
                raise ValueError('Incomplete full-history feature schema')
            if item['reference_sample_std_pct'] < 0 or not 0 <= item['percentile_pct'] <= 100:
                raise ValueError('Invalid reference normalization')
            expected_reason = 'zero_reference_volatility' if item['reference_sample_std_pct'] == 0 else None
            if (
                features['indices'][code]['shock_unavailable_reason'] != expected_reason
                or (item['shock_z'] is None) != (expected_reason is not None)
            ):
                raise ValueError('Inconsistent normalization availability')
        if set(features['return_spreads_pp']) != set(SPREADS):
            raise ValueError('Incomplete US-index return spreads')
        with localcontext(Context(prec=34, rounding=ROUND_HALF_EVEN)):
            for name, value in features['return_spreads_pp'].items():
                left, right = name.split('-')
                if _number(value) != indices[left]['return_pct'] - indices[right]['return_pct']:
                    raise ValueError('Return spread does not match synchronized event returns')
        return indices
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError('Malformed US rebound feature schema') from exc


def evaluate_us_rebound(
    features: dict, config: USReboundRuleConfig = USReboundRuleConfig(),
) -> dict:
    if not isinstance(config, USReboundRuleConfig):
        raise ValueError('US rebound requires validated rule configuration')
    indices = _validated(features)
    unavailable = [
        f'{code}:{features["indices"][code]["shock_unavailable_reason"]}' for code in CODES
        if features['indices'][code]['shock_unavailable_reason'] is not None
    ]
    conditions = {} if unavailable else {
        'positive_spx_comp_returns': all(
            indices[code]['return_pct'] > config.min_return_pct for code in ('SPX', 'COMP')
        ),
        'positive_tech_acceleration': any(
            indices[code]['acceleration_pp'] > config.min_acceleration_pp for code in ('COMP', 'SOX')
        ),
        'matching_shock_and_percentile': any(
            item['shock_z'] >= config.min_shock_z and item['percentile_pct'] >= config.min_percentile_pct
            for item in indices.values()
        ),
        'tech_outperformance': any(
            _number(features['return_spreads_pp'][f'{code}-SPX']) > config.min_outperformance_pp for code in ('COMP', 'SOX')
        ),
    }
    status = 'UNAVAILABLE' if unavailable else 'WATCH' if all(conditions.values()) else 'NO_SIGNAL'
    return {
        'strategy_id': STRATEGY_ID, 'rule_version': RULE_VERSION,
        'validation_status': VALIDATION_STATUS, 'order_authority': False, 'status': status,
        'config': config.as_dict(), 'config_hash': config.config_hash,
        **{name: features[name] for name in (
            'feature_id', 'input_manifest_id', 'target_trade_date', 'source_environment',
            'us_session_date', 'previous_us_session_date',
        )},
        'reasons': (
            unavailable or [name for name, passed in conditions.items() if not passed]
            or ['exploratory_rule_conditions_met']
        ),
        'evidence': {
            'indices': {code: {name: features['indices'][code][name] for name in (
                'return_pct', 'acceleration_pp', 'shock_z', 'percentile_pct', 'shock_unavailable_reason',
            )} for code in CODES},
            'return_spreads_pp': dict(features['return_spreads_pp']), 'conditions': conditions,
        },
    }
