from dataclasses import FrozenInstanceError, replace
from copy import deepcopy
from decimal import Decimal

import pytest

from app.internal.exact_repair_manifest import canonical_json_sha256
from app.overnight_rebound.rules import USReboundRuleConfig, evaluate_us_rebound


def features():
    body = {
        'status': 'computed', 'schema_version': 'us_index_features_v2',
        'target_trade_date': '2026-11-26', 'source_environment': 'real',
        'us_session_date': '2026-11-25', 'previous_us_session_date': '2026-11-24',
        'input_manifest_id': 'a' * 64, 'history_inputs': [],
        'reference_window': 20, 'history_count': 20,
        'history_source': 'current_snapshot_history', 'history_unavailable_reason': None,
        'indices': {
            code: {
                'return_pct': ret, 'acceleration_pp': acc,
                'reference_mean_pct': '0', 'reference_sample_std_pct': '1',
                'shock_z': ret, 'shock_unavailable_reason': None, 'percentile_pct': pct,
            }
            for code, ret, acc, pct in (
                ('SPX', '1', '0', '90'), ('COMP', '2', '0.5', '95'), ('SOX', '3', '1', '100'),
            )
        },
        'return_spreads_pp': {'COMP-SPX': '1', 'SOX-SPX': '2', 'SOX-COMP': '1'},
    }
    return seal(body)


def seal(body):
    body.pop('feature_id', None)
    return {**body, 'feature_id': canonical_json_sha256(body)}


def test_strong_positive_leadership_is_an_observational_watch():
    assert evaluate_us_rebound(features())['status'] == 'WATCH'


def test_a_broad_market_shock_can_combine_with_separate_tech_acceleration():
    data = features()
    data['indices']['SPX'].update({
        'reference_sample_std_pct': '0.4', 'shock_z': '2.5', 'percentile_pct': '100',
    })
    data['indices']['COMP'].update({
        'reference_mean_pct': '1.45', 'reference_sample_std_pct': '0.5',
        'shock_z': '1.1', 'percentile_pct': '85',
    })
    data['indices']['SOX'].update({
        'return_pct': '0.5', 'acceleration_pp': '-0.1',
        'reference_mean_pct': '0.05', 'reference_sample_std_pct': '0.5',
        'shock_z': '0.9', 'percentile_pct': '75',
    })
    data['return_spreads_pp'] = {'COMP-SPX': '1', 'SOX-SPX': '-0.5', 'SOX-COMP': '-1.5'}
    result = evaluate_us_rebound(seal(data))
    assert result['status'] == 'WATCH'
    assert result['order_authority'] is False


@pytest.mark.parametrize('updates', [
    {'SPX': {'return_pct': '0'}},
    {'COMP': {'return_pct': '0'}},
    {'COMP': {'acceleration_pp': '0'}, 'SOX': {'acceleration_pp': '-1'}},
    {'COMP': {'shock_z': '1.99'}, 'SOX': {'shock_z': '1.99'}},
    {'COMP': {'percentile_pct': '90'}, 'SOX': {'percentile_pct': '90'}},
    {'SPX': {'shock_z': '2'}, 'COMP': {'shock_z': '1'}, 'SOX': {'shock_z': '1'}},
    {'SPX': {'return_pct': '3'}, 'COMP': {'return_pct': '2'}, 'SOX': {'return_pct': '3'}},
])
def test_each_required_condition_must_pass_and_shock_and_percentile_must_match(updates):
    data = features()
    for code, changes in updates.items():
        data['indices'][code].update(changes)
    data['return_spreads_pp'] = {
        f'{left}-{right}': str(Decimal(data['indices'][left]['return_pct']) - Decimal(data['indices'][right]['return_pct']))
        for left, right in (('COMP', 'SPX'), ('SOX', 'SPX'), ('SOX', 'COMP'))
    }

    assert evaluate_us_rebound(seal(data))['status'] == 'NO_SIGNAL'


@pytest.mark.parametrize('code', ('SPX', 'COMP', 'SOX'))
def test_zero_reference_volatility_is_unavailable_even_if_another_index_qualifies(code):
    data = features()
    data['indices'][code].update({
        'reference_sample_std_pct': '0', 'shock_z': None,
        'shock_unavailable_reason': 'zero_reference_volatility',
    })

    result = evaluate_us_rebound(seal(data))

    assert result['status'] == 'UNAVAILABLE'
    assert result['reasons'] == [f'{code}:zero_reference_volatility']


@pytest.mark.parametrize(('field', 'value'), [
    ('return_pct', None), ('return_pct', True), ('return_pct', 1.0),
    ('acceleration_pp', 'NaN'), ('shock_z', 'Infinity'), ('percentile_pct', '-Infinity'),
    ('reference_mean_pct', 'bad'), ('reference_sample_std_pct', ' 1'),
    ('return_pct', '1_0'), ('return_pct', []), ('return_pct', '\u0661'),
])
def test_malformed_numeric_features_are_errors_instead_of_observations(field, value):
    data = features()
    data['indices']['SPX'][field] = value

    with pytest.raises(ValueError):
        evaluate_us_rebound(seal(data))


@pytest.mark.parametrize(('path', 'value'), [
    (('schema_version',), 'us_index_features_v1'), (('status',), 'unavailable'),
    (('history_source',), 'explicit_history'), (('reference_window',), 19),
    (('history_count',), 19), (('history_count',), True),
    (('history_inputs',), ['old-seal']), (('source_environment',), 'production'),
    (('target_trade_date',), '2026-13-99'), (('us_session_date',), '2026-11-26'),
    (('indices',), {}), (('indices', 'COMP'), []),
    (('indices', 'SPX', 'percentile_pct'), '101'),
    (('indices', 'SPX', 'reference_sample_std_pct'), '-1'),
    (('indices', 'SPX', 'reference_mean_pct'), None),
    (('indices', 'SPX', 'shock_z'), None),
    (('return_spreads_pp', 'COMP-SPX'), 'NaN'),
])
def test_inconsistent_or_unsupported_feature_schema_is_rejected(path, value):
    data = features()
    parent = data
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value

    with pytest.raises(ValueError):
        evaluate_us_rebound(seal(data))


def test_rule_thresholds_are_immutable_explicit_and_change_the_observation():
    config = USReboundRuleConfig()
    stricter = replace(config, min_shock_z=Decimal('4'))

    assert evaluate_us_rebound(features(), config=stricter)['status'] == 'NO_SIGNAL'
    assert config.as_dict() == {
        'min_return_pct': '0', 'min_acceleration_pp': '0', 'min_shock_z': '2',
        'min_percentile_pct': '95', 'min_outperformance_pp': '0',
        'validation_status': 'exploratory_unvalidated',
    }
    assert config.config_hash == replace(config, min_shock_z=Decimal('2.00')).config_hash
    assert config.config_hash != stricter.config_hash
    with pytest.raises(FrozenInstanceError):
        config.min_shock_z = Decimal('0')


@pytest.mark.parametrize(('field', 'value'), [
    ('min_return_pct', Decimal('-0.1')), ('min_acceleration_pp', Decimal('-1')),
    ('min_shock_z', Decimal('0')), ('min_shock_z', 2),
    ('min_percentile_pct', Decimal('101')), ('min_percentile_pct', Decimal('0')),
    ('min_outperformance_pp', Decimal('NaN')), ('min_return_pct', Decimal('Infinity')),
])
def test_invalid_or_inexact_rule_configuration_is_rejected(field, value):
    with pytest.raises(ValueError):
        replace(USReboundRuleConfig(), **{field: value})


@pytest.mark.parametrize('status', ('WATCH', 'NO_SIGNAL', 'UNAVAILABLE'))
def test_every_observation_binds_provenance_evidence_and_unvalidated_non_authority(status):
    data = features()
    config = USReboundRuleConfig()
    if status == 'NO_SIGNAL':
        config = replace(config, min_shock_z=Decimal('4'))
    if status == 'UNAVAILABLE':
        data['indices']['SPX'].update({
            'reference_sample_std_pct': '0', 'shock_z': None,
            'shock_unavailable_reason': 'zero_reference_volatility',
        })
        data = seal(data)
    original = deepcopy(data)

    result = evaluate_us_rebound(data, config=config)

    assert result['status'] == status
    assert result['strategy_id'] == 'krx_us_rebound'
    assert result['rule_version'] == 'krx_us_rebound_exploratory_v1'
    assert result['validation_status'] == 'exploratory_unvalidated'
    assert result['order_authority'] is False
    assert result['config'] == config.as_dict()
    assert result['config_hash'] == config.config_hash
    for key in ('feature_id', 'input_manifest_id', 'target_trade_date', 'source_environment',
                'us_session_date', 'previous_us_session_date'):
        assert result[key] == data[key]
    assert result['reasons']
    assert result['evidence']['indices']['COMP']['return_pct'] == '2'
    result['evidence']['indices']['SPX']['return_pct'] = '999'
    assert data == original


def test_changed_feature_content_cannot_reuse_the_original_replay_identity():
    data = features()
    data['indices']['COMP']['return_pct'] = '200'

    with pytest.raises(ValueError):
        evaluate_us_rebound(data)


@pytest.mark.parametrize(('path', 'value'), [
    (('input_manifest_id',), 'not-a-manifest'), (('feature_id',), 'not-a-feature'),
    (('reference_window',), 20.0), (('history_count',), 20.0),
    (('us_session_date',), '20261125'), (('previous_us_session_date',), '20261124'),
])
def test_replay_bindings_use_exact_supported_identifiers_and_metadata(path, value):
    data = features()
    data[path[0]] = value
    if path != ('feature_id',):
        data = seal(data)

    with pytest.raises(ValueError):
        evaluate_us_rebound(data)


def test_return_spreads_cannot_claim_leadership_inconsistent_with_event_returns():
    data = features()
    data['indices']['SPX']['return_pct'] = '3'

    with pytest.raises(ValueError):
        evaluate_us_rebound(seal(data))


def test_equivalent_canonical_features_replay_the_same_unavailable_evidence():
    data = features()
    for item in data['indices'].values():
        item.update({
            'reference_sample_std_pct': '0', 'shock_z': None,
            'shock_unavailable_reason': 'zero_reference_volatility',
        })
    data = seal(data)
    reordered = {**data, 'indices': dict(reversed(data['indices'].items()))}

    assert evaluate_us_rebound(reordered) == evaluate_us_rebound(data)


def test_callers_cannot_replace_validated_configuration_with_an_untyped_mapping():
    with pytest.raises(ValueError):
        evaluate_us_rebound(features(), config={'min_shock_z': '-100'})
