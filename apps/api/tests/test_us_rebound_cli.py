from datetime import date
import json

from app.core.settings import Settings, settings
from app.internal.us_rebound import main


def test_cli_uses_independent_configuration_and_returns_safe_job_failures(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(settings, 'us_rebound_mode', 'observe')
    monkeypatch.setattr(settings, 'us_rebound_enabled_from', '2026-11-27')
    seen = []

    def runner(**kwargs):
        seen.append(kwargs)
        return {'status': 'not_due', 'reason': 'outside_window'}

    assert main(['--state-dir', str(tmp_path)], runner=runner) == 0
    assert seen == [{'mode': 'observe', 'enabled_from': date(2026, 11, 27), 'state_dir': str(tmp_path)}]
    assert json.loads(capsys.readouterr().out)['status'] == 'not_due'

    def failed(**kwargs):
        raise ValueError('private-test-secret must never enter job output')

    assert main([], runner=failed) == 1
    value = json.loads(capsys.readouterr().out)
    assert value == {'status': 'failed', 'stage': 'us_rebound', 'error_type': 'ValueError'}


def test_bad_optional_strategy_configuration_does_not_prevent_core_settings_loading(monkeypatch, capsys):
    configured = Settings(us_rebound_mode='observe', us_rebound_enabled_from='bad-date')
    assert configured.us_rebound_enabled_from == 'bad-date'
    monkeypatch.setattr(settings, 'us_rebound_mode', configured.us_rebound_mode)
    monkeypatch.setattr(settings, 'us_rebound_enabled_from', configured.us_rebound_enabled_from)
    assert main([], runner=lambda **kwargs: (_ for _ in ()).throw(AssertionError())) == 1
    assert json.loads(capsys.readouterr().out)['error_type'] == 'ValueError'


def test_execution_configuration_is_separate_and_optional_values_do_not_break_core_startup(monkeypatch, capsys):
    configured = Settings(us_rebound_mode='paper', us_rebound_enabled_from='2026-11-30',
                          us_rebound_execution_enabled='not-a-boolean', us_rebound_execution_authority_until='not-a-date')
    monkeypatch.setattr(settings, 'us_rebound_mode', configured.us_rebound_mode)
    monkeypatch.setattr(settings, 'us_rebound_enabled_from', configured.us_rebound_enabled_from)
    monkeypatch.setattr(settings, 'us_rebound_execution_enabled', configured.us_rebound_execution_enabled)
    seen = []
    def runner(**kwargs):
        seen.append(kwargs)
        return {'status': 'completed', 'execution': {'status': 'failed'}}
    assert main([], runner=runner) == 1
    assert seen[0]['execution_config'].enabled == 'not-a-boolean'
    assert json.loads(capsys.readouterr().out)['execution']['status'] == 'failed'
