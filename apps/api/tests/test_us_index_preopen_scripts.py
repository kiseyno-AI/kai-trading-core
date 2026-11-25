import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys

import pytest


SCRIPTS = Path(__file__).resolve().parents[3] / 'scripts'


def test_printing_launchd_configuration_is_inert_and_escapes_paths(tmp_path):
    root = tmp_path / 'source & repo'
    (root / 'scripts').mkdir(parents=True)
    (root / 'apps/api').mkdir(parents=True)
    installer = root / 'scripts/install_us_index_preopen_launchd.sh'
    shutil.copy2(SCRIPTS / installer.name, installer)
    shutil.copy2(SCRIPTS / 'run_us_index_preopen.sh', root / 'scripts/run_us_index_preopen.sh')
    rendered = subprocess.run(
        ['bash', str(installer), '--print'], check=True, capture_output=True,
        env={**os.environ, 'PYTHON_BIN': sys.executable},
    )

    configuration = plistlib.loads(rendered.stdout)
    assert configuration['StartInterval'] == 60
    assert configuration['RunAtLoad'] is True
    assert configuration['KeepAlive'] is False
    assert configuration['ProgramArguments'] == ['/bin/bash', str(root / 'scripts/run_us_index_preopen.sh')]
    assert configuration['EnvironmentVariables']['PYTHON_BIN'] == sys.executable
    assert not (root / 'apps/api/var/log').exists()


@pytest.mark.parametrize(('hhmm', 'should_run'), [('0659', False), ('0700', True), ('0711', True), ('0712', False)])
def test_wrapper_checks_kst_before_starting_python(tmp_path, hhmm, should_run):
    root = tmp_path / 'worker'
    (root / 'scripts').mkdir(parents=True)
    (root / 'apps/api/.venv/bin').mkdir(parents=True)
    wrapper = root / 'scripts/run_us_index_preopen.sh'
    shutil.copy2(SCRIPTS / wrapper.name, wrapper)
    fake_date = root / 'scripts/date'
    fake_date.write_text('#!/bin/sh\nprintf "%s" "$TZ" > "$TZ_PROBE"\nprintf "%s\\n" "$TEST_KRX_TIME"\n')
    fake_python = root / 'apps/api/.venv/bin/python'
    fake_python.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
    fake_date.chmod(0o755)
    fake_python.chmod(0o755)
    probe = tmp_path / 'timezone'

    result = subprocess.run(
        ['bash', str(wrapper)], check=True, capture_output=True, text=True,
        env={**{k: v for k, v in os.environ.items() if k not in ('PYTHON_BIN', 'VIRTUAL_ENV')},
             'PATH': f'{root / "scripts"}:{os.environ["PATH"]}',
             'TEST_KRX_TIME': hhmm, 'TZ_PROBE': str(probe)},
    )

    assert probe.read_text() == 'Asia/Seoul'
    assert bool(result.stdout) is should_run
    if should_run:
        assert result.stdout.splitlines() == ['-m', 'app.internal.us_index_preopen']


@pytest.mark.parametrize('mode', ['explicit', 'active', 'local', 'system', 'relative'])
def test_wrapper_resolves_the_supported_interpreter_precedence(tmp_path, mode):
    root = tmp_path / 'worker'
    (root / 'scripts').mkdir(parents=True)
    (root / 'apps/api').mkdir(parents=True)
    wrapper = root / 'scripts/run_us_index_preopen.sh'
    shutil.copy2(SCRIPTS / wrapper.name, wrapper)
    local = root / 'apps/api/.venv/bin/python'
    active = root / 'active/bin/python'
    explicit = root / 'configured/python'
    system = root / 'scripts/python3.12'
    paths = [system]
    if mode != 'system':
        paths.append(local)
    if mode in ('explicit', 'active'):
        paths.append(active)
    if mode == 'explicit':
        paths.append(explicit)
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('#!/bin/sh\nexit 0\n')
        path.chmod(0o755)
    environment = {k: v for k, v in os.environ.items() if k not in ('PYTHON_BIN', 'VIRTUAL_ENV')}
    environment['PATH'] = f'{root / "scripts"}:{environment["PATH"]}'
    if mode in ('explicit', 'active'):
        environment['VIRTUAL_ENV'] = str(root / 'active')
    if mode == 'explicit':
        environment['PYTHON_BIN'] = str(explicit)
        (root / 'apps/api/.env').write_text('PYTHON_BIN=/should/not/override/process\n')
    if mode == 'relative':
        (root / 'apps/api/.env').write_text('PYTHON_BIN=.venv/bin/python\n')

    result = subprocess.run(
        ['bash', str(wrapper), '--resolve-python'], env=environment,
        check=True, capture_output=True, text=True,
    )
    expected = {'explicit': explicit, 'active': active, 'local': local, 'system': system, 'relative': local}[mode]
    assert result.stdout.strip() == str(expected)
