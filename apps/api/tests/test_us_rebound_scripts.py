import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys

import pytest


SCRIPTS = Path(__file__).resolve().parents[3] / 'scripts'


@pytest.mark.parametrize(
    ('hhmm', 'should_run'),
    [('0644', False), ('0645', True), ('1530', True), ('1531', False)],
)
def test_wrapper_checks_kst_before_sourcing_env_or_starting_python(tmp_path, hhmm, should_run):
    root = tmp_path / 'worker'
    (root / 'scripts').mkdir(parents=True)
    (root / 'apps/api/.venv/bin').mkdir(parents=True)
    wrapper = root / 'scripts/run_us_rebound.sh'
    assert (SCRIPTS / wrapper.name).is_file()
    shutil.copy2(SCRIPTS / wrapper.name, wrapper)
    fake_date = root / 'scripts/date'
    fake_date.write_text('#!/bin/sh\nprintf "%s" "$TZ" > "$TZ_PROBE"\nprintf "%s\\n" "$TEST_KRX_TIME"\n')
    fake_python = root / 'apps/api/.venv/bin/python'
    fake_python.write_text('#!/bin/sh\nprintf "%s\\n" "$@" "$REBOUND_TEST_ENV"\n')
    fake_date.chmod(0o755)
    fake_python.chmod(0o755)
    env_probe = tmp_path / 'env-loaded'
    (root / 'apps/api/.env').write_text('printf loaded > "$ENV_PROBE"\nREBOUND_TEST_ENV=loaded\n')
    timezone_probe = tmp_path / 'timezone'
    state_dir = tmp_path / 'shared state'

    result = subprocess.run(
        ['bash', str(wrapper), '--state-dir', str(state_dir)],
        check=True, capture_output=True, text=True,
        env={**{k: v for k, v in os.environ.items() if k not in ('PYTHON_BIN', 'VIRTUAL_ENV')},
             'PATH': f'{root / "scripts"}:{os.environ["PATH"]}',
             'TZ': 'America/New_York', 'TEST_KRX_TIME': hhmm,
             'TZ_PROBE': str(timezone_probe), 'ENV_PROBE': str(env_probe)},
    )

    assert timezone_probe.read_text() == 'Asia/Seoul'
    assert env_probe.exists() is should_run
    assert bool(result.stdout) is should_run
    assert not state_dir.exists()
    assert not (root / 'apps/api/var').exists()
    if should_run:
        assert result.stdout.splitlines() == [
            '-m', 'app.internal.us_rebound', '--state-dir', str(state_dir), 'loaded',
        ]


@pytest.mark.parametrize(
    'mode', ['explicit', 'dotenv', 'active', 'dotenv-active', 'local', 'system', 'relative', 'empty'],
)
def test_resolving_python_is_inert_and_preserves_interpreter_precedence(tmp_path, mode):
    root = tmp_path / 'worker'
    (root / 'scripts').mkdir(parents=True)
    (root / 'apps/api').mkdir(parents=True)
    wrapper = root / 'scripts/run_us_rebound.sh'
    shutil.copy2(SCRIPTS / wrapper.name, wrapper)
    local = root / 'apps/api/.venv/bin/python'
    active = root / 'active/bin/python'
    explicit = root / 'configured/python'
    system = root / 'scripts/python3.12'
    paths = [system, root / 'scripts/date']
    if mode != 'system':
        paths.append(local)
    if mode in ('explicit', 'active', 'dotenv-active', 'empty'):
        paths.append(active)
    if mode in ('explicit', 'dotenv'):
        paths.append(explicit)
    probe = tmp_path / 'executed'
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('#!/bin/sh\nprintf called > "$EXECUTION_PROBE"\nexit 81\n')
        path.chmod(0o755)
    environment = {k: v for k, v in os.environ.items() if k not in ('PYTHON_BIN', 'VIRTUAL_ENV')}
    environment.update(PATH=f'{root / "scripts"}:{environment["PATH"]}', EXECUTION_PROBE=str(probe))
    if mode in ('explicit', 'active', 'empty'):
        environment['VIRTUAL_ENV'] = str(root / 'active')
    if mode == 'explicit':
        environment['PYTHON_BIN'] = str(explicit)
        (root / 'apps/api/.env').write_text('PYTHON_BIN=/should/not/override/process\n')
    if mode == 'dotenv':
        (root / 'apps/api/.env').write_text(f'PYTHON_BIN="{explicit}"\n')
    if mode == 'dotenv-active':
        (root / 'apps/api/.env').write_text(f'VIRTUAL_ENV="{root / "active"}"\n')
    if mode == 'relative':
        (root / 'apps/api/.env').write_text('PYTHON_BIN=.venv/bin/python\n')
    if mode == 'empty':
        environment['PYTHON_BIN'] = ''
        (root / 'apps/api/.env').write_text('PYTHON_BIN=/should/not/override/process\n')

    result = subprocess.run(
        ['bash', str(wrapper), '--resolve-python'], env=environment,
        check=True, capture_output=True, text=True,
    )

    expected = {
        'explicit': explicit, 'dotenv': explicit, 'active': active, 'dotenv-active': active,
        'local': local, 'system': system, 'relative': local, 'empty': active,
    }[mode]
    assert result.stdout.strip() == str(expected)
    assert Path(result.stdout.strip()).is_absolute()
    assert not probe.exists()
    assert not (root / 'apps/api/var').exists()


def test_printing_launchd_configuration_is_inert_and_escapes_paths(tmp_path):
    root = tmp_path / 'source & <repo> "quoted"'
    (root / 'scripts').mkdir(parents=True)
    (root / 'apps/api').mkdir(parents=True)
    installer = root / 'scripts/install_us_rebound_launchd.sh'
    assert (SCRIPTS / installer.name).is_file()
    shutil.copy2(SCRIPTS / installer.name, installer)
    shutil.copy2(SCRIPTS / 'run_us_rebound.sh', root / 'scripts/run_us_rebound.sh')
    (root / 'apps/api/.env').write_text('PYTHON_BIN=/should/not/override/process\n')
    probe = tmp_path / 'external-command'
    for name in ('date', 'launchctl'):
        path = root / 'scripts' / name
        path.write_text('#!/bin/sh\nprintf called > "$EXTERNAL_PROBE"\nexit 81\n')
        path.chmod(0o755)
    launch_home = tmp_path / 'home'

    rendered = subprocess.run(
        ['bash', str(installer), '--print'], check=True, capture_output=True,
        env={**os.environ, 'PYTHON_BIN': sys.executable, 'HOME': str(launch_home),
             'PATH': f'{root / "scripts"}:{os.environ["PATH"]}', 'EXTERNAL_PROBE': str(probe)},
    )

    configuration = plistlib.loads(rendered.stdout)
    assert configuration['Label'] == 'com.kiseyno.kai-us-rebound'
    assert configuration['StartInterval'] == 60
    assert configuration['RunAtLoad'] is True
    assert configuration['KeepAlive'] is False
    assert configuration['ProgramArguments'] == ['/bin/bash', str(root / 'scripts/run_us_rebound.sh')]
    assert configuration['EnvironmentVariables']['PYTHON_BIN'] == sys.executable
    assert configuration['StandardOutPath'] == str(root / 'apps/api/var/log/us-rebound.log')
    assert configuration['StandardErrorPath'] == str(root / 'apps/api/var/log/us-rebound-error.log')
    assert not (root / 'apps/api/var/log').exists()
    assert not launch_home.exists()
    assert not probe.exists()


@pytest.mark.parametrize(('bootstrap_status', 'old_bootout_status'), [(0, 0), (0, 3), (7, 0)])
def test_install_retires_only_preopen_after_successful_registration(
    tmp_path, bootstrap_status, old_bootout_status,
):
    root = tmp_path / 'worker'
    (root / 'scripts').mkdir(parents=True)
    (root / 'apps/api').mkdir(parents=True)
    installer = root / 'scripts/install_us_rebound_launchd.sh'
    shutil.copy2(SCRIPTS / installer.name, installer)
    shutil.copy2(SCRIPTS / 'run_us_rebound.sh', root / 'scripts/run_us_rebound.sh')
    launch_home = tmp_path / 'home'
    agents = launch_home / 'Library/LaunchAgents'
    preserved = {
        agents / 'com.kiseyno.kai-us-index-preopen.plist': b'old preopen job',
        agents / 'com.kiseyno.kai-core.plist': b'core service',
        agents / 'com.kiseyno.kai-exec.plist': b'exec service',
        agents / 'com.kiseyno.kai-daily.plist': b'daily service',
        root / 'apps/api/var/us-index-preopen/checkpoint.json': b'spent attempts',
        root / 'apps/api/var/us-index-preopen/frozen-history/input.json': b'frozen input',
        root / 'apps/api/var/us-index-preopen/frozen-history/seal.json': b'immutable seal',
        root / 'apps/api/var/us-rebound/records/daily.json': b'rebound record',
    }
    for path, contents in preserved.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
    probe = tmp_path / 'launchctl-calls'
    fake_launchctl = root / 'scripts/launchctl'
    fake_launchctl.write_text(
        '#!/bin/sh\n'
        'printf "%s\\t%s\\t%s\\n" "$1" "$2" "${3-}" >> "$LAUNCHCTL_PROBE"\n'
        'case "$1:$2" in\n'
        '  bootstrap:*) exit "$BOOTSTRAP_STATUS" ;;\n'
        '  bootout:*/com.kiseyno.kai-us-index-preopen) exit "$OLD_BOOTOUT_STATUS" ;;\n'
        '  bootout:*) exit 3 ;;\n'
        '  enable:*|disable:*) exit 0 ;;\n'
        'esac\n'
        'exit 90\n'
    )
    fake_launchctl.chmod(0o755)

    result = subprocess.run(
        ['bash', str(installer)], capture_output=True, text=True,
        env={**os.environ, 'PYTHON_BIN': sys.executable, 'HOME': str(launch_home),
             'PATH': f'{root / "scripts"}:{os.environ["PATH"]}', 'LAUNCHCTL_PROBE': str(probe),
             'BOOTSTRAP_STATUS': str(bootstrap_status), 'OLD_BOOTOUT_STATUS': str(old_bootout_status)},
    )

    domain = f'gui/{os.getuid()}'
    plist = agents / 'com.kiseyno.kai-us-rebound.plist'
    expected_calls = [
        ['enable', f'{domain}/com.kiseyno.kai-us-rebound', ''],
        ['bootout', f'{domain}/com.kiseyno.kai-us-rebound', ''],
        ['bootstrap', domain, str(plist)],
    ]
    if bootstrap_status == 0:
        expected_calls.append(['disable', f'{domain}/com.kiseyno.kai-us-index-preopen', ''])
        expected_calls.append(['bootout', f'{domain}/com.kiseyno.kai-us-index-preopen', ''])
    assert [line.split('\t') for line in probe.read_text().splitlines()] == expected_calls
    assert result.returncode == bootstrap_status
    assert bool(result.stdout) is (bootstrap_status == 0)
    configuration = plistlib.loads(plist.read_bytes())
    assert configuration['Label'] == 'com.kiseyno.kai-us-rebound'
    assert configuration['EnvironmentVariables']['PYTHON_BIN'] == sys.executable
    assert (root / 'apps/api/var/log').is_dir()
    for path, contents in preserved.items():
        assert path.read_bytes() == contents
