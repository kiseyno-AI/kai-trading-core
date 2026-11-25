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
    installer = root / 'scripts/install_us_index_preopen_launchd.sh'
    shutil.copy2(SCRIPTS / installer.name, installer)
    rendered = subprocess.run(
        ['bash', str(installer), '--print'], check=True, capture_output=True,
        env={**os.environ, 'PYTHON_BIN': sys.executable},
    )

    configuration = plistlib.loads(rendered.stdout)
    assert configuration['StartInterval'] == 60
    assert configuration['RunAtLoad'] is True
    assert configuration['KeepAlive'] is False
    assert configuration['ProgramArguments'] == ['/bin/bash', str(root / 'scripts/run_us_index_preopen.sh')]
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
        env={**os.environ, 'PATH': f'{root / "scripts"}:{os.environ["PATH"]}',
             'TEST_KRX_TIME': hhmm, 'TZ_PROBE': str(probe)},
    )

    assert probe.read_text() == 'Asia/Seoul'
    assert bool(result.stdout) is should_run
    if should_run:
        assert result.stdout.splitlines() == ['-m', 'app.internal.us_index_preopen']
