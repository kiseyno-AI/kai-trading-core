"""Exercise the weekly wrapper with isolated local command substitutes."""

import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


@pytest.mark.parametrize('improvement_exit', [0, 1, 2])
def test_week_is_complete_only_after_successful_improvement(tmp_path, improvement_exit):
    repo = Path(__file__).resolve().parents[3]
    script = tmp_path / 'scripts' / 'run_agent_improvement_local.sh'
    script.parent.mkdir()
    shutil.copyfile(repo / 'scripts' / script.name, script)
    fake_python = tmp_path / 'apps' / 'api' / '.venv' / 'bin' / 'python'
    fake_python.parent.mkdir(parents=True)
    fake_python.write_text(
        f'#!{sys.executable}\n'
        'import os, pathlib, sys\n'
        'root = pathlib.Path(__file__).resolve().parents[4]\n'
        'out = root / "var" / "alpha-lab"\n'
        'args = sys.argv[1:]\n'
        'if args[:2] == ["-m", "app.analytics.signal_lab"]:\n'
        '    (out / "report-test.json").write_text("{}")\n'
        'elif args[:2] == ["-m", "app.analytics.improvement_findings"]:\n'
        '    pathlib.Path(args[args.index("--out") + 1]).write_text("{}")\n'
        'elif args[0] == "scripts/agent_improvement_run.py":\n'
        '    sys.exit(int(os.environ["AUDIT_IMPROVEMENT_EXIT"]))\n'
        'else:\n'
        '    sys.exit(99)\n'
    )
    fake_python.chmod(0o755)

    result = subprocess.run(
        ['/bin/bash', str(script), '--force'],
        cwd=tmp_path,
        env={**os.environ, 'AUDIT_IMPROVEMENT_EXIT': str(improvement_exit)},
        capture_output=True,
        text=True,
        timeout=10,
    )

    markers = list((tmp_path / 'var' / 'alpha-lab').glob('.done-*'))
    if improvement_exit == 0:
        assert result.returncode == 0
        assert len(markers) == 1
    else:
        assert result.returncode != 0
        assert markers == []
