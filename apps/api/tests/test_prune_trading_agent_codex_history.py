import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[3] / 'scripts' / 'prune_trading_agent_codex_history.py'
spec = importlib.util.spec_from_file_location('prune_trading_agent_codex_history', SCRIPT)
prune = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prune)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('x')


def test_targets_only_trading_agent_history_and_keeps_config(tmp_path):
    home = tmp_path / 'eod-summarizer' / 'agent' / 'codex-home'
    for rel in ('sessions/2026/a.jsonl', 'state_5.sqlite', 'state_5.sqlite-wal', 'logs_2.sqlite',
                'thread_history_1.sqlite', 'memories_1.sqlite', 'memories/m', 'tmp/t',
                'config.toml', 'installation_id', 'skills/s', 'queue_1.sqlite', 'models_cache.json'):
        _touch(home / rel)
    _touch(tmp_path / 'tony' / 'agent' / 'codex-home' / 'sessions' / 'a.jsonl')
    _touch(tmp_path / 'eod-summarizer' / 'agent' / 'openclaw-agent.sqlite')

    names = sorted(str(p.relative_to(home)) for p in prune.history_targets(tmp_path))

    assert names == ['logs_2.sqlite', 'memories', 'memories_1.sqlite', 'sessions', 'state_5.sqlite',
                     'state_5.sqlite-wal', 'thread_history_1.sqlite', 'tmp']


def test_dry_run_never_touches_files(tmp_path, capsys):
    home = tmp_path / 'signal-decider' / 'agent' / 'codex-home'
    _touch(home / 'sessions' / 'a.jsonl')
    assert prune.main(['--agents-root', str(tmp_path)]) == 0
    assert (home / 'sessions' / 'a.jsonl').exists()
    assert '"dry_run"' in capsys.readouterr().out


def test_apply_skips_when_free_space_is_enough(tmp_path, capsys):
    _touch(tmp_path / 'signal-decider' / 'agent' / 'codex-home' / 'sessions' / 'a.jsonl')
    assert prune.main(['--agents-root', str(tmp_path), '--apply', '--min-free-gib', '0']) == 0
    assert '"enough_free_space"' in capsys.readouterr().out
