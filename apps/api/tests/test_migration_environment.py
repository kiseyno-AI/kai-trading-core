import runpy
from pathlib import Path
from unittest.mock import patch

import pytest
from alembic import context
from alembic.config import Config
from sqlalchemy.engine import make_url

from app.core.settings import settings


ENV_PATH = Path(__file__).resolve().parents[1] / 'db_migrations' / 'env.py'


def _run_environment(*, offline):
    with (
        patch.object(context, 'config', Config(), create=True),
        patch.object(context, 'is_offline_mode', return_value=offline),
        patch.object(context, 'configure') as configure,
        patch.object(context, 'begin_transaction'),
        patch.object(context, 'run_migrations') as run_migrations,
        patch('sqlalchemy.engine_from_config') as engine_from_config,
    ):
        runpy.run_path(str(ENV_PATH))

    run_migrations.assert_called_once_with()
    if offline:
        engine_from_config.assert_not_called()
        return configure.call_args.kwargs['url']
    return engine_from_config.call_args.args[0]['sqlalchemy.url']


@pytest.mark.parametrize('offline', [False, True])
@pytest.mark.parametrize(('username', 'password'), [
    ('audit', 'plain'),
    ('audit@worker', 'p@ss:/word'),
    ('audit:name', 'literal%40value'),
])
def test_migration_environment_preserves_component_credentials(offline, username, password):
    with (
        patch.object(settings, 'database_url', None),
        patch.object(settings, 'postgres_user', username),
        patch.object(settings, 'postgres_password', password),
        patch.object(settings, 'postgres_host', '127.0.0.1'),
        patch.object(settings, 'postgres_port', 55438),
        patch.object(settings, 'postgres_db', 'kai_trading_core_url_test'),
    ):
        parsed = make_url(_run_environment(offline=offline))

    assert parsed.username == username
    assert parsed.password == password
    assert parsed.host == '127.0.0.1'
    assert parsed.port == 55438
    assert parsed.database == 'kai_trading_core_url_test'


@pytest.mark.parametrize('offline', [False, True])
def test_migration_environment_keeps_explicit_encoded_url(offline):
    url = 'postgresql+psycopg://audit:p%40ss%25@127.0.0.1:55438/kai_trading_core_url_test'
    with patch.object(settings, 'database_url', url):
        assert _run_environment(offline=offline) == url
