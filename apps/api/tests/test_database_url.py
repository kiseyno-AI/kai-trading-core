from unittest.mock import patch

import pytest
from sqlalchemy.engine import make_url

from app import db


@pytest.mark.parametrize('password', ['plain', 'p%40ss', 'literal%25value'])
def test_alembic_configuration_preserves_the_database_url(password):
    url = f'postgresql+psycopg://audit:{password}@127.0.0.1:55432/kai_trading_core_url_test'
    with patch.object(db, 'DATABASE_URL', url):
        configuration = db.get_alembic_config()

    assert configuration.get_main_option('sqlalchemy.url') == url


@pytest.mark.parametrize(('username', 'password'), [
    ('audit', 'plain'), ('audit@worker', 'p@ss:/word'), ('audit:name', 'literal%40value'),
])
def test_component_database_credentials_roundtrip_without_changing_the_host(username, password):
    with (
        patch.object(db.settings, 'database_url', None),
        patch.object(db.settings, 'postgres_user', username),
        patch.object(db.settings, 'postgres_password', password),
        patch.object(db.settings, 'postgres_host', '127.0.0.1'),
        patch.object(db.settings, 'postgres_port', 55432),
        patch.object(db.settings, 'postgres_db', 'kai_trading_core_url_test'),
    ):
        parsed = make_url(db._build_database_url())

    assert parsed.username == username
    assert parsed.password == password
    assert parsed.host == '127.0.0.1'
    assert parsed.port == 55432
    assert parsed.database == 'kai_trading_core_url_test'


def test_explicit_database_url_keeps_precedence_verbatim():
    url = 'postgresql+psycopg://audit:encoded%40value@127.0.0.1:55432/kai_trading_core_url_test'
    with patch.object(db.settings, 'database_url', url):
        assert db._build_database_url() == url
