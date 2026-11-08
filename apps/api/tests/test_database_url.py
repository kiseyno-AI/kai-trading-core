from unittest.mock import patch

import pytest

from app import db


@pytest.mark.parametrize('password', ['plain', 'p%40ss', 'literal%25value'])
def test_alembic_configuration_preserves_the_database_url(password):
    url = f'postgresql+psycopg://audit:{password}@127.0.0.1:55432/kai_trading_core_url_test'
    with patch.object(db, 'DATABASE_URL', url):
        configuration = db.get_alembic_config()

    assert configuration.get_main_option('sqlalchemy.url') == url
