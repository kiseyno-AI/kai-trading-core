"""Repository-wide pytest safety gates loaded before application test modules."""

from app.core.settings import settings
from app.core.test_database_safety import assert_safe_test_database_settings


assert_safe_test_database_settings(settings)
