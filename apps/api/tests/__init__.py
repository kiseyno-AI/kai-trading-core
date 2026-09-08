"""Runner-independent safety gate for package-imported Core tests."""

from app.core.settings import settings
from app.core.test_database_safety import assert_safe_test_database_settings


assert_safe_test_database_settings(settings)
