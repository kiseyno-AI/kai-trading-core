"""Repository-wide pytest safety gates loaded before application test modules."""

import os
import tempfile

# Keep the daily-pipeline live feed (snapshot + JSONL) out of the real
# apps/api/var while tests drive the run-state service.
_LIVE_DIR = tempfile.mkdtemp(prefix='kai-live-progress-tests-')
os.environ.setdefault('DAILY_PIPELINE_LIVE_DIR', _LIVE_DIR)
os.environ.setdefault('DAILY_PIPELINE_LIVE_LOG', os.path.join(_LIVE_DIR, 'live.jsonl'))

from app.core.settings import settings  # noqa: E402
from app.core.test_database_safety import assert_safe_test_database_settings  # noqa: E402


assert_safe_test_database_settings(settings)
