from __future__ import annotations

from app.brokers.kis.client import KISClient
from app.core.settings import settings

kis_client = KISClient()
# Quote/enrichment endpoints must stay on the real domain even when order
# execution is configured for the mock environment. Reuse the process-wide
# client (and its OAuth cache) when it already targets production; only create
# a real-domain override when the main client is intentionally in mock mode.
kis_real_client = KISClient(use_mock=False) if settings.kis_use_mock else kis_client
