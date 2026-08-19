from __future__ import annotations

from app.brokers.kis.client import KISClient

kis_client = KISClient()
# Quote/enrichment endpoints must stay on the real domain even when order
# execution is configured for the mock environment. Reuse this instance so its
# OAuth token cache is shared across candidate fallback requests.
kis_real_client = KISClient(use_mock=False)
