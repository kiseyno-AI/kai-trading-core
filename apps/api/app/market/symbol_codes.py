from __future__ import annotations

import re
from typing import Any


KRX_SHORT_CODE_PATTERN = re.compile(r'[0-9]{4}[0-9A-Z]{2}\Z')


def normalize_krx_short_code(value: Any) -> str | None:
    """Return one canonical six-character KRX short code.

    KRX market payloads contain both legacy numeric codes and newer codes with
    an ASCII letter in the final two positions.  Short numeric inputs retain
    the legacy zero-padding behavior; every other shape fails closed.
    """

    raw = str(value or '').strip().upper()
    if not raw:
        return None
    normalized = raw.zfill(6) if raw.isascii() and raw.isdigit() else raw
    if KRX_SHORT_CODE_PATTERN.fullmatch(normalized) is None:
        return None
    return normalized


def is_canonical_krx_short_code(value: Any) -> bool:
    raw = str(value or '').strip()
    return (
        raw == raw.upper()
        and KRX_SHORT_CODE_PATTERN.fullmatch(raw) is not None
    )
