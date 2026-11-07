from __future__ import annotations

import math
from decimal import InvalidOperation
from typing import Any


def finite_float(value: Any) -> float | None:
    """A finite float, or ``None`` for anything that is not a real number.

    Booleans, blanks, NaN and infinities are ``None`` so they can never reach
    a score, a ranking or a JSON payload as a number. Strings may carry
    thousands separators (``'1,234.5'``).
    """

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        value = value.strip().replace(',', '')
        if not value:
            return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError, InvalidOperation):
        return None
    return number if math.isfinite(number) else None


