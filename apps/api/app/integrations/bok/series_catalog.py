from __future__ import annotations

from typing import Any


ECOS_SERIES_ALIASES: dict[str, dict[str, Any]] = {
    'kr_base_rate': {
        'alias': 'kr_base_rate',
        'description': 'Bank of Korea base rate.',
        'stat_code': '722Y001',
        'cycle': 'M',
        'item_code_1': '0101000',
    },
}


def list_series_aliases() -> list[dict[str, Any]]:
    return [ECOS_SERIES_ALIASES[key] for key in sorted(ECOS_SERIES_ALIASES)]


def resolve_series_alias(alias: str | None) -> dict[str, Any] | None:
    if alias is None:
        return None
    return ECOS_SERIES_ALIASES.get(alias.strip().lower())
