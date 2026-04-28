from __future__ import annotations

from dataclasses import dataclass


_ACCOUNT_REFERENCE_ERROR = (
    'account_reference must use the KIS:<account_number>-<product_code> format for control and inquiry operations'
)


@dataclass(slots=True)
class AccountScope:
    account_reference: str
    account_number: str
    account_product_code: str


def build_kis_account_reference(account_number: str, account_product_code: str) -> str:
    normalized_account_number = str(account_number or '').strip()
    normalized_account_product_code = str(account_product_code or '').strip()
    if not normalized_account_number or not normalized_account_product_code:
        raise ValueError(_ACCOUNT_REFERENCE_ERROR)
    return f'KIS:{normalized_account_number}-{normalized_account_product_code}'


def parse_kis_account_reference(account_reference: str) -> AccountScope:
    normalized = str(account_reference or '').strip()
    if not normalized.startswith('KIS:') or '-' not in normalized:
        raise ValueError(_ACCOUNT_REFERENCE_ERROR)

    account_number, account_product_code = normalized.split(':', 1)[1].rsplit('-', 1)
    if not account_number or not account_product_code:
        raise ValueError(_ACCOUNT_REFERENCE_ERROR)

    return AccountScope(
        account_reference=normalized,
        account_number=account_number,
        account_product_code=account_product_code,
    )
