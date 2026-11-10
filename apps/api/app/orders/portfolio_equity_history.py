from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from app.db import session_scope
from app.db_models import PortfolioAssetSnapshot


SEOUL = ZoneInfo('Asia/Seoul')
SOURCE = 'portfolio_daily_equity_v1'
PORTFOLIO_REFERENCE = 'portfolio:total'
CENT = Decimal('0.01')
MAX_AMOUNT = Decimal(2**53 - 1)


def capture_daily_equity(
    snapshot: dict, session_close: datetime, *, now: datetime | None = None,
) -> bool:
    if (
        snapshot.get('sync', {}).get('status') != 'synced'
        or snapshot.get('accountCoverage', {}).get('status') != 'complete'
        or snapshot.get('freshness', {}).get('isStale') is not False
        or not snapshot.get('accounts')
    ):
        return False
    try:
        as_of = datetime.fromisoformat(snapshot['asOf'])
    except (KeyError, TypeError, ValueError):
        return False
    current = now or datetime.now(timezone.utc)
    session_date = session_close.astimezone(SEOUL).date()
    if (
        as_of.tzinfo is None or as_of < session_close or as_of > current
        or as_of.astimezone(SEOUL).date() != session_date
        or current.astimezone(SEOUL).date() != session_date
    ):
        return False

    totals: dict[str, Decimal] = {}
    for account in snapshot['accounts']:
        money = (account.get('summary') or {}).get('totalEquity') or {}
        currency = money.get('currency')
        if not isinstance(currency, str) or not re.fullmatch('[A-Z]{3}', currency):
            return False
        try:
            amount = Decimal(str(money.get('amount')))
        except InvalidOperation:
            return False
        if not amount.is_finite() or abs(amount) > MAX_AMOUNT:
            return False
        totals[currency] = totals.get(currency, Decimal(0)) + amount

    if any(abs(total) > MAX_AMOUNT for total in totals.values()):
        return False
    try:
        store_equity_days({as_of: totals})
    except SQLAlchemyError:
        raise RuntimeError('Daily portfolio valuation storage failed') from None
    return True


def store_equity_days(
    valuations: dict[datetime, dict[str, Decimal]], *, verify_existing: bool = False,
) -> None:
    with session_scope(statement_timeout_ms=3000, lock_timeout_ms=1000) as session:
        # The existing snapshot table permits intraday rows, so serialize this daily writer.
        session.execute(text('SELECT pg_advisory_xact_lock(hashtext(:source))'), {'source': SOURCE})
        missing = []
        for as_of, totals in valuations.items():
            day_start = datetime.combine(as_of.astimezone(SEOUL).date(), time.min, SEOUL)
            totals = {currency: amount.quantize(CENT) for currency, amount in totals.items()}
            existing = session.execute(select(
                PortfolioAssetSnapshot.currency, PortfolioAssetSnapshot.total_assets,
            ).where(
                PortfolioAssetSnapshot.account_reference == PORTFOLIO_REFERENCE,
                PortfolioAssetSnapshot.source == SOURCE,
                PortfolioAssetSnapshot.captured_at >= day_start,
                PortfolioAssetSnapshot.captured_at < day_start + timedelta(days=1),
            )).all()
            if existing:
                if verify_existing and (
                    len(existing) != len(totals) or {currency: amount for currency, amount in existing} != totals
                ):
                    raise ValueError('Legacy valuations do not match the Core history for one or more dates')
                continue
            missing.extend(
                PortfolioAssetSnapshot(
                    account_reference=PORTFOLIO_REFERENCE, total_assets=amount,
                    currency=currency, as_of=as_of, captured_at=as_of, source=SOURCE,
                )
                for currency, amount in totals.items()
            )
        session.add_all(missing)


def read_equity_history() -> dict:
    series: dict[str, list[dict]] = {}
    baselines: dict[str, Decimal] = {}
    with session_scope(statement_timeout_ms=3000) as session:
        rows = session.scalars(select(PortfolioAssetSnapshot).where(
            PortfolioAssetSnapshot.account_reference == PORTFOLIO_REFERENCE,
            PortfolioAssetSnapshot.source == SOURCE,
        ).order_by(PortfolioAssetSnapshot.captured_at, PortfolioAssetSnapshot.id))
        for row in rows:
            baseline = baselines.setdefault(row.currency, row.total_assets)
            change = (row.total_assets / baseline - 1) * 100 if baseline > 0 else None
            series.setdefault(row.currency, []).append({
                'date': row.as_of.astimezone(SEOUL).date().isoformat(),
                'equity': float(row.total_assets),
                'returnPct': float(change.quantize(CENT)) if change is not None else None,
            })
    return {'equity': [{'currency': currency, 'points': series[currency]} for currency in sorted(series)]}
