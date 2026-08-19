from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from app.us_equities.screening import USFundamentalScreenMetrics


REVENUE_TAGS = (
    'RevenueFromContractWithCustomerExcludingAssessedTax',
    'Revenues',
    'SalesRevenueNet',
)
OPERATING_CASH_FLOW_TAGS = (
    'NetCashProvidedByUsedInOperatingActivities',
    'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
)
NET_INCOME_TAGS = ('NetIncomeLoss', 'ProfitLoss')
ASSET_TAGS = ('Assets',)
LIABILITY_TAGS = ('Liabilities',)
SHARE_TAGS = ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding')
PERIODIC_FORMS = {'10-K', '10-Q', '10-K/A', '10-Q/A'}


@dataclass(frozen=True, slots=True)
class SECFundamentalSnapshotData:
    symbol: str
    cik: str
    accession_number: str
    form: str
    filed_date: date
    period_end_date: date | None
    fiscal_year: int | None
    fiscal_period: str | None
    revenue_usd: Decimal | None
    operating_cash_flow_usd: Decimal | None
    net_income_usd: Decimal | None
    assets_usd: Decimal | None
    liabilities_usd: Decimal | None
    shares_outstanding: Decimal | None
    metrics: USFundamentalScreenMetrics
    source_payload: dict[str, Any]


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value or '').strip())
    except ValueError:
        return None


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _fact_entries(
    companyfacts: dict[str, Any],
    tags: Iterable[str],
    *,
    unit: str,
    market_date: date,
) -> list[dict[str, Any]]:
    facts = companyfacts.get('facts')
    us_gaap = facts.get('us-gaap') if isinstance(facts, dict) else None
    dei = facts.get('dei') if isinstance(facts, dict) else None
    namespaces = [namespace for namespace in (us_gaap, dei) if isinstance(namespace, dict)]
    rows: list[dict[str, Any]] = []
    seen: set[tuple] = set()
    for tag in tags:
        concept = next((namespace.get(tag) for namespace in namespaces if isinstance(namespace.get(tag), dict)), None)
        units = concept.get('units') if isinstance(concept, dict) else None
        raw_rows = units.get(unit) if isinstance(units, dict) else None
        if not isinstance(raw_rows, list):
            continue
        for raw in raw_rows:
            if not isinstance(raw, dict):
                continue
            filed = _date(raw.get('filed'))
            end = _date(raw.get('end'))
            form = str(raw.get('form') or '').strip().upper()
            value = _decimal(raw.get('val'))
            if filed is None or end is None or filed > market_date or end > market_date or value is None:
                continue
            key = (raw.get('accn'), raw.get('start'), raw.get('end'), raw.get('fp'), value)
            if key in seen:
                continue
            seen.add(key)
            rows.append({**raw, '_tag': tag, '_filed': filed, '_end': end, '_value': value, '_form': form})
    return sorted(rows, key=lambda row: (row['_filed'], row['_end'], str(row.get('accn') or '')))


def _latest_periodic(entries: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    eligible = [row for row in entries if row['_form'] in PERIODIC_FORMS]
    return max(eligible, key=lambda row: (row['_filed'], row['_end'], str(row.get('accn') or '')), default=None)


def _duration_days(row: dict[str, Any]) -> int | None:
    start = _date(row.get('start'))
    return (row['_end'] - start).days if start else None


def _ttm_value(entries: list[dict[str, Any]]) -> Decimal | None:
    periodic = [row for row in entries if row['_form'] in PERIODIC_FORMS]
    annual = [row for row in periodic if row['_form'].startswith('10-K') and (_duration_days(row) or 0) >= 250]
    latest_annual = max(annual, key=lambda row: (row['_end'], row['_filed']), default=None)
    if latest_annual is None:
        return None
    latest_interim = max(
        (row for row in periodic if row['_form'].startswith('10-Q') and row['_end'] > latest_annual['_end']),
        key=lambda row: (row['_end'], row['_filed']),
        default=None,
    )
    if latest_interim is None:
        return latest_annual['_value']
    current_fy = latest_interim.get('fy')
    prior_fy = int(current_fy) - 1 if str(current_fy or '').isdigit() else None
    current_fp = str(latest_interim.get('fp') or '')
    current_duration = _duration_days(latest_interim)
    comparable = [
        row
        for row in periodic
        if row['_form'].startswith('10-Q')
        and prior_fy is not None
        and row.get('fy') == prior_fy
        and str(row.get('fp') or '') == current_fp
        and (
            current_duration is None
            or _duration_days(row) is None
            or abs((_duration_days(row) or 0) - current_duration) <= 35
        )
    ]
    prior_interim = max(comparable, key=lambda row: (row['_end'], row['_filed']), default=None)
    if prior_interim is None:
        return latest_annual['_value']
    return latest_annual['_value'] + latest_interim['_value'] - prior_interim['_value']


def _latest_point(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    return max(
        (row for row in entries if row['_form'] in PERIODIC_FORMS),
        key=lambda row: (row['_end'], row['_filed']),
        default=None,
    )


def _recent_submission(submissions: dict[str, Any], *, market_date: date) -> tuple[dict[str, Any] | None, bool]:
    filings = submissions.get('filings')
    recent = filings.get('recent') if isinstance(filings, dict) else None
    if not isinstance(recent, dict):
        return None, False
    row_count = max((len(value) for value in recent.values() if isinstance(value, list)), default=0)
    periodic: list[dict[str, Any]] = []
    flagged = False
    cutoff = market_date - timedelta(days=365)
    for index in range(row_count):
        row = {key: value[index] for key, value in recent.items() if isinstance(value, list) and index < len(value)}
        filed = _date(row.get('filingDate'))
        form = str(row.get('form') or '').strip().upper()
        if filed is None or filed > market_date:
            continue
        if form in PERIODIC_FORMS:
            periodic.append({**row, '_filed': filed, '_form': form})
        items = {item.strip() for item in str(row.get('items') or '').split(',')}
        if filed >= cutoff and (form in {'NT 10-K', 'NT 10-Q'} or '1.03' in items):
            flagged = True
    latest = max(periodic, key=lambda row: (row['_filed'], str(row.get('accessionNumber') or '')), default=None)
    return latest, flagged


def parse_sec_fundamental_snapshot(
    *,
    symbol: str,
    cik: str,
    companyfacts: dict[str, Any],
    submissions: dict[str, Any],
    market_date: date,
) -> SECFundamentalSnapshotData | None:
    revenue_rows = _fact_entries(companyfacts, REVENUE_TAGS, unit='USD', market_date=market_date)
    cash_flow_rows = _fact_entries(companyfacts, OPERATING_CASH_FLOW_TAGS, unit='USD', market_date=market_date)
    net_income_rows = _fact_entries(companyfacts, NET_INCOME_TAGS, unit='USD', market_date=market_date)
    asset_rows = _fact_entries(companyfacts, ASSET_TAGS, unit='USD', market_date=market_date)
    liability_rows = _fact_entries(companyfacts, LIABILITY_TAGS, unit='USD', market_date=market_date)
    share_rows = _fact_entries(companyfacts, SHARE_TAGS, unit='shares', market_date=market_date)
    latest_submission, flagged = _recent_submission(submissions, market_date=market_date)
    latest_fact = _latest_periodic((*revenue_rows, *cash_flow_rows, *net_income_rows, *asset_rows))
    if latest_submission is None and latest_fact is None:
        return None

    filed_date = latest_submission['_filed'] if latest_submission else latest_fact['_filed']
    accession_number = str(
        (latest_submission or {}).get('accessionNumber')
        or (latest_fact or {}).get('accn')
        or f'{cik}-{filed_date.isoformat()}'
    )
    form = str((latest_submission or {}).get('_form') or (latest_fact or {}).get('_form') or 'UNKNOWN')
    period_end = _date((latest_submission or {}).get('reportDate')) or ((latest_fact or {}).get('_end'))
    fiscal_year_raw = (latest_fact or {}).get('fy')
    fiscal_year = int(fiscal_year_raw) if str(fiscal_year_raw or '').isdigit() else None
    fiscal_period = str((latest_fact or {}).get('fp') or '').strip() or None

    assets = _latest_point(asset_rows)
    liabilities = _latest_point(liability_rows)
    latest_shares = _latest_point(share_rows)
    shares_growth: Decimal | None = None
    if latest_shares is not None and latest_shares['_value'] > 0:
        prior_cutoff = latest_shares['_end'] - timedelta(days=300)
        prior_shares = max(
            (row for row in share_rows if row['_end'] <= prior_cutoff and row['_value'] > 0),
            key=lambda row: (row['_end'], row['_filed']),
            default=None,
        )
        if prior_shares is not None:
            shares_growth = (latest_shares['_value'] / prior_shares['_value']) - Decimal('1')

    revenue = _ttm_value(revenue_rows)
    operating_cash_flow = _ttm_value(cash_flow_rows)
    net_income = _ttm_value(net_income_rows)
    metrics = USFundamentalScreenMetrics(
        symbol=symbol.strip().upper(),
        latest_periodic_filing_date=filed_date,
        ttm_revenue=revenue,
        ttm_operating_cash_flow=operating_cash_flow,
        ttm_net_income=net_income,
        total_assets=assets['_value'] if assets else None,
        total_liabilities=liabilities['_value'] if liabilities else None,
        shares_outstanding_growth_yoy=shares_growth,
        has_bankruptcy_or_delinquency_flag=flagged,
    )
    return SECFundamentalSnapshotData(
        symbol=metrics.symbol,
        cik=cik,
        accession_number=accession_number,
        form=form,
        filed_date=filed_date,
        period_end_date=period_end,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        revenue_usd=revenue,
        operating_cash_flow_usd=operating_cash_flow,
        net_income_usd=net_income,
        assets_usd=assets['_value'] if assets else None,
        liabilities_usd=liabilities['_value'] if liabilities else None,
        shares_outstanding=latest_shares['_value'] if latest_shares else None,
        metrics=metrics,
        source_payload={
            'companyfacts_entity_name': companyfacts.get('entityName'),
            'periodic_filing': {
                'accession_number': accession_number,
                'form': form,
                'filed_date': filed_date.isoformat(),
                'period_end_date': period_end.isoformat() if period_end else None,
            },
            'bankruptcy_or_delinquency_flag': flagged,
        },
    )
