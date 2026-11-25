from collections.abc import Sequence
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext

from app.internal.exact_repair_manifest import canonical_json_sha256
from app.market.us_index_input_freeze import USIndexInputFreezeRecord, _validate_snapshots
from app.market.us_index_prices import SUPPORTED_US_INDEX_CODES
from app.market.us_index_snapshot_store import _utc


REFERENCE_WINDOW = 20


def _prices(record: USIndexInputFreezeRecord):
    as_of, selected, confirmed, cutoff = (
        _utc(getattr(record, name), name) for name in ('as_of', 'selected_at', 'confirmed_at', 'cutoff_at')
    )
    if not as_of <= selected <= confirmed < cutoff:
        raise ValueError('US index features require a valid pre-cutoff input confirmation')
    if len(record.snapshots) != len(SUPPORTED_US_INDEX_CODES):
        raise ValueError('US index features require all three frozen indices')
    _validate_snapshots(record.snapshots, record.as_of, record.selected_at, record.source_environment)
    for snapshot in record.snapshots:
        for value in (snapshot.prices.price, snapshot.prices.previous_price):
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError('US index features require finite positive closing prices')
    return {snapshot.prices.index_code: snapshot.prices for snapshot in record.snapshots}


def _text(value: Decimal) -> str:
    return format(value.normalize(), 'f')


def _return(prices) -> Decimal:
    # Subtract first so a known tiny move is not rounded to zero near a ratio of one.
    return (prices.price - prices.previous_price) / prices.previous_price * 100


def _history_prices(current, history):
    if len(history) > REFERENCE_WINDOW:
        raise ValueError('At most 20 explicitly selected historical inputs are supported')
    records = (*history, current)
    prices = [_prices(record) for record in records]
    for older, newer, left, right in zip(records, records[1:], prices, prices[1:]):
        if (
            older.source_environment != current.source_environment
            or older.target_trade_date >= newer.target_trade_date or older.cutoff_at > newer.as_of
            or left['SPX'].session.session_date >= right['SPX'].session.session_date
        ):
            raise ValueError('History must be distinct, chronological, same-environment and strictly prior')
    chain = []
    newer = prices[-1]
    for older in reversed(prices[:-1]):
        if older['SPX'].session.session_date != newer['SPX'].session.previous_session_date:
            break
        if older['SPX'].session.close_at != newer['SPX'].session.previous_close_at:
            raise ValueError('Historical US session window mismatch')
        if any(older[code].price != newer[code].previous_price for code in SUPPORTED_US_INDEX_CODES):
            raise ValueError('Historical closing price continuity mismatch')
        chain.append(older)
        newer = older
    reason = 'nonconsecutive_history' if len(chain) != len(history) else 'insufficient_history'
    return prices[-1], chain, None if len(chain) == REFERENCE_WINDOW else reason


def calculate_us_index_features(
    current: USIndexInputFreezeRecord, *, history: Sequence[USIndexInputFreezeRecord] = (),
) -> dict:
    """Calculate descriptive price-index features from validated sealed input."""
    history = tuple(history)
    with localcontext(Context(prec=34, rounding=ROUND_HALF_EVEN)):
        prices, prior, reason = _history_prices(current, history)
        returns = {code: _return(item) for code, item in prices.items()}
        indices = {}
        for code, value in returns.items():
            item = {
                'return_pct': _text(value),
                'acceleration_pp': _text(value - _return(prior[0][code])) if prior else None,
                'reference_mean_pct': None, 'reference_sample_std_pct': None,
                'shock_z': None, 'shock_unavailable_reason': reason,
                'percentile_pct': None,
            }
            if reason is None:
                observed = [_return(prices[code]) for prices in prior]
                mean = sum(observed, Decimal(0)) / REFERENCE_WINDOW
                deviation = (sum(((x - mean) ** 2 for x in observed), Decimal(0)) / (REFERENCE_WINDOW - 1)).sqrt()
                item.update({
                    'reference_mean_pct': _text(mean), 'reference_sample_std_pct': _text(deviation),
                    'shock_z': _text((value - mean) / deviation) if deviation else None,
                    'shock_unavailable_reason': None if deviation else 'zero_reference_volatility',
                    'percentile_pct': _text(Decimal(sum(x <= value for x in observed)) * 100 / REFERENCE_WINDOW),
                })
            indices[code] = item
        result = {
            'status': 'computed', 'schema_version': 'us_index_features_v1',
            'target_trade_date': current.target_trade_date.isoformat(),
            'source_environment': current.source_environment,
            'us_session_date': prices['SPX'].session.session_date.isoformat(),
            'previous_us_session_date': prices['SPX'].session.previous_session_date.isoformat(),
            'input_manifest_id': current.manifest_id,
            'history_inputs': [
                {'target_trade_date': record.target_trade_date.isoformat(), 'manifest_id': record.manifest_id}
                for record in history
            ],
            'reference_window': REFERENCE_WINDOW,
            'history_count': len(prior),
            'history_unavailable_reason': reason,
            'indices': indices,
            'return_spreads_pp': {
                f'{left}-{right}': _text(returns[left] - returns[right])
                for left, right in (('COMP', 'SPX'), ('SOX', 'SPX'), ('SOX', 'COMP'))
            },
        }
        return {**result, 'feature_id': canonical_json_sha256(result)}
