from __future__ import annotations

from typing import Any

from app.signals.market_risk_snapshot import classify_vi_code

MARKET_RISK_FILTER_SOURCE = 'kis_market_risk_v1'
BLOCKING_MARKET_WARNING_CODES = frozenset({'02', '03'})
WATCHLIST_MARKET_WARNING_CODES = frozenset({'01'})


def _append_reason(target: list[str], reason: str, *, when: bool) -> None:
    if when and reason not in target:
        target.append(reason)


def _risk_state(*, passed_filter: bool, watchlist_flagged: bool) -> str:
    if not passed_filter:
        return 'fail'
    if watchlist_flagged:
        return 'watchlist'
    return 'pass'


def evaluate_market_warning_reasons(snapshot: dict[str, Any]) -> tuple[list[str], list[str]]:
    blocking: list[str] = []
    watchlist: list[str] = []
    market_warning_code = snapshot.get('market_warning_code')
    _append_reason(
        blocking,
        f'market_warning_{market_warning_code}',
        when=market_warning_code in BLOCKING_MARKET_WARNING_CODES,
    )
    _append_reason(
        watchlist,
        f'market_warning_{market_warning_code}',
        when=market_warning_code in WATCHLIST_MARKET_WARNING_CODES,
    )
    return blocking, watchlist


def evaluate_management_item_reasons(snapshot: dict[str, Any]) -> tuple[list[str], list[str]]:
    blocking: list[str] = []
    _append_reason(blocking, 'management_item', when=bool(snapshot.get('is_management_item')))
    return blocking, []


def evaluate_volatility_interruption_reasons(snapshot: dict[str, Any]) -> tuple[list[str], list[str]]:
    watchlist: list[str] = []
    snapshot_payload = snapshot.get('snapshot_payload')
    payload_status = snapshot_payload.get('vi_evidence_status') if isinstance(snapshot_payload, dict) else None
    _, inferred_status = classify_vi_code(snapshot.get('vi_class_code'))
    evidence_status = snapshot.get('vi_evidence_status') or payload_status or inferred_status
    is_triggered = evidence_status == 'active'
    _append_reason(watchlist, 'volatility_interruption', when=is_triggered)
    _append_reason(
        watchlist,
        'volatility_interruption_evidence_unknown',
        when=evidence_status == 'unknown',
    )
    return [], watchlist


def evaluate_disclosure_reasons(snapshot: dict[str, Any]) -> tuple[list[str], list[str]]:
    blocking: list[str] = []
    _append_reason(blocking, 'dishonest_disclosure', when=bool(snapshot.get('is_dishonest_disclosure')))
    return blocking, []


def evaluate_trade_halt_reasons(snapshot: dict[str, Any]) -> tuple[list[str], list[str]]:
    blocking: list[str] = []
    watchlist: list[str] = []
    _append_reason(blocking, 'trade_halted', when=bool(snapshot.get('is_trade_halted')))
    return blocking, watchlist


def evaluate_watchlist_signal_reasons(snapshot: dict[str, Any]) -> tuple[list[str], list[str]]:
    watchlist: list[str] = []
    _append_reason(watchlist, 'investment_caution', when=bool(snapshot.get('is_investment_caution')))
    return [], watchlist


def evaluate_short_overheat_reasons(snapshot: dict[str, Any]) -> tuple[list[str], list[str]]:
    watchlist: list[str] = []
    _append_reason(watchlist, 'short_overheated', when=bool(snapshot.get('is_short_overheated')))
    _append_reason(watchlist, 'short_sale_overheated', when=bool(snapshot.get('is_short_sale_overheated')))
    return [], watchlist


def evaluate_market_risk_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    blocking_reasons: list[str] = []
    watchlist_reasons: list[str] = []
    for evaluator in (
        evaluate_management_item_reasons,
        evaluate_market_warning_reasons,
        evaluate_watchlist_signal_reasons,
        evaluate_short_overheat_reasons,
        evaluate_volatility_interruption_reasons,
        evaluate_disclosure_reasons,
        evaluate_trade_halt_reasons,
    ):
        blocking, watchlist = evaluator(snapshot)
        for reason in blocking:
            _append_reason(blocking_reasons, reason, when=True)
        for reason in watchlist:
            _append_reason(watchlist_reasons, reason, when=True)

    passed_filter = not blocking_reasons
    watchlist_flagged = bool(watchlist_reasons)
    return {
        'blocking_reasons': blocking_reasons,
        'watchlist_reasons': watchlist_reasons,
        'passed_filter': passed_filter,
        'watchlist_flagged': watchlist_flagged,
        'risk_state': _risk_state(
            passed_filter=passed_filter,
            watchlist_flagged=watchlist_flagged,
        ),
    }


def build_market_risk_filter_result_from_snapshot(
    snapshot: dict[str, Any],
    *,
    source: str = MARKET_RISK_FILTER_SOURCE,
    eod_run_id: int | None = None,
) -> dict[str, Any]:
    evaluation = evaluate_market_risk_snapshot(snapshot)
    snapshot_payload = snapshot.get('snapshot_payload')
    vi_evidence_status = snapshot.get('vi_evidence_status')
    if vi_evidence_status is None and isinstance(snapshot_payload, dict):
        vi_evidence_status = snapshot_payload.get('vi_evidence_status')
    if vi_evidence_status is None:
        _, vi_evidence_status = classify_vi_code(snapshot.get('vi_class_code'))
    return {
        'symbol': snapshot['symbol'],
        'name_ko': snapshot.get('name_ko'),
        'market': snapshot.get('market') or 'KRX',
        'eod_run_id': eod_run_id if eod_run_id is not None else snapshot.get('eod_run_id'),
        'market_date': snapshot['market_date'],
        'source': source,
        'feature_version': source,
        'risk_state': evaluation['risk_state'],
        'passed_filter': evaluation['passed_filter'],
        'watchlist_flagged': evaluation['watchlist_flagged'],
        'filter_payload': {
            'feature_version': source,
            'snapshot_source': snapshot.get('source'),
            'snapshot_feature_version': snapshot.get('feature_version') or snapshot.get('source'),
            'snapshot_market_date': snapshot.get('market_date'),
            'snapshot_market_code': snapshot.get('market_code'),
            'snapshot_source_endpoint': snapshot.get('source_endpoint'),
            'snapshot_captured_at': snapshot.get('captured_at'),
            'vi_evidence_status': vi_evidence_status,
            'blocking_reasons': evaluation['blocking_reasons'],
            'watchlist_reasons': evaluation['watchlist_reasons'],
        },
        '_snapshot_fields': {
            'market_warning_code': snapshot.get('market_warning_code'),
            'market_warning_name': snapshot.get('market_warning_name'),
            'is_management_item': bool(snapshot.get('is_management_item')),
            'is_investment_caution': bool(snapshot.get('is_investment_caution')),
            'is_short_overheated': bool(snapshot.get('is_short_overheated')),
            'short_overheat_class_code': snapshot.get('short_overheat_class_code'),
            'is_short_sale_overheated': bool(snapshot.get('is_short_sale_overheated')),
            'vi_class_code': snapshot.get('vi_class_code'),
            'is_vi_triggered': bool(snapshot.get('is_vi_triggered')),
            'is_dishonest_disclosure': bool(snapshot.get('is_dishonest_disclosure')),
            'is_trade_halted': bool(snapshot.get('is_trade_halted')),
        },
    }
