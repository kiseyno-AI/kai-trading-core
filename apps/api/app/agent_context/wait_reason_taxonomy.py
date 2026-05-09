from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME


WAIT_REASON_BUCKET_SETUP = 'setup'
WAIT_REASON_BUCKET_EXECUTION = 'execution'
WAIT_REASON_BUCKET_DATA_GAP = 'data_gap'

WAIT_REASON_ISSUE_MISSING = 'wait_reason_missing_for_wait_action'
WAIT_REASON_ISSUE_GENERIC = 'generic_wait_reason_missing_specific_blocker'

INSUFFICIENT_LIQUIDITY_PATTERNS = (
    re.compile(r'\billiquid\b'),
    re.compile(r'\b(?:low|poor|weak|thin|insufficient)\s+(?:liquidity|turnover|volume)\b'),
    re.compile(r'\b(?:liquidity|turnover)\b(?:\s+\w+){0,3}\s+(?:too\s+)?(?:low|poor|weak|thin|insufficient)\b'),
    re.compile(r'\b(?:high|elevated|large|excessive)\s+slippage\b'),
    re.compile(r'\bslippage\b(?:\s+\w+){0,3}\s+(?:too\s+)?(?:high|elevated|large|excessive)\b'),
    re.compile(r'\b(?:wide|too\s+wide)\s+spread\b'),
    re.compile(r'\bspread\b(?:\s+\w+){0,3}\s+(?:too\s+)?wide\b'),
    re.compile(r'\b(?:execution\s+)?liquidity gap\b'),
    re.compile(r'유동성 부족'),
    re.compile(r'유동성이\s*(?:약|낮|부족)'),
    re.compile(r'거래대금(?:이)?\s*(?:약|낮|부족)'),
    re.compile(r'슬리피지(?:가)?\s*(?:크|높)'),
    re.compile(r'호가 공백'),
)


@dataclass(frozen=True)
class WaitReasonDefinition:
    code: str
    label: str
    bucket: str
    blocking_by_default: bool
    default_summary: str
    keywords: tuple[str, ...]
    aliases: tuple[str, ...] = ()


WAIT_REASON_DEFINITIONS: tuple[WaitReasonDefinition, ...] = (
    WaitReasonDefinition(
        code='extended_entry_price',
        label='Extended entry price',
        bucket=WAIT_REASON_BUCKET_SETUP,
        blocking_by_default=True,
        default_summary='Entry is too extended versus the preferred trigger or support zone.',
        keywords=(
            'extended entry',
            'entry is extended',
            'poor entry',
            'bad entry',
            'entry too high',
            'too extended',
            'overextended',
            'stretched',
            'chasing',
            'chase risk',
            'gap too far',
            'far above support',
            '추격',
            '과열',
            '진입 가격 부담',
            '진입이 너무 멀다',
        ),
        aliases=('poor_entry_price', 'extended_price', 'avoid_chase'),
    ),
    WaitReasonDefinition(
        code='unattractive_risk_reward',
        label='Unattractive risk-reward',
        bucket=WAIT_REASON_BUCKET_SETUP,
        blocking_by_default=True,
        default_summary='Upside does not compensate for the required stop distance or downside risk.',
        keywords=(
            'risk-reward',
            'reward/risk',
            'risk reward',
            'upside is limited',
            'stop is too wide',
            'poor rr',
            '손익비',
            '리스크 대비 보상',
            'reward does not justify',
        ),
        aliases=('poor_risk_reward', 'bad_rr'),
    ),
    WaitReasonDefinition(
        code='insufficient_liquidity',
        label='Insufficient liquidity/turnover',
        bucket=WAIT_REASON_BUCKET_EXECUTION,
        blocking_by_default=True,
        default_summary='Liquidity and turnover are too weak for reliable execution.',
        keywords=(
            'illiquid',
            'low liquidity',
            'poor liquidity',
            'weak liquidity',
            'thin liquidity',
            'insufficient liquidity',
            'low turnover',
            'poor turnover',
            'weak turnover',
            'thin turnover',
            'insufficient turnover',
            'thin volume',
            'low volume',
            'high slippage',
            'slippage is too high',
            'spread is too wide',
            'wide spread',
            'liquidity gap',
            'execution liquidity gap',
            '거래대금 부족',
            '유동성 부족',
            '슬리피지가 크다',
            '호가 공백',
        ),
        aliases=('low_liquidity', 'liquidity_turnover'),
    ),
    WaitReasonDefinition(
        code='stale_quote_orderbook',
        label='Stale quote/orderbook freshness',
        bucket=WAIT_REASON_BUCKET_DATA_GAP,
        blocking_by_default=True,
        default_summary='Realtime quote or orderbook freshness is too stale to trust the entry.',
        keywords=(
            'stale quote',
            'stale orderbook',
            'orderbook freshness',
            'quote freshness',
            'stale realtime',
            'delayed quote',
            'outdated orderbook',
            'bid/ask',
            'order book',
            'orderbook',
            'quote is stale',
            '호가',
            '실시간 시세',
            '체결 신선도',
            '시세 지연',
        ),
        aliases=('stale_quotes', 'orderbook_freshness', 'stale_realtime_context'),
    ),
    WaitReasonDefinition(
        code='high_volatility_drawdown_risk',
        label='High volatility/drawdown risk',
        bucket=WAIT_REASON_BUCKET_EXECUTION,
        blocking_by_default=True,
        default_summary='Volatility and drawdown risk are too high for a controlled entry.',
        keywords=(
            'volatility',
            'too volatile',
            'drawdown',
            'whipsaw',
            'atr',
            'swing risk',
            '변동성',
            '낙폭',
            '흔들림',
            '변동성 리스크',
        ),
        aliases=('high_volatility', 'drawdown_risk'),
    ),
    WaitReasonDefinition(
        code='weak_trend_relative_strength',
        label='Weak trend/relative strength',
        bucket=WAIT_REASON_BUCKET_SETUP,
        blocking_by_default=True,
        default_summary='Trend and relative-strength evidence are too weak to support the entry.',
        keywords=(
            'weak trend',
            'trend is weak',
            'relative strength',
            'weak momentum',
            'momentum is weak',
            'breakout not confirmed',
            'trend confirmation is weak',
            '추세가 약하다',
            '상대강도',
            '모멘텀 약화',
            '돌파 확인 부족',
        ),
        aliases=('weak_trend', 'weak_relative_strength', 'weak_momentum'),
    ),
    WaitReasonDefinition(
        code='unsuitable_execution_timing',
        label='Unsuitable execution timing',
        bucket=WAIT_REASON_BUCKET_SETUP,
        blocking_by_default=True,
        default_summary='Execution timing is not yet favorable for the setup.',
        keywords=(
            'watch for trigger',
            'wait for trigger',
            'entry timing',
            'execution timing',
            'opening auction',
            'too early',
            'not the right session',
            'session timing',
            'trigger not confirmed',
            '장초반',
            '시가 변동성',
            '타이밍',
            '트리거 확인',
            '진입 시점',
        ),
        aliases=('timing', 'bad_timing', 'watch_trigger'),
    ),
    WaitReasonDefinition(
        code='core_execution_evidence_gap',
        label='Core execution evidence gap',
        bucket=WAIT_REASON_BUCKET_DATA_GAP,
        blocking_by_default=True,
        default_summary='A required core execution input is missing or unusable for the entry.',
        keywords=(
            'execution context missing',
            'missing execution context',
            'core execution evidence',
            'core blocker',
            'session state missing',
            'intraday context missing',
            'quote context missing',
            'missing quote context',
            'missing orderbook',
            '실행 컨텍스트 부족',
            '핵심 실행 근거 부족',
            '세션 상태 부족',
            '장중 컨텍스트 부족',
        ),
        aliases=('execution_gap', 'core_data_gap', 'missing_core_execution_evidence'),
    ),
)

WAIT_REASON_DEFINITIONS_BY_CODE = {definition.code: definition for definition in WAIT_REASON_DEFINITIONS}
WAIT_REASON_ALIASES = {
    alias: definition.code
    for definition in WAIT_REASON_DEFINITIONS
    for alias in (definition.code, *definition.aliases)
}
WAIT_REASON_PRIORITY = tuple(definition.code for definition in WAIT_REASON_DEFINITIONS)

WAIT_REASON_REQUIRED_ACTIONS: dict[str, set[str]] = {
    SIGNAL_DECIDER_AGENT_NAME: {'wait'},
    SHORT_TERM_DECIDER_AGENT_NAME: {'watch_for_trigger', 'avoid_chase', 'wait'},
}

GENERIC_WAIT_PHRASES = (
    'insufficient information',
    'not enough information',
    'need more data',
    'need additional data',
    'need more confirmation',
    'wait for more confirmation',
    'wait for confirmation',
    'more confirmation is needed',
    'insufficient context',
    'lack of information',
    '정보 부족',
    '데이터 부족',
    '추가 데이터 필요',
    '추가 확인 필요',
    '확인 더 필요',
    '근거 부족',
)

CORE_GAP_KEYWORDS = (
    'quote',
    'orderbook',
    'order book',
    'bid/ask',
    'execution context',
    'session state',
    'intraday',
    'realtime',
    'real-time',
    'liquidity',
    'turnover',
    'slippage',
    'spread',
    '호가',
    '실시간',
    '유동성',
    '거래대금',
    '슬리피지',
)


def _normalize_text(value: Any) -> str:
    return re.sub(r'\s+', ' ', str(value or '').strip()).lower()


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _dedupe_texts(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or '').strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped


def wait_reason_required_for_action(*, role: str, action: str | None) -> bool:
    normalized_action = str(action or '').strip().lower()
    return normalized_action in WAIT_REASON_REQUIRED_ACTIONS.get(role, set())


def normalize_wait_reason_code(value: Any) -> str | None:
    normalized = _normalize_text(value).replace('-', '_').replace(' ', '_')
    if not normalized:
        return None
    return WAIT_REASON_ALIASES.get(normalized)


def wait_reason_taxonomy_codes() -> list[str]:
    return list(WAIT_REASON_PRIORITY)


def _contains_generic_wait_phrase(text: str) -> bool:
    normalized = _normalize_text(text)
    return any(phrase in normalized for phrase in GENERIC_WAIT_PHRASES)


def _contains_core_gap_keyword(text: str) -> bool:
    normalized = _normalize_text(text)
    return any(keyword in normalized for keyword in CORE_GAP_KEYWORDS)


def _extract_explicit_wait_reason(payload: dict[str, Any] | None) -> tuple[str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None

    explicit_code = _first_text(
        payload.get('primary_wait_reason'),
        payload.get('wait_reason_code'),
    )
    explicit_summary = _first_text(
        payload.get('wait_reason_summary'),
        payload.get('wait_reason_explanation'),
        payload.get('actionable_wait_reason'),
    )

    wait_reason = payload.get('wait_reason')
    if isinstance(wait_reason, dict):
        explicit_code = _first_text(
            wait_reason.get('code'),
            wait_reason.get('reason_code'),
            wait_reason.get('key'),
            explicit_code,
        )
        explicit_summary = _first_text(
            wait_reason.get('summary'),
            wait_reason.get('message'),
            wait_reason.get('explanation'),
            wait_reason.get('detail'),
            explicit_summary,
        )
    elif isinstance(wait_reason, str):
        if normalize_wait_reason_code(wait_reason) is not None:
            explicit_code = wait_reason
        else:
            explicit_summary = _first_text(wait_reason, explicit_summary)

    details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
    for nested_key in ('decision_support', 'tactical_decision_support'):
        nested = details.get(nested_key) if isinstance(details.get(nested_key), dict) else {}
        nested_wait_reason = nested.get('wait_reason')
        if isinstance(nested_wait_reason, dict):
            explicit_code = _first_text(
                nested_wait_reason.get('code'),
                nested_wait_reason.get('reason_code'),
                explicit_code,
            )
            explicit_summary = _first_text(
                nested_wait_reason.get('summary'),
                nested_wait_reason.get('message'),
                explicit_summary,
            )
        elif isinstance(nested_wait_reason, str):
            if normalize_wait_reason_code(nested_wait_reason) is not None:
                explicit_code = nested_wait_reason
            else:
                explicit_summary = _first_text(nested_wait_reason, explicit_summary)

    return normalize_wait_reason_code(explicit_code), explicit_summary


def _collect_candidate_texts(payload: dict[str, Any] | None) -> list[str]:
    if not isinstance(payload, dict):
        return []

    texts: list[str] = []
    for key in (
        'wait_reason_summary',
        'wait_reason_explanation',
        'actionable_wait_reason',
        'thesis_summary',
        'summary',
    ):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            texts.append(value.strip())

    for list_key in (
        'decision_risks',
        'tactical_risks',
        'invalidation_conditions',
        'data_quality_warnings',
        'risk_flags',
    ):
        value = payload.get(list_key)
        if isinstance(value, list):
            texts.extend(str(item).strip() for item in value if str(item).strip())
        elif isinstance(value, str) and value.strip():
            texts.append(value.strip())

    details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
    for nested_key in ('decision_support', 'tactical_decision_support'):
        nested = details.get(nested_key) if isinstance(details.get(nested_key), dict) else {}
        nested_wait_reason = nested.get('wait_reason')
        if isinstance(nested_wait_reason, dict):
            texts.extend(
                str(value).strip()
                for value in (
                    nested_wait_reason.get('summary'),
                    nested_wait_reason.get('message'),
                    nested_wait_reason.get('explanation'),
                )
                if str(value or '').strip()
            )

    return _dedupe_texts(texts)


def _text_matches_definition(*, definition: WaitReasonDefinition, text: str) -> bool:
    normalized = _normalize_text(text)
    if definition.code == 'insufficient_liquidity':
        return any(pattern.search(normalized) for pattern in INSUFFICIENT_LIQUIDITY_PATTERNS)
    return any(keyword in normalized for keyword in definition.keywords)


def _infer_wait_reason_code(texts: list[str]) -> str | None:
    normalized_texts = [_normalize_text(text) for text in texts if str(text or '').strip()]
    for code in WAIT_REASON_PRIORITY:
        definition = WAIT_REASON_DEFINITIONS_BY_CODE[code]
        for text in normalized_texts:
            if _text_matches_definition(definition=definition, text=text):
                return code
    return None


def derive_wait_reason(
    *,
    role: str,
    action: str | None,
    payload: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, list[str]]:
    if not wait_reason_required_for_action(role=role, action=action):
        return None, []

    candidate_texts = _collect_candidate_texts(payload)
    explicit_code, explicit_summary = _extract_explicit_wait_reason(payload)
    inferred_code = explicit_code or _infer_wait_reason_code(candidate_texts)
    generic_detected = any(_contains_generic_wait_phrase(text) for text in candidate_texts)

    if inferred_code is None and generic_detected and any(_contains_core_gap_keyword(text) for text in candidate_texts):
        inferred_code = 'core_execution_evidence_gap'

    issues: list[str] = []
    if inferred_code is None:
        issues.append(WAIT_REASON_ISSUE_MISSING)
        if generic_detected:
            issues.append(WAIT_REASON_ISSUE_GENERIC)
        return None, issues

    definition = WAIT_REASON_DEFINITIONS_BY_CODE[inferred_code]
    summary = _first_text(explicit_summary)
    if summary is None or _contains_generic_wait_phrase(summary):
        matched_text = next(
            (
                text
                for text in candidate_texts
                if not _contains_generic_wait_phrase(text)
                and _text_matches_definition(definition=definition, text=text)
            ),
            None,
        )
        summary = matched_text or definition.default_summary

    source = 'explicit' if explicit_code is not None else 'inferred'
    return (
        {
            'code': definition.code,
            'label': definition.label,
            'bucket': definition.bucket,
            'blocking_by_default': definition.blocking_by_default,
            'summary': summary,
            'source': source,
        },
        issues,
    )
