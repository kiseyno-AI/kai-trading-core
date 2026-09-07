from __future__ import annotations

import re
from datetime import date, datetime
from urllib.parse import urlparse
from typing import Any

CLASSIFIER_VERSION = 'rule_based_external_news_contract_v1'
MATERIALITY_VALUES = {'material', 'noise', 'unknown'}
ACCESS_STATUS_VALUES = {'full_text', 'headline_only', 'paywalled', 'fetch_failed'}

TAG_KEYWORDS: tuple[tuple[str, tuple[str, ...], float], ...] = (
    ('earnings', ('실적', '영업이익', '순이익', '매출', '어닝', 'earnings', 'profit', 'revenue'), 0.22),
    ('guidance', ('전망', '가이던스', '목표가', '상향', '하향', 'forecast', 'guidance', 'outlook'), 0.16),
    ('contract', ('계약', '수주', '공급', '납품', 'contract', 'order', 'supply deal'), 0.20),
    ('investment', ('투자계획', '신규 투자', '증설', '공장', '설비', 'capex', 'investment plan', 'plant'), 0.18),
    ('mna', ('인수', '합병', '매각', '지분', 'm&a', 'merger', 'acquisition', 'stake sale'), 0.20),
    ('financing', ('유상증자', '전환사채', 'cb', 'bw', '차입', '회사채', '증자', 'financing', 'bond'), 0.18),
    ('regulatory', ('공시', '금감원', '거래소', '제재', '조사', '규제', '소송', 'regulator', 'lawsuit'), 0.16),
    ('executive_change', ('대표이사', 'ceo', 'cfo', '임원', '선임', '사임', 'appoint', 'resign', 'executive'), 0.16),
    ('product', ('출시', '신제품', '승인', '허가', '임상', '특허', 'launch', 'approval', 'patent'), 0.14),
    ('supply_chain', ('공급망', '원자재', '재고', '부품', '납기', 'supply chain', 'inventory'), 0.12),
    ('macro_sector', ('환율', '금리', '유가', '반도체', '배터리', '조선', '바이오', '테마', 'fx', 'rates'), 0.10),
    ('analyst', ('리포트', '증권', '투자의견', '목표주가', 'analyst', 'rating', 'target price'), 0.10),
    ('rumor', ('루머', '소문', '풍문', 'rumor', 'unconfirmed', 'speculation'), 0.05),
)

POSITIVE_KEYWORDS = (
    '상승', '강세', '호조', '개선', '증가', '성장', '흑자', '수혜', '돌파', '최대', '상향', '성공',
    'beat', 'growth', 'positive', 'surge', 'record', 'upgrade', 'benefit', 'improve', 'profit'
)
NEGATIVE_KEYWORDS = (
    '하락', '약세', '부진', '감소', '적자', '손실', '우려', '리스크', '하향', '취소', '소송', '제재', '급락',
    'miss', 'loss', 'negative', 'risk', 'downgrade', 'lawsuit', 'probe', 'fall', 'decline'
)
URGENT_KEYWORDS = ('긴급', '단독', '속보', '공시', '조회공시', '거래정지', '상한가', '하한가', 'breaking')

_WORD_RE = re.compile(r'\s+')
_PAYWALL_RE = re.compile(r'paywall|subscription|subscriber|premium|유료|구독|회원전용', re.IGNORECASE)

EVENT_TYPE_BY_TAG = {
    'earnings': 'earnings',
    'guidance': 'guidance',
    'contract': 'contract',
    'investment': 'capital_investment',
    'mna': 'mna',
    'financing': 'financing',
    'regulatory': 'regulatory',
    'executive_change': 'executive_change',
    'product': 'product_launch_or_approval',
    'supply_chain': 'supply_chain',
    'macro_sector': 'macro_sector',
    'analyst': 'analyst_rating',
    'rumor': 'rumor',
}
MATERIAL_EVENT_TYPES = {
    'earnings',
    'guidance',
    'contract',
    'capital_investment',
    'mna',
    'financing',
    'regulatory',
    'executive_change',
    'product_launch_or_approval',
}


def _normalize_text(*values: Any) -> str:
    return _WORD_RE.sub(' ', ' '.join(str(value or '') for value in values).lower()).strip()


def _normalize_iso_datetime(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def _extract_published_at(*, payload: dict[str, Any] | None, published_at: Any) -> str | None:
    normalized = _normalize_iso_datetime(published_at)
    if normalized:
        return normalized
    if isinstance(payload, dict):
        for container in (payload, payload.get('raw') if isinstance(payload.get('raw'), dict) else {}):
            for key in ('published_at', 'publication_date', 'pubDate', 'date'):
                value = container.get(key)
                if value:
                    return str(value).strip()
    return None


def _publication_from_url(value: Any) -> str | None:
    if not value:
        return None
    try:
        parsed = urlparse(str(value))
    except ValueError:
        return None
    host = parsed.netloc.strip().lower()
    if host.startswith('www.'):
        host = host[4:]
    return host or None


def _extract_publication(*, payload: dict[str, Any] | None, url: str | None, source_url: str | None) -> str | None:
    if isinstance(payload, dict):
        for container in (payload, payload.get('raw') if isinstance(payload.get('raw'), dict) else {}):
            for key in ('publication', 'publisher', 'source_name', 'press', 'officeName', 'media_name'):
                value = container.get(key)
                if value:
                    return str(value).strip()
    return _publication_from_url(url) or _publication_from_url(source_url)


def _resolve_access_status(*, text: str, payload: dict[str, Any] | None, url: str | None, source_url: str | None) -> str:
    payload = payload if isinstance(payload, dict) else {}
    raw = payload.get('raw') if isinstance(payload.get('raw'), dict) else {}
    status = str(payload.get('access_status') or raw.get('access_status') or '').strip().lower()
    if status in ACCESS_STATUS_VALUES:
        return status
    fetch_status = str(payload.get('fetch_status') or raw.get('fetch_status') or '').strip().lower()
    if fetch_status in {'failed', 'error', 'timeout'}:
        return 'fetch_failed'
    if payload.get('full_text') or payload.get('article_text') or payload.get('body'):
        return 'full_text'
    if payload.get('paywalled') or raw.get('paywalled') or _PAYWALL_RE.search(' '.join([text, url or '', source_url or ''])):
        return 'paywalled'
    return 'headline_only'


def _resolve_event_type(tags: list[str]) -> str:
    for tag in tags:
        event_type = EVENT_TYPE_BY_TAG.get(tag)
        if event_type:
            return event_type
    return 'generic_commentary'


def _resolve_materiality(*, event_type: str, score: float, tags: list[str], sentiment: str, access_status: str) -> tuple[str, str]:
    if event_type in MATERIAL_EVENT_TYPES and score >= 0.30:
        reason = f'Detected company-specific material event type "{event_type}" from headline/snippet keywords.'
        if access_status != 'full_text':
            reason += ' Full article text was not available, so the evidence basis is limited.'
        return 'material', reason
    if event_type == 'analyst_rating' and sentiment in {'positive', 'negative', 'mixed'} and score >= 0.35:
        reason = 'Detected analyst-rating or target-price language with directional rating context.'
        if access_status != 'full_text':
            reason += ' Treat as headline/snippet evidence, not full-report analysis.'
        return 'material', reason
    if event_type == 'rumor':
        return 'unknown', 'Detected rumor/speculation language; treat as unverified until confirmed by a primary or full-text source.'
    if not tags or tags == ['general_news']:
        return 'noise', 'No company-specific material event keywords were detected; treat as background commentary unless corroborated elsewhere.'
    if event_type in {'generic_commentary', 'macro_sector'}:
        return 'noise', f'Detected {event_type} context without a specific company event; keep only as bounded background noise.'
    return 'unknown', f'Detected {event_type} context but rule-based evidence is insufficient for material/noise classification.'


def external_news_classification_has_schema_contract(
    classification: dict[str, Any] | None,
) -> bool:
    if not isinstance(classification, dict):
        return False
    return (
        bool(classification.get('classifier'))
        and classification.get('materiality') in MATERIALITY_VALUES
        and bool(classification.get('materiality_reason'))
        and classification.get('access_status') in ACCESS_STATUS_VALUES
        and bool(classification.get('event_type'))
        and isinstance(classification.get('source_evidence'), dict)
    )


def external_news_classification_has_contract(classification: dict[str, Any] | None) -> bool:
    if not external_news_classification_has_schema_contract(classification):
        return False
    return bool(classification and classification.get('classifier') == CLASSIFIER_VERSION)


def external_news_materiality_sort_key(item: dict[str, Any]) -> tuple[int, float]:
    materiality = str(item.get('materiality') or '').strip().lower()
    rank = {'material': 0, 'unknown': 1, 'noise': 2}.get(materiality, 1)
    try:
        score = float(item.get('materiality_score') or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    return rank, -score


def classify_external_news_item(
    *,
    title: str | None,
    description: str | None,
    payload: dict[str, Any] | None = None,
    url: str | None = None,
    source_url: str | None = None,
    published_at: Any = None,
) -> dict[str, Any]:
    """Return deterministic lightweight event/sentiment/materiality labels for a news item.

    This intentionally stays rule-based so collection never depends on an LLM or external classifier.
    """
    text = _normalize_text(title, description)
    tags: list[str] = []
    score = 0.10
    matched_keywords: dict[str, list[str]] = {}

    for tag, keywords, weight in TAG_KEYWORDS:
        matches = [keyword for keyword in keywords if keyword.lower() in text]
        if not matches:
            continue
        tags.append(tag)
        matched_keywords[tag] = matches[:5]
        score += weight

    positive_hits = [keyword for keyword in POSITIVE_KEYWORDS if keyword.lower() in text]
    negative_hits = [keyword for keyword in NEGATIVE_KEYWORDS if keyword.lower() in text]
    if positive_hits and not negative_hits:
        sentiment = 'positive'
        score += 0.08
    elif negative_hits and not positive_hits:
        sentiment = 'negative'
        score += 0.08
    elif positive_hits and negative_hits:
        sentiment = 'mixed'
        score += 0.05
    else:
        sentiment = 'neutral'

    urgent_hits = [keyword for keyword in URGENT_KEYWORDS if keyword.lower() in text]
    if urgent_hits:
        score += 0.10
        matched_keywords['urgency'] = urgent_hits[:5]

    if payload and isinstance(payload.get('raw'), dict):
        raw = payload['raw']
        if raw.get('pubDate'):
            score += 0.02

    score = max(0.0, min(1.0, round(score, 2)))
    if score >= 0.65:
        materiality_bucket = 'high'
    elif score >= 0.35:
        materiality_bucket = 'medium'
    else:
        materiality_bucket = 'low'

    event_type = _resolve_event_type(tags)
    access_status = _resolve_access_status(text=text, payload=payload, url=url, source_url=source_url)
    materiality, materiality_reason = _resolve_materiality(
        event_type=event_type,
        score=score,
        tags=tags,
        sentiment=sentiment,
        access_status=access_status,
    )
    source_evidence = {
        'url': url,
        'source_url': source_url,
        'publication': _extract_publication(payload=payload, url=url, source_url=source_url),
        'published_at': _extract_published_at(payload=payload, published_at=published_at),
    }
    content_basis = 'full_text' if access_status == 'full_text' else 'headline_or_snippet_only'

    return {
        'event_tags': tags or ['general_news'],
        'event_type': event_type,
        'sentiment': sentiment,
        'materiality_score': score,
        'materiality_bucket': materiality_bucket,
        'materiality': materiality,
        'materiality_reason': materiality_reason,
        'source_evidence': source_evidence,
        'access_status': access_status,
        'content_basis': content_basis,
        'price_forecast_prohibited': True,
        'summary_policy': (
            'Use the item only as source-backed context. Do not infer price reaction from news alone. When '
            'access_status is not full_text, full article text was not read; do not summarize headline-only/paywalled '
            'items as full-text evidence.'
        ),
        'classifier': CLASSIFIER_VERSION,
        'matched_keywords': matched_keywords,
    }
