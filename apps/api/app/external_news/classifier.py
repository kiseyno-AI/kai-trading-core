from __future__ import annotations

import re
from typing import Any

TAG_KEYWORDS: tuple[tuple[str, tuple[str, ...], float], ...] = (
    ('earnings', ('실적', '영업이익', '순이익', '매출', '어닝', 'earnings', 'profit', 'revenue'), 0.22),
    ('guidance', ('전망', '가이던스', '목표가', '상향', '하향', 'forecast', 'guidance', 'outlook'), 0.16),
    ('contract', ('계약', '수주', '공급', '납품', 'contract', 'order', 'supply deal'), 0.20),
    ('investment', ('투자', '증설', '공장', '설비', 'capex', 'investment', 'plant'), 0.18),
    ('mna', ('인수', '합병', '매각', '지분', 'm&a', 'merger', 'acquisition', 'stake sale'), 0.20),
    ('financing', ('유상증자', '전환사채', 'cb', 'bw', '차입', '회사채', '증자', 'financing', 'bond'), 0.18),
    ('regulatory', ('공시', '금감원', '거래소', '제재', '조사', '규제', '소송', 'regulator', 'lawsuit'), 0.16),
    ('product', ('출시', '신제품', '승인', '허가', '임상', '특허', 'launch', 'approval', 'patent'), 0.14),
    ('supply_chain', ('공급망', '원자재', '재고', '부품', '납기', 'supply chain', 'inventory'), 0.12),
    ('macro_sector', ('환율', '금리', '유가', '반도체', '배터리', '조선', '바이오', '테마', 'fx', 'rates'), 0.10),
    ('analyst', ('리포트', '증권', '투자의견', '목표주가', 'analyst', 'rating', 'target price'), 0.10),
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


def _normalize_text(*values: Any) -> str:
    return _WORD_RE.sub(' ', ' '.join(str(value or '') for value in values).lower()).strip()


def classify_external_news_item(*, title: str | None, description: str | None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
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

    return {
        'event_tags': tags or ['general_news'],
        'sentiment': sentiment,
        'materiality_score': score,
        'materiality_bucket': materiality_bucket,
        'classifier': 'rule_based_external_news_v1',
        'matched_keywords': matched_keywords,
    }
