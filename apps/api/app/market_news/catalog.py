"""Market and business-line news topics collected from NAVER News Search.

Each topic is one query. ``business_lines`` names the business-line keys the
topic speaks to (the taxonomy in ``app/market/business_lines.py`` uses the
same keys); market-wide topics leave it empty. Regions describe the intended
scope of the query, not where the publisher is.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketNewsTopic:
    key: str
    region: str
    query: str
    business_lines: tuple[str, ...] = ()


def _t(key: str, region: str, query: str, *lines: str) -> MarketNewsTopic:
    return MarketNewsTopic(key=key, region=region, query=query, business_lines=lines)


MARKET_NEWS_TOPICS: tuple[MarketNewsTopic, ...] = (
    # Korean market and policy
    _t('kr_kospi', 'KR', '코스피'),
    _t('kr_kosdaq', 'KR', '코스닥'),
    _t('kr_base_rate', 'KR', '한국은행 기준금리', 'banks', 'insurance', 'securities'),
    _t('kr_usdkrw', 'KR', '원달러 환율'),
    _t('kr_foreign_flows', 'KR', '외국인 순매수'),
    # United States
    _t('us_equities', 'US', '미국 증시'),
    _t('us_fed', 'US', '연준 금리'),
    _t('us_inflation', 'US', '미국 물가'),
    _t('us_jobs', 'US', '미국 고용'),
    _t('us_big_tech', 'US', '미국 빅테크', 'internet_platform', 'semiconductor_memory'),
    # Global
    _t('global_oil', 'GLOBAL', '국제유가', 'refining_chemicals', 'transport_logistics'),
    _t('global_tariffs', 'GLOBAL', '관세 무역', 'autos_parts', 'steel_metals'),
    _t('global_china', 'GLOBAL', '중국 경제', 'steel_metals', 'cosmetics', 'refining_chemicals'),
    _t('global_europe', 'GLOBAL', '유럽 경제'),
    _t('global_geopolitics', 'GLOBAL', '지정학 리스크', 'defense'),
    # Korean business lines
    _t('line_semiconductor_exports', 'KR', '반도체 수출', 'semiconductor_memory', 'semiconductor_equipment_materials'),
    _t('line_hbm', 'KR', 'HBM 메모리', 'semiconductor_memory', 'semiconductor_equipment_materials'),
    _t('line_semiconductor_equipment', 'KR', '반도체 장비 소부장', 'semiconductor_equipment_materials'),
    _t('line_ai_datacenter', 'KR', 'AI 데이터센터', 'semiconductor_memory', 'electrical_equipment', 'it_services'),
    _t('line_display', 'KR', '디스플레이 OLED', 'display'),
    _t('line_battery', 'KR', '2차전지 배터리', 'battery_cells', 'battery_materials'),
    _t('line_battery_materials', 'KR', '양극재 리튬', 'battery_materials'),
    _t('line_shipbuilding', 'KR', '조선 수주', 'shipbuilding'),
    _t('line_defense', 'KR', '방산 수출', 'defense'),
    _t('line_nuclear', 'KR', '원전 수출', 'nuclear_power'),
    _t('line_power_grid', 'KR', '전력기기 변압기', 'electrical_equipment'),
    _t('line_autos', 'KR', '자동차 수출', 'autos_parts'),
    _t('line_biotech_licensing', 'KR', '바이오 기술수출', 'biotech'),
    _t('line_clinical', 'KR', '신약 임상', 'biotech', 'pharma'),
    _t('line_cosmetics', 'KR', '화장품 수출', 'cosmetics'),
    _t('line_entertainment', 'KR', '엔터 K팝', 'entertainment_media'),
    _t('line_games', 'KR', '게임 신작', 'games'),
    _t('line_internet', 'KR', '네이버 카카오 플랫폼', 'internet_platform'),
    _t('line_steel', 'KR', '철강 가격', 'steel_metals'),
    _t('line_refining', 'KR', '정제마진 정유', 'refining_chemicals'),
    _t('line_petrochemicals', 'KR', '석유화학 업황', 'refining_chemicals'),
    _t('line_construction', 'KR', '건설 수주 부동산', 'construction'),
    _t('line_banks', 'KR', '은행 실적', 'banks'),
    _t('line_securities', 'KR', '증권 거래대금', 'securities'),
    _t('line_shipping', 'KR', '해운 운임', 'transport_logistics'),
    _t('line_airlines', 'KR', '항공 여객', 'transport_logistics'),
    _t('line_telecom', 'KR', '통신 요금', 'telecom'),
    _t('line_food', 'KR', '음식료 가격 인상', 'food_beverage'),
    _t('line_retail', 'KR', '소비 유통 실적', 'retail_consumer'),
    _t('line_robots', 'KR', '로봇 산업', 'machinery'),
)

TOPICS_BY_KEY: dict[str, MarketNewsTopic] = {topic.key: topic for topic in MARKET_NEWS_TOPICS}


def resolve_topics(keys: list[str] | None = None) -> list[MarketNewsTopic]:
    """Catalog order; ``None`` means every topic. Unknown keys are an error."""

    if keys is None:
        return list(MARKET_NEWS_TOPICS)
    unknown = sorted(set(keys) - TOPICS_BY_KEY.keys())
    if unknown:
        raise ValueError(f"unknown market news topics: {', '.join(unknown)}")
    wanted = set(keys)
    return [topic for topic in MARKET_NEWS_TOPICS if topic.key in wanted]
