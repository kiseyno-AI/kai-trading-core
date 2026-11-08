"""Business-line taxonomy for Korean listed companies.

One primary business line per symbol, decided in this order:
1. ``SYMBOL_OVERRIDES``: large caps whose statutory industry code misleads
   (Samsung Electronics files as telecom equipment, bank holding companies
   as generic holding companies, cathode makers as battery cells);
2. the longest matching KSIC prefix in ``KSIC_RULES`` (DART company
   overview ``induty_code``);
3. the KRX sector name in ``KRX_SECTOR_RULES``;
4. ``other``.

Keys are shared with ``app/market_news/catalog.py`` topic tags.
"""

from __future__ import annotations

BUSINESS_LINES: dict[str, str] = {
    'semiconductor_memory': '반도체 칩',
    'semiconductor_equipment_materials': '반도체 장비·소재',
    'display': '디스플레이',
    'electronic_components': '전자부품',
    'it_hardware': 'IT 하드웨어·통신장비',
    'battery_cells': '2차전지 셀',
    'battery_materials': '2차전지 소재',
    'electrical_equipment': '전력기기·전선',
    'nuclear_power': '원전',
    'shipbuilding': '조선',
    'defense': '방산·항공우주',
    'autos_parts': '자동차·부품',
    'machinery': '기계·로봇',
    'steel_metals': '철강·비철금속',
    'refining_chemicals': '정유·화학',
    'other_materials': '기타 소재',
    'construction': '건설·건자재',
    'biotech': '바이오',
    'pharma': '제약',
    'medical_devices': '의료기기',
    'healthcare_services': '헬스케어 서비스',
    'cosmetics': '화장품',
    'food_beverage': '음식료·담배',
    'apparel_textiles': '섬유·의류',
    'retail_consumer': '유통·소비재',
    'leisure_travel': '레저·여행',
    'entertainment_media': '엔터·미디어',
    'games': '게임',
    'internet_platform': '인터넷 플랫폼',
    'it_services': 'IT 서비스·소프트웨어',
    'telecom': '통신',
    'utilities': '전력·가스',
    'transport_logistics': '운송·물류',
    'banks': '은행',
    'securities': '증권',
    'insurance': '보험',
    'other_financials': '기타 금융',
    'holding_companies': '지주회사',
    'real_estate': '부동산',
    'business_services': '전문·사업서비스',
    'education': '교육',
    'other': '기타',
}

# Longest prefix wins. KSIC 10th revision; DART returns 2-5 digit codes.
KSIC_RULES: dict[str, str] = {
    '10': 'food_beverage', '11': 'food_beverage', '12': 'food_beverage',
    '13': 'apparel_textiles', '14': 'apparel_textiles', '15': 'apparel_textiles',
    '16': 'other_materials', '17': 'other_materials', '18': 'other_materials',
    '19': 'refining_chemicals',
    '20': 'refining_chemicals', '20423': 'cosmetics',
    '21': 'pharma', '21102': 'biotech',
    '22': 'other_materials', '23': 'construction',
    '24': 'steel_metals',
    '25': 'machinery', '252': 'defense', '2513': 'nuclear_power',
    '26': 'electronic_components', '261': 'semiconductor_memory', '2621': 'display',
    '263': 'it_hardware', '264': 'it_hardware', '265': 'it_hardware',
    '27': 'medical_devices',
    '28': 'electrical_equipment', '2820': 'battery_cells', '285': 'it_hardware',
    '29': 'machinery', '2927': 'semiconductor_equipment_materials',
    '30': 'autos_parts',
    '31': 'transport_logistics', '311': 'shipbuilding', '313': 'defense',
    '32': 'retail_consumer', '33': 'retail_consumer',
    '35': 'utilities', '36': 'utilities',
    '41': 'construction', '42': 'construction',
    '45': 'retail_consumer', '46': 'retail_consumer', '47': 'retail_consumer',
    '49': 'transport_logistics', '50': 'transport_logistics', '51': 'transport_logistics', '52': 'transport_logistics',
    '55': 'leisure_travel', '56': 'leisure_travel',
    '58': 'it_services', '5821': 'games', '581': 'entertainment_media',
    '59': 'entertainment_media', '60': 'entertainment_media',
    '61': 'telecom',
    '62': 'it_services', '63': 'it_services', '631': 'internet_platform',
    '64': 'other_financials', '641': 'banks', '64992': 'holding_companies',
    '65': 'insurance',
    '66': 'other_financials', '6612': 'securities',
    '68': 'real_estate',
    '70': 'business_services', '7011': 'biotech',
    '71': 'business_services', '72': 'business_services', '73': 'business_services',
    '74': 'business_services', '75': 'leisure_travel', '76': 'business_services',
    '85': 'education', '86': 'healthcare_services',
    '90': 'entertainment_media', '91': 'leisure_travel',
}

KRX_SECTOR_RULES: dict[str, str] = {
    '음식료·담배': 'food_beverage', '섬유·의류': 'apparel_textiles', '종이·목재': 'other_materials',
    '화학': 'refining_chemicals', '제약': 'pharma', '비금속': 'construction', '금속': 'steel_metals',
    '기계·장비': 'machinery', '전기·전자': 'electronic_components', '의료·정밀기기': 'medical_devices',
    '운송장비·부품': 'autos_parts', '유통': 'retail_consumer', '전기·가스': 'utilities',
    '건설': 'construction', '운송·창고': 'transport_logistics', '통신': 'telecom',
    '금융': 'other_financials', '증권': 'securities', '보험': 'insurance',
    '일반서비스': 'business_services', '오락·문화': 'entertainment_media', 'IT 서비스': 'it_services',
    '부동산': 'real_estate', '은행': 'banks', '기타금융': 'other_financials',
    '전기·가스·수도': 'utilities', '출판·매체복제': 'entertainment_media',
}

# Curated: large caps whose filed industry code points elsewhere.
SYMBOL_OVERRIDES: dict[str, str] = {
    '005930': 'semiconductor_memory',  # 삼성전자 (KSIC 264)
    '005935': 'semiconductor_memory',  # 삼성전자우
    '000660': 'semiconductor_memory',  # SK하이닉스
    '042700': 'semiconductor_equipment_materials',  # 한미반도체
    '247540': 'battery_materials',  # 에코프로비엠 (KSIC 28202)
    '086520': 'battery_materials',  # 에코프로
    '003670': 'battery_materials',  # 포스코퓨처엠
    '066970': 'battery_materials',  # 엘앤에프
    '005070': 'battery_materials',  # 코스모신소재
    '020150': 'battery_materials',  # 롯데에너지머티리얼즈
    '278280': 'battery_materials',  # 천보
    '361610': 'battery_materials',  # SK아이이테크놀로지
    '373220': 'battery_cells',  # LG에너지솔루션
    '006400': 'battery_cells',  # 삼성SDI
    '051910': 'refining_chemicals',  # LG화학
    '096770': 'refining_chemicals',  # SK이노베이션
    '034020': 'nuclear_power',  # 두산에너빌리티
    '052690': 'nuclear_power',  # 한전기술
    '051600': 'nuclear_power',  # 한전KPS
    '015760': 'utilities',  # 한국전력
    '012450': 'defense',  # 한화에어로스페이스
    '047810': 'defense',  # 한국항공우주
    '064350': 'defense',  # 현대로템
    '079550': 'defense',  # LIG넥스원
    '272210': 'defense',  # 한화시스템
    '105560': 'banks',  # KB금융
    '055550': 'banks',  # 신한지주
    '086790': 'banks',  # 하나금융지주
    '316140': 'banks',  # 우리금융지주
    '138930': 'banks',  # BNK금융지주
    '175330': 'banks',  # JB금융지주
    '139130': 'banks',  # iM금융지주
    '024110': 'banks',  # 기업은행
    '071050': 'securities',  # 한국금융지주
    '035420': 'internet_platform',  # NAVER
    '035720': 'internet_platform',  # 카카오
    '207940': 'biotech',  # 삼성바이오로직스
    '068270': 'biotech',  # 셀트리온
    '005380': 'autos_parts',  # 현대차
    '000270': 'autos_parts',  # 기아
    '012330': 'autos_parts',  # 현대모비스
    '009540': 'shipbuilding',  # HD한국조선해양
    '329180': 'shipbuilding',  # HD현대중공업
    '010140': 'shipbuilding',  # 삼성중공업
    '042660': 'shipbuilding',  # 한화오션
    '267260': 'electrical_equipment',  # HD현대일렉트릭
    '010120': 'electrical_equipment',  # LS ELECTRIC
    '298040': 'electrical_equipment',  # 효성중공업
    '352820': 'entertainment_media',  # 하이브
    '041510': 'entertainment_media',  # 에스엠
    '035900': 'entertainment_media',  # JYP Ent.
    '122870': 'entertainment_media',  # 와이지엔터테인먼트
    '259960': 'games',  # 크래프톤
    '036570': 'games',  # 엔씨소프트
    '251270': 'games',  # 넷마블
    '090430': 'cosmetics',  # 아모레퍼시픽
    '051900': 'cosmetics',  # LG생활건강
}


def classify_business_line(
    *,
    symbol: str,
    ksic_code: str | None,
    krx_sector: str | None = None,
) -> tuple[str, str]:
    """``(business_line, source)`` where source is override/ksic/krx_sector/default."""

    if symbol in SYMBOL_OVERRIDES:
        return SYMBOL_OVERRIDES[symbol], 'override'
    code = ''.join(ch for ch in str(ksic_code or '') if ch.isdigit())
    for length in range(len(code), 1, -1):
        line = KSIC_RULES.get(code[:length])
        if line is not None:
            return line, 'ksic'
    sector = str(krx_sector or '').strip()
    if sector in KRX_SECTOR_RULES:
        return KRX_SECTOR_RULES[sector], 'krx_sector'
    return 'other', 'default'
