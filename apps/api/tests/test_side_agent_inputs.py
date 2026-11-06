from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from app.side_agents import market_inputs as mi
from app.side_agents import news_inputs as ni
from app.side_agents import postmortem_inputs as pi
from app.side_agents.payloads import assemble_news_inputs

KST = ZoneInfo('Asia/Seoul')
DAY = date(2026, 11, 6)
NOW = datetime(2026, 11, 6, 18, 40, tzinfo=KST)

# Captured shape of the real KIS responses (values trimmed to what the parsers use).
KIS_INDEX = {
    'bstp_nmix_prpr': '5059.69', 'bstp_nmix_prdy_vrss': '-324.11', 'bstp_nmix_prdy_ctrt': '-6.02',
    'acml_vol': '323267', 'acml_tr_pbmn': '28982905', 'prdy_tr_pbmn': '23612859',
    'bstp_nmix_oprc': '5263.26', 'bstp_nmix_hgpr': '5269.10', 'bstp_nmix_lwpr': '4911.28',
    'ascn_issu_cnt': '131', 'uplm_issu_cnt': '4', 'stnr_issu_cnt': '20', 'down_issu_cnt': '760', 'lslm_issu_cnt': '0',
    'dryy_bstp_nmix_hgpr': '9385.59', 'dryy_hgpr_vrss_prpr_rate': '46.09', 'dryy_bstp_nmix_lwpr': '4216.68', 'dryy_lwpr_vrss_prpr_rate': '-19.99',
}
KIS_FLOW = {
    'frgn_ntby_tr_pbmn': '-2689162', 'frgn_shnu_tr_pbmn': '10397461', 'frgn_seln_tr_pbmn': '13086623',
    'orgn_ntby_tr_pbmn': '1709171', 'orgn_shnu_tr_pbmn': '9703355', 'orgn_seln_tr_pbmn': '7994185',
    'prsn_ntby_tr_pbmn': '521056', 'scrt_ntby_tr_pbmn': '1624870', 'ivtr_ntby_tr_pbmn': '-188578',
}
KIS_QUOTE = {'last': '737.5200', 'base': '733.0000', 'diff': '4.5200', 'rate': '+0.62', 'tvol': '19334079'}
BOK_ROWS = [
    {'TIME': '20261104', 'DATA_VALUE': '3.10'}, {'TIME': '20261105', 'DATA_VALUE': '3.12'}, {'TIME': '20261106', 'DATA_VALUE': '3.20'},
]


class FakeKIS:
    def __init__(self, fail_flows=False):
        self.fail_flows = fail_flows
        self.calls = []

    async def fetch_market_index_price(self, *, index_code):
        self.calls.append(('index', index_code))
        out = dict(KIS_INDEX)
        if index_code == '1001':
            out['bstp_nmix_prdy_ctrt'] = '-5.03'
        return {'output': out}

    async def fetch_investor_time_by_market(self, *, market_code, sector_code):
        self.calls.append(('flow', market_code))
        if self.fail_flows:
            raise TimeoutError('kis timeout')
        return {'output': [dict(KIS_FLOW, frgn_ntby_tr_pbmn='-1' if market_code == 'KSQ' else KIS_FLOW['frgn_ntby_tr_pbmn'])]}

    async def fetch_overseas_quote(self, *, exchange, symbol):
        self.calls.append(('us', symbol))
        return {'output': KIS_QUOTE}


class FakeECOS:
    def __init__(self, fail_for=()):
        self.fail_for = set(fail_for)

    async def fetch_series(self, *, stat_code, item_code_1, **kw):
        if item_code_1 in self.fail_for:
            raise RuntimeError('ecos down')
        return {'StatisticSearch': {'row': BOK_ROWS}}


def breadth_rows():
    rows = []
    for i in range(10):
        rows.append({'market': 'KOSPI', 'sector': '반도체', 'ret': -6.0 + i * 0.1, 'turnover': 1e9, 'caution': i == 0, 'halted': False, 'vi': i < 2})
    for i in range(6):
        rows.append({'market': 'KOSDAQ', 'sector': '바이오', 'ret': 1.0, 'turnover': 5e8, 'caution': False, 'halted': i == 0, 'vi': False})
    rows.append({'market': 'KOSDAQ', 'sector': '바이오', 'ret': None, 'turnover': 1, 'caution': False, 'halted': False, 'vi': False})
    return rows


# ---- market inputs -------------------------------------------------------

def test_parse_index_extracts_breadth_range_and_position():
    out = mi.parse_index(KIS_INDEX)
    assert out['level'] == 5059.69 and out['change_pct'] == -6.02
    assert (out['advancers'], out['decliners'], out['limit_up']) == (131, 760, 4)
    assert out['day_range_pct'] == pytest.approx((5269.10 - 4911.28) / 4911.28 * 100, abs=1e-3)
    assert 0 <= out['close_position_in_range'] <= 1


def test_parse_flows_and_quote_and_series():
    flows = mi.parse_flows(KIS_FLOW)
    assert flows['foreign_net'] == -2689162 and flows['institution_gross_traded'] == 9703355 + 7994185 and flows['unit'] == 'million_krw'
    assert mi.parse_quote(KIS_QUOTE) == {'last': 737.52, 'previous_close': 733.0, 'change': 4.52, 'change_pct': 0.62, 'volume': 19334079.0}
    series = mi.summarize_series(BOK_ROWS, unit_hint='yield_pct')
    assert series['last'] == 3.20 and series['change_bp'] == 8.0 and series['as_of'] == '20261106'
    assert mi.summarize_series([]) == {'status': 'empty'}


def test_aggregate_breadth_counts_sectors_and_flags():
    out = mi.aggregate_breadth(breadth_rows())
    kospi, kosdaq = out['markets']['KOSPI'], out['markets']['KOSDAQ']
    assert kospi['names'] == 10 and kospi['advancers'] == 0 and kospi['decliners'] == 10
    assert kospi['down_5pct_or_worse'] == 10 and kosdaq['names'] == 6 and kosdaq['advancer_share'] == 1.0  # None-return row excluded
    assert out['weakest_sectors'][0]['sector'] == '반도체' and out['strongest_sectors'][0]['sector'] == '바이오'
    assert out['warning_flags'] == {'investment_caution': 1, 'trade_halted': 1, 'vi_triggered': 2}


def _collect(kis=None, ecos=None, now=NOW, market_date=DAY, **kw):
    return asyncio.run(
        mi.collect_market_inputs(
            market_date, kis=kis or FakeKIS(), ecos=ecos or FakeECOS(), now=now,
            breadth_loader=kw.pop('breadth_loader', lambda d: breadth_rows()),
            fx_loader=kw.pop('fx_loader', lambda: {'status': 'ok', 'usd_krw': 1342.0}),
            is_trading_day=lambda d: d.weekday() < 5, **kw,
        )
    )


def test_collect_keeps_index_and_flow_legs_separate_per_market():
    out = _collect()['inputs']
    assert out['indices']['kospi']['data']['change_pct'] == -6.02 and out['indices']['kosdaq']['data']['change_pct'] == -5.03
    assert out['investor_flows']['kospi']['data']['foreign_net'] == -2689162 and out['investor_flows']['kosdaq']['data']['foreign_net'] == -1
    assert set(out['us_overnight']) >= {'spy', 'qqq', 'soxx', 'ewy', 'tlt', 'uup', 'uso', 'gld'}
    assert out['rates_and_fx']['ktb_3y']['data']['change_bp'] == 8.0
    assert out['event_calendar']['status'] == 'not_available'
    assert out['stored_breadth']['status'] == 'ok' and out['fx_snapshot']['usd_krw'] == 1342.0


def test_one_failing_leg_never_hides_the_others():
    out = _collect(kis=FakeKIS(fail_flows=True), ecos=FakeECOS(fail_for={'010210000'}))['inputs']
    assert out['investor_flows']['kospi']['status'] == 'error' and 'TimeoutError' in out['investor_flows']['kospi']['error']
    assert out['rates_and_fx']['ktb_10y']['status'] == 'error' and out['rates_and_fx']['ktb_3y']['status'] == 'ok'
    assert out['indices']['kospi']['status'] == 'ok'


def test_historical_date_skips_live_legs_and_never_calls_kis():
    kis = FakeKIS()
    out = _collect(kis=kis, market_date=date(2026, 11, 3))
    assert kis.calls == [] and out['live_legs_collected'] is False
    assert out['inputs']['indices'] == {'status': 'skipped_historical_date'}
    assert out['inputs']['rates_and_fx']['ktb_3y']['status'] == 'ok'


def test_empty_or_failing_breadth_is_reported_not_raised():
    assert _collect(breadth_loader=lambda d: [])['inputs']['stored_breadth']['status'] == 'unavailable'

    def boom(d):
        raise RuntimeError('db down')

    out = _collect(breadth_loader=boom)['inputs']
    assert out['stored_breadth']['status'] == 'error' and out['breadth_history'] == []


def test_breadth_history_walks_back_over_trading_days_only():
    seen = []

    def loader(d):
        seen.append(d)
        return [{'ret': 1.0}, {'ret': -1.0}, {'ret': 2.0}]

    hist = mi.breadth_history(date(2026, 11, 9), days=3, loader=loader, is_trading_day=lambda d: d.weekday() < 5)
    assert [h['date'] for h in hist] == ['2026-11-06', '2026-11-05', '2026-11-04']
    assert all(d.weekday() < 5 for d in seen) and hist[0]['advancer_share'] == pytest.approx(2 / 3, abs=1e-3)


# ---- news inputs ---------------------------------------------------------

@pytest.mark.parametrize('title,expected', [
    ('유상증자결정', 'high'), ('[기재정정]단일판매ㆍ공급계약체결', 'high'), ('연결재무제표기준영업(잠정)실적(공정공시)', 'high'),
    ('임원ㆍ주요주주특정증권등소유상황보고서', 'low'), ('증권발행실적보고서', 'low'), ('주식등의대량보유상황보고서(일반)', 'medium'), (None, 'low'),
])
def test_filing_importance_rules(title, expected):
    assert ni.classify_filing(title) == expected


def filing(code, title, day, rcept='1'):
    return {'stock_code': code, 'rcept_no': rcept, 'report_nm': title, 'filing_category': 'x', 'filer_name': 'f', 'receipt_date': day}


def test_group_filings_prefers_important_newest_and_keeps_one_low_when_nothing_else():
    rows = [
        filing('A', '임원ㆍ주요주주특정증권등소유상황보고서', date(2026, 11, 6)),
        filing('A', '유상증자결정', date(2026, 11, 5), '2'),
        filing('A', '주식등의대량보유상황보고서(일반)', date(2026, 11, 6), '3'),
        filing('B', '증권발행실적보고서', date(2026, 11, 6), '4'),
    ]
    grouped = ni.group_filings(rows)
    assert [f['title'] for f in grouped['A']] == ['유상증자결정', '주식등의대량보유상황보고서(일반)']
    assert grouped['A'][0]['source_tier'] == 'official' and grouped['A'][0]['content_basis'] == 'title_only'
    assert [f['importance'] for f in grouped['B']] == ['low']


def test_price_context_moves_and_volume_ratio():
    bars = [{'trade_date': date(2026, 10, 1) + timedelta(days=i), 'close_price': 100 + i, 'volume': 1000} for i in range(25)]
    bars[-1]['volume'] = 3000
    ctx = ni.compute_price_context(bars)
    assert ctx['move_1d_pct'] == pytest.approx((124 / 123 - 1) * 100, abs=1e-3)
    assert ctx['move_5d_pct'] == pytest.approx((124 / 119 - 1) * 100, abs=1e-3) and ctx['volume_ratio_20d'] == 3.0
    assert ni.compute_price_context(bars[:1]) == {'status': 'insufficient_history'}


def test_select_symbols_ranks_high_filings_then_material_news_and_caps():
    news = {'N1': [{'materiality': 'material'}], 'N2': [{'materiality': 'noise'}] * 3, 'F': []}
    filings = {'F': [{'importance': 'high'}], 'N2': [{'importance': 'low'}]}
    assert sorted(ni.select_symbols(news, filings, max_symbols=10)) == ['F', 'N1', 'N2']
    # neither source crowds out the other: one news-driven and one filing-driven symbol when capped at 2
    chosen = ni.select_symbols(news, filings, max_symbols=2)
    assert 'F' in chosen and chosen[0] == 'N1'
    many_filings = {f'F{i}': [{'importance': 'high'}] for i in range(6)}
    capped = ni.select_symbols({'N1': [{'materiality': 'material'}]}, many_filings, max_symbols=4)
    assert 'N1' in capped and len(capped) == 4


def test_assemble_news_inputs_merges_sources_and_trims_fields():
    items = [{'symbol': 'N1', 'title': 't' * 600, 'materiality': 'material', 'url': 'https://x', 'secret': 'x'}]
    filings = [filing('F', '유상증자결정', date(2026, 11, 5))]
    out = assemble_news_inputs(
        DAY, items, filings, max_symbols=5,
        price_loader=lambda codes, d: {c: {'move_1d_pct': 1.0} for c in codes}, name_loader=lambda codes: {c: f'{c}-name' for c in codes},
    )
    assert list(out) == ['N1', 'F']
    assert out['N1']['news'][0]['title'] == 't' * 400 and 'secret' not in out['N1']['news'][0] and 'url' not in out['N1']['news'][0]
    assert out['F']['official_filings'][0]['importance'] == 'high' and out['F']['name_ko'] == 'F-name'
    assert out['N1']['price_context'] == {'move_1d_pct': 1.0}


# ---- postmortem inputs ---------------------------------------------------

def session_bars(start=date(2026, 11, 2), n=8, open0=100.0, step=1.0):
    return [
        {'trade_date': start + timedelta(days=i), 'open_price': open0 + i * step, 'close_price': open0 + i * step + step}
        for i in range(n)
    ]


def test_realized_return_uses_next_session_open_and_horizon_close():
    bars = session_bars()
    # decision on 11-02 -> entry open of 11-03 (101); horizon 3 -> close of the third session counting 11-03, i.e. 11-05 (104)
    assert pi.realized_return(bars, date(2026, 11, 2), 3) == pytest.approx((104 / 101 - 1) * 100)
    assert pi.realized_return(bars, date(2026, 11, 8), 3) is None  # not enough future bars
    # boundary: exactly enough bars (entry + horizon - 1 is the last bar) must still resolve; one fewer must not
    assert pi.realized_return(session_bars(n=4), date(2026, 11, 2), 3) is not None
    assert pi.realized_return(session_bars(n=3), date(2026, 11, 2), 3) is None


def test_classification_rules():
    c = pi.ROUND_TRIP_COST_PCT
    assert pi.classify('buy', c + 0.5, None, c) == 'true_positive'
    assert pi.classify('buy', c / 2, None, c) == 'cost_drag'
    assert pi.classify('buy', -1.0, None, c) == 'false_positive'
    assert pi.classify('buy', -1.0, -2.0, c) == 'regime_mismatch'
    assert pi.classify('wait', 3 * c, None, c) == 'missed_opportunity'
    assert pi.classify('reject', c, None, c) == 'correct_abstention'


def test_summarize_outcomes_counts_buckets_and_sample_sufficiency():
    bars = {'AAA': session_bars(), 'BBB': session_bars(step=-1.0, open0=200.0)}
    decisions = [
        {'market_date': date(2026, 11, 2), 'symbol': 'AAA', 'agent_name': 'signal-decider', 'decision_action': 'buy', 'confidence_score_10': 8, 'tactical_action': None, 'preferred_action': None},
        {'market_date': date(2026, 11, 2), 'symbol': 'BBB', 'agent_name': 'signal-decider', 'decision_action': 'buy', 'confidence_score_10': 8, 'tactical_action': None, 'preferred_action': None},
        {'market_date': date(2026, 11, 2), 'symbol': 'AAA', 'agent_name': 'short-term-decider', 'decision_action': 'buy', 'confidence_score_10': 6, 'tactical_action': 'watch_for_trigger', 'preferred_action': None},
        {'market_date': date(2026, 11, 20), 'symbol': 'AAA', 'agent_name': 'signal-decider', 'decision_action': 'buy', 'confidence_score_10': 5, 'tactical_action': None, 'preferred_action': None},
        {'market_date': date(2026, 11, 2), 'symbol': 'AAA', 'agent_name': 'signal-decider', 'decision_action': 'hold', 'confidence_score_10': 5, 'tactical_action': None, 'preferred_action': None},
    ]
    out = pi.summarize_outcomes(decisions, bars, {})
    signal = out['agents']['signal-decider']
    assert signal['recorded'] == 3 and signal['resolved'] == 2 and signal['unresolved'] == 1  # 'hold' is not an evaluable action
    assert signal['classification']['true_positive'] == 1 and signal['classification']['false_positive'] == 1
    assert signal['sufficient_sample'] is False and signal['horizon_sessions'] == 5
    assert [b['bucket'] for b in signal['buy_confidence_buckets']] == ['8']
    assert signal['candidate_baseline']['n'] == 2
    short = out['agents']['short-term-decider']
    assert short['by_action']['wait']['n'] == 1  # watch_for_trigger is an observation, not a buy
    assert out['total_resolved'] == 3
