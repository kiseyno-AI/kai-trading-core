"""Conditional statistics: which business lines led or lagged under which conditions.

    python -m app.market.business_line_conditions --out report.md --json stats.json

Reads ``business_line_daily_metrics`` and ``macro_series_history`` (and
``market_news_items`` when present). Every macro input is the value that was
public before the Korean trading day (``released_on < trade_date``), so no
day sees a later number. Outcome = the line's summed excess return over the
next 5 or 20 trading days, starting that day.

Overlapping windows make neighbouring days dependent, so the effective
sample is days / horizon; a result needs at least ``MIN_EFFECTIVE`` effective
observations and |t| >= 2, and it is marked consistent only when the lift has
the same sign in both halves of the history. Read-only; writes files only.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.db import engine
from app.market.business_lines import BUSINESS_LINES

HORIZONS = (5, 20)
MIN_EFFECTIVE = 20
SPLIT_DATE = pd.Timestamp('2020-01-01')
# Stored bars cover the universe only from 2014; days with fewer classified
# symbols than this are skipped so a handful of names cannot pose as a market.
MIN_DAILY_SYMBOLS = 500


# ---------------------------------------------------------------- inputs
def load_metrics(connection) -> pd.DataFrame:
    frame = pd.read_sql(
        text('select trade_date, business_line, excess_return, symbols from business_line_daily_metrics'),
        connection,
    )
    frame['trade_date'] = pd.to_datetime(frame['trade_date'])
    frame['excess_return'] = frame['excess_return'].astype(float)
    return frame


def load_macro(connection) -> dict[str, pd.DataFrame]:
    frame = pd.read_sql(
        text('select series_id, observation_date, released_on, value from macro_series_history order by observation_date'),
        connection,
    )
    frame['observation_date'] = pd.to_datetime(frame['observation_date'])
    frame['released_on'] = pd.to_datetime(frame['released_on'])
    frame['value'] = frame['value'].astype(float)
    return {series_id: group.reset_index(drop=True) for series_id, group in frame.groupby('series_id')}


# -------------------------------------------------------- point in time
def _derived(series: pd.DataFrame, kind: str, periods: int) -> pd.DataFrame:
    """Level, change or percent change over ``periods`` observations, released with the newer value."""

    out = series[['released_on']].copy()
    if kind == 'level':
        out['value'] = series['value']
    elif kind == 'diff':
        out['value'] = series['value'].diff(periods)
    elif kind == 'pct':
        out['value'] = series['value'].pct_change(periods, fill_method=None) * 100
    else:
        raise ValueError(kind)
    return out.dropna()


def as_of(days: pd.DatetimeIndex, series: pd.DataFrame) -> pd.Series:
    """Latest value released strictly before each trading day."""

    ordered = series.sort_values('released_on').drop_duplicates('released_on', keep='last')
    left = pd.DataFrame({'trade_date': days})
    merged = pd.merge_asof(
        left, ordered.rename(columns={'released_on': 'trade_date'}),
        on='trade_date', allow_exact_matches=False, direction='backward',
    )
    return pd.Series(merged['value'].to_numpy(), index=days)


FEATURES: dict[str, tuple[str, str, int]] = {
    'vix': ('VIXCLS', 'level', 0),
    'vix_chg5': ('VIXCLS', 'pct', 5),
    'us10y_chg20_bp': ('DGS10', 'diff', 20),
    'us10y_chg60_bp': ('DGS10', 'diff', 60),
    'curve': ('T10Y2Y', 'level', 0),
    'usdkrw_chg20': ('DEXKOUS', 'pct', 20),
    'wti_chg20': ('DCOILWTICO', 'pct', 20),
    'wti_chg60': ('DCOILWTICO', 'pct', 60),
    'cpi_yoy': ('CPIAUCSL', 'pct', 12),
    'fedfunds_chg63': ('DFF', 'diff', 63),
}


def feature_frame(days: pd.DatetimeIndex, macro: dict[str, pd.DataFrame]) -> pd.DataFrame:
    columns = {}
    for name, (series_id, kind, periods) in FEATURES.items():
        series = macro.get(series_id)
        if series is None or series.empty:
            columns[name] = pd.Series(np.nan, index=days)
            continue
        derived = _derived(series, kind, periods)
        if kind == 'diff' and series_id.startswith('DGS'):
            derived['value'] = derived['value'] * 100  # percent points -> bp
        columns[name] = as_of(days, derived)
    return pd.DataFrame(columns, index=days)


# ------------------------------------------------------------ conditions
@dataclass(frozen=True)
class Condition:
    key: str
    label: str
    test: Callable[[pd.DataFrame], pd.Series]


CONDITIONS: tuple[Condition, ...] = (
    Condition('vix_lt15', 'VIX 15 미만 (안정)', lambda f: f.vix < 15),
    Condition('vix_20_30', 'VIX 20~30 (경계)', lambda f: (f.vix >= 20) & (f.vix < 30)),
    Condition('vix_ge30', 'VIX 30 이상 (공포)', lambda f: f.vix >= 30),
    Condition('vix_spike', 'VIX 5일 +25% 이상 급등', lambda f: f.vix_chg5 >= 25),
    Condition('rates_up', '미 10년물 20일 +30bp 이상 상승', lambda f: f.us10y_chg20_bp >= 30),
    Condition('rates_down', '미 10년물 20일 -30bp 이상 하락', lambda f: f.us10y_chg20_bp <= -30),
    Condition('curve_inverted', '미 장단기 금리 역전 (10년-2년 < 0)', lambda f: f.curve < 0),
    Condition('krw_weak', '원/달러 20일 +2% 이상 (원화 약세)', lambda f: f.usdkrw_chg20 >= 2),
    Condition('krw_strong', '원/달러 20일 -2% 이하 (원화 강세)', lambda f: f.usdkrw_chg20 <= -2),
    Condition('oil_up', '유가(WTI) 20일 +10% 이상', lambda f: f.wti_chg20 >= 10),
    Condition('oil_down', '유가(WTI) 20일 -10% 이하', lambda f: f.wti_chg20 <= -10),
    Condition('cpi_hot', '미 CPI 전년비 3.5% 이상', lambda f: f.cpi_yoy >= 3.5),
    Condition('cpi_cool', '미 CPI 전년비 2.0% 이하', lambda f: f.cpi_yoy <= 2.0),
    Condition('fed_hiking', '연준 금리 3개월 +0.25%p 이상 인상', lambda f: f.fedfunds_chg63 >= 0.25),
    Condition('fed_cutting', '연준 금리 3개월 -0.25%p 이상 인하', lambda f: f.fedfunds_chg63 <= -0.25),
)

REGIMES = ('위기', '위험회피', '위험선호·경기민감', '위험선호·성장')


def regime_series(features: pd.DataFrame) -> pd.Series:
    f = features
    cyclical = (f.wti_chg60 >= 10) | (f.us10y_chg60_bp >= 25)
    regime = np.select(
        [f.vix >= 30, f.vix >= 20, (f.vix < 20) & cyclical, f.vix < 20],
        list(REGIMES),
        default='',
    )
    return pd.Series(regime, index=f.index).replace('', np.nan)


# --------------------------------------------------------------- statistics
def forward_excess(metrics: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Dates x lines: summed excess return over the next ``horizon`` days, in percent."""

    daily = metrics.pivot(index='trade_date', columns='business_line', values='excess_return').sort_index()
    return daily.rolling(horizon, min_periods=horizon).sum().shift(-(horizon - 1)) * 100


def _stats(values: pd.Series, baseline: float, horizon: int) -> dict[str, Any]:
    values = values.dropna()
    n = len(values)
    n_eff = n / horizon
    mean = float(values.mean()) if n else math.nan
    std = float(values.std(ddof=1)) if n > 1 else math.nan
    t = (mean - baseline) / (std / math.sqrt(n_eff)) if n_eff >= 2 and std and std > 0 else math.nan
    return {
        'days': n,
        'effective': round(n_eff, 1),
        'mean': mean,
        'lift': mean - baseline,
        'hit_rate': float((values > 0).mean()) if n else math.nan,
        't': t,
    }


def condition_table(forward: pd.DataFrame, mask: pd.Series, horizon: int) -> list[dict[str, Any]]:
    mask = mask.reindex(forward.index).fillna(False).astype(bool)
    rows = []
    for line in forward.columns:
        series = forward[line]
        baseline = float(series.mean())
        full = _stats(series[mask], baseline, horizon)
        early = _stats(series[mask & (forward.index < SPLIT_DATE)], float(series[forward.index < SPLIT_DATE].mean()), horizon)
        late = _stats(series[mask & (forward.index >= SPLIT_DATE)], float(series[forward.index >= SPLIT_DATE].mean()), horizon)
        consistent = (
            early['effective'] >= MIN_EFFECTIVE / 2
            and late['effective'] >= MIN_EFFECTIVE / 2
            and np.sign(early['lift']) == np.sign(late['lift']) == np.sign(full['lift'])
        )
        rows.append({'line': line, **full, 'early_lift': early['lift'], 'late_lift': late['lift'], 'consistent': bool(consistent)})
    return rows


def episodes(mask: pd.Series, horizon: int) -> int:
    """Condition onsets at least ``horizon`` days after the previous onset."""

    count, last = 0, -10**9
    for position, (flag, previous) in enumerate(zip(mask.to_numpy(), np.r_[False, mask.to_numpy()[:-1]])):
        if flag and not previous and position - last >= horizon:
            count += 1
            last = position
    return count


def transition_matrix(regimes: pd.Series) -> pd.DataFrame:
    monthly = regimes.dropna().resample('ME').last().dropna()
    pairs = pd.DataFrame({'from': monthly.iloc[:-1].to_numpy(), 'to': monthly.iloc[1:].to_numpy()})
    table = pd.crosstab(pairs['from'], pairs['to'], normalize='index').reindex(index=REGIMES, columns=REGIMES).fillna(0.0)
    return table


def build_report(
    metrics: pd.DataFrame,
    macro: dict[str, pd.DataFrame],
    *,
    min_daily_symbols: int = MIN_DAILY_SYMBOLS,
) -> dict[str, Any]:
    if 'symbols' in metrics.columns:
        per_day = metrics.groupby('trade_date')['symbols'].transform('sum')
        metrics = metrics[per_day >= min_daily_symbols]
    days = pd.DatetimeIndex(sorted(metrics['trade_date'].unique()))
    features = feature_frame(days, macro)
    regimes = regime_series(features)
    result: dict[str, Any] = {
        'range': [days.min().date().isoformat(), days.max().date().isoformat()],
        'trading_days': len(days),
        'lines': sorted(metrics['business_line'].unique()),
        'conditions': {},
        'regimes': {},
    }
    forwards = {horizon: forward_excess(metrics, horizon) for horizon in HORIZONS}
    for condition in CONDITIONS:
        mask = condition.test(features).fillna(False)
        entry = {'label': condition.label, 'days': int(mask.sum()), 'episodes': episodes(mask, 20)}
        for horizon in HORIZONS:
            entry[f'h{horizon}'] = condition_table(forwards[horizon], mask, horizon)
        result['conditions'][condition.key] = entry
    for regime in REGIMES:
        mask = (regimes == regime)
        result['regimes'][regime] = {
            'days': int(mask.sum()),
            'h20': condition_table(forwards[20], mask, 20),
        }
    result['transitions'] = transition_matrix(regimes).round(3).to_dict()
    latest = features.dropna(how='all').iloc[-1]
    result['latest'] = {
        'date': features.dropna(how='all').index[-1].date().isoformat(),
        'regime': regimes.dropna().iloc[-1] if regimes.notna().any() else None,
        'features': {key: (None if pd.isna(value) else round(float(value), 2)) for key, value in latest.items()},
        'active_conditions': [c.key for c in CONDITIONS if bool(c.test(features.tail(1)).fillna(False).iloc[0])],
    }
    return result


# ---------------------------------------------------------------- markdown
WEAK_T = 1.5


def _significant(rows: list[dict[str, Any]], sign: int) -> list[dict[str, Any]]:
    """Up to three lines in one direction: |t| >= 2, or 1.5-2 when both halves agree."""

    picked = [
        row for row in rows
        if row['effective'] >= MIN_EFFECTIVE
        and not math.isnan(row['t'])
        and np.sign(row['lift']) == sign
        and (abs(row['t']) >= 2 or (abs(row['t']) >= WEAK_T and row['consistent']))
    ]
    return sorted(picked, key=lambda row: -abs(row['t']))[:3]


def _fmt(row: dict[str, Any]) -> str:
    if abs(row['t']) >= 2 and row['consistent']:
        mark = ' **유의**'
    elif abs(row['t']) >= 2:
        mark = ' (전후반 불일치)'
    else:
        mark = ' (약한 후보)'
    return f"{BUSINESS_LINES.get(row['line'], row['line'])} {row['lift']:+.2f}%p (t {row['t']:.1f}){mark}"


def to_markdown(report: dict[str, Any], news: dict[str, Any] | None = None) -> str:
    lines = [
        '# 사업군 조건부 통계 보고서',
        '',
        f"- 기간: {report['range'][0]} ~ {report['range'][1]} ({report['trading_days']:,} 거래일), 사업군 {len(report['lines'])}개",
        '- 결과값: 조건이 성립한 날부터 20거래일 동안 사업군의 **시장 대비 초과수익 합계**가 평소보다 얼마나 높았나(lift, %p).',
        f"- 표시 기준: 겹치는 기간을 감안한 유효 표본 {MIN_EFFECTIVE}개 이상. |t| ≥ 2이고 {report['range'][0][:4]}~2019와 2020~ 두 구간 방향이 같으면 **유의**, 방향이 다르면 '전후반 불일치', |t| 1.5~2이면서 두 구간 방향이 같으면 '약한 후보'.",
        '- 빈칸(—)은 그 조건에서 평소와 뚜렷이 다르게 움직인 사업군이 없었다는 뜻입니다. 유효 표본이 20개 미만인 조건(드물게 발생)은 판단하지 않았습니다.',
        f"- 저장된 일봉은 2014년부터 전 종목이 있어서, 분류 종목이 하루 {MIN_DAILY_SYMBOLS}개 미만인 날은 뺐습니다.",
        '- 거시 값은 한국 장 시작 전에 이미 공표된 값만 사용했습니다(공표일 기준).',
        '',
    ]
    latest = report['latest']
    labels = {c.key: c.label for c in CONDITIONS}
    lines += [
        '## 지금 상태',
        '',
        f"- 기준일 {latest['date']}, 국면: **{latest['regime'] or '판단 불가'}**",
        '- 성립 중인 조건: ' + (', '.join(labels[key] for key in latest['active_conditions']) or '없음'),
        '',
        '## 조건별 강세·약세 사업군 (20거래일)',
        '',
        '| 조건 | 발생일 | 독립 발생 | 강세 상위 | 약세 상위 |',
        '|---|---:|---:|---|---|',
    ]
    for key, entry in report['conditions'].items():
        rows = entry['h20']
        strong = '<br>'.join(_fmt(row) for row in _significant(rows, 1)) or '—'
        weak = '<br>'.join(_fmt(row) for row in _significant(rows, -1)) or '—'
        lines.append(f"| {entry['label']} | {entry['days']:,} | {entry['episodes']} | {strong} | {weak} |")
    lines += ['', '## 국면별 강세·약세 사업군 (20거래일)', '', '| 국면 | 일수 | 강세 상위 | 약세 상위 |', '|---|---:|---|---|']
    for regime, entry in report['regimes'].items():
        rows = entry['h20']
        strong = '<br>'.join(_fmt(row) for row in _significant(rows, 1)) or '—'
        weak = '<br>'.join(_fmt(row) for row in _significant(rows, -1)) or '—'
        lines.append(f"| {regime} | {entry['days']:,} | {strong} | {weak} |")
    lines += ['', '## 국면 전환 확률 (월말 기준, 다음 달)', '', '| 이번 달 \\ 다음 달 | ' + ' | '.join(REGIMES) + ' |', '|---' * (len(REGIMES) + 1) + '|']
    transitions = report['transitions']
    for source in REGIMES:
        lines.append(f'| {source} | ' + ' | '.join(f"{transitions.get(target, {}).get(source, 0):.0%}" for target in REGIMES) + ' |')
    if news:
        lines += ['', '## 시장 뉴스 (수집된 기간만)', '', news['summary'], '']
        if news.get('rows'):
            lines += ['| 토픽 | 기사 수 | 관련 사업군 | 기사 많은 날 다음 5일 초과수익 |', '|---|---:|---|---|']
            lines += [
                f"| {row['topic']} | {row['articles']:,} | {row['lines']} | {row['surge']} |" for row in news['rows']
            ]
    lines += [
        '',
        '## 해석할 때 주의',
        '',
        '- 사업군 분류는 현재 기준을 과거에 그대로 적용했습니다. 상장폐지된 회사가 빠져 있어 과거 수익이 좋게 보일 수 있습니다(생존 편향).',
        '- 미 하이일드 스프레드는 FRED 라이선스상 최근 3년만 있어 조건에 넣지 않았습니다.',
        '- 15개 조건 × 42개 사업군을 동시에 봤기 때문에 우연히 |t| ≥ 2가 나오는 칸이 일부 있습니다. 전후반이 일치하는 결과만 전략 근거 후보로 보는 것을 권합니다.',
        '- 매매 신호가 아니라 검증 후보입니다. signal lab(#703)에서 같은 규칙으로 채점한 뒤 사용 여부를 정해야 합니다.',
        '',
    ]
    return '\n'.join(lines)


def news_section(connection, metrics: pd.DataFrame) -> dict[str, Any] | None:
    from app.market_news.catalog import TOPICS_BY_KEY

    try:
        counts = pd.read_sql(
            text(
                "select date(timezone('Asia/Seoul', published_at)) as day, topic_key, count(*) as n "
                'from market_news_items where published_at is not null group by 1, 2'
            ),
            connection,
        )
    except Exception:  # noqa: BLE001 - table absent before the news migration
        return None
    if counts.empty:
        return None
    counts['day'] = pd.to_datetime(counts['day'])
    first, last = counts['day'].min().date(), counts['day'].max().date()
    forward = forward_excess(metrics, 5)
    rows = []
    for topic_key, group in counts.groupby('topic_key'):
        topic = TOPICS_BY_KEY.get(topic_key)
        if topic is None or not topic.business_lines:
            continue
        daily = group.set_index('day')['n'].asfreq('D', fill_value=0)
        surge_days = daily[daily >= 2 * daily.rolling(14, min_periods=7).mean().shift(1)].index
        values = [
            forward.loc[forward.index[forward.index.searchsorted(day + pd.Timedelta(days=1))], line]
            for day in surge_days
            for line in topic.business_lines
            if line in forward.columns and forward.index.searchsorted(day + pd.Timedelta(days=1)) < len(forward.index)
        ]
        values = [value for value in values if not pd.isna(value)]
        rows.append(
            {
                'topic': f'{topic.query} ({topic_key})',
                'articles': int(group['n'].sum()),
                'lines': ', '.join(BUSINESS_LINES.get(line, line) for line in topic.business_lines),
                'surge': f'{np.mean(values):+.2f}%p ({len(values)}건)' if values else '표본 없음',
            }
        )
    rows.sort(key=lambda row: -row['articles'])
    return {
        'summary': (
            f'수집된 기사의 게재일 범위는 {first} ~ {last}입니다. 네이버 검색은 토픽마다 최근 1,000건까지만 주기 때문에 '
            '기간이 짧고, 아래 수치는 통계적 근거가 아니라 수집 상태 확인용입니다. 의미 있는 판단은 6개월 이상 쌓인 뒤에 합니다. '
            "'기사 많은 날'은 기사 수가 직전 14일 평균의 2배 이상인 날입니다."
        ),
        'rows': rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description='Write the business-line conditional statistics report.')
    parser.add_argument('--out', required=True)
    parser.add_argument('--json')
    args = parser.parse_args()
    with engine.connect() as connection:
        metrics = load_metrics(connection)
        report = build_report(metrics, load_macro(connection))
        news = news_section(connection, metrics)
    with open(args.out, 'w', encoding='utf-8') as handle:
        handle.write(to_markdown(report, news))
    if args.json:
        with open(args.json, 'w', encoding='utf-8') as handle:
            json.dump(report, handle, ensure_ascii=False, default=str, indent=1)
    print(json.dumps({'out': args.out, 'trading_days': report['trading_days'], 'regime_now': report['latest']['regime']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
