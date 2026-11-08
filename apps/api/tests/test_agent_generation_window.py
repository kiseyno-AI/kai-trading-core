"""Agent-performance measurements never pool agents from before the current generation."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import unittest
from unittest.mock import patch

from app.analytics import signal_lab as lab
from app.core.settings import settings
from app.side_agents import postmortem_inputs


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)

    def mappings(self):
        return self


class _Session:
    def __init__(self, rows_for):
        self.calls: list[tuple[str, dict]] = []
        self._rows_for = rows_for

    def execute(self, statement, params=None):
        sql = str(statement)
        self.calls.append((sql, dict(params or {})))
        return _Result(self._rows_for(sql))


def _scope(session):
    @contextmanager
    def scope(**_kwargs):
        yield session

    return scope


class SignalLabAgentWindowTests(unittest.TestCase):
    def _load(self, **kwargs):
        bars = [('005930', date(2026, 11, d), 100 + d, 101 + d, 1000) for d in (2, 3, 4, 5, 6)]
        session = _Session(lambda sql: bars if 'from daily_price_bars' in sql else [])
        with patch('app.db.session_scope', _scope(session)):
            panel = lab.load_panel('2026-04-14', '2026-11-06', horizons=(1,), **kwargs)
        agent_params = [params for sql, params in session.calls
                        if 'agent_context_feedback' in sql or 'signal_decider_decision_snapshots' in sql]
        factor_params = [params for sql, params in session.calls if 'factor_results' in sql]
        return panel, agent_params, factor_params

    def test_agent_rows_start_at_the_generation_while_universe_signals_keep_the_window(self) -> None:
        panel, agent_params, factor_params = self._load(agent_start='2026-11-07')
        self.assertEqual([p['a'] for p in agent_params], ['2026-11-07', '2026-11-07'])
        self.assertTrue(all(p['a'] == '2026-04-14' for p in factor_params))
        self.assertEqual(panel.attrs['agent_rows_from'], '2026-11-07')

    def test_without_a_generation_agent_rows_use_the_window(self) -> None:
        _, agent_params, _ = self._load()
        self.assertEqual([p['a'] for p in agent_params], ['2026-04-14', '2026-04-14'])

    def test_a_generation_before_the_window_does_not_widen_it(self) -> None:
        _, agent_params, _ = self._load(agent_start='2026-01-01')
        self.assertEqual([p['a'] for p in agent_params], ['2026-04-14', '2026-04-14'])

    def test_report_states_where_agent_rows_start(self) -> None:
        from tests.test_signal_lab import _synthetic

        panel = _synthetic(n_days=30)
        result = lab.evaluate_panel(panel, horizons=(1,))
        result['agent_rows_from'] = '2026-11-07'
        markdown = lab.render_markdown(result, start='2026-04-14', end='2026-11-20', universe_days=30)
        self.assertIn('agent rows (summaries, decisions) from: 2026-11-07', markdown)

    def test_default_generation_is_the_persona_replacement(self) -> None:
        self.assertEqual(settings.agent_generation_start_date, date(2026, 11, 7))


class PostmortemGenerationTests(unittest.TestCase):
    def _summary(self, today, generation_start):
        session = _Session(lambda sql: [])
        summary = postmortem_inputs.load_summary(
            today, weeks=6, session_scope=_scope(session), generation_start=generation_start
        )
        decision_params = session.calls[1][1]
        return summary, decision_params

    def test_decisions_before_the_generation_are_left_out(self) -> None:
        summary, params = self._summary(date(2026, 11, 21), date(2026, 11, 7))
        self.assertEqual(params['a'], date(2026, 11, 7))
        self.assertEqual(summary['period']['from'], '2026-11-07')
        self.assertEqual(summary['period']['agent_generation_start'], '2026-11-07')

    def test_an_old_generation_keeps_the_six_week_window(self) -> None:
        _, params = self._summary(date(2027, 3, 1), date(2026, 11, 7))
        self.assertEqual(params['a'], date(2027, 1, 18))

    def test_no_generation_pools_the_window(self) -> None:
        summary, params = self._summary(date(2026, 11, 21), None)
        self.assertEqual(params['a'], date(2026, 10, 10))
        self.assertIsNone(summary['period']['agent_generation_start'])


if __name__ == '__main__':
    unittest.main()
