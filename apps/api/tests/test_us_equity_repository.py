from contextlib import contextmanager
from datetime import date

from sqlalchemy.dialects import postgresql

from app.repositories import us_equity_repository as repository


class _EmptyResult:
    def all(self):
        return []


class _CapturingSession:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)
        return _EmptyResult()


def _capture_statements(monkeypatch):
    session = _CapturingSession()

    @contextmanager
    def scope():
        yield session

    monkeypatch.setattr(repository, 'session_scope', scope)
    return session


def _compiled_params(statement) -> dict:
    return statement.compile().params


def test_price_screen_candidates_are_scoped_to_collection_run(monkeypatch) -> None:
    session = _capture_statements(monkeypatch)

    repository.load_us_price_screen_candidates(
        market_date=date(2026, 8, 19),
        collection_run_id=77,
    )

    params = _compiled_params(session.statements[0])
    assert 77 in params.values()
    assert 'collection_run_id' in str(session.statements[0])


def test_agent_candidates_are_scoped_to_collection_run(monkeypatch) -> None:
    session = _capture_statements(monkeypatch)

    repository.load_us_agent_candidates(
        market_date=date(2026, 8, 19),
        collection_run_id=88,
    )

    params = _compiled_params(session.statements[0])
    assert 88 in params.values()
    assert 'collection_run_id' in str(session.statements[0])


def test_signal_feedback_is_always_scoped_to_explicit_agent_run(monkeypatch) -> None:
    session = _capture_statements(monkeypatch)

    repository.load_us_signal_feedback(
        market_date=date(2026, 8, 19),
        run_id='us-agent-run-1',
    )

    params = _compiled_params(session.statements[0])
    assert 'us-agent-run-1' in params.values()
    assert 'run_id' in str(session.statements[0])


def test_agent_feedback_upsert_preserves_same_symbol_across_two_runs(monkeypatch) -> None:
    stored_identities = {}
    statements = []

    class ScalarResult:
        def __init__(self, value):
            self.value = value

        def scalar_one(self):
            return self.value

    class FeedbackSession:
        def execute(self, statement):
            compiled = statement.compile(dialect=postgresql.dialect())
            statements.append(str(compiled))
            params = compiled.params
            run_id = next(
                value
                for value in params.values()
                if isinstance(value, str) and value.startswith('us-agent-run-')
            )
            identity = (run_id, params['market_date'], params['symbol_id'], params['agent_name'])
            stored_identities.setdefault(identity, len(stored_identities) + 1)
            return ScalarResult(stored_identities[identity])

    session = FeedbackSession()

    @contextmanager
    def scope():
        yield session

    monkeypatch.setattr(repository, 'session_scope', scope)
    common = {
        'market_date': date(2026, 8, 19),
        'symbol_id': 1,
        'agent_name': 'signal-decider',
        'context_payload': {},
        'response_payload': {},
        'normalized_feedback_payload': {},
    }

    first_id = repository.upsert_us_agent_feedback(run_id='us-agent-run-1', **common)
    second_id = repository.upsert_us_agent_feedback(run_id='us-agent-run-2', **common)

    assert first_id != second_id
    assert len(stored_identities) == 2
    assert all(
        'ON CONSTRAINT uq_us_agent_feedback_run_date_symbol_agent' in statement
        for statement in statements
    )
