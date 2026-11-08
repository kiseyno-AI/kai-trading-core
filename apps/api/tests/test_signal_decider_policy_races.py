from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.agent_context import signal_decider_loop_service as loop_module
from app.db_models import Base, SignalDeciderPolicyCandidate, SignalDeciderPolicyVersion


def test_activation_rejects_policy_retired_after_initial_validation(monkeypatch):
    engine = create_engine('sqlite+pysqlite:///:memory:')
    tables = [
        SignalDeciderPolicyVersion.__table__,
        SignalDeciderPolicyCandidate.__table__,
    ]
    Base.metadata.create_all(engine, tables=tables)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    @contextmanager
    def local_session_scope():
        with session_factory.begin() as session:
            yield session

    monkeypatch.setattr(loop_module, 'session_scope', local_session_scope)
    repository = loop_module.SignalDeciderLoopRepository()
    try:
        with local_session_scope() as session:
            for version, status in [('candidate_v2', 'candidate'), ('active_v3', 'active')]:
                session.add(SignalDeciderPolicyVersion(
                    policy_version=version,
                    status=status,
                    change_summary='Local policy race fixture.',
                    rule_bundle_payload={},
                    metadata_payload={},
                    created_by='test',
                ))

        # The service checks this snapshot before its replay. Another activation
        # can retire the target before the repository takes its write lock.
        observed = repository.get_policy_version(policy_version='candidate_v2')
        assert observed['status'] == 'candidate'
        with local_session_scope() as session:
            target = session.execute(select(SignalDeciderPolicyVersion).where(
                SignalDeciderPolicyVersion.policy_version == 'candidate_v2',
            )).scalar_one()
            target.status = 'retired'
            target.retired_at = datetime.now(timezone.utc)

        with pytest.raises(ValueError, match='retired policy versions cannot be re-activated'):
            repository.activate_policy_version(
                policy_version='candidate_v2',
                activated_by='test',
                activation_note=None,
                promotion_evidence={'replay_artifact_sha256': 'a' * 64},
                activated_at=datetime.now(timezone.utc),
            )

        with local_session_scope() as session:
            rows = session.execute(select(SignalDeciderPolicyVersion)).scalars().all()
            assert {row.policy_version: row.status for row in rows} == {
                'candidate_v2': 'retired',
                'active_v3': 'active',
            }
            assert all(row.metadata_payload == {} for row in rows)
    finally:
        engine.dispose()
