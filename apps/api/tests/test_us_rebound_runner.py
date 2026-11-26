from datetime import date, datetime
from contextlib import contextmanager
from decimal import Decimal
from functools import partial
import os
from pathlib import Path
import shutil
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db_models import USIndexInputSnapshot
from app.market.us_index_input_freeze import freeze_us_index_inputs, load_frozen_us_index_inputs
from app.market.us_index_preopen import run_us_index_preopen
from app.market.us_session import latest_completed_us_session, prior_completed_us_sessions
from app.overnight_rebound.runner import load_us_rebound_status, run_us_rebound_tick


KST = ZoneInfo('Asia/Seoul')


def forbidden(**kwargs):
    raise AssertionError('An unavailable US session must not start collection or execution')


def test_thanksgiving_ends_normally_without_reusing_the_previous_us_session(tmp_path):
    now = datetime(2026, 11, 27, 7, 15, tzinfo=KST)
    result = run_us_rebound_tick(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: now, collector=forbidden, loader=forbidden,
    )

    assert result['status'] == 'completed'
    assert result['evaluation']['status'] == 'NO_SIGNAL'
    assert result['evaluation']['reason'] == 'no_new_us_session'
    assert result['evaluation']['order_authority'] is False
    assert result['evaluation']['input_manifest_id'] is None
    replay = load_us_rebound_status(target_trade_date=now.date(), state_dir=tmp_path)
    assert replay['evaluation'] == result['evaluation']


def test_disabled_and_future_enablement_do_not_create_state_or_call_providers(tmp_path):
    for mode, enabled_from, reason in (
        ('disabled', date(2026, 11, 27), 'disabled'),
        ('observe', date(2026, 11, 30), 'before_enabled_session'),
    ):
        result = run_us_rebound_tick(
            mode=mode, enabled_from=enabled_from, state_dir=tmp_path,
            clock=lambda: datetime(2026, 11, 27, 7, 15, tzinfo=KST),
            collector=forbidden, loader=forbidden,
        )
        assert result['status'] == 'not_due'
        assert result['reason'] == reason
    assert list(tmp_path.iterdir()) == []


def test_collection_reuses_the_history_request_budget_before_the_existing_cutoff(tmp_path):
    calls = []

    def collect(**kwargs):
        calls.append(kwargs)
        return {'status': 'completed', 'data_complete': False, 'snapshot_ids': {}}

    result = run_us_rebound_tick(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: datetime(2026, 11, 30, 7, 5, tzinfo=KST), collector=collect,
    )

    assert result['status'] == 'collecting'
    assert calls[0]['include_history'] is True
    assert calls[0]['state_dir'] == tmp_path
    persisted = load_us_rebound_status(target_trade_date=date(2026, 11, 30), state_dir=tmp_path)
    assert persisted['status'] == 'collecting'
    assert persisted['last_tick']['checked_at'] == '2026-11-30T07:05:00+09:00'
    after_cutoff = run_us_rebound_tick(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: datetime(2026, 11, 30, 7, 12, tzinfo=KST), collector=forbidden,
    )
    assert after_cutoff['reason'] == 'awaiting_evaluation'


def test_full_collection_freeze_and_evaluation_recover_without_extra_provider_calls(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "evidence.sqlite"}')
    USIndexInputSnapshot.__table__.create(engine)

    @contextmanager
    def database():
        with Session(engine) as session:
            yield session
            session.commit()

    now = datetime(2026, 11, 30, 7, 1, tzinfo=KST)
    session = latest_completed_us_session(as_of=now)
    dates = sorted({day for window in (*prior_completed_us_sessions(session), session)
                    for day in (window.previous_session_date, window.session_date)})

    class Quotes:
        use_mock = False
        calls = []

        async def fetch_overseas_index_daily_prices(self, *, index_code, **kwargs):
            self.calls.append(index_code)
            prices = [Decimal(100 if position % 2 == 0 else 99) for position in range(21)]
            prices.append(Decimal({'SPX': 105, 'COMP': 106, 'SOX': 107}[index_code]))
            return {'rt_cd': '0', 'output1': {'stck_shrn_iscd': index_code}, 'output2': [
                {'stck_bsop_date': day.strftime('%Y%m%d'), 'ovrs_nmix_prpr': str(price)}
                for day, price in zip(dates, prices)
            ]}

    client = Quotes()
    options = dict(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: now,
        collector=partial(run_us_index_preopen, client=client, session_factory=database),
        freezer=partial(freeze_us_index_inputs, session_factory=database),
        loader=partial(load_frozen_us_index_inputs, session_factory=database),
    )
    captured = run_us_rebound_tick(**options)
    assert captured['collection']['input_manifest_id']
    now = now.replace(minute=5)
    assert run_us_rebound_tick(**options)['collection']['data_complete'] is True
    assert len(client.calls) == 3

    now = now.replace(minute=15)
    evaluated = run_us_rebound_tick(**options)
    assert evaluated['status'] == 'completed'
    assert evaluated['evaluation']['status'] == 'WATCH'
    assert evaluated['evaluation']['order_authority'] is False
    assert evaluated['evaluation']['input_manifest_id'] == captured['collection']['input_manifest_id']
    assert evaluated['evaluation']['features']['us_session_date'] == '2026-11-27'
    options.update(collector=forbidden, loader=forbidden)
    now = now.replace(minute=16)
    assert run_us_rebound_tick(**options)['evaluation'] == evaluated['evaluation']
    assert load_us_rebound_status(target_trade_date=now.date(), state_dir=tmp_path)['evaluation'] == evaluated['evaluation']
    engine.dispose()


def test_a_valid_old_record_cannot_be_relabelled_as_another_session(tmp_path):
    run_us_rebound_tick(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: datetime(2026, 11, 27, 7, 15, tzinfo=KST),
    )
    other = tmp_path / 'rebound' / 'real-2026-11-30'
    other.mkdir()
    shutil.copy2(tmp_path / 'rebound/real-2026-11-27/evaluation.json', other / 'evaluation.json')
    with pytest.raises(ValueError, match='identity'):
        load_us_rebound_status(target_trade_date=date(2026, 11, 30), state_dir=tmp_path)


def test_a_collection_exception_is_reported_only_in_the_independent_run(tmp_path):
    def failed(**kwargs):
        raise RuntimeError('private-provider-details')

    result = run_us_rebound_tick(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: datetime(2026, 11, 30, 7, 5, tzinfo=KST), collector=failed,
    )
    assert result['status'] == 'failed'
    assert result['error_type'] == 'RuntimeError'
    status = load_us_rebound_status(target_trade_date=date(2026, 11, 30), state_dir=tmp_path)
    assert status['status'] == 'failed'
    assert 'private-provider-details' not in str(status)


def test_missing_input_and_expiry_are_normal_and_do_not_recollect(tmp_path):
    now = datetime(2026, 11, 30, 7, 15, tzinfo=KST)
    result = run_us_rebound_tick(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: now, loader=lambda **kwargs: None, collector=forbidden,
    )
    assert result['status'] == 'unavailable'
    assert load_us_rebound_status(target_trade_date=now.date(), state_dir=tmp_path)['evaluation'] is None

    now = now.replace(hour=8, minute=40)
    result = run_us_rebound_tick(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: now, loader=forbidden, collector=forbidden,
    )
    assert result['status'] == 'completed'
    assert result['evaluation']['status'] == 'UNAVAILABLE'
    assert result['evaluation']['reason'] == 'evaluation_expired'
    assert result['evaluation']['order_authority'] is False


@pytest.mark.parametrize('state', ['failed', 'partial'])
def test_technical_collection_outcomes_remain_failures(tmp_path, state):
    result = run_us_rebound_tick(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: datetime(2026, 11, 30, 7, 5, tzinfo=KST),
        collector=lambda **kwargs: {'status': state, 'data_complete': False},
    )
    assert result['status'] == 'failed'
    assert load_us_rebound_status(target_trade_date=date(2026, 11, 30), state_dir=tmp_path)['status'] == 'failed'


def test_new_evaluation_cannot_complete_before_its_parent_links_are_durable(tmp_path, monkeypatch):
    original_open = os.open

    def deny_parent_sync(path, *args, **kwargs):
        if Path(path) == tmp_path / 'rebound':
            raise OSError('directory durability unavailable')
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(os, 'open', deny_parent_sync)
    result = run_us_rebound_tick(
        mode='observe', enabled_from=date(2026, 11, 27), state_dir=tmp_path,
        clock=lambda: datetime(2026, 11, 27, 7, 15, tzinfo=KST),
    )
    assert result['status'] == 'failed'
    assert result['error_type'] == 'OSError'
    assert not (tmp_path / 'rebound/real-2026-11-27/evaluation.json').exists()
