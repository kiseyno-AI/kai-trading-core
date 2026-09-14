import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import textwrap
import threading
from unittest.mock import patch

import httpx
import pytest

from app.brokers.kis.client import (
    KISAPIError,
    KISClient,
    KISConfigurationError,
    KISHTTPError,
    KISTimeoutError,
    KISTransportError,
)
from app.brokers.kis.token_cache import (
    KISTokenCacheError,
    KISTokenCacheStore,
    run_token_cache_io,
)
from app.core.settings import Settings, settings


def _token_response(access_token: str = 'token-1') -> httpx.Response:
    return httpx.Response(
        200,
        json={
            'access_token': access_token,
            'token_type': 'Bearer',
            'expires_in': 3600,
        },
        request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
    )


def _store(
    cache_directory: Path,
    *,
    minimum_issue_interval_seconds: int = 60,
    issuance_lease_seconds: int = 90,
) -> KISTokenCacheStore:
    return KISTokenCacheStore(
        directory=str(cache_directory),
        environment='real',
        app_key='test-app-key',
        app_secret='test-app-secret',
        minimum_issue_interval_seconds=minimum_issue_interval_seconds,
        issuance_lease_seconds=issuance_lease_seconds,
    )


def _settings_overrides(cache_directory: Path, **overrides: object) -> dict[str, object]:
    return {
        'kis_app_key': 'test-app-key',
        'kis_app_secret': 'test-app-secret',
        'kis_token_cache_directory': str(cache_directory),
        **overrides,
    }


def test_concurrent_cold_calls_share_one_token_request(tmp_path) -> None:
    async def exercise() -> tuple[list[dict], int]:
        client = KISClient(use_mock=False)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.02)
            return _token_response()

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(tmp_path),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            results = await asyncio.gather(*(client.issue_access_token() for _ in range(8)))
        return results, call_count

    results, call_count = asyncio.run(exercise())

    assert call_count == 1
    assert {result['access_token'] for result in results} == {'token-1'}
    assert sum(result['cached'] is False for result in results) == 1


def test_process_singleton_client_supports_recreated_event_loops(tmp_path) -> None:
    client = KISClient(use_mock=False)
    call_count = 0

    async def fake_request(*args, **kwargs) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.02)
        return _token_response(f'token-{call_count}')

    async def issue_concurrently() -> list[dict]:
        return await asyncio.gather(
            client.issue_access_token(),
            client.issue_access_token(),
        )

    with patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request):
        with patch.dict(
            settings.__dict__,
            _settings_overrides(tmp_path / 'first-loop-cache'),
            clear=False,
        ):
            first_results = asyncio.run(issue_concurrently())
        with patch.dict(
            settings.__dict__,
            _settings_overrides(tmp_path / 'second-loop-cache'),
            clear=False,
        ):
            second_results = asyncio.run(issue_concurrently())

    assert call_count == 2
    assert {result['access_token'] for result in first_results} == {'token-1'}
    assert {result['access_token'] for result in second_results} == {'token-2'}
    assert sum(result['cached'] is False for result in first_results) == 1
    assert sum(result['cached'] is False for result in second_results) == 1


def test_concurrent_failure_is_shared_during_cooldown(tmp_path) -> None:
    async def exercise() -> tuple[list[object], object, int]:
        client = KISClient(use_mock=False)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.02)
            return httpx.Response(
                429,
                json={'msg1': 'rate limited'},
                request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
            )

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(tmp_path),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            results = await asyncio.gather(
                *(client.issue_access_token() for _ in range(5)),
                return_exceptions=True,
            )
            try:
                await client.issue_access_token()
            except Exception as exc:  # noqa: BLE001 - the assertion verifies the public error type.
                cooldown_result: object = exc
            else:
                cooldown_result = None
        return results, cooldown_result, call_count

    results, cooldown_result, call_count = asyncio.run(exercise())

    assert call_count == 1
    assert all(isinstance(result, KISHTTPError) for result in results), [
        type(result).__name__ for result in results
    ]
    assert isinstance(cooldown_result, KISHTTPError)
    assert cooldown_result.status_code == 429


def test_token_request_has_total_deadline_and_shares_timeout_cooldown(tmp_path) -> None:
    async def exercise() -> tuple[object, object, int, bool, float]:
        client = KISClient(use_mock=False)
        call_count = 0
        cancelled = False

        async def never_complete(*args, **kwargs) -> httpx.Response:
            nonlocal call_count, cancelled
            call_count += 1
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                cancelled = True
                raise
            return _token_response('must-not-be-issued')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(tmp_path / 'cache'),
            'kis_token_request_timeout_seconds': 0.02,
            'kis_token_issuance_lease_seconds': 90,
            'kis_token_min_issue_interval_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=never_complete),
        ):
            loop = asyncio.get_running_loop()
            started_at = loop.time()
            first = await asyncio.gather(
                client.issue_access_token(),
                return_exceptions=True,
            )
            second = await asyncio.gather(
                client.issue_access_token(),
                return_exceptions=True,
            )
            elapsed = loop.time() - started_at
        return first[0], second[0], call_count, cancelled, elapsed

    first, second, call_count, cancelled, elapsed = asyncio.run(exercise())

    assert isinstance(first, KISTimeoutError)
    assert isinstance(second, KISTimeoutError)
    assert call_count == 1
    assert cancelled is True
    assert elapsed < 0.5


def test_new_client_reuses_unexpired_persisted_token(tmp_path) -> None:
    async def exercise() -> tuple[dict, dict, int]:
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('persisted-token')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(tmp_path / 'cache'),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            first = await KISClient(use_mock=False).issue_access_token()
            second = await KISClient(use_mock=False).issue_access_token()
        return first, second, call_count

    first, second, call_count = asyncio.run(exercise())

    assert call_count == 1
    assert first['access_token'] == 'persisted-token'
    assert first['cached'] is False
    assert second['access_token'] == 'persisted-token'
    assert second['cached'] is True
    assert 3200 <= second['expires_in'] <= 3300

    cache_directory = tmp_path / 'cache'
    token_files = list(cache_directory.glob('*.token.json'))
    assert len(token_files) == 1
    payload = json.loads(token_files[0].read_text())
    assert payload['schema_version'] == 1
    assert payload['provider'] == 'kis'
    assert payload['environment'] == 'real'
    assert payload['credential_fingerprint'].startswith('hmac-sha256:')
    assert payload['generation']
    assert payload['access_token'] == 'persisted-token'
    provider_expires_at = datetime.fromisoformat(payload['expires_at'])
    refresh_after = datetime.fromisoformat(payload['refresh_after'])
    assert provider_expires_at > refresh_after
    assert (provider_expires_at - refresh_after).total_seconds() == 300
    assert 'test-app-key' not in token_files[0].read_text()
    assert 'test-app-secret' not in token_files[0].read_text()
    assert stat.S_IMODE(cache_directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(token_files[0].stat().st_mode) == 0o600


def test_concurrent_clients_share_one_process_file_lock(tmp_path) -> None:
    async def exercise() -> tuple[list[dict], int]:
        clients = [KISClient(use_mock=False) for _ in range(6)]
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.02)
            return _token_response('lease-token')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(tmp_path / 'cache'),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            results = await asyncio.gather(*(client.issue_access_token() for client in clients))
        return results, call_count

    results, call_count = asyncio.run(exercise())

    assert call_count == 1
    assert {result['access_token'] for result in results} == {'lease-token'}


def test_failure_cooldown_is_shared_across_clients(tmp_path) -> None:
    async def exercise() -> tuple[list[object], object, int]:
        clients = [KISClient(use_mock=False) for _ in range(4)]
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.02)
            return httpx.Response(
                429,
                json={'msg1': 'rate limited'},
                request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
            )

        cache_directory = tmp_path / 'cache'
        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
            'kis_token_failure_cooldown_seconds': 60,
            'kis_token_issuance_lease_seconds': 90,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            results = await asyncio.gather(
                *(client.issue_access_token() for client in clients),
                return_exceptions=True,
            )
            try:
                await KISClient(use_mock=False).issue_access_token()
            except Exception as exc:  # noqa: BLE001 - public type is asserted below.
                cooldown_result: object = exc
            else:
                cooldown_result = None
        return results, cooldown_result, call_count

    results, cooldown_result, call_count = asyncio.run(exercise())

    assert call_count == 1
    assert all(isinstance(result, KISHTTPError) for result in results), [
        type(result).__name__ for result in results
    ]
    assert isinstance(cooldown_result, KISHTTPError)
    assert cooldown_result.status_code == 429
    failure_files = list((tmp_path / 'cache').glob('*.failure.json'))
    assert len(failure_files) == 1
    failure_payload = json.loads(failure_files[0].read_text())
    assert failure_payload['schema_version'] == 1
    assert failure_payload['provider'] == 'kis'
    assert failure_payload['environment'] == 'real'
    assert failure_payload['error_kind'] == 'http'
    assert failure_payload['status_code'] == 429
    assert stat.S_IMODE(failure_files[0].stat().st_mode) == 0o600


def test_timeout_failure_cooldown_preserves_minimum_issue_interval(tmp_path) -> None:
    async def exercise() -> tuple[object, int, dict]:
        cache_directory = tmp_path / 'cache'
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            raise httpx.ReadTimeout('ambiguous token response')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
            'kis_token_failure_cooldown_seconds': 60,
            'kis_token_request_timeout_seconds': 15,
            'kis_token_min_issue_interval_seconds': 120,
            'kis_token_issuance_lease_seconds': 135,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        failure_file = next(cache_directory.glob('*.failure.json'))
        return result[0], call_count, json.loads(failure_file.read_text())

    result, call_count, failure_payload = asyncio.run(exercise())

    assert isinstance(result, KISTimeoutError)
    assert call_count == 1
    failed_at = datetime.fromisoformat(failure_payload['failed_at'])
    cooldown_until = datetime.fromisoformat(failure_payload['cooldown_until'])
    assert (cooldown_until - failed_at).total_seconds() == 120


def test_concurrent_force_refresh_reuses_new_generation_written_by_peer(tmp_path) -> None:
    async def exercise() -> tuple[list[dict], int]:
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.02)
            return _token_response('token-2')

        cache_directory = tmp_path / 'cache'
        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            store = KISTokenCacheStore(
                directory=str(cache_directory),
                environment='real',
                app_key='test-app-key',
                app_secret='test-app-secret',
            )
            issued_at = datetime.now(timezone.utc) - timedelta(minutes=10)
            store.save_token(
                access_token='token-1',
                token_type='Bearer',
                issued_at=issued_at,
                expires_at=issued_at + timedelta(hours=1),
                refresh_after=issued_at + timedelta(minutes=55),
            )
            clients = [KISClient(use_mock=False), KISClient(use_mock=False)]
            refreshed = await asyncio.gather(
                *(client.issue_access_token(force_refresh=True) for client in clients)
            )
        return refreshed, call_count

    results, call_count = asyncio.run(exercise())

    assert call_count == 1
    assert [result['access_token'] for result in results] == ['token-2', 'token-2']
    assert sum(result['cached'] is False for result in results) == 1


def test_sequential_force_refresh_cannot_bypass_minimum_issue_interval(tmp_path) -> None:
    async def exercise() -> tuple[dict, dict, int]:
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response(f'fresh-token-{call_count}')

        cache_directory = tmp_path / 'cache'
        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
            'kis_token_failure_cooldown_seconds': 60,
            'kis_token_min_issue_interval_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            store = KISTokenCacheStore(
                directory=str(cache_directory),
                environment='real',
                app_key='test-app-key',
                app_secret='test-app-secret',
            )
            issued_at = datetime.now(timezone.utc) - timedelta(minutes=10)
            store.save_token(
                access_token='old-token',
                token_type='Bearer',
                issued_at=issued_at,
                expires_at=issued_at + timedelta(hours=1),
                refresh_after=issued_at + timedelta(minutes=55),
            )
            first = await KISClient(use_mock=False).issue_access_token(force_refresh=True)
            second = await KISClient(use_mock=False).issue_access_token(force_refresh=True)
        return first, second, call_count

    first, second, call_count = asyncio.run(exercise())

    assert call_count == 1
    assert first['access_token'] == 'fresh-token-1'
    assert first['cached'] is False
    assert second['access_token'] == 'fresh-token-1'
    assert second['cached'] is True
    assert second['refresh_deferred'] is True
    assert second['defer_reason'] == 'minimum_issue_interval'


def test_in_memory_failure_cooldown_is_scoped_to_credentials(tmp_path) -> None:
    async def exercise() -> tuple[dict, int]:
        client = KISClient(use_mock=False)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(
                    429,
                    json={'msg1': 'rate limited'},
                    request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
                )
            return _token_response('other-app-token')

        common = {
            'kis_token_cache_directory': str(tmp_path / 'cache'),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, common, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            with patch.dict(
                settings.__dict__,
                {'kis_app_key': 'first-app-key', 'kis_app_secret': 'first-app-secret'},
                clear=False,
            ):
                first = await asyncio.gather(client.issue_access_token(), return_exceptions=True)
                assert isinstance(first[0], KISHTTPError)
            with patch.dict(
                settings.__dict__,
                {'kis_app_key': 'second-app-key', 'kis_app_secret': 'second-app-secret'},
                clear=False,
            ):
                second = await client.issue_access_token()
        return second, call_count

    result, call_count = asyncio.run(exercise())

    assert call_count == 2
    assert result['access_token'] == 'other-app-token'


def test_cancelled_lock_waiter_does_not_leak_cross_process_lock(tmp_path) -> None:
    async def exercise() -> None:
        store = KISTokenCacheStore(
            directory=str(tmp_path / 'cache'),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        first_descriptor = store.open_lock()
        assert store.try_acquire_lock(first_descriptor)

        async def wait_for_lock() -> None:
            async with store.lock(poll_interval_seconds=0.005):
                raise AssertionError('cancelled waiter must not enter the lock')

        waiter = asyncio.create_task(wait_for_lock())
        await asyncio.sleep(0.02)
        waiter.cancel()
        try:
            await waiter
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError('lock waiter was not cancelled')
        store.release_lock(first_descriptor)

        async with asyncio.timeout(0.5):
            async with store.lock(poll_interval_seconds=0.005):
                return

    asyncio.run(exercise())


def test_cache_directory_rejects_symlink_parent_without_requesting_token(tmp_path) -> None:
    redirected_parent = tmp_path / 'redirected'
    redirected_parent.mkdir()
    symlink_parent = tmp_path / 'unsafe-parent'
    symlink_parent.symlink_to(redirected_parent, target_is_directory=True)

    async def exercise() -> tuple[object, int]:
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('must-not-be-issued')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(symlink_parent / 'cache'),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return result[0], call_count

    result, call_count = asyncio.run(exercise())

    assert isinstance(result, KISConfigurationError)
    assert call_count == 0
    assert not (redirected_parent / 'cache').exists()


def test_cancelled_token_request_leaves_shared_issuance_lease(tmp_path) -> None:
    async def exercise() -> tuple[object, int, dict]:
        request_started = asyncio.Event()
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                request_started.set()
                await asyncio.Event().wait()
                raise AssertionError('unreachable')
            return _token_response('unsafe-retry-token')

        cache_directory = tmp_path / 'cache'
        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            issuing = asyncio.create_task(KISClient(use_mock=False).issue_access_token())
            await asyncio.wait_for(request_started.wait(), timeout=0.5)
            issuing.cancel()
            try:
                await issuing
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError('token request was not cancelled')
            blocked = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        lease_files = list(cache_directory.glob('*.failure.json'))
        assert len(lease_files) == 1
        return blocked[0], call_count, json.loads(lease_files[0].read_text())

    blocked, call_count, lease_payload = asyncio.run(exercise())

    assert call_count == 1
    assert isinstance(blocked, KISTransportError)
    assert lease_payload['record_type'] == 'issuance_attempt'
    assert lease_payload['attempt_id']
    assert lease_payload['environment'] == 'real'
    started_at = datetime.fromisoformat(lease_payload['started_at'])
    cooldown_until = datetime.fromisoformat(lease_payload['cooldown_until'])
    assert (cooldown_until - started_at).total_seconds() >= 90


def test_malformed_success_response_enters_shared_protocol_cooldown(tmp_path) -> None:
    async def exercise() -> tuple[list[object], int, dict]:
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(
                200,
                json={
                    'access_token': '',
                    'token_type': 'Bearer',
                    'expires_in': 'not-a-number',
                },
                request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
            )

        cache_directory = tmp_path / 'cache'
        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            first = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
            second = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        failure_files = list(cache_directory.glob('*.failure.json'))
        assert len(failure_files) == 1
        return [first[0], second[0]], call_count, json.loads(failure_files[0].read_text())

    results, call_count, failure_payload = asyncio.run(exercise())

    assert call_count == 1
    assert all(isinstance(result, KISAPIError) for result in results), [
        type(result).__name__ for result in results
    ]
    assert failure_payload['record_type'] == 'failure'
    assert failure_payload['error_kind'] == 'protocol'


def test_default_token_cache_path_is_user_scoped_and_absolute() -> None:
    default_directory = str(Settings.model_fields['kis_token_cache_directory'].default)

    assert Path(default_directory).expanduser().is_absolute()


def test_token_issuance_lease_covers_timeout_and_minimum_interval() -> None:
    valid = Settings(
        kis_token_request_timeout_seconds=15,
        kis_token_min_issue_interval_seconds=60,
        kis_token_issuance_lease_seconds=75,
    )

    assert valid.kis_token_issuance_lease_seconds == 75
    with pytest.raises(ValueError, match='KIS_TOKEN_ISSUANCE_LEASE_SECONDS'):
        Settings(
            kis_token_request_timeout_seconds=15,
            kis_token_min_issue_interval_seconds=61,
            kis_token_issuance_lease_seconds=75,
        )


def test_unsafe_or_ambiguous_cache_directory_values_are_rejected() -> None:
    unsafe_values = ('', '.', '..', '/', 'relative/cache', '/tmp/../unsafe-cache')
    accepted: list[str] = []

    for directory in unsafe_values:
        try:
            KISTokenCacheStore(
                directory=directory,
                environment='real',
                app_key='test-app-key',
                app_secret='test-app-secret',
            )
        except KISTokenCacheError:
            continue
        accepted.append(directory)

    assert accepted == []


def test_token_cache_v1_has_stable_interop_identity_and_json_contract(tmp_path) -> None:
    store = KISTokenCacheStore(
        directory=str(tmp_path / 'cache'),
        environment='real',
        app_key='test-app-key',
        app_secret='test-app-secret',
    )
    issued_at = datetime(2026, 9, 9, 1, 2, 3, tzinfo=timezone.utc)
    expires_at = issued_at + timedelta(hours=24)
    refresh_after = expires_at - timedelta(minutes=5)

    record = store.save_token(
        access_token='interop-token',
        token_type='Bearer',
        issued_at=issued_at,
        expires_at=expires_at,
        refresh_after=refresh_after,
    )
    payload = json.loads(store.token_path.read_text())

    expected_digest = 'ed79e64233bdae81aa466510d22091f34a2019beee1773c697db68151f10125e'
    assert store.credential_fingerprint == f'hmac-sha256:{expected_digest}'
    assert store.token_path.name == f'real-{expected_digest[:32]}.token.json'
    assert payload == {
        'schema_version': 1,
        'record_type': 'token',
        'provider': 'kis',
        'environment': 'real',
        'credential_fingerprint': f'hmac-sha256:{expected_digest}',
        'generation': record.generation,
        'access_token': 'interop-token',
        'token_type': 'Bearer',
        'issued_at': issued_at.isoformat(),
        'expires_at': expires_at.isoformat(),
        'refresh_after': refresh_after.isoformat(),
    }


@pytest.mark.parametrize('save_kind', ('token', 'issuance_attempt', 'failure'))
def test_shared_cache_public_writes_reject_naive_datetimes(tmp_path, save_kind: str) -> None:
    store = _store(tmp_path / 'cache')
    naive_now = datetime.now()
    if save_kind == 'token':
        operation = lambda: store.save_token(
            access_token='naive-cache-token',
            token_type='Bearer',
            issued_at=naive_now,
            expires_at=naive_now + timedelta(hours=1),
            refresh_after=naive_now + timedelta(minutes=30),
        )
    elif save_kind == 'issuance_attempt':
        operation = lambda: store.save_issuance_attempt(
            started_at=naive_now,
            cooldown_until=naive_now + timedelta(seconds=90),
            invalidates_generation=None,
        )
    else:
        operation = lambda: store.save_failure(
            error_kind='transport',
            status_code=503,
            retryable=True,
            failed_at=naive_now,
            cooldown_until=naive_now + timedelta(seconds=60),
        )

    with pytest.raises(KISTokenCacheError, match='timezone-aware'):
        operation()


@pytest.mark.parametrize(
    'record_type',
    ('token', 'revocation', 'issuance_attempt', 'failure', 'issuance_success'),
)
@pytest.mark.parametrize('mutation', ('extra', 'missing', 'unknown_record_type'))
def test_shared_cache_records_require_exact_record_type_shape(
    tmp_path,
    record_type: str,
    mutation: str,
) -> None:
    store = _store(tmp_path / 'cache')
    now = datetime.now(timezone.utc)
    if record_type in {'token', 'revocation'}:
        record = store.save_token(
            access_token='strict-schema-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=30),
        )
        if record_type == 'revocation':
            assert asyncio.run(
                store.invalidate_if_matches(
                    access_token=record.access_token,
                    generation=record.generation,
                    minimum_issue_interval_seconds=60,
                )
            )
        path = store.token_path
        load = store.load_cache_state
    elif record_type == 'issuance_attempt':
        store.save_issuance_attempt(
            started_at=now,
            cooldown_until=now + timedelta(seconds=90),
            invalidates_generation=None,
        )
        path = store.failure_path
        load = lambda: store.load_failure(include_expired=True)
    elif record_type == 'failure':
        store.save_failure(
            error_kind='transport',
            status_code=503,
            retryable=True,
            failed_at=now,
            cooldown_until=now + timedelta(seconds=60),
        )
        path = store.failure_path
        load = lambda: store.load_failure(include_expired=True)
    else:
        store.save_token_after_issuance(
            access_token='strict-success-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=30),
            minimum_issue_interval_seconds=60,
        )
        path = store.failure_path
        load = lambda: store.load_failure(include_expired=True)

    payload = json.loads(path.read_text(encoding='utf-8'))
    if mutation == 'extra':
        payload['unexpected'] = 'schema drift'
    elif mutation == 'missing':
        required_field = {
            'token': 'access_token',
            'revocation': 'revoked_generation',
            'issuance_attempt': 'attempt_id',
            'failure': 'error_kind',
            'issuance_success': 'generation',
        }[record_type]
        payload.pop(required_field)
    else:
        payload['record_type'] = 'unknown'
    path.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(KISTokenCacheError):
        load()


def test_token_success_payload_rejects_noncanonical_values_and_shares_cooldown(tmp_path) -> None:
    invalid_payloads = [
        {'access_token': 'token', 'token_type': 'Bearer', 'expires_in': float('inf')},
        {'access_token': 'token', 'token_type': 'Bearer', 'expires_in': 3600.5},
        {'access_token': 123, 'token_type': 'Bearer', 'expires_in': 3600},
        {'access_token': 'token\nvalue', 'token_type': 'Bearer', 'expires_in': 3600},
        {'access_token': 'tést', 'token_type': 'Bearer', 'expires_in': 3600},
        {'access_token': 'token\x7f', 'token_type': 'Bearer', 'expires_in': 3600},
        {'access_token': 'token', 'token_type': 123, 'expires_in': 3600},
        {'access_token': 'token', 'token_type': 'Bearer', 'expires_in': 10**30},
        {'access_token': 'token', 'token_type': 'Bearer', 'expires_in': '9' * 10_000},
    ]

    async def exercise(payload: dict, index: int) -> tuple[list[object], int]:
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(
                200,
                content=json.dumps(payload).encode('utf-8'),
                headers={'content-type': 'application/json'},
                request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
            )

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(tmp_path / f'cache-{index}'),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            first = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
            second = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return [first[0], second[0]], call_count

    for index, payload in enumerate(invalid_payloads):
        results, call_count = asyncio.run(exercise(payload, index))
        assert call_count == 1, payload
        assert all(isinstance(result, KISAPIError) for result in results), (
            payload,
            [type(result).__name__ for result in results],
        )


def test_unbounded_failure_marker_fails_closed_without_token_issuance(tmp_path) -> None:
    async def exercise() -> tuple[object, int]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        failed_at = datetime.now(timezone.utc)
        store.save_failure(
            error_kind='http',
            status_code=429,
            retryable=False,
            failed_at=failed_at,
            cooldown_until=failed_at + timedelta(seconds=60),
        )
        payload = json.loads(store.failure_path.read_text())
        payload['cooldown_until'] = (failed_at + timedelta(days=365)).isoformat()
        store.failure_path.write_text(json.dumps(payload), encoding='utf-8')
        store.failure_path.chmod(0o600)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('recovered-token')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
            'kis_token_failure_cooldown_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return result[0], call_count

    result, call_count = asyncio.run(exercise())

    assert call_count == 0
    assert isinstance(result, KISConfigurationError)


def test_minimum_issue_interval_applies_after_refresh_boundary(tmp_path) -> None:
    async def exercise() -> tuple[dict, object, int]:
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('must-not-be-issued')

        common = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_failure_cooldown_seconds': 60,
            'kis_token_min_issue_interval_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, common, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            now = datetime.now(timezone.utc)

            reusable_directory = tmp_path / 'reusable-cache'
            settings.__dict__['kis_token_cache_directory'] = str(reusable_directory)
            reusable_store = KISTokenCacheStore(
                directory=str(reusable_directory),
                environment='real',
                app_key='test-app-key',
                app_secret='test-app-secret',
            )
            reusable_store.save_token(
                access_token='still-valid-token',
                token_type='Bearer',
                issued_at=now - timedelta(seconds=30),
                expires_at=now + timedelta(minutes=5),
                refresh_after=now - timedelta(seconds=1),
            )
            deferred = await KISClient(use_mock=False).issue_access_token()

            expired_directory = tmp_path / 'expired-cache'
            settings.__dict__['kis_token_cache_directory'] = str(expired_directory)
            expired_store = KISTokenCacheStore(
                directory=str(expired_directory),
                environment='real',
                app_key='test-app-key',
                app_secret='test-app-secret',
            )
            expired_store.save_token(
                access_token='expired-token',
                token_type='Bearer',
                issued_at=now - timedelta(seconds=30),
                expires_at=now - timedelta(seconds=10),
                refresh_after=now - timedelta(seconds=15),
            )
            expired = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return deferred, expired[0], call_count

    deferred, expired, call_count = asyncio.run(exercise())

    assert call_count == 0
    assert deferred['access_token'] == 'still-valid-token'
    assert deferred['refresh_deferred'] is True
    assert deferred['defer_reason'] == 'minimum_issue_interval'
    assert isinstance(expired, KISTransportError)


def test_unsafe_or_corrupt_token_cache_files_fail_closed(tmp_path) -> None:
    async def exercise(case: str, index: int) -> tuple[object, int]:
        cache_directory = tmp_path / f'cache-{index}'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        now = datetime.now(timezone.utc)
        store.save_token(
            access_token='cached-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
        )

        if case == 'public-mode':
            store.token_path.chmod(0o644)
        elif case == 'corrupt-json':
            store.token_path.write_text('{not-json', encoding='utf-8')
            store.token_path.chmod(0o600)
        elif case == 'deep-json':
            store.token_path.write_text('[' * 2_000 + '0' + ']' * 2_000, encoding='utf-8')
            store.token_path.chmod(0o600)
        elif case in {
            'wrong-environment',
            'wrong-fingerprint',
            'non-ascii-token',
            'nonfinite-json',
            'boolean-schema',
            'invalid-generation',
            'future-issued-at',
            'excessive-lifetime',
            'invalid-refresh-order',
        }:
            payload = json.loads(store.token_path.read_text())
            if case == 'wrong-environment':
                payload['environment'] = 'demo'
            elif case == 'wrong-fingerprint':
                payload['credential_fingerprint'] = 'hmac-sha256:' + ('0' * 64)
            elif case == 'non-ascii-token':
                payload['access_token'] = 'tést'
            elif case == 'nonfinite-json':
                payload['refresh_after'] = float('nan')
            elif case == 'boolean-schema':
                payload['schema_version'] = True
            elif case == 'invalid-generation':
                payload['generation'] = 'not-a-uuid'
            elif case == 'future-issued-at':
                future = datetime.now(timezone.utc) + timedelta(minutes=10)
                payload['issued_at'] = future.isoformat()
                payload['refresh_after'] = (future + timedelta(minutes=30)).isoformat()
                payload['expires_at'] = (future + timedelta(hours=1)).isoformat()
            elif case == 'excessive-lifetime':
                issued = datetime.fromisoformat(payload['issued_at'])
                payload['refresh_after'] = (issued + timedelta(days=2)).isoformat()
                payload['expires_at'] = (issued + timedelta(days=3)).isoformat()
            else:
                payload['refresh_after'] = (
                    datetime.fromisoformat(payload['expires_at']) + timedelta(seconds=1)
                ).isoformat()
            store.token_path.write_text(json.dumps(payload), encoding='utf-8')
            store.token_path.chmod(0o600)
        elif case == 'duplicate-key':
            serialized = store.token_path.read_text()
            store.token_path.write_text(
                f'{serialized[:-1]},"environment":"real"}}',
                encoding='utf-8',
            )
            store.token_path.chmod(0o600)
        elif case == 'symlink':
            victim = tmp_path / f'victim-{index}.json'
            victim.write_text(store.token_path.read_text(), encoding='utf-8')
            victim.chmod(0o600)
            store.token_path.unlink()
            store.token_path.symlink_to(victim)
        elif case == 'hardlink':
            victim = tmp_path / f'victim-{index}.json'
            victim.write_text(store.token_path.read_text(), encoding='utf-8')
            victim.chmod(0o600)
            store.token_path.unlink()
            os.link(victim, store.token_path)
        elif case == 'fifo':
            store.token_path.unlink()
            os.mkfifo(store.token_path, mode=0o600)
        else:
            raise AssertionError(case)

        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('must-not-be-issued')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return result[0], call_count

    for index, case in enumerate(
        (
            'public-mode',
            'corrupt-json',
            'deep-json',
            'wrong-environment',
            'wrong-fingerprint',
            'non-ascii-token',
            'nonfinite-json',
            'boolean-schema',
            'invalid-generation',
            'future-issued-at',
            'excessive-lifetime',
            'invalid-refresh-order',
            'duplicate-key',
            'symlink',
            'hardlink',
            'fifo',
        )
    ):
        result, call_count = asyncio.run(exercise(case, index))
        assert isinstance(result, KISConfigurationError), (case, result)
        assert call_count == 0, case


def test_existing_private_token_file_rejects_unknown_optional_fields(tmp_path) -> None:
    async def exercise() -> tuple[object, int]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        now = datetime.now(timezone.utc)
        store.save_token(
            access_token='safe-existing-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
        )
        payload = json.loads(store.token_path.read_text())
        payload['writer_metadata'] = {'implementation': 'exec', 'revision': 1}
        store.token_path.write_text(json.dumps(payload), encoding='utf-8')
        store.token_path.chmod(0o600)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('must-not-be-issued')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return result[0], call_count

    result, call_count = asyncio.run(exercise())

    assert isinstance(result, KISConfigurationError)
    assert call_count == 0


def test_real_demo_and_credential_token_caches_are_isolated(tmp_path) -> None:
    async def exercise() -> tuple[list[str], int]:
        cache_directory = tmp_path / 'cache'
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response(f'isolated-token-{call_count}')

        common = {'kis_token_cache_directory': str(cache_directory)}
        first_credentials = {
            'kis_app_key': 'first-app-key',
            'kis_app_secret': 'first-app-secret',
        }
        second_credentials = {
            'kis_app_key': 'second-app-key',
            'kis_app_secret': 'second-app-secret',
        }
        with (
            patch.dict(settings.__dict__, common, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            with patch.dict(settings.__dict__, first_credentials, clear=False):
                real = await KISClient(use_mock=False).issue_access_token()
                demo = await KISClient(use_mock=True).issue_access_token()
            with patch.dict(settings.__dict__, second_credentials, clear=False):
                other_app = await KISClient(use_mock=False).issue_access_token()
            with patch.dict(settings.__dict__, first_credentials, clear=False):
                real_reused = await KISClient(use_mock=False).issue_access_token()
                demo_reused = await KISClient(use_mock=True).issue_access_token()
        return [
            real['access_token'],
            demo['access_token'],
            other_app['access_token'],
            real_reused['access_token'],
            demo_reused['access_token'],
        ], call_count

    tokens, call_count = asyncio.run(exercise())

    assert tokens == [
        'isolated-token-1',
        'isolated-token-2',
        'isolated-token-3',
        'isolated-token-1',
        'isolated-token-2',
    ]
    assert call_count == 3


def test_invalid_failure_cache_records_fail_closed(tmp_path) -> None:
    async def exercise(case: str, index: int) -> tuple[object, int]:
        cache_directory = tmp_path / f'failure-cache-{index}'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        now = datetime.now(timezone.utc)
        store.save_failure(
            error_kind='http',
            status_code=429,
            retryable=True,
            failed_at=now,
            cooldown_until=now + timedelta(seconds=60),
        )
        if case == 'duplicate-key':
            serialized = store.failure_path.read_text()
            store.failure_path.write_text(
                f'{serialized[:-1]},"status_code":429}}',
                encoding='utf-8',
            )
        else:
            payload = json.loads(store.failure_path.read_text())
            if case == 'nonfinite-json':
                payload['status_code'] = float('nan')
            elif case == 'boolean-status':
                payload['status_code'] = True
            elif case == 'wrong-identity':
                payload['credential_fingerprint'] = 'hmac-sha256:' + ('0' * 64)
            elif case == 'boolean-schema':
                payload['schema_version'] = True
            else:
                raise AssertionError(case)
            store.failure_path.write_text(json.dumps(payload), encoding='utf-8')
        store.failure_path.chmod(0o600)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('must-not-be-issued')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return result[0], call_count

    for index, case in enumerate(
        (
            'duplicate-key',
            'nonfinite-json',
            'boolean-status',
            'wrong-identity',
            'boolean-schema',
        )
    ):
        result, call_count = asyncio.run(exercise(case, index))
        assert isinstance(result, KISConfigurationError), (case, result)
        assert call_count == 0, case


def test_world_writable_non_sticky_ancestor_is_rejected_without_chmod(tmp_path) -> None:
    unsafe_ancestor = tmp_path / 'shared'
    unsafe_ancestor.mkdir(mode=0o777)
    unsafe_ancestor.chmod(0o777)
    cache_directory = unsafe_ancestor / 'cache'
    store = KISTokenCacheStore(
        directory=str(cache_directory),
        environment='real',
        app_key='test-app-key',
        app_secret='test-app-secret',
    )

    with pytest.raises(KISTokenCacheError):
        store.open_lock()

    assert stat.S_IMODE(unsafe_ancestor.stat().st_mode) == 0o777
    assert not cache_directory.exists()


def test_invalid_attempt_bounds_cannot_bypass_old_generation_guard(tmp_path) -> None:
    async def exercise(case: str, index: int) -> tuple[object, int]:
        cache_directory = tmp_path / f'attempt-cache-{index}'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        old_record = store.save_token(
            access_token='old-token',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        now = datetime.now(timezone.utc)
        store.save_issuance_attempt(
            started_at=now,
            cooldown_until=now + timedelta(seconds=90),
            invalidates_generation=old_record.generation,
        )
        payload = json.loads(store.failure_path.read_text())
        if case == 'future':
            future = now + timedelta(seconds=30)
            payload['started_at'] = future.isoformat()
            payload['cooldown_until'] = (future + timedelta(seconds=90)).isoformat()
        elif case == 'reversed':
            payload['cooldown_until'] = (now - timedelta(seconds=1)).isoformat()
        elif case == 'zero':
            payload['cooldown_until'] = payload['started_at']
        elif case == 'too-short':
            payload['cooldown_until'] = (now + timedelta(seconds=1)).isoformat()
        elif case == 'overlong':
            payload['cooldown_until'] = (now + timedelta(seconds=3601)).isoformat()
        else:
            raise AssertionError(case)
        store.failure_path.write_text(json.dumps(payload), encoding='utf-8')
        store.failure_path.chmod(0o600)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('must-not-be-issued')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(force_refresh=True),
                return_exceptions=True,
            )
        return result[0], call_count

    for index, case in enumerate(('future', 'reversed', 'zero', 'too-short', 'overlong')):
        result, call_count = asyncio.run(exercise(case, index))
        expected_error = KISTransportError if case == 'too-short' else KISConfigurationError
        assert isinstance(result, expected_error), (case, result)
        assert call_count == 0, case


def test_one_second_failure_and_success_guards_fail_closed(tmp_path) -> None:
    async def exercise(record_type: str, index: int) -> tuple[object, int]:
        cache_directory = tmp_path / f'short-guard-{index}'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        now = datetime.now(timezone.utc)
        if record_type == 'failure':
            store.save_failure(
                error_kind='http',
                status_code=429,
                retryable=True,
                failed_at=now,
                cooldown_until=now + timedelta(seconds=60),
            )
            timestamp_field = 'failed_at'
        elif record_type == 'issuance_success':
            store.save_token_after_issuance(
                access_token='matching-token',
                token_type='Bearer',
                issued_at=now,
                expires_at=now + timedelta(hours=1),
                refresh_after=now + timedelta(minutes=55),
                minimum_issue_interval_seconds=60,
            )
            timestamp_field = 'issued_at'
        else:
            raise AssertionError(record_type)

        payload = json.loads(store.failure_path.read_text())
        short_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        payload[timestamp_field] = short_start.isoformat()
        payload['cooldown_until'] = (short_start + timedelta(seconds=1)).isoformat()
        store.failure_path.write_text(json.dumps(payload), encoding='utf-8')
        store.failure_path.chmod(0o600)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('must-not-be-issued')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(force_refresh=True),
                return_exceptions=True,
            )
        return result[0], call_count

    for index, record_type in enumerate(('failure', 'issuance_success')):
        result, call_count = asyncio.run(exercise(record_type, index))
        if record_type == 'failure':
            assert isinstance(result, KISHTTPError), (record_type, result)
        else:
            assert isinstance(result, dict), (record_type, result)
            assert result['access_token'] == 'matching-token'
            assert result['refresh_deferred'] is True
        assert call_count == 0, record_type


def test_store_writers_reject_timing_outside_reader_contract(tmp_path) -> None:
    store = KISTokenCacheStore(
        directory=str(tmp_path / 'cache'),
        environment='real',
        app_key='test-app-key',
        app_secret='test-app-secret',
        minimum_issue_interval_seconds=120,
        issuance_lease_seconds=180,
    )
    now = datetime.now(timezone.utc)

    with pytest.raises(KISTokenCacheError):
        store.save_failure(
            error_kind='http',
            status_code=429,
            retryable=True,
            failed_at=now,
            cooldown_until=now + timedelta(seconds=119),
        )
    assert not store.failure_path.exists()

    with pytest.raises(KISTokenCacheError):
        store.save_issuance_attempt(
            started_at=now,
            cooldown_until=now + timedelta(seconds=179),
            invalidates_generation=None,
        )
    assert not store.failure_path.exists()

    with pytest.raises(KISTokenCacheError):
        store.save_token_after_issuance(
            access_token='must-not-be-written',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
            minimum_issue_interval_seconds=60,
        )
    assert not store.token_path.exists()
    assert not store.failure_path.exists()

    record = store.save_token(
        access_token='existing-token',
        token_type='Bearer',
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=55),
    )
    persisted_before = store.token_path.read_bytes()
    with pytest.raises(KISTokenCacheError):
        asyncio.run(
            store.invalidate_if_matches(
                generation=record.generation,
                minimum_issue_interval_seconds=60,
            )
        )
    assert store.token_path.read_bytes() == persisted_before


def test_existing_non_private_leaf_is_rejected_without_chmod(tmp_path) -> None:
    cache_directory = tmp_path / 'cache'
    cache_directory.mkdir(mode=0o755)
    cache_directory.chmod(0o755)
    store = KISTokenCacheStore(
        directory=str(cache_directory),
        environment='real',
        app_key='test-app-key',
        app_secret='test-app-secret',
    )

    with pytest.raises(KISTokenCacheError):
        store.open_lock()

    assert stat.S_IMODE(cache_directory.stat().st_mode) == 0o755


def test_actual_subprocess_waits_for_lock_and_reuses_persisted_token(tmp_path) -> None:
    async def exercise() -> tuple[bool, str, str]:
        cache_directory = tmp_path / 'cache'
        marker = tmp_path / 'child-result.txt'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        now = datetime.now(timezone.utc)
        store.save_token(
            access_token='cross-process-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
        )
        child_source = textwrap.dedent(
            '''
            import asyncio
            from pathlib import Path
            import sys
            from app.brokers.kis.token_cache import KISTokenCacheStore

            async def main():
                store = KISTokenCacheStore(
                    directory=sys.argv[1],
                    environment='real',
                    app_key='test-app-key',
                    app_secret='test-app-secret',
                )
                async with store.lock(poll_interval_seconds=0.005):
                    record = store.load_token()
                    Path(sys.argv[2]).write_text(record.access_token, encoding='utf-8')

            asyncio.run(main())
            '''
        )
        async with store.lock(poll_interval_seconds=0.005):
            process = subprocess.Popen(
                [sys.executable, '-c', child_source, str(cache_directory), str(marker)],
                cwd=Path(__file__).resolve().parents[1],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            await asyncio.sleep(0.15)
            marker_created_while_locked = marker.exists()
        stdout, stderr = await asyncio.to_thread(process.communicate, timeout=3)
        assert process.returncode == 0, (stdout, stderr)
        return marker_created_while_locked, marker.read_text(), stderr

    marker_created_while_locked, token, stderr = asyncio.run(exercise())

    assert marker_created_while_locked is False
    assert token == 'cross-process-token'
    assert stderr == ''


def test_cas_revocation_invalidates_an_existing_clients_memory_cache(tmp_path) -> None:
    async def exercise() -> tuple[dict, bool, object, int, dict]:
        cache_directory = tmp_path / 'cache'
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('revoked-token')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
            'kis_token_min_issue_interval_seconds': 60,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            first = await client.issue_access_token()
            store = client._token_store()
            record = store.load_token_record()
            assert record is not None
            invalidated = await store.invalidate_if_matches(
                generation=record.generation,
                access_token=record.access_token,
                minimum_issue_interval_seconds=60,
            )
            second = await asyncio.gather(client.issue_access_token(), return_exceptions=True)
            tombstone = json.loads(store.token_path.read_text())
        return first, invalidated, second[0], call_count, tombstone

    first, invalidated, second, call_count, tombstone = asyncio.run(exercise())

    assert first['access_token'] == 'revoked-token'
    assert invalidated is True
    assert isinstance(second, KISTransportError)
    assert call_count == 1
    assert tombstone['record_type'] == 'revocation'
    assert tombstone['revoked_generation']
    assert tombstone['revoked_token_fingerprint'].startswith('sha256:')
    assert 'revoked-token' not in json.dumps(tombstone)


def test_cas_revocation_does_not_delete_a_newer_generation(tmp_path) -> None:
    async def exercise() -> tuple[bool, str]:
        store = KISTokenCacheStore(
            directory=str(tmp_path / 'cache'),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        old_record = store.save_token(
            access_token='old-token',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        new_time = datetime.now(timezone.utc)
        new_record = store.save_token(
            access_token='new-token',
            token_type='Bearer',
            issued_at=new_time,
            expires_at=new_time + timedelta(hours=1),
            refresh_after=new_time + timedelta(minutes=55),
        )
        invalidated = await store.invalidate_if_matches(
            generation=old_record.generation,
            access_token=old_record.access_token,
            minimum_issue_interval_seconds=60,
        )
        remaining = store.load_token_record()
        assert remaining is not None
        assert remaining.generation == new_record.generation
        return invalidated, remaining.access_token

    invalidated, remaining_token = asyncio.run(exercise())

    assert invalidated is False
    assert remaining_token == 'new-token'


def test_disk_delete_and_peer_generation_replace_in_memory_token(tmp_path) -> None:
    async def exercise() -> tuple[str, object, str, int]:
        cache_directory = tmp_path / 'cache'
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response(f'upstream-token-{call_count}')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            first = await client.issue_access_token()
            store = client._token_store()
            store.token_path.unlink()
            after_delete = await asyncio.gather(
                client.issue_access_token(),
                return_exceptions=True,
            )

            peer_time = datetime.now(timezone.utc)
            store.save_token_after_issuance(
                access_token='peer-token',
                token_type='Bearer',
                issued_at=peer_time,
                expires_at=peer_time + timedelta(hours=1),
                refresh_after=peer_time + timedelta(minutes=55),
                minimum_issue_interval_seconds=60,
            )
            after_peer_refresh = await client.issue_access_token()
        return (
            first['access_token'],
            after_delete[0],
            after_peer_refresh['access_token'],
            call_count,
        )

    first_token, after_delete, peer_token, call_count = asyncio.run(exercise())

    assert first_token == 'upstream-token-1'
    assert isinstance(after_delete, KISTransportError)
    assert peer_token == 'peer-token'
    assert call_count == 1


def test_missing_token_recovers_once_after_success_cooldown_expires(tmp_path) -> None:
    async def exercise() -> tuple[object, list[dict], int]:
        cache_directory = tmp_path / 'cache'
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.02)
            return _token_response(f'upstream-token-{call_count}')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            first_client = KISClient(use_mock=False)
            await first_client.issue_access_token()
            store = first_client._token_store()
            store.token_path.unlink()
            blocked = await asyncio.gather(
                first_client.issue_access_token(),
                return_exceptions=True,
            )

            success_payload = json.loads(store.failure_path.read_text())
            expired_at = datetime.now(timezone.utc) - timedelta(minutes=2)
            success_payload['issued_at'] = expired_at.isoformat()
            success_payload['cooldown_until'] = (
                expired_at + timedelta(seconds=60)
            ).isoformat()
            store.failure_path.write_text(json.dumps(success_payload), encoding='utf-8')
            store.failure_path.chmod(0o600)

            clients = [KISClient(use_mock=False) for _ in range(5)]
            recovered = await asyncio.gather(
                *(client.issue_access_token() for client in clients)
            )
        return blocked[0], recovered, call_count

    blocked, recovered, call_count = asyncio.run(exercise())

    assert isinstance(blocked, KISTransportError)
    assert call_count == 2
    assert {result['access_token'] for result in recovered} == {'upstream-token-2'}
    assert sum(result['cached'] is False for result in recovered) == 1


def test_rolled_back_token_recovers_once_after_success_cooldown_expires(tmp_path) -> None:
    async def exercise() -> tuple[object, list[dict], int]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        old_record = store.save_token(
            access_token='rolled-back-token',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        now = datetime.now(timezone.utc)
        store.save_token_after_issuance(
            access_token='newer-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
            minimum_issue_interval_seconds=60,
        )
        store._write_token(old_record)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.02)
            return _token_response('replacement-token')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            blocked = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )

            success_payload = json.loads(store.failure_path.read_text())
            expired_at = datetime.now(timezone.utc) - timedelta(minutes=2)
            success_payload['issued_at'] = expired_at.isoformat()
            success_payload['cooldown_until'] = (
                expired_at + timedelta(seconds=60)
            ).isoformat()
            store.failure_path.write_text(json.dumps(success_payload), encoding='utf-8')
            store.failure_path.chmod(0o600)

            clients = [KISClient(use_mock=False) for _ in range(5)]
            recovered = await asyncio.gather(
                *(client.issue_access_token() for client in clients)
            )
        return blocked[0], recovered, call_count

    blocked, recovered, call_count = asyncio.run(exercise())

    assert isinstance(blocked, KISTransportError)
    assert call_count == 1
    assert {result['access_token'] for result in recovered} == {'replacement-token'}
    assert sum(result['cached'] is False for result in recovered) == 1


def test_matching_token_is_reused_after_success_ledger_expires(tmp_path) -> None:
    async def exercise() -> tuple[dict, int]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        now = datetime.now(timezone.utc)
        store.save_token_after_issuance(
            access_token='matching-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
            minimum_issue_interval_seconds=60,
        )
        success_payload = json.loads(store.failure_path.read_text())
        expired_at = now - timedelta(minutes=2)
        success_payload['issued_at'] = expired_at.isoformat()
        success_payload['cooldown_until'] = (
            expired_at + timedelta(seconds=60)
        ).isoformat()
        store.failure_path.write_text(json.dumps(success_payload), encoding='utf-8')
        store.failure_path.chmod(0o600)
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('must-not-be-issued')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await KISClient(use_mock=False).issue_access_token()
        return result, call_count

    result, call_count = asyncio.run(exercise())

    assert result['access_token'] == 'matching-token'
    assert result['cached'] is True
    assert call_count == 0


def test_token_write_failure_blocks_old_generation_then_recovers_once(tmp_path) -> None:
    async def exercise() -> tuple[object, object, dict, int]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        store.save_token(
            access_token='old-token',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response(f'recovery-token-{call_count}')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
            'kis_token_issuance_lease_seconds': 90,
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            with patch.object(
                KISTokenCacheStore,
                '_write_token',
                side_effect=KISTokenCacheError('injected token write failure'),
            ):
                failed_refresh = await asyncio.gather(
                    client.issue_access_token(force_refresh=True),
                    return_exceptions=True,
                )
                suppressed = await asyncio.gather(
                    client.issue_access_token(),
                    return_exceptions=True,
                )

            guard_payload = json.loads(store.failure_path.read_text())
            expired_start = datetime.now(timezone.utc) - timedelta(minutes=2)
            guard_payload['started_at'] = expired_start.isoformat()
            guard_payload['cooldown_until'] = (
                expired_start + timedelta(seconds=90)
            ).isoformat()
            store.failure_path.write_text(json.dumps(guard_payload), encoding='utf-8')
            store.failure_path.chmod(0o600)
            recovered = await client.issue_access_token()
        return failed_refresh[0], suppressed[0], recovered, call_count

    failed_refresh, suppressed, recovered, call_count = asyncio.run(exercise())

    assert isinstance(failed_refresh, KISConfigurationError)
    assert isinstance(suppressed, KISTransportError)
    assert recovered['access_token'] == 'recovery-token-2'
    assert recovered['cached'] is False
    assert call_count == 2


def test_success_ledger_write_failure_never_restores_old_generation(tmp_path) -> None:
    async def exercise() -> tuple[object, dict, int]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        old_record = store.save_token(
            access_token='old-token',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('new-token')

        original_write_json = KISTokenCacheStore._write_json

        def fail_success_ledger(
            cache_store: KISTokenCacheStore,
            path: Path,
            payload: dict,
        ) -> None:
            if payload.get('record_type') == 'issuance_success':
                raise KISTokenCacheError('injected success ledger failure')
            original_write_json(cache_store, path, payload)

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            with patch.object(KISTokenCacheStore, '_write_json', new=fail_success_ledger):
                failed_result = await asyncio.gather(
                    client.issue_access_token(force_refresh=True),
                    return_exceptions=True,
                )
            recovered = await client.issue_access_token()
            persisted = store.load_token_record()
            assert persisted is not None
            assert persisted.generation != old_record.generation
        return failed_result[0], recovered, call_count

    failed_result, recovered, call_count = asyncio.run(exercise())

    assert isinstance(failed_result, KISConfigurationError)
    assert recovered['access_token'] == 'new-token'
    assert recovered['cached'] is True
    assert call_count == 1


def test_active_mismatched_success_ledger_suppresses_token_issuance(tmp_path) -> None:
    async def exercise() -> tuple[object, int]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        now = datetime.now(timezone.utc)
        store.save_token_after_issuance(
            access_token='ledger-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
            minimum_issue_interval_seconds=60,
        )
        peer_time = now + timedelta(seconds=1)
        store.save_token(
            access_token='torn-peer-token',
            token_type='Bearer',
            issued_at=peer_time,
            expires_at=peer_time + timedelta(hours=1),
            refresh_after=peer_time + timedelta(minutes=55),
        )
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return _token_response('must-not-be-issued')

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return result[0], call_count

    result, call_count = asyncio.run(exercise())

    assert isinstance(result, KISTransportError)
    assert call_count == 0


def test_401_cas_invalidates_exact_generation_then_next_call_refreshes(tmp_path) -> None:
    async def exercise() -> tuple[object, dict, dict, list[str]]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        store.save_token(
            access_token='rejected-token',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        paths: list[str] = []

        async def fake_request(*args, **kwargs) -> httpx.Response:
            path = str(args[3])
            paths.append(path)
            if path == '/oauth2/tokenP':
                return _token_response('replacement-token')
            return httpx.Response(
                401,
                json={'msg1': 'expired access token'},
                request=httpx.Request('GET', f'https://example.test{path}'),
            )

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            rejected = await asyncio.gather(
                client._request('GET', '/protected', tr_id='TEST', params={}),
                return_exceptions=True,
            )
            tombstone = json.loads(store.token_path.read_text())
            replacement = await client.issue_access_token()
        return rejected[0], tombstone, replacement, paths

    rejected, tombstone, replacement, paths = asyncio.run(exercise())

    assert isinstance(rejected, KISHTTPError)
    assert rejected.status_code == 401
    assert tombstone['record_type'] == 'revocation'
    assert replacement['access_token'] == 'replacement-token'
    assert paths == ['/protected', '/oauth2/tokenP']


def test_stale_401_cannot_invalidate_peer_generation(tmp_path) -> None:
    async def exercise() -> tuple[object, dict, dict, int]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        store.save_token(
            access_token='stale-token',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        request_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal request_count
            request_count += 1
            peer_time = datetime.now(timezone.utc)
            store.save_token(
                access_token='peer-token',
                token_type='Bearer',
                issued_at=peer_time,
                expires_at=peer_time + timedelta(hours=1),
                refresh_after=peer_time + timedelta(minutes=55),
            )
            return httpx.Response(
                401,
                json={'msg1': 'stale request'},
                request=httpx.Request('GET', 'https://example.test/protected'),
            )

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            rejected = await asyncio.gather(
                client._request('GET', '/protected', tr_id='TEST', params={}),
                return_exceptions=True,
            )
            state_payload = json.loads(store.token_path.read_text())
            next_token = await client.issue_access_token()
        return rejected[0], state_payload, next_token, request_count

    rejected, state_payload, next_token, request_count = asyncio.run(exercise())

    assert isinstance(rejected, KISHTTPError)
    assert rejected.status_code == 401
    assert state_payload['record_type'] == 'token'
    assert state_payload['access_token'] == 'peer-token'
    assert next_token['access_token'] == 'peer-token'
    assert request_count == 1


def test_order_post_401_invalidates_token_without_retransmission(tmp_path) -> None:
    async def exercise() -> tuple[object, dict, list[str]]:
        cache_directory = tmp_path / 'cache'
        store = KISTokenCacheStore(
            directory=str(cache_directory),
            environment='real',
            app_key='test-app-key',
            app_secret='test-app-secret',
        )
        now = datetime.now(timezone.utc)
        store.save_token(
            access_token='order-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
        )
        paths: list[str] = []

        async def fake_request(*args, **kwargs) -> httpx.Response:
            path = str(args[3])
            paths.append(path)
            if path == '/uapi/hashkey':
                return httpx.Response(
                    200,
                    json={'HASH': 'hash-key'},
                    request=httpx.Request('POST', 'https://example.test/uapi/hashkey'),
                )
            return httpx.Response(
                401,
                json={'msg1': 'expired access token'},
                request=httpx.Request('POST', f'https://example.test{path}'),
            )

        overrides = {
            'kis_app_key': 'test-app-key',
            'kis_app_secret': 'test-app-secret',
            'kis_token_cache_directory': str(cache_directory),
        }
        with (
            patch.dict(settings.__dict__, overrides, clear=False),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            rejected = await asyncio.gather(
                client._request(
                    'POST',
                    '/uapi/order-test',
                    tr_id='ORDER',
                    json_body={'symbol': '005930'},
                    use_hashkey=True,
                ),
                return_exceptions=True,
            )
            tombstone = json.loads(store.token_path.read_text())
        return rejected[0], tombstone, paths

    rejected, tombstone, paths = asyncio.run(exercise())

    assert isinstance(rejected, KISHTTPError)
    assert rejected.status_code == 401
    assert paths == ['/uapi/hashkey', '/uapi/order-test']
    assert tombstone['record_type'] == 'revocation'


def test_token_cas_requires_every_supplied_comparator_and_allows_single_fallbacks(tmp_path) -> None:
    async def exercise() -> tuple[bool, bool, bool, str]:
        store = _store(tmp_path / 'cache')
        now = datetime.now(timezone.utc) - timedelta(minutes=10)
        original = store.save_token(
            access_token='same-token',
            token_type='Bearer',
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
        )
        rewritten = store.save_token(
            access_token='same-token',
            token_type='Bearer',
            issued_at=now + timedelta(seconds=1),
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
        )
        same_token_invalidated = await store.invalidate_if_matches(
            generation=original.generation,
            access_token=original.access_token,
            minimum_issue_interval_seconds=60,
        )

        different = store.save_token(
            access_token='different-token',
            token_type='Bearer',
            issued_at=now + timedelta(seconds=2),
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=55),
        )
        different_token_preserved = not await store.invalidate_if_matches(
            generation=different.generation,
            access_token='same-token',
            minimum_issue_interval_seconds=60,
        )
        generation_fallback_invalidated = await store.invalidate_if_matches(
            generation=different.generation,
            minimum_issue_interval_seconds=60,
        )
        state = store.load_cache_state()
        assert rewritten.generation != original.generation
        return (
            same_token_invalidated,
            different_token_preserved,
            generation_fallback_invalidated,
            type(state).__name__,
        )

    result = asyncio.run(exercise())

    assert result == (False, True, True, 'CachedKISTokenRevocation')


def test_egw00123_invalidates_exact_token_without_retrying_get_or_post(tmp_path) -> None:
    async def exercise(method: str, index: int) -> tuple[object, list[str], dict]:
        cache_directory = tmp_path / f'cache-{index}'
        store = _store(cache_directory)
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        store.save_token(
            access_token=f'expired-token-{index}',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        paths: list[str] = []

        async def fake_request(*args, **kwargs) -> httpx.Response:
            path = str(args[3])
            paths.append(path)
            if path == '/uapi/hashkey':
                return httpx.Response(
                    200,
                    json={'HASH': 'hash-key'},
                    request=httpx.Request('POST', 'https://example.test/uapi/hashkey'),
                )
            return httpx.Response(
                200,
                json={
                    'rt_cd': '1',
                    'msg_cd': 'EGW00123',
                    'msg1': 'access token expired',
                },
                request=httpx.Request(method, f'https://example.test{path}'),
            )

        with (
            patch.dict(
                settings.__dict__,
                _settings_overrides(cache_directory),
                clear=False,
            ),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            if method == 'POST':
                result = await asyncio.gather(
                    client._request(
                        'POST',
                        '/uapi/order-test',
                        tr_id='ORDER',
                        json_body={'symbol': '005930'},
                        use_hashkey=True,
                    ),
                    return_exceptions=True,
                )
            else:
                result = await asyncio.gather(
                    client._request('GET', '/protected', tr_id='TEST', params={}),
                    return_exceptions=True,
                )
        return result[0], paths, json.loads(store.token_path.read_text())

    for index, method in enumerate(('GET', 'POST')):
        result, paths, state = asyncio.run(exercise(method, index))
        assert isinstance(result, KISAPIError), (method, result)
        assert result.msg_code == 'EGW00123'
        assert paths == (
            ['/protected']
            if method == 'GET'
            else ['/uapi/hashkey', '/uapi/order-test']
        )
        assert state['record_type'] == 'revocation'


def test_stale_egw00123_cannot_invalidate_peer_token(tmp_path) -> None:
    async def exercise() -> tuple[object, dict, dict, int]:
        cache_directory = tmp_path / 'cache'
        store = _store(cache_directory)
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        store.save_token(
            access_token='stale-token',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        request_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal request_count
            request_count += 1
            peer_time = datetime.now(timezone.utc)
            store.save_token(
                access_token='peer-token',
                token_type='Bearer',
                issued_at=peer_time,
                expires_at=peer_time + timedelta(hours=1),
                refresh_after=peer_time + timedelta(minutes=55),
            )
            return httpx.Response(
                200,
                json={'rt_cd': '1', 'msg_cd': 'EGW00123', 'msg1': 'expired'},
                request=httpx.Request('GET', 'https://example.test/protected'),
            )

        with (
            patch.dict(
                settings.__dict__,
                _settings_overrides(cache_directory),
                clear=False,
            ),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            rejected = await asyncio.gather(
                client._request('GET', '/protected', tr_id='TEST', params={}),
                return_exceptions=True,
            )
            state = json.loads(store.token_path.read_text())
            next_token = await client.issue_access_token()
        return rejected[0], state, next_token, request_count

    rejected, state, next_token, request_count = asyncio.run(exercise())

    assert isinstance(rejected, KISAPIError)
    assert rejected.msg_code == 'EGW00123'
    assert state['record_type'] == 'token'
    assert state['access_token'] == 'peer-token'
    assert next_token['access_token'] == 'peer-token'
    assert request_count == 1


def test_invalidation_cache_faults_are_configuration_errors_without_retry(tmp_path) -> None:
    async def exercise(method: str, index: int) -> tuple[object, list[str]]:
        cache_directory = tmp_path / f'cache-{index}'
        store = _store(cache_directory)
        old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        store.save_token(
            access_token=f'rejected-token-{index}',
            token_type='Bearer',
            issued_at=old_time,
            expires_at=old_time + timedelta(hours=1),
            refresh_after=old_time + timedelta(minutes=55),
        )
        paths: list[str] = []

        async def fake_request(*args, **kwargs) -> httpx.Response:
            path = str(args[3])
            paths.append(path)
            if path == '/uapi/hashkey':
                return httpx.Response(
                    200,
                    json={'HASH': 'hash-key'},
                    request=httpx.Request('POST', 'https://example.test/uapi/hashkey'),
                )
            return httpx.Response(
                401,
                json={'msg1': 'expired'},
                request=httpx.Request(method, f'https://example.test{path}'),
            )

        original_load_cache_state = KISTokenCacheStore.load_cache_state
        original_write_json = KISTokenCacheStore._write_json
        load_count = 0

        def fail_invalidation_read(cache_store, *args, **kwargs):
            nonlocal load_count
            load_count += 1
            if load_count == 2:
                raise KISTokenCacheError('invalidation read fault')
            return original_load_cache_state(cache_store, *args, **kwargs)

        def fail_invalidation_write(cache_store, path, payload) -> None:
            if payload.get('record_type') == 'revocation':
                raise KISTokenCacheError('invalidation write fault')
            original_write_json(cache_store, path, payload)

        with (
            patch.dict(
                settings.__dict__,
                _settings_overrides(cache_directory),
                clear=False,
            ),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            client = KISClient(use_mock=False)
            async def send_request() -> object:
                if method == 'POST':
                    request = client._request(
                        'POST',
                        '/uapi/order-test',
                        tr_id='ORDER',
                        json_body={'symbol': '005930'},
                        use_hashkey=True,
                    )
                else:
                    request = client._request(
                        'GET',
                        '/protected',
                        tr_id='TEST',
                        params={},
                    )
                return (await asyncio.gather(request, return_exceptions=True))[0]

            if method == 'GET':
                with patch.object(
                    KISTokenCacheStore,
                    'load_cache_state',
                    new=fail_invalidation_read,
                ):
                    result = await send_request()
            else:
                with patch.object(
                    KISTokenCacheStore,
                    '_write_json',
                    new=fail_invalidation_write,
                ):
                    result = await send_request()
        return result, paths

    for index, method in enumerate(('GET', 'POST')):
        result, paths = asyncio.run(exercise(method, index))
        assert isinstance(result, KISConfigurationError), (method, result)
        assert paths == (
            ['/protected']
            if method == 'GET'
            else ['/uapi/hashkey', '/uapi/order-test']
        )


def test_provider_kst_expiry_is_a_conservative_absolute_bound(tmp_path) -> None:
    async def exercise() -> tuple[dict, datetime, datetime]:
        from zoneinfo import ZoneInfo

        cache_directory = tmp_path / 'cache'
        provider_expiry = (
            datetime.now(timezone.utc) + timedelta(minutes=10)
        ).replace(microsecond=0)
        provider_text = provider_expiry.astimezone(ZoneInfo('Asia/Seoul')).strftime(
            '%Y-%m-%d %H:%M:%S'
        )

        async def fake_request(*args, **kwargs) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    'access_token': 'absolute-expiry-token',
                    'token_type': 'Bearer',
                    'expires_in': 3_600,
                    'access_token_token_expired': provider_text,
                },
                request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
            )

        with (
            patch.dict(
                settings.__dict__,
                _settings_overrides(cache_directory),
                clear=False,
            ),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result = await KISClient(use_mock=False).issue_access_token()
            record = _store(cache_directory).load_token_record()
            assert record is not None
        return result, record.expires_at, provider_expiry

    result, persisted_expiry, provider_expiry = asyncio.run(exercise())

    assert persisted_expiry == provider_expiry
    assert 0 < result['expires_in'] <= 600


def test_token_expiry_without_absolute_timestamp_uses_request_start(tmp_path) -> None:
    async def exercise() -> tuple[dict, datetime, datetime, datetime, float]:
        cache_directory = tmp_path / 'cache'
        before_request = datetime.now(timezone.utc)

        async def delayed_request(*args, **kwargs) -> httpx.Response:
            await asyncio.sleep(0.25)
            return httpx.Response(
                200,
                json={
                    'access_token': 'delayed-token',
                    'token_type': 'Bearer',
                    'expires_in': 2,
                },
                request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
            )

        with (
            patch.dict(
                settings.__dict__,
                _settings_overrides(
                    cache_directory,
                    kis_token_refresh_margin_seconds=0,
                ),
                clear=False,
            ),
            patch('app.brokers.kis.client.request_with_audit', side_effect=delayed_request),
        ):
            result = await KISClient(use_mock=False).issue_access_token()
            store = _store(cache_directory)
            record = store.load_token_record()
            assert record is not None
            success = store.load_failure(include_expired=True)
            assert success is not None
        return (
            result,
            record.expires_at,
            before_request,
            record.issued_at,
            (success.cooldown_until - success.failed_at).total_seconds(),
        )

    result, persisted_expiry, before_request, issued_at, success_cooldown = asyncio.run(
        exercise()
    )

    assert persisted_expiry <= before_request + timedelta(seconds=2.1)
    assert issued_at >= before_request + timedelta(seconds=0.2)
    assert success_cooldown == 60
    assert result['expires_in'] <= 1


def test_response_whose_refresh_boundary_already_passed_is_rejected(tmp_path) -> None:
    async def exercise() -> tuple[list[object], int]:
        cache_directory = tmp_path / 'cache'
        call_count = 0

        async def delayed_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(1.2)
            return httpx.Response(
                200,
                json={
                    'access_token': 'too-late-token',
                    'token_type': 'Bearer',
                    'expires_in': 2,
                },
                request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
            )

        with (
            patch.dict(
                settings.__dict__,
                _settings_overrides(
                    cache_directory,
                    kis_token_refresh_margin_seconds=1,
                ),
                clear=False,
            ),
            patch('app.brokers.kis.client.request_with_audit', side_effect=delayed_request),
        ):
            first = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
            second = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return [first[0], second[0]], call_count

    results, call_count = asyncio.run(exercise())

    assert call_count == 1
    assert all(isinstance(result, KISAPIError) for result in results)


def test_malformed_provider_absolute_expiry_enters_shared_cooldown(tmp_path) -> None:
    invalid_values: tuple[object, ...] = (
        '2026-09-09T10:00:00+09:00',
        '2026-9-9 1:2:3',
        '2026-02-30 10:00:00',
        123,
    )

    async def exercise(value: object, index: int) -> tuple[list[object], int]:
        cache_directory = tmp_path / f'cache-{index}'
        call_count = 0

        async def fake_request(*args, **kwargs) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(
                200,
                json={
                    'access_token': 'must-not-be-used',
                    'token_type': 'Bearer',
                    'expires_in': 3_600,
                    'access_token_token_expired': value,
                },
                request=httpx.Request('POST', 'https://example.test/oauth2/tokenP'),
            )

        with (
            patch.dict(
                settings.__dict__,
                _settings_overrides(cache_directory),
                clear=False,
            ),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            first = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
            second = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                return_exceptions=True,
            )
        return [first[0], second[0]], call_count

    for index, value in enumerate(invalid_values):
        results, call_count = asyncio.run(exercise(value, index))
        assert call_count == 1, value
        assert all(isinstance(result, KISAPIError) for result in results), value


def test_refresh_margin_has_protocol_bounds() -> None:
    assert Settings(kis_token_refresh_margin_seconds=0).kis_token_refresh_margin_seconds == 0
    assert (
        Settings(kis_token_refresh_margin_seconds=172_799).kis_token_refresh_margin_seconds
        == 172_799
    )
    with pytest.raises(ValueError):
        Settings(kis_token_refresh_margin_seconds=-1)
    with pytest.raises(ValueError):
        Settings(kis_token_refresh_margin_seconds=172_800)


def test_short_persisted_cooldowns_are_extended_to_local_timing_floor(tmp_path) -> None:
    started_at = datetime.now(timezone.utc) - timedelta(seconds=100)

    for index, record_type in enumerate(('failure', 'issuance_attempt', 'issuance_success')):
        cache_directory = tmp_path / f'cache-{index}'
        writer = _store(cache_directory)
        if record_type == 'failure':
            writer.save_failure(
                error_kind='http',
                status_code=429,
                retryable=True,
                failed_at=started_at,
                cooldown_until=started_at + timedelta(seconds=60),
            )
        elif record_type == 'issuance_attempt':
            writer.save_issuance_attempt(
                started_at=started_at,
                cooldown_until=started_at + timedelta(seconds=90),
                invalidates_generation=None,
            )
        else:
            writer.save_token_after_issuance(
                access_token='matching-token',
                token_type='Bearer',
                issued_at=started_at,
                expires_at=started_at + timedelta(hours=1),
                refresh_after=started_at + timedelta(minutes=55),
                minimum_issue_interval_seconds=60,
            )

        reader = _store(
            cache_directory,
            minimum_issue_interval_seconds=120,
            issuance_lease_seconds=180,
        )
        active = reader.load_failure(now=started_at + timedelta(seconds=110))
        expired = reader.load_failure(now=started_at + timedelta(seconds=181))
        assert active is not None, record_type
        expected_floor = 180 if record_type == 'issuance_attempt' else 120
        assert active.cooldown_until == started_at + timedelta(seconds=expected_floor)
        assert expired is None, record_type


def test_revocation_accepts_max_issue_interval_plus_allowed_clock_skew(tmp_path) -> None:
    async def exercise():
        store = _store(
            tmp_path / 'cache',
            minimum_issue_interval_seconds=3_600,
            issuance_lease_seconds=3_600,
        )
        issued_at = datetime.now(timezone.utc) + timedelta(minutes=4, seconds=59)
        token = store.save_token(
            access_token='future-issued-token',
            token_type='Bearer',
            issued_at=issued_at,
            expires_at=issued_at + timedelta(hours=1),
            refresh_after=issued_at + timedelta(minutes=55),
        )
        invalidated = await store.invalidate_if_matches(
            access_token=token.access_token,
            minimum_issue_interval_seconds=3_600,
        )
        return invalidated, store.load_cache_state()

    invalidated, state = asyncio.run(exercise())

    assert invalidated is True
    assert type(state).__name__ == 'CachedKISTokenRevocation'
    assert 3_898 <= (state.not_before - state.revoked_at).total_seconds() <= 3_900


@pytest.mark.parametrize('case', ('negative', 'overlong', 'future'))
def test_revocation_rejects_invalid_temporal_bounds(tmp_path, case: str) -> None:
    async def seed_revocation() -> KISTokenCacheStore:
        store = _store(tmp_path / case)
        issued_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        token = store.save_token(
            access_token=f'{case}-token',
            token_type='Bearer',
            issued_at=issued_at,
            expires_at=issued_at + timedelta(hours=1),
            refresh_after=issued_at + timedelta(minutes=55),
        )
        assert await store.invalidate_if_matches(
            access_token=token.access_token,
            minimum_issue_interval_seconds=60,
        )
        return store

    store = asyncio.run(seed_revocation())
    payload = json.loads(store.token_path.read_text())
    revoked_at = datetime.fromisoformat(payload['revoked_at'])
    if case == 'negative':
        payload['not_before'] = (revoked_at - timedelta(seconds=1)).isoformat()
    elif case == 'overlong':
        payload['not_before'] = (revoked_at + timedelta(seconds=3_901)).isoformat()
    else:
        future = datetime.now(timezone.utc) + timedelta(minutes=5, seconds=1)
        payload['revoked_at'] = future.isoformat()
        payload['not_before'] = future.isoformat()
    store.token_path.write_text(json.dumps(payload), encoding='utf-8')
    store.token_path.chmod(0o600)

    with pytest.raises(KISTokenCacheError):
        store.load_cache_state()


def test_token_issue_cache_read_yields_to_event_loop(tmp_path) -> None:
    async def exercise() -> tuple[dict, int]:
        cache_directory = tmp_path / 'cache'
        entered = threading.Event()
        release = threading.Event()
        heartbeat_ticks = 0
        should_block = True
        original_load = KISTokenCacheStore.load_cache_state

        def blocking_load(store, *args, **kwargs):
            nonlocal should_block
            if should_block:
                should_block = False
                entered.set()
                if not release.wait(timeout=0.5):
                    raise AssertionError('cache read blocked the event loop')
            return original_load(store, *args, **kwargs)

        async def heartbeat() -> None:
            nonlocal heartbeat_ticks
            while not entered.is_set():
                await asyncio.sleep(0)
            for _ in range(3):
                heartbeat_ticks += 1
                await asyncio.sleep(0)
            release.set()

        async def fake_request(*args, **kwargs) -> httpx.Response:
            return _token_response('nonblocking-token')

        with (
            patch.dict(
                settings.__dict__,
                _settings_overrides(cache_directory),
                clear=False,
            ),
            patch.object(KISTokenCacheStore, 'load_cache_state', new=blocking_load),
            patch('app.brokers.kis.client.request_with_audit', side_effect=fake_request),
        ):
            result, _ = await asyncio.gather(
                KISClient(use_mock=False).issue_access_token(),
                heartbeat(),
            )
        return result, heartbeat_ticks

    result, heartbeat_ticks = asyncio.run(exercise())

    assert result['access_token'] == 'nonblocking-token'
    assert heartbeat_ticks == 3


def test_token_invalidation_cache_write_yields_to_event_loop(tmp_path) -> None:
    async def exercise() -> tuple[bool, int]:
        store = _store(tmp_path / 'cache')
        issued_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        token = store.save_token(
            access_token='rejected-token',
            token_type='Bearer',
            issued_at=issued_at,
            expires_at=issued_at + timedelta(hours=1),
            refresh_after=issued_at + timedelta(minutes=55),
        )
        entered = threading.Event()
        release = threading.Event()
        heartbeat_ticks = 0
        original_write = KISTokenCacheStore._write_json

        def blocking_write(cache_store, path, payload) -> None:
            if payload.get('record_type') == 'revocation':
                entered.set()
                if not release.wait(timeout=0.5):
                    raise AssertionError('cache write blocked the event loop')
            original_write(cache_store, path, payload)

        async def heartbeat() -> None:
            nonlocal heartbeat_ticks
            while not entered.is_set():
                await asyncio.sleep(0)
            for _ in range(3):
                heartbeat_ticks += 1
                await asyncio.sleep(0)
            release.set()

        with patch.object(KISTokenCacheStore, '_write_json', new=blocking_write):
            invalidated, _ = await asyncio.gather(
                store.invalidate_if_matches(
                    access_token=token.access_token,
                    minimum_issue_interval_seconds=60,
                ),
                heartbeat(),
            )
        return invalidated, heartbeat_ticks

    invalidated, heartbeat_ticks = asyncio.run(exercise())

    assert invalidated is True
    assert heartbeat_ticks == 3


def test_cancelled_cache_write_completes_before_shared_lock_is_released(tmp_path) -> None:
    async def exercise() -> tuple[bool, str]:
        store = _store(tmp_path / 'cache')
        issued_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        token = store.save_token(
            access_token='cancelled-write-token',
            token_type='Bearer',
            issued_at=issued_at,
            expires_at=issued_at + timedelta(hours=1),
            refresh_after=issued_at + timedelta(minutes=55),
        )
        entered = threading.Event()
        release = threading.Event()
        original_write = KISTokenCacheStore._write_json

        def blocking_write(cache_store, path, payload) -> None:
            if payload.get('record_type') == 'revocation':
                entered.set()
                if not release.wait(timeout=1):
                    raise AssertionError('cache write was never released')
            original_write(cache_store, path, payload)

        with patch.object(KISTokenCacheStore, '_write_json', new=blocking_write):
            invalidation = asyncio.create_task(
                store.invalidate_if_matches(
                    access_token=token.access_token,
                    minimum_issue_interval_seconds=60,
                )
            )
            while not entered.is_set():
                await asyncio.sleep(0)
            invalidation.cancel()
            await asyncio.sleep(0.02)
            cancellation_waited_for_write = not invalidation.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await invalidation
            state = await run_token_cache_io(store.load_cache_state)
        return cancellation_waited_for_write, type(state).__name__

    cancellation_waited, state_type = asyncio.run(exercise())

    assert cancellation_waited is True
    assert state_type == 'CachedKISTokenRevocation'


def test_cache_io_cancellation_wins_over_late_worker_exception() -> None:
    async def exercise() -> None:
        entered = threading.Event()
        release = threading.Event()

        def late_failure() -> None:
            entered.set()
            if not release.wait(timeout=1):
                raise AssertionError('worker was never released')
            raise KISTokenCacheError('late cache I/O failure')

        def ordinary_failure() -> None:
            raise KISTokenCacheError('ordinary cache I/O failure')

        operation = asyncio.create_task(run_token_cache_io(late_failure))
        while not entered.is_set():
            await asyncio.sleep(0)
        operation.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation

        with pytest.raises(KISTokenCacheError, match='ordinary cache I/O failure'):
            await run_token_cache_io(ordinary_failure)

    asyncio.run(exercise())


def test_lock_open_cancellation_wins_over_late_open_exception(tmp_path) -> None:
    async def exercise() -> None:
        store = _store(tmp_path / 'cache')
        entered = threading.Event()
        release = threading.Event()

        def late_open_failure() -> int:
            entered.set()
            if not release.wait(timeout=1):
                raise AssertionError('lock opener was never released')
            raise KISTokenCacheError('late lock open failure')

        async def use_lock() -> None:
            async with store.lock():
                raise AssertionError('lock unexpectedly opened')

        with patch.object(store, 'open_lock', new=late_open_failure):
            lock_user = asyncio.create_task(use_lock())
            while not entered.is_set():
                await asyncio.sleep(0)
            lock_user.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await lock_user

        with (
            patch.object(
                store,
                'open_lock',
                side_effect=KISTokenCacheError('ordinary lock open failure'),
            ),
            pytest.raises(KISTokenCacheError, match='ordinary lock open failure'),
        ):
            async with store.lock():
                raise AssertionError('lock unexpectedly opened')

    asyncio.run(exercise())
