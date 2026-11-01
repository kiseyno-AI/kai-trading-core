from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from contextvars import ContextVar
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import httpx

from app.core.settings import settings

API_AUDIT_LOGGER_NAME = 'kai.audit.api'
UPSTREAM_AUDIT_LOGGER_NAME = 'kai.audit.upstream'

_request_id_var: ContextVar[str | None] = ContextVar('request_id', default=None)
_configured_signature: tuple[Any, ...] | None = None

_SENSITIVE_KEYS = {
    'access_token',
    'api_key',
    'appkey',
    'appsecret',
    'auth',
    'authkey',
    'authorization',
    'cookie',
    'crtfc_key',
    'password',
    'proxy-authorization',
    'refresh_token',
    'secret',
    'set-cookie',
    'token',
    'upstream_bearer_token',
    'x-api-key',
}
_SAFE_HEADERS = {'accept', 'content-length', 'content-type', 'host', 'user-agent', 'x-request-id'}
_MAX_AUDIT_STRING_LENGTH = 300
_MAX_AUDIT_COLLECTION_ITEMS = 25
_MAX_AUDIT_MAPPING_ITEMS = 25
_MAX_AUDIT_MAPPING_DEPTH = 6
_AUDIT_DIAGNOSTIC_KEYS = frozenset(
    {
        'status',
        'reason',
        'error',
        'error_message',
        'id',
        'run_id',
        'eod_run_id',
        'pipeline_run_id',
        'market_date',
        'trade_date',
        'started_at',
        'completed_at',
        'updated_at',
        'symbols_total',
        'symbols_succeeded',
        'symbols_failed',
    }
)
_IDEMPOTENT_READ_METHODS = frozenset({'GET', 'HEAD'})
_TRANSIENT_READ_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})
_MAX_READ_ATTEMPTS = 3
_READ_RETRY_BASE_DELAY_SECONDS = 0.25
_READ_RETRY_MAX_DELAY_SECONDS = 2.0


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = getattr(record, 'payload', None)
        if not isinstance(payload, dict):
            payload = {'message': record.getMessage()}
        return json.dumps(payload, ensure_ascii=True, sort_keys=True)


def get_request_id() -> str | None:
    return _request_id_var.get()


def set_request_id(request_id: str) -> object:
    return _request_id_var.set(request_id)


def reset_request_id(token: object) -> None:
    _request_id_var.reset(token)


def _is_sensitive_key(key: str) -> bool:
    normalized = key.strip().lower()
    return normalized in _SENSITIVE_KEYS or normalized.endswith('_token') or normalized.endswith('_secret')


def redact_mapping(values: Mapping[str, Any] | None) -> dict[str, Any]:
    if not values:
        return {}
    redacted: dict[str, Any] = {}
    for key, value in values.items():
        redacted[key] = '[REDACTED]' if _is_sensitive_key(key) else value
    return redacted


def _truncate_string(value: str, *, limit: int = _MAX_AUDIT_STRING_LENGTH) -> str:
    if len(value) <= limit:
        return value
    return f'{value[: limit - 3]}...'


def _sanitize_audit_value(
    value: Any,
    *,
    key: str | None = None,
    depth: int = 0,
) -> Any:
    if key and _is_sensitive_key(key):
        return '[REDACTED]'
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate_string(value)
    if isinstance(value, Mapping):
        if depth >= _MAX_AUDIT_MAPPING_DEPTH:
            return {
                '_truncated': 'max_mapping_depth',
                'field_count': len(value),
            }
        ordered_items = sorted(
            enumerate(value.items()),
            key=lambda indexed_item: (
                0
                if str(indexed_item[1][0]) in _AUDIT_DIAGNOSTIC_KEYS
                else 1,
                indexed_item[0],
            ),
        )
        selected_items = ordered_items[:_MAX_AUDIT_MAPPING_ITEMS]
        sanitized = {
            str(item_key): _sanitize_audit_value(
                item_value,
                key=str(item_key),
                depth=depth + 1,
            )
            for _, (item_key, item_value) in selected_items
        }
        omitted = len(ordered_items) - len(selected_items)
        if omitted:
            sanitized['_omitted_field_count'] = omitted
        return sanitized
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        sanitized = [
            _sanitize_audit_value(item, depth=depth + 1)
            for item in items[:_MAX_AUDIT_COLLECTION_ITEMS]
        ]
        if len(items) > _MAX_AUDIT_COLLECTION_ITEMS:
            sanitized.append(f'... ({len(items) - _MAX_AUDIT_COLLECTION_ITEMS} more)')
        return sanitized
    return _truncate_string(str(value))


def redact_url(url: str, *, secret_values: Iterable[str] | None = None) -> str:
    parsed = urlsplit(url)
    if not parsed.query:
        redacted_url = url
    else:
        query = []
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            query.append((key, '[REDACTED]' if _is_sensitive_key(key) else value))
        redacted_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))

    for secret_value in secret_values or ():
        if secret_value:
            redacted_url = redacted_url.replace(secret_value, '[REDACTED]')
    return redacted_url


def sanitize_headers(headers: Mapping[str, str] | None) -> dict[str, Any]:
    if not headers:
        return {}
    sanitized: dict[str, Any] = {}
    for key, value in headers.items():
        lowered = key.lower()
        if lowered in _SAFE_HEADERS:
            sanitized[lowered] = value
        elif _is_sensitive_key(lowered):
            sanitized[lowered] = '[REDACTED]'
    return sanitized


def get_log_directory() -> Path:
    return Path(settings.api_log_directory).expanduser().resolve()


def _build_handler(filename: str) -> RotatingFileHandler:
    log_dir = get_log_directory()
    log_dir.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        log_dir / filename,
        maxBytes=settings.api_log_rotation_max_bytes,
        backupCount=settings.api_log_rotation_backup_count,
        encoding='utf-8',
    )
    handler.setFormatter(JsonLineFormatter())
    return handler


def _configure_named_logger(name: str, *, filename: str) -> None:
    logger = logging.getLogger(name)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(_build_handler(filename))


def configure_file_logging(*, force: bool = False) -> None:
    global _configured_signature

    signature = (
        settings.api_request_logging_enabled,
        settings.upstream_http_logging_enabled,
        settings.api_log_directory,
        settings.api_request_log_file_name,
        settings.upstream_http_log_file_name,
        settings.api_log_rotation_max_bytes,
        settings.api_log_rotation_backup_count,
    )
    if not force and _configured_signature == signature:
        return

    for name in (API_AUDIT_LOGGER_NAME, UPSTREAM_AUDIT_LOGGER_NAME):
        logger = logging.getLogger(name)
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()

    if settings.api_request_logging_enabled:
        _configure_named_logger(API_AUDIT_LOGGER_NAME, filename=settings.api_request_log_file_name)
    if settings.upstream_http_logging_enabled:
        _configure_named_logger(UPSTREAM_AUDIT_LOGGER_NAME, filename=settings.upstream_http_log_file_name)

    _configured_signature = signature


def reset_logging_configuration_for_tests() -> None:
    global _configured_signature
    for name in (API_AUDIT_LOGGER_NAME, UPSTREAM_AUDIT_LOGGER_NAME):
        logger = logging.getLogger(name)
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()
    _configured_signature = None


def _emit(logger_name: str, payload: dict[str, Any]) -> None:
    logging.getLogger(logger_name).info(payload.get('event', 'audit'), extra={'payload': payload})


def log_api_request_event(payload: dict[str, Any]) -> None:
    if not settings.api_request_logging_enabled:
        return
    _emit(API_AUDIT_LOGGER_NAME, payload)


def log_upstream_event(payload: dict[str, Any]) -> None:
    if not settings.upstream_http_logging_enabled:
        return
    _emit(UPSTREAM_AUDIT_LOGGER_NAME, payload)


def log_fetch_event(
    *,
    service: str,
    operation: str,
    status: str,
    **fields: Any,
) -> None:
    payload = {
        'event': 'fetch_operation',
        'request_id': get_request_id(),
        'service': service,
        'operation': operation,
        'status': status,
    }
    for key, value in fields.items():
        if value is None:
            continue
        payload[key] = _sanitize_audit_value(value, key=key)
    log_upstream_event(payload)


def exception_message(error: BaseException) -> str:
    message = str(error).strip()
    return message or type(error).__name__


def build_api_request_event(
    *,
    request_id: str,
    method: str,
    path: str,
    query_params: Mapping[str, Any] | None,
    client_ip: str | None,
    user_agent: str | None,
    status_code: int,
    duration_ms: float,
    request_headers: Mapping[str, str] | None,
    response_headers: Mapping[str, str] | None,
    error: Exception | None = None,
) -> dict[str, Any]:
    response_content_length = None
    if response_headers:
        response_content_length = response_headers.get('content-length')
    return {
        'event': 'api_request',
        'request_id': request_id,
        'method': method,
        'path': path,
        'query_params': redact_mapping(query_params),
        'client_ip': client_ip,
        'user_agent': user_agent,
        'status_code': status_code,
        'duration_ms': round(duration_ms, 3),
        'request_headers': sanitize_headers(request_headers),
        'request_content_length': (request_headers or {}).get('content-length'),
        'request_content_type': (request_headers or {}).get('content-type'),
        'response_content_length': response_content_length,
        'response_content_type': (response_headers or {}).get('content-type') if response_headers else None,
        'error_type': type(error).__name__ if error else None,
        'error_message': exception_message(error) if error else None,
    }


def _request_content_length(request: httpx.Request) -> int | None:
    if request.content is None:
        return None
    try:
        return len(request.content)
    except TypeError:
        return None


async def request_with_audit(
    client: httpx.AsyncClient,
    service: str,
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    json_body: Any = None,
    secret_values: Iterable[str] | None = None,
) -> httpx.Response:
    request_headers = dict(headers or {})
    if get_request_id() and 'X-Request-ID' not in request_headers:
        request_headers['X-Request-ID'] = get_request_id() or ''

    request = client.build_request(method, url, headers=request_headers, params=params, json=json_body)
    started_at = time.perf_counter()
    error: Exception | None = None
    response: httpx.Response | None = None

    try:
        response = await client.send(request)
        return response
    except Exception as exc:
        error = exc
        raise
    finally:
        duration_ms = (time.perf_counter() - started_at) * 1000
        sanitized_url = redact_url(str(request.url), secret_values=secret_values)
        payload = {
            'event': 'upstream_request',
            'request_id': get_request_id(),
            'service': service,
            'method': request.method,
            'url': sanitized_url,
            'target_host': request.url.host,
            'path': request.url.path,
            'request_headers': sanitize_headers(request.headers),
            'request_content_length': _request_content_length(request),
            'request_content_type': request.headers.get('content-type'),
            'status_code': response.status_code if response else None,
            'duration_ms': round(duration_ms, 3),
            'response_content_length': response.headers.get('content-length') if response else None,
            'response_content_type': response.headers.get('content-type') if response else None,
            'error_type': type(error).__name__ if error else None,
            'error_message': exception_message(error) if error else None,
        }
        log_upstream_event(payload)


def _retry_after_delay_seconds(value: str | None) -> float | None:
    normalized = str(value or '').strip()
    if not normalized:
        return None
    try:
        delay = float(normalized)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(normalized)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
    if not math.isfinite(delay):
        return None
    return min(max(0.0, delay), _READ_RETRY_MAX_DELAY_SECONDS)


async def request_read_with_retry_and_audit(
    client: httpx.AsyncClient,
    service: str,
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    json_body: Any = None,
    secret_values: Iterable[str] | None = None,
    max_attempts: int = _MAX_READ_ATTEMPTS,
) -> httpx.Response:
    normalized_method = str(method or '').strip().upper()
    if normalized_method not in _IDEMPOTENT_READ_METHODS:
        raise ValueError('Upstream read retries are limited to GET and HEAD requests')
    if not 1 <= max_attempts <= _MAX_READ_ATTEMPTS:
        raise ValueError(f'max_attempts must be between 1 and {_MAX_READ_ATTEMPTS}')

    for attempt in range(1, max_attempts + 1):
        try:
            response = await request_with_audit(
                client,
                service,
                normalized_method,
                url,
                headers=headers,
                params=params,
                json_body=json_body,
                secret_values=secret_values,
            )
        except httpx.TransportError as exc:
            if attempt >= max_attempts:
                raise
            delay = min(
                _READ_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
                _READ_RETRY_MAX_DELAY_SECONDS,
            )
            log_fetch_event(
                service=service,
                operation='upstream_read_retry',
                status='retrying',
                method=normalized_method,
                attempt=attempt,
                max_attempts=max_attempts,
                retry_delay_seconds=delay,
                error_type=type(exc).__name__,
                error_message=exception_message(exc),
            )
            await asyncio.sleep(delay)
            continue

        if response.status_code not in _TRANSIENT_READ_STATUS_CODES or attempt >= max_attempts:
            return response

        retry_after_delay = _retry_after_delay_seconds(response.headers.get('Retry-After'))
        delay = retry_after_delay
        if delay is None:
            delay = min(
                _READ_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
                _READ_RETRY_MAX_DELAY_SECONDS,
            )
        status_code = response.status_code
        await response.aclose()
        log_fetch_event(
            service=service,
            operation='upstream_read_retry',
            status='retrying',
            method=normalized_method,
            attempt=attempt,
            max_attempts=max_attempts,
            retry_delay_seconds=delay,
            status_code=status_code,
        )
        await asyncio.sleep(delay)

    raise AssertionError('unreachable upstream read retry state')


def download_bytes_with_audit(*, service: str, url: str, timeout: float) -> bytes:
    request_headers: dict[str, str] = {}
    if get_request_id():
        request_headers['X-Request-ID'] = get_request_id() or ''

    request = Request(url, headers=request_headers)
    started_at = time.perf_counter()
    error: Exception | None = None
    status_code: int | None = None
    response_headers: Mapping[str, Any] | None = None
    payload = b''

    try:
        with urlopen(request, timeout=timeout) as response:
            status_code = getattr(response, 'status', None)
            response_headers = dict(response.headers.items())
            payload = response.read()
            return payload
    except Exception as exc:
        error = exc
        raise
    finally:
        duration_ms = (time.perf_counter() - started_at) * 1000
        log_upstream_event(
            {
                'event': 'upstream_request',
                'request_id': get_request_id(),
                'service': service,
                'method': 'GET',
                'url': redact_url(url),
                'target_host': urlsplit(url).hostname,
                'path': urlsplit(url).path,
                'request_headers': sanitize_headers(request_headers),
                'request_content_length': None,
                'request_content_type': None,
                'status_code': status_code,
                'duration_ms': round(duration_ms, 3),
                'response_content_length': response_headers.get('content-length') if response_headers else len(payload) or None,
                'response_content_type': response_headers.get('content-type') if response_headers else None,
                'error_type': type(error).__name__ if error else None,
                'error_message': exception_message(error) if error else None,
            }
        )
