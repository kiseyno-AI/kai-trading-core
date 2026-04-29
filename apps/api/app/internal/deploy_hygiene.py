from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any, Iterable

import httpx
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.engine.url import make_url

from app.core.settings import settings
from app.db import DATABASE_URL, engine, get_alembic_config
from app.market.snapshot import market_snapshot_service
from app.orders.portfolio_summary_service import portfolio_summary_service

OK_STATUS = 'ok'
WARN_STATUS = 'warn'
FAIL_STATUS = 'fail'


def _market_snapshot_runtime_actions(runtime_status: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    if runtime_status.get('effective_kis_environment') != 'real':
        actions.append(
            'Enable MARKET_SNAPSHOT_KIS_USE_REAL=true or run with KIS_USE_MOCK=false so /market/snapshot uses the real KIS domain.'
        )
    if not runtime_status.get('kis_app_key_configured') or not runtime_status.get('kis_app_secret_configured'):
        actions.append('Set KIS_APP_KEY and KIS_APP_SECRET in apps/api/.env before deploying market snapshot.')
    if not runtime_status.get('koreaexim_api_key_configured'):
        actions.append('Set KOREAEXIM_API_KEY in apps/api/.env before deploying market snapshot.')
    actions.append(
        'After restart, run the deploy smoke check against /health, /eod/freshness, /market/snapshot, and /portfolio/summary.'
    )
    return actions


def _portfolio_summary_runtime_actions(runtime_status: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    if not runtime_status.get('portfolio_account_references_configured'):
        actions.append(
            'Set PORTFOLIO_ACCOUNT_REFERENCES in apps/api/.env to one or more Label=KIS:<account_number>-<product_code> entries.'
        )
    elif not runtime_status.get('portfolio_account_references_valid'):
        actions.append(
            'Fix PORTFOLIO_ACCOUNT_REFERENCES so each entry uses KIS:<account_number>-<product_code> or Label=KIS:<account_number>-<product_code>.'
        )
    if not runtime_status.get('kis_app_key_configured') or not runtime_status.get('kis_app_secret_configured'):
        actions.append('Set KIS_APP_KEY and KIS_APP_SECRET in apps/api/.env before deploying portfolio summary.')
    if runtime_status.get('app_env', '').lower() == 'production' and runtime_status.get('kis_use_mock'):
        actions.append('Set KIS_USE_MOCK=false in production so portfolio summary uses live KIS account inquiries.')
    actions.append('After restart, verify GET /portfolio/summary returns sync.status other than not_configured.')
    return actions


def _redacted_database_url() -> str:
    try:
        return make_url(DATABASE_URL).render_as_string(hide_password=True)
    except Exception:
        return DATABASE_URL


def _parse_create_table_metadata(path: Path) -> tuple[list[str], bool]:
    source = path.read_text(encoding='utf-8')
    create_tables: list[str] = []

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return create_tables, False

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        function = node.func
        if not isinstance(function, ast.Attribute) or function.attr != 'create_table' or not node.args:
            continue

        first_arg = node.args[0]
        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
            create_tables.append(first_arg.value)

    duplicate_table_guarded = any(
        marker in source for marker in ('_table_exists(', '.has_table(', 'checkfirst=True')
    )
    return sorted(dict.fromkeys(create_tables)), duplicate_table_guarded


def _load_revision_catalog() -> tuple[tuple[str, ...], list[dict[str, Any]]]:
    script_dir = ScriptDirectory.from_config(get_alembic_config())
    chain = list(script_dir.walk_revisions(base='base', head='heads'))
    chain.reverse()

    catalog: list[dict[str, Any]] = []
    for revision in chain:
        path = Path(revision.path)
        create_tables, duplicate_table_guarded = _parse_create_table_metadata(path)
        catalog.append(
            {
                'revision': revision.revision,
                'migration': path.name,
                'create_tables': create_tables,
                'duplicate_table_guarded': duplicate_table_guarded,
            }
        )

    return tuple(script_dir.get_heads()), catalog


def _build_preflight_report(*, require_head: bool) -> dict[str, Any]:
    redacted_database_url = _redacted_database_url()
    market_snapshot_runtime = market_snapshot_service.runtime_status()
    portfolio_summary_runtime = portfolio_summary_service.runtime_status()

    try:
        head_revisions, revision_catalog = _load_revision_catalog()
        with engine.connect() as connection:
            existing_tables = sorted(sqlalchemy_inspect(connection).get_table_names())
            current_revisions = tuple(MigrationContext.configure(connection).get_current_heads())
    except Exception as exc:
        return {
            'status': FAIL_STATUS,
            'safe_to_continue': False,
            'require_head': require_head,
            'summary': 'Database migration preflight could not connect to the configured database.',
            'database': {
                'connected': False,
                'url': redacted_database_url,
                'error': str(exc),
            },
            'alembic': {
                'head_revisions': [],
                'current_revisions': [],
                'at_head': False,
                'pending_revision_count': 0,
                'pending_revisions': [],
            },
            'drift': {
                'status': 'unknown',
                'heuristic': 'Skipped because the database connection failed before Alembic state inspection.',
                'findings': [],
            },
            'recommended_actions': [
                'Verify the configured database is reachable from apps/api and rerun the preflight.',
            ],
            'runtime_checks': {
                'market_snapshot': market_snapshot_runtime,
                'portfolio_summary': portfolio_summary_runtime,
            },
        }

    revision_ids = [item['revision'] for item in revision_catalog]
    head_revisions_list = list(head_revisions)
    current_revisions_list = list(current_revisions)
    at_head = bool(head_revisions) and tuple(current_revisions) == tuple(head_revisions)

    pending_revisions: list[dict[str, Any]] = []
    drift_findings: list[dict[str, Any]] = []
    status = OK_STATUS
    summary = 'Database is already at the Alembic head revision.'
    recommended_actions = [
        'After restart, run the deploy smoke check against /health, /eod/freshness, /market/snapshot, and /portfolio/summary.',
    ]

    if len(head_revisions) != 1:
        status = FAIL_STATUS
        summary = 'Repository Alembic state has multiple heads; deployment should stop until the revision graph is linearized.'
        recommended_actions = [
            'Resolve the multiple Alembic heads in the repo before deploying this service.',
        ]
    elif len(current_revisions) > 1:
        status = FAIL_STATUS
        summary = 'Database is stamped to multiple Alembic revisions; deployment should stop until the version table is reconciled.'
        recommended_actions = [
            'Inspect the alembic_version table and reconcile it to a single linear revision before restarting the API.',
        ]
    elif current_revisions and current_revisions[0] not in revision_ids:
        status = FAIL_STATUS
        summary = 'Database is stamped to an Alembic revision that does not exist in this repo checkout.'
        recommended_actions = [
            'Verify the deployed code and the database revision history are from the same migration lineage before proceeding.',
        ]
    else:
        if not at_head:
            if current_revisions:
                current_index = revision_ids.index(current_revisions[0])
                pending_revisions = revision_catalog[current_index + 1 :]
            else:
                pending_revisions = revision_catalog

            existing_table_set = set(existing_tables)
            for revision in pending_revisions:
                if revision['duplicate_table_guarded']:
                    continue
                overlapping_tables = sorted(existing_table_set.intersection(revision['create_tables']))
                if overlapping_tables:
                    drift_findings.append(
                        {
                            'revision': revision['revision'],
                            'migration': revision['migration'],
                            'tables': overlapping_tables,
                        }
                    )

            if drift_findings:
                status = FAIL_STATUS
                summary = (
                    'Likely duplicate-table drift detected: pending unguarded create-table revisions already have '
                    'matching live tables.'
                )
                recommended_actions = [
                    'Stop automatic restart or migration retries for this deploy.',
                    'Inspect the flagged tables and confirm the live schema matches the pending migration file, including indexes and constraints.',
                    'Only after schema parity is confirmed, run alembic stamp <matching_revision>; do not stamp blindly.',
                    'If parity does not hold, add a corrective migration instead of dropping tables in place.',
                ]
            elif require_head:
                status = FAIL_STATUS
                summary = 'Database is not at the Alembic head revision.'
                recommended_actions = [
                    'Run alembic upgrade head before restart, then rerun this preflight with --require-head.',
                    'After restart, run the deploy smoke check against /health, /eod/freshness, /market/snapshot, and /portfolio/summary.',
                ]
            else:
                status = WARN_STATUS
                summary = (
                    'Database is behind the Alembic head revision, but no likely duplicate-table drift pattern was detected.'
                )
                recommended_actions = [
                    'If DB_AUTO_UPGRADE=true, startup may continue and let Alembic apply the pending revisions.',
                    'For a gated deploy, prefer alembic upgrade head before restart, then rerun this preflight with --require-head.',
                    'After restart, run the deploy smoke check against /health, /eod/freshness, /market/snapshot, and /portfolio/summary.',
                ]

    if not market_snapshot_runtime['ready']:
        if status != FAIL_STATUS:
            status = FAIL_STATUS
            summary = 'Market snapshot runtime is not ready for deploy.'
        recommended_actions.extend(_market_snapshot_runtime_actions(market_snapshot_runtime))
    if not portfolio_summary_runtime['ready']:
        if status != FAIL_STATUS:
            status = FAIL_STATUS
            summary = 'Portfolio summary runtime is not ready for deploy.'
        recommended_actions.extend(_portfolio_summary_runtime_actions(portfolio_summary_runtime))

    return {
        'status': status,
        'safe_to_continue': status != FAIL_STATUS,
        'require_head': require_head,
        'summary': summary,
        'database': {
            'connected': True,
            'url': redacted_database_url,
            'error': None,
            'existing_table_count': len(existing_tables),
        },
        'alembic': {
            'head_revisions': head_revisions_list,
            'current_revisions': current_revisions_list,
            'at_head': at_head,
            'pending_revision_count': len(pending_revisions),
            'pending_revisions': pending_revisions,
        },
        'drift': {
            'status': 'likely_duplicate_table_drift' if drift_findings else 'none',
            'heuristic': (
                'Flags tables that already exist even though they are still created by a pending migration file '
                'without an explicit duplicate-table guard.'
            ),
            'findings': drift_findings,
        },
        'recommended_actions': list(dict.fromkeys(recommended_actions)),
        'runtime_checks': {
            'market_snapshot': market_snapshot_runtime,
            'portfolio_summary': portfolio_summary_runtime,
        },
    }


def _render_preflight_text(report: dict[str, Any]) -> str:
    lines = [f"{report['status'].upper()} migration preflight: {report['summary']}"]
    lines.append(f"Database: {report['database']['url']}")

    current_revisions = report['alembic']['current_revisions']
    head_revisions = report['alembic']['head_revisions']
    lines.append(f"Current revision(s): {', '.join(current_revisions) if current_revisions else '<unversioned>'}")
    lines.append(f"Head revision(s): {', '.join(head_revisions) if head_revisions else '<unknown>'}")

    pending_revisions = report['alembic']['pending_revisions']
    if pending_revisions:
        lines.append(f"Pending revisions: {report['alembic']['pending_revision_count']}")
        for revision in pending_revisions[:12]:
            table_note = ''
            if revision['create_tables']:
                table_note = f" creates tables [{', '.join(revision['create_tables'])}]"
            guard_note = ' guarded' if revision['duplicate_table_guarded'] else ' unguarded'
            lines.append(f"- {revision['revision']} ({revision['migration']}){table_note}; duplicate-table check:{guard_note}")
        remaining = len(pending_revisions) - 12
        if remaining > 0:
            lines.append(f"- ... {remaining} additional pending revision(s) omitted")

    if report['drift']['findings']:
        lines.append('Likely duplicate-table drift:')
        for finding in report['drift']['findings']:
            tables = ', '.join(finding['tables'])
            lines.append(f"- {finding['revision']} ({finding['migration']}): live table(s) already present -> {tables}")

    runtime_checks = report.get('runtime_checks') or {}
    market_snapshot = runtime_checks.get('market_snapshot')
    if isinstance(market_snapshot, dict):
        lines.append(
            'Market snapshot runtime: '
            + ('ready' if market_snapshot.get('ready') else 'blocked')
            + f" (effective_kis_environment={market_snapshot.get('effective_kis_environment')}, "
            + f"uses_real_domain_override={market_snapshot.get('uses_real_domain_override')})"
        )
        for reason in market_snapshot.get('blocking_reasons') or []:
            lines.append(f"- market_snapshot: {reason}")
    portfolio_summary = runtime_checks.get('portfolio_summary')
    if isinstance(portfolio_summary, dict):
        lines.append(
            'Portfolio summary runtime: '
            + ('ready' if portfolio_summary.get('ready') else 'blocked')
            + f" (kis_environment={portfolio_summary.get('kis_environment')}, "
            + f"configured_account_count={portfolio_summary.get('configured_account_count')}, "
            + f"kis_use_mock={portfolio_summary.get('kis_use_mock')})"
        )
        for reason in portfolio_summary.get('blocking_reasons') or []:
            lines.append(f"- portfolio_summary: {reason}")

    if report['database'].get('error'):
        lines.append(f"Database error: {report['database']['error']}")

    if report['recommended_actions']:
        lines.append('Recommended actions:')
        for action in report['recommended_actions']:
            lines.append(f"- {action}")

    return '\n'.join(lines)


def collect_migration_preflight(*, require_head: bool = False) -> tuple[dict[str, Any], int]:
    report = _build_preflight_report(require_head=require_head)
    exit_code = 0 if report['safe_to_continue'] else 1
    if report['drift']['status'] == 'likely_duplicate_table_drift':
        exit_code = 2
    return report, exit_code


def collect_deploy_smoke_check(
    *,
    base_url: str = 'http://127.0.0.1:8000',
    timeout_seconds: float = 10.0,
    transport: httpx.BaseTransport | None = None,
) -> tuple[dict[str, Any], int]:
    headers = {'Accept': 'application/json'}
    if settings.upstream_bearer_token:
        headers['Authorization'] = f'Bearer {settings.upstream_bearer_token}'

    checks: dict[str, Any] = {}
    status = OK_STATUS
    summary = 'Deploy smoke checks passed.'

    with httpx.Client(
        base_url=base_url,
        timeout=timeout_seconds,
        headers=headers,
        transport=transport,
    ) as client:
        for name, path in (
            ('health', '/health'),
            ('eod_freshness', '/eod/freshness'),
            ('market_snapshot', '/market/snapshot'),
            ('portfolio_summary', '/portfolio/summary'),
        ):
            try:
                response = client.get(path)
                payload = response.json()
            except Exception as exc:
                checks[name] = {
                    'ok': False,
                    'path': path,
                    'error': str(exc),
                }
                status = FAIL_STATUS
                summary = 'Deploy smoke checks failed.'
                continue

            if name == 'health':
                app_status = payload.get('status')
                database_connected = bool((payload.get('database') or {}).get('connected'))
                ok = response.status_code == 200 and app_status == 'ok' and database_connected
                checks[name] = {
                    'ok': ok,
                    'path': path,
                    'status_code': response.status_code,
                    'app_status': app_status,
                    'database_connected': database_connected,
                }
            elif name == 'market_snapshot':
                markets = payload.get('markets') if isinstance(payload.get('markets'), dict) else {}
                kospi = markets.get('kospi') if isinstance(markets.get('kospi'), dict) else {}
                kosdaq = markets.get('kosdaq') if isinstance(markets.get('kosdaq'), dict) else {}
                usd_krw = payload.get('usd_krw') if isinstance(payload.get('usd_krw'), dict) else {}
                ok = (
                    response.status_code == 200
                    and bool(payload.get('as_of'))
                    and usd_krw.get('rate') is not None
                    and kospi.get('value') is not None
                    and kosdaq.get('value') is not None
                )
                checks[name] = {
                    'ok': ok,
                    'path': path,
                    'status_code': response.status_code,
                    'as_of': payload.get('as_of'),
                    'usd_krw_rate': usd_krw.get('rate'),
                    'kospi_value': kospi.get('value'),
                    'kosdaq_value': kosdaq.get('value'),
                    'detail': payload.get('detail'),
                }
            elif name == 'portfolio_summary':
                sync = payload.get('sync') if isinstance(payload.get('sync'), dict) else {}
                accounts = payload.get('accounts') if isinstance(payload.get('accounts'), list) else []
                sync_status = str(sync.get('status') or '').strip()
                ok = (
                    response.status_code == 200
                    and sync_status in {'synced', 'stale', 'unavailable'}
                    and len(accounts) > 0
                )
                checks[name] = {
                    'ok': ok,
                    'path': path,
                    'status_code': response.status_code,
                    'sync_status': sync_status,
                    'accounts_count': len(accounts),
                    'detail': sync.get('message'),
                }
            else:
                today = payload.get('today') if isinstance(payload.get('today'), dict) else {}
                latest = payload.get('latest') if isinstance(payload.get('latest'), dict) else {}
                ok = response.status_code == 200 and bool(today.get('trade_date'))
                checks[name] = {
                    'ok': ok,
                    'path': path,
                    'status_code': response.status_code,
                    'today_trade_date': today.get('trade_date'),
                    'latest_trade_date': latest.get('trade_date') or latest.get('price_trade_date'),
                    'latest_updated_at': latest.get('updated_at'),
                }

            if not checks[name]['ok']:
                status = FAIL_STATUS
                summary = 'Deploy smoke checks failed.'

    report = {
        'status': status,
        'base_url': base_url,
        'summary': summary,
        'checks': checks,
    }
    return report, 0 if status == OK_STATUS else 1


def _render_smoke_check_text(report: dict[str, Any]) -> str:
    lines = [f"{report['status'].upper()} deploy smoke check: {report['summary']}"]
    lines.append(f"Base URL: {report['base_url']}")
    for name, check in report['checks'].items():
        detail_parts = [f"path={check['path']}"]
        if 'status_code' in check:
            detail_parts.append(f"status_code={check['status_code']}")
        if name == 'health':
            detail_parts.append(f"app_status={check.get('app_status')}")
            detail_parts.append(f"database_connected={check.get('database_connected')}")
        elif name == 'market_snapshot':
            detail_parts.append(f"as_of={check.get('as_of')}")
            detail_parts.append(f"usd_krw_rate={check.get('usd_krw_rate')}")
            detail_parts.append(f"kospi_value={check.get('kospi_value')}")
            detail_parts.append(f"kosdaq_value={check.get('kosdaq_value')}")
        elif name == 'portfolio_summary':
            detail_parts.append(f"sync_status={check.get('sync_status')}")
            detail_parts.append(f"accounts_count={check.get('accounts_count')}")
        else:
            detail_parts.append(f"today_trade_date={check.get('today_trade_date')}")
            detail_parts.append(f"latest_trade_date={check.get('latest_trade_date')}")
        if check.get('detail'):
            detail_parts.append(f"detail={check['detail']}")
        if check.get('error'):
            detail_parts.append(f"error={check['error']}")
        lines.append(f"- {name}: {'ok' if check['ok'] else 'fail'} ({', '.join(detail_parts)})")
    return '\n'.join(lines)


def _emit_report(report: dict[str, Any], *, output_format: str, renderer) -> None:
    if output_format == 'text':
        print(renderer(report))
        return
    print(json.dumps(report, ensure_ascii=False))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Deployment hygiene helpers for kai-trading-core.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    preflight_parser = subparsers.add_parser(
        'preflight',
        help='Inspect Alembic current/head state and flag likely duplicate-table drift before deploy or restart.',
    )
    preflight_parser.add_argument('--require-head', action='store_true')
    preflight_parser.add_argument('--format', choices=('json', 'text'), default='json')

    smoke_parser = subparsers.add_parser(
        'smoke-check',
        help='Hit /health, /eod/freshness, and /market/snapshot after restart and fail if the service does not look healthy.',
    )
    smoke_parser.add_argument('--base-url', default='http://127.0.0.1:8000')
    smoke_parser.add_argument('--timeout-seconds', type=float, default=10.0)
    smoke_parser.add_argument('--format', choices=('json', 'text'), default='json')
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == 'preflight':
        report, exit_code = collect_migration_preflight(require_head=args.require_head)
        _emit_report(report, output_format=args.format, renderer=_render_preflight_text)
        return exit_code

    if args.command == 'smoke-check':
        report, exit_code = collect_deploy_smoke_check(
            base_url=args.base_url,
            timeout_seconds=args.timeout_seconds,
        )
        _emit_report(report, output_format=args.format, renderer=_render_smoke_check_text)
        return exit_code

    parser.error(f'Unsupported command: {args.command}')


if __name__ == '__main__':
    raise SystemExit(main())
