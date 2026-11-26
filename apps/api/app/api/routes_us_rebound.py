from datetime import date, datetime

from fastapi import APIRouter, HTTPException

from app.core.settings import settings
from app.overnight_rebound.runner import KST, load_us_rebound_status


router = APIRouter(prefix='/strategies/us-rebound', tags=['strategies'])


@router.get('/status')
def get_us_rebound_status(target_trade_date: date | None = None) -> dict:
    """Read strategy evidence; never collect data or grant order authority."""
    now = datetime.now(KST)
    effective_target = target_trade_date or now.date()
    try:
        value = load_us_rebound_status(
            target_trade_date=effective_target, state_dir=settings.us_rebound_state_dir,
        )
    except (ValueError, OSError) as exc:
        raise HTTPException(503, detail={'code': 'us_rebound_status_unavailable', 'error_type': type(exc).__name__}) from exc
    execution = value.get('execution') or {}
    registered = any(item.get('status') in {'accepted', 'activation_pending', 'active'} for item in execution.get('items', []))
    requested = (settings.us_rebound_mode == 'paper'
                 and effective_target == now.date()
                 and str(settings.us_rebound_execution_enabled).strip().lower() in {'true', '1'}
                 and execution.get('mode') == settings.us_rebound_mode)
    return {
        **value, 'server_time': now.isoformat(), 'mode': settings.us_rebound_mode,
        'enabled_from': settings.us_rebound_enabled_from,
        'order_execution_enabled': requested and execution.get('status') == 'active',
        'execution_connection': 'rule_v4' if registered else 'not_connected',
        'schedule_kst': {'collection': ['07:00', '07:05', '07:10'], 'evaluation': '07:15',
                         'execution_evidence': ['08:20', '08:30'], 'deadline': '08:40'},
    }
