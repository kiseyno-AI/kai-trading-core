from __future__ import annotations

from enum import StrEnum


class OrderRequestSource(StrEnum):
    MANUAL = 'manual'
    STRATEGY = 'strategy'
    AGENT = 'agent'
    API = 'api'
    SYSTEM = 'system'


class OrderSide(StrEnum):
    BUY = 'buy'
    SELL = 'sell'


class OrderType(StrEnum):
    MARKET = 'market'
    LIMIT = 'limit'
    STOP = 'stop'
    STOP_LIMIT = 'stop_limit'
    BEST = 'best'


class TimeInForce(StrEnum):
    DAY = 'day'
    IOC = 'ioc'
    FOK = 'fok'
    GTC = 'gtc'
    GTD = 'gtd'


class OrderRequestValidationStatus(StrEnum):
    PENDING = 'pending'
    VALID = 'valid'
    INVALID = 'invalid'


class OrderRequestSubmissionStatus(StrEnum):
    PENDING_SUBMIT = 'pending_submit'
    SUBMITTING = 'submitting'
    SUBMITTED = 'submitted'
    FAILED = 'failed'
    REJECTED = 'rejected'


class BrokerOrderStatus(StrEnum):
    PENDING_SUBMIT = 'pending_submit'
    SUBMITTED = 'submitted'
    ACCEPTED = 'accepted'
    PARTIAL_FILL = 'partial_fill'
    FILLED = 'filled'
    CANCEL_PENDING = 'cancel_pending'
    CANCELED = 'canceled'
    REPLACE_PENDING = 'replace_pending'
    REPLACED = 'replaced'
    REJECTED = 'rejected'
    RECONCILE_ONLY = 'reconcile_only'


class BrokerOrderEventType(StrEnum):
    SUBMIT_REQUEST = 'submit_request'
    SUBMIT_RESPONSE = 'submit_response'
    ACCEPT = 'accept'
    PARTIAL_FILL = 'partial_fill'
    FILL = 'fill'
    CANCEL_REQUEST = 'cancel_request'
    CANCEL_CONFIRMED = 'cancel_confirmed'
    REPLACE_REQUEST = 'replace_request'
    REPLACE_CONFIRMED = 'replace_confirmed'
    REJECT = 'reject'
    RECONCILE_SNAPSHOT = 'reconcile_snapshot'
    REALTIME_EXECUTION_NOTICE = 'realtime_execution_notice'
    MANUAL_PATCH = 'manual_patch'


class BrokerOrderEventSource(StrEnum):
    LOCAL_REQUEST = 'local_request'
    REST_SUBMIT = 'rest_submit'
    REST_QUERY = 'rest_query'
    WS_NOTICE = 'ws_notice'
    POLL_RECONCILE = 'poll_reconcile'
    MANUAL_PATCH = 'manual_patch'
