"""Make the application's own INFO logs visible.

Nothing in the project configured the root logger, so Python's last-resort handler printed only
WARNING and above. Every ``logger.info`` in the runner, the pipeline and the agent services was
silently dropped, which left stderr with tracebacks but no record of what the daily process was
doing. This attaches one stderr handler to the ``app`` logger namespace only (third-party loggers,
uvicorn's own access log and the audit file handlers are untouched).
"""

from __future__ import annotations

import logging
import sys

_HANDLER_MARKER = '_kai_app_log_handler'
_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


def configure_application_logging(level: str | int = 'INFO') -> logging.Logger:
    """Idempotently route ``app.*`` loggers at ``level`` to stderr."""

    resolved = (
        level
        if isinstance(level, int)
        else logging.getLevelName(str(level or 'INFO').strip().upper())
    )
    if not isinstance(resolved, int):
        resolved = logging.INFO
    app_logger = logging.getLogger('app')
    app_logger.setLevel(resolved)
    if not any(getattr(handler, _HANDLER_MARKER, False) for handler in app_logger.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(_FORMAT))
        setattr(handler, _HANDLER_MARKER, True)
        app_logger.addHandler(handler)
    return app_logger
