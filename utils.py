"""
utils.py
========
Shared utilities: logging setup and the _log_crash helper.
Import logger and _log_crash from here across all modules.
"""

import logging
import os
from typing import Any

_LOG_LEVEL = os.environ.get('LUMVI_LOG_LEVEL', 'INFO').upper()


def get_logger(name: str) -> logging.Logger:
    lg = logging.getLogger(name)
    lg.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
    return lg


def log_crash(logger: logging.Logger, tag: str, err: Exception, **ctx: Any) -> None:
    """Structured exception logger. All modules call this instead of bare logger.error."""
    ctx_str = ' '.join(f'{k}={v}' for k, v in ctx.items())
    logger.error(f"[{tag}] {type(err).__name__}: {err} | {ctx_str}", exc_info=True)
